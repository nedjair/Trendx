"""Minimal ML pipeline orchestrator: Quality → Preprocessing → Train → Forecast → Anomaly.

W49 implementation gate. Reuses existing services (no duplication) and existing
persistence contracts (no new table, no new run ID):

  * quality: ``DataQualityService.full_check`` + ``generate_report`` (persists
    into ``trendx_analytics.data_quality`` via upsert, existing contract).
  * preprocessing: ``Resampler`` + ``Normalizer`` validation in memory.
    Time segmentation for anomaly detection stays inside
    ``FeatureExtractor``/``AnomalyDetectorService.scan`` (existing contract).
  * train: ``TrainingService.train_model`` (fail-closed customer scope,
    tz-naive, synthetic exclusion ; registration + champion promotion owned
    by ``ModelRegistry.register(promote=True)``, history recorded).
    (existing contract — a freshly trained single model becomes champion so the
    forecast step can resolve it; competition thresholds in ``ModelSelector``
    are unchanged).
  * forecast: ``InferenceService`` forced ``dry_run=True``;
    ``generate_forecast`` + ``save_forecast_results`` (idempotent upsert).
    ``writeback_forecast`` is NEVER called here.
  * anomaly: ``AnomalyDetectorService.scan`` (persists episodes into the
    existing ``anomaly`` table). ``AlertingService`` is never used: no alarms.

Explicit W48-ambiguity decisions (fail-fast, observable, no silent pass):

  * retry inter-étapes: none. A failed step stops the pipeline with status
    ``failed`` and an explicit error; failed runs are NOT cached (retry allowed).
  * dedup inter-étapes: session-scoped in-memory cache keyed by
    ``(entity_id, metric_key, frequency, horizon, algorithm, lookback_days,
    customer_id)``.
    Durable dedup relies on existing contracts (predictions composite-PK
    upsert, ``TaskService`` claim ``SKIP LOCKED`` + terminal-state idempotence).
    A durable run ledger via ``scheduler_run`` is deferred to scheduler wiring.
  * run IDs: no new IDs. Traceability reuses ``execution_id`` (uuid4 per run,
    propagated to every step) + existing ``model_id`` / ``mlflow_run_id``.
  * error policy: ``run_many`` isolates failures per entity/metric; one item
    never corrupts or cancels its siblings.
  * champion promotion: owned by ``ModelRegistry.register(promote=True)``
    (ModelSelector thresholds untouched, status history recorded) ; the
    orchestrator never promotes explicitly.

No broker, no ThingsBoard dependency, UTC everywhere, bounded reads owned by
the reused services (``AGENTS.md``).
"""

from __future__ import annotations

import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

import pandas as pd
from loguru import logger

STEP_QUALITY = "quality"
STEP_PREPROCESSING = "preprocessing"
STEP_TRAIN = "train"
STEP_FORECAST = "forecast"
STEP_ANOMALY = "anomaly"

STEPS: tuple[str, ...] = (
    STEP_QUALITY,
    STEP_PREPROCESSING,
    STEP_TRAIN,
    STEP_FORECAST,
    STEP_ANOMALY,
)

STATUS_OK = "ok"
STATUS_FAILED = "failed"
STATUS_SKIPPED = "skipped"

MIN_PREPARED_ROWS = 10


@dataclass(frozen=True)
class PipelineContext:
    """Entry contract. Key remains tenant/entity/metric (never per-device handlers)."""

    tenant_id: str
    entity_type: str
    entity_id: str
    metric_name: str
    lookback_days: int | None = None
    frequency: str = "1h"
    horizon: int | None = None
    algorithm: str = "Prophet"
    customer_id: str | None = None
    execution_id: str = field(default_factory=lambda: uuid.uuid4().hex)

    def cache_key(self) -> tuple[str, str, str, str, str, str, str]:
        return (
            self.entity_id,
            self.metric_name,
            self.frequency,
            str(self.horizon),
            self.algorithm,
            str(self.lookback_days),
            str(self.customer_id),
        )


@dataclass
class StepResult:
    step: str
    status: str
    detail: dict[str, Any]
    error: str | None = None


@dataclass
class PipelineResult:
    context: PipelineContext
    execution_id: str
    status: str
    steps: dict[str, StepResult]
    model_id: str | None = None
    forecast_points: int = 0
    forecast_saved: int = 0
    anomaly_episodes: int = 0
    writeback: str = "disabled"
    alarms: str = "disabled"
    error: str | None = None
    deduped: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "tenant_id": self.context.tenant_id,
            "entity_type": self.context.entity_type,
            "entity_id": self.context.entity_id,
            "metric_name": self.context.metric_name,
            "execution_id": self.execution_id,
            "status": self.status,
            "steps": {
                name: {
                    "status": step.status,
                    "detail": step.detail,
                    "error": step.error,
                }
                for name, step in self.steps.items()
            },
            "model_id": self.model_id,
            "forecast_points": self.forecast_points,
            "forecast_saved": self.forecast_saved,
            "anomaly_episodes": self.anomaly_episodes,
            "writeback": self.writeback,
            "alarms": self.alarms,
            "error": self.error,
            "deduped": self.deduped,
            "generated_at": datetime.now(UTC).isoformat(),
        }


DataFetcher = Callable[[PipelineContext], pd.DataFrame]


def _default_fetcher(context: PipelineContext) -> pd.DataFrame:
    from trendx.services.training import TrainingService

    return TrainingService().fetch_training_frame(
        context.entity_id,
        context.metric_name,
        context.lookback_days,
    )


class MLPipelineOrchestrator:
    """Minimal sequencer over existing ML services (injectable for tests)."""

    def __init__(
        self,
        quality: Any | None = None,
        resampler: Any | None = None,
        normalizer: Any | None = None,
        training: Any | None = None,
        inference: Any | None = None,
        anomaly: Any | None = None,
        registry: Any | None = None,
        fetcher: DataFetcher | None = None,
    ) -> None:
        self._quality = quality
        self._resampler = resampler
        self._normalizer = normalizer
        self._training = training
        self._inference = inference
        self._anomaly = anomaly
        self._registry = registry
        self._fetcher = fetcher or _default_fetcher
        self._completed: dict[tuple[str, str, str, str, str, str, str], PipelineResult] = {}

    def _quality_service(self) -> Any:
        if self._quality is None:
            from trendx.preprocessing.quality import DataQualityService

            self._quality = DataQualityService()
        return self._quality

    def _resampler_service(self) -> Any:
        if self._resampler is None:
            from trendx.preprocessing.resampling import Resampler

            self._resampler = Resampler()
        return self._resampler

    def _normalizer_service(self) -> Any:
        if self._normalizer is None:
            from trendx.preprocessing.normalizer import Normalizer

            self._normalizer = Normalizer(method="auto")
        return self._normalizer

    def _training_service(self) -> Any:
        if self._training is None:
            from trendx.services.training import TrainingService

            self._training = TrainingService()
        return self._training

    def _inference_service(self) -> Any:
        if self._inference is None:
            from trendx.services.inference import InferenceService

            self._inference = InferenceService(dry_run=True)
        self._inference.dry_run = True
        return self._inference

    def _anomaly_service(self) -> Any:
        if self._anomaly is None:
            from trendx.anomalies.detectors import AnomalyDetectorService

            self._anomaly = AnomalyDetectorService()
        return self._anomaly

    def _model_registry(self) -> Any:
        if self._registry is None:
            from trendx.mlops.registry import ModelRegistry

            self._registry = ModelRegistry()
        return self._registry

    @staticmethod
    def _fail(
        steps: dict[str, StepResult],
        step: str,
        context: PipelineContext,
        error: str,
    ) -> PipelineResult:
        steps[step] = StepResult(
            step=step,
            status=STATUS_FAILED,
            detail={"entity_id": context.entity_id, "metric_name": context.metric_name},
            error=error,
        )
        for remaining in STEPS[STEPS.index(step) + 1 :]:
            steps[remaining] = StepResult(
                step=remaining, status=STATUS_SKIPPED, detail={}, error=None
            )
        logger.error(
            "Pipeline {}/{} failed at {}: {}",
            context.entity_id[:12],
            context.metric_name,
            step,
            error,
        )
        return PipelineResult(
            context=context,
            execution_id=context.execution_id,
            status=STATUS_FAILED,
            steps=steps,
            error=f"{step}: {error}",
        )

    def run_single(self, context: PipelineContext) -> PipelineResult:
        """Run Quality → Preprocessing → Train → Forecast → Anomaly for one entity/metric."""
        cached = self._completed.get(context.cache_key())
        if cached is not None and cached.status == STATUS_OK:
            logger.info(
                "Pipeline {}/{} deduped (already completed execution {})",
                context.entity_id[:12],
                context.metric_name,
                cached.execution_id,
            )
            return PipelineResult(
                context=cached.context,
                execution_id=cached.execution_id,
                status=cached.status,
                steps=cached.steps,
                model_id=cached.model_id,
                forecast_points=cached.forecast_points,
                forecast_saved=cached.forecast_saved,
                anomaly_episodes=cached.anomaly_episodes,
                error=None,
                deduped=True,
            )

        steps: dict[str, StepResult] = {}
        base = {
            "entity_id": context.entity_id,
            "metric_name": context.metric_name,
            "execution_id": context.execution_id,
        }

        try:
            df = self._fetcher(context)
        except Exception as exc:
            return self._fail(steps, STEP_QUALITY, context, f"fetch failed: {exc}")
        if df.empty:
            return self._fail(steps, STEP_QUALITY, context, "no data for entity/metric")

        try:
            quality = self._quality_service()
            start_ts = pd.Timestamp(df["ts"].min()).to_pydatetime()
            end_ts = pd.Timestamp(df["ts"].max()).to_pydatetime()
            report = quality.generate_report(
                context.entity_id,
                context.metric_name,
                start_ts,
                end_ts,
                df,
                expected_frequency=context.frequency,
            )
            steps[STEP_QUALITY] = StepResult(
                step=STEP_QUALITY,
                status=STATUS_OK,
                detail={
                    **base,
                    "rows": len(df),
                    "completeness_ratio": report.get("completeness_ratio"),
                },
            )
        except Exception as exc:
            return self._fail(steps, STEP_QUALITY, context, f"quality failed: {exc}")

        try:
            resampler = self._resampler_service()
            prepared = resampler.resample(df, frequency=context.frequency, method="mean")
            prepared = resampler.interpolate_missing(prepared, method="linear", limit=3)
            if prepared.empty or len(prepared) < MIN_PREPARED_ROWS:
                return self._fail(
                    steps,
                    STEP_PREPROCESSING,
                    context,
                    f"insufficient data after preprocessing ({len(prepared)} rows)",
                )
            normalizer = self._normalizer_service()
            values = prepared.select_dtypes(include=["number"]).values
            normalizer.fit(values)
            steps[STEP_PREPROCESSING] = StepResult(
                step=STEP_PREPROCESSING,
                status=STATUS_OK,
                detail={**base, "rows_in": len(df), "rows_out": len(prepared)},
            )
        except Exception as exc:
            return self._fail(steps, STEP_PREPROCESSING, context, f"preprocessing failed: {exc}")

        try:
            training = self._training_service()
            model = training.train_model(
                entity_id=context.entity_id,
                metric_key=context.metric_name,
                algorithm=context.algorithm,
                lookback_days=context.lookback_days,
                frequency=context.frequency,
                horizon=context.horizon,
                customer_id=context.customer_id,
            )
            if model is None:
                return self._fail(steps, STEP_TRAIN, context, "training returned no model")
            # Champion promotion is owned by ModelRegistry.register()
            # (promote=True, history recorded) : no explicit promotion here.
            model_id = str(getattr(model, "id", ""))
            steps[STEP_TRAIN] = StepResult(
                step=STEP_TRAIN, status=STATUS_OK, detail={**base, "model_id": model_id}
            )
        except Exception as exc:
            return self._fail(steps, STEP_TRAIN, context, f"training failed: {exc}")

        try:
            inference = self._inference_service()
            forecast = inference.generate_forecast(
                context.entity_id, context.metric_name, horizon=context.horizon
            )
            if forecast is None:
                return self._fail(
                    steps, STEP_FORECAST, context, "no forecast (no champion or no data)"
                )
            saved = inference.save_forecast_results(
                context.entity_id, context.metric_name, forecast
            )
            raw_values = getattr(forecast, "values", None)
            points = len(raw_values) if raw_values is not None else 0
            steps[STEP_FORECAST] = StepResult(
                step=STEP_FORECAST,
                status=STATUS_OK,
                detail={**base, "model_id": model_id, "points": points, "saved": saved},
            )
        except Exception as exc:
            return self._fail(steps, STEP_FORECAST, context, f"forecast failed: {exc}")

        try:
            anomaly = self._anomaly_service()
            episodes = anomaly.scan(context.entity_id, context.metric_name)
            count = len(episodes or [])
            steps[STEP_ANOMALY] = StepResult(
                step=STEP_ANOMALY, status=STATUS_OK, detail={**base, "episodes": count}
            )
        except Exception as exc:
            return self._fail(steps, STEP_ANOMALY, context, f"anomaly failed: {exc}")

        result = PipelineResult(
            context=context,
            execution_id=context.execution_id,
            status=STATUS_OK,
            steps=steps,
            model_id=model_id,
            forecast_points=points,
            forecast_saved=saved,
            anomaly_episodes=count,
        )
        self._completed[context.cache_key()] = result
        logger.info(
            "Pipeline {}/{} ok (model={}, points={}, episodes={})",
            context.entity_id[:12],
            context.metric_name,
            model_id[:12],
            points,
            count,
        )
        return result

    def run_many(self, contexts: list[PipelineContext]) -> list[PipelineResult]:
        """Run the pipeline for several entity/metric pairs with per-item isolation."""
        results: list[PipelineResult] = []
        for context in contexts:
            try:
                results.append(self.run_single(context))
            except Exception as exc:
                logger.error(
                    "Pipeline {}/{} crashed: {}",
                    context.entity_id[:12],
                    context.metric_name,
                    exc,
                )
                steps: dict[str, StepResult] = {
                    name: StepResult(step=name, status=STATUS_SKIPPED, detail={}) for name in STEPS
                }
                results.append(
                    PipelineResult(
                        context=context,
                        execution_id=context.execution_id,
                        status=STATUS_FAILED,
                        steps=steps,
                        error=f"unexpected: {exc}",
                    )
                )
        return results
