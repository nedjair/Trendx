"""Generic multi-metric forecast pipeline (W88 + W96).

Wires the W84-W87 contracts end to end without metric branching:

    ForecastRequest -> FeatureResolver -> DatasetBuilder
        -> ModelRegistry lookup -> ForecastModel -> ForecastEnvelope

When a W87 ``ChampionRegistry`` and a W94 ``ModelArtifactStore`` are injected,
the pipeline takes the W96 persisted path instead of accepting an in-memory
engine:

    ChampionRegistry -> artifact URI -> W95 reload -> predict -> envelope

The injected-model path remains supported for existing W88 callers. There is
no training, registration, promotion, external-provider, or metric-specific
logic in this module.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

import pandas as pd
from trendx.forecasting.artifact import (
    ArtifactError,
    ArtifactIntegrityError,
    ArtifactNotFoundError,
    ModelArtifactStore,
)
from trendx.forecasting.contract import Algorithm, ForecastRequest
from trendx.forecasting.dataset import DatasetBuilder, ForecastDataset
from trendx.forecasting.execution import (
    ExecutionProvenance,
    ExecutionRecord,
    ExecutionStatus,
    ExecutionStore,
    ExecutionStoreError,
)
from trendx.forecasting.registry import (
    ChampionRegistry,
    Compatibility,
    RegisteredModel,
    check_compatibility,
)
from trendx.forecasting.resolution import FeatureResolver, ResolvedFeatureSet
from trendx.forecasting.serving import ServingError, serve_registered_model

ModelLoader = Callable[[RegisteredModel], Any]
ChampionSelector = Callable[[ForecastRequest], RegisteredModel | None]


@dataclass(frozen=True)
class PipelineOutcome:
    status: str
    reason: str = ""
    compatibility: Compatibility | None = None
    resolved: ResolvedFeatureSet | None = None
    dataset_meta: dict[str, Any] = field(default_factory=dict)
    envelope: Any = field(default=None)
    execution_record: ExecutionRecord | None = None
    served_model: RegisteredModel | None = None
    duplicate: bool = False


class ForecastPipeline:
    """Metric-generic forecast wiring (request in, envelope out)."""

    def __init__(
        self,
        resolver: FeatureResolver | None = None,
        builder: DatasetBuilder | None = None,
        models: Iterable[RegisteredModel] = (),
        model_loader: ModelLoader | None = None,
        champion_selector: ChampionSelector | None = None,
        artifact_store: ModelArtifactStore | None = None,
        champion_registry: ChampionRegistry | None = None,
        execution_store: ExecutionStore | None = None,
    ) -> None:
        self._resolver = resolver or FeatureResolver()
        self._builder = builder or DatasetBuilder()
        self._models = list(models)
        self._loader = model_loader
        self._selector = champion_selector
        self._artifact_store = artifact_store
        self._champion_registry = champion_registry
        self._execution_store = execution_store

    def run(
        self,
        request: ForecastRequest,
        frames: Mapping[tuple[str, str], pd.DataFrame] | None = None,
        execution_id: str = "",
        reference_key: str = "",
    ) -> PipelineOutcome:
        """Run one forecast and, when configured, persist its execution record."""
        if self._execution_store is None:
            return self._run_forecast(request, frames, execution_id)

        try:
            record, early = self._begin_execution(request, execution_id, reference_key)
        except ExecutionStoreError:
            return PipelineOutcome(status="refused", reason="execution-store-error")
        if early is not None:
            return early
        if record is None:  # pragma: no cover - defensive store contract guard
            return PipelineOutcome(status="refused", reason="execution-store-error")

        try:
            outcome = self._run_forecast(request, frames, record.execution_id)
        except Exception as exc:
            try:
                failed = self._mark_execution_failed(
                    record,
                    request,
                    reason="pipeline-error",
                    error_reason=str(exc)[:500],
                )
            except ExecutionStoreError:
                return PipelineOutcome(
                    status="refused",
                    reason="execution-store-error",
                    execution_record=record,
                )
            return PipelineOutcome(
                status="refused",
                reason="pipeline-error",
                execution_record=failed,
            )

        try:
            if outcome.status == "ok" and outcome.envelope is not None:
                record = self._mark_execution_success(record, request, outcome)
            else:
                record = self._mark_execution_failed(
                    record,
                    request,
                    reason=outcome.reason or "pipeline-refused",
                    error_reason=outcome.reason or "forecast pipeline refused",
                    outcome=outcome,
                )
        except ExecutionStoreError:
            return PipelineOutcome(
                status="refused",
                reason="execution-store-error",
                execution_record=record,
                compatibility=outcome.compatibility,
                resolved=outcome.resolved,
                dataset_meta=outcome.dataset_meta,
            )
        outcome = replace(outcome, execution_record=record)
        return outcome

    def _run_forecast(
        self,
        request: ForecastRequest,
        frames: Mapping[tuple[str, str], pd.DataFrame] | None = None,
        execution_id: str = "",
    ) -> PipelineOutcome:
        from trendx.forecasting.contract import ForecastEnvelope

        resolved = self._resolver.resolve(request)
        if not resolved.ok:
            return PipelineOutcome(
                status="refused", reason="unresolved-features", resolved=resolved
            )
        store = {k: v.copy() for k, v in (frames or {}).items()}
        dataset = self._builder.build(
            request,
            resolved,
            execution_id=execution_id,
            frames=store,
        )
        if self._champion_registry is not None:
            return self._run_persisted_champion(
                request,
                dataset,
                resolved,
                execution_id=execution_id,
            )

        model = self._select_model(request, dataset)
        if model is None:
            return PipelineOutcome(
                status="refused",
                reason="no-compatible-model",
                resolved=resolved,
                dataset_meta=dataset.metadata,
            )
        compatibility = check_compatibility(model, dataset)
        if not compatibility.ok:
            return PipelineOutcome(
                status="refused",
                reason=compatibility.reason.value,
                compatibility=compatibility,
                resolved=resolved,
                dataset_meta=dataset.metadata,
            )
        if self._loader is None:
            return PipelineOutcome(
                status="refused",
                reason="no-model-loader",
                resolved=resolved,
                dataset_meta=dataset.metadata,
            )
        engine = self._loader(model)
        forecast = engine.predict(request.horizon)
        envelope = ForecastEnvelope(
            execution_id=execution_id or dataset.metadata.get("execution_id", ""),
            tenant_id=request.tenant_id,
            entity_id=request.entity_id,
            target_metric=request.target_metric,
            model_id=model.model_id,
            model_version=model.model_version,
            feature_schema_version=model.feature_schema_version,
            result=forecast,
        )
        return PipelineOutcome(
            status="ok",
            reason="forecast",
            compatibility=compatibility,
            resolved=resolved,
            dataset_meta=dataset.metadata,
            envelope=envelope,
            served_model=model,
        )

    def _begin_execution(
        self,
        request: ForecastRequest,
        execution_id: str,
        reference_key: str,
    ) -> tuple[ExecutionRecord | None, PipelineOutcome | None]:
        store = self._execution_store
        if store is None:
            return None, None

        resolved_execution_id = str(execution_id or uuid4().hex)
        resolved_reference_key = str(reference_key or resolved_execution_id)
        prior_records = store.list(reference_key=resolved_reference_key)
        successful = next(
            (record for record in prior_records if record.status is ExecutionStatus.SUCCESS),
            None,
        )
        if successful is not None:
            return None, PipelineOutcome(
                status="ok",
                reason="duplicate",
                execution_record=successful,
                duplicate=True,
            )
        active = next(
            (
                record
                for record in prior_records
                if record.status in (ExecutionStatus.PENDING, ExecutionStatus.RUNNING)
            ),
            None,
        )
        if active is not None:
            return None, PipelineOutcome(
                status="refused",
                reason="execution-in-progress",
                execution_record=active,
            )

        schema = request.feature_schema()
        record = ExecutionRecord(
            execution_id=resolved_execution_id,
            reference_key=resolved_reference_key,
            tenant_id=request.tenant_id,
            entity_type=request.entity_type,
            entity_id=request.entity_id,
            target_metric=request.target_metric,
            frequency=request.frequency,
            horizon=request.horizon,
            algorithm=request.algorithm.value,
            feature_schema_version=schema.schema_version,
            feature_schema_fingerprint=schema.fingerprint(),
            created_at=datetime.now(UTC).isoformat(),
            metadata={
                "source": "forecast-pipeline",
                "feature_columns": list(schema.names()),
            },
        )
        store.create(record)
        return store.mark_started(resolved_execution_id), None

    def _execution_provenance(
        self,
        request: ForecastRequest,
        outcome: PipelineOutcome | None = None,
    ) -> ExecutionProvenance:
        model = outcome.served_model if outcome is not None else None
        if model is not None:
            model_id = model.model_id
            model_version = model.model_version
            algorithm = model.algorithm.value
            schema_version = model.feature_schema_version
            fingerprint = model.feature_schema_fingerprint
            artifact_uri = model.model_uri
        else:
            schema = request.feature_schema()
            model_id = ""
            model_version = ""
            algorithm = request.algorithm.value
            schema_version = schema.schema_version
            fingerprint = schema.fingerprint()
            artifact_uri = ""

        result_payload = None
        prediction_count = None
        metadata = {
            "feature_columns": list(
                outcome.dataset_meta.get("feature_columns", request.feature_schema().names())
                if outcome is not None
                else request.feature_schema().names()
            )
        }
        if outcome is not None and outcome.envelope is not None:
            result_payload = outcome.envelope.to_dict()
            values = result_payload.get("result", {}).get("values", [])
            prediction_count = len(values) if isinstance(values, list) else None
        return ExecutionProvenance(
            model_id=model_id,
            model_version=model_version,
            algorithm=algorithm,
            feature_schema_version=schema_version,
            feature_schema_fingerprint=fingerprint,
            artifact_uri=artifact_uri,
            prediction_count=prediction_count,
            metadata=metadata,
            result=result_payload,
        )

    def _mark_execution_success(
        self,
        record: ExecutionRecord,
        request: ForecastRequest,
        outcome: PipelineOutcome,
    ) -> ExecutionRecord:
        store = self._execution_store
        if store is None:  # pragma: no cover - guarded by the caller
            raise ExecutionStoreError("execution store is not configured")
        return store.mark_success(
            record.execution_id,
            self._execution_provenance(request, outcome),
        )

    def _mark_execution_failed(
        self,
        record: ExecutionRecord,
        request: ForecastRequest,
        *,
        reason: str,
        error_reason: str,
        outcome: PipelineOutcome | None = None,
    ) -> ExecutionRecord:
        store = self._execution_store
        if store is None:  # pragma: no cover - guarded by the caller
            raise ExecutionStoreError("execution store is not configured")
        provenance = self._execution_provenance(request, outcome)
        provenance = replace(
            provenance,
            metadata={**provenance.metadata, "failure_stage": reason},
        )
        return store.mark_failed(
            record.execution_id,
            error_code=reason or "pipeline-refused",
            error_reason=error_reason or reason or "forecast pipeline refused",
            provenance=provenance,
        )

    def _run_persisted_champion(
        self,
        request: ForecastRequest,
        dataset: ForecastDataset,
        resolved: ResolvedFeatureSet,
        *,
        execution_id: str,
    ) -> PipelineOutcome:
        """Serve a registry champion exclusively through the W95 artifact path."""

        registry = self._champion_registry
        if registry is None:  # pragma: no cover - guarded by the caller
            return PipelineOutcome(
                status="refused",
                reason="no-champion-registry",
                resolved=resolved,
                dataset_meta=dataset.metadata,
            )

        model, compatibility, selection_reason = self._select_persisted_champion(request, dataset)
        if model is None:
            return PipelineOutcome(
                status="refused",
                reason=selection_reason,
                compatibility=compatibility,
                resolved=resolved,
                dataset_meta=dataset.metadata,
            )

        if self._artifact_store is None:
            return PipelineOutcome(
                status="refused",
                reason="no-artifact-store",
                compatibility=compatibility,
                resolved=resolved,
                dataset_meta=dataset.metadata,
                served_model=model,
            )

        try:
            envelope = serve_registered_model(
                model.model_id,
                registry.get,
                self._artifact_store,
                dataset,
                horizon=request.horizon,
                execution_id=execution_id,
            )
        except ArtifactNotFoundError:
            return PipelineOutcome(
                status="refused",
                reason="artifact-missing",
                compatibility=compatibility,
                resolved=resolved,
                dataset_meta=dataset.metadata,
                served_model=model,
            )
        except ArtifactIntegrityError:
            return PipelineOutcome(
                status="refused",
                reason="artifact-corrupted",
                compatibility=compatibility,
                resolved=resolved,
                dataset_meta=dataset.metadata,
                served_model=model,
            )
        except ServingError:
            return PipelineOutcome(
                status="refused",
                reason="serving-refused",
                compatibility=compatibility,
                resolved=resolved,
                dataset_meta=dataset.metadata,
                served_model=model,
            )
        except ArtifactError:
            return PipelineOutcome(
                status="refused",
                reason="artifact-error",
                compatibility=compatibility,
                resolved=resolved,
                dataset_meta=dataset.metadata,
                served_model=model,
            )

        return PipelineOutcome(
            status="ok",
            reason="forecast",
            compatibility=compatibility,
            resolved=resolved,
            dataset_meta=dataset.metadata,
            envelope=envelope,
            served_model=model,
        )

    def _select_persisted_champion(
        self, request: ForecastRequest, dataset: ForecastDataset
    ) -> tuple[RegisteredModel | None, Compatibility | None, str]:
        """Select exactly one W87-compatible champion, never a challenger."""

        registry = self._champion_registry
        if registry is None:  # pragma: no cover - guarded by the caller
            return None, None, "no-champion-registry"

        candidates = [
            model
            for model in registry.champions()
            if request.algorithm is Algorithm.AUTO or model.algorithm is request.algorithm
        ]
        if not candidates:
            return None, None, "no-compatible-champion"

        evaluated = [(model, check_compatibility(model, dataset)) for model in candidates]
        compatible = [(model, compat) for model, compat in evaluated if compat.ok]
        if len(compatible) == 1:
            model, compatibility = compatible[0]
            return model, compatibility, ""
        if len(compatible) > 1:
            return None, None, "ambiguous-champion"
        if len(candidates) == 1:
            compatibility = evaluated[0][1]
            return None, compatibility, compatibility.reason.value
        return None, None, "no-compatible-champion"

    def _select_model(
        self, request: ForecastRequest, dataset: ForecastDataset
    ) -> RegisteredModel | None:
        if request.algorithm is Algorithm.AUTO:
            if self._selector is None:
                return None
            return self._selector(request)
        wanted = request.algorithm.value
        for model in self._models:
            if (
                model.algorithm.value == wanted
                and model.entity_id in ("", request.entity_id)
                and model.target_metric in ("", request.target_metric)
            ):
                return model
        return None


__all__ = [
    "ChampionRegistry",
    "ForecastPipeline",
    "PipelineOutcome",
]
