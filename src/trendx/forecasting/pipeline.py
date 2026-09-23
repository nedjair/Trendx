"""Generic multi-metric forecast pipeline (W88).

Wires the W84-W87 contracts end to end without metric branching:

    ForecastRequest -> FeatureResolver -> DatasetBuilder
        -> ModelRegistry lookup -> ForecastModel -> ForecastEnvelope

No training, no MLflow writes, no scheduler/worker changes, no persistence
changes. Model loading and champion selection are injected callables so the
pipeline stays free of infrastructure imports.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field
from typing import Any

import pandas as pd
from trendx.forecasting.contract import Algorithm, ForecastRequest
from trendx.forecasting.dataset import DatasetBuilder, ForecastDataset
from trendx.forecasting.registry import (
    ChampionRegistry,
    Compatibility,
    RegisteredModel,
    check_compatibility,
)
from trendx.forecasting.resolution import FeatureResolver, ResolvedFeatureSet

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


class ForecastPipeline:
    """Metric-generic forecast wiring (request in, envelope out)."""

    def __init__(
        self,
        resolver: FeatureResolver | None = None,
        builder: DatasetBuilder | None = None,
        models: Iterable[RegisteredModel] = (),
        model_loader: ModelLoader | None = None,
        champion_selector: ChampionSelector | None = None,
    ) -> None:
        self._resolver = resolver or FeatureResolver()
        self._builder = builder or DatasetBuilder()
        self._models = list(models)
        self._loader = model_loader
        self._selector = champion_selector

    def run(
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
        )

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
