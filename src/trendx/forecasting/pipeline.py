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
from dataclasses import dataclass, field
from typing import Any

import pandas as pd
from trendx.forecasting.artifact import (
    ArtifactError,
    ArtifactIntegrityError,
    ArtifactNotFoundError,
    ModelArtifactStore,
)
from trendx.forecasting.contract import Algorithm, ForecastRequest
from trendx.forecasting.dataset import DatasetBuilder, ForecastDataset
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
    ) -> None:
        self._resolver = resolver or FeatureResolver()
        self._builder = builder or DatasetBuilder()
        self._models = list(models)
        self._loader = model_loader
        self._selector = champion_selector
        self._artifact_store = artifact_store
        self._champion_registry = champion_registry

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
            )
        except ArtifactIntegrityError:
            return PipelineOutcome(
                status="refused",
                reason="artifact-corrupted",
                compatibility=compatibility,
                resolved=resolved,
                dataset_meta=dataset.metadata,
            )
        except ServingError:
            return PipelineOutcome(
                status="refused",
                reason="serving-refused",
                compatibility=compatibility,
                resolved=resolved,
                dataset_meta=dataset.metadata,
            )
        except ArtifactError:
            return PipelineOutcome(
                status="refused",
                reason="artifact-error",
                compatibility=compatibility,
                resolved=resolved,
                dataset_meta=dataset.metadata,
            )

        return PipelineOutcome(
            status="ok",
            reason="forecast",
            compatibility=compatibility,
            resolved=resolved,
            dataset_meta=dataset.metadata,
            envelope=envelope,
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
