"""Generic model training + registration (W93): selection -> fit -> register.

    SelectionResult (W92) + ForecastDataset (W86)
        -> ModelTrainer -> RegisteredModel (W87, CHALLENGER, no auto-promote)

No metric branching, no orchestrator involvement, no external tracking,
no persistence writes, no external writes. Model instantiation goes through an
injected factory (default: existing ``create_model``); artifact URIs come
from an injected store (default: empty, honestly reported).
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

import pandas as pd
from trendx.forecasting.contract import Algorithm, ForecastRequest
from trendx.forecasting.dataset import ForecastDataset
from trendx.forecasting.evaluation import NO_VALID_CANDIDATE, SelectionResult
from trendx.forecasting.registry import (
    ChampionRegistry,
    ModelRole,
    ModelStatus,
    RegisteredModel,
    check_compatibility,
)

ModelFactory = Callable[[Algorithm], Any]
ArtifactStore = Callable[[str, Any], str]


@dataclass(frozen=True)
class TrainingRequest:
    tenant_id: str
    entity_type: str
    entity_id: str
    target_metric: str
    frequency: str = "1h"
    horizon: int = 24
    training_window: str = ""
    algorithm: Algorithm = Algorithm.PROPHET
    model_version_policy: str = "auto"
    execution_id: str = ""

    def __post_init__(self) -> None:
        if not self.target_metric or not str(self.target_metric).strip():
            msg = "TrainingRequest.target_metric is required"
            raise ValueError(msg)
        if not self.entity_id or not str(self.entity_id).strip():
            msg = "TrainingRequest.entity_id is required"
            raise ValueError(msg)
        object.__setattr__(self, "algorithm", Algorithm.coerce(self.algorithm))

    @classmethod
    def from_forecast_request(
        cls, request: ForecastRequest, training_window: str = ""
    ) -> TrainingRequest:
        return cls(
            tenant_id=request.tenant_id,
            entity_type=request.entity_type,
            entity_id=request.entity_id,
            target_metric=request.target_metric,
            frequency=request.frequency,
            horizon=request.horizon,
            training_window=training_window,
            algorithm=request.algorithm
            if request.algorithm is not Algorithm.AUTO
            else Algorithm.PROPHET,
            execution_id="",
        )


@dataclass(frozen=True)
class TrainingOutcome:
    status: str
    reason: str = ""
    model: RegisteredModel | None = None
    provenance: dict[str, Any] = field(default_factory=dict)


def _default_factory(algorithm: Algorithm) -> Any:
    from trendx.forecasting import create_model

    return create_model(algorithm.value)


class ModelTrainer:
    """Fit the W92-selected candidate and register it (challenger only)."""

    def __init__(
        self,
        model_factory: ModelFactory | None = None,
        artifact_store: ArtifactStore | None = None,
        registry: ChampionRegistry | None = None,
    ) -> None:
        self._factory = model_factory or _default_factory
        self._store = artifact_store or (lambda model_id, engine: "")
        self._registry = registry or ChampionRegistry()

    def train(
        self,
        selection: SelectionResult,
        dataset: ForecastDataset,
        request: TrainingRequest,
        candidates: Mapping[str, RegisteredModel] | None = None,
        existing: Sequence[RegisteredModel] = (),
    ) -> TrainingOutcome:
        if selection.winner_model_id is None:
            return TrainingOutcome(status="refused", reason=NO_VALID_CANDIDATE)
        lookup = dict(candidates or {})
        model = lookup.get(selection.winner_model_id)
        if model is None:
            return TrainingOutcome(status="refused", reason="unknown-candidate")
        compatibility = check_compatibility(model, dataset)
        if not compatibility.ok:
            return TrainingOutcome(status="refused", reason=compatibility.reason.value)
        if dataset.target is None or len(dataset.target) == 0:
            return TrainingOutcome(status="failed", reason="missing-target")
        cutoff = pd.Timestamp(dataset.metadata.get("cutoff", dataset.ds.max()))
        if dataset.ds.max() != cutoff or (dataset.ds > cutoff).any():
            return TrainingOutcome(status="failed", reason="future-leakage")
        frame = pd.DataFrame({"ds": dataset.ds, "y": dataset.target.values})
        try:
            engine = self._factory(model.algorithm)
            engine.fit(frame)
        except Exception as exc:  # controlled failure path
            return TrainingOutcome(status="failed", reason=f"fit-failure: {exc}"[:200])
        try:
            uri = self._store(model.model_id, engine)
        except Exception as exc:  # controlled failure path
            return TrainingOutcome(status="failed", reason=f"artifact-failure: {exc}"[:200])
        version = self._next_version(model, existing, request)
        registered = RegisteredModel(
            model_id=f"{model.model_id}@{version}",
            tenant_id=model.tenant_id,
            entity_type=model.entity_type,
            entity_id=model.entity_id,
            target_metric=model.target_metric,
            algorithm=model.algorithm,
            feature_schema_version=model.feature_schema_version,
            feature_schema_fingerprint=model.feature_schema_fingerprint,
            frequency=model.frequency,
            horizon=model.horizon,
            training_window=request.training_window or model.training_window,
            model_version=version,
            status=ModelStatus.READY,
            role=ModelRole.CHALLENGER,
            model_uri=uri,
        )
        try:
            self._registry.register(registered)
        except (ValueError, KeyError) as exc:
            return TrainingOutcome(status="failed", reason=f"registry-failure: {exc}"[:200])
        return TrainingOutcome(
            status="ok",
            reason="trained",
            model=registered,
            provenance={
                "execution_id": request.execution_id,
                "selection_criterion": selection.criterion,
                "selection_detail": selection.detail,
                "cutoff": str(dataset.ds.max()),
                "rows": int(len(frame)),
                "schema_fingerprint": model.feature_schema_fingerprint,
            },
        )

    @staticmethod
    def _next_version(
        model: RegisteredModel,
        existing: Sequence[RegisteredModel],
        request: TrainingRequest,
    ) -> str:
        if request.model_version_policy not in ("auto", "explicit"):
            msg = f"Unknown version policy: {request.model_version_policy!r}"
            raise ValueError(msg)
        if request.model_version_policy == "explicit":
            return model.model_version
        same = sum(1 for m in existing if m.compatibility_key() == model.compatibility_key())
        return f"{model.model_version}.{same + 1}"
