"""Generic model registry contract (W87): model identity + compatibility.

Application-level contract (zero DB migration): the persisted
``prediction_model`` row stays untouched. ``RegisteredModel`` carries the
full identity (entity, target, schema, algorithm, frequency, horizon,
window, version, status, role, URI); ``check_compatibility`` gates
``predict``/reuse on fingerprint equality. ``from_legacy`` adapts existing
rows with explicit UNKNOWN/LEGACY markers instead of invented values.
"""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from enum import Enum
from typing import Any

from trendx.forecasting.contract import Algorithm
from trendx.forecasting.dataset import ForecastDataset

UNKNOWN = "UNKNOWN"
LEGACY = "LEGACY"


class ModelStatus(str, Enum):
    TRAINING = "training"
    READY = "ready"
    FAILED = "failed"
    ARCHIVED = "archived"


class ModelRole(str, Enum):
    CHAMPION = "champion"
    CHALLENGER = "challenger"


class IncompatibilityReason(str, Enum):
    COMPATIBLE = "compatible"
    TARGET_MISMATCH = "target_mismatch"
    ENTITY_MISMATCH = "entity_mismatch"
    FEATURE_SCHEMA_MISMATCH = "feature_schema_mismatch"
    FREQUENCY_MISMATCH = "frequency_mismatch"
    HORIZON_MISMATCH = "horizon_mismatch"
    MODEL_NOT_READY = "model_not_ready"
    MODEL_URI_MISSING = "model_uri_missing"
    ALGORITHM_UNRESOLVED = "algorithm_unresolved"


@dataclass(frozen=True)
class RegisteredModel:
    """Full model identity (concrete algorithm; AUTO rejected, UNKNOWN marks legacy)."""

    model_id: str
    tenant_id: str = ""
    entity_type: str = ""
    entity_id: str = ""
    target_metric: str = ""
    algorithm: Algorithm = Algorithm.PROPHET
    feature_schema_version: str = "v1"
    feature_schema_fingerprint: str = ""
    frequency: str = "1h"
    horizon: int = 24
    training_window: str = ""
    model_version: str = "1"
    status: ModelStatus = ModelStatus.READY
    role: ModelRole = ModelRole.CHALLENGER
    model_uri: str = ""
    created_at: str = ""
    updated_at: str = ""

    def __post_init__(self) -> None:
        if not self.model_id:
            msg = "RegisteredModel.model_id is required"
            raise ValueError(msg)
        coerced = Algorithm.coerce(self.algorithm)
        if coerced is Algorithm.AUTO:
            msg = "AUTO is a selection request, not a concrete registered algorithm"
            raise ValueError(msg)
        object.__setattr__(self, "algorithm", coerced)

    def compatibility_key(self) -> tuple[str, str, str, str, str, str]:
        return (
            self.tenant_id,
            self.entity_id,
            self.target_metric,
            self.feature_schema_fingerprint,
            self.frequency,
            str(self.horizon),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "model_id": self.model_id,
            "tenant_id": self.tenant_id,
            "entity_type": self.entity_type,
            "entity_id": self.entity_id,
            "target_metric": self.target_metric,
            "algorithm": self.algorithm.value,
            "feature_schema_version": self.feature_schema_version,
            "feature_schema_fingerprint": self.feature_schema_fingerprint,
            "frequency": self.frequency,
            "horizon": self.horizon,
            "training_window": self.training_window,
            "model_version": self.model_version,
            "status": self.status.value,
            "role": self.role.value,
            "model_uri": self.model_uri,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }

    @classmethod
    def from_legacy(cls, row: Any) -> RegisteredModel:
        """Adapt a persisted ``prediction_model`` row (missing fields marked)."""

        def _get(name: str, default: Any = "") -> Any:
            value = getattr(row, name, default)
            return value if value is not None else default

        raw_status = str(_get("status", ""))
        if raw_status == "champion":
            status, role = ModelStatus.READY, ModelRole.CHAMPION
        elif raw_status == "challenger":
            status, role = ModelStatus.READY, ModelRole.CHALLENGER
        elif raw_status == "failed":
            status, role = ModelStatus.FAILED, ModelRole.CHALLENGER
        elif raw_status == "archived":
            status, role = ModelStatus.ARCHIVED, ModelRole.CHALLENGER
        elif raw_status == "training":
            status, role = ModelStatus.TRAINING, ModelRole.CHALLENGER
        else:
            msg = f"Unknown legacy model status: {raw_status!r}"
            raise ValueError(msg)
        hyper = _get("hyperparameters", {}) or {}
        raw_algorithm = _get("algorithm", "") or (
            hyper.get("algorithm", "") if isinstance(hyper, dict) else ""
        )
        coerced = Algorithm.coerce(raw_algorithm) if raw_algorithm else Algorithm.UNKNOWN
        legacy_schema = (
            (hyper.get("feature_schema_fingerprint", "") or "") if isinstance(hyper, dict) else ""
        )
        return cls(
            model_id=str(_get("id", "")),
            tenant_id=str(_get("tenant_id", "")),
            entity_id=str(_get("business_entity_id", "")),
            target_metric=str(_get("tb_telemetry_key", "")),
            algorithm=coerced,
            feature_schema_version=LEGACY,
            feature_schema_fingerprint=legacy_schema,
            frequency=str(_get("frequency", "") or UNKNOWN),
            horizon=int(hyper.get("horizon", 0) or 0) if isinstance(hyper, dict) else 0,
            training_window=str(hyper.get("training_window", "") or UNKNOWN)
            if isinstance(hyper, dict)
            else UNKNOWN,
            model_version=str(hyper.get("model_version", "1") or "1")
            if isinstance(hyper, dict)
            else "1",
            status=status,
            role=role,
            model_uri=str(_get("model_uri", "") or ""),
        )


@dataclass(frozen=True)
class Compatibility:
    ok: bool
    reason: IncompatibilityReason
    detail: str = ""


def check_compatibility(
    model: RegisteredModel,
    dataset: ForecastDataset,
    *,
    require_uri: bool = True,
) -> Compatibility:
    """Gate predict/reuse: every dimension must match explicitly.

    ``require_uri`` (default ``True``) controls whether an empty
    ``model_uri`` is treated as incompatible.  Set to ``False`` when
    checking compatibility *before* training (the URI is assigned by the
    trainer only after a successful fit + save).
    """
    meta = dataset.metadata
    if model.target_metric != dataset.target.name:
        return Compatibility(
            ok=False,
            reason=IncompatibilityReason.TARGET_MISMATCH,
            detail=f"{model.target_metric!r} != {dataset.target.name!r}",
        )
    if model.entity_id and model.entity_id != meta.get("entity_id", ""):
        return Compatibility(
            ok=False,
            reason=IncompatibilityReason.ENTITY_MISMATCH,
            detail=f"{model.entity_id!r} != {meta.get('entity_id')!r}",
        )
    if model.tenant_id and model.tenant_id != meta.get("tenant_id", ""):
        return Compatibility(
            ok=False, reason=IncompatibilityReason.ENTITY_MISMATCH, detail="tenant mismatch"
        )
    if model.feature_schema_fingerprint != meta.get("feature_schema_fingerprint", ""):
        return Compatibility(
            ok=False,
            reason=IncompatibilityReason.FEATURE_SCHEMA_MISMATCH,
            detail="fingerprint mismatch",
        )
    if model.frequency != meta.get("frequency", ""):
        return Compatibility(
            ok=False,
            reason=IncompatibilityReason.FREQUENCY_MISMATCH,
            detail=f"{model.frequency!r} != {meta.get('frequency')!r}",
        )
    if int(model.horizon) != int(meta.get("horizon", -1)):
        return Compatibility(
            ok=False,
            reason=IncompatibilityReason.HORIZON_MISMATCH,
            detail=f"{model.horizon!r} != {meta.get('horizon')!r}",
        )
    if model.status is not ModelStatus.READY:
        return Compatibility(
            ok=False,
            reason=IncompatibilityReason.MODEL_NOT_READY,
            detail=f"status={model.status.value}",
        )
    if model.algorithm is Algorithm.UNKNOWN:
        return Compatibility(
            ok=False,
            reason=IncompatibilityReason.ALGORITHM_UNRESOLVED,
            detail="legacy algorithm unknown",
        )
    if require_uri and not (model.model_uri or "").strip():
        return Compatibility(
            ok=False, reason=IncompatibilityReason.MODEL_URI_MISSING, detail="empty model_uri"
        )
    return Compatibility(ok=True, reason=IncompatibilityReason.COMPATIBLE, detail="exact match")


@dataclass
class ChampionRegistry:
    """In-memory champion/challenger bookkeeping (explicit promotion only)."""

    _models: dict[str, RegisteredModel] = field(default_factory=dict)

    def register(self, model: RegisteredModel) -> None:
        if model.model_id in self._models:
            msg = f"Duplicate model_id: {model.model_id!r}"
            raise ValueError(msg)
        self._models[model.model_id] = model

    def get(self, model_id: str) -> RegisteredModel | None:
        """Look up a model by its model_id (returns None if absent)."""
        return self._models.get(model_id)

    def promote_to_champion(self, model_id: str) -> RegisteredModel:
        current = self._models.get(model_id)
        if current is None:
            msg = f"Unknown model_id: {model_id!r}"
            raise KeyError(msg)
        promoted = replace(current, role=ModelRole.CHAMPION)
        for mid, other in self._models.items():
            if (
                mid != model_id
                and other.role is ModelRole.CHAMPION
                and other.compatibility_key() == promoted.compatibility_key()
            ):
                self._models[mid] = replace(other, role=ModelRole.CHALLENGER)
        self._models[model_id] = promoted
        return promoted

    def promote(
        self,
        model_id: str,
        *,
        artifact_store: Any = None,
    ) -> RegisteredModel:
        """Explicit, validated promotion to champion.

        Unlike ``promote_to_champion`` (the raw W87 primitive), this adds
        fail-closed guards required by W94:

        * model must exist in the registry
        * status must be ``READY``
        * ``model_uri`` must be non-empty
        * ``algorithm`` must be concrete (not ``UNKNOWN``)
        * if an ``artifact_store`` is supplied, the artifact must exist
          and pass integrity check

        Training, registration, and evaluation never call this — only an
        explicit, audited promotion step does.

        Idempotent: calling ``promote`` on the current champion returns
        the same champion unchanged.
        """
        current = self._models.get(model_id)
        if current is None:
            msg = f"Cannot promote unknown model_id: {model_id!r}"
            raise KeyError(msg)

        if current.status is not ModelStatus.READY:
            msg = (
                f"Cannot promote model {model_id!r}: status is "
                f"{current.status.value!r}, not READY"
            )
            raise ValueError(msg)

        if current.algorithm is Algorithm.UNKNOWN:
            msg = (
                f"Cannot promote model {model_id!r}: algorithm is UNKNOWN " "(legacy/incompatible)"
            )
            raise ValueError(msg)

        if not (current.model_uri or "").strip():
            msg = f"Cannot promote model {model_id!r}: empty model_uri (artifact absent)"
            raise ValueError(msg)

        if artifact_store is not None:
            from trendx.forecasting.artifact import (
                ArtifactError,
                ArtifactIntegrityError,
            )

            if not artifact_store.exists(current.model_uri):
                msg = f"Cannot promote model {model_id!r}: artifact not found"
                raise ArtifactError(msg)
            if not artifact_store.integrity_check(current.model_uri):
                msg = f"Cannot promote model {model_id!r}: artifact integrity check failed"
                raise ArtifactIntegrityError(msg)

        # Already champion → idempotent no-op.
        if current.role is ModelRole.CHAMPION:
            return current

        return self.promote_to_champion(model_id)

    def champions(self) -> list[RegisteredModel]:
        return [m for m in self._models.values() if m.role is ModelRole.CHAMPION]
