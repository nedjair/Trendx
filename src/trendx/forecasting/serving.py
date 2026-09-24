"""W95: serve a persisted, registered champion artifact.

The serving path is deliberately composed from existing contracts:

    ChampionRegistry lookup
        -> W94 load_registered_model
        -> W94 ArtifactMetadata identity checks
        -> W87 check_compatibility
        -> ForecastModel.predict
        -> ForecastEnvelope

No registry, metadata store, compatibility implementation, scheduler, worker,
or external service is introduced here.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

from trendx.forecasting.artifact import (
    ArtifactError,
    ArtifactMetadata,
    ModelArtifactStore,
    load_registered_model,
)
from trendx.forecasting.contract import ForecastEnvelope
from trendx.forecasting.dataset import ForecastDataset
from trendx.forecasting.registry import RegisteredModel, check_compatibility


class ServingError(ArtifactError):
    """A persisted artifact cannot be served safely."""


def _validate_metadata_identity(
    model: RegisteredModel,
    metadata: ArtifactMetadata,
) -> None:
    """Reject an artifact whose W94 metadata does not identify ``model``."""
    expected: tuple[tuple[str, Any, Any], ...] = (
        ("model_id", metadata.model_id, model.model_id),
        ("model_version", metadata.model_version, model.model_version),
        ("algorithm", metadata.algorithm, model.algorithm.value),
        (
            "feature_schema_version",
            metadata.feature_schema_version,
            model.feature_schema_version,
        ),
        (
            "feature_schema_fingerprint",
            metadata.feature_schema_fingerprint,
            model.feature_schema_fingerprint,
        ),
        ("frequency", metadata.frequency, model.frequency),
        ("horizon", metadata.horizon, model.horizon),
        ("target_metric", metadata.target_metric, model.target_metric),
        ("entity_type", metadata.entity_type, model.entity_type),
        ("entity_id", metadata.entity_id, model.entity_id),
        ("tenant_id", metadata.tenant_id, model.tenant_id),
    )
    for field, actual, wanted in expected:
        if actual != wanted:
            msg = (
                f"Artifact metadata mismatch for {model.model_id!r}: "
                f"{field}={actual!r}, expected {wanted!r}"
            )
            raise ServingError(msg)
    if not metadata.checksum:
        msg = f"Artifact metadata has no checksum for {model.model_id!r}"
        raise ServingError(msg)


def _resolve_horizon(dataset: ForecastDataset, horizon: int | None) -> int:
    raw = dataset.metadata.get("horizon") if horizon is None else horizon
    if raw is None:
        msg = "Serving horizon is missing"
        raise ServingError(msg)
    try:
        resolved = int(raw)
    except (TypeError, ValueError) as exc:
        msg = f"Invalid serving horizon: {raw!r}"
        raise ServingError(msg) from exc
    if resolved <= 0:
        msg = f"Serving horizon must be > 0, got {resolved}"
        raise ServingError(msg)
    return resolved


def serve_registered_model(
    model_id: str,
    model_lookup: Callable[[str], RegisteredModel | None],
    artifact_store: ModelArtifactStore,
    dataset: ForecastDataset,
    *,
    horizon: int | None = None,
    execution_id: str = "",
    created_at: str = "",
) -> ForecastEnvelope:
    """Reload, validate, predict, and return a provenance envelope.

    The training-time model object is not accepted by this API.  The model is
    reconstructed exclusively through the W94 artifact store after a registry
    lookup.  All compatibility decisions are delegated to W87
    ``check_compatibility``.
    """
    model = model_lookup(model_id)
    if model is None:
        msg = f"Model not found in registry: {model_id!r}"
        raise ServingError(msg)

    def same_model(_requested_id: str) -> RegisteredModel | None:
        return model

    try:
        loaded_model = load_registered_model(model_id, same_model, artifact_store)
    except ArtifactError:
        raise
    except Exception as exc:
        msg = f"Artifact reload failed for {model_id!r}: {exc}"
        raise ServingError(msg) from exc

    try:
        metadata = artifact_store.metadata(model.model_uri)
    except ArtifactError:
        raise
    except Exception as exc:
        msg = f"Artifact metadata unreadable for {model_id!r}: {exc}"
        raise ServingError(msg) from exc
    _validate_metadata_identity(model, metadata)

    compatibility = check_compatibility(model, dataset)
    if not compatibility.ok:
        msg = (
            f"Cannot serve model {model_id!r}: "
            f"{compatibility.reason.value} ({compatibility.detail})"
        )
        raise ServingError(msg)

    resolved_horizon = _resolve_horizon(dataset, horizon)
    try:
        result = loaded_model.predict(resolved_horizon)
    except Exception as exc:
        msg = f"Prediction failed after reload for {model_id!r}: {exc}"
        raise ServingError(msg) from exc

    return ForecastEnvelope(
        execution_id=execution_id or str(dataset.metadata.get("execution_id", "")),
        tenant_id=model.tenant_id,
        entity_id=model.entity_id,
        target_metric=model.target_metric,
        model_id=model.model_id,
        model_version=model.model_version,
        feature_schema_version=model.feature_schema_version,
        created_at=created_at or datetime.now(UTC).isoformat(),
        result=result,
    )


__all__ = ["ServingError", "serve_registered_model"]
