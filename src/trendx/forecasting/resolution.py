"""Generic feature resolution (W85): contract -> ResolvedFeatureSet.

Pure resolution layer (no DB access, no network, no model training):

    ForecastRequest + FeatureSchema
        ↓ FeatureResolver
    ResolvedFeatureSet

Availability input is an explicit snapshot
``available: set[(entity_id, metric_name)]`` plus the set of configured
external providers — never invented data. Target/feature separation,
entity scope, categories, lag/transformation passthrough, required vs
optional handling and schema-membership checks are all structural:
no ``metric == "<name>"`` branching exists in this module.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from enum import Enum
from typing import Any

from trendx.forecasting.contract import (
    FeatureCategory,
    FeatureDefinition,
    FeatureSchema,
    ForecastRequest,
)


class ResolutionStatus(str, Enum):
    """Explicit per-feature resolution outcome."""

    RESOLVED = "resolved"
    UNAVAILABLE = "unavailable"
    INVALID = "invalid"
    NOT_SUPPORTED = "not_supported"


@dataclass(frozen=True)
class ResolvedFeature:
    """One feature with its resolution outcome (deterministic description)."""

    name: str
    source: str = ""
    metric: str = ""
    entity_id: str = ""
    category: FeatureCategory = FeatureCategory.HISTORICAL
    lag: str = ""
    transformation: str = ""
    availability: str = ""
    required: bool = False
    status: ResolutionStatus = ResolutionStatus.UNAVAILABLE
    detail: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "source": self.source,
            "metric": self.metric,
            "entity_id": self.entity_id,
            "category": self.category.value,
            "lag": self.lag,
            "transformation": self.transformation,
            "availability": self.availability,
            "required": self.required,
            "status": self.status.value,
            "detail": self.detail,
        }


@dataclass(frozen=True)
class ResolvedFeatureSet:
    """Deterministic resolution outcome for one forecast request."""

    tenant_id: str
    entity_id: str
    target_metric: str
    schema_fingerprint: str = ""
    resolutions: tuple[ResolvedFeature, ...] = ()
    errors: tuple[str, ...] = ()

    @property
    def ok(self) -> bool:
        return not self.errors

    def resolved_names(self) -> tuple[str, ...]:
        return tuple(r.name for r in self.resolutions if r.status is ResolutionStatus.RESOLVED)

    def to_dict(self) -> dict[str, Any]:
        return {
            "tenant_id": self.tenant_id,
            "entity_id": self.entity_id,
            "target_metric": self.target_metric,
            "schema_fingerprint": self.schema_fingerprint,
            "ok": self.ok,
            "resolutions": [r.to_dict() for r in self.resolutions],
            "errors": list(self.errors),
        }


class FeatureResolver:
    """Resolve W84 feature contracts against an explicit availability snapshot.

    ``available`` maps ``(entity_id, metric_name)`` to historical presence.
    ``external_providers`` names the configured EXTERNAL_FORECAST sources
    (empty means none are implemented — W85 never invents providers).
    """

    def __init__(
        self,
        available: Iterable[tuple[str, str]] | None = None,
        external_providers: Iterable[str] | None = None,
    ) -> None:
        self._available = frozenset(tuple(p) for p in (available or ()))
        self._providers = frozenset(external_providers or ())

    def resolve(
        self,
        request: ForecastRequest,
        schema: FeatureSchema | None = None,
    ) -> ResolvedFeatureSet:
        active = schema if schema is not None else request.feature_schema()
        schema_names = set(active.names())
        resolutions: list[ResolvedFeature] = []
        errors: list[str] = []
        for fdef in active.features:
            resolved, error = self._resolve_one(request, schema_names, fdef)
            resolutions.append(resolved)
            if error is not None:
                errors.append(error)
        return ResolvedFeatureSet(
            tenant_id=request.tenant_id,
            entity_id=request.entity_id,
            target_metric=request.target_metric,
            schema_fingerprint=active.fingerprint(),
            resolutions=tuple(resolutions),
            errors=tuple(errors),
        )

    def _resolve_one(
        self,
        request: ForecastRequest,
        schema_names: set[str],
        fdef: FeatureDefinition,
    ) -> tuple[ResolvedFeature, str | None]:
        if not fdef.name or not str(fdef.name).strip():
            return (
                ResolvedFeature(name="", status=ResolutionStatus.INVALID, detail="empty name"),
                "invalid feature definition: empty name",
            )
        if fdef.name not in schema_names:
            return (
                _spawn(
                    fdef,
                    request.entity_id,
                    ResolutionStatus.INVALID,
                    "not a member of the expected schema",
                ),
                f"schema mismatch: feature {fdef.name!r} not in schema",
            )
        entity_id = fdef.entity_scope or request.entity_id
        if entity_id != request.entity_id:
            return (
                _spawn(
                    fdef,
                    entity_id,
                    ResolutionStatus.INVALID,
                    "cross-entity feature not explicitly allowed",
                ),
                f"entity scope mismatch: {entity_id!r} != {request.entity_id!r}",
            )
        if fdef.metric == request.target_metric and not (fdef.lag or "").strip():
            return (
                _spawn(
                    fdef,
                    entity_id,
                    ResolutionStatus.INVALID,
                    "target metric must not be its own contemporary feature",
                ),
                f"target/feature confusion: {fdef.metric!r} without lag",
            )
        if fdef.category is FeatureCategory.HISTORICAL:
            key = (entity_id, fdef.metric or request.target_metric)
            if key in self._available:
                return (
                    _spawn(fdef, entity_id, ResolutionStatus.RESOLVED, ""),
                    None,
                )
            return self._missing(fdef, entity_id, "no historical data")
        if fdef.category is FeatureCategory.KNOWN_FUTURE:
            return (
                _spawn(
                    fdef,
                    entity_id,
                    ResolutionStatus.RESOLVED,
                    "known-future variable, no external source needed",
                ),
                None,
            )
        if fdef.category is FeatureCategory.EXTERNAL_FORECAST:
            if fdef.source and fdef.source in self._providers:
                return (
                    _spawn(fdef, entity_id, ResolutionStatus.RESOLVED, ""),
                    None,
                )
            return self._missing(fdef, entity_id, "no external provider")
        return (
            _spawn(
                fdef,
                entity_id,
                ResolutionStatus.NOT_SUPPORTED,
                f"unknown category {fdef.category!r}",
            ),
            f"unsupported category for feature {fdef.name!r}",
        )

    @staticmethod
    def _missing(
        fdef: FeatureDefinition,
        entity_id: str,
        detail: str,
    ) -> tuple[ResolvedFeature, str | None]:
        resolved = _spawn(
            fdef,
            entity_id,
            ResolutionStatus.UNAVAILABLE,
            detail,
        )
        if fdef.required:
            return resolved, f"required feature unavailable: {fdef.name!r} ({detail})"
        return resolved, None


def _spawn(
    fdef: FeatureDefinition,
    entity_id: str,
    status: ResolutionStatus,
    detail: str,
) -> ResolvedFeature:
    """Copy a definition into a resolved record (explicit fields, no **dict)."""
    return ResolvedFeature(
        name=fdef.name,
        source=fdef.source,
        metric=fdef.metric,
        entity_id=entity_id,
        category=fdef.category,
        lag=fdef.lag,
        transformation=fdef.transformation,
        availability=fdef.availability,
        required=fdef.required,
        status=status,
        detail=detail,
    )
