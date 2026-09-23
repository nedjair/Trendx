"""Generic forecast contract: ENTITY + TARGET METRIC + FEATURES (W84).

Contract-first layer shared by future stages (W85+: feature resolution,
dataset building, model selection). No metric-specific branching is allowed
in this module — no metric name may appear here as a structural hypothesis
(test data may use metric names, architecture must not).

Rule locked by this contract (see ``ARCHITECTURE_RULE``)::

    NO NEW FORECAST METRIC SHOULD REQUIRE A CHANGE TO:
        Scheduler, Worker, Forecast Engine, Prediction Persistence.

A new metric is introduced by Entity + MetricDefinition + FeaturePolicy /
FeatureSchema + Model configuration — never by metric-specific code.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

ARCHITECTURE_RULE = (
    "NO NEW FORECAST METRIC SHOULD REQUIRE A CHANGE TO: "
    "Scheduler, Worker, Forecast Engine, Prediction Persistence."
)

_KNOWN_FREQUENCIES = frozenset({"5m", "15m", "1h", "1d"})
_FREQUENCY_RE = re.compile(r"^\d+[mhd]$")


class FeatureCategory(str, Enum):
    """Where a feature comes from (representation only, no resolution)."""

    HISTORICAL = "historical"
    KNOWN_FUTURE = "known_future"
    EXTERNAL_FORECAST = "external_forecast"


class Algorithm(str, Enum):
    """Forecast algorithm selector.

    Members mirror the existing ``MODEL_REGISTRY`` implementations plus the
    ``AUTO`` selection mode. Adding a member here never adds an algorithm
    implementation — implementations live in ``trendx.forecasting``.
    """

    AUTO = "AUTO"
    PROPHET = "Prophet"
    ARIMA = "ARIMA"
    FOURIER = "Fourier"
    LINEAR_REGRESSION = "LinearRegression"
    OLS = "OLS"

    @classmethod
    def coerce(cls, raw: Algorithm | str) -> Algorithm:
        if isinstance(raw, cls):
            return raw
        try:
            return cls(str(raw))
        except ValueError as exc:
            msg = f"Unknown algorithm: {raw!r}"
            raise ValueError(msg) from exc


@dataclass(frozen=True)
class FeatureDefinition:
    """One named input variable (X) for a forecast target (Y)."""

    name: str
    source: str = ""
    metric: str = ""
    entity_scope: str = ""
    category: FeatureCategory = FeatureCategory.HISTORICAL
    lag: str = ""
    transformation: str = ""
    availability: str = ""
    required: bool = False

    def __post_init__(self) -> None:
        if not self.name or not str(self.name).strip():
            msg = "FeatureDefinition.name is required"
            raise ValueError(msg)

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "source": self.source,
            "metric": self.metric,
            "entity_scope": self.entity_scope,
            "category": self.category.value,
            "lag": self.lag,
            "transformation": self.transformation,
            "availability": self.availability,
            "required": self.required,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> FeatureDefinition:
        payload = dict(data)
        if "category" in payload and payload["category"] is not None:
            payload["category"] = FeatureCategory(str(payload["category"]))
        return cls(**{k: v for k, v in payload.items() if k in cls.__dataclass_fields__})


@dataclass(frozen=True)
class FeatureSchema:
    """Exact feature set expected by a model version.

    ``fingerprint`` distinguishes schemas so a model trained with
    ``[metric_a, metric_b]`` is never silently run with ``[metric_a]``.
    """

    schema_version: str = "v1"
    features: tuple[FeatureDefinition, ...] = ()

    def __post_init__(self) -> None:
        names = [f.name for f in self.features]
        if len(set(names)) != len(names):
            msg = f"Duplicate feature names in schema: {names}"
            raise ValueError(msg)

    def names(self) -> tuple[str, ...]:
        return tuple(f.name for f in self.features)

    def fingerprint(self) -> str:
        canonical = json.dumps(
            {
                "schema_version": self.schema_version,
                "features": sorted(
                    (f.to_dict() for f in self.features),
                    key=lambda d: str(d.get("name", "")),
                ),
            },
            sort_keys=True,
            separators=(",", ":"),
        )
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "features": [f.to_dict() for f in self.features],
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> FeatureSchema:
        return cls(
            schema_version=str(data.get("schema_version", "v1")),
            features=tuple(FeatureDefinition.from_dict(f) for f in data.get("features", [])),
        )


@dataclass(frozen=True)
class ForecastRequest:
    """Generic forecast order: ENTITY + TARGET METRIC (Y) + FEATURES (X)."""

    tenant_id: str
    entity_type: str
    entity_id: str
    target_metric: str
    horizon: int = 24
    frequency: str = "1h"
    features: tuple[FeatureDefinition, ...] = ()
    algorithm: Algorithm = Algorithm.PROPHET

    def __post_init__(self) -> None:
        for attr in ("tenant_id", "entity_type", "entity_id", "target_metric"):
            if not getattr(self, attr) or not str(getattr(self, attr)).strip():
                msg = f"ForecastRequest.{attr} is required"
                raise ValueError(msg)
        if not isinstance(self.horizon, int) or isinstance(self.horizon, bool):
            msg = f"ForecastRequest.horizon must be an int, got {self.horizon!r}"
            raise ValueError(msg)
        if self.horizon <= 0:
            msg = f"ForecastRequest.horizon must be > 0, got {self.horizon}"
            raise ValueError(msg)
        freq = str(self.frequency)
        if freq not in _KNOWN_FREQUENCIES and not _FREQUENCY_RE.match(freq):
            msg = f"ForecastRequest.frequency invalid: {self.frequency!r}"
            raise ValueError(msg)
        object.__setattr__(self, "algorithm", Algorithm.coerce(self.algorithm))

    def feature_schema(self, schema_version: str = "v1") -> FeatureSchema:
        return FeatureSchema(schema_version=schema_version, features=self.features)

    def to_dict(self) -> dict[str, Any]:
        return {
            "tenant_id": self.tenant_id,
            "entity_type": self.entity_type,
            "entity_id": self.entity_id,
            "target_metric": self.target_metric,
            "horizon": self.horizon,
            "frequency": self.frequency,
            "features": [f.to_dict() for f in self.features],
            "algorithm": self.algorithm.value,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> ForecastRequest:
        payload = dict(data)
        payload["features"] = tuple(
            FeatureDefinition.from_dict(f) for f in payload.get("features", [])
        )
        if "algorithm" in payload:
            payload["algorithm"] = Algorithm.coerce(payload["algorithm"])
        return cls(**{k: v for k, v in payload.items() if k in cls.__dataclass_fields__})


@dataclass
class ForecastEnvelope:
    """Provenance envelope around an existing ``ForecastResult`` (no storage change).

    Carries execution identity (Y = ``target_metric``) without modifying the
    persisted ``ForecastResult`` shape.
    """

    execution_id: str = ""
    tenant_id: str = ""
    entity_id: str = ""
    target_metric: str = ""
    model_id: str = ""
    model_version: str = ""
    feature_schema_version: str = ""
    created_at: str = ""
    result: Any = field(default=None)

    def to_dict(self) -> dict[str, Any]:
        result_dict = None
        if self.result is not None and hasattr(self.result, "to_dict"):
            result_dict = self.result.to_dict()
        return {
            "execution_id": self.execution_id,
            "tenant_id": self.tenant_id,
            "entity_id": self.entity_id,
            "target_metric": self.target_metric,
            "model_id": self.model_id,
            "model_version": self.model_version,
            "feature_schema_version": self.feature_schema_version,
            "created_at": self.created_at,
            "result": result_dict,
        }
