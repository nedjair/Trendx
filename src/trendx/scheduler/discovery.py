"""Generic multi-metric forecast discovery + dispatch planning (W89).

Reads candidate (entity, metric) pairs from the existing catalogue mechanism
(``fanout.iter_forecast_pairs`` shape), applies an explicit application-level
``ForecastPolicy`` (no DB migration), and produces valid ``ForecastRequest``
objects plus generic worker payloads via the existing
``fanout.build_forecast_payload``.

This module never resolves features, builds datasets, selects models, runs
inference, or touches persistence or external systems. Metric names are configuration
keys, never branching conditions.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

from trendx.forecasting.contract import (
    Algorithm,
    FeatureDefinition,
    ForecastRequest,
)
from trendx.scheduler.fanout import build_forecast_payload, forecast_reference_key


@dataclass(frozen=True)
class MetricPolicy:
    """Per-metric forecast configuration (application level, no migration)."""

    forecast_enabled: bool = True
    frequency: str = "1h"
    horizon: int = 24
    algorithm: str = "AUTO"
    features: tuple[FeatureDefinition, ...] = ()


@dataclass(frozen=True)
class ForecastPolicy:
    """Defaults plus per-metric overrides keyed by metric name."""

    defaults: MetricPolicy = field(default_factory=MetricPolicy)
    overrides: Mapping[str, MetricPolicy] = field(default_factory=dict)

    def for_metric(self, metric_name: str) -> MetricPolicy:
        return self.overrides.get(metric_name, self.defaults)


@dataclass(frozen=True)
class MetricCandidate:
    tenant_id: str
    entity_type: str
    entity_id: str
    metric_name: str
    policy: MetricPolicy = field(default_factory=MetricPolicy)


@dataclass(frozen=True)
class Rejection:
    entity_id: str
    metric_name: str
    reason: str


@dataclass(frozen=True)
class TickPlan:
    payloads: tuple[dict[str, Any], ...] = ()
    skipped_duplicates: tuple[str, ...] = ()
    errors: tuple[Rejection, ...] = ()


def discover(
    pairs: list[dict[str, str]],
    policy: ForecastPolicy | None = None,
) -> tuple[list[MetricCandidate], list[Rejection]]:
    """Filter catalogue pairs into eligible candidates (never crashes)."""
    active = policy or ForecastPolicy()
    eligible: list[MetricCandidate] = []
    rejected: list[Rejection] = []
    for pair in pairs:
        tenant = str(pair.get("tenant_id", "") or "").strip()
        entity = str(pair.get("entity_id", "") or "").strip()
        metric = str(pair.get("metric_name", "") or "").strip()
        entity_type = str(pair.get("entity_type", "") or "DEVICE").strip()
        if not entity or not metric:
            rejected.append(Rejection(entity, metric, "invalid entity/metric"))
            continue
        if not tenant:
            rejected.append(Rejection(entity, metric, "missing tenant"))
            continue
        metric_policy = active.for_metric(metric)
        if not metric_policy.forecast_enabled:
            rejected.append(Rejection(entity, metric, "forecast disabled"))
            continue
        candidate = MetricCandidate(
            tenant_id=tenant,
            entity_type=entity_type or "DEVICE",
            entity_id=entity,
            metric_name=metric,
            policy=metric_policy,
        )
        try:
            build_request(candidate)
        except ValueError as exc:
            rejected.append(Rejection(entity, metric, f"invalid config: {exc}"))
            continue
        eligible.append(candidate)
    return eligible, rejected


def build_request(candidate: MetricCandidate) -> ForecastRequest:
    """Build a valid ForecastRequest from a candidate (target from discovery)."""
    return ForecastRequest(
        tenant_id=candidate.tenant_id,
        entity_type=candidate.entity_type,
        entity_id=candidate.entity_id,
        target_metric=candidate.metric_name,
        horizon=candidate.policy.horizon,
        frequency=candidate.policy.frequency,
        features=candidate.policy.features,
        algorithm=Algorithm.coerce(candidate.policy.algorithm),
    )


def plan_tick(
    candidates: list[MetricCandidate],
    *,
    job_type: str,
    window: str,
    dispatched: set[str] | None = None,
) -> TickPlan:
    """Plan one tick: generic payloads, deduped, errors isolated per unit."""
    seen: set[str] = dispatched if dispatched is not None else set()
    payloads: list[dict[str, Any]] = []
    skipped: list[str] = []
    errors: list[Rejection] = []
    for candidate in candidates:
        try:
            request = build_request(candidate)
        except ValueError as exc:
            errors.append(Rejection(candidate.entity_id, candidate.metric_name, str(exc)))
            continue
        key = forecast_reference_key(
            tenant_id=request.tenant_id,
            entity_id=request.entity_id,
            metric_name=request.target_metric,
            job_type=job_type,
            window=window,
        )
        if key in seen:
            skipped.append(key)
            continue
        try:
            payload = build_forecast_payload(
                pair={
                    "tenant_id": request.tenant_id,
                    "entity_type": request.entity_type,
                    "entity_id": request.entity_id,
                    "metric_name": request.target_metric,
                },
                job_type=job_type,
                window=window,
                overrides={
                    "frequency": request.frequency,
                    "horizon": request.horizon,
                    "algorithm": request.algorithm.value,
                    "features": [f.to_dict() for f in request.features],
                    "target_metric": request.target_metric,
                },
            )
        except ValueError as exc:
            errors.append(Rejection(candidate.entity_id, candidate.metric_name, str(exc)))
            continue
        seen.add(key)
        payloads.append(payload)
    return TickPlan(
        payloads=tuple(payloads),
        skipped_duplicates=tuple(skipped),
        errors=tuple(errors),
    )


__all__ = [
    "Algorithm",
    "ForecastPolicy",
    "ForecastRequest",
    "MetricCandidate",
    "MetricPolicy",
    "Rejection",
    "TickPlan",
    "build_request",
    "discover",
    "plan_tick",
]
