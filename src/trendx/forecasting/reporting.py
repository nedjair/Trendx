"""W106 — stable, read-only reporting contract over W104 analytics.

The reporting layer is intentionally a representation layer.  It accepts the
already-computed :class:`ExecutionAnalytics` projection and only maps typed
structures.  It does not access persistence, models, artifacts, or external
systems.  The single analytics query remains owned by W104.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from trendx.forecasting.analytics import (
    TREND_CONTRACT,
    AnalyticsGroup,
    EntityAnalytics,
    ExecutionAnalytics,
    FailureAnalytics,
    ModelAnalytics,
    TimeBucket,
    analyze_execution_history,
)
from trendx.forecasting.execution import ExecutionStatus, ExecutionStoreError
from trendx.forecasting.history import ExecutionHistoryService, ExecutionQuery, ExecutionStatistics

REPORT_CONTRACT_VERSION = "1"


class ExecutionReportingDataError(ExecutionStoreError):
    """The analytics result cannot safely be represented as a W106 report."""


@dataclass(frozen=True)
class ReportTenantContext:
    """Authorized tenant scope carried by a report."""

    tenant_id: str

    def to_dict(self) -> dict[str, str]:
        return {"tenant_id": self.tenant_id}


@dataclass(frozen=True)
class ExecutionReportFilters:
    """Normalized, typed representation of all W99 report filters."""

    tenant_id: str
    entity_type: str | None = None
    entity_id: str | None = None
    target_metric: str | None = None
    execution_id: str | None = None
    reference_key: str | None = None
    model_id: str | None = None
    model_version: str | None = None
    algorithm: str | None = None
    feature_schema_version: str | None = None
    feature_schema_fingerprint: str | None = None
    status: str | None = None
    created_at_from: str | None = None
    created_at_to: str | None = None
    completed_at_from: str | None = None
    completed_at_to: str | None = None

    @classmethod
    def from_query(cls, query: ExecutionQuery) -> ExecutionReportFilters:
        tenant_id = query.tenant_id
        if not tenant_id or not tenant_id.strip():
            msg = "Execution analytics report requires a tenant scope"
            raise ValueError(msg)
        return cls(
            tenant_id=tenant_id,
            entity_type=query.entity_type,
            entity_id=query.entity_id,
            target_metric=query.target_metric,
            execution_id=query.execution_id,
            reference_key=query.reference_key,
            model_id=query.model_id,
            model_version=query.model_version,
            algorithm=query.algorithm,
            feature_schema_version=query.feature_schema_version,
            feature_schema_fingerprint=query.feature_schema_fingerprint,
            status=_status_text(query.status),
            created_at_from=_timestamp_text(query.created_at_from),
            created_at_to=_timestamp_text(query.created_at_to),
            completed_at_from=_timestamp_text(query.completed_at_from),
            completed_at_to=_timestamp_text(query.completed_at_to),
        )

    def to_dict(self) -> dict[str, str | None]:
        return {
            "tenant_id": self.tenant_id,
            "entity_type": self.entity_type,
            "entity_id": self.entity_id,
            "target_metric": self.target_metric,
            "execution_id": self.execution_id,
            "reference_key": self.reference_key,
            "model_id": self.model_id,
            "model_version": self.model_version,
            "algorithm": self.algorithm,
            "feature_schema_version": self.feature_schema_version,
            "feature_schema_fingerprint": self.feature_schema_fingerprint,
            "status": self.status,
            "created_at_from": self.created_at_from,
            "created_at_to": self.created_at_to,
            "completed_at_from": self.completed_at_from,
            "completed_at_to": self.completed_at_to,
        }


@dataclass(frozen=True)
class ReportPagination:
    """Explicit aggregate-report pagination policy.

    W104 analytics already removes W99 pagination before computing its single
    snapshot.  A report therefore has no page of rows to paginate; this marker
    makes that policy visible instead of silently omitting it.
    """

    applied: bool = False
    reason: str = "aggregate_report"

    def to_dict(self) -> dict[str, str | bool]:
        return {"applied": self.applied, "reason": self.reason}


@dataclass(frozen=True)
class ReportTrendContract:
    """Versioned representation of the W104 exact-timestamp trend contract."""

    dimension: str
    bucket: str
    timezone: str
    inclusivity: str
    ordering: str

    @classmethod
    def from_w104(cls, contract: Mapping[str, str]) -> ReportTrendContract:
        return cls(
            dimension=contract["dimension"],
            bucket=contract["bucket"],
            timezone=contract["timezone"],
            inclusivity=contract["inclusivity"],
            ordering=contract["ordering"],
        )

    def to_dict(self) -> dict[str, str]:
        return {
            "dimension": self.dimension,
            "bucket": self.bucket,
            "timezone": self.timezone,
            "inclusivity": self.inclusivity,
            "ordering": self.ordering,
        }


@dataclass(frozen=True)
class ReportDimensions:
    """All W104 dimensions, copied without recalculation."""

    by_metric: tuple[AnalyticsGroup, ...]
    by_entity: tuple[EntityAnalytics, ...]
    by_algorithm: tuple[AnalyticsGroup, ...]
    by_model: tuple[ModelAnalytics, ...]
    by_status: tuple[AnalyticsGroup, ...]

    def to_dict(self) -> dict[str, list[dict[str, Any]]]:
        return {
            "by_metric": [item.to_dict() for item in self.by_metric],
            "by_entity": [item.to_dict() for item in self.by_entity],
            "by_algorithm": [item.to_dict() for item in self.by_algorithm],
            "by_model": [item.to_dict() for item in self.by_model],
            "by_status": [item.to_dict() for item in self.by_status],
        }


@dataclass(frozen=True)
class ReportTemporalData:
    """Exact W104 temporal data and its immutable contract metadata."""

    created_at_trend: tuple[TimeBucket, ...]
    trend_contract: ReportTrendContract

    def to_dict(self) -> dict[str, Any]:
        return {
            "created_at_trend": [item.to_dict() for item in self.created_at_trend],
            "trend_contract": self.trend_contract.to_dict(),
        }


@dataclass(frozen=True)
class ExecutionAnalyticsReport:
    """Stable W106 contract for an operational execution-history report.

    ``generated_at`` is deliberately ``None`` in the default deterministic
    mode.  A wall-clock generation timestamp would make two reads of the same
    durable snapshot differ without adding analytical information.  Callers
    that need an out-of-band generation time may provide a fixed ISO value;
    it is never consumed by W104 and never affects aggregates.
    """

    contract_version: str
    generated_at: str | None
    tenant_context: ReportTenantContext
    applied_filters: ExecutionReportFilters
    pagination: ReportPagination
    summary: ExecutionStatistics
    dimensions: ReportDimensions
    temporal: ReportTemporalData
    failures: tuple[FailureAnalytics, ...]

    @property
    def tenant_id(self) -> str:
        return self.tenant_context.tenant_id

    def to_dict(self) -> dict[str, Any]:
        return {
            "contract_version": self.contract_version,
            "generated_at": self.generated_at,
            "tenant_id": self.tenant_context.tenant_id,
            "tenant_context": self.tenant_context.to_dict(),
            "applied_filters": self.applied_filters.to_dict(),
            "pagination": self.pagination.to_dict(),
            "summary": self.summary.to_dict(),
            "dimensions": self.dimensions.to_dict(),
            "temporal": self.temporal.to_dict(),
            "failures": [item.to_dict() for item in self.failures],
        }


def _status_text(value: ExecutionStatus | str | None) -> str | None:
    if value is None:
        return None
    return value.value if isinstance(value, ExecutionStatus) else str(value)


def _timestamp_text(value: datetime | str | None) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        normalized = value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)
        return normalized.isoformat()
    return str(value)


def build_execution_analytics_report(
    analytics: ExecutionAnalytics,
    query: ExecutionQuery,
    *,
    generated_at: str | None = None,
) -> ExecutionAnalyticsReport:
    """Represent one W104 result as a deterministic W106 report.

    This function performs no history read and no aggregation.  Its only
    validation is the tenant identity check between the query and the W104
    result.
    """

    if not query.tenant_id or query.tenant_id != analytics.tenant_id:
        msg = "Execution analytics result does not match the report tenant scope"
        raise ExecutionReportingDataError(msg)
    return ExecutionAnalyticsReport(
        contract_version=REPORT_CONTRACT_VERSION,
        generated_at=generated_at,
        tenant_context=ReportTenantContext(tenant_id=analytics.tenant_id),
        applied_filters=ExecutionReportFilters.from_query(query),
        pagination=ReportPagination(),
        summary=analytics.summary,
        dimensions=ReportDimensions(
            by_metric=analytics.by_metric,
            by_entity=analytics.by_entity,
            by_algorithm=analytics.by_algorithm,
            by_model=analytics.by_model,
            by_status=analytics.by_status,
        ),
        temporal=ReportTemporalData(
            created_at_trend=analytics.created_at_trend,
            trend_contract=ReportTrendContract.from_w104(TREND_CONTRACT),
        ),
        failures=analytics.failures,
    )


class ExecutionAnalyticsReportingService:
    """Read-only W106 facade over the canonical W104 analytics service."""

    def __init__(self, service: ExecutionHistoryService) -> None:
        self._service = service

    def report(
        self,
        query: ExecutionQuery,
        *,
        generated_at: str | None = None,
    ) -> ExecutionAnalyticsReport:
        analytics = analyze_execution_history(self._service, query)
        return build_execution_analytics_report(analytics, query, generated_at=generated_at)


__all__ = [
    "ExecutionAnalyticsReport",
    "ExecutionAnalyticsReportingService",
    "ExecutionReportFilters",
    "ExecutionReportingDataError",
    "REPORT_CONTRACT_VERSION",
    "ReportDimensions",
    "ReportPagination",
    "ReportTemporalData",
    "ReportTenantContext",
    "ReportTrendContract",
    "build_execution_analytics_report",
]
