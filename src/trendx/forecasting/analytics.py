"""W104 — deterministic, tenant-scoped analytical projection of execution history.

This module consumes the read-only :class:`ExecutionHistoryService` contract.
It never reads a datastore directly, loads an artifact, selects a model, or
mutates a record.  All dimensions are generic values already present on
``ExecutionRecord``.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from types import MappingProxyType
from typing import Any, TypeVar

from trendx.forecasting.execution import (
    ExecutionRecord,
    ExecutionStatus,
    ExecutionStoreError,
)
from trendx.forecasting.history import (
    ExecutionHistoryService,
    ExecutionQuery,
    ExecutionStatistics,
)

T = TypeVar("T")

TREND_CONTRACT: Mapping[str, str] = MappingProxyType(
    {
        "dimension": "created_at",
        "bucket": "exact_timestamp",
        "timezone": "UTC",
        "inclusivity": "inclusive",
        "ordering": "ascending",
    }
)


class ExecutionHistoryDataError(ExecutionStoreError):
    """Persisted history is structurally readable but not analytics-valid."""


@dataclass(frozen=True)
class AnalyticsGroup:
    """Generic aggregate for a named dimension value."""

    key: str
    total: int
    success_count: int
    failed_count: int
    success_rate: float
    prediction_count: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "key": self.key,
            "total": self.total,
            "success_count": self.success_count,
            "failed_count": self.failed_count,
            "success_rate": self.success_rate,
            "prediction_count": self.prediction_count,
        }


@dataclass(frozen=True)
class EntityAnalytics:
    """Aggregate for the generic entity type/entity pair dimension."""

    entity_type: str
    entity_id: str
    total: int
    success_count: int
    failed_count: int
    success_rate: float
    prediction_count: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "entity_type": self.entity_type,
            "entity_id": self.entity_id,
            "total": self.total,
            "success_count": self.success_count,
            "failed_count": self.failed_count,
            "success_rate": self.success_rate,
            "prediction_count": self.prediction_count,
        }


@dataclass(frozen=True)
class ModelAnalytics:
    """Aggregate for persisted model provenance."""

    model_id: str
    model_version: str
    total: int
    success_count: int
    failed_count: int
    success_rate: float
    prediction_count: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "model_id": self.model_id,
            "model_version": self.model_version,
            "total": self.total,
            "success_count": self.success_count,
            "failed_count": self.failed_count,
            "success_rate": self.success_rate,
            "prediction_count": self.prediction_count,
        }


@dataclass(frozen=True)
class FailureAnalytics:
    """Count and proportion for a persisted failure diagnostic code."""

    error_code: str
    count: int
    proportion: float

    def to_dict(self) -> dict[str, Any]:
        return {
            "error_code": self.error_code,
            "count": self.count,
            "proportion": self.proportion,
        }


@dataclass(frozen=True)
class TimeBucket:
    """Aggregate for one exact normalized UTC ``created_at`` value.

    W104 deliberately uses exact timestamps as buckets.  The available
    record contract does not define a calendar bucket size, so no daily or
    hourly approximation is invented.
    """

    bucket: str
    total: int
    success_count: int
    failed_count: int
    prediction_count: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "bucket": self.bucket,
            "total": self.total,
            "success_count": self.success_count,
            "failed_count": self.failed_count,
            "prediction_count": self.prediction_count,
        }


@dataclass(frozen=True)
class ExecutionAnalytics:
    """Complete deterministic analytical projection for one tenant scope."""

    tenant_id: str
    summary: ExecutionStatistics
    by_metric: tuple[AnalyticsGroup, ...]
    by_entity: tuple[EntityAnalytics, ...]
    by_algorithm: tuple[AnalyticsGroup, ...]
    by_model: tuple[ModelAnalytics, ...]
    by_status: tuple[AnalyticsGroup, ...]
    failures: tuple[FailureAnalytics, ...]
    created_at_trend: tuple[TimeBucket, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "tenant_id": self.tenant_id,
            "summary": self.summary.to_dict(),
            "by_metric": [item.to_dict() for item in self.by_metric],
            "by_entity": [item.to_dict() for item in self.by_entity],
            "by_algorithm": [item.to_dict() for item in self.by_algorithm],
            "by_model": [item.to_dict() for item in self.by_model],
            "by_status": [item.to_dict() for item in self.by_status],
            "failures": [item.to_dict() for item in self.failures],
            "created_at_trend": [item.to_dict() for item in self.created_at_trend],
            "trend_contract": dict(TREND_CONTRACT),
        }


def _status_value(record: ExecutionRecord) -> str:
    return record.status.value if isinstance(record.status, ExecutionStatus) else str(record.status)


def _parse_timestamp(value: str | datetime | None) -> datetime | None:
    """Parse a persisted timestamp using W99's UTC-compatible semantics."""

    if value is None:
        return None
    if isinstance(value, datetime):
        parsed = value
    else:
        raw = value.strip()
        if not raw:
            return None
        if raw.endswith("Z"):
            raw = f"{raw[:-1]}+00:00"
        try:
            parsed = datetime.fromisoformat(raw)
        except ValueError:
            return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _statistics(records: tuple[ExecutionRecord, ...]) -> ExecutionStatistics:
    """Build the W99 statistics contract from one immutable query snapshot."""

    success_count = sum(
        _status_value(record) == ExecutionStatus.SUCCESS.value for record in records
    )
    failure_count = sum(_status_value(record) == ExecutionStatus.FAILED.value for record in records)
    prediction_count_total = sum(record.prediction_count or 0 for record in records)
    durations: list[float] = []
    for record in records:
        started_at = _parse_timestamp(record.started_at)
        completed_at = _parse_timestamp(record.completed_at)
        if started_at is None or completed_at is None:
            continue
        duration = (completed_at - started_at).total_seconds()
        if duration >= 0:
            durations.append(duration)
    total_duration = sum(durations) if durations else None
    total = len(records)
    return ExecutionStatistics(
        total_executions=total,
        success_count=success_count,
        failure_count=failure_count,
        success_rate=success_count / total if total else 0.0,
        prediction_count_total=prediction_count_total,
        duration_sample_count=len(durations),
        average_duration_seconds=(
            total_duration / len(durations) if total_duration is not None and durations else None
        ),
        total_duration_seconds=total_duration,
    )


def _aggregate(records: Iterable[ExecutionRecord]) -> tuple[int, int, int, float, int]:
    values = tuple(records)
    total = len(values)
    success_count = sum(_status_value(record) == ExecutionStatus.SUCCESS.value for record in values)
    failed_count = sum(_status_value(record) == ExecutionStatus.FAILED.value for record in values)
    prediction_count = sum(record.prediction_count or 0 for record in values)
    success_rate = success_count / total if total else 0.0
    return total, success_count, failed_count, success_rate, prediction_count


def _group_values(
    records: tuple[ExecutionRecord, ...],
    key: Callable[[ExecutionRecord], T],
) -> dict[T, tuple[ExecutionRecord, ...]]:
    grouped: defaultdict[T, list[ExecutionRecord]] = defaultdict(list)
    for record in records:
        grouped[key(record)].append(record)
    return {group_key: tuple(values) for group_key, values in grouped.items()}


def _named_groups(
    records: tuple[ExecutionRecord, ...],
    key: Callable[[ExecutionRecord], T],
    *,
    sort_key: Callable[[T], tuple[str, ...]] | None = None,
) -> tuple[AnalyticsGroup, ...]:
    grouped = _group_values(records, key)
    values = list(grouped.items())
    if sort_key is not None:
        values.sort(key=lambda item: sort_key(item[0]))
    else:
        values.sort(key=lambda item: str(item[0]))
    return tuple(
        AnalyticsGroup(
            key=str(group_key),
            total=counts[0],
            success_count=counts[1],
            failed_count=counts[2],
            success_rate=counts[3],
            prediction_count=counts[4],
        )
        for group_key, group_records in values
        for counts in (_aggregate(group_records),)
    )


def _entity_groups(records: tuple[ExecutionRecord, ...]) -> tuple[EntityAnalytics, ...]:
    grouped = _group_values(records, lambda record: (record.entity_type, record.entity_id))
    return tuple(
        EntityAnalytics(
            entity_type=entity_type,
            entity_id=entity_id,
            total=counts[0],
            success_count=counts[1],
            failed_count=counts[2],
            success_rate=counts[3],
            prediction_count=counts[4],
        )
        for (entity_type, entity_id), group_records in sorted(grouped.items())
        for counts in (_aggregate(group_records),)
    )


def _model_groups(records: tuple[ExecutionRecord, ...]) -> tuple[ModelAnalytics, ...]:
    grouped = _group_values(records, lambda record: (record.model_id, record.model_version))
    return tuple(
        ModelAnalytics(
            model_id=model_id,
            model_version=model_version,
            total=counts[0],
            success_count=counts[1],
            failed_count=counts[2],
            success_rate=counts[3],
            prediction_count=counts[4],
        )
        for (model_id, model_version), group_records in sorted(grouped.items())
        for counts in (_aggregate(group_records),)
    )


def _normalize_created_at(value: str) -> str:
    raw = value.strip()
    if raw.endswith("Z"):
        raw = f"{raw[:-1]}+00:00"
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError as exc:
        msg = "Execution history contains an invalid created_at timestamp"
        raise ExecutionHistoryDataError(msg) from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC).isoformat()


def _trend(records: tuple[ExecutionRecord, ...]) -> tuple[TimeBucket, ...]:
    grouped = _group_values(records, lambda record: _normalize_created_at(record.created_at))
    buckets: list[TimeBucket] = []
    for bucket, bucket_records in sorted(grouped.items(), key=lambda item: item[0]):
        total, success_count, failed_count, _success_rate, prediction_count = _aggregate(
            bucket_records
        )
        buckets.append(
            TimeBucket(
                bucket=bucket,
                total=total,
                success_count=success_count,
                failed_count=failed_count,
                prediction_count=prediction_count,
            )
        )
    return tuple(buckets)


def _failures(records: tuple[ExecutionRecord, ...]) -> tuple[FailureAnalytics, ...]:
    failed = tuple(
        record for record in records if _status_value(record) == ExecutionStatus.FAILED.value
    )
    total_failures = len(failed)
    grouped = _group_values(failed, lambda record: record.error_code)
    return tuple(
        FailureAnalytics(
            error_code=error_code,
            count=len(group_records),
            proportion=(len(group_records) / total_failures if total_failures else 0.0),
        )
        for error_code, group_records in sorted(grouped.items())
    )


class ExecutionHistoryAnalytics:
    """Read-only analytical projection over the canonical history service."""

    def __init__(self, service: ExecutionHistoryService) -> None:
        self._service = service

    def analyze(self, query: ExecutionQuery | None = None) -> ExecutionAnalytics:
        """Analyze the complete filtered tenant scope, never a page subset."""

        active_query = query or ExecutionQuery()
        tenant_id = active_query.tenant_id
        if not tenant_id or not tenant_id.strip():
            msg = "Execution history analytics requires a tenant scope"
            raise ValueError(msg)
        normalized_query = replace(active_query, limit=None, offset=0)
        page = self._service.query(normalized_query)
        records = tuple(page.records)
        if any(record.tenant_id != tenant_id for record in records):
            msg = "Execution history service returned records outside the tenant scope"
            raise ExecutionHistoryDataError(msg)
        return ExecutionAnalytics(
            tenant_id=tenant_id,
            summary=_statistics(records),
            by_metric=_named_groups(records, lambda record: record.target_metric),
            by_entity=_entity_groups(records),
            by_algorithm=_named_groups(records, lambda record: record.algorithm),
            by_model=_model_groups(records),
            by_status=_named_groups(records, _status_value),
            failures=_failures(records),
            created_at_trend=_trend(records),
        )


def analyze_execution_history(
    service: ExecutionHistoryService,
    query: ExecutionQuery | None = None,
) -> ExecutionAnalytics:
    """Convenience entry point for API and external read-only consumers."""

    return ExecutionHistoryAnalytics(service).analyze(query)


__all__ = [
    "AnalyticsGroup",
    "EntityAnalytics",
    "ExecutionAnalytics",
    "ExecutionHistoryDataError",
    "ExecutionHistoryAnalytics",
    "FailureAnalytics",
    "ModelAnalytics",
    "TimeBucket",
    "TREND_CONTRACT",
    "analyze_execution_history",
]
