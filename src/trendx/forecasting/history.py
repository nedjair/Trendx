"""W99 — read-only forecast execution history and observability.

The history layer consumes the W98 ``ExecutionRecord`` contract through a
read-only projection of ``ExecutionStore``.  It never loads artifacts,
selects models, runs forecasts, trains, promotes, or mutates a registry.
"""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from typing import Any, Protocol, overload

from trendx.forecasting.execution import ExecutionRecord, ExecutionStatus

MAX_HISTORY_PAGE_SIZE = 1000
HISTORY_ORDER = "created_at DESC, execution_id DESC"


class ExecutionHistoryReader(Protocol):
    """Read-only subset of the W98 ExecutionStore contract."""

    def get(self, execution_id: str) -> ExecutionRecord | None: ...

    def get_by_reference_key(self, reference_key: str) -> ExecutionRecord | None: ...

    def list(
        self,
        *,
        reference_key: str | None = None,
        status: ExecutionStatus | None = None,
    ) -> tuple[ExecutionRecord, ...]: ...


def _coerce_status(value: ExecutionStatus | str) -> ExecutionStatus:
    if isinstance(value, ExecutionStatus):
        return value
    try:
        return ExecutionStatus(str(value).upper())
    except ValueError as exc:
        msg = f"Unknown execution status: {value!r}"
        raise ValueError(msg) from exc


def _coerce_datetime(value: datetime | str | None, field_name: str) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        parsed = value
    else:
        raw = str(value).strip()
        if not raw:
            return None
        if raw.endswith("Z"):
            raw = f"{raw[:-1]}+00:00"
        try:
            parsed = datetime.fromisoformat(raw)
        except ValueError as exc:
            msg = f"{field_name} must be an ISO-8601 timestamp"
            raise ValueError(msg) from exc
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _format_datetime(value: datetime | str | None) -> str | None:
    parsed = _coerce_datetime(value, "timestamp")
    return parsed.isoformat() if parsed is not None else None


def _record_timestamp(value: str) -> datetime | None:
    try:
        return _coerce_datetime(value, "record timestamp")
    except ValueError:
        return None


def _status_value(value: ExecutionStatus | str) -> str:
    return value.value if isinstance(value, ExecutionStatus) else str(value)


@dataclass(frozen=True)
class ExecutionQuery:
    """Optional AND-composed filters for execution history.

    Date ranges are inclusive.  ``created_at_from``/``created_at_to`` filter
    creation time; ``completed_at_from``/``completed_at_to`` filter terminal
    completion time.  Pagination is applied after deterministic ordering.
    """

    tenant_id: str | None = None
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
    status: ExecutionStatus | str | None = None
    created_at_from: datetime | str | None = None
    created_at_to: datetime | str | None = None
    completed_at_from: datetime | str | None = None
    completed_at_to: datetime | str | None = None
    limit: int | None = None
    offset: int = 0

    def __post_init__(self) -> None:
        if self.status is not None:
            object.__setattr__(self, "status", _coerce_status(self.status))
        normalized_dates: dict[str, datetime | None] = {}
        for field_name in (
            "created_at_from",
            "created_at_to",
            "completed_at_from",
            "completed_at_to",
        ):
            normalized = _coerce_datetime(getattr(self, field_name), field_name)
            normalized_dates[field_name] = normalized
            object.__setattr__(self, field_name, normalized)
        created_from = normalized_dates["created_at_from"]
        created_to = normalized_dates["created_at_to"]
        completed_from = normalized_dates["completed_at_from"]
        completed_to = normalized_dates["completed_at_to"]
        if created_from is not None and created_to is not None:
            if created_from > created_to:
                msg = "created_at_from must not be after created_at_to"
                raise ValueError(msg)
        if completed_from is not None and completed_to is not None:
            if completed_from > completed_to:
                msg = "completed_at_from must not be after completed_at_to"
                raise ValueError(msg)
        if self.limit is not None:
            if isinstance(self.limit, bool) or not isinstance(self.limit, int):
                msg = "limit must be an integer"
                raise ValueError(msg)
            if self.limit < 1 or self.limit > MAX_HISTORY_PAGE_SIZE:
                msg = f"limit must be between 1 and {MAX_HISTORY_PAGE_SIZE}"
                raise ValueError(msg)
        if isinstance(self.offset, bool) or not isinstance(self.offset, int):
            msg = "offset must be an integer"
            raise ValueError(msg)
        if self.offset < 0:
            msg = "offset must be non-negative"
            raise ValueError(msg)

    @property
    def created_from(self) -> datetime | str | None:
        """Alias for callers using the shorter range name."""

        return self.created_at_from

    @property
    def created_to(self) -> datetime | str | None:
        """Alias for callers using the shorter range name."""

        return self.created_at_to

    def to_dict(self) -> dict[str, Any]:
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
            "status": _status_value(self.status) if self.status is not None else None,
            "created_at_from": _format_datetime(self.created_at_from),
            "created_at_to": _format_datetime(self.created_at_to),
            "completed_at_from": _format_datetime(self.completed_at_from),
            "completed_at_to": _format_datetime(self.completed_at_to),
            "limit": self.limit,
            "offset": self.offset,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> ExecutionQuery:
        return cls(
            tenant_id=data.get("tenant_id"),
            entity_type=data.get("entity_type"),
            entity_id=data.get("entity_id"),
            target_metric=data.get("target_metric"),
            execution_id=data.get("execution_id"),
            reference_key=data.get("reference_key"),
            model_id=data.get("model_id"),
            model_version=data.get("model_version"),
            algorithm=data.get("algorithm"),
            feature_schema_version=data.get("feature_schema_version"),
            feature_schema_fingerprint=data.get("feature_schema_fingerprint"),
            status=data.get("status"),
            created_at_from=data.get("created_at_from"),
            created_at_to=data.get("created_at_to"),
            completed_at_from=data.get("completed_at_from"),
            completed_at_to=data.get("completed_at_to"),
            limit=data.get("limit"),
            offset=data.get("offset", 0),
        )


@dataclass(frozen=True)
class ExecutionHistoryPage(Sequence[ExecutionRecord]):
    """Stable, optionally paginated sequence of execution records."""

    records: tuple[ExecutionRecord, ...]
    total: int
    limit: int | None
    offset: int

    @property
    def has_more(self) -> bool:
        return self.limit is not None and self.offset + len(self.records) < self.total

    def __iter__(self) -> Iterator[ExecutionRecord]:
        return iter(self.records)

    def __len__(self) -> int:
        return len(self.records)

    @overload
    def __getitem__(self, index: int) -> ExecutionRecord: ...

    @overload
    def __getitem__(self, index: slice) -> tuple[ExecutionRecord, ...]: ...

    def __getitem__(self, index: int | slice) -> ExecutionRecord | tuple[ExecutionRecord, ...]:
        return self.records[index]

    def to_dict(self) -> dict[str, Any]:
        return {
            "records": [record.to_dict() for record in self.records],
            "total": self.total,
            "limit": self.limit,
            "offset": self.offset,
            "has_more": self.has_more,
            "order": HISTORY_ORDER,
        }


@dataclass(frozen=True)
class ExecutionStatistics:
    """Execution metrics derived from a filtered history set."""

    total_executions: int
    success_count: int
    failure_count: int
    success_rate: float
    prediction_count_total: int
    duration_sample_count: int
    average_duration_seconds: float | None
    total_duration_seconds: float | None

    @property
    def failed_count(self) -> int:
        """Compatibility alias for the W99 terminology."""

        return self.failure_count

    def to_dict(self) -> dict[str, Any]:
        return {
            "total_executions": self.total_executions,
            "success_count": self.success_count,
            "failure_count": self.failure_count,
            "failed_count": self.failure_count,
            "success_rate": self.success_rate,
            "prediction_count_total": self.prediction_count_total,
            "duration_sample_count": self.duration_sample_count,
            "average_duration_seconds": self.average_duration_seconds,
            "total_duration_seconds": self.total_duration_seconds,
        }


@dataclass(frozen=True)
class FailureDiagnostic:
    """Read-only diagnostic view of one FAILED execution."""

    execution_id: str
    reference_key: str
    tenant_id: str
    entity_type: str
    entity_id: str
    target_metric: str
    model_id: str
    model_version: str
    algorithm: str
    feature_schema_version: str
    feature_schema_fingerprint: str
    artifact_uri: str
    status: ExecutionStatus
    error_code: str
    error_reason: str
    created_at: str
    started_at: str
    completed_at: str

    @classmethod
    def from_record(cls, record: ExecutionRecord) -> FailureDiagnostic:
        status = _coerce_status(record.status)
        if status is not ExecutionStatus.FAILED:
            msg = "FailureDiagnostic requires a FAILED ExecutionRecord"
            raise ValueError(msg)
        return cls(
            execution_id=record.execution_id,
            reference_key=record.reference_key,
            tenant_id=record.tenant_id,
            entity_type=record.entity_type,
            entity_id=record.entity_id,
            target_metric=record.target_metric,
            model_id=record.model_id,
            model_version=record.model_version,
            algorithm=record.algorithm,
            feature_schema_version=record.feature_schema_version,
            feature_schema_fingerprint=record.feature_schema_fingerprint,
            artifact_uri=record.artifact_uri,
            status=status,
            error_code=record.error_code,
            error_reason=record.error_reason,
            created_at=record.created_at,
            started_at=record.started_at,
            completed_at=record.completed_at,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "execution_id": self.execution_id,
            "reference_key": self.reference_key,
            "tenant_id": self.tenant_id,
            "entity_type": self.entity_type,
            "entity_id": self.entity_id,
            "target_metric": self.target_metric,
            "model_id": self.model_id,
            "model_version": self.model_version,
            "algorithm": self.algorithm,
            "feature_schema_version": self.feature_schema_version,
            "feature_schema_fingerprint": self.feature_schema_fingerprint,
            "artifact_uri": self.artifact_uri,
            "status": self.status.value,
            "error_code": self.error_code,
            "error_reason": self.error_reason,
            "created_at": self.created_at,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
        }


class ExecutionHistoryService:
    """Read-only query and observability facade over ExecutionStore records."""

    def __init__(self, store: ExecutionHistoryReader) -> None:
        self._store = store

    def query(self, query: ExecutionQuery | None = None) -> ExecutionHistoryPage:
        active_query = query or ExecutionQuery()
        records = self._filtered_records(active_query)
        ordered = tuple(sorted(records, key=_history_sort_key, reverse=True))
        total = len(ordered)
        start = active_query.offset
        if active_query.limit is None:
            page_records = ordered[start:]
        else:
            page_records = ordered[start : start + active_query.limit]
        return ExecutionHistoryPage(
            records=page_records,
            total=total,
            limit=active_query.limit,
            offset=start,
        )

    def search(self, query: ExecutionQuery | None = None) -> ExecutionHistoryPage:
        return self.query(query)

    def list_history(self, query: ExecutionQuery | None = None) -> ExecutionHistoryPage:
        return self.query(query)

    def list_records(self, query: ExecutionQuery | None = None) -> tuple[ExecutionRecord, ...]:
        return self.query(query).records

    def count(self, query: ExecutionQuery | None = None) -> int:
        return self.query(query).total

    def get(self, execution_id: str) -> ExecutionRecord | None:
        return self._store.get(execution_id)

    def get_by_reference_key(self, reference_key: str) -> ExecutionRecord | None:
        return self._store.get_by_reference_key(reference_key)

    def recent(self, limit: int = 10, query: ExecutionQuery | None = None) -> ExecutionHistoryPage:
        active_query = query or ExecutionQuery()
        return self.query(replace(active_query, limit=limit, offset=0))

    def find_by_model(
        self,
        model_id: str,
        *,
        model_version: str | None = None,
        query: ExecutionQuery | None = None,
    ) -> ExecutionHistoryPage:
        if not model_id:
            msg = "model_id is required"
            raise ValueError(msg)
        active_query = query or ExecutionQuery()
        return self.query(
            replace(
                active_query,
                model_id=model_id,
                model_version=model_version or active_query.model_version,
            )
        )

    def statistics(self, query: ExecutionQuery | None = None) -> ExecutionStatistics:
        records = self._filtered_records(query or ExecutionQuery())
        success_count = sum(
            _status_value(record.status) == ExecutionStatus.SUCCESS.value for record in records
        )
        failure_count = sum(
            _status_value(record.status) == ExecutionStatus.FAILED.value for record in records
        )
        prediction_count_total = sum(record.prediction_count or 0 for record in records)
        durations: list[float] = []
        for record in records:
            duration = _record_duration_seconds(record)
            if duration is not None:
                durations.append(duration)
        total_duration: float | None = sum(durations) if durations else None
        average_duration = (
            total_duration / len(durations) if total_duration is not None and durations else None
        )
        total = len(records)
        success_rate = success_count / total if total else 0.0
        return ExecutionStatistics(
            total_executions=total,
            success_count=success_count,
            failure_count=failure_count,
            success_rate=success_rate,
            prediction_count_total=prediction_count_total,
            duration_sample_count=len(durations),
            average_duration_seconds=average_duration,
            total_duration_seconds=total_duration,
        )

    def failure_diagnostics(
        self, query: ExecutionQuery | None = None
    ) -> tuple[FailureDiagnostic, ...]:
        active_query = query or ExecutionQuery()
        diagnostic_query = (
            active_query
            if active_query.status is not None
            else replace(active_query, status=ExecutionStatus.FAILED)
        )
        records = sorted(
            self._filtered_records(diagnostic_query),
            key=_history_sort_key,
            reverse=True,
        )
        return tuple(
            FailureDiagnostic.from_record(record)
            for record in records
            if _status_value(record.status) == ExecutionStatus.FAILED.value
        )

    def diagnostics(self, query: ExecutionQuery | None = None) -> tuple[FailureDiagnostic, ...]:
        return self.failure_diagnostics(query)

    def _filtered_records(self, query: ExecutionQuery) -> tuple[ExecutionRecord, ...]:
        return tuple(record for record in self._store.list() if _matches_query(record, query))


def _history_sort_key(record: ExecutionRecord) -> tuple[datetime, str]:
    timestamp = _record_timestamp(record.created_at) or datetime.min.replace(tzinfo=UTC)
    return timestamp, record.execution_id


def _matches_query(record: ExecutionRecord, query: ExecutionQuery) -> bool:
    exact_filters = (
        (query.tenant_id, record.tenant_id),
        (query.entity_type, record.entity_type),
        (query.entity_id, record.entity_id),
        (query.target_metric, record.target_metric),
        (query.execution_id, record.execution_id),
        (query.reference_key, record.reference_key),
        (query.model_id, record.model_id),
        (query.model_version, record.model_version),
        (query.feature_schema_version, record.feature_schema_version),
        (query.feature_schema_fingerprint, record.feature_schema_fingerprint),
    )
    if any(expected is not None and expected != actual for expected, actual in exact_filters):
        return False
    if query.algorithm is not None and query.algorithm.casefold() != record.algorithm.casefold():
        return False
    if query.status is not None and _status_value(query.status) != _status_value(record.status):
        return False

    created_at = _record_timestamp(record.created_at)
    created_from = _coerce_datetime(query.created_at_from, "created_at_from")
    created_to = _coerce_datetime(query.created_at_to, "created_at_to")
    if created_from is not None and (created_at is None or created_at < created_from):
        return False
    if created_to is not None and (created_at is None or created_at > created_to):
        return False

    completed_at = _record_timestamp(record.completed_at)
    completed_from = _coerce_datetime(query.completed_at_from, "completed_at_from")
    completed_to = _coerce_datetime(query.completed_at_to, "completed_at_to")
    if completed_from is not None and (completed_at is None or completed_at < completed_from):
        return False
    if completed_to is not None and (completed_at is None or completed_at > completed_to):
        return False
    return True


def _record_duration_seconds(record: ExecutionRecord) -> float | None:
    started_at = _record_timestamp(record.started_at)
    completed_at = _record_timestamp(record.completed_at)
    if started_at is None or completed_at is None:
        return None
    duration = (completed_at - started_at).total_seconds()
    return duration if duration >= 0 else None


__all__ = [
    "ExecutionHistoryPage",
    "ExecutionHistoryReader",
    "ExecutionHistoryService",
    "ExecutionQuery",
    "ExecutionStatistics",
    "FailureDiagnostic",
    "HISTORY_ORDER",
    "MAX_HISTORY_PAGE_SIZE",
]
