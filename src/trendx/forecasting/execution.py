"""W98 — generic forecast execution records and injectable store.

The contract is technology-neutral.  It records the request, the model
provenance actually used for serving, terminal status, and a serializable
result when available.  ``MemoryExecutionStore`` is deliberately the only
bundled implementation: production persistence can be supplied through the
same protocol without making a remote database mandatory.
"""

from __future__ import annotations

from collections.abc import Mapping
from copy import deepcopy
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from enum import Enum
from threading import RLock
from typing import Any, Protocol


class ExecutionStatus(str, Enum):
    """Lifecycle states for one forecast execution."""

    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"


class ExecutionStoreError(Exception):
    """Base error for execution-store operations."""


class InvalidExecutionTransitionError(ExecutionStoreError):
    """Raised when a lifecycle transition is not allowed."""


def _now() -> str:
    return datetime.now(UTC).isoformat()


def _copy_mapping(value: Mapping[str, Any] | None) -> dict[str, Any]:
    return deepcopy(dict(value or {}))


def _copy_result(value: Mapping[str, Any] | None) -> dict[str, Any] | None:
    return deepcopy(dict(value)) if value is not None else None


@dataclass(frozen=True)
class ExecutionProvenance:
    """Model and prediction provenance captured at serving time."""

    model_id: str = ""
    model_version: str = ""
    algorithm: str = ""
    feature_schema_version: str = ""
    feature_schema_fingerprint: str = ""
    artifact_uri: str = ""
    prediction_count: int | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    result: dict[str, Any] | None = None

    def __post_init__(self) -> None:
        if self.prediction_count is not None and self.prediction_count < 0:
            msg = "prediction_count must be non-negative"
            raise ValueError(msg)

    def to_dict(self) -> dict[str, Any]:
        return {
            "model_id": self.model_id,
            "model_version": self.model_version,
            "algorithm": self.algorithm,
            "feature_schema_version": self.feature_schema_version,
            "feature_schema_fingerprint": self.feature_schema_fingerprint,
            "artifact_uri": self.artifact_uri,
            "prediction_count": self.prediction_count,
            "metadata": _copy_mapping(self.metadata),
            "result": _copy_result(self.result),
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> ExecutionProvenance:
        return cls(
            model_id=str(data.get("model_id", "")),
            model_version=str(data.get("model_version", "")),
            algorithm=str(data.get("algorithm", "")),
            feature_schema_version=str(data.get("feature_schema_version", "")),
            feature_schema_fingerprint=str(data.get("feature_schema_fingerprint", "")),
            artifact_uri=str(data.get("artifact_uri", "")),
            prediction_count=(
                int(data["prediction_count"]) if data.get("prediction_count") is not None else None
            ),
            metadata=_copy_mapping(data.get("metadata")),
            result=_copy_result(data.get("result")),
        )


@dataclass(frozen=True)
class ExecutionRecord:
    """Persisted provenance for one generic forecast execution."""

    execution_id: str
    reference_key: str
    tenant_id: str
    entity_type: str
    entity_id: str
    target_metric: str
    frequency: str
    horizon: int
    model_id: str = ""
    model_version: str = ""
    algorithm: str = ""
    feature_schema_version: str = ""
    feature_schema_fingerprint: str = ""
    artifact_uri: str = ""
    status: ExecutionStatus = ExecutionStatus.PENDING
    created_at: str = ""
    started_at: str = ""
    completed_at: str = ""
    error_code: str = ""
    error_reason: str = ""
    prediction_count: int | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    result: dict[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "execution_id": self.execution_id,
            "reference_key": self.reference_key,
            "tenant_id": self.tenant_id,
            "entity_type": self.entity_type,
            "entity_id": self.entity_id,
            "target_metric": self.target_metric,
            "frequency": self.frequency,
            "horizon": self.horizon,
            "model_id": self.model_id,
            "model_version": self.model_version,
            "algorithm": self.algorithm,
            "feature_schema_version": self.feature_schema_version,
            "feature_schema_fingerprint": self.feature_schema_fingerprint,
            "artifact_uri": self.artifact_uri,
            "status": self.status.value,
            "created_at": self.created_at,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "error_code": self.error_code,
            "error_reason": self.error_reason,
            "prediction_count": self.prediction_count,
            "metadata": _copy_mapping(self.metadata),
            "result": _copy_result(self.result),
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> ExecutionRecord:
        return cls(
            execution_id=str(data.get("execution_id", "")),
            reference_key=str(data.get("reference_key", "")),
            tenant_id=str(data.get("tenant_id", "")),
            entity_type=str(data.get("entity_type", "")),
            entity_id=str(data.get("entity_id", "")),
            target_metric=str(data.get("target_metric", "")),
            frequency=str(data.get("frequency", "")),
            horizon=int(data.get("horizon", 0)),
            model_id=str(data.get("model_id", "")),
            model_version=str(data.get("model_version", "")),
            algorithm=str(data.get("algorithm", "")),
            feature_schema_version=str(data.get("feature_schema_version", "")),
            feature_schema_fingerprint=str(data.get("feature_schema_fingerprint", "")),
            artifact_uri=str(data.get("artifact_uri", "")),
            status=ExecutionStatus(str(data.get("status", ExecutionStatus.PENDING.value))),
            created_at=str(data.get("created_at", "")),
            started_at=str(data.get("started_at", "")),
            completed_at=str(data.get("completed_at", "")),
            error_code=str(data.get("error_code", "")),
            error_reason=str(data.get("error_reason", "")),
            prediction_count=(
                int(data["prediction_count"]) if data.get("prediction_count") is not None else None
            ),
            metadata=_copy_mapping(data.get("metadata")),
            result=_copy_result(data.get("result")),
        )


class ExecutionStore(Protocol):
    """Technology-neutral persistence port for forecast executions."""

    def create(self, record: ExecutionRecord) -> ExecutionRecord: ...

    def mark_started(self, execution_id: str) -> ExecutionRecord: ...

    def mark_success(
        self,
        execution_id: str,
        provenance: ExecutionProvenance,
        *,
        completed_at: str | None = None,
    ) -> ExecutionRecord: ...

    def mark_failed(
        self,
        execution_id: str,
        *,
        error_code: str,
        error_reason: str,
        provenance: ExecutionProvenance | None = None,
        completed_at: str | None = None,
    ) -> ExecutionRecord: ...

    def get(self, execution_id: str) -> ExecutionRecord | None: ...

    def get_by_reference_key(self, reference_key: str) -> ExecutionRecord | None: ...

    def list(
        self,
        *,
        reference_key: str | None = None,
        status: ExecutionStatus | None = None,
    ) -> tuple[ExecutionRecord, ...]: ...


@dataclass
class MemoryExecutionStore:
    """Thread-safe in-memory store used by unit and synthetic integration tests."""

    _records: dict[str, ExecutionRecord] = field(default_factory=dict)
    _lock: RLock = field(default_factory=RLock, init=False, repr=False)

    @staticmethod
    def _copy(record: ExecutionRecord) -> ExecutionRecord:
        return replace(
            record,
            metadata=_copy_mapping(record.metadata),
            result=_copy_result(record.result),
        )

    def _require(self, execution_id: str) -> ExecutionRecord:
        record = self._records.get(execution_id)
        if record is None:
            msg = f"Unknown execution_id: {execution_id!r}"
            raise ExecutionStoreError(msg)
        return record

    def create(self, record: ExecutionRecord) -> ExecutionRecord:
        with self._lock:
            if record.status is not ExecutionStatus.PENDING:
                msg = "ExecutionStore.create requires a PENDING record"
                raise InvalidExecutionTransitionError(msg)
            if record.execution_id in self._records:
                msg = f"Execution already exists: {record.execution_id!r}"
                raise ExecutionStoreError(msg)
            stored = self._copy(record)
            self._records[record.execution_id] = stored
            return self._copy(stored)

    def mark_started(self, execution_id: str) -> ExecutionRecord:
        with self._lock:
            record = self._require(execution_id)
            if record.status is not ExecutionStatus.PENDING:
                msg = (
                    f"Invalid transition {record.status.value} -> RUNNING " f"for {execution_id!r}"
                )
                raise InvalidExecutionTransitionError(msg)
            updated = replace(record, status=ExecutionStatus.RUNNING, started_at=_now())
            self._records[execution_id] = self._copy(updated)
            return self._copy(updated)

    def mark_success(
        self,
        execution_id: str,
        provenance: ExecutionProvenance,
        *,
        completed_at: str | None = None,
    ) -> ExecutionRecord:
        with self._lock:
            record = self._require(execution_id)
            if record.status is not ExecutionStatus.RUNNING:
                msg = (
                    f"Invalid transition {record.status.value} -> SUCCESS " f"for {execution_id!r}"
                )
                raise InvalidExecutionTransitionError(msg)
            metadata = _copy_mapping(record.metadata)
            metadata.update(_copy_mapping(provenance.metadata))
            updated = replace(
                record,
                model_id=provenance.model_id,
                model_version=provenance.model_version,
                algorithm=provenance.algorithm,
                feature_schema_version=provenance.feature_schema_version,
                feature_schema_fingerprint=provenance.feature_schema_fingerprint,
                artifact_uri=provenance.artifact_uri,
                prediction_count=provenance.prediction_count,
                status=ExecutionStatus.SUCCESS,
                completed_at=completed_at or _now(),
                error_code="",
                error_reason="",
                metadata=metadata,
                result=_copy_result(provenance.result),
            )
            self._records[execution_id] = self._copy(updated)
            return self._copy(updated)

    def mark_failed(
        self,
        execution_id: str,
        *,
        error_code: str,
        error_reason: str,
        provenance: ExecutionProvenance | None = None,
        completed_at: str | None = None,
    ) -> ExecutionRecord:
        with self._lock:
            record = self._require(execution_id)
            if record.status is not ExecutionStatus.RUNNING:
                msg = f"Invalid transition {record.status.value} -> FAILED " f"for {execution_id!r}"
                raise InvalidExecutionTransitionError(msg)
            if not error_code.strip():
                msg = "error_code is required"
                raise ExecutionStoreError(msg)
            metadata = _copy_mapping(record.metadata)
            metadata.update(_copy_mapping(provenance.metadata) if provenance else {})
            updated = replace(
                record,
                model_id=provenance.model_id if provenance else record.model_id,
                model_version=provenance.model_version if provenance else record.model_version,
                algorithm=provenance.algorithm if provenance else record.algorithm,
                feature_schema_version=(
                    provenance.feature_schema_version
                    if provenance
                    else record.feature_schema_version
                ),
                feature_schema_fingerprint=(
                    provenance.feature_schema_fingerprint
                    if provenance
                    else record.feature_schema_fingerprint
                ),
                artifact_uri=provenance.artifact_uri if provenance else record.artifact_uri,
                prediction_count=(
                    provenance.prediction_count if provenance else record.prediction_count
                ),
                status=ExecutionStatus.FAILED,
                completed_at=completed_at or _now(),
                error_code=error_code,
                error_reason=error_reason,
                metadata=metadata,
                result=None,
            )
            self._records[execution_id] = self._copy(updated)
            return self._copy(updated)

    def get(self, execution_id: str) -> ExecutionRecord | None:
        with self._lock:
            record = self._records.get(execution_id)
            return self._copy(record) if record is not None else None

    def get_by_reference_key(self, reference_key: str) -> ExecutionRecord | None:
        with self._lock:
            matches = [r for r in self._records.values() if r.reference_key == reference_key]
            return self._copy(matches[-1]) if matches else None

    def list(
        self,
        *,
        reference_key: str | None = None,
        status: ExecutionStatus | None = None,
    ) -> tuple[ExecutionRecord, ...]:
        with self._lock:
            records = list(self._records.values())
            if reference_key is not None:
                records = [r for r in records if r.reference_key == reference_key]
            if status is not None:
                records = [r for r in records if r.status is status]
            return tuple(self._copy(r) for r in records)


__all__ = [
    "ExecutionProvenance",
    "ExecutionRecord",
    "ExecutionStatus",
    "ExecutionStore",
    "ExecutionStoreError",
    "InvalidExecutionTransitionError",
    "MemoryExecutionStore",
]
