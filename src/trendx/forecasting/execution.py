"""W98/W100 — generic forecast execution records and injectable stores.

The contract is technology-neutral.  It records the request, the model
provenance actually used for serving, terminal status, and a serializable
result when available. ``MemoryExecutionStore`` remains useful for fast unit
fixtures; ``DurableExecutionStore`` provides an atomic, process-safe local
JSON datastore for durable history without a remote database dependency.
"""

from __future__ import annotations

import json
import math
import os
import stat
import tempfile
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from copy import deepcopy
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from enum import Enum
from fcntl import LOCK_EX, LOCK_UN, flock
from pathlib import Path
from threading import RLock
from typing import Any, Protocol, Self


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


class ExecutionStoreSerializationError(ExecutionStoreError):
    """Raised when durable execution data is not strict JSON."""


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

    def delete_if_unchanged(
        self,
        execution_id: str,
        expected: ExecutionRecord,
    ) -> bool:
        """Delete only the exact record observed by a lifecycle caller."""

        with self._lock:
            current = self._records.get(execution_id)
            if current is None or not _strict_payload_equal(current.to_dict(), expected.to_dict()):
                return False
            del self._records[execution_id]
            return True

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


_DURABLE_STRING_FIELDS = frozenset(
    {
        "execution_id",
        "reference_key",
        "tenant_id",
        "entity_type",
        "entity_id",
        "target_metric",
        "frequency",
        "model_id",
        "model_version",
        "algorithm",
        "feature_schema_version",
        "feature_schema_fingerprint",
        "artifact_uri",
        "created_at",
        "started_at",
        "completed_at",
        "error_code",
        "error_reason",
    }
)
_DURABLE_REQUIRED_FIELDS = _DURABLE_STRING_FIELDS | {
    "horizon",
    "status",
    "metadata",
    "result",
    "prediction_count",
}


def _validate_durable_record_shape(raw_record: Mapping[str, Any], execution_id: str) -> None:
    missing = _DURABLE_REQUIRED_FIELDS - raw_record.keys()
    if missing:
        names = ", ".join(sorted(missing))
        msg = f"Durable execution record {execution_id!r} is missing fields: {names}"
        raise ExecutionStoreSerializationError(msg)
    invalid_strings = [
        field_name
        for field_name in _DURABLE_STRING_FIELDS
        if not isinstance(raw_record[field_name], str)
    ]
    if invalid_strings:
        names = ", ".join(sorted(invalid_strings))
        msg = f"Durable execution record {execution_id!r} has non-text fields: {names}"
        raise ExecutionStoreSerializationError(msg)
    horizon = raw_record["horizon"]
    if isinstance(horizon, bool) or not isinstance(horizon, int):
        msg = f"Durable execution record {execution_id!r} has an invalid horizon"
        raise ExecutionStoreSerializationError(msg)
    if not isinstance(raw_record["status"], str):
        msg = f"Durable execution record {execution_id!r} has an invalid status"
        raise ExecutionStoreSerializationError(msg)
    prediction_count = raw_record["prediction_count"]
    if prediction_count is not None and (
        isinstance(prediction_count, bool) or not isinstance(prediction_count, int)
    ):
        msg = f"Durable execution record {execution_id!r} has an invalid prediction_count"
        raise ExecutionStoreSerializationError(msg)
    if not isinstance(raw_record["metadata"], dict) or not (
        raw_record["result"] is None or isinstance(raw_record["result"], dict)
    ):
        msg = f"Durable execution record {execution_id!r} has invalid metadata/result"
        raise ExecutionStoreSerializationError(msg)


def _strict_payload_equal(left: Mapping[str, Any], right: Mapping[str, Any]) -> bool:
    """Compare JSON payloads without Python's numeric/bool coercion rules."""

    try:
        left_json = json.dumps(
            left,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
        right_json = json.dumps(
            right,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
    except (TypeError, ValueError):
        return False
    return left_json == right_json


def _validate_json_value(value: Any, *, path: str = "value") -> None:
    if value is None or isinstance(value, str | bool | int):
        return
    if isinstance(value, float):
        if not math.isfinite(value):
            msg = f"{path} contains a non-finite number"
            raise ExecutionStoreSerializationError(msg)
        return
    if isinstance(value, list):
        for index, item in enumerate(value):
            _validate_json_value(item, path=f"{path}[{index}]")
        return
    if isinstance(value, dict):
        for key, item in value.items():
            if not isinstance(key, str):
                msg = f"{path} contains a non-string object key"
                raise ExecutionStoreSerializationError(msg)
            _validate_json_value(item, path=f"{path}.{key}")
        return
    msg = f"{path} contains an unsupported value type: {type(value).__name__}"
    raise ExecutionStoreSerializationError(msg)


class DurableExecutionStore:
    """Atomic, process-safe JSON implementation of the W98 ExecutionStore.

    The JSON file is the sole source of truth.  A sibling lock file and an
    atomic replace serialize writers across processes; readers never use an
    in-memory cache as authority.  The backend is intentionally local and
    provider-agnostic; it does not open a database or contact a remote service.
    """

    FORMAT_VERSION = 1

    def __init__(self, path: str | Path) -> None:
        self._path = Path(path)
        self._lock_path = self._path.with_name(f".{self._path.name}.lock")
        self._path.parent.mkdir(parents=True, exist_ok=True)
        with self._locked():
            if not self._path.exists():
                self._write_unlocked({})
            else:
                self._read_unlocked()

    @property
    def path(self) -> Path:
        return self._path

    def close(self) -> None:
        """Close the context.

        The implementation opens short-lived lock/data files per operation,
        so there is no long-lived connection to release.  The method exists
        to make process lifecycle explicit for callers and tests.
        """

    def __enter__(self) -> Self:
        return self

    def __exit__(self, exc_type: object, exc_value: object, traceback: object) -> None:
        self.close()

    @contextmanager
    def _locked(self) -> Iterator[None]:
        self._lock_path.parent.mkdir(parents=True, exist_ok=True)
        with self._lock_path.open("a+") as lock_handle:
            lock_mode = stat.S_IMODE(os.fstat(lock_handle.fileno()).st_mode)
            if lock_mode != 0o600:
                os.fchmod(lock_handle.fileno(), 0o600)
            flock(lock_handle.fileno(), LOCK_EX)
            try:
                yield
            finally:
                flock(lock_handle.fileno(), LOCK_UN)

    def _read_unlocked(self) -> dict[str, dict[str, Any]]:
        if not self._path.exists():
            return {}
        try:
            raw_text = self._path.read_text(encoding="utf-8")
        except OSError as exc:
            raise ExecutionStoreError("Unable to read durable execution store") from exc
        if not raw_text.strip():
            msg = "Durable execution store is empty or corrupted"
            raise ExecutionStoreSerializationError(msg)
        try:
            payload = json.loads(raw_text)
        except (TypeError, ValueError) as exc:
            msg = "Durable execution store contains invalid JSON"
            raise ExecutionStoreSerializationError(msg) from exc
        if not isinstance(payload, dict) or payload.get("format") != self.FORMAT_VERSION:
            msg = "Durable execution store has an unsupported format"
            raise ExecutionStoreSerializationError(msg)
        raw_records = payload.get("records")
        if not isinstance(raw_records, dict):
            msg = "Durable execution store records are malformed"
            raise ExecutionStoreSerializationError(msg)
        records: dict[str, dict[str, Any]] = {}
        for execution_id, raw_record in raw_records.items():
            if not isinstance(execution_id, str) or not isinstance(raw_record, dict):
                msg = "Durable execution store contains a malformed record"
                raise ExecutionStoreSerializationError(msg)
            _validate_json_value(raw_record, path=f"records.{execution_id}")
            _validate_durable_record_shape(raw_record, execution_id)
            try:
                record = ExecutionRecord.from_dict(raw_record)
            except (TypeError, ValueError) as exc:
                msg = f"Durable execution record is invalid: {execution_id!r}"
                raise ExecutionStoreSerializationError(msg) from exc
            if record.execution_id != execution_id:
                msg = f"Durable execution record key mismatch: {execution_id!r}"
                raise ExecutionStoreSerializationError(msg)
            records[execution_id] = raw_record
        return records

    def _write_unlocked(self, records: Mapping[str, Mapping[str, Any]]) -> None:
        payload = {"format": self.FORMAT_VERSION, "records": dict(records)}
        try:
            _validate_json_value(payload)
            encoded = json.dumps(
                payload,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            )
        except (TypeError, ValueError) as exc:
            msg = "ExecutionRecord contains data that cannot be persisted as strict JSON"
            raise ExecutionStoreSerializationError(msg) from exc

        temporary_path: str | None = None
        file_descriptor = -1
        try:
            file_descriptor, temporary_path = tempfile.mkstemp(
                prefix=f".{self._path.name}.",
                suffix=".tmp",
                dir=str(self._path.parent),
            )
            os.fchmod(file_descriptor, 0o600)
            with os.fdopen(file_descriptor, "w", encoding="utf-8") as handle:
                file_descriptor = -1
                handle.write(encoded)
                handle.write("\n")
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary_path, self._path)
            temporary_path = None
        except OSError as exc:
            raise ExecutionStoreError("Unable to write durable execution store") from exc
        finally:
            if file_descriptor >= 0:
                os.close(file_descriptor)
            if temporary_path is not None:
                try:
                    os.unlink(temporary_path)
                except FileNotFoundError:
                    pass

    @staticmethod
    def _record_from_raw(raw_record: Mapping[str, Any]) -> ExecutionRecord:
        try:
            return ExecutionRecord.from_dict(raw_record)
        except (TypeError, ValueError) as exc:
            msg = "Durable execution record cannot be decoded"
            raise ExecutionStoreSerializationError(msg) from exc

    def _require_unlocked(
        self, records: Mapping[str, dict[str, Any]], execution_id: str
    ) -> dict[str, Any]:
        raw_record = records.get(execution_id)
        if raw_record is None:
            msg = f"Unknown execution_id: {execution_id!r}"
            raise ExecutionStoreError(msg)
        return raw_record

    def create(self, record: ExecutionRecord) -> ExecutionRecord:
        if record.status is not ExecutionStatus.PENDING:
            msg = "ExecutionStore.create requires a PENDING record"
            raise InvalidExecutionTransitionError(msg)
        with self._locked():
            records = self._read_unlocked()
            if record.execution_id in records:
                msg = f"Execution already exists: {record.execution_id!r}"
                raise ExecutionStoreError(msg)
            for raw_record in records.values():
                existing = self._record_from_raw(raw_record)
                if existing.reference_key != record.reference_key:
                    continue
                if existing.status in (
                    ExecutionStatus.PENDING,
                    ExecutionStatus.RUNNING,
                    ExecutionStatus.SUCCESS,
                ):
                    msg = f"reference_key already has an active or successful execution: {record.reference_key!r}"
                    raise ExecutionStoreError(msg)
            records[record.execution_id] = record.to_dict()
            self._write_unlocked(records)
            return self._record_from_raw(records[record.execution_id])

    def mark_started(self, execution_id: str) -> ExecutionRecord:
        with self._locked():
            records = self._read_unlocked()
            record = self._record_from_raw(self._require_unlocked(records, execution_id))
            if record.status is not ExecutionStatus.PENDING:
                msg = (
                    f"Invalid transition {record.status.value} -> RUNNING " f"for {execution_id!r}"
                )
                raise InvalidExecutionTransitionError(msg)
            updated = replace(record, status=ExecutionStatus.RUNNING, started_at=_now())
            records[execution_id] = updated.to_dict()
            self._write_unlocked(records)
            return updated

    def mark_success(
        self,
        execution_id: str,
        provenance: ExecutionProvenance,
        *,
        completed_at: str | None = None,
    ) -> ExecutionRecord:
        provenance_data = provenance.to_dict()
        _validate_json_value(provenance_data, path="provenance")
        with self._locked():
            records = self._read_unlocked()
            record = self._record_from_raw(self._require_unlocked(records, execution_id))
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
            records[execution_id] = updated.to_dict()
            self._write_unlocked(records)
            return updated

    def mark_failed(
        self,
        execution_id: str,
        *,
        error_code: str,
        error_reason: str,
        provenance: ExecutionProvenance | None = None,
        completed_at: str | None = None,
    ) -> ExecutionRecord:
        if not error_code.strip():
            msg = "error_code is required"
            raise ExecutionStoreError(msg)
        provenance_data = provenance.to_dict() if provenance is not None else None
        if provenance_data is not None:
            _validate_json_value(provenance_data, path="provenance")
        with self._locked():
            records = self._read_unlocked()
            record = self._record_from_raw(self._require_unlocked(records, execution_id))
            if record.status is not ExecutionStatus.RUNNING:
                msg = f"Invalid transition {record.status.value} -> FAILED " f"for {execution_id!r}"
                raise InvalidExecutionTransitionError(msg)
            metadata = _copy_mapping(record.metadata)
            if provenance is not None:
                metadata.update(_copy_mapping(provenance.metadata))
            updated = replace(
                record,
                model_id=provenance.model_id if provenance is not None else record.model_id,
                model_version=provenance.model_version
                if provenance is not None
                else record.model_version,
                algorithm=provenance.algorithm if provenance is not None else record.algorithm,
                feature_schema_version=(
                    provenance.feature_schema_version
                    if provenance is not None
                    else record.feature_schema_version
                ),
                feature_schema_fingerprint=(
                    provenance.feature_schema_fingerprint
                    if provenance is not None
                    else record.feature_schema_fingerprint
                ),
                artifact_uri=provenance.artifact_uri
                if provenance is not None
                else record.artifact_uri,
                prediction_count=(
                    provenance.prediction_count
                    if provenance is not None
                    else record.prediction_count
                ),
                status=ExecutionStatus.FAILED,
                completed_at=completed_at or _now(),
                error_code=error_code,
                error_reason=error_reason,
                metadata=metadata,
                result=None,
            )
            records[execution_id] = updated.to_dict()
            self._write_unlocked(records)
            return updated

    def get(self, execution_id: str) -> ExecutionRecord | None:
        with self._locked():
            records = self._read_unlocked()
            raw_record = records.get(execution_id)
            return self._record_from_raw(raw_record) if raw_record is not None else None

    def delete_if_unchanged(
        self,
        execution_id: str,
        expected: ExecutionRecord,
    ) -> bool:
        """Atomically delete only the exact record observed by a lifecycle caller."""

        with self._locked():
            records = self._read_unlocked()
            current = records.get(execution_id)
            if current is None or not _strict_payload_equal(current, expected.to_dict()):
                return False
            del records[execution_id]
            self._write_unlocked(records)
            return True

    def get_by_reference_key(self, reference_key: str) -> ExecutionRecord | None:
        with self._locked():
            records = self._read_unlocked()
            matches = [
                self._record_from_raw(raw_record)
                for raw_record in records.values()
                if raw_record.get("reference_key") == reference_key
            ]
            return (
                max(matches, key=lambda item: (item.created_at, item.execution_id))
                if matches
                else None
            )

    def list(
        self,
        *,
        reference_key: str | None = None,
        status: ExecutionStatus | None = None,
    ) -> tuple[ExecutionRecord, ...]:
        with self._locked():
            records = self._read_unlocked()
            values = [self._record_from_raw(raw_record) for raw_record in records.values()]
            if reference_key is not None:
                values = [record for record in values if record.reference_key == reference_key]
            if status is not None:
                values = [record for record in values if record.status is status]
            return tuple(sorted(values, key=lambda item: (item.created_at, item.execution_id)))


__all__ = [
    "DurableExecutionStore",
    "ExecutionProvenance",
    "ExecutionRecord",
    "ExecutionStatus",
    "ExecutionStore",
    "ExecutionStoreError",
    "ExecutionStoreSerializationError",
    "InvalidExecutionTransitionError",
    "MemoryExecutionStore",
]
