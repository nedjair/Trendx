"""W110 — durable operational audit trail for execution history.

The audit trail is deliberately separate from :class:`ExecutionRecord` and
from the W99/W104/W106 business projections.  It records a small, typed and
sanitized event after an operation has reached a known outcome.  The default
store is a local, process-safe JSON-event directory; it does not provide a
database transaction and it never stores a record payload, credential or
filesystem path.

The service policy is best-effort by design.  A failed audit append is logged
with a bounded diagnostic event and never rolls back an otherwise completed
business operation.  This protects W107/W108 integrity while making audit
failures visible to operators.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
import tempfile
import uuid
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import Enum
from pathlib import Path
from threading import RLock
from typing import Any, Protocol, Self

from loguru import logger

AUDIT_EVENT_VERSION = "1"
EVENT_VERSION = AUDIT_EVENT_VERSION
MAX_AUDIT_QUERY_LIMIT = 1000
DEFAULT_AUDIT_QUERY_LIMIT = 100
MAX_AUDIT_OFFSET = 10_000_000

_SAFE_TEXT = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:@+\-]*$")
_SENSITIVE_MARKERS = (
    "authorization",
    "bearer",
    "password",
    "secret",
    "credential",
    "access_token",
    "refresh_token",
    "api_key",
    "apikey",
    "token",
)
_SHA256 = re.compile(r"^[0-9a-f]{64}$")


class AuditOperation(str, Enum):
    """Stable, deliberately small operation taxonomy."""

    EXECUTION = "EXECUTION"
    ARCHIVE = "ARCHIVE"
    PURGE = "PURGE"
    RESTORE = "RESTORE"
    RECOVERY_API = "RECOVERY_API"
    ARCHIVE_VERIFY = "ARCHIVE_VERIFY"
    ARCHIVE_CREATE = "ARCHIVE_CREATE"
    PURGE_PREVIEW = "PURGE_PREVIEW"
    PURGE_EXECUTE = "PURGE_EXECUTE"
    RESTORE_VERIFY = "RESTORE_VERIFY"
    RESTORE_EXECUTE = "RESTORE_EXECUTE"
    # W112: lifecycle of the audit trail itself.  They are ordinary W110 events
    # so they are recorded through the same service, store, sanitation, checksum
    # and idempotency contract as every other operation.
    AUDIT_LIFECYCLE_PREVIEW = "AUDIT_LIFECYCLE_PREVIEW"
    AUDIT_ARCHIVE = "AUDIT_ARCHIVE"
    AUDIT_ARCHIVE_VERIFY = "AUDIT_ARCHIVE_VERIFY"
    AUDIT_PURGE = "AUDIT_PURGE"
    AUDIT_RESTORE = "AUDIT_RESTORE"


class AuditOutcome(str, Enum):
    """Outcomes shared by W107-W109 operations."""

    SUCCESS = "SUCCESS"
    ALREADY_PRESENT = "ALREADY_PRESENT"
    CONFLICT = "CONFLICT"
    REJECTED = "REJECTED"
    FAILED = "FAILED"
    FORBIDDEN = "FORBIDDEN"


class AuditActorType(str, Enum):
    """The source class of an audited operation."""

    SYSTEM = "SYSTEM"
    API = "API"
    OPERATOR = "OPERATOR"
    TEST = "TEST"


class AuditFailurePolicy(str, Enum):
    """Explicit consequence of an unavailable audit backend."""

    BEST_EFFORT = "BEST_EFFORT"
    MANDATORY = "MANDATORY"


class AuditError(Exception):
    """Base class for W110 audit errors."""


class AuditValidationError(AuditError, ValueError):
    """An event or query violates the bounded audit contract."""


class AuditStoreError(AuditError):
    """The audit backend cannot safely complete an operation."""


class AuditIntegrityError(AuditStoreError):
    """A persisted audit event failed checksum or shape validation."""


class AuditSerializationError(AuditIntegrityError):
    """A persisted audit event is not strict canonical JSON."""


class AuditEventVersionError(AuditIntegrityError):
    """A persisted event uses an unsupported W110 version."""


# W111 refinement: each integrity phase raises a distinct subtype so a read-only
# control plane can classify a failure without re-reading the document.  All of
# them remain ``AuditIntegrityError`` subtypes, so the W110 contract is intact.
class AuditEventShapeError(AuditIntegrityError):
    """A persisted event does not match the strict W110 document shape."""


class AuditChecksumError(AuditIntegrityError):
    """A persisted event checksum is missing or does not match its content."""


class AuditCanonicalizationError(AuditIntegrityError):
    """A persisted event is not stored as strict canonical JSON."""


class AuditDocumentIdentityError(AuditIntegrityError):
    """A persisted event document does not match its hashed identity."""


class AuditDocumentPermissionError(AuditIntegrityError):
    """A persisted event document or its parent store is not safe to trust."""


# Stable, non-sensitive failure codes returned by the W111 read-only scan.
AUDIT_FAILURE_SCHEMA = "SCHEMA"
AUDIT_FAILURE_VERSION = "VERSION"
AUDIT_FAILURE_CHECKSUM = "CHECKSUM"
AUDIT_FAILURE_CANONICAL = "CANONICALIZATION"
AUDIT_FAILURE_IDENTITY = "IDENTITY"
AUDIT_FAILURE_MALFORMED_JSON = "MALFORMED_JSON"
AUDIT_FAILURE_UNSAFE_DOCUMENT = "UNSAFE_DOCUMENT"
AUDIT_FAILURE_UNREADABLE = "UNREADABLE"


class AuditIdempotencyConflictError(AuditStoreError):
    """An idempotency key was reused with a different event payload."""


class AuditDeleteOutcome(str, Enum):
    """W112 compare-and-delete outcome.

    ``CONFLICT`` and ``INVALID`` are fail-closed: the caller must not treat them
    as a successful removal.
    """

    DELETED = "DELETED"
    CONFLICT = "CONFLICT"
    NOT_FOUND = "NOT_FOUND"
    INVALID = "INVALID"


@dataclass(frozen=True)
class AuditDeleteResult:
    """Result of a W112 ``delete_if_unchanged`` call."""

    event_id: str
    tenant_id: str
    outcome: AuditDeleteOutcome

    @property
    def deleted(self) -> bool:
        return self.outcome is AuditDeleteOutcome.DELETED

    def to_dict(self) -> dict[str, Any]:
        return {
            "event_id": self.event_id,
            "tenant_id": self.tenant_id,
            "outcome": self.outcome.value,
        }


# Compatibility spelling for callers that used the initial W110 draft name.
AuditIdempotencyConflict = AuditIdempotencyConflictError


class AuditQueryError(AuditValidationError):
    """The query is invalid or exceeds a hard bound."""


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _normalize_datetime(value: datetime | str) -> datetime:
    if isinstance(value, str):
        raw = value.strip()
        if raw.endswith("Z"):
            raw = f"{raw[:-1]}+00:00"
        try:
            parsed = datetime.fromisoformat(raw)
        except ValueError as exc:
            raise AuditValidationError("audit timestamp is invalid") from exc
    elif isinstance(value, datetime):
        parsed = value
    else:
        raise AuditValidationError("audit timestamp is invalid")
    return parsed.replace(tzinfo=UTC) if parsed.tzinfo is None else parsed.astimezone(UTC)


def _format_datetime(value: datetime) -> str:
    return _normalize_datetime(value).isoformat()


def _validate_text(
    value: str | None,
    field_name: str,
    *,
    max_length: int,
    optional: bool = False,
) -> str | None:
    if optional and value is None:
        return None
    if not isinstance(value, str):
        raise AuditValidationError(f"{field_name} must be text")
    if not value or len(value) > max_length or value != value.strip():
        raise AuditValidationError(f"{field_name} is not bounded")
    if any(ord(character) < 32 or ord(character) == 127 for character in value):
        raise AuditValidationError(f"{field_name} contains control characters")
    if not _SAFE_TEXT.fullmatch(value) or ".." in value:
        raise AuditValidationError(f"{field_name} contains an unsafe value")
    lowered = value.casefold()
    if any(marker in lowered for marker in _SENSITIVE_MARKERS):
        raise AuditValidationError(f"{field_name} contains a sensitive marker")
    return value


def _validate_sha256(value: str | None, field_name: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or not _SHA256.fullmatch(value):
        raise AuditValidationError(f"{field_name} is invalid")
    return value


def _coerce_operation(value: AuditOperation | str) -> AuditOperation:
    try:
        return value if isinstance(value, AuditOperation) else AuditOperation(str(value).upper())
    except (TypeError, ValueError) as exc:
        raise AuditValidationError("audit operation is unsupported") from exc


def _coerce_outcome(value: AuditOutcome | str) -> AuditOutcome:
    try:
        return value if isinstance(value, AuditOutcome) else AuditOutcome(str(value).upper())
    except (TypeError, ValueError) as exc:
        raise AuditValidationError("audit outcome is unsupported") from exc


def _coerce_actor(value: AuditActorType | str) -> AuditActorType:
    try:
        return value if isinstance(value, AuditActorType) else AuditActorType(str(value).upper())
    except (TypeError, ValueError) as exc:
        raise AuditValidationError("audit actor type is unsupported") from exc


def _canonical_json(value: Mapping[str, Any]) -> bytes:
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError, UnicodeError) as exc:
        raise AuditSerializationError("audit payload is not strict JSON") from exc


def _reject_json_constant(value: str) -> None:
    raise ValueError(f"unsupported JSON constant: {value}")


def _object_pairs_without_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON object key")
        result[key] = value
    return result


def _strict_json_loads(value: str) -> Any:
    try:
        return json.loads(
            value,
            object_pairs_hook=_object_pairs_without_duplicates,
            parse_constant=_reject_json_constant,
        )
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise AuditSerializationError("audit event contains invalid JSON") from exc


def _event_id_for_key(key: str) -> str:
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"trendx:w110:audit:{key}"))


def _bounded_or_redacted(
    value: str | None,
    field_name: str,
    *,
    max_length: int,
    optional: bool = False,
) -> str | None:
    """Keep untrusted free-form values out of the event document."""

    if optional and value is None:
        return None
    try:
        return _validate_text(value, field_name, max_length=max_length, optional=optional)
    except AuditValidationError:
        digest = hashlib.sha256(str(value).encode("utf-8", errors="replace")).hexdigest()[:24]
        return f"redacted-{digest}"


def _default_idempotency_key(
    operation: AuditOperation,
    tenant_id: str,
    execution_id: str | None,
    request_id: str,
) -> str:
    material = "\x1f".join((operation.value, tenant_id, execution_id or "", request_id))
    return hashlib.sha256(material.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class ExecutionAuditEvent:
    """Versioned, minimal operational event contract.

    No field is intended for a record payload, request body, credential or
    path.  ``execution_id`` and ``reference_key`` are nullable for pre-record
    failures such as a missing archive or an unavailable backend.
    """

    event_id: str
    occurred_at: datetime
    operation: AuditOperation
    outcome: AuditOutcome
    tenant_id: str
    execution_id: str | None
    reference_key: str | None
    actor_type: AuditActorType
    actor_id: str
    request_id: str
    source: str
    reason_code: str
    event_version: str = AUDIT_EVENT_VERSION
    idempotency_key: str | None = None
    checksum: str | None = None

    def __post_init__(self) -> None:
        event_id = _validate_text(self.event_id, "event_id", max_length=128)
        tenant_id = _validate_text(self.tenant_id, "tenant_id", max_length=256)
        execution_id = _validate_text(
            self.execution_id,
            "execution_id",
            max_length=512,
            optional=True,
        )
        reference_key = _validate_text(
            self.reference_key,
            "reference_key",
            max_length=512,
            optional=True,
        )
        actor_id = _validate_text(self.actor_id, "actor_id", max_length=128)
        request_id = _validate_text(self.request_id, "request_id", max_length=128)
        source = _validate_text(self.source, "source", max_length=128)
        reason_code = _validate_text(self.reason_code, "reason_code", max_length=128)
        if event_id is None or tenant_id is None or actor_id is None or request_id is None:
            raise AuditValidationError("required audit identity is missing")
        if self.event_version != AUDIT_EVENT_VERSION:
            raise AuditEventVersionError("unsupported audit event version")
        operation = _coerce_operation(self.operation)
        outcome = _coerce_outcome(self.outcome)
        actor_type = _coerce_actor(self.actor_type)
        occurred_at = _normalize_datetime(self.occurred_at)
        checksum = _validate_sha256(self.checksum, "checksum")
        idempotency_key = _validate_text(
            self.idempotency_key,
            "idempotency_key",
            max_length=256,
            optional=True,
        ) or _default_idempotency_key(operation, tenant_id, execution_id, request_id)
        object.__setattr__(self, "event_id", event_id)
        object.__setattr__(self, "occurred_at", occurred_at)
        object.__setattr__(self, "operation", operation)
        object.__setattr__(self, "outcome", outcome)
        object.__setattr__(self, "tenant_id", tenant_id)
        object.__setattr__(self, "execution_id", execution_id)
        object.__setattr__(self, "reference_key", reference_key)
        object.__setattr__(self, "actor_type", actor_type)
        object.__setattr__(self, "actor_id", actor_id)
        object.__setattr__(self, "request_id", request_id)
        object.__setattr__(self, "source", source)
        object.__setattr__(self, "reason_code", reason_code)
        object.__setattr__(self, "event_version", AUDIT_EVENT_VERSION)
        object.__setattr__(self, "idempotency_key", idempotency_key)
        object.__setattr__(self, "checksum", checksum)

    @property
    def calculated_checksum(self) -> str:
        return hashlib.sha256(self.canonical_payload()).hexdigest()

    def canonical_payload(self) -> bytes:
        data = self.to_dict()
        data.pop("checksum", None)
        return _canonical_json(data)

    def with_checksum(self) -> ExecutionAuditEvent:
        checksum = self.calculated_checksum
        return ExecutionAuditEvent(
            event_id=self.event_id,
            occurred_at=self.occurred_at,
            operation=self.operation,
            outcome=self.outcome,
            tenant_id=self.tenant_id,
            execution_id=self.execution_id,
            reference_key=self.reference_key,
            actor_type=self.actor_type,
            actor_id=self.actor_id,
            request_id=self.request_id,
            source=self.source,
            reason_code=self.reason_code,
            event_version=self.event_version,
            idempotency_key=self.idempotency_key,
            checksum=checksum,
        )

    def verify_integrity(self) -> None:
        if self.checksum is None:
            raise AuditChecksumError("audit event checksum is missing")
        if self.calculated_checksum != self.checksum:
            raise AuditChecksumError("audit event checksum mismatch")

    def to_dict(self) -> dict[str, Any]:
        return {
            "event_id": self.event_id,
            "event_version": self.event_version,
            "occurred_at": _format_datetime(self.occurred_at),
            "operation": self.operation.value,
            "outcome": self.outcome.value,
            "tenant_id": self.tenant_id,
            "execution_id": self.execution_id,
            "reference_key": self.reference_key,
            "actor_type": self.actor_type.value,
            "actor_id": self.actor_id,
            "request_id": self.request_id,
            "source": self.source,
            "reason_code": self.reason_code,
            "idempotency_key": self.idempotency_key,
            "checksum": self.checksum,
        }

    def model_dump(self) -> dict[str, Any]:
        """Pydantic-style compatibility for callers using typed DTOs."""

        return self.to_dict()

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> ExecutionAuditEvent:
        expected = {
            "event_id",
            "event_version",
            "occurred_at",
            "operation",
            "outcome",
            "tenant_id",
            "execution_id",
            "reference_key",
            "actor_type",
            "actor_id",
            "request_id",
            "source",
            "reason_code",
            "idempotency_key",
            "checksum",
        }
        if not isinstance(data, Mapping) or set(data) != expected:
            raise AuditEventShapeError("audit event shape is invalid")
        if data.get("event_version") != AUDIT_EVENT_VERSION:
            raise AuditEventVersionError("unsupported audit event version")
        required_text = (
            "event_id",
            "occurred_at",
            "operation",
            "outcome",
            "tenant_id",
            "actor_type",
            "actor_id",
            "request_id",
            "source",
            "reason_code",
            "event_version",
        )
        if any(not isinstance(data.get(name), str) for name in required_text):
            raise AuditEventShapeError("audit event fields are invalid")
        if any(
            data.get(name) is not None and not isinstance(data.get(name), str)
            for name in ("execution_id", "reference_key", "idempotency_key", "checksum")
        ):
            raise AuditEventShapeError("audit event fields are invalid")
        try:
            event = cls(
                event_id=data["event_id"],
                occurred_at=_normalize_datetime(data["occurred_at"]),
                operation=_coerce_operation(data["operation"]),
                outcome=_coerce_outcome(data["outcome"]),
                tenant_id=data["tenant_id"],
                execution_id=data["execution_id"],
                reference_key=data["reference_key"],
                actor_type=_coerce_actor(data["actor_type"]),
                actor_id=data["actor_id"],
                request_id=data["request_id"],
                source=data["source"],
                reason_code=data["reason_code"],
                event_version=data["event_version"],
                idempotency_key=data["idempotency_key"],
                checksum=data["checksum"],
            )
        except AuditEventVersionError:
            raise
        except (AuditError, TypeError, ValueError) as exc:
            raise AuditEventShapeError("audit event fields are invalid") from exc
        if event.checksum is None:
            raise AuditChecksumError("audit event checksum is missing")
        event.verify_integrity()
        return event


@dataclass(frozen=True)
class ExecutionAuditQuery:
    """Bounded, deterministic filters for audit reads."""

    tenant_id: str | None = None
    event_id: str | None = None
    operation: AuditOperation | None = None
    outcome: AuditOutcome | None = None
    execution_id: str | None = None
    reference_key: str | None = None
    occurred_at_from: datetime | None = None
    occurred_at_to: datetime | None = None
    request_id: str | None = None
    actor_type: AuditActorType | None = None
    source: str | None = None
    reason_code: str | None = None
    limit: int = DEFAULT_AUDIT_QUERY_LIMIT
    offset: int = 0

    def __post_init__(self) -> None:
        tenant_id = _validate_text(self.tenant_id, "tenant_id", max_length=256, optional=True)
        event_id = _validate_text(self.event_id, "event_id", max_length=128, optional=True)
        execution_id = _validate_text(
            self.execution_id, "execution_id", max_length=512, optional=True
        )
        reference_key = _validate_text(
            self.reference_key, "reference_key", max_length=512, optional=True
        )
        request_id = _validate_text(self.request_id, "request_id", max_length=128, optional=True)
        source = _validate_text(self.source, "source", max_length=128, optional=True)
        reason_code = _validate_text(self.reason_code, "reason_code", max_length=128, optional=True)
        operation = None if self.operation is None else _coerce_operation(self.operation)
        outcome = None if self.outcome is None else _coerce_outcome(self.outcome)
        actor_type = None if self.actor_type is None else _coerce_actor(self.actor_type)
        occurred_from = (
            None if self.occurred_at_from is None else _normalize_datetime(self.occurred_at_from)
        )
        occurred_to = (
            None if self.occurred_at_to is None else _normalize_datetime(self.occurred_at_to)
        )
        if occurred_from is not None and occurred_to is not None and occurred_from > occurred_to:
            raise AuditQueryError("occurred_at_from must not be after occurred_at_to")
        if isinstance(self.limit, bool) or not isinstance(self.limit, int):
            raise AuditQueryError("limit must be an integer")
        if self.limit < 1 or self.limit > MAX_AUDIT_QUERY_LIMIT:
            raise AuditQueryError("limit is outside the allowed range")
        if isinstance(self.offset, bool) or not isinstance(self.offset, int):
            raise AuditQueryError("offset must be an integer")
        if self.offset < 0 or self.offset > MAX_AUDIT_OFFSET:
            raise AuditQueryError("offset is outside the allowed range")
        for name, value in (
            ("tenant_id", tenant_id),
            ("event_id", event_id),
            ("execution_id", execution_id),
            ("reference_key", reference_key),
            ("request_id", request_id),
            ("source", source),
            ("reason_code", reason_code),
        ):
            object.__setattr__(self, name, value)
        object.__setattr__(self, "operation", operation)
        object.__setattr__(self, "outcome", outcome)
        object.__setattr__(self, "actor_type", actor_type)
        object.__setattr__(self, "occurred_at_from", occurred_from)
        object.__setattr__(self, "occurred_at_to", occurred_to)

    def matches(self, event: ExecutionAuditEvent) -> bool:
        checks: tuple[tuple[object | None, object], ...] = (
            (self.tenant_id, event.tenant_id),
            (self.event_id, event.event_id),
            (self.operation, event.operation),
            (self.outcome, event.outcome),
            (self.execution_id, event.execution_id),
            (self.reference_key, event.reference_key),
            (self.request_id, event.request_id),
            (self.actor_type, event.actor_type),
            (self.source, event.source),
            (self.reason_code, event.reason_code),
        )
        if any(expected is not None and expected != actual for expected, actual in checks):
            return False
        if self.occurred_at_from is not None and event.occurred_at < self.occurred_at_from:
            return False
        if self.occurred_at_to is not None and event.occurred_at > self.occurred_at_to:
            return False
        return True

    def to_dict(self) -> dict[str, Any]:
        return {
            "tenant_id": self.tenant_id,
            "event_id": self.event_id,
            "operation": self.operation.value if self.operation else None,
            "outcome": self.outcome.value if self.outcome else None,
            "execution_id": self.execution_id,
            "reference_key": self.reference_key,
            "occurred_at_from": (
                _format_datetime(self.occurred_at_from) if self.occurred_at_from else None
            ),
            "occurred_at_to": _format_datetime(self.occurred_at_to)
            if self.occurred_at_to
            else None,
            "request_id": self.request_id,
            "actor_type": self.actor_type.value if self.actor_type else None,
            "source": self.source,
            "reason_code": self.reason_code,
            "limit": self.limit,
            "offset": self.offset,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> ExecutionAuditQuery:
        allowed = {
            "tenant_id",
            "event_id",
            "operation",
            "outcome",
            "execution_id",
            "reference_key",
            "occurred_at_from",
            "occurred_at_to",
            "request_id",
            "actor_type",
            "source",
            "reason_code",
            "limit",
            "offset",
        }
        if not isinstance(data, Mapping) or set(data) - allowed:
            raise AuditQueryError("audit query fields are invalid")
        try:
            return cls(
                tenant_id=data.get("tenant_id"),
                event_id=data.get("event_id"),
                operation=data.get("operation"),
                outcome=data.get("outcome"),
                execution_id=data.get("execution_id"),
                reference_key=data.get("reference_key"),
                occurred_at_from=data.get("occurred_at_from"),
                occurred_at_to=data.get("occurred_at_to"),
                request_id=data.get("request_id"),
                actor_type=data.get("actor_type"),
                source=data.get("source"),
                reason_code=data.get("reason_code"),
                limit=data.get("limit", DEFAULT_AUDIT_QUERY_LIMIT),
                offset=data.get("offset", 0),
            )
        except (TypeError, ValueError) as exc:
            raise AuditQueryError("audit query fields are invalid") from exc


AuditQueryInput = ExecutionAuditQuery | Mapping[str, Any] | None


def _coerce_query(query: AuditQueryInput) -> ExecutionAuditQuery:
    if query is None:
        return ExecutionAuditQuery()
    if isinstance(query, ExecutionAuditQuery):
        return query
    if isinstance(query, Mapping):
        return ExecutionAuditQuery.from_dict(query)
    raise AuditQueryError("audit query is invalid")


def _select_query(
    query: AuditQueryInput,
    criteria: AuditQueryInput,
) -> AuditQueryInput:
    if criteria is None:
        return query
    if query is not None:
        raise AuditQueryError("query and criteria are mutually exclusive")
    return criteria


@dataclass(frozen=True)
class ExecutionAuditPage:
    """Deterministically ordered page returned by the store."""

    events: tuple[ExecutionAuditEvent, ...]
    total: int
    limit: int
    offset: int

    @property
    def items(self) -> tuple[ExecutionAuditEvent, ...]:
        return self.events

    @property
    def has_more(self) -> bool:
        return self.offset + len(self.events) < self.total

    def __len__(self) -> int:
        return len(self.events)

    def __iter__(self) -> Iterator[ExecutionAuditEvent]:
        return iter(self.events)

    def __getitem__(self, key: str) -> Any:
        data = self.to_dict()
        if key not in data:
            raise KeyError(key)
        return data[key]

    def to_dict(self) -> dict[str, Any]:
        return {
            "items": [event.to_dict() for event in self.events],
            "total": self.total,
            "limit": self.limit,
            "offset": self.offset,
            "has_more": self.has_more,
        }


@dataclass(frozen=True)
class ExecutionAuditStatistics:
    """Tenant-scoped operational counters, independent of W104 analytics."""

    total_events: int
    successful_events: int
    failed_events: int
    rejected_events: int
    conflicts: int
    forbidden: int
    by_operation: dict[str, int]
    by_outcome: dict[str, int]
    by_tenant: dict[str, int]

    @property
    def success_count(self) -> int:
        return self.successful_events

    @property
    def failure_count(self) -> int:
        return self.failed_events

    def __getitem__(self, key: str) -> Any:
        data = self.to_dict()
        if key not in data:
            raise KeyError(key)
        return data[key]

    def to_dict(self) -> dict[str, Any]:
        return {
            "total_events": self.total_events,
            "successful_events": self.successful_events,
            "failed_events": self.failed_events,
            "rejected_events": self.rejected_events,
            "conflicts": self.conflicts,
            "forbidden": self.forbidden,
            "by_operation": dict(sorted(self.by_operation.items())),
            "by_outcome": dict(sorted(self.by_outcome.items())),
            "by_tenant": dict(sorted(self.by_tenant.items())),
        }


@dataclass(frozen=True)
class AuditDocumentRef:
    """Read-only verdict for one persisted audit document.

    ``document_ref`` is a truncated SHA-256 of the internal document name, so
    a consumer can correlate two independent scans without ever receiving a
    filesystem path or an internal file name.  ``event`` is populated only when
    the document is fully valid.
    """

    document_ref: str
    event: ExecutionAuditEvent | None
    tenant_id: str | None
    failure_code: str | None = None
    detail: str | None = None

    @property
    def is_valid(self) -> bool:
        return self.event is not None

    def to_dict(self) -> dict[str, Any]:
        return {
            "document_ref": self.document_ref,
            "event_id": self.event.event_id if self.event is not None else None,
            "tenant_id": self.tenant_id,
            "valid": self.is_valid,
            "failure_code": self.failure_code,
            "detail": self.detail,
        }


@dataclass(frozen=True)
class AuditDocumentScan:
    """Immutable result of a read-only integrity scan."""

    documents: tuple[AuditDocumentRef, ...]

    def __len__(self) -> int:
        return len(self.documents)

    def __iter__(self) -> Iterator[AuditDocumentRef]:
        return iter(self.documents)

    @property
    def valid_documents(self) -> tuple[AuditDocumentRef, ...]:
        return tuple(document for document in self.documents if document.is_valid)

    @property
    def invalid_documents(self) -> tuple[AuditDocumentRef, ...]:
        return tuple(document for document in self.documents if not document.is_valid)

    @property
    def events(self) -> tuple[ExecutionAuditEvent, ...]:
        return tuple(document.event for document in self.documents if document.event is not None)

    def to_dict(self) -> dict[str, Any]:
        return {
            "documents": [document.to_dict() for document in self.documents],
            "documents_scanned": len(self.documents),
            "valid_documents": len(self.valid_documents),
            "invalid_documents": len(self.invalid_documents),
        }


class AuditStore(Protocol):
    """Persistence port required by the audit service."""

    def append(self, event: ExecutionAuditEvent) -> ExecutionAuditEvent: ...

    def delete_if_unchanged(self, expected: ExecutionAuditEvent) -> AuditDeleteResult: ...

    def get(self, event_id: str, *, tenant_id: str | None = None) -> ExecutionAuditEvent | None: ...

    def query(
        self,
        query: AuditQueryInput = None,
        *,
        criteria: AuditQueryInput = None,
    ) -> ExecutionAuditPage: ...

    def statistics(
        self,
        query: AuditQueryInput = None,
        *,
        criteria: AuditQueryInput = None,
    ) -> ExecutionAuditStatistics: ...

    def scan_documents(self) -> AuditDocumentScan: ...


class ExecutionAuditStore:
    """Process-safe local JSON event store.

    Each event is an individual strict JSON document.  A sibling lock file
    serializes writers across processes, and temporary-file + ``os.replace``
    makes each append atomic.  The lock file is coordination metadata, not
    business data, and is never returned by a query.
    """

    FORMAT_VERSION = 1

    def __init__(self, path: str | Path, *, create_if_missing: bool = True) -> None:
        self._path = Path(path)
        self._create_if_missing = create_if_missing
        self._thread_lock = RLock()
        self._event_cache: dict[str, ExecutionAuditEvent] | None = None
        self._event_signatures: dict[str, tuple[int, int, int, int, int]] = {}
        if self._path.is_symlink():
            raise AuditStoreError("audit store path must not be a symlink")
        if self._path.exists():
            if not self._path.is_dir():
                raise AuditStoreError("audit store path is not a directory")
        elif create_if_missing:
            self._ensure_root()
        else:
            raise AuditStoreError("audit store is unavailable")
        self._lock_path = self._path.with_name(f".{self._path.name}.lock")
        if self._path.exists() and self._path.is_dir():
            self._check_root()
            if self._lock_path.is_symlink():
                raise AuditStoreError("audit lock path must not be a symlink")
            try:
                with self._lock_path.open("a+") as lock_handle:
                    os.fchmod(lock_handle.fileno(), 0o600)
            except OSError as exc:
                raise AuditStoreError("audit lock is unavailable") from exc

    @property
    def path(self) -> Path:
        return self._path

    def close(self) -> None:
        """Close the short-lived file handles used by this store."""

    def __enter__(self) -> Self:
        return self

    def __exit__(self, exc_type: object, exc_value: object, traceback: object) -> None:
        self.close()

    def _ensure_root(self) -> None:
        if self._path.is_symlink():
            raise AuditStoreError("audit store path must not be a symlink")
        try:
            self._path.mkdir(parents=True, exist_ok=True, mode=0o700)
            if not self._path.is_dir():
                raise AuditStoreError("audit store path is not a directory")
            os.chmod(self._path, 0o700)
            metadata = self._path.stat()
        except AuditStoreError:
            raise
        except OSError as exc:
            raise AuditStoreError("audit store directory is unavailable") from exc
        if metadata.st_uid != os.geteuid() or stat.S_IMODE(metadata.st_mode) & 0o077:
            raise AuditDocumentPermissionError("audit store directory permissions are insecure")

    def _check_root(self) -> None:
        if not self._path.exists() or self._path.is_symlink() or not self._path.is_dir():
            raise AuditStoreError("audit store is unavailable")
        try:
            metadata = self._path.stat()
        except OSError as exc:
            raise AuditStoreError("audit store directory is unavailable") from exc
        if metadata.st_uid != os.geteuid() or stat.S_IMODE(metadata.st_mode) & 0o077:
            raise AuditDocumentPermissionError("audit store directory permissions are insecure")

    def _require_root(self) -> None:
        self._check_root()

    @contextmanager
    def _locked(self) -> Iterator[None]:
        self._require_root()
        with self._thread_lock:
            if self._lock_path.is_symlink():
                raise AuditStoreError("audit lock path must not be a symlink")
            try:
                handle = self._lock_path.open("a+")
            except OSError as exc:
                raise AuditStoreError("audit lock is unavailable") from exc
            try:
                os.fchmod(handle.fileno(), 0o600)
                import fcntl

                fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
                yield
            finally:
                try:
                    import fcntl

                    fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
                finally:
                    handle.close()

    @staticmethod
    def _filename(event_id: str) -> str:
        return hashlib.sha256(event_id.encode("utf-8")).hexdigest() + ".json"

    def _event_path(self, event_id: str) -> Path:
        return self._path / self._filename(event_id)

    def _event_paths(self) -> tuple[Path, ...]:
        try:
            paths = tuple(sorted(self._path.glob("*.json")))
        except OSError as exc:
            raise AuditStoreError("audit store is unavailable") from exc
        for path in paths:
            if path.is_symlink():
                raise AuditDocumentPermissionError("audit event file must not be a symlink")
        return paths

    def _read_event_path(self, path: Path) -> ExecutionAuditEvent:
        if path.is_symlink():
            raise AuditDocumentPermissionError("audit event file must not be a symlink")
        try:
            metadata = path.stat()
        except OSError as exc:
            raise AuditStoreError("audit event cannot be read") from exc
        if metadata.st_uid != os.geteuid() or stat.S_IMODE(metadata.st_mode) & 0o077:
            raise AuditDocumentPermissionError("audit event permissions are insecure")
        try:
            raw = path.read_text(encoding="utf-8")
        except (OSError, UnicodeError) as exc:
            raise AuditStoreError("audit event cannot be read") from exc
        payload = _strict_json_loads(raw)
        if not isinstance(payload, Mapping):
            raise AuditEventShapeError("audit event document is malformed")
        event = ExecutionAuditEvent.from_dict(payload)
        if path.name != self._filename(event.event_id):
            raise AuditDocumentIdentityError("audit event filename does not match identity")
        canonical = _canonical_json(payload).decode("utf-8")
        if raw != f"{canonical}\n":
            raise AuditCanonicalizationError("audit event is not canonical")
        return event

    @staticmethod
    def _signature(path: Path) -> tuple[int, int, int, int, int]:
        try:
            metadata = path.stat()
        except OSError as exc:
            raise AuditStoreError("audit event cannot be inspected") from exc
        return (
            metadata.st_mtime_ns,
            metadata.st_size,
            stat.S_IMODE(metadata.st_mode),
            metadata.st_uid,
            metadata.st_ino,
        )

    def _refresh_event_cache_unlocked(
        self, *, force_verify: bool = False
    ) -> dict[str, ExecutionAuditEvent]:
        paths = self._event_paths()
        current = {path.name: self._signature(path) for path in paths}
        cache = self._event_cache
        if cache is None or force_verify:
            cache = {}
            for path in paths:
                event = self._read_event_path(path)
                cache[event.event_id] = event
            self._event_cache = cache
            self._event_signatures = current
            return cache
        current_names = set(current)
        known_names = set(self._event_signatures)
        changed_existing = any(
            name in self._event_signatures and self._event_signatures[name] != signature
            for name, signature in current.items()
        )
        if known_names - current_names or changed_existing:
            cache = {}
            for path in paths:
                event = self._read_event_path(path)
                cache[event.event_id] = event
            self._event_cache = cache
            self._event_signatures = current
            return cache
        for path in paths:
            if path.name not in self._event_signatures:
                event = self._read_event_path(path)
                cache[event.event_id] = event
        self._event_signatures = current
        return cache

    def _read_all_unlocked(self, *, force_verify: bool = False) -> tuple[ExecutionAuditEvent, ...]:
        events = tuple(self._refresh_event_cache_unlocked(force_verify=force_verify).values())
        return tuple(
            sorted(events, key=lambda event: (event.occurred_at, event.event_id), reverse=True)
        )

    @staticmethod
    def _same_event(left: ExecutionAuditEvent, right: ExecutionAuditEvent) -> bool:
        left_data = left.to_dict()
        right_data = right.to_dict()
        for key in ("event_id", "occurred_at", "checksum"):
            left_data.pop(key, None)
            right_data.pop(key, None)
        return _canonical_json(left_data) == _canonical_json(right_data)

    def append(self, event: ExecutionAuditEvent) -> ExecutionAuditEvent:
        if not isinstance(event, ExecutionAuditEvent):
            raise AuditValidationError("audit event is required")
        if event.checksum is not None:
            event.verify_integrity()
        persisted = event.with_checksum()
        with self._locked():
            for existing in self._read_all_unlocked():
                if existing.event_id == persisted.event_id or (
                    persisted.idempotency_key is not None
                    and existing.idempotency_key == persisted.idempotency_key
                ):
                    if self._same_event(existing, persisted):
                        return existing
                    raise AuditIdempotencyConflictError("audit idempotency key was reused")
            path = self._event_path(persisted.event_id)
            if path.exists():
                raise AuditIdempotencyConflictError("audit event identity already exists")
            self._atomic_write(path, persisted)
            if self._event_cache is None:
                self._event_cache = {}
            self._event_cache[persisted.event_id] = persisted
            self._event_signatures[path.name] = self._signature(path)
            return persisted

    def _atomic_write(self, path: Path, event: ExecutionAuditEvent) -> None:
        temporary_path: str | None = None
        descriptor = -1
        try:
            descriptor, temporary_path = tempfile.mkstemp(
                prefix=f".{path.stem}.",
                suffix=".tmp",
                dir=str(self._path),
            )
            os.fchmod(descriptor, 0o600)
            with os.fdopen(descriptor, "wb") as handle:
                descriptor = -1
                handle.write(_canonical_json(event.to_dict()))
                handle.write(b"\n")
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary_path, path)
            temporary_path = None
            self._fsync_directory()
        except AuditError:
            raise
        except OSError as exc:
            raise AuditStoreError("audit event cannot be written") from exc
        finally:
            if descriptor >= 0:
                os.close(descriptor)
            if temporary_path is not None:
                try:
                    os.unlink(temporary_path)
                except FileNotFoundError:
                    pass

    def _fsync_directory(self) -> None:
        try:
            descriptor = os.open(self._path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
            try:
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
        except OSError as exc:
            raise AuditStoreError("audit directory cannot be synced") from exc

    def get(self, event_id: str, *, tenant_id: str | None = None) -> ExecutionAuditEvent | None:
        _validate_text(event_id, "event_id", max_length=128)
        _validate_text(tenant_id, "tenant_id", max_length=256, optional=True)
        with self._locked():
            path = self._event_path(event_id)
            if path.exists():
                event = self._read_event_path(path)
            else:
                event = None
                for candidate in self._event_paths():
                    candidate_event = self._read_event_path(candidate)
                    if candidate_event.event_id == event_id:
                        event = candidate_event
                        break
            if event is None:
                return None
            if tenant_id is not None and event.tenant_id != tenant_id:
                return None
            return event

    def delete_if_unchanged(self, expected: ExecutionAuditEvent) -> AuditDeleteResult:
        """Delete one event only if it is byte-for-byte the expected document.

        This is the W112 purge safety primitive.  Under the same exclusive lock
        used by :meth:`append` it re-reads the persisted document and compares the
        identity, version, tenant, occurred_at, checksum and the full canonical
        representation.  Any divergence yields ``CONFLICT`` and nothing is
        removed, so a concurrently modified event can never be deleted by a
        lifecycle run that selected it earlier.
        """

        if not isinstance(expected, ExecutionAuditEvent):
            raise AuditValidationError("audit event is required")
        if expected.checksum is None:
            raise AuditValidationError("audit event checksum is required")
        expected.verify_integrity()
        with self._locked():
            path = self._event_path(expected.event_id)
            if not path.exists():
                for candidate in self._event_paths():
                    try:
                        probe = self._read_event_path(candidate)
                    except AuditError:
                        continue
                    if probe.event_id == expected.event_id:
                        path = candidate
                        break
                else:
                    return AuditDeleteResult(
                        event_id=expected.event_id,
                        tenant_id=expected.tenant_id,
                        outcome=AuditDeleteOutcome.NOT_FOUND,
                    )
            try:
                current = self._read_event_path(path)
            except AuditError:
                return AuditDeleteResult(
                    event_id=expected.event_id,
                    tenant_id=expected.tenant_id,
                    outcome=AuditDeleteOutcome.INVALID,
                )
            if not self._same_event(current, expected):
                return AuditDeleteResult(
                    event_id=expected.event_id,
                    tenant_id=expected.tenant_id,
                    outcome=AuditDeleteOutcome.CONFLICT,
                )
            if (
                current.checksum != expected.checksum
                or current.event_version != expected.event_version
                or current.tenant_id != expected.tenant_id
                or current.occurred_at != expected.occurred_at
                or _canonical_json(current.to_dict()) != _canonical_json(expected.to_dict())
            ):
                return AuditDeleteResult(
                    event_id=expected.event_id,
                    tenant_id=expected.tenant_id,
                    outcome=AuditDeleteOutcome.CONFLICT,
                )
            try:
                path.unlink()
            except FileNotFoundError:
                return AuditDeleteResult(
                    event_id=expected.event_id,
                    tenant_id=expected.tenant_id,
                    outcome=AuditDeleteOutcome.NOT_FOUND,
                )
            except OSError as exc:
                raise AuditStoreError("audit event cannot be deleted") from exc
            self._fsync_directory()
            if self._event_cache is not None:
                self._event_cache.pop(expected.event_id, None)
                self._event_signatures.pop(path.name, None)
            return AuditDeleteResult(
                event_id=expected.event_id,
                tenant_id=expected.tenant_id,
                outcome=AuditDeleteOutcome.DELETED,
            )

    @staticmethod
    def _document_ref(path: Path) -> str:
        return hashlib.sha256(path.name.encode("utf-8")).hexdigest()[:32]

    @staticmethod
    def _failure_verdict(document_ref: str, exc: AuditError) -> AuditDocumentRef:
        """Map an integrity phase failure to a stable, non-sensitive code.

        The mapping is intentionally explicit: a control plane must be able to
        distinguish a version, schema, checksum, canonicalization, identity or
        unsafe-document failure without exposing the document or a path.
        """

        if isinstance(exc, AuditEventVersionError):
            code, detail = AUDIT_FAILURE_VERSION, "unsupported_event_version"
        elif isinstance(exc, AuditSerializationError):
            code, detail = AUDIT_FAILURE_MALFORMED_JSON, "malformed_json"
        elif isinstance(exc, AuditChecksumError):
            code, detail = AUDIT_FAILURE_CHECKSUM, "checksum_mismatch"
        elif isinstance(exc, AuditCanonicalizationError):
            code, detail = AUDIT_FAILURE_CANONICAL, "non_canonical_document"
        elif isinstance(exc, AuditDocumentIdentityError):
            code, detail = AUDIT_FAILURE_IDENTITY, "document_identity_mismatch"
        elif isinstance(exc, AuditDocumentPermissionError):
            code, detail = AUDIT_FAILURE_UNSAFE_DOCUMENT, "unsafe_document"
        elif isinstance(exc, AuditEventShapeError):
            code, detail = AUDIT_FAILURE_SCHEMA, "schema_violation"
        else:
            code, detail = AUDIT_FAILURE_UNREADABLE, "unreadable_document"
        return AuditDocumentRef(
            document_ref=document_ref,
            event=None,
            tenant_id=None,
            failure_code=code,
            detail=detail,
        )

    def scan_documents(self) -> AuditDocumentScan:
        """Return one verdict per persisted document without mutating anything.

        Unlike :meth:`query`, a corrupt document is reported as data instead of
        aborting the whole read, so an operator can see exactly what failed.
        """

        with self._locked():
            documents = [self._classify_event_path(path) for path in self._event_paths()]
        return AuditDocumentScan(documents=tuple(documents))

    def _classify_event_path(self, path: Path) -> AuditDocumentRef:
        document_ref = self._document_ref(path)
        try:
            event = self._read_event_path(path)
        except AuditError as exc:
            return self._failure_verdict(document_ref, exc)
        return AuditDocumentRef(
            document_ref=document_ref,
            event=event,
            tenant_id=event.tenant_id,
        )

    def query(
        self,
        query: AuditQueryInput = None,
        *,
        criteria: AuditQueryInput = None,
    ) -> ExecutionAuditPage:
        active = _coerce_query(_select_query(query, criteria))
        with self._locked():
            filtered = [
                event
                for event in self._read_all_unlocked(force_verify=True)
                if active.matches(event)
            ]
        total = len(filtered)
        selected = filtered[active.offset : active.offset + active.limit]
        return ExecutionAuditPage(
            events=tuple(selected),
            total=total,
            limit=active.limit,
            offset=active.offset,
        )

    def statistics(
        self,
        query: AuditQueryInput = None,
        *,
        criteria: AuditQueryInput = None,
    ) -> ExecutionAuditStatistics:
        active = _coerce_query(_select_query(query, criteria))
        with self._locked():
            events = [
                event
                for event in self._read_all_unlocked(force_verify=True)
                if active.matches(event)
            ]
        return _statistics_for(events)


# A descriptive alias for callers that want to make the backend explicit.
FileExecutionAuditStore = ExecutionAuditStore


def _statistics_for(events: list[ExecutionAuditEvent]) -> ExecutionAuditStatistics:
    by_operation: dict[str, int] = {}
    by_outcome: dict[str, int] = {}
    by_tenant: dict[str, int] = {}
    successful = failed = rejected = conflicts = forbidden = 0
    for event in events:
        by_operation[event.operation.value] = by_operation.get(event.operation.value, 0) + 1
        by_outcome[event.outcome.value] = by_outcome.get(event.outcome.value, 0) + 1
        by_tenant[event.tenant_id] = by_tenant.get(event.tenant_id, 0) + 1
        if event.outcome in {AuditOutcome.SUCCESS, AuditOutcome.ALREADY_PRESENT}:
            successful += 1
        elif event.outcome is AuditOutcome.FAILED:
            failed += 1
        elif event.outcome is AuditOutcome.REJECTED:
            rejected += 1
        elif event.outcome is AuditOutcome.CONFLICT:
            conflicts += 1
        elif event.outcome is AuditOutcome.FORBIDDEN:
            forbidden += 1
    return ExecutionAuditStatistics(
        total_events=len(events),
        successful_events=successful,
        failed_events=failed,
        rejected_events=rejected,
        conflicts=conflicts,
        forbidden=forbidden,
        by_operation=by_operation,
        by_outcome=by_outcome,
        by_tenant=by_tenant,
    )


class MemoryExecutionAuditStore:
    """Thread-safe in-memory adapter for unit tests and small fixtures."""

    def __init__(self) -> None:
        self._events: dict[str, ExecutionAuditEvent] = {}
        self._lock = RLock()

    def close(self) -> None:
        """Close the in-memory adapter; no external resources are held."""

    def append(self, event: ExecutionAuditEvent) -> ExecutionAuditEvent:
        if not isinstance(event, ExecutionAuditEvent):
            raise AuditValidationError("audit event is required")
        if event.checksum is not None:
            event.verify_integrity()
        persisted = event.with_checksum()
        with self._lock:
            for existing in self._events.values():
                if existing.event_id == persisted.event_id or (
                    persisted.idempotency_key is not None
                    and existing.idempotency_key == persisted.idempotency_key
                ):
                    if ExecutionAuditStore._same_event(existing, persisted):
                        return existing
                    raise AuditIdempotencyConflictError("audit idempotency key was reused")
            self._events[persisted.event_id] = persisted
            return persisted

    def delete_if_unchanged(self, expected: ExecutionAuditEvent) -> AuditDeleteResult:
        if not isinstance(expected, ExecutionAuditEvent):
            raise AuditValidationError("audit event is required")
        if expected.checksum is None:
            raise AuditValidationError("audit event checksum is required")
        expected.verify_integrity()
        with self._lock:
            current = self._events.get(expected.event_id)
            if current is None:
                return AuditDeleteResult(
                    event_id=expected.event_id,
                    tenant_id=expected.tenant_id,
                    outcome=AuditDeleteOutcome.NOT_FOUND,
                )
            if not ExecutionAuditStore._same_event(current, expected):
                return AuditDeleteResult(
                    event_id=expected.event_id,
                    tenant_id=expected.tenant_id,
                    outcome=AuditDeleteOutcome.CONFLICT,
                )
            self._events.pop(expected.event_id, None)
            return AuditDeleteResult(
                event_id=expected.event_id,
                tenant_id=expected.tenant_id,
                outcome=AuditDeleteOutcome.DELETED,
            )

    def scan_documents(self) -> AuditDocumentScan:
        """Return one verdict per stored event without mutating anything."""

        with self._lock:
            documents = []
            for event in self._events.values():
                document_ref = hashlib.sha256(event.event_id.encode("utf-8")).hexdigest()[:32]
                try:
                    event.verify_integrity()
                except AuditError as exc:
                    documents.append(ExecutionAuditStore._failure_verdict(document_ref, exc))
                    continue
                documents.append(
                    AuditDocumentRef(
                        document_ref=document_ref,
                        event=event,
                        tenant_id=event.tenant_id,
                    )
                )
        return AuditDocumentScan(documents=tuple(documents))

    def get(self, event_id: str, *, tenant_id: str | None = None) -> ExecutionAuditEvent | None:
        with self._lock:
            event = self._events.get(event_id)
            if event is None or (tenant_id is not None and event.tenant_id != tenant_id):
                return None
            return event

    def query(
        self,
        query: AuditQueryInput = None,
        *,
        criteria: AuditQueryInput = None,
    ) -> ExecutionAuditPage:
        active = _coerce_query(_select_query(query, criteria))
        with self._lock:
            filtered = [event for event in self._events.values() if active.matches(event)]
        filtered.sort(key=lambda event: (event.occurred_at, event.event_id), reverse=True)
        return ExecutionAuditPage(
            events=tuple(filtered[active.offset : active.offset + active.limit]),
            total=len(filtered),
            limit=active.limit,
            offset=active.offset,
        )

    def statistics(
        self,
        query: AuditQueryInput = None,
        *,
        criteria: AuditQueryInput = None,
    ) -> ExecutionAuditStatistics:
        active = _coerce_query(_select_query(query, criteria))
        with self._lock:
            events = [event for event in self._events.values() if active.matches(event)]
        return _statistics_for(events)


_request_id_context: ContextVar[str | None] = ContextVar(
    "trendx_execution_audit_request_id",
    default=None,
)


@contextmanager
def audit_request_context(request_id: str) -> Iterator[None]:
    """Propagate one HTTP correlation id to an underlying domain service."""

    token = _request_id_context.set(request_id)
    try:
        yield
    finally:
        _request_id_context.reset(token)


def current_request_id() -> str | None:
    return _request_id_context.get()


class ExecutionAuditService:
    """Best-effort application boundary for recording audit events."""

    def __init__(
        self,
        store: AuditStore,
        *,
        failure_policy: AuditFailurePolicy = AuditFailurePolicy.BEST_EFFORT,
        clock: Any = None,
    ) -> None:
        self._store = store
        self._failure_policy = failure_policy
        self._clock = clock or _utc_now

    @property
    def store(self) -> AuditStore:
        return self._store

    @property
    def failure_policy(self) -> AuditFailurePolicy:
        return self._failure_policy

    def query(
        self,
        query: AuditQueryInput = None,
        *,
        criteria: AuditQueryInput = None,
    ) -> ExecutionAuditPage:
        if criteria is None:
            return self._store.query(query)
        return self._store.query(criteria=criteria)

    def statistics(
        self,
        query: AuditQueryInput = None,
        *,
        criteria: AuditQueryInput = None,
    ) -> ExecutionAuditStatistics:
        if criteria is None:
            return self._store.statistics(query)
        return self._store.statistics(criteria=criteria)

    def get(self, event_id: str, *, tenant_id: str | None = None) -> ExecutionAuditEvent | None:
        return self._store.get(event_id, tenant_id=tenant_id)

    def record(
        self,
        *,
        operation: AuditOperation | str,
        outcome: AuditOutcome | str,
        tenant_id: str,
        execution_id: str | None = None,
        reference_key: str | None = None,
        actor_type: AuditActorType | str = AuditActorType.SYSTEM,
        actor_id: str = "system",
        request_id: str | None = None,
        source: str = "system",
        reason_code: str = "unspecified",
        event_id: str | None = None,
        occurred_at: datetime | None = None,
        idempotency_key: str | None = None,
    ) -> ExecutionAuditEvent | None:
        log_operation = operation.value if isinstance(operation, AuditOperation) else "unknown"
        log_outcome = outcome.value if isinstance(outcome, AuditOutcome) else "unknown"
        try:
            active_operation = _coerce_operation(operation)
            active_outcome = _coerce_outcome(outcome)
            active_actor_type = _coerce_actor(actor_type)
            active_request_id = request_id or current_request_id()
            if active_request_id is None:
                active_request_id = f"audit-{uuid.uuid4()}"
            active_request_id = (
                _bounded_or_redacted(
                    active_request_id,
                    "request_id",
                    max_length=128,
                )
                or "request-redacted"
            )
            active_tenant_id = (
                _bounded_or_redacted(
                    tenant_id,
                    "tenant_id",
                    max_length=256,
                )
                or "tenant-redacted"
            )
            active_execution_id = _bounded_or_redacted(
                execution_id,
                "execution_id",
                max_length=512,
                optional=True,
            )
            active_reference_key = _bounded_or_redacted(
                reference_key,
                "reference_key",
                max_length=512,
                optional=True,
            )
            active_actor_id = (
                _bounded_or_redacted(
                    actor_id,
                    "actor_id",
                    max_length=128,
                )
                or "actor-redacted"
            )
            active_source = (
                _bounded_or_redacted(source, "source", max_length=128) or "source-redacted"
            )
            active_reason_code = (
                _bounded_or_redacted(
                    reason_code,
                    "reason_code",
                    max_length=128,
                )
                or "reason-redacted"
            )
            active_event_id = event_id
            try:
                active_event_id = _validate_text(active_event_id, "event_id", max_length=128)
            except AuditValidationError:
                active_event_id = None
            if active_event_id is None:
                key = idempotency_key or _default_idempotency_key(
                    active_operation,
                    active_tenant_id,
                    active_execution_id,
                    active_request_id,
                )
                active_event_id = _event_id_for_key(key)
            active_idempotency_key = _bounded_or_redacted(
                idempotency_key,
                "idempotency_key",
                max_length=256,
                optional=True,
            )
            event = ExecutionAuditEvent(
                event_id=active_event_id,
                occurred_at=occurred_at or self._clock(),
                operation=active_operation,
                outcome=active_outcome,
                tenant_id=active_tenant_id,
                execution_id=active_execution_id,
                reference_key=active_reference_key,
                actor_type=active_actor_type,
                actor_id=active_actor_id,
                request_id=active_request_id,
                source=active_source,
                reason_code=active_reason_code,
                idempotency_key=active_idempotency_key,
            )
            return self._store.append(event)
        except Exception as exc:  # audit must not roll back a business result
            logger.warning(
                "execution_audit operation={} outcome={} reason_code={} error_type={}",
                log_operation,
                log_outcome,
                "audit_write_failed",
                type(exc).__name__,
            )
            if self._failure_policy is AuditFailurePolicy.MANDATORY:
                raise
            return None

    def record_restore_result(
        self,
        result: Any,
        *,
        operation: AuditOperation = AuditOperation.RESTORE,
        actor_type: AuditActorType = AuditActorType.SYSTEM,
        actor_id: str = "system",
        request_id: str | None = None,
        source: str = "w108-recovery",
    ) -> ExecutionAuditEvent | None:
        status = str(getattr(getattr(result, "status", ""), "value", ""))
        outcome = {
            "RESTORED": AuditOutcome.SUCCESS,
            "VERIFIED": AuditOutcome.SUCCESS,
            "ALREADY_PRESENT": AuditOutcome.ALREADY_PRESENT,
            "CONFLICT": AuditOutcome.CONFLICT,
            "TENANT_FORBIDDEN": AuditOutcome.FORBIDDEN,
            "ARCHIVE_NOT_FOUND": AuditOutcome.REJECTED,
            "INVALID_ARCHIVE": AuditOutcome.REJECTED,
            "INTEGRITY_FAILURE": AuditOutcome.FAILED,
            "RESTORE_FAILED": AuditOutcome.FAILED,
        }.get(status, AuditOutcome.FAILED)
        reason = str(getattr(result, "reason", "restore_failed"))
        return self.record(
            operation=operation,
            outcome=outcome,
            tenant_id=str(getattr(result, "tenant_id", "")),
            execution_id=str(getattr(result, "execution_id", "")) or None,
            actor_type=actor_type,
            actor_id=actor_id,
            request_id=request_id,
            source=source,
            reason_code=reason,
        )

    def record_recovery_api_result(
        self,
        result: Any,
        *,
        status_code: int,
        actor_type: AuditActorType = AuditActorType.API,
        actor_id: str = "api-key-context",
        request_id: str | None = None,
    ) -> ExecutionAuditEvent | None:
        status = str(getattr(getattr(result, "status", ""), "value", ""))
        if status_code == 200 and status == "RESTORED":
            outcome = AuditOutcome.SUCCESS
            reason = "record_restored"
        elif status_code == 200 and status == "ALREADY_PRESENT":
            outcome = AuditOutcome.ALREADY_PRESENT
            reason = "record_already_present"
        elif status_code == 409:
            outcome = AuditOutcome.CONFLICT
            reason = "restore_conflict"
        elif status_code == 403:
            outcome = AuditOutcome.FORBIDDEN
            reason = "tenant_forbidden"
        elif status_code == 404:
            outcome = AuditOutcome.REJECTED
            reason = "archive_not_found"
        elif status_code == 503:
            outcome = AuditOutcome.FAILED
            reason = "backend_unavailable"
        else:
            outcome = AuditOutcome.REJECTED
            reason = "request_rejected"
        return self.record(
            operation=AuditOperation.RECOVERY_API,
            outcome=outcome,
            tenant_id=str(getattr(result, "tenant_id", "")),
            execution_id=str(getattr(result, "execution_id", "")) or None,
            actor_type=actor_type,
            actor_id=actor_id,
            request_id=request_id,
            source="w109-http",
            reason_code=reason,
        )


# Short aliases keep the W110 port easy to discover for operational callers.
AuditEvent = ExecutionAuditEvent
AuditQuery = ExecutionAuditQuery
AuditPage = ExecutionAuditPage
AuditStatistics = ExecutionAuditStatistics
FileAuditStore = ExecutionAuditStore
AuditEventStore = ExecutionAuditStore


__all__ = [
    "AUDIT_EVENT_VERSION",
    "AUDIT_FAILURE_CANONICAL",
    "AUDIT_FAILURE_CHECKSUM",
    "AUDIT_FAILURE_IDENTITY",
    "AUDIT_FAILURE_MALFORMED_JSON",
    "AUDIT_FAILURE_SCHEMA",
    "AUDIT_FAILURE_UNREADABLE",
    "AUDIT_FAILURE_UNSAFE_DOCUMENT",
    "AUDIT_FAILURE_VERSION",
    "AuditActorType",
    "AuditCanonicalizationError",
    "AuditChecksumError",
    "AuditDeleteOutcome",
    "AuditDeleteResult",
    "AuditDocumentIdentityError",
    "AuditDocumentPermissionError",
    "AuditDocumentRef",
    "AuditDocumentScan",
    "AuditEventShapeError",
    "AuditQueryInput",
    "AuditEvent",
    "AuditEventStore",
    "AuditPage",
    "AuditQuery",
    "AuditStatistics",
    "AuditError",
    "AuditEventVersionError",
    "AuditFailurePolicy",
    "AuditIdempotencyConflict",
    "AuditIdempotencyConflictError",
    "AuditIntegrityError",
    "AuditOperation",
    "AuditOutcome",
    "AuditQueryError",
    "AuditSerializationError",
    "AuditStore",
    "AuditStoreError",
    "AuditValidationError",
    "DEFAULT_AUDIT_QUERY_LIMIT",
    "EVENT_VERSION",
    "ExecutionAuditEvent",
    "ExecutionAuditPage",
    "ExecutionAuditQuery",
    "ExecutionAuditService",
    "ExecutionAuditStatistics",
    "ExecutionAuditStore",
    "FileExecutionAuditStore",
    "FileAuditStore",
    "MAX_AUDIT_OFFSET",
    "MAX_AUDIT_QUERY_LIMIT",
    "MemoryExecutionAuditStore",
    "audit_request_context",
    "current_request_id",
]
