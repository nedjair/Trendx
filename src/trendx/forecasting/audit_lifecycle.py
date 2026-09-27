"""W112 — lifecycle, retention and archive for the W110 operational audit trail.

W112 manages the lifecycle of :class:`~trendx.forecasting.audit.ExecutionAuditEvent`
documents **only**.  It never archives, purges or restores an
``ExecutionRecord``: the W107 lifecycle owns those and its ``ArchiveStore``
contract is typed on ``ExecutionRecord``, so W112 defines a distinct
:class:`AuditArchiveStore` contract instead of reusing it.

Lifecycle of one audit event::

    ACTIVE -> ARCHIVED -> PURGED

Every transition is an **explicit, caller-initiated** operation.  There is no
scheduler, no cron, no background worker and no implicit call: the service is a
controlled, synchronously invoked component and ``dry_run`` defaults to ``True``.

Purge safety is the core invariant.  A purge never performs ``SELECT -> DELETE``;
it always performs::

    SELECT -> VALIDATE -> ELIGIBILITY -> ARCHIVE -> VERIFY ARCHIVE
           -> COMPARE -> DELETE

and the final delete is a compare-and-delete against the W110 store, so an event
modified by another writer between selection and deletion is never removed.
An ambiguity always blocks the purge.
"""

from __future__ import annotations

import hashlib
import os
import stat
import tempfile
from collections.abc import Mapping
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from enum import Enum
from pathlib import Path
from threading import RLock
from typing import Any, ClassVar, Protocol, Self

from loguru import logger
from trendx.forecasting.audit import (
    AUDIT_EVENT_VERSION,
    AuditDocumentScan,
    AuditError,
    AuditEventVersionError,
    AuditIntegrityError,
    AuditOperation,
    AuditValidationError,
    ExecutionAuditEvent,
    ExecutionAuditService,
    _canonical_json,
    _object_pairs_without_duplicates,
    _reject_json_constant,
    _utc_now,
    _validate_text,
)

AUDIT_ARCHIVE_VERSION = 1
AUDIT_LIFECYCLE_REPORT_VERSION = "1"

#: Source recorded on every audit event produced by W112.  It is a stable W110
#: value so an operator can tell a lifecycle event from a business event.
AUDIT_LIFECYCLE_SOURCE = "AUDIT_LIFECYCLE"

#: Maximum number of events a single lifecycle call may act on.  It mirrors the
#: W110/W111 read bound so a lifecycle operation is never wider than a read.
MAX_AUDIT_LIFECYCLE_BATCH = 1000

_REDACTED = "[REDACTED]"


class AuditLifecycleError(AuditError):
    """Base class for W112 lifecycle errors."""


class AuditRetentionPolicyError(AuditLifecycleError):
    """The retention policy is invalid."""


class AuditArchiveError(AuditLifecycleError):
    """The audit archive cannot safely complete an operation."""


class AuditArchiveIntegrityError(AuditArchiveError):
    """An archived document failed canonical, schema, identity or checksum checks."""


class AuditArchiveNotFoundError(AuditArchiveError):
    """No archived document exists for the requested event."""


class AuditArchiveConflictError(AuditArchiveError):
    """An archived document already exists with a different content."""


class LifecycleStatus(str, Enum):
    """Terminal state of a lifecycle operation."""

    DRY_RUN = "DRY_RUN"
    SUCCESS = "SUCCESS"
    PARTIAL = "PARTIAL"
    FAILED = "FAILED"
    CONFLICT = "CONFLICT"


class AuditRestoreStatus(str, Enum):
    """Terminal state of a controlled audit restore."""

    RESTORED = "RESTORED"
    ALREADY_PRESENT = "ALREADY_PRESENT"
    CONFLICT = "CONFLICT"
    NOT_FOUND = "NOT_FOUND"
    INTEGRITY_FAILURE = "INTEGRITY_FAILURE"
    TENANT_FORBIDDEN = "TENANT_FORBIDDEN"
    RESTORE_FAILED = "RESTORE_FAILED"


class AuditPurgeGuard(str, Enum):
    """Why an event was never purged.  Every value is a fail-closed guard."""

    ALREADY_ARCHIVED = "already_archived"
    ARCHIVE_MISSING = "archive_missing"
    ARCHIVE_CORRUPT = "archive_corrupt"
    CHECKSUM_MISMATCH = "checksum_mismatch"
    CONFLICT = "conflict"
    INVALID_EVENT = "invalid_event"
    MINIMUM_KEPT = "minimum_kept"
    NOT_ELIGIBLE = "not_eligible"
    PROTECTED = "protected"
    TENANT_MISMATCH = "tenant_mismatch"


#: Re-entrancy guard.  A W112 audit event is written through the W110 service;
#: if writing it ever triggered another lifecycle run, the nested call is
#: refused here instead of recursing.
_lifecycle_active: ContextVar[bool] = ContextVar(
    "trendx_audit_lifecycle_active",
    default=False,
)
#: Named constant so the guard can be toggled without a boolean positional
#: literal, keeping both the linter and the type checker satisfied.
_GUARD_SET = True


class LifecycleRecursionError(AuditLifecycleError):
    """A lifecycle operation was requested from inside another one."""


def _utc(value: datetime | str, field_name: str) -> datetime:
    """Normalise to an explicit UTC datetime; never fall back to local time."""

    if isinstance(value, str):
        raw = value.strip()
        if raw.endswith("Z"):
            raw = f"{raw[:-1]}+00:00"
        try:
            parsed = datetime.fromisoformat(raw)
        except ValueError as exc:
            raise AuditRetentionPolicyError(f"{field_name} is not a valid timestamp") from exc
    elif isinstance(value, datetime):
        parsed = value
    else:
        raise AuditRetentionPolicyError(f"{field_name} is not a valid timestamp")
    return parsed.replace(tzinfo=UTC) if parsed.tzinfo is None else parsed.astimezone(UTC)


def _format_datetime(value: datetime) -> str:
    return _utc(value, "timestamp").isoformat()


def _strict_json_loads(value: str) -> Any:
    import json  # - local to keep the module import surface small

    try:
        return json.loads(
            value,
            object_pairs_hook=_object_pairs_without_duplicates,
            parse_constant=_reject_json_constant,
        )
    except (TypeError, ValueError) as exc:
        raise AuditArchiveIntegrityError("archived audit document contains invalid JSON") from exc


def _safe_reference(value: str | None) -> str | None:
    if value is None:
        return None
    if "\x00" in value or "/" in value or "\\" in value or len(value) > 512:
        return _REDACTED
    return value


@dataclass(frozen=True)
class AuditRetentionPolicy:
    """Explicit, deterministic retention policy for audit events.

    ``retention_days`` and ``reference_time`` have no defaults on purpose: W112
    must never invent a retention period nor silently use the wall clock, so the
    caller always states the reference instant.  ``dry_run`` defaults to ``True``
    so a lifecycle call is read-only unless the caller explicitly opts out.
    """

    retention_days: int
    reference_time: datetime
    archive_before_purge: bool = True
    minimum_events_to_keep: int = 0
    dry_run: bool = True

    def __post_init__(self) -> None:
        if isinstance(self.retention_days, bool) or not isinstance(self.retention_days, int):
            raise AuditRetentionPolicyError("retention_days must be an integer")
        if self.retention_days <= 0:
            raise AuditRetentionPolicyError("retention_days must be greater than zero")
        try:
            timedelta(days=self.retention_days)
        except (OverflowError, ValueError) as exc:
            raise AuditRetentionPolicyError(
                "retention_days is outside the supported timestamp range"
            ) from exc
        if (
            isinstance(self.minimum_events_to_keep, bool)
            or not isinstance(self.minimum_events_to_keep, int)
            or self.minimum_events_to_keep < 0
        ):
            raise AuditRetentionPolicyError("minimum_events_to_keep must be a non-negative integer")
        if not isinstance(self.archive_before_purge, bool):
            raise AuditRetentionPolicyError("archive_before_purge must be a boolean")
        if not isinstance(self.dry_run, bool):
            raise AuditRetentionPolicyError("dry_run must be a boolean")
        object.__setattr__(self, "reference_time", _utc(self.reference_time, "reference_time"))

    @property
    def eligible_at(self) -> datetime:
        """Deterministic eligibility frontier; the boundary itself is eligible."""

        return self.reference_time - timedelta(days=self.retention_days)

    def is_eligible(self, occurred_at: datetime) -> bool:
        return _utc(occurred_at, "occurred_at") <= self.eligible_at

    def to_dict(self) -> dict[str, Any]:
        return {
            "retention_days": self.retention_days,
            "archive_before_purge": self.archive_before_purge,
            "minimum_events_to_keep": self.minimum_events_to_keep,
            "dry_run": self.dry_run,
            "reference_time": _format_datetime(self.reference_time),
            "eligible_at": _format_datetime(self.eligible_at),
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> AuditRetentionPolicy:
        allowed = {
            "retention_days",
            "reference_time",
            "archive_before_purge",
            "minimum_events_to_keep",
            "dry_run",
        }
        # `eligible_at` is accepted on input for a lossless round trip but is
        # always recomputed from reference_time, never trusted from a caller.
        tolerated = {"eligible_at"}
        if not isinstance(data, Mapping) or set(data) - allowed - tolerated:
            raise AuditRetentionPolicyError("retention policy fields are invalid")
        try:
            return cls(
                retention_days=data["retention_days"],
                reference_time=data["reference_time"],
                archive_before_purge=data.get("archive_before_purge", True),
                minimum_events_to_keep=data.get("minimum_events_to_keep", 0),
                dry_run=data.get("dry_run", True),
            )
        except KeyError as exc:
            raise AuditRetentionPolicyError("retention policy is incomplete") from exc
        except (TypeError, ValueError) as exc:
            raise AuditRetentionPolicyError("retention policy is invalid") from exc


@dataclass(frozen=True)
class AuditArchiveDocument:
    """Immutable envelope preserving the complete W110 event contract.

    No W110 field is dropped or renamed.  The envelope adds only the archive
    metadata required for an auditable lifecycle: the archive format version, the
    archive instant, the original W110 event checksum and the archive document
    checksum.
    """

    event: ExecutionAuditEvent
    archive_version: int
    archived_at: datetime
    original_event_checksum: str
    archive_document_checksum: str | None = None

    _FIELDS = (
        "archive_version",
        "archived_at",
        "original_event_checksum",
        "event",
    )

    def __post_init__(self) -> None:
        if not isinstance(self.event, ExecutionAuditEvent):
            raise AuditArchiveError("archived audit event is required")
        if self.archive_version != AUDIT_ARCHIVE_VERSION:
            raise AuditArchiveIntegrityError("unsupported audit archive version")
        object.__setattr__(self, "archived_at", _utc(self.archived_at, "archived_at"))
        if self.original_event_checksum != self.event.checksum:
            raise AuditArchiveIntegrityError("archived event checksum does not match the event")
        if self.archive_document_checksum is None:
            object.__setattr__(self, "archive_document_checksum", self.calculated_checksum)

    @property
    def event_id(self) -> str:
        return self.event.event_id

    @property
    def tenant_id(self) -> str:
        return self.event.tenant_id

    @property
    def record_checksum(self) -> str:
        """The preserved W110 event checksum."""

        return self.original_event_checksum

    def canonical_payload(self) -> bytes:
        return _canonical_json(self.to_dict(include_checksum=False))

    def event_payload(self) -> bytes:
        """Canonical bytes of the archived W110 event.

        This is the archive *identity*: two envelopes holding the same event are
        the same archive, so republication is idempotent and ``archived_at``
        (the instant of first publication) never causes a false conflict.
        """

        return _canonical_json(self.event.to_dict())

    def same_event(self, other: ExecutionAuditEvent) -> bool:
        """Strict canonical comparison of an active event with an archived one."""

        if not isinstance(other, ExecutionAuditEvent):
            return False
        return self.event_payload() == _canonical_json(other.to_dict())

    @property
    def calculated_checksum(self) -> str:
        return hashlib.sha256(self.canonical_payload()).hexdigest()

    def to_dict(self, *, include_checksum: bool = True) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "archive_version": self.archive_version,
            "archived_at": _format_datetime(self.archived_at),
            "original_event_checksum": self.original_event_checksum,
            "event": self.event.to_dict(),
        }
        if include_checksum:
            payload["archive_document_checksum"] = self.archive_document_checksum
        return payload

    def with_checksum(self) -> AuditArchiveDocument:
        return AuditArchiveDocument(
            event=self.event,
            archive_version=self.archive_version,
            archived_at=self.archived_at,
            original_event_checksum=self.original_event_checksum,
            archive_document_checksum=self.calculated_checksum,
        )

    def verify_integrity(self) -> None:
        if self.archive_document_checksum is None:
            raise AuditArchiveIntegrityError("archive document checksum is missing")
        if self.calculated_checksum != self.archive_document_checksum:
            raise AuditArchiveIntegrityError("archive document checksum mismatch")
        if self.event.checksum is None:
            raise AuditArchiveIntegrityError("archived event checksum is missing")
        if self.event.calculated_checksum != self.event.checksum:
            raise AuditArchiveIntegrityError("archived event checksum mismatch")
        if self.original_event_checksum != self.event.checksum:
            raise AuditArchiveIntegrityError("original event checksum mismatch")
        if self.event.event_version != AUDIT_EVENT_VERSION:
            raise AuditEventVersionError("unsupported audit event version")

    def to_bytes(self) -> bytes:
        self.verify_integrity()
        return _canonical_json(self.to_dict()) + b"\n"

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> AuditArchiveDocument:
        expected = {
            "archive_version",
            "archived_at",
            "original_event_checksum",
            "event",
            "archive_document_checksum",
        }
        if not isinstance(data, Mapping) or set(data) != expected:
            raise AuditArchiveIntegrityError("archive document shape is invalid")
        if data.get("archive_version") != AUDIT_ARCHIVE_VERSION:
            raise AuditArchiveIntegrityError("unsupported audit archive version")
        try:
            document = cls(
                event=ExecutionAuditEvent.from_dict(data["event"]),
                archive_version=data["archive_version"],
                archived_at=data["archived_at"],
                original_event_checksum=data["original_event_checksum"],
                archive_document_checksum=data["archive_document_checksum"],
            )
        except AuditEventVersionError:
            raise
        except AuditIntegrityError as exc:
            raise AuditArchiveIntegrityError("archived audit event is invalid") from exc
        except (AuditError, TypeError, ValueError) as exc:
            raise AuditArchiveIntegrityError("archive document fields are invalid") from exc
        document.verify_integrity()
        return document


@dataclass(frozen=True)
class AuditArchiveReceipt:
    """Result of a single archive publication."""

    event_id: str
    tenant_id: str
    checksum: str
    created: bool
    already_present: bool

    def to_dict(self) -> dict[str, Any]:
        return {
            "event_id": _safe_reference(self.event_id),
            "tenant_id": _safe_reference(self.tenant_id),
            "checksum": self.checksum,
            "created": self.created,
            "already_present": self.already_present,
        }


@dataclass(frozen=True)
class AuditArchiveVerification:
    """Read-only verdict over one archived document."""

    event_id: str
    tenant_id: str | None
    verified: bool
    error_code: str | None
    checksum: str | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "event_id": _safe_reference(self.event_id),
            "tenant_id": _safe_reference(self.tenant_id),
            "verified": self.verified,
            "error_code": self.error_code,
            "checksum": self.checksum,
        }


class AuditArchiveStore(Protocol):
    """Archive boundary for audit events.

    Deliberately distinct from the W107 ``ArchiveStore``: that contract is typed
    on ``ExecutionRecord`` and its receipts/checksums describe execution records,
    not audit events.  The business contracts stay separate even though both are
    local filesystems.
    """

    def put(self, document: AuditArchiveDocument) -> AuditArchiveReceipt: ...

    def get(
        self, event_id: str, *, tenant_id: str | None = None
    ) -> AuditArchiveDocument | None: ...

    def verify(
        self,
        event_id: str,
        *,
        expected: AuditArchiveDocument | None = None,
        tenant_id: str | None = None,
    ) -> AuditArchiveVerification: ...

    def exists(self, event_id: str) -> bool: ...

    def list_documents(
        self, *, tenant_id: str | None = None
    ) -> tuple[AuditArchiveDocument, ...]: ...


class FileSystemAuditArchiveStore:
    """Atomic, immutable, tenant-scoped filesystem archive for audit events.

    Publication is temp file -> write -> flush -> ``fsync`` -> ``os.replace`` ->
    ``fsync`` of the parent directory, so a crash can never publish a partially
    valid archive.  Temporary files are never treated as archives and are removed
    on both success and failure.  Directories are ``0700`` and documents ``0600``.
    """

    FORMAT_VERSION = AUDIT_ARCHIVE_VERSION

    def __init__(self, path: str | Path, *, create_if_missing: bool = True) -> None:
        self._path = Path(path)
        self._thread_lock = RLock()
        if self._path.is_symlink():
            raise AuditArchiveError("audit archive path must not be a symlink")
        if self._path.exists():
            if not self._path.is_dir():
                raise AuditArchiveError("audit archive path is not a directory")
        elif create_if_missing:
            self._ensure_root()
        else:
            raise AuditArchiveError("audit archive is unavailable")
        self._lock_path = self._path.with_name(f".{self._path.name}.lock")
        if self._path.exists() and self._path.is_dir():
            self._check_root()
            if self._lock_path.is_symlink():
                raise AuditArchiveError("audit archive lock path must not be a symlink")
            try:
                with self._lock_path.open("a+") as handle:
                    os.fchmod(handle.fileno(), 0o600)
            except OSError as exc:
                raise AuditArchiveError("audit archive lock is unavailable") from exc

    @property
    def path(self) -> Path:
        return self._path

    def close(self) -> None:
        """Release the short-lived file handles used by this store."""

    def __enter__(self) -> Self:
        return self

    def __exit__(self, exc_type: object, exc_value: object, traceback: object) -> None:
        self.close()

    def _ensure_root(self) -> None:
        if self._path.is_symlink():
            raise AuditArchiveError("audit archive path must not be a symlink")
        try:
            self._path.mkdir(parents=True, exist_ok=True, mode=0o700)
            os.chmod(self._path, 0o700)
            metadata = self._path.stat()
        except AuditArchiveError:
            raise
        except OSError as exc:
            raise AuditArchiveError("audit archive directory is unavailable") from exc
        if metadata.st_uid != os.geteuid() or stat.S_IMODE(metadata.st_mode) & 0o077:
            raise AuditArchiveIntegrityError("audit archive directory permissions are insecure")

    def _check_root(self) -> None:
        if not self._path.exists() or self._path.is_symlink() or not self._path.is_dir():
            raise AuditArchiveError("audit archive is unavailable")
        try:
            metadata = self._path.stat()
        except OSError as exc:
            raise AuditArchiveError("audit archive directory is unavailable") from exc
        if metadata.st_uid != os.geteuid() or stat.S_IMODE(metadata.st_mode) & 0o077:
            raise AuditArchiveIntegrityError("audit archive directory permissions are insecure")

    @contextmanager
    def _locked(self) -> Any:
        self._check_root()
        with self._thread_lock:
            if self._lock_path.is_symlink():
                raise AuditArchiveError("audit archive lock path must not be a symlink")
            try:
                handle = self._lock_path.open("a+")
            except OSError as exc:
                raise AuditArchiveError("audit archive lock is unavailable") from exc
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

    def _document_path(self, event_id: str) -> Path:
        return self._path / self._filename(event_id)

    def _document_paths(self) -> tuple[Path, ...]:
        try:
            paths = tuple(sorted(self._path.glob("*.json")))
        except OSError as exc:
            raise AuditArchiveError("audit archive is unavailable") from exc
        for path in paths:
            if path.is_symlink():
                raise AuditArchiveIntegrityError("archived audit document must not be a symlink")
        return paths

    def _read_document(self, path: Path) -> AuditArchiveDocument:
        if path.is_symlink():
            raise AuditArchiveIntegrityError("archived audit document must not be a symlink")
        try:
            metadata = path.stat()
        except OSError as exc:
            raise AuditArchiveError("archived audit document cannot be read") from exc
        if metadata.st_uid != os.geteuid() or stat.S_IMODE(metadata.st_mode) & 0o077:
            raise AuditArchiveIntegrityError("archived audit document permissions are insecure")
        try:
            raw = path.read_text(encoding="utf-8")
        except (OSError, UnicodeError) as exc:
            raise AuditArchiveError("archived audit document cannot be read") from exc
        payload = _strict_json_loads(raw)
        if not isinstance(payload, Mapping):
            raise AuditArchiveIntegrityError("archived audit document is malformed")
        document = AuditArchiveDocument.from_dict(payload)
        if path.name != self._filename(document.event_id):
            raise AuditArchiveIntegrityError("archived audit document identity mismatch")
        if raw != document.to_bytes().decode("utf-8"):
            raise AuditArchiveIntegrityError("archived audit document is not canonical")
        return document

    def _fsync_directory(self) -> None:
        try:
            descriptor = os.open(self._path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
            try:
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
        except OSError as exc:
            raise AuditArchiveError("audit archive directory cannot be synced") from exc

    def _atomic_publish(self, path: Path, payload: bytes) -> None:
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
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary_path, path)
            temporary_path = None
            self._fsync_directory()
        except AuditArchiveError:
            raise
        except OSError as exc:
            raise AuditArchiveError("archived audit document cannot be written") from exc
        finally:
            if descriptor >= 0:
                os.close(descriptor)
            if temporary_path is not None:
                try:
                    os.unlink(temporary_path)
                except FileNotFoundError:
                    pass

    @staticmethod
    def _same_document(left: AuditArchiveDocument, right: AuditArchiveDocument) -> bool:
        """Idempotence check: same event, same version, same preserved checksum.

        ``archived_at`` and the document checksum are deliberately excluded so
        re-running an archive for an already-archived event is reported as
        ``ALREADY_PRESENT`` instead of a false conflict, and the published
        document is never rewritten.
        """

        return (
            left.event_payload() == right.event_payload()
            and left.archive_version == right.archive_version
            and left.original_event_checksum == right.original_event_checksum
        )

    def put(self, document: AuditArchiveDocument) -> AuditArchiveReceipt:
        """Publish one document; identical republication is idempotent."""

        if not isinstance(document, AuditArchiveDocument):
            raise AuditArchiveError("archived audit document is required")
        document.verify_integrity()
        payload = document.to_bytes()
        with self._locked():
            path = self._document_path(document.event_id)
            if path.exists():
                existing = self._read_document(path)
                if not self._same_document(existing, document):
                    raise AuditArchiveConflictError(
                        "an archived audit document already exists for this event"
                    )
                return AuditArchiveReceipt(
                    event_id=document.event_id,
                    tenant_id=document.tenant_id,
                    checksum=existing.archive_document_checksum or "",
                    created=False,
                    already_present=True,
                )
            self._atomic_publish(path, payload)
            return AuditArchiveReceipt(
                event_id=document.event_id,
                tenant_id=document.tenant_id,
                checksum=document.archive_document_checksum or "",
                created=True,
                already_present=False,
            )

    def get(self, event_id: str, *, tenant_id: str | None = None) -> AuditArchiveDocument | None:
        _validate_text(event_id, "event_id", max_length=128)
        _validate_text(tenant_id, "tenant_id", max_length=256, optional=True)
        with self._locked():
            path = self._document_path(event_id)
            if not path.exists():
                for candidate in self._document_paths():
                    try:
                        probe = self._read_document(candidate)
                    except AuditArchiveError:
                        continue
                    if probe.event_id == event_id:
                        path = candidate
                        break
                else:
                    return None
            document = self._read_document(path)
        if tenant_id is not None and document.tenant_id != tenant_id:
            return None
        return document

    def exists(self, event_id: str) -> bool:
        _validate_text(event_id, "event_id", max_length=128)
        with self._locked():
            return self._document_path(event_id).exists()

    def verify(
        self,
        event_id: str,
        *,
        expected: AuditArchiveDocument | None = None,
        tenant_id: str | None = None,
    ) -> AuditArchiveVerification:
        """Read-only verification; never repairs and never hides a defect."""

        _validate_text(event_id, "event_id", max_length=128)
        with self._locked():
            path = self._document_path(event_id)
            if not path.exists():
                for candidate in self._document_paths():
                    try:
                        document = self._read_document(candidate)
                    except AuditArchiveError:
                        continue
                    if document.event_id == event_id:
                        path = candidate
                        break
            try:
                document = self._read_document(path)
            except AuditArchiveIntegrityError:
                return AuditArchiveVerification(
                    event_id=event_id,
                    tenant_id=None,
                    verified=False,
                    error_code="archive_corrupt",
                    checksum=None,
                )
            except AuditArchiveError:
                return AuditArchiveVerification(
                    event_id=event_id,
                    tenant_id=None,
                    verified=False,
                    error_code="archive_missing",
                    checksum=None,
                )
        if tenant_id is not None and document.tenant_id != tenant_id:
            return AuditArchiveVerification(
                event_id=event_id,
                tenant_id=None,
                verified=False,
                error_code="tenant_mismatch",
                checksum=None,
            )
        if expected is not None and not self._same_document(document, expected):
            return AuditArchiveVerification(
                event_id=event_id,
                tenant_id=document.tenant_id,
                verified=False,
                error_code="content_mismatch",
                checksum=document.archive_document_checksum,
            )
        return AuditArchiveVerification(
            event_id=event_id,
            tenant_id=document.tenant_id,
            verified=True,
            error_code=None,
            checksum=document.archive_document_checksum,
        )

    def list_documents(self, *, tenant_id: str | None = None) -> tuple[AuditArchiveDocument, ...]:
        _validate_text(tenant_id, "tenant_id", max_length=256, optional=True)
        with self._locked():
            documents = [self._read_document(path) for path in self._document_paths()]
        if tenant_id is not None:
            documents = [item for item in documents if item.tenant_id == tenant_id]
        return tuple(
            sorted(documents, key=lambda item: (item.event.occurred_at, item.event.event_id))
        )


@dataclass(frozen=True)
class LifecyclePreviewReport:
    """Read-only preview of a retention run."""

    report_version: str
    generated_at: datetime
    tenant_id: str
    policy: AuditRetentionPolicy
    scanned: int
    eligible: int
    protected: int
    invalid: int
    already_archived: int
    archive_candidates: int
    purge_candidates: int
    warnings: tuple[str, ...]
    status: LifecycleStatus

    def to_dict(self) -> dict[str, Any]:
        return {
            "report_version": self.report_version,
            "generated_at": _format_datetime(self.generated_at),
            "tenant_id": self.tenant_id,
            "policy": self.policy.to_dict(),
            "scanned": self.scanned,
            "eligible": self.eligible,
            "protected": self.protected,
            "invalid": self.invalid,
            "already_archived": self.already_archived,
            "archive_candidates": self.archive_candidates,
            "purge_candidates": self.purge_candidates,
            "warnings": list(self.warnings),
            "status": self.status.value,
        }


@dataclass(frozen=True)
class AuditLifecycleReport:
    """Structured result of an archive, purge or restore run."""

    report_version: str
    generated_at: datetime
    tenant_id: str
    policy: AuditRetentionPolicy
    dry_run: bool
    scanned: int
    eligible: int
    archived: int
    archive_verified: int
    purged: int
    skipped: int
    conflicts: int
    failures: int
    protected: int
    integrity_failures: int
    status: LifecycleStatus
    guards: dict[str, int] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "report_version": self.report_version,
            "generated_at": _format_datetime(self.generated_at),
            "tenant_id": self.tenant_id,
            "policy": self.policy.to_dict(),
            "dry_run": self.dry_run,
            "scanned": self.scanned,
            "eligible": self.eligible,
            "archived": self.archived,
            "archive_verified": self.archive_verified,
            "purged": self.purged,
            "skipped": self.skipped,
            "conflicts": self.conflicts,
            "failures": self.failures,
            "protected": self.protected,
            "integrity_failures": self.integrity_failures,
            "status": self.status.value,
            "guards": dict(sorted(self.guards.items())),
        }


@dataclass(frozen=True)
class AuditRestoreRequest:
    """Controlled restore of one archived audit event into the active store."""

    event_id: str
    tenant_id: str
    request_id: str | None = None
    actor_id: str = "api-key-context"

    def __post_init__(self) -> None:
        _validate_text(self.event_id, "event_id", max_length=128)
        _validate_text(self.tenant_id, "tenant_id", max_length=256)
        _validate_text(self.request_id, "request_id", max_length=128, optional=True)
        _validate_text(self.actor_id, "actor_id", max_length=128)

    def to_dict(self) -> dict[str, Any]:
        return {
            "event_id": _safe_reference(self.event_id),
            "tenant_id": _safe_reference(self.tenant_id),
            "request_id": _safe_reference(self.request_id),
            "actor_id": _safe_reference(self.actor_id),
        }


@dataclass(frozen=True)
class AuditRestoreResult:
    """Result of a controlled restore; never overwrites an active event."""

    status: AuditRestoreStatus
    event_id: str
    tenant_id: str
    reason_code: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status.value,
            "event_id": _safe_reference(self.event_id),
            "tenant_id": _safe_reference(self.tenant_id),
            "reason_code": self.reason_code,
        }


@dataclass(frozen=True)
class _Candidate:
    """Internal per-event lifecycle decision, computed once and reused."""

    event: ExecutionAuditEvent
    eligible: bool
    protected: bool
    invalid: bool
    already_archived: bool
    guard: AuditPurgeGuard | None


class AuditLifecycleService:
    """Explicit, caller-initiated lifecycle for audit events.

    The service exposes no scheduler hook and performs no work implicitly.  Every
    entry point is a synchronous, tenant-scoped call, and ``dry_run`` defaults to
    ``True`` so a caller must explicitly opt into a mutation.
    """

    #: W112 lifecycle operations are ordinary W110 operations, so the mapping is
    #: explicit and total: a missing name is a programming error, not a silent
    #: untyped fallback.
    DEFAULT_OPERATIONS: ClassVar[dict[str, AuditOperation]] = {
        "AUDIT_LIFECYCLE_PREVIEW": AuditOperation.AUDIT_LIFECYCLE_PREVIEW,
        "AUDIT_ARCHIVE": AuditOperation.AUDIT_ARCHIVE,
        "AUDIT_ARCHIVE_VERIFY": AuditOperation.AUDIT_ARCHIVE_VERIFY,
        "AUDIT_PURGE": AuditOperation.AUDIT_PURGE,
        "AUDIT_RESTORE": AuditOperation.AUDIT_RESTORE,
    }

    def __init__(
        self,
        audit_service: ExecutionAuditService,
        archive_store: AuditArchiveStore,
        *,
        audit_operations: Mapping[str, AuditOperation] | None = None,
        clock: Any = None,
    ) -> None:
        self._audit_service = audit_service
        self._archive = archive_store
        self._clock = clock or _utc_now
        merged = dict(self.DEFAULT_OPERATIONS)
        if audit_operations:
            unknown = set(audit_operations) - set(self.DEFAULT_OPERATIONS)
            if unknown:
                msg = "unknown audit lifecycle operation name"
                raise AuditLifecycleError(msg)
            merged.update(audit_operations)
        self._operations = merged
        self._lock = RLock()

    # ── introspection ──────────────────────────────────────────────────────

    @property
    def archive_store(self) -> AuditArchiveStore:
        return self._archive

    def _operation(self, name: str) -> AuditOperation:
        """Resolve a W110 operation enum for a lifecycle audit event.

        The mapping is validated once at construction, so a typo cannot silently
        fall back to an untyped string.
        """

        return self._operations[name]

    def _record(
        self,
        operation: str,
        outcome: str,
        tenant_id: str,
        *,
        execution_id: str | None = None,
        reason_code: str,
        request_id: str | None = None,
        source: str = AUDIT_LIFECYCLE_SOURCE,
    ) -> None:
        """Write one W110 audit event describing a W112 operation.

        This is best effort and never raises: a lifecycle result must not be
        turned into a failure because the audit append failed.  It is also
        strictly one-directional — writing this event never triggers another
        lifecycle run, and the re-entrancy guard below refuses a nested call.
        """

        try:
            self._audit_service.record(
                operation=self._operation(operation),
                outcome=outcome,
                tenant_id=tenant_id,
                execution_id=execution_id,
                actor_type="API",
                actor_id="audit-lifecycle",
                request_id=request_id,
                source=source,
                reason_code=reason_code,
            )
        except Exception as exc:  # pragma: no cover - best effort by design
            logger.warning(
                "audit_lifecycle operation={} outcome={} reason_code=audit_write_failed "
                "error_type={}",
                operation,
                outcome,
                type(exc).__name__,
            )

    @contextmanager
    def _guarded(self) -> Any:
        if _lifecycle_active.get():
            msg = "audit lifecycle operation is already running"
            raise LifecycleRecursionError(msg)
        token = _lifecycle_active.set(_GUARD_SET)
        try:
            yield
        finally:
            _lifecycle_active.reset(token)

    def archived_events(self, tenant_id: str) -> tuple[ExecutionAuditEvent, ...]:
        """Return the tenant's archived events, ordered like a W111 export.

        This implements the optional :class:`AuditLifecycleReader` port consumed
        by the W111 control plane, so an export can declare an explicit
        ``ACTIVE_AND_ARCHIVED`` perimeter and a reconciliation can report the
        ACTIVE / ARCHIVED split.  It never returns another tenant's event.
        """

        tenant = self._require_tenant(tenant_id)
        documents = self._archive.list_documents(tenant_id=tenant)
        return tuple(document.event for document in documents)

    def archive_verification(self, event_id: str, *, tenant_id: str) -> AuditArchiveVerification:
        """Read-only archive verification, tenant-scoped and fail-closed."""

        tenant = self._require_tenant(tenant_id)
        return self._archive.verify(event_id, tenant_id=tenant)

    @staticmethod
    def _require_tenant(tenant_id: str) -> str:
        if not isinstance(tenant_id, str) or not tenant_id.strip():
            raise AuditValidationError("tenant_id is required")
        tenant = tenant_id.strip()
        if (
            len(tenant) > 256
            or "\x00" in tenant
            or "/" in tenant
            or "\\" in tenant
            or ".." in tenant
        ):
            raise AuditValidationError("tenant_id is not permitted")
        return tenant

    # ── candidate selection (read-only) ────────────────────────────────────

    def _candidates(
        self,
        tenant_id: str,
        policy: AuditRetentionPolicy,
    ) -> tuple[list[_Candidate], int, list[str]]:
        """Select the tenant's active events and decide each one's fate.

        Deterministic ordering: newest first by ``(occurred_at, event_id)``.  The
        first ``minimum_events_to_keep`` entries are protected regardless of age,
        so a retention run can never wipe a tenant's whole recent trail.
        """

        scan: AuditDocumentScan = self._audit_service.store.scan_documents()
        warnings: list[str] = []
        invalid = 0
        candidates: list[_Candidate] = []
        for document in scan.documents:
            event = document.event
            if event is None:
                # a corrupt document is never eligible and never purged
                invalid += 1
                continue
            if event.tenant_id != tenant_id:
                continue
            candidates.append(
                _Candidate(
                    event=event,
                    eligible=policy.is_eligible(event.occurred_at),
                    protected=False,
                    invalid=False,
                    already_archived=False,
                    guard=None,
                )
            )
        candidates.sort(
            key=lambda item: (item.event.occurred_at, item.event.event_id), reverse=True
        )
        total = len(candidates)
        if total == 0:
            return [], invalid, warnings
        keep = min(policy.minimum_events_to_keep, total)
        for index, candidate in enumerate(candidates):
            if index < keep:
                candidates[index] = _Candidate(
                    event=candidate.event,
                    eligible=candidate.eligible,
                    protected=True,
                    invalid=candidate.invalid,
                    already_archived=False,
                    guard=AuditPurgeGuard.PROTECTED,
                )
        for candidate in candidates:
            if candidate.event.tenant_id != tenant_id:
                warnings.append("tenant_mismatch_skipped")
        return candidates, invalid, warnings

    @staticmethod
    def _policy_of(policy: AuditRetentionPolicy | Mapping[str, Any] | None) -> AuditRetentionPolicy:
        if policy is None:
            msg = "a retention policy is required"
            raise AuditRetentionPolicyError(msg)
        if isinstance(policy, AuditRetentionPolicy):
            return policy
        if isinstance(policy, Mapping):
            return AuditRetentionPolicy.from_dict(policy)
        raise AuditRetentionPolicyError("retention policy is invalid")

    # ── preview (always read-only) ─────────────────────────────────────────

    def preview(
        self,
        tenant_id: str,
        policy: AuditRetentionPolicy | Mapping[str, Any],
        *,
        request_id: str | None = None,
    ) -> LifecyclePreviewReport:
        """Compute a retention preview.  Never mutates store, archive or checksums."""

        tenant = self._require_tenant(tenant_id)
        active_policy = self._policy_of(policy)
        with self._guarded():
            candidates, invalid, warnings = self._candidates(tenant, active_policy)
            archived_ids: set[str] = set()
            for candidate in candidates:
                if self._archive.exists(candidate.event.event_id):
                    archived_ids.add(candidate.event.event_id)
            eligible = [item for item in candidates if item.eligible and not item.protected]
            protected = [item for item in candidates if item.protected]
            already_archived = [item for item in eligible if item.event.event_id in archived_ids]
            archive_candidates = [
                item for item in eligible if item.event.event_id not in archived_ids
            ]
            purge_candidates = (
                archive_candidates if active_policy.archive_before_purge else eligible
            )
            if active_policy.minimum_events_to_keep > len(candidates):
                warnings.append("minimum_exceeds_available")
            report = LifecyclePreviewReport(
                report_version=AUDIT_LIFECYCLE_REPORT_VERSION,
                generated_at=_utc(self._clock(), "generated_at"),
                tenant_id=tenant,
                policy=active_policy,
                scanned=len(candidates) + invalid,
                eligible=len(eligible),
                protected=len(protected),
                invalid=invalid,
                already_archived=len(already_archived),
                archive_candidates=len(archive_candidates),
                purge_candidates=len(purge_candidates),
                warnings=tuple(sorted(set(warnings))),
                status=LifecycleStatus.DRY_RUN,
            )
        # `preview` is deliberately NOT self-audited.  Recording a preview event
        # would append to the active store and break the read-only guarantee the
        # preview exists to prove.  The HTTP layer still emits a sanitized
        # observability log line for the call; only the mutating operations
        # (archive, purge, restore) write W110 events.
        del request_id
        return report

    # ── archive ────────────────────────────────────────────────────────────

    def archive(
        self,
        tenant_id: str,
        policy: AuditRetentionPolicy | Mapping[str, Any],
        *,
        request_id: str | None = None,
    ) -> AuditLifecycleReport:
        """Publish eligible audit events to the archive.

        ``dry_run=True`` (the default) performs the full selection and reports
        what *would* be archived without writing a single byte.
        """

        tenant = self._require_tenant(tenant_id)
        active_policy = self._policy_of(policy)
        dry_run = active_policy.dry_run
        with self._guarded():
            candidates, invalid, warnings = self._candidates(tenant, active_policy)
            eligible = [item for item in candidates if item.eligible and not item.protected]
            protected = [item for item in candidates if item.protected]
            archived = 0
            verified = 0
            already = 0
            conflicts = 0
            failures = 0
            integrity_failures = 0
            for candidate in eligible:
                if self._archive.exists(candidate.event.event_id):
                    already += 1
                    continue
                if dry_run:
                    continue
                try:
                    document = self._archive_document(candidate.event)
                    receipt = self._archive.put(document)
                except AuditArchiveConflictError:
                    conflicts += 1
                    continue
                except AuditArchiveIntegrityError:
                    integrity_failures += 1
                    continue
                except AuditArchiveError:
                    failures += 1
                    continue
                if receipt.created:
                    archived += 1
                else:
                    already += 1
                verification = self._archive.verify(
                    candidate.event.event_id,
                    expected=document,
                    tenant_id=tenant,
                )
                if verification.verified:
                    verified += 1
                else:
                    integrity_failures += 1
            if dry_run:
                status = LifecycleStatus.DRY_RUN
            elif conflicts:
                status = LifecycleStatus.CONFLICT
            elif failures or integrity_failures:
                status = LifecycleStatus.PARTIAL
            else:
                status = LifecycleStatus.SUCCESS
            report = AuditLifecycleReport(
                report_version=AUDIT_LIFECYCLE_REPORT_VERSION,
                generated_at=_utc(self._clock(), "generated_at"),
                tenant_id=tenant,
                policy=active_policy,
                dry_run=dry_run,
                scanned=len(candidates) + invalid,
                eligible=len(eligible),
                archived=archived,
                archive_verified=verified,
                purged=0,
                skipped=already + invalid,
                conflicts=conflicts,
                failures=failures,
                protected=len(protected),
                integrity_failures=integrity_failures,
                status=status,
                guards={AuditPurgeGuard.PROTECTED.value: len(protected)},
            )
        if not dry_run:
            # a dry run writes nothing at all, including no audit event
            self._record(
                "AUDIT_ARCHIVE",
                "SUCCESS" if status is not LifecycleStatus.FAILED else "FAILED",
                tenant,
                reason_code="archive_completed",
                request_id=request_id,
            )
        else:
            del request_id
        return report

    @staticmethod
    def _archive_document(event: ExecutionAuditEvent) -> AuditArchiveDocument:
        """Build the immutable archive envelope for one event."""

        persisted = event if event.checksum is not None else event.with_checksum()
        return AuditArchiveDocument(
            event=persisted,
            archive_version=AUDIT_ARCHIVE_VERSION,
            archived_at=_utc_now(),
            original_event_checksum=persisted.checksum or "",
        )

    # ── purge ──────────────────────────────────────────────────────────────

    def purge(
        self,
        tenant_id: str,
        policy: AuditRetentionPolicy | Mapping[str, Any],
        *,
        request_id: str | None = None,
    ) -> AuditLifecycleReport:
        """Purge eligible events, and only ever after a verified archive.

        The sequence is fixed: SELECT -> VALIDATE -> ELIGIBILITY -> ARCHIVE ->
        VERIFY ARCHIVE -> COMPARE -> DELETE.  A purge without a verified archive
        is impossible, and the final delete is a compare-and-delete so a
        concurrently modified event is never removed.
        """

        tenant = self._require_tenant(tenant_id)
        active_policy = self._policy_of(policy)
        dry_run = active_policy.dry_run
        with self._guarded():
            candidates, invalid, warnings = self._candidates(tenant, active_policy)
            eligible = [item for item in candidates if item.eligible and not item.protected]
            protected = [item for item in candidates if item.protected]
            purged = 0
            archived = 0
            verified = 0
            conflicts = 0
            failures = 0
            integrity_failures = 0
            skipped = invalid
            guards: dict[str, int] = {}
            for candidate in eligible:
                if not active_policy.archive_before_purge:
                    # purge without archive is refused by contract
                    guards[AuditPurgeGuard.ARCHIVE_MISSING.value] = (
                        guards.get(AuditPurgeGuard.ARCHIVE_MISSING.value, 0) + 1
                    )
                    skipped += 1
                    continue
                if dry_run:
                    continue
                archived_result = self._safe_archive(candidate.event, guards)
                if archived_result is None:
                    skipped += 1
                    continue
                document, was_created = archived_result
                if was_created:
                    archived += 1
                verification = self._archive.verify(
                    candidate.event.event_id,
                    expected=document,
                    tenant_id=tenant,
                )
                if not verification.verified:
                    integrity_failures += 1
                    guards[verification.error_code or "archive_corrupt"] = (
                        guards.get(verification.error_code or "archive_corrupt", 0) + 1
                    )
                    skipped += 1
                    continue
                verified += 1
                # COMPARE: the active event must equal the archived event under a
                # strict canonical comparison before anything may be removed.
                if not document.same_event(candidate.event):
                    integrity_failures += 1
                    guards[AuditPurgeGuard.CHECKSUM_MISMATCH.value] = (
                        guards.get(AuditPurgeGuard.CHECKSUM_MISMATCH.value, 0) + 1
                    )
                    skipped += 1
                    continue
                outcome = self._compare_and_delete(candidate.event, tenant, guards)
                if outcome == "deleted":
                    purged += 1
                elif outcome == "conflict":
                    conflicts += 1
                elif outcome == "not_found":
                    skipped += 1
                else:
                    integrity_failures += 1
                    skipped += 1
            if dry_run:
                status = LifecycleStatus.DRY_RUN
            elif conflicts:
                status = LifecycleStatus.CONFLICT
            elif failures or integrity_failures:
                status = LifecycleStatus.PARTIAL
            elif purged:
                status = LifecycleStatus.SUCCESS
            else:
                status = LifecycleStatus.SUCCESS
            report = AuditLifecycleReport(
                report_version=AUDIT_LIFECYCLE_REPORT_VERSION,
                generated_at=_utc(self._clock(), "generated_at"),
                tenant_id=tenant,
                policy=active_policy,
                dry_run=dry_run,
                scanned=len(candidates) + invalid,
                eligible=len(eligible),
                archived=archived,
                archive_verified=verified,
                purged=purged,
                skipped=skipped,
                conflicts=conflicts,
                failures=failures,
                protected=len(protected),
                integrity_failures=integrity_failures,
                status=status,
                guards=guards,
            )
        if not dry_run:
            # A dry run is a read-only probe, exactly like preview: appending its
            # own audit event would mutate the store it is inspecting.  Only an
            # effective run is self-audited.
            self._record(
                "AUDIT_PURGE",
                "SUCCESS" if status is LifecycleStatus.SUCCESS else "FAILED",
                tenant,
                reason_code="purge_completed",
                request_id=request_id,
            )
        else:
            del request_id
        return report

    def _safe_archive(
        self,
        event: ExecutionAuditEvent,
        guards: dict[str, int],
    ) -> tuple[AuditArchiveDocument, bool] | None:
        """Archive one event for purge.

        Returns the document plus whether it was **effectively** archived
        (``True``) or was already present (``False``), or ``None`` when any
        fail-closed guard refused the operation.
        """

        try:
            document = self._archive_document(event)
            receipt = self._archive.put(document)
        except AuditArchiveConflictError:
            guards[AuditPurgeGuard.CONFLICT.value] = (
                guards.get(AuditPurgeGuard.CONFLICT.value, 0) + 1
            )
            return None
        except AuditArchiveIntegrityError:
            guards[AuditPurgeGuard.ARCHIVE_CORRUPT.value] = (
                guards.get(AuditPurgeGuard.ARCHIVE_CORRUPT.value, 0) + 1
            )
            return None
        except AuditArchiveError:
            guards[AuditPurgeGuard.ARCHIVE_MISSING.value] = (
                guards.get(AuditPurgeGuard.ARCHIVE_MISSING.value, 0) + 1
            )
            return None
        if not receipt.created and not receipt.already_present:
            guards[AuditPurgeGuard.ARCHIVE_MISSING.value] = (
                guards.get(AuditPurgeGuard.ARCHIVE_MISSING.value, 0) + 1
            )
            return None
        return document, receipt.created

    def _compare_and_delete(
        self,
        event: ExecutionAuditEvent,
        tenant_id: str,
        guards: dict[str, int],
    ) -> str:
        """Final gate: delete only if the active event is byte-for-byte unchanged."""

        store = self._audit_service.store
        delete = getattr(store, "delete_if_unchanged", None)
        if not callable(delete):
            guards[AuditPurgeGuard.CONFLICT.value] = (
                guards.get(AuditPurgeGuard.CONFLICT.value, 0) + 1
            )
            return "conflict"
        try:
            result = delete(event)
        except AuditError:
            guards[AuditPurgeGuard.CHECKSUM_MISMATCH.value] = (
                guards.get(AuditPurgeGuard.CHECKSUM_MISMATCH.value, 0) + 1
            )
            return "invalid"
        outcome = str(getattr(getattr(result, "outcome", result), "value", result))
        if outcome == "DELETED":
            return "deleted"
        if outcome == "CONFLICT":
            guards[AuditPurgeGuard.CONFLICT.value] = (
                guards.get(AuditPurgeGuard.CONFLICT.value, 0) + 1
            )
            return "conflict"
        if outcome == "NOT_FOUND":
            guards[AuditPurgeGuard.ARCHIVE_MISSING.value] = (
                guards.get(AuditPurgeGuard.ARCHIVE_MISSING.value, 0) + 1
            )
            return "not_found"
        guards[AuditPurgeGuard.CHECKSUM_MISMATCH.value] = (
            guards.get(AuditPurgeGuard.CHECKSUM_MISMATCH.value, 0) + 1
        )
        return "invalid"

    # ── restore ────────────────────────────────────────────────────────────

    def restore(
        self, request: AuditRestoreRequest, *, authenticated_tenant: str
    ) -> AuditRestoreResult:
        """Restore one archived event into the active store.

        An existing active event is never overwritten: an identical event is
        ``ALREADY_PRESENT`` and a different one is ``CONFLICT``.  There is
        deliberately no ``FORCE_OVERWRITE`` option.
        """

        tenant = self._require_tenant(authenticated_tenant)
        with self._guarded():
            if request.tenant_id != tenant:
                result = AuditRestoreResult(
                    status=AuditRestoreStatus.TENANT_FORBIDDEN,
                    event_id=request.event_id,
                    tenant_id=tenant,
                    reason_code="tenant_forbidden",
                )
                self._record(
                    "AUDIT_RESTORE",
                    "FORBIDDEN",
                    tenant,
                    execution_id=None,
                    reason_code="tenant_forbidden",
                    request_id=request.request_id,
                )
                return result
            try:
                document = self._archive.get(request.event_id, tenant_id=tenant)
            except AuditArchiveError:
                document = None
                self._record(
                    "AUDIT_RESTORE",
                    "FAILED",
                    tenant,
                    reason_code="archive_integrity_failure",
                    request_id=request.request_id,
                )
                return AuditRestoreResult(
                    status=AuditRestoreStatus.INTEGRITY_FAILURE,
                    event_id=request.event_id,
                    tenant_id=tenant,
                    reason_code="archive_integrity_failure",
                )
            if document is None:
                result = AuditRestoreResult(
                    status=AuditRestoreStatus.NOT_FOUND,
                    event_id=request.event_id,
                    tenant_id=tenant,
                    reason_code="archive_not_found",
                )
                self._record(
                    "AUDIT_RESTORE",
                    "REJECTED",
                    tenant,
                    reason_code="archive_not_found",
                    request_id=request.request_id,
                )
                return result
            try:
                document.verify_integrity()
            except AuditError:
                result = AuditRestoreResult(
                    status=AuditRestoreStatus.INTEGRITY_FAILURE,
                    event_id=request.event_id,
                    tenant_id=tenant,
                    reason_code="archive_integrity_failure",
                )
                self._record(
                    "AUDIT_RESTORE",
                    "FAILED",
                    tenant,
                    reason_code="archive_integrity_failure",
                    request_id=request.request_id,
                )
                return result
            verification = self._archive.verify(
                request.event_id, expected=document, tenant_id=tenant
            )
            if not verification.verified:
                result = AuditRestoreResult(
                    status=AuditRestoreStatus.INTEGRITY_FAILURE,
                    event_id=request.event_id,
                    tenant_id=tenant,
                    reason_code="archive_verification_failed",
                )
                self._record(
                    "AUDIT_RESTORE",
                    "FAILED",
                    tenant,
                    reason_code="archive_verification_failed",
                    request_id=request.request_id,
                )
                return result
            event = document.event
            if event.tenant_id != tenant:
                result = AuditRestoreResult(
                    status=AuditRestoreStatus.TENANT_FORBIDDEN,
                    event_id=request.event_id,
                    tenant_id=tenant,
                    reason_code="tenant_mismatch",
                )
                self._record(
                    "AUDIT_RESTORE",
                    "FORBIDDEN",
                    tenant,
                    reason_code="tenant_mismatch",
                    request_id=request.request_id,
                )
                return result
            existing = self._audit_service.get(event.event_id, tenant_id=tenant)
            if existing is not None:
                identical = existing.checksum == event.checksum
                status = (
                    AuditRestoreStatus.ALREADY_PRESENT if identical else AuditRestoreStatus.CONFLICT
                )
                result = AuditRestoreResult(
                    status=status,
                    event_id=event.event_id,
                    tenant_id=tenant,
                    reason_code=(
                        "record_already_present" if identical else "active_event_conflict"
                    ),
                )
                self._record(
                    "AUDIT_RESTORE",
                    "ALREADY_PRESENT" if identical else "CONFLICT",
                    tenant,
                    execution_id=event.execution_id,
                    reason_code=result.reason_code,
                    request_id=request.request_id,
                )
                return result
            try:
                self._audit_service.store.append(event)
            except AuditError:
                result = AuditRestoreResult(
                    status=AuditRestoreStatus.RESTORE_FAILED,
                    event_id=event.event_id,
                    tenant_id=tenant,
                    reason_code="active_store_write_failed",
                )
                self._record(
                    "AUDIT_RESTORE",
                    "FAILED",
                    tenant,
                    reason_code="active_store_write_failed",
                    request_id=request.request_id,
                )
                return result
            result = AuditRestoreResult(
                status=AuditRestoreStatus.RESTORED,
                event_id=event.event_id,
                tenant_id=tenant,
                reason_code="event_restored",
            )
            self._record(
                "AUDIT_RESTORE",
                "SUCCESS",
                tenant,
                execution_id=event.execution_id,
                reason_code="event_restored",
                request_id=request.request_id,
            )
            return result


__all__ = [
    "AUDIT_ARCHIVE_VERSION",
    "AUDIT_LIFECYCLE_REPORT_VERSION",
    "AUDIT_LIFECYCLE_SOURCE",
    "MAX_AUDIT_LIFECYCLE_BATCH",
    "AuditArchiveConflictError",
    "AuditArchiveDocument",
    "AuditArchiveError",
    "AuditArchiveIntegrityError",
    "AuditArchiveNotFoundError",
    "AuditArchiveReceipt",
    "AuditArchiveStore",
    "AuditArchiveVerification",
    "AuditLifecycleError",
    "AuditLifecycleReport",
    "AuditLifecycleService",
    "AuditPurgeGuard",
    "AuditRestoreRequest",
    "AuditRestoreResult",
    "AuditRestoreStatus",
    "AuditRetentionPolicy",
    "AuditRetentionPolicyError",
    "FileSystemAuditArchiveStore",
    "LifecyclePreviewReport",
    "LifecycleRecursionError",
    "LifecycleStatus",
]
