"""W107 — explicit lifecycle, archive, and controlled purge for W100 history.

Lifecycle is a write-capable maintenance boundary around the existing store
abstraction.  It never parses the W100 datastore itself, never changes the
history/analytics/reporting services, and defaults to a read-only preview.
The only deletion primitive is an atomic compare-and-delete supplied by the
store implementation.
"""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import stat
import tempfile
from collections.abc import Callable, Iterable, Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Protocol

from loguru import logger
from trendx.forecasting.audit import (
    AuditActorType,
    AuditOperation,
    AuditOutcome,
    ExecutionAuditService,
)
from trendx.forecasting.execution import (
    ExecutionRecord,
    ExecutionStatus,
    ExecutionStore,
    ExecutionStoreError,
)
from trendx.forecasting.history import ExecutionHistoryReader

ARCHIVE_FORMAT_VERSION = 1
_EXECUTION_RECORD_FIELDS = frozenset(ExecutionRecord.__dataclass_fields__)
_ARCHIVE_DOCUMENT_FIELDS = frozenset(
    {"archive_format_version", "metadata", "record", "content_checksum"}
)
_ARCHIVE_METADATA_FIELDS = frozenset(
    {
        "archive_format_version",
        "reason",
        "policy",
        "eligible_at",
        "archived_at",
        "execution_id",
        "tenant_id",
        "record_checksum",
    }
)
_POLICY_FIELDS = frozenset(
    {"retention_days", "archive_before_purge", "minimum_records_to_keep", "dry_run"}
)


class LifecycleError(Exception):
    """Base class for controlled lifecycle failures."""


class InvalidRetentionPolicyError(LifecycleError, ValueError):
    """The retention policy is not safe or well-formed."""


class LifecycleTenantError(LifecycleError):
    """The requested record or tenant is not authorized by the lifecycle scope."""


class LifecycleProtectedError(LifecycleError):
    """A record is not eligible for archive or purge."""


class ArchiveError(LifecycleError):
    """Base class for archive failures."""


class ArchiveNotFoundError(ArchiveError):
    """The requested archive does not exist."""


class ArchiveIntegrityError(ArchiveError):
    """The archive exists but failed deterministic integrity validation."""


class ArchiveConflictError(ArchiveError):
    """An existing archive describes different data or lifecycle metadata."""


class PurgeableExecutionStore(ExecutionStore, Protocol):
    """ExecutionStore plus the W107 atomic compare-and-delete capability."""

    def delete_if_unchanged(
        self,
        execution_id: str,
        expected: ExecutionRecord,
    ) -> bool: ...


class ArchiveStore(Protocol):
    """Local or remote archive boundary used by lifecycle."""

    def archive(
        self,
        records: Iterable[ExecutionRecord],
        *,
        policy: ExecutionRetentionPolicy,
        archived_at: datetime,
        reason: str = "retention",
    ) -> tuple[ArchiveReceipt, ...]: ...

    def exists(self, execution_id: str) -> bool: ...

    def read(self, execution_id: str) -> ArchivedExecution: ...

    def verify(
        self,
        execution_id: str,
        *,
        expected_record: ExecutionRecord | None = None,
        expected_policy: ExecutionRetentionPolicy | None = None,
    ) -> ArchiveVerification: ...


@dataclass(frozen=True)
class ExecutionRetentionPolicy:
    """Explicit lifecycle policy.

    ``retention_days`` has no default because W107 must not invent a business
    retention period.  The other defaults are deliberately conservative:
    archive-before-purge is enabled, no minimum is imposed, and every
    operation is read-only unless the caller explicitly supplies
    ``dry_run=False``.
    """

    retention_days: int
    archive_before_purge: bool = True
    minimum_records_to_keep: int = 0
    dry_run: bool = True

    def __post_init__(self) -> None:
        if isinstance(self.retention_days, bool) or not isinstance(self.retention_days, int):
            msg = "retention_days must be an integer"
            raise InvalidRetentionPolicyError(msg)
        if self.retention_days < 0:
            msg = "retention_days must be non-negative"
            raise InvalidRetentionPolicyError(msg)
        try:
            timedelta(days=self.retention_days)
        except (OverflowError, ValueError) as exc:
            msg = "retention_days is outside the supported timestamp range"
            raise InvalidRetentionPolicyError(msg) from exc
        if (
            isinstance(self.minimum_records_to_keep, bool)
            or not isinstance(self.minimum_records_to_keep, int)
            or self.minimum_records_to_keep < 0
        ):
            msg = "minimum_records_to_keep must be a non-negative integer"
            raise InvalidRetentionPolicyError(msg)
        if not isinstance(self.archive_before_purge, bool):
            msg = "archive_before_purge must be a boolean"
            raise InvalidRetentionPolicyError(msg)
        if not isinstance(self.dry_run, bool):
            msg = "dry_run must be a boolean"
            raise InvalidRetentionPolicyError(msg)

    def to_dict(self) -> dict[str, int | bool]:
        return {
            "retention_days": self.retention_days,
            "archive_before_purge": self.archive_before_purge,
            "minimum_records_to_keep": self.minimum_records_to_keep,
            "dry_run": self.dry_run,
        }


@dataclass(frozen=True)
class ArchiveMetadata:
    """Auditable metadata stored beside an archived record."""

    archive_format_version: int
    reason: str
    policy: ExecutionRetentionPolicy
    eligible_at: datetime
    archived_at: datetime
    execution_id: str
    tenant_id: str
    record_checksum: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "archive_format_version": self.archive_format_version,
            "reason": self.reason,
            "policy": self.policy.to_dict(),
            "eligible_at": _format_timestamp(self.eligible_at),
            "archived_at": _format_timestamp(self.archived_at),
            "execution_id": self.execution_id,
            "tenant_id": self.tenant_id,
            "record_checksum": self.record_checksum,
        }

    @property
    def checksum(self) -> str:
        """Alias for the expected record checksum."""

        return self.record_checksum

    @property
    def expected_checksum(self) -> str:
        """Explicit alias used by audit consumers."""

        return self.record_checksum

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> ArchiveMetadata:
        if not isinstance(data, Mapping) or set(data) != _ARCHIVE_METADATA_FIELDS:
            raise ArchiveIntegrityError("archive metadata is malformed")
        try:
            policy_data = data["policy"]
            if not isinstance(policy_data, Mapping) or set(policy_data) != _POLICY_FIELDS:
                raise ArchiveIntegrityError("archive policy is malformed")
            policy = ExecutionRetentionPolicy(
                retention_days=_strict_int(policy_data["retention_days"], "retention_days"),
                archive_before_purge=_strict_bool(
                    policy_data["archive_before_purge"], "archive_before_purge"
                ),
                minimum_records_to_keep=_strict_int(
                    policy_data["minimum_records_to_keep"], "minimum_records_to_keep"
                ),
                dry_run=_strict_bool(policy_data["dry_run"], "dry_run"),
            )
            execution_id = data["execution_id"]
            tenant_id = data["tenant_id"]
            reason = data["reason"]
            record_checksum = data["record_checksum"]
            if not all(
                isinstance(item, str) and item for item in (execution_id, tenant_id, reason)
            ):
                msg = "archive identity metadata is invalid"
                raise ArchiveIntegrityError(msg)
            if not _is_sha256(record_checksum):
                msg = "archive checksum metadata is invalid"
                raise ArchiveIntegrityError(msg)
            return cls(
                archive_format_version=_strict_int(
                    data["archive_format_version"], "archive_format_version"
                ),
                reason=reason,
                policy=policy,
                eligible_at=_parse_timestamp(data["eligible_at"], "eligible_at"),
                archived_at=_parse_timestamp(data["archived_at"], "archived_at"),
                execution_id=execution_id,
                tenant_id=tenant_id,
                record_checksum=record_checksum,
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise ArchiveIntegrityError("archive metadata is incomplete") from exc


@dataclass(frozen=True)
class ArchiveReceipt:
    """Result of a successful archive operation."""

    execution_id: str
    tenant_id: str
    record_checksum: str
    created: bool

    @property
    def checksum(self) -> str:
        return self.record_checksum


@dataclass(frozen=True)
class ArchiveVerification:
    """Integrity result without exposing archive contents or filesystem paths."""

    valid: bool
    checksum: str | None = None
    error_code: str | None = None


@dataclass(frozen=True)
class ArchivedExecution:
    """Verified archived record and its lifecycle metadata."""

    record: ExecutionRecord
    metadata: ArchiveMetadata


@dataclass(frozen=True)
class LifecycleFailure:
    """Sanitized failure associated with one lifecycle candidate or scan."""

    execution_id: str | None
    stage: str
    error_code: str


@dataclass(frozen=True)
class LifecycleDecision:
    """Explainable, read-only decision for one tenant record."""

    execution_id: str
    state: str
    reason: str
    eligible_at: str | None

    def to_dict(self) -> dict[str, str | None]:
        return {
            "execution_id": self.execution_id,
            "state": self.state,
            "reason": self.reason,
            "eligible_at": self.eligible_at,
        }


@dataclass(frozen=True)
class _Classification:
    record: ExecutionRecord
    decision: LifecycleDecision

    @property
    def state(self) -> str:
        return self.decision.state


@dataclass(frozen=True)
class ExecutionLifecycleReport:
    """Serializable result of preview, archive, or purge."""

    tenant_id: str
    policy: ExecutionRetentionPolicy
    generated_at: str
    scanned: int
    eligible: int
    protected: int
    archived: int
    purged: int
    already_purged: int
    skipped: int
    failed: int
    dry_run: bool
    archive_candidates: tuple[str, ...] = ()
    already_archived: tuple[str, ...] = ()
    archived_ids: tuple[str, ...] = ()
    purged_ids: tuple[str, ...] = ()
    already_purged_ids: tuple[str, ...] = ()
    protected_ids: tuple[str, ...] = ()
    skipped_ids: tuple[str, ...] = ()
    decisions: tuple[LifecycleDecision, ...] = ()
    failures: tuple[LifecycleFailure, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "tenant_id": self.tenant_id,
            "policy": self.policy.to_dict(),
            "generated_at": self.generated_at,
            "scanned": self.scanned,
            "eligible": self.eligible,
            "protected": self.protected,
            "archived": self.archived,
            "purged": self.purged,
            "already_purged": self.already_purged,
            "skipped": self.skipped,
            "failed": self.failed,
            "dry_run": self.dry_run,
            "archive_candidates": list(self.archive_candidates),
            "already_archived": list(self.already_archived),
            "archived_ids": list(self.archived_ids),
            "purged_ids": list(self.purged_ids),
            "already_purged_ids": list(self.already_purged_ids),
            "protected_ids": list(self.protected_ids),
            "skipped_ids": list(self.skipped_ids),
            "decisions": [item.to_dict() for item in self.decisions],
            "failures": [
                {
                    "execution_id": item.execution_id,
                    "stage": item.stage,
                    "error_code": item.error_code,
                }
                for item in self.failures
            ],
        }


def _strict_int(value: Any, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        msg = f"{field_name} is not an integer"
        raise ArchiveIntegrityError(msg)
    return int(value)


def _strict_bool(value: Any, field_name: str) -> bool:
    if not isinstance(value, bool):
        msg = f"{field_name} is not a boolean"
        raise ArchiveIntegrityError(msg)
    return value


def _is_sha256(value: Any) -> bool:
    return (
        isinstance(value, str)
        and len(value) == 64
        and all(character in "0123456789abcdef" for character in value)
    )


def _parse_timestamp(value: Any, field_name: str) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        raw = value.strip()
        if raw.endswith("Z"):
            raw = f"{raw[:-1]}+00:00"
        try:
            parsed = datetime.fromisoformat(raw)
        except ValueError as exc:
            msg = f"{field_name} is not a valid timestamp"
            raise ArchiveIntegrityError(msg) from exc
    else:
        msg = f"{field_name} is not a timestamp"
        raise ArchiveIntegrityError(msg)
    return parsed.replace(tzinfo=UTC) if parsed.tzinfo is None else parsed.astimezone(UTC)


def _format_timestamp(value: datetime) -> str:
    normalized = value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)
    return normalized.isoformat()


def _status_value(record: ExecutionRecord) -> str:
    status = record.status
    return status.value if isinstance(status, ExecutionStatus) else str(status)


def _reject_json_constant(value: str) -> None:
    raise ValueError(f"unsupported JSON constant: {value}")


def _strict_json_loads(value: str) -> Any:
    return json.loads(value, parse_constant=_reject_json_constant)


def _canonical_json(value: Mapping[str, Any]) -> str:
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
    except (TypeError, ValueError) as exc:
        raise ArchiveError("archive payload is not strict JSON") from exc


def _strict_payload_equal(left: Mapping[str, Any], right: Mapping[str, Any]) -> bool:
    try:
        return _canonical_json(left) == _canonical_json(right)
    except ArchiveError:
        return False


def _record_checksum(record: ExecutionRecord) -> str:
    try:
        encoded = _canonical_json(record.to_dict()).encode("utf-8")
    except (ArchiveError, UnicodeEncodeError) as exc:
        raise ArchiveError("record payload is not strict UTF-8 JSON") from exc
    return hashlib.sha256(encoded).hexdigest()


def _eligible_at(record: ExecutionRecord, policy: ExecutionRetentionPolicy) -> datetime:
    created_at = _parse_timestamp(record.created_at, "created_at")
    try:
        return created_at + timedelta(days=policy.retention_days)
    except OverflowError as exc:
        raise ArchiveIntegrityError("eligible_at is outside the supported range") from exc


def _document_checksum(metadata: Mapping[str, Any], record: Mapping[str, Any]) -> str:
    try:
        encoded = _canonical_json({"metadata": metadata, "record": record}).encode("utf-8")
    except (ArchiveError, UnicodeEncodeError) as exc:
        raise ArchiveIntegrityError("archive document is not strict JSON") from exc
    return hashlib.sha256(encoded).hexdigest()


def _safe_archive_key(execution_id: str) -> str:
    if not isinstance(execution_id, str) or not execution_id:
        msg = "execution_id is required for archive"
        raise ArchiveError(msg)
    try:
        encoded = execution_id.encode("utf-8")
    except UnicodeEncodeError as exc:
        raise ArchiveError("execution_id is not valid UTF-8") from exc
    return hashlib.sha256(encoded).hexdigest()


class FileSystemArchiveStore:
    """Deterministic local JSON archive with atomic per-record writes.

    This adapter verifies and stores records but does not decide retention
    eligibility; callers must use :class:`ExecutionHistoryLifecycle` for the
    protected-status and archive-before-purge policy.
    """

    def __init__(
        self,
        root: str | Path,
        *,
        replace: Callable[[Path, Path], None] | None = None,
        sync_directory: Callable[[Path], None] | None = None,
    ) -> None:
        self._root = Path(root)
        if self._root.is_symlink():
            msg = "archive root must not be a symlink"
            raise ArchiveError(msg)
        self._replace = replace or os.replace
        self._sync_directory = sync_directory or self._fsync_directory

    @property
    def root(self) -> Path:
        return self._root

    def _ensure_root(self) -> None:
        if self._root.is_symlink():
            raise ArchiveError("archive root must not be a symlink")
        try:
            self._root.mkdir(parents=True, exist_ok=True)
            if not self._root.is_dir():
                raise ArchiveError("archive root is not a directory")
            os.chmod(self._root, 0o700)
        except ArchiveError:
            raise
        except OSError as exc:
            raise ArchiveError("unable to secure archive directory") from exc
        self._assert_secure_directory(self._root)

    @staticmethod
    def _fsync_directory(path: Path) -> None:
        flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
        try:
            directory_fd = os.open(path, flags)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
        except OSError as exc:
            raise ArchiveError("unable to sync archive directory") from exc

    @staticmethod
    def _assert_secure_directory(path: Path) -> None:
        try:
            metadata = path.stat()
        except OSError as exc:
            raise ArchiveIntegrityError("archive directory is unavailable") from exc
        mode = stat.S_IMODE(metadata.st_mode)
        if metadata.st_uid != os.geteuid() or mode & 0o077:
            raise ArchiveIntegrityError("archive directory permissions are insecure")

    @staticmethod
    def _assert_secure_file(path: Path) -> None:
        try:
            metadata = path.stat()
        except OSError as exc:
            raise ArchiveIntegrityError("archive file is unavailable") from exc
        mode = stat.S_IMODE(metadata.st_mode)
        if metadata.st_uid != os.geteuid() or mode & 0o077:
            raise ArchiveIntegrityError("archive file permissions are insecure")

    def _path_for(self, execution_id: str) -> Path:
        return self._root / f"{_safe_archive_key(execution_id)}.json"

    def _lock_path_for(self, execution_id: str) -> Path:
        return self._root / f".{_safe_archive_key(execution_id)}.lock"

    def exists(self, execution_id: str) -> bool:
        path = self._path_for(execution_id)
        return path.is_file() and not path.is_symlink()

    def read(self, execution_id: str) -> ArchivedExecution:
        document = self._load(execution_id)
        return document

    def verify(
        self,
        execution_id: str,
        *,
        expected_record: ExecutionRecord | None = None,
        expected_policy: ExecutionRetentionPolicy | None = None,
    ) -> ArchiveVerification:
        try:
            archived = self.read(execution_id)
        except ArchiveNotFoundError:
            return ArchiveVerification(valid=False, error_code="missing_archive")
        except ArchiveIntegrityError as exc:
            return ArchiveVerification(valid=False, error_code=_integrity_error_code(str(exc)))
        except (OSError, UnicodeError):
            return ArchiveVerification(valid=False, error_code="archive_read_failed")
        if expected_record is not None and archived.record.to_dict() != expected_record.to_dict():
            return ArchiveVerification(valid=False, error_code="record_mismatch")
        if expected_policy is not None and archived.metadata.policy != expected_policy:
            return ArchiveVerification(valid=False, error_code="policy_mismatch")
        return ArchiveVerification(
            valid=True,
            checksum=archived.metadata.record_checksum,
        )

    def archive(
        self,
        records: Iterable[ExecutionRecord],
        *,
        policy: ExecutionRetentionPolicy,
        archived_at: datetime,
        reason: str = "retention",
    ) -> tuple[ArchiveReceipt, ...]:
        if policy.dry_run:
            msg = "archive store cannot write during dry-run"
            raise ArchiveError(msg)
        self._ensure_root()
        archived_at = _parse_timestamp(archived_at, "archived_at")
        try:
            return tuple(
                self._archive_one(record, policy=policy, archived_at=archived_at, reason=reason)
                for record in records
            )
        except ArchiveError:
            raise
        except (OSError, RuntimeError, TypeError, ValueError) as exc:
            raise ArchiveError("archive operation failed") from exc

    def _archive_one(
        self,
        record: ExecutionRecord,
        *,
        policy: ExecutionRetentionPolicy,
        archived_at: datetime,
        reason: str,
    ) -> ArchiveReceipt:
        checksum = _record_checksum(record)
        eligible_at = _eligible_at(record, policy)
        with self._locked(record.execution_id):
            existing = self._load_if_present(record.execution_id)
            if existing is not None:
                if existing.record.to_dict() != record.to_dict():
                    raise ArchiveConflictError("archive contains a different record")
                if existing.metadata.policy != policy:
                    raise ArchiveConflictError("archive contains a different policy")
                if existing.metadata.reason != reason:
                    raise ArchiveConflictError("archive contains a different reason")
                if existing.metadata.record_checksum != checksum:
                    raise ArchiveIntegrityError("archive checksum does not match record")
                return ArchiveReceipt(
                    execution_id=record.execution_id,
                    tenant_id=record.tenant_id,
                    record_checksum=checksum,
                    created=False,
                )
            metadata = ArchiveMetadata(
                archive_format_version=ARCHIVE_FORMAT_VERSION,
                reason=reason,
                policy=policy,
                eligible_at=eligible_at,
                archived_at=archived_at,
                execution_id=record.execution_id,
                tenant_id=record.tenant_id,
                record_checksum=checksum,
            )
            metadata_data = metadata.to_dict()
            record_data = record.to_dict()
            document = {
                "archive_format_version": ARCHIVE_FORMAT_VERSION,
                "metadata": metadata_data,
                "record": record_data,
                "content_checksum": _document_checksum(metadata_data, record_data),
            }
            self._atomic_write(self._path_for(record.execution_id), document)
            verification = self.verify(
                record.execution_id, expected_record=record, expected_policy=policy
            )
            if not verification.valid:
                raise ArchiveIntegrityError(
                    verification.error_code or "archive verification failed"
                )
            return ArchiveReceipt(
                execution_id=record.execution_id,
                tenant_id=record.tenant_id,
                record_checksum=checksum,
                created=True,
            )

    def _load_if_present(self, execution_id: str) -> ArchivedExecution | None:
        if not self.exists(execution_id):
            return None
        return self._load(execution_id)

    def _load(self, execution_id: str) -> ArchivedExecution:
        path = self._path_for(execution_id)
        if not self.exists(execution_id):
            raise ArchiveNotFoundError("archive does not exist")
        if self._root.is_symlink():
            raise ArchiveIntegrityError("archive root is a symlink")
        self._assert_secure_directory(self._root)
        self._assert_secure_file(path)
        try:
            raw = path.read_text(encoding="utf-8")
            payload = _strict_json_loads(raw)
        except (OSError, UnicodeError, TypeError, ValueError) as exc:
            raise ArchiveIntegrityError("archive_read_failed") from exc
        version = payload.get("archive_format_version") if isinstance(payload, dict) else None
        if (
            not isinstance(payload, dict)
            or set(payload) != _ARCHIVE_DOCUMENT_FIELDS
            or not isinstance(version, int)
            or isinstance(version, bool)
            or version != ARCHIVE_FORMAT_VERSION
        ):
            raise ArchiveIntegrityError("archive format mismatch")
        metadata_data = payload.get("metadata")
        record_data = payload.get("record")
        if not isinstance(metadata_data, dict) or not isinstance(record_data, dict):
            raise ArchiveIntegrityError("archive payload is malformed")
        if set(record_data) != _EXECUTION_RECORD_FIELDS:
            raise ArchiveIntegrityError("archived record fields are incomplete")
        content_checksum = payload.get("content_checksum")
        if not isinstance(content_checksum, str) or content_checksum != _document_checksum(
            metadata_data, record_data
        ):
            raise ArchiveIntegrityError("archive content checksum mismatch")
        metadata = ArchiveMetadata.from_dict(metadata_data)
        if metadata.archive_format_version != ARCHIVE_FORMAT_VERSION:
            raise ArchiveIntegrityError("archive metadata version mismatch")
        if metadata.policy.dry_run:
            raise ArchiveIntegrityError("archive policy is dry-run")
        try:
            record = ExecutionRecord.from_dict(record_data)
        except (TypeError, ValueError) as exc:
            raise ArchiveIntegrityError("archived record is invalid") from exc
        if not _strict_payload_equal(record.to_dict(), record_data):
            raise ArchiveIntegrityError("archived record is not canonical")
        if record.execution_id != metadata.execution_id or record.tenant_id != metadata.tenant_id:
            raise ArchiveIntegrityError("archive identity mismatch")
        if metadata.eligible_at != _eligible_at(record, metadata.policy):
            raise ArchiveIntegrityError("archive eligibility metadata mismatch")
        if _record_checksum(record) != metadata.record_checksum:
            raise ArchiveIntegrityError("archive checksum mismatch")
        return ArchivedExecution(record=record, metadata=metadata)

    def _atomic_write(self, path: Path, document: Mapping[str, Any]) -> None:
        temporary_path: str | None = None
        file_descriptor = -1
        try:
            file_descriptor, temporary_path = tempfile.mkstemp(
                prefix=f".{path.stem}.",
                suffix=".tmp",
                dir=str(self._root),
            )
            os.fchmod(file_descriptor, 0o600)
            with os.fdopen(file_descriptor, "w", encoding="utf-8") as handle:
                file_descriptor = -1
                handle.write(_canonical_json(document))
                handle.write("\n")
                handle.flush()
                os.fsync(handle.fileno())
            self._replace(Path(temporary_path), path)
            temporary_path = None
            self._sync_directory(self._root)
        except ArchiveError:
            raise
        except OSError as exc:
            raise ArchiveError("unable to atomically write archive") from exc
        finally:
            if file_descriptor >= 0:
                os.close(file_descriptor)
            if temporary_path is not None:
                try:
                    os.unlink(temporary_path)
                except FileNotFoundError:
                    pass

    @contextmanager
    def _locked(self, execution_id: str) -> Iterator[None]:
        lock_path = self._lock_path_for(execution_id)
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        handle = lock_path.open("a+")
        try:
            os.chmod(lock_path, 0o600)
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
            handle.close()


class ExecutionHistoryLifecycle:
    """Tenant-scoped lifecycle service with archive-before-purge ordering."""

    def __init__(
        self,
        store: PurgeableExecutionStore,
        archive_store: ArchiveStore,
        clock: Callable[[], datetime],
        *,
        audit_service: ExecutionAuditService | None = None,
    ) -> None:
        self._store = store
        self._reader: ExecutionHistoryReader = store
        self._archive_store = archive_store
        self._clock = clock
        self._audit_service = audit_service

    def _record_audit(
        self,
        *,
        operation: AuditOperation,
        outcome: AuditOutcome,
        tenant_id: str,
        execution_id: str | None,
        reference_key: str | None,
        reason_code: str,
        request_id: str,
    ) -> None:
        if self._audit_service is None:
            return
        try:
            self._audit_service.record(
                operation=operation,
                outcome=outcome,
                tenant_id=tenant_id,
                execution_id=execution_id,
                reference_key=reference_key,
                actor_type=AuditActorType.SYSTEM,
                actor_id="system",
                request_id=request_id,
                source="w107-lifecycle",
                reason_code=reason_code,
            )
        except Exception:
            # Audit is observational and must never change lifecycle semantics.
            return

    def preview(
        self,
        tenant_id: str,
        policy: ExecutionRetentionPolicy,
        *,
        now: datetime | None = None,
        request_id: str | None = None,
    ) -> ExecutionLifecycleReport:
        effective = replace(policy, dry_run=True)
        return self._execute(
            tenant_id,
            effective,
            purge=False,
            now=now,
            outcome="preview",
            request_id=request_id,
        )

    def archive_eligible(
        self,
        tenant_id: str,
        policy: ExecutionRetentionPolicy,
        *,
        now: datetime | None = None,
        request_id: str | None = None,
    ) -> ExecutionLifecycleReport:
        if policy.dry_run:
            self._record_audit(
                operation=AuditOperation.ARCHIVE,
                outcome=AuditOutcome.REJECTED,
                tenant_id=tenant_id,
                execution_id=None,
                reference_key=None,
                reason_code="dry_run",
                request_id=request_id or f"lifecycle:{tenant_id}",
            )
            return self.preview(tenant_id, policy, now=now, request_id=request_id)
        return self._execute(
            tenant_id,
            policy,
            purge=False,
            now=now,
            outcome="archive",
            request_id=request_id,
        )

    def purge_eligible(
        self,
        tenant_id: str,
        policy: ExecutionRetentionPolicy,
        *,
        now: datetime | None = None,
        request_id: str | None = None,
    ) -> ExecutionLifecycleReport:
        if policy.dry_run:
            self._record_audit(
                operation=AuditOperation.PURGE,
                outcome=AuditOutcome.REJECTED,
                tenant_id=tenant_id,
                execution_id=None,
                reference_key=None,
                reason_code="dry_run",
                request_id=request_id or f"lifecycle:{tenant_id}",
            )
            return self.preview(tenant_id, policy, now=now, request_id=request_id)
        return self._execute(
            tenant_id,
            policy,
            purge=True,
            now=now,
            outcome="purge",
            request_id=request_id,
        )

    def archive_execution(
        self,
        execution_id: str,
        tenant_id: str,
        policy: ExecutionRetentionPolicy,
        *,
        now: datetime | None = None,
        request_id: str | None = None,
    ) -> ArchiveReceipt:
        active_request_id = request_id or f"lifecycle:{tenant_id}:{execution_id}"
        try:
            receipt = self._archive_execution(
                execution_id,
                tenant_id,
                policy,
                now=now,
            )
        except LifecycleTenantError:
            self._record_audit(
                operation=AuditOperation.ARCHIVE,
                outcome=AuditOutcome.FORBIDDEN,
                tenant_id=tenant_id,
                execution_id=execution_id,
                reference_key=None,
                reason_code="tenant_forbidden",
                request_id=active_request_id,
            )
            raise
        except LifecycleProtectedError:
            self._record_audit(
                operation=AuditOperation.ARCHIVE,
                outcome=AuditOutcome.REJECTED,
                tenant_id=tenant_id,
                execution_id=execution_id,
                reference_key=None,
                reason_code="retention_protected",
                request_id=active_request_id,
            )
            raise
        except Exception:
            self._record_audit(
                operation=AuditOperation.ARCHIVE,
                outcome=AuditOutcome.FAILED,
                tenant_id=tenant_id,
                execution_id=execution_id,
                reference_key=None,
                reason_code="archive_failed",
                request_id=active_request_id,
            )
            raise
        try:
            source_record = self._reader.get(receipt.execution_id)
        except Exception:
            source_record = None
        self._record_audit(
            operation=AuditOperation.ARCHIVE,
            outcome=AuditOutcome.SUCCESS if receipt.created else AuditOutcome.ALREADY_PRESENT,
            tenant_id=receipt.tenant_id,
            execution_id=receipt.execution_id,
            reference_key=source_record.reference_key if source_record is not None else None,
            reason_code="archive_created" if receipt.created else "archive_already_present",
            request_id=active_request_id,
        )
        return receipt

    def _archive_execution(
        self,
        execution_id: str,
        tenant_id: str,
        policy: ExecutionRetentionPolicy,
        *,
        now: datetime | None = None,
    ) -> ArchiveReceipt:
        if policy.dry_run:
            msg = "archive_execution requires dry_run=false"
            raise LifecycleProtectedError(msg)
        current = self._now(now)
        self._validate_tenant(tenant_id)
        record = self._reader.get(execution_id)
        if record is None:
            raise LifecycleTenantError("execution is not visible in tenant scope")
        if record.tenant_id != tenant_id:
            raise LifecycleTenantError("execution belongs to another tenant")
        tenant_records = tuple(
            candidate for candidate in self._reader.list() if candidate.tenant_id == tenant_id
        )
        decisions = {
            item.record.execution_id: item.state
            for item in self._classify(tenant_records, policy, current)
        }
        if decisions.get(record.execution_id) != "eligible":
            raise LifecycleProtectedError("execution is protected or not retention eligible")
        receipt = self._archive_one(record, policy, current)
        self._observe("archive", "archive", purged=0, failed=0)
        return receipt

    def _execute(
        self,
        tenant_id: str,
        policy: ExecutionRetentionPolicy,
        *,
        purge: bool,
        now: datetime | None,
        outcome: str,
        request_id: str | None = None,
    ) -> ExecutionLifecycleReport:
        self._validate_tenant(tenant_id)
        current = self._now(now)
        active_request_id = request_id or f"lifecycle:{tenant_id}"
        try:
            records = tuple(
                sorted(
                    (record for record in self._reader.list() if record.tenant_id == tenant_id),
                    key=lambda record: (record.created_at, record.execution_id),
                )
            )
        except (ExecutionStoreError, OSError, TypeError, UnicodeError, ValueError):
            report = ExecutionLifecycleReport(
                tenant_id=tenant_id,
                policy=policy,
                generated_at=_format_timestamp(current),
                scanned=0,
                eligible=0,
                protected=0,
                archived=0,
                purged=0,
                already_purged=0,
                skipped=0,
                failed=1,
                dry_run=policy.dry_run,
                failures=(LifecycleFailure(None, "scan", "store_read_failed"),),
            )
            self._observe(
                outcome,
                "error",
                purged=0,
                failed=1,
                error_code="store_read_failed",
            )
            if not policy.dry_run:
                self._record_audit(
                    operation=AuditOperation.PURGE if purge else AuditOperation.ARCHIVE,
                    outcome=AuditOutcome.FAILED,
                    tenant_id=tenant_id,
                    execution_id=None,
                    reference_key=None,
                    reason_code="store_read_failed",
                    request_id=active_request_id,
                )
            return report
        classifications = self._classify(records, policy, current)
        eligible = tuple(item.record for item in classifications if item.state == "eligible")
        protected = tuple(
            item.record.execution_id for item in classifications if item.state == "protected"
        )
        skipped = sum(item.state == "skipped" for item in classifications)
        candidates: list[str] = []
        already: list[str] = []
        archived_ids: list[str] = []
        purged_ids: list[str] = []
        already_purged_ids: list[str] = []
        protected_ids = list(protected)
        skipped_ids = [
            item.record.execution_id for item in classifications if item.state == "skipped"
        ]
        decisions = tuple(item.decision for item in classifications)
        archived_count = 0
        purged_count = 0
        already_purged_count = 0
        failures: list[LifecycleFailure] = []

        for record in eligible:
            try:
                exists = self._archive_store.exists(record.execution_id)
                if not isinstance(exists, bool):
                    raise ArchiveError("archive adapter returned an invalid existence result")
                if policy.dry_run:
                    if not exists:
                        candidates.append(record.execution_id)
                    else:
                        verification = self._coerce_verification(
                            self._archive_store.verify(
                                record.execution_id,
                                expected_record=record,
                                expected_policy=replace(policy, dry_run=False),
                            )
                        )
                        if verification.valid and verification.checksum == _record_checksum(record):
                            already.append(record.execution_id)
                            self._record_audit(
                                operation=AuditOperation.ARCHIVE_VERIFY,
                                outcome=AuditOutcome.SUCCESS,
                                tenant_id=record.tenant_id,
                                execution_id=record.execution_id,
                                reference_key=record.reference_key,
                                reason_code="archive_verified",
                                request_id=active_request_id,
                            )
                        else:
                            self._record_audit(
                                operation=AuditOperation.ARCHIVE_VERIFY,
                                outcome=AuditOutcome.FAILED,
                                tenant_id=record.tenant_id,
                                execution_id=record.execution_id,
                                reference_key=None,
                                reason_code="archive_verification_failed",
                                request_id=active_request_id,
                            )
                            failures.append(
                                LifecycleFailure(
                                    record.execution_id,
                                    "verify",
                                    (
                                        "record_checksum_mismatch"
                                        if verification.valid
                                        else verification.error_code or "archive_invalid"
                                    ),
                                )
                            )
                    continue
                if purge and not policy.archive_before_purge:
                    failures.append(
                        LifecycleFailure(record.execution_id, "archive", "archive_required")
                    )
                    self._record_audit(
                        operation=AuditOperation.PURGE,
                        outcome=AuditOutcome.REJECTED,
                        tenant_id=record.tenant_id,
                        execution_id=record.execution_id,
                        reference_key=record.reference_key,
                        reason_code="archive_required",
                        request_id=active_request_id,
                    )
                    continue
                if not exists:
                    candidates.append(record.execution_id)
                receipt = self._archive_one(record, policy, current)
                self._record_audit(
                    operation=AuditOperation.ARCHIVE,
                    outcome=AuditOutcome.SUCCESS
                    if receipt.created
                    else AuditOutcome.ALREADY_PRESENT,
                    tenant_id=record.tenant_id,
                    execution_id=record.execution_id,
                    reference_key=record.reference_key,
                    reason_code="archive_created" if receipt.created else "archive_already_present",
                    request_id=active_request_id,
                )
                if receipt.created:
                    archived_count += 1
                    archived_ids.append(record.execution_id)
                else:
                    already.append(record.execution_id)
                if purge:
                    if self._store.delete_if_unchanged(record.execution_id, record):
                        purged_count += 1
                        purged_ids.append(record.execution_id)
                        self._record_audit(
                            operation=AuditOperation.PURGE,
                            outcome=AuditOutcome.SUCCESS,
                            tenant_id=record.tenant_id,
                            execution_id=record.execution_id,
                            reference_key=record.reference_key,
                            reason_code="record_purged",
                            request_id=active_request_id,
                        )
                        continue
                    current_record = self._reader.get(record.execution_id)
                    if current_record is None:
                        verification = self._coerce_verification(
                            self._archive_store.verify(
                                record.execution_id,
                                expected_record=record,
                                expected_policy=policy,
                            )
                        )
                        if not verification.valid or verification.checksum != _record_checksum(
                            record
                        ):
                            failures.append(
                                LifecycleFailure(
                                    record.execution_id,
                                    "purge",
                                    "source_missing_archive_unverified",
                                )
                            )
                            self._record_audit(
                                operation=AuditOperation.PURGE,
                                outcome=AuditOutcome.FAILED,
                                tenant_id=record.tenant_id,
                                execution_id=record.execution_id,
                                reference_key=record.reference_key,
                                reason_code="source_missing_archive_unverified",
                                request_id=active_request_id,
                            )
                            continue
                        already_purged_count += 1
                        already_purged_ids.append(record.execution_id)
                        self._record_audit(
                            operation=AuditOperation.PURGE,
                            outcome=AuditOutcome.ALREADY_PRESENT,
                            tenant_id=record.tenant_id,
                            execution_id=record.execution_id,
                            reference_key=record.reference_key,
                            reason_code="record_already_purged",
                            request_id=active_request_id,
                        )
                        continue
                    failures.append(
                        LifecycleFailure(record.execution_id, "purge", "record_changed")
                    )
                    self._record_audit(
                        operation=AuditOperation.PURGE,
                        outcome=AuditOutcome.CONFLICT,
                        tenant_id=record.tenant_id,
                        execution_id=record.execution_id,
                        reference_key=record.reference_key,
                        reason_code="record_changed",
                        request_id=active_request_id,
                    )
            except ArchiveError as exc:
                failures.append(
                    LifecycleFailure(
                        record.execution_id,
                        "archive",
                        _archive_error_code(exc),
                    )
                )
                self._record_audit(
                    operation=AuditOperation.PURGE if purge else AuditOperation.ARCHIVE,
                    outcome=AuditOutcome.FAILED,
                    tenant_id=record.tenant_id,
                    execution_id=record.execution_id,
                    reference_key=record.reference_key,
                    reason_code=_archive_error_code(exc),
                    request_id=active_request_id,
                )
                self._observe(
                    outcome,
                    "error",
                    purged=0,
                    failed=1,
                    error_code=_archive_error_code(exc),
                )
            except (ExecutionStoreError, OSError, TypeError, UnicodeError, ValueError):
                failures.append(
                    LifecycleFailure(record.execution_id, "lifecycle", "operation_failed")
                )
                self._record_audit(
                    operation=AuditOperation.PURGE if purge else AuditOperation.ARCHIVE,
                    outcome=AuditOutcome.FAILED,
                    tenant_id=record.tenant_id,
                    execution_id=record.execution_id,
                    reference_key=record.reference_key,
                    reason_code="operation_failed",
                    request_id=active_request_id,
                )
                self._observe(
                    outcome,
                    "error",
                    purged=0,
                    failed=1,
                    error_code="operation_failed",
                )

        report = ExecutionLifecycleReport(
            tenant_id=tenant_id,
            policy=policy,
            generated_at=_format_timestamp(current),
            scanned=len(records),
            eligible=len(eligible),
            protected=len(protected),
            archived=archived_count,
            purged=purged_count,
            already_purged=already_purged_count,
            skipped=skipped,
            failed=len(failures),
            dry_run=policy.dry_run,
            archive_candidates=tuple(sorted(candidates)),
            already_archived=tuple(sorted(already)),
            archived_ids=tuple(sorted(archived_ids)),
            purged_ids=tuple(sorted(purged_ids)),
            already_purged_ids=tuple(sorted(already_purged_ids)),
            protected_ids=tuple(sorted(protected_ids)),
            skipped_ids=tuple(sorted(skipped_ids)),
            decisions=decisions,
            failures=tuple(failures),
        )
        if not policy.dry_run:
            self._record_audit(
                operation=AuditOperation.PURGE if purge else AuditOperation.ARCHIVE,
                outcome=AuditOutcome.SUCCESS if report.failed == 0 else AuditOutcome.FAILED,
                tenant_id=tenant_id,
                execution_id=None,
                reference_key=None,
                reason_code="lifecycle_completed" if report.failed == 0 else "lifecycle_failed",
                request_id=active_request_id,
            )
        self._observe(
            outcome,
            outcome if report.failed == 0 else "error",
            purged=report.purged,
            failed=report.failed,
        )
        return report

    @staticmethod
    def _coerce_verification(value: object) -> ArchiveVerification:
        if not isinstance(value, ArchiveVerification) or not isinstance(value.valid, bool):
            raise ArchiveError("archive adapter returned an invalid verification")
        if value.valid:
            if not isinstance(value.checksum, str) or len(value.checksum) != 64:
                raise ArchiveError("archive adapter returned an invalid checksum")
            return value
        return ArchiveVerification(
            valid=False,
            error_code=_safe_adapter_error_code(value.error_code),
        )

    def _archive_one(
        self,
        record: ExecutionRecord,
        policy: ExecutionRetentionPolicy,
        current: datetime,
    ) -> ArchiveReceipt:
        receipts = self._archive_store.archive(
            (record,),
            policy=policy,
            archived_at=current,
            reason="retention",
        )
        try:
            receipt_count = len(receipts)
        except (TypeError, AttributeError) as exc:
            raise ArchiveError("archive adapter returned invalid receipts") from exc
        if receipt_count != 1:
            raise ArchiveError("archive store returned an unexpected receipt count")
        receipt = receipts[0]
        if not isinstance(receipt, ArchiveReceipt) or not isinstance(receipt.created, bool):
            raise ArchiveError("archive adapter returned an invalid receipt")
        expected_checksum = _record_checksum(record)
        if (
            receipt.execution_id != record.execution_id
            or receipt.tenant_id != record.tenant_id
            or receipt.record_checksum != expected_checksum
        ):
            raise ArchiveIntegrityError("archive receipt does not match source record")
        verification = self._coerce_verification(
            self._archive_store.verify(
                record.execution_id,
                expected_record=record,
                expected_policy=policy,
            )
        )
        if (
            not verification.valid
            or verification.checksum != expected_checksum
            or receipt.record_checksum != expected_checksum
        ):
            raise ArchiveIntegrityError(verification.error_code or "archive verification failed")
        return receipt

    def _classify(
        self,
        records: tuple[ExecutionRecord, ...],
        policy: ExecutionRetentionPolicy,
        current: datetime,
    ) -> tuple[_Classification, ...]:
        valid_created: list[tuple[datetime, ExecutionRecord]] = []
        for record in records:
            try:
                valid_created.append((_parse_timestamp(record.created_at, "created_at"), record))
            except ArchiveIntegrityError:
                continue
        keep_ids = {
            record.execution_id
            for _, record in sorted(
                valid_created, key=lambda item: (item[0], item[1].execution_id), reverse=True
            )[: policy.minimum_records_to_keep]
        }
        classifications: list[_Classification] = []
        for record in records:
            status = _status_value(record)
            if status not in {ExecutionStatus.SUCCESS.value, ExecutionStatus.FAILED.value}:
                if status == ExecutionStatus.PENDING.value:
                    state, reason = "protected", "status_pending"
                elif status == ExecutionStatus.RUNNING.value:
                    state, reason = "protected", "status_running"
                else:
                    state, reason = "skipped", "status_unknown"
                classifications.append(
                    _Classification(
                        record,
                        LifecycleDecision(record.execution_id, state, reason, None),
                    )
                )
                continue
            try:
                eligible_at = _eligible_at(record, policy)
            except ArchiveIntegrityError:
                classifications.append(
                    _Classification(
                        record,
                        LifecycleDecision(
                            record.execution_id, "skipped", "invalid_created_at", None
                        ),
                    )
                )
                continue
            eligible_text = _format_timestamp(eligible_at)
            if record.execution_id in keep_ids:
                state, reason = "protected", "minimum_records_to_keep"
            elif eligible_at > current:
                state, reason = "protected", "within_retention_window"
            else:
                state, reason = "eligible", "retention_eligible"
            classifications.append(
                _Classification(
                    record,
                    LifecycleDecision(record.execution_id, state, reason, eligible_text),
                )
            )
        return tuple(classifications)

    def _now(self, now: datetime | None) -> datetime:
        value = now if now is not None else self._clock()
        return _parse_timestamp(value, "now")

    @staticmethod
    def _validate_tenant(tenant_id: str) -> None:
        if not isinstance(tenant_id, str) or not tenant_id.strip():
            msg = "tenant_id is required"
            raise LifecycleTenantError(msg)

    @staticmethod
    def _observe(
        outcome: str,
        event: str,
        *,
        purged: int,
        failed: int,
        error_code: str | None = None,
    ) -> None:
        logger.info(
            "execution_history operation=lifecycle outcome={} event={} purged={} failed={} error_code={}",
            outcome,
            event,
            purged,
            failed,
            error_code or "-",
        )


LifecycleService = ExecutionHistoryLifecycle


def _integrity_error_code(message: str) -> str:
    if "content checksum" in message:
        return "archive_content_checksum_mismatch"
    if "record checksum" in message or "checksum" in message:
        return "archive_record_checksum_mismatch"
    if "format" in message or "version" in message:
        return "archive_format_mismatch"
    if "identity" in message:
        return "archive_identity_mismatch"
    if "eligibility" in message:
        return "archive_metadata_mismatch"
    if "malformed" in message or "incomplete" in message or "invalid" in message:
        return "archive_payload_invalid"
    return "archive_integrity"


_SAFE_ADAPTER_ERROR_CODES = frozenset(
    {
        "missing_archive",
        "archive_content_checksum_mismatch",
        "archive_record_checksum_mismatch",
        "archive_format_mismatch",
        "archive_identity_mismatch",
        "archive_metadata_mismatch",
        "archive_payload_invalid",
        "archive_integrity",
        "archive_read_failed",
        "record_mismatch",
        "policy_mismatch",
        "archive_invalid",
    }
)


def _safe_adapter_error_code(value: object) -> str:
    if isinstance(value, str) and value in _SAFE_ADAPTER_ERROR_CODES:
        return value
    return "archive_invalid"


def _archive_error_code(error: ArchiveError) -> str:
    if isinstance(error, ArchiveIntegrityError):
        return "archive_integrity"
    if isinstance(error, ArchiveConflictError):
        return "archive_conflict"
    if isinstance(error, ArchiveNotFoundError):
        return "archive_missing"
    return "archive_failed"


__all__ = [
    "ARCHIVE_FORMAT_VERSION",
    "ArchiveConflictError",
    "ArchiveError",
    "ArchiveIntegrityError",
    "ArchiveMetadata",
    "ArchiveNotFoundError",
    "ArchiveReceipt",
    "ArchiveStore",
    "ArchiveVerification",
    "ArchivedExecution",
    "ExecutionHistoryLifecycle",
    "ExecutionLifecycleReport",
    "ExecutionRetentionPolicy",
    "FileSystemArchiveStore",
    "InvalidRetentionPolicyError",
    "LifecycleError",
    "LifecycleDecision",
    "LifecycleFailure",
    "LifecycleProtectedError",
    "LifecycleService",
    "LifecycleTenantError",
    "PurgeableExecutionStore",
]
