"""W108 — explicit, tenant-safe recovery of archived execution records.

Recovery reads a W107 archive only through :class:`ArchiveStore`, validates
its integrity and identity, and inserts the reconstructed record through an
atomic store capability.  It never reads W99/W104/W106, archive files, MLflow,
ThingsBoard, or business execution services.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import Enum
from typing import Any, Protocol

from loguru import logger
from trendx.forecasting.execution import (
    ExecutionRecord,
    ExecutionStatus,
    ExecutionStore,
    ExecutionStoreError,
    RestoreStoreResult,
    RestoreStoreStatus,
)
from trendx.forecasting.lifecycle import (
    ARCHIVE_FORMAT_VERSION,
    ArchivedExecution,
    ArchiveError,
    ArchiveIntegrityError,
    ArchiveMetadata,
    ArchiveNotFoundError,
    ArchiveStore,
    ArchiveVerification,
    ExecutionRetentionPolicy,
)

RESTORE_OPERATION = "restore"
RESTORE_VERSION = "1"


class RestorableExecutionStore(ExecutionStore, Protocol):
    """ExecutionStore plus atomic insert-if-absent recovery capability."""

    def restore_if_absent(self, record: ExecutionRecord) -> RestoreStoreResult:
        """Insert ``record`` atomically and return the locked state snapshot."""
        ...


class RestoreConflictPolicy(str, Enum):
    """Safe conflict policies supported by W108."""

    FAIL_IF_EXISTS = "FAIL_IF_EXISTS"


class ExecutionRestoreStatus(str, Enum):
    """Stable W108 result states."""

    VERIFIED = "VERIFIED"
    RESTORED = "RESTORED"
    ALREADY_PRESENT = "ALREADY_PRESENT"
    CONFLICT = "CONFLICT"
    ARCHIVE_NOT_FOUND = "ARCHIVE_NOT_FOUND"
    INVALID_ARCHIVE = "INVALID_ARCHIVE"
    INTEGRITY_FAILURE = "INTEGRITY_FAILURE"
    TENANT_FORBIDDEN = "TENANT_FORBIDDEN"
    RESTORE_FAILED = "RESTORE_FAILED"


class ExecutionRestoreOutcome(str, Enum):
    """Whether the operation changed active history."""

    RESTORED = "RESTORED"
    NO_CHANGE = "NO_CHANGE"
    REJECTED = "REJECTED"


@dataclass(frozen=True)
class ExecutionRestoreRequest:
    """Explicit authorization to restore one archived execution."""

    execution_id: str
    tenant_id: str
    expected_archive_checksum: str
    conflict_policy: RestoreConflictPolicy = RestoreConflictPolicy.FAIL_IF_EXISTS

    def __post_init__(self) -> None:
        if not isinstance(self.execution_id, str) or not self.execution_id.strip():
            msg = "execution_id is required"
            raise ValueError(msg)
        if not isinstance(self.tenant_id, str) or not self.tenant_id.strip():
            msg = "tenant_id is required"
            raise ValueError(msg)
        if (
            not isinstance(self.expected_archive_checksum, str)
            or not self.expected_archive_checksum.strip()
        ):
            msg = "expected_archive_checksum is required"
            raise ValueError(msg)
        try:
            policy = RestoreConflictPolicy(self.conflict_policy)
        except (TypeError, ValueError) as exc:
            msg = "unsupported restore conflict policy"
            raise ValueError(msg) from exc
        object.__setattr__(self, "conflict_policy", policy)

    def to_dict(self) -> dict[str, str]:
        return {
            "execution_id": self.execution_id,
            "tenant_id": self.tenant_id,
            "expected_archive_checksum": self.expected_archive_checksum,
            "conflict_policy": self.conflict_policy.value,
        }


@dataclass(frozen=True)
class ExecutionRestoreMetadata:
    """Non-invasive audit metadata for one recovery operation."""

    archive_format_version: int
    archive_checksum: str
    restore_timestamp: str
    operation: str = RESTORE_OPERATION
    version: str = RESTORE_VERSION

    def to_dict(self) -> dict[str, str | int]:
        return {
            "archive_format_version": self.archive_format_version,
            "archive_checksum": self.archive_checksum,
            "restore_timestamp": self.restore_timestamp,
            "operation": self.operation,
            "version": self.version,
        }


@dataclass(frozen=True)
class ExecutionRestoreResult:
    """Serializable W108 result without payloads, paths, or secrets."""

    execution_id: str
    tenant_id: str
    status: ExecutionRestoreStatus
    outcome: ExecutionRestoreOutcome
    archive_verified: bool
    record_restored: bool
    already_present: bool
    conflict: bool
    reason: str
    archive_format_version: int | None = None
    archive_checksum: str | None = None
    metadata: ExecutionRestoreMetadata | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "execution_id": self.execution_id,
            "tenant_id": self.tenant_id,
            "status": self.status.value,
            "outcome": self.outcome.value,
            "archive_verified": self.archive_verified,
            "record_restored": self.record_restored,
            "already_present": self.already_present,
            "conflict": self.conflict,
            "reason": self.reason,
            "archive_format_version": self.archive_format_version,
            "archive_checksum": self.archive_checksum,
            "restore_metadata": self.metadata.to_dict() if self.metadata is not None else None,
        }


@dataclass(frozen=True)
class _VerifiedArchive:
    request: ExecutionRestoreRequest
    archived: ArchivedExecution
    checksum: str
    verified_at: datetime


class RecoveryService:
    """Explicit archive-to-active-history recovery service."""

    def __init__(
        self,
        store: RestorableExecutionStore,
        archive_store: ArchiveStore,
        clock: Callable[[], datetime],
    ) -> None:
        self._store = store
        self._archive_store = archive_store
        self._clock = clock

    def verify_archive(
        self,
        request: ExecutionRestoreRequest,
        *,
        now: datetime | None = None,
    ) -> ExecutionRestoreResult:
        """Verify an archive and reconstruct its record without any write."""

        checked, result = self._check_archive(request, now)
        if checked is None:
            self._observe(result)
            return result
        metadata = self._metadata(checked, checked.verified_at)
        result = self._result(
            request,
            ExecutionRestoreStatus.VERIFIED,
            ExecutionRestoreOutcome.NO_CHANGE,
            archive_verified=True,
            reason="archive_verified",
            metadata=metadata,
        )
        self._observe(result)
        return result

    def restore(
        self,
        request: ExecutionRestoreRequest,
        *,
        now: datetime | None = None,
    ) -> ExecutionRestoreResult:
        """Verify then atomically restore one archive; never overwrite."""

        checked, result = self._check_archive(request, now)
        if checked is None:
            self._observe(result)
            return result
        metadata = self._metadata(checked, checked.verified_at)
        try:
            store_result = self._store.restore_if_absent(checked.archived.record)
        except (ExecutionStoreError, OSError, UnicodeError, ValueError, TypeError, RuntimeError):
            result = self._result(
                request,
                ExecutionRestoreStatus.RESTORE_FAILED,
                ExecutionRestoreOutcome.REJECTED,
                archive_verified=True,
                reason="active_store_write_failed",
                metadata=metadata,
            )
            self._observe(result)
            return result

        if not isinstance(store_result, RestoreStoreResult) or not isinstance(
            store_result.status, RestoreStoreStatus
        ):
            result = self._result(
                request,
                ExecutionRestoreStatus.RESTORE_FAILED,
                ExecutionRestoreOutcome.REJECTED,
                archive_verified=True,
                reason="active_store_invalid_result",
                metadata=metadata,
            )
            self._observe(result)
            return result

        # The store captures the post-operation record while holding the same
        # lock used for the insert, so this is the active-record verification
        # without a racy second read.
        active = store_result.record
        if store_result.status is RestoreStoreStatus.INSERTED:
            if active is None or not _records_equal(active, checked.archived.record):
                result = self._result(
                    request,
                    ExecutionRestoreStatus.RESTORE_FAILED,
                    ExecutionRestoreOutcome.REJECTED,
                    archive_verified=True,
                    reason="active_record_verification_failed",
                    metadata=metadata,
                )
                self._observe(result)
                return result
            result = self._result(
                request,
                ExecutionRestoreStatus.RESTORED,
                ExecutionRestoreOutcome.RESTORED,
                archive_verified=True,
                record_restored=True,
                reason="record_restored",
                metadata=metadata,
            )
            self._observe(result)
            return result

        if store_result.status is RestoreStoreStatus.ALREADY_PRESENT:
            if active is None or not _records_equal(active, checked.archived.record):
                result = self._result(
                    request,
                    ExecutionRestoreStatus.RESTORE_FAILED,
                    ExecutionRestoreOutcome.REJECTED,
                    archive_verified=True,
                    reason="active_state_inconsistent",
                    metadata=metadata,
                )
                self._observe(result)
                return result
            result = self._result(
                request,
                ExecutionRestoreStatus.ALREADY_PRESENT,
                ExecutionRestoreOutcome.NO_CHANGE,
                archive_verified=True,
                already_present=True,
                reason="record_already_present",
                metadata=metadata,
            )
            self._observe(result)
            return result

        if store_result.status is RestoreStoreStatus.CONFLICT:
            if active is not None and _records_equal(active, checked.archived.record):
                result = self._result(
                    request,
                    ExecutionRestoreStatus.ALREADY_PRESENT,
                    ExecutionRestoreOutcome.NO_CHANGE,
                    archive_verified=True,
                    already_present=True,
                    reason="record_already_present",
                    metadata=metadata,
                )
                self._observe(result)
                return result
            if active is None:
                result = self._result(
                    request,
                    ExecutionRestoreStatus.RESTORE_FAILED,
                    ExecutionRestoreOutcome.REJECTED,
                    archive_verified=True,
                    reason="active_state_inconsistent",
                    metadata=metadata,
                )
                self._observe(result)
                return result
            reason = (
                "reference_key_conflict"
                if store_result.reason == "reference_key_conflict"
                else "execution_id_conflict"
            )
            result = self._result(
                request,
                ExecutionRestoreStatus.CONFLICT,
                ExecutionRestoreOutcome.REJECTED,
                archive_verified=True,
                conflict=True,
                reason=reason,
                metadata=metadata,
            )
            self._observe(result)
            return result

        result = self._result(
            request,
            ExecutionRestoreStatus.RESTORE_FAILED,
            ExecutionRestoreOutcome.REJECTED,
            archive_verified=True,
            reason="active_store_invalid_result",
            metadata=metadata,
        )
        self._observe(result)
        return result

    def _check_archive(
        self,
        request: ExecutionRestoreRequest,
        now: datetime | None,
    ) -> tuple[_VerifiedArchive | None, ExecutionRestoreResult]:
        checked_at = self._now(now)
        if not _is_sha256(request.expected_archive_checksum):
            return None, self._result(
                request,
                ExecutionRestoreStatus.INTEGRITY_FAILURE,
                ExecutionRestoreOutcome.REJECTED,
                reason="expected_checksum_invalid",
            )
        try:
            exists = self._archive_store.exists(request.execution_id)
        except ArchiveNotFoundError:
            return None, self._result(
                request,
                ExecutionRestoreStatus.ARCHIVE_NOT_FOUND,
                ExecutionRestoreOutcome.REJECTED,
                reason="archive_not_found",
            )
        except ArchiveIntegrityError:
            return None, self._result(
                request,
                ExecutionRestoreStatus.INTEGRITY_FAILURE,
                ExecutionRestoreOutcome.REJECTED,
                reason="archive_integrity_failure",
            )
        except (
            ArchiveError,
            ExecutionStoreError,
            OSError,
            UnicodeError,
            ValueError,
            TypeError,
            RuntimeError,
        ):
            return None, self._result(
                request,
                ExecutionRestoreStatus.RESTORE_FAILED,
                ExecutionRestoreOutcome.REJECTED,
                reason="archive_unavailable",
            )
        if not isinstance(exists, bool):
            return None, self._result(
                request,
                ExecutionRestoreStatus.INVALID_ARCHIVE,
                ExecutionRestoreOutcome.REJECTED,
                reason="invalid_archive_adapter",
            )
        if not exists:
            return None, self._result(
                request,
                ExecutionRestoreStatus.ARCHIVE_NOT_FOUND,
                ExecutionRestoreOutcome.REJECTED,
                reason="archive_not_found",
            )

        try:
            verification = self._archive_store.verify(request.execution_id)
        except ArchiveNotFoundError:
            return None, self._result(
                request,
                ExecutionRestoreStatus.ARCHIVE_NOT_FOUND,
                ExecutionRestoreOutcome.REJECTED,
                reason="archive_not_found",
            )
        except ArchiveIntegrityError:
            return None, self._result(
                request,
                ExecutionRestoreStatus.INTEGRITY_FAILURE,
                ExecutionRestoreOutcome.REJECTED,
                reason="archive_integrity_failure",
            )
        except (ArchiveError, OSError, UnicodeError, ValueError, TypeError, RuntimeError):
            return None, self._result(
                request,
                ExecutionRestoreStatus.INVALID_ARCHIVE,
                ExecutionRestoreOutcome.REJECTED,
                reason="invalid_archive",
            )
        if not isinstance(verification, ArchiveVerification) or not isinstance(
            verification.valid, bool
        ):
            return None, self._result(
                request,
                ExecutionRestoreStatus.INVALID_ARCHIVE,
                ExecutionRestoreOutcome.REJECTED,
                reason="invalid_archive_adapter",
            )
        if not verification.valid:
            status = _archive_error_status(verification.error_code)
            return None, self._result(
                request,
                status,
                ExecutionRestoreOutcome.REJECTED,
                reason=_archive_reason(verification.error_code),
            )
        if not isinstance(verification.checksum, str) or not _is_sha256(verification.checksum):
            return None, self._result(
                request,
                ExecutionRestoreStatus.INTEGRITY_FAILURE,
                ExecutionRestoreOutcome.REJECTED,
                reason="archive_checksum_invalid",
            )
        if verification.checksum != request.expected_archive_checksum:
            return None, self._result(
                request,
                ExecutionRestoreStatus.INTEGRITY_FAILURE,
                ExecutionRestoreOutcome.REJECTED,
                archive_verified=True,
                reason="expected_checksum_mismatch",
            )
        try:
            archived = self._archive_store.read(request.execution_id)
        except ArchiveNotFoundError:
            return None, self._result(
                request,
                ExecutionRestoreStatus.ARCHIVE_NOT_FOUND,
                ExecutionRestoreOutcome.REJECTED,
                reason="archive_not_found",
            )
        except ArchiveIntegrityError:
            return None, self._result(
                request,
                ExecutionRestoreStatus.INTEGRITY_FAILURE,
                ExecutionRestoreOutcome.REJECTED,
                reason="archive_integrity_failure",
            )
        except (ArchiveError, OSError, UnicodeError, ValueError, TypeError, RuntimeError):
            return None, self._result(
                request,
                ExecutionRestoreStatus.INVALID_ARCHIVE,
                ExecutionRestoreOutcome.REJECTED,
                reason="invalid_archive",
            )
        if (
            not isinstance(archived, ArchivedExecution)
            or not isinstance(archived.record, ExecutionRecord)
            or not isinstance(archived.metadata, ArchiveMetadata)
        ):
            return None, self._result(
                request,
                ExecutionRestoreStatus.INVALID_ARCHIVE,
                ExecutionRestoreOutcome.REJECTED,
                reason="invalid_archive_adapter",
            )
        metadata = archived.metadata
        if (
            isinstance(metadata.archive_format_version, bool)
            or not isinstance(metadata.archive_format_version, int)
            or metadata.archive_format_version != ARCHIVE_FORMAT_VERSION
            or not isinstance(metadata.execution_id, str)
            or not metadata.execution_id
            or not isinstance(metadata.tenant_id, str)
            or not metadata.tenant_id
            or not isinstance(metadata.reason, str)
            or not metadata.reason
            or not isinstance(metadata.policy, ExecutionRetentionPolicy)
            or metadata.policy.dry_run
            or not isinstance(metadata.eligible_at, datetime)
            or not isinstance(metadata.archived_at, datetime)
            or not isinstance(metadata.record_checksum, str)
            or not _is_sha256(metadata.record_checksum)
        ):
            return None, self._result(
                request,
                ExecutionRestoreStatus.INVALID_ARCHIVE,
                ExecutionRestoreOutcome.REJECTED,
                reason="invalid_archive_metadata",
            )
        record = archived.record
        if any(
            not isinstance(value, str)
            for value in (
                record.execution_id,
                record.tenant_id,
                record.reference_key,
                record.created_at,
                record.completed_at,
            )
        ) or not isinstance(record.status, ExecutionStatus):
            return None, self._result(
                request,
                ExecutionRestoreStatus.INVALID_ARCHIVE,
                ExecutionRestoreOutcome.REJECTED,
                reason="invalid_record_identity",
            )
        if record.status not in {ExecutionStatus.SUCCESS, ExecutionStatus.FAILED}:
            return None, self._result(
                request,
                ExecutionRestoreStatus.INVALID_ARCHIVE,
                ExecutionRestoreOutcome.REJECTED,
                reason="non_terminal_archive",
            )
        if record.execution_id != request.execution_id:
            return None, self._result(
                request,
                ExecutionRestoreStatus.INVALID_ARCHIVE,
                ExecutionRestoreOutcome.REJECTED,
                reason="execution_id_mismatch",
            )
        if archived.metadata.execution_id != record.execution_id:
            return None, self._result(
                request,
                ExecutionRestoreStatus.INVALID_ARCHIVE,
                ExecutionRestoreOutcome.REJECTED,
                reason="execution_id_mismatch",
            )
        if record.tenant_id != request.tenant_id:
            return None, self._result(
                request,
                ExecutionRestoreStatus.TENANT_FORBIDDEN,
                ExecutionRestoreOutcome.REJECTED,
                archive_verified=True,
                reason="tenant_forbidden",
            )
        if archived.metadata.tenant_id != record.tenant_id:
            return None, self._result(
                request,
                ExecutionRestoreStatus.INVALID_ARCHIVE,
                ExecutionRestoreOutcome.REJECTED,
                archive_verified=True,
                reason="archive_tenant_mismatch",
            )
        if archived.metadata.record_checksum != verification.checksum:
            return None, self._result(
                request,
                ExecutionRestoreStatus.INTEGRITY_FAILURE,
                ExecutionRestoreOutcome.REJECTED,
                archive_verified=True,
                reason="archive_checksum_mismatch",
            )
        try:
            reverified = self._archive_store.verify(
                request.execution_id,
                expected_record=record,
            )
        except ArchiveNotFoundError:
            return None, self._result(
                request,
                ExecutionRestoreStatus.ARCHIVE_NOT_FOUND,
                ExecutionRestoreOutcome.REJECTED,
                reason="archive_not_found",
            )
        except ArchiveIntegrityError:
            return None, self._result(
                request,
                ExecutionRestoreStatus.INTEGRITY_FAILURE,
                ExecutionRestoreOutcome.REJECTED,
                archive_verified=True,
                reason="archive_integrity_failure",
            )
        except (ArchiveError, OSError, UnicodeError, ValueError, TypeError, RuntimeError):
            return None, self._result(
                request,
                ExecutionRestoreStatus.INVALID_ARCHIVE,
                ExecutionRestoreOutcome.REJECTED,
                reason="archive_reverification_failed",
            )
        if (
            not isinstance(reverified, ArchiveVerification)
            or not isinstance(reverified.valid, bool)
            or not isinstance(reverified.checksum, str)
            or not _is_sha256(reverified.checksum)
        ):
            return None, self._result(
                request,
                ExecutionRestoreStatus.INTEGRITY_FAILURE,
                ExecutionRestoreOutcome.REJECTED,
                archive_verified=True,
                reason="archive_reverification_failed",
            )
        if not reverified.valid:
            return None, self._result(
                request,
                _archive_error_status(reverified.error_code),
                ExecutionRestoreOutcome.REJECTED,
                archive_verified=True,
                reason=_archive_reason(reverified.error_code),
            )
        if reverified.checksum != verification.checksum:
            return None, self._result(
                request,
                ExecutionRestoreStatus.INTEGRITY_FAILURE,
                ExecutionRestoreOutcome.REJECTED,
                archive_verified=True,
                reason="archive_checksum_mismatch",
            )
        return (
            _VerifiedArchive(
                request=request,
                archived=archived,
                checksum=verification.checksum,
                verified_at=checked_at,
            ),
            self._result(
                request,
                ExecutionRestoreStatus.VERIFIED,
                ExecutionRestoreOutcome.NO_CHANGE,
                archive_verified=True,
                reason="archive_verified",
            ),
        )

    def _result(
        self,
        request: ExecutionRestoreRequest,
        status: ExecutionRestoreStatus,
        outcome: ExecutionRestoreOutcome,
        *,
        archive_verified: bool = False,
        record_restored: bool = False,
        already_present: bool = False,
        conflict: bool = False,
        reason: str,
        metadata: ExecutionRestoreMetadata | None = None,
        archive_checksum: str | None = None,
        archive_format_version: int | None = None,
    ) -> ExecutionRestoreResult:
        if metadata is not None:
            archive_checksum = archive_checksum or metadata.archive_checksum
            if archive_format_version is None:
                archive_format_version = metadata.archive_format_version
        return ExecutionRestoreResult(
            execution_id=request.execution_id,
            tenant_id=request.tenant_id,
            status=status,
            outcome=outcome,
            archive_verified=archive_verified,
            record_restored=record_restored,
            already_present=already_present,
            conflict=conflict,
            reason=reason,
            archive_format_version=archive_format_version,
            archive_checksum=archive_checksum,
            metadata=metadata,
        )

    def _metadata(self, checked: _VerifiedArchive, timestamp: datetime) -> ExecutionRestoreMetadata:
        return ExecutionRestoreMetadata(
            archive_format_version=checked.archived.metadata.archive_format_version,
            archive_checksum=checked.checksum,
            restore_timestamp=self._format(timestamp),
        )

    def _now(self, now: datetime | None) -> datetime:
        value = now if now is not None else self._clock()
        return _normalize_datetime(value)

    @staticmethod
    def _format(value: datetime) -> str:
        return _normalize_datetime(value).isoformat()

    @staticmethod
    def _observe(result: ExecutionRestoreResult) -> None:
        outcome = {
            ExecutionRestoreStatus.RESTORED: "success",
            ExecutionRestoreStatus.ALREADY_PRESENT: "already_present",
            ExecutionRestoreStatus.CONFLICT: "conflict",
            ExecutionRestoreStatus.INVALID_ARCHIVE: "invalid_archive",
            ExecutionRestoreStatus.INTEGRITY_FAILURE: "invalid_archive",
            ExecutionRestoreStatus.TENANT_FORBIDDEN: "tenant_rejected",
            ExecutionRestoreStatus.ARCHIVE_NOT_FOUND: "archive_unavailable",
            ExecutionRestoreStatus.RESTORE_FAILED: "archive_unavailable",
            ExecutionRestoreStatus.VERIFIED: "success",
        }[result.status]
        logger.info(
            "execution_history operation=restore outcome={} status={} reason={}",
            outcome,
            result.status.value,
            result.reason,
        )


def _normalize_datetime(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _is_sha256(value: str) -> bool:
    return len(value) == 64 and all(character in "0123456789abcdef" for character in value)


def _records_equal(left: ExecutionRecord, right: ExecutionRecord) -> bool:
    try:
        return json.dumps(
            left.to_dict(),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ) == json.dumps(
            right.to_dict(),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
    except (TypeError, ValueError, UnicodeError):
        return False


def _archive_error_status(error_code: str | None) -> ExecutionRestoreStatus:
    if error_code == "missing_archive":
        return ExecutionRestoreStatus.ARCHIVE_NOT_FOUND
    if error_code in {
        "archive_content_checksum_mismatch",
        "archive_record_checksum_mismatch",
        "record_checksum_mismatch",
        "archive_integrity",
    }:
        return ExecutionRestoreStatus.INTEGRITY_FAILURE
    return ExecutionRestoreStatus.INVALID_ARCHIVE


def _archive_reason(error_code: str | None) -> str:
    if error_code in {
        "archive_content_checksum_mismatch",
        "archive_record_checksum_mismatch",
        "record_checksum_mismatch",
        "archive_integrity",
    }:
        return "archive_integrity_failure"
    if error_code == "missing_archive":
        return "archive_not_found"
    return "invalid_archive"


__all__ = [
    "ExecutionRestoreMetadata",
    "ExecutionRestoreOutcome",
    "ExecutionRestoreRequest",
    "ExecutionRestoreResult",
    "ExecutionRestoreStatus",
    "RecoveryService",
    "RestoreConflictPolicy",
    "RestorableExecutionStore",
]
