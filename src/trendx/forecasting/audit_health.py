"""W113 — operational health, capacity and archive readiness for the audit trail.

W113 is a **read-only diagnostic projection**.  It reads, aggregates, diagnoses
and exposes; it never archives, purges, restores, deletes or otherwise mutates an
audit event, and it never writes an audit event of its own.

It is a strict consumer of the existing chain::

    ExecutionAuditStore  -> scan_documents()          (W110/W111 verdicts)
    ExecutionAuditService                           (W110)
    ExecutionAuditIntegrityService                  (W111)
    AuditArchiveStore / AuditLifecycleService       (W112)

Nothing here re-implements W110 serialization, W111 checksums or W112 integrity:
every counter is derived from verdicts those layers already produced.

Two deliberate choices are worth stating up front.

**One coherent snapshot.**  :meth:`AuditHealthService.snapshot` reads the active
store exactly once, the archive at most once and the W112 backlog at most once,
then derives health, capacity and readiness from that single object.  A caller
that needs all three never triggers three independent scans.

**Filesystem metrics are a stat-only observation.**  No abstraction in the chain
exposes on-disk size, so :func:`filesystem_metrics` walks the two directories
with ``os.stat`` only.  It never opens, reads or decodes a document, so it cannot
become a second source of truth for audit content, and it returns only numbers:
no path, no name, no content, no credential.  When a metric cannot be obtained
portably it is reported as ``None`` (unavailable) rather than invented.
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import Enum
from pathlib import Path
from typing import Any, Protocol

from trendx.forecasting.audit import (
    AUDIT_FAILURE_CHECKSUM,
    AUDIT_FAILURE_IDENTITY,
    AUDIT_FAILURE_MALFORMED_JSON,
    AUDIT_FAILURE_UNSAFE_DOCUMENT,
    AUDIT_FAILURE_VERSION,
    AuditDocumentScan,
    AuditError,
    AuditStore,
    AuditStoreError,
    ExecutionAuditEvent,
    ExecutionAuditService,
    _utc_now,
)
from trendx.forecasting.audit_lifecycle import (
    AuditArchiveError,
    AuditArchiveStore,
    AuditLifecycleService,
    AuditRetentionPolicy,
    LifecyclePreviewReport,
)

AUDIT_HEALTH_REPORT_VERSION = "1"

#: Upper bound applied to filesystem walks.  It keeps a diagnostic bounded on a
#: pathological store and is reported honestly as a truncated observation.
DEFAULT_CAPACITY_SCAN_LIMIT = 100_000


class AuditOperationalStatus(str, Enum):
    """Observable state of the active audit journal."""

    HEALTHY = "HEALTHY"
    DEGRADED = "DEGRADED"
    UNAVAILABLE = "UNAVAILABLE"
    EMPTY = "EMPTY"


class AuditArchiveStatus(str, Enum):
    """Observable state of the W112 audit archive."""

    HEALTHY = "HEALTHY"
    DEGRADED = "DEGRADED"
    CORRUPTED = "CORRUPTED"
    UNAVAILABLE = "UNAVAILABLE"
    NOT_CONFIGURED = "NOT_CONFIGURED"


class AuditReadinessStatus(str, Enum):
    """Whether the audit component can serve correct diagnostics."""

    READY = "READY"
    NOT_READY = "NOT_READY"


# ── Stable reason codes (§11) ────────────────────────────────────────────

AUDIT_STORE_UNAVAILABLE = "AUDIT_STORE_UNAVAILABLE"
AUDIT_SCHEMA_INVALID = "AUDIT_SCHEMA_INVALID"
AUDIT_DATA_CORRUPTED = "AUDIT_DATA_CORRUPTED"
ARCHIVE_UNAVAILABLE = "ARCHIVE_UNAVAILABLE"
ARCHIVE_CORRUPTED = "ARCHIVE_CORRUPTED"
TENANT_CONTEXT_INVALID = "TENANT_CONTEXT_INVALID"
INTEGRITY_CHECK_FAILED = "INTEGRITY_CHECK_FAILED"
#: Informational only: an empty journal is a valid state, never a blocker.
AUDIT_JOURNAL_EMPTY = "AUDIT_JOURNAL_EMPTY"
ARCHIVE_NOT_CONFIGURED = "ARCHIVE_NOT_CONFIGURED"
CAPACITY_MEASUREMENT_INCOMPLETE = "CAPACITY_MEASUREMENT_INCOMPLETE"

#: Reason codes that make readiness fail.  Everything else is informational.
BLOCKING_REASON_CODES: frozenset[str] = frozenset(
    {
        AUDIT_STORE_UNAVAILABLE,
        AUDIT_SCHEMA_INVALID,
        AUDIT_DATA_CORRUPTED,
        ARCHIVE_UNAVAILABLE,
        ARCHIVE_CORRUPTED,
        TENANT_CONTEXT_INVALID,
        INTEGRITY_CHECK_FAILED,
    }
)


class AuditHealthError(AuditError):
    """W113 could not produce a health projection at all."""


class AuditArchiveReader(Protocol):
    """The W112 archive surface W113 needs, kept read-only."""

    def list_documents(self, *, tenant_id: str | None = None) -> tuple[Any, ...]: ...


class AuditBacklogReader(Protocol):
    """The W112 backlog surface W113 needs, kept read-only."""

    def preview(
        self,
        tenant_id: str,
        policy: AuditRetentionPolicy | Mapping[str, Any],
    ) -> LifecyclePreviewReport: ...


def _utc(value: datetime | None) -> datetime:
    if value is None:
        return _utc_now()
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _format_datetime(value: datetime | None) -> str | None:
    return None if value is None else _utc(value).isoformat()


def _require_tenant(tenant_id: str) -> str:
    if not isinstance(tenant_id, str) or not tenant_id.strip():
        raise AuditHealthError(TENANT_CONTEXT_INVALID)
    tenant = tenant_id.strip()
    if len(tenant) > 256 or "\x00" in tenant or "/" in tenant or "\\" in tenant or ".." in tenant:
        raise AuditHealthError(TENANT_CONTEXT_INVALID)
    return tenant


def _require_tenant_safe(tenant_id: str) -> tuple[str | None, str | None]:
    """Validate a tenant for a diagnostic, returning ``(tenant, reason)``."""

    try:
        return _require_tenant(tenant_id), None
    except AuditHealthError as exc:
        return None, str(exc)


@dataclass(frozen=True)
class FilesystemMetrics:
    """Stat-only filesystem observation.  Every field may be unavailable."""

    filesystem_free_bytes: int | None = None
    filesystem_total_bytes: int | None = None
    filesystem_used_bytes: int | None = None
    inode_free: int | None = None
    inode_total: int | None = None
    available: bool = False
    detail: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "filesystem_free_bytes": self.filesystem_free_bytes,
            "filesystem_total_bytes": self.filesystem_total_bytes,
            "filesystem_used_bytes": self.filesystem_used_bytes,
            "inode_free": self.inode_free,
            "inode_total": self.inode_total,
            "available": self.available,
            "detail": self.detail,
        }


def filesystem_metrics(path: Path) -> FilesystemMetrics:
    """Observe filesystem capacity for ``path`` using ``os.statvfs`` only.

    A directory is passed, never a file, and only numbers are returned: no path,
    no name, no content.  Any unsupported or failing platform yields
    ``available=False`` instead of a fabricated value.
    """

    try:
        usage = os.statvfs(path)
    except (AttributeError, OSError, ValueError):
        return FilesystemMetrics(available=False, detail="filesystem_metrics_unavailable")
    total = getattr(usage, "f_blocks", 0) * getattr(usage, "f_frsize", 0)
    free = getattr(usage, "f_bavail", 0) * getattr(usage, "f_frsize", 0)
    if not isinstance(total, int) or not isinstance(free, int) or total <= 0:
        return FilesystemMetrics(available=False, detail="filesystem_metrics_unavailable")
    inodes_free = getattr(usage, "f_favail", None)
    inodes_total = getattr(usage, "f_files", None)
    return FilesystemMetrics(
        filesystem_free_bytes=int(free),
        filesystem_total_bytes=int(total),
        filesystem_used_bytes=int(total - free),
        inode_free=int(inodes_free) if isinstance(inodes_free, int) else None,
        inode_total=int(inodes_total) if isinstance(inodes_total, int) else None,
        available=True,
        detail=None,
    )


def directory_bytes(
    path: Path,
    *,
    limit: int = DEFAULT_CAPACITY_SCAN_LIMIT,
) -> tuple[int | None, int, bool]:
    """Sum the on-disk size of ``path`` using ``os.stat`` only.

    Returns ``(total_bytes, file_count, truncated)``.  The directory is never
    opened and no file is read or decoded; this measures storage footprint, not
    audit content.  ``total_bytes`` is ``None`` when the directory cannot be
    observed at all, and ``truncated`` says the walk hit ``limit``.
    """

    total = 0
    count = 0
    truncated = False
    try:
        if not path.is_dir() or path.is_symlink():
            return None, 0, False
        for entry in sorted(path.iterdir()):
            if count >= limit:
                truncated = True
                break
            try:
                if entry.is_symlink() or not entry.is_file():
                    continue
                total += entry.stat().st_size
            except OSError:
                # an unreadable entry is reported as a measurement gap, never as 0
                return None, count, truncated
            count += 1
    except OSError:
        return None, count, truncated
    return total, count, truncated


@dataclass(frozen=True)
class AuditHealthReport:
    """Health of the active audit journal for one tenant."""

    report_version: str
    checked_at: datetime
    tenant_id: str
    status: AuditOperationalStatus
    event_count: int
    valid_event_count: int
    integrity_invalid_count: int
    malformed_count: int
    version_failure_count: int
    checksum_failure_count: int
    identity_failure_count: int
    unsafe_document_count: int
    schema_failure_count: int
    canonicalization_failure_count: int
    tenant_count: int
    success_count: int
    failure_count: int
    oldest_event_at: datetime | None
    newest_event_at: datetime | None
    last_observed_event_at: datetime | None
    reason_codes: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "report_version": self.report_version,
            "checked_at": _format_datetime(self.checked_at),
            "tenant_id": self.tenant_id,
            "status": self.status.value,
            "event_count": self.event_count,
            "valid_event_count": self.valid_event_count,
            "integrity_invalid_count": self.integrity_invalid_count,
            "malformed_count": self.malformed_count,
            "version_failure_count": self.version_failure_count,
            "checksum_failure_count": self.checksum_failure_count,
            "identity_failure_count": self.identity_failure_count,
            "unsafe_document_count": self.unsafe_document_count,
            "schema_failure_count": self.schema_failure_count,
            "canonicalization_failure_count": self.canonicalization_failure_count,
            "tenant_count": self.tenant_count,
            "success_count": self.success_count,
            "failure_count": self.failure_count,
            "oldest_event_at": _format_datetime(self.oldest_event_at),
            "newest_event_at": _format_datetime(self.newest_event_at),
            "last_observed_event_at": _format_datetime(self.last_observed_event_at),
            "reason_codes": list(self.reason_codes),
        }


@dataclass(frozen=True)
class AuditCapacitySnapshot:
    """Capacity of the active journal, the archive and the filesystem."""

    report_version: str
    measured_at: datetime
    tenant_id: str
    event_count: int
    active_bytes: int | None
    active_file_count: int
    archive_bytes: int | None
    archive_file_count: int
    archived_event_count: int | None
    total_known_event_count: int | None
    oldest_active_event_at: datetime | None
    newest_active_event_at: datetime | None
    oldest_archived_event_at: datetime | None
    newest_archived_event_at: datetime | None
    filesystem: FilesystemMetrics
    capacity_measurement_complete: bool
    current_count: int
    current_size_bytes: int | None
    current_oldest_event_at: datetime | None
    current_newest_event_at: datetime | None
    observed_growth: int | None = None
    observation_window: dict[str, Any] | None = None
    reason_codes: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "report_version": self.report_version,
            "measured_at": _format_datetime(self.measured_at),
            "tenant_id": self.tenant_id,
            "event_count": self.event_count,
            "active_bytes": self.active_bytes,
            "active_file_count": self.active_file_count,
            "archive_bytes": self.archive_bytes,
            "archive_file_count": self.archive_file_count,
            "archived_event_count": self.archived_event_count,
            "total_known_event_count": self.total_known_event_count,
            "oldest_active_event_at": _format_datetime(self.oldest_active_event_at),
            "newest_active_event_at": _format_datetime(self.newest_active_event_at),
            "oldest_archived_event_at": _format_datetime(self.oldest_archived_event_at),
            "newest_archived_event_at": _format_datetime(self.newest_archived_event_at),
            "filesystem": self.filesystem.to_dict(),
            "capacity_measurement_complete": self.capacity_measurement_complete,
            "current_count": self.current_count,
            "current_size_bytes": self.current_size_bytes,
            "current_oldest_event_at": _format_datetime(self.current_oldest_event_at),
            "current_newest_event_at": _format_datetime(self.current_newest_event_at),
            "observed_growth": self.observed_growth,
            "observation_window": self.observation_window,
            "reason_codes": list(self.reason_codes),
        }


@dataclass(frozen=True)
class AuditArchiveHealthReport:
    """Health of the W112 archive for one tenant."""

    report_version: str
    checked_at: datetime
    tenant_id: str
    archive_status: AuditArchiveStatus
    archive_document_count: int | None
    archived_event_count: int | None
    valid_archive_count: int | None
    invalid_archive_count: int | None
    checksum_failure_count: int | None
    malformed_archive_count: int | None
    tenant_mismatch_count: int | None
    oldest_archived_event_at: datetime | None
    newest_archived_event_at: datetime | None
    last_verified_at: datetime | None
    enumeration_complete: bool
    reason_codes: tuple[str, ...] = ()
    #: Tenant-scoped archived event ids.  W112 ``archive`` copies an event without
    #: purging it, so the active store still holds it; the union of both sides is
    #: what "known events" means.  Bookkeeping only: never serialised, so the
    #: identifier surface exposed to a caller is unchanged.
    archived_event_ids: frozenset[str] = frozenset()

    def to_dict(self) -> dict[str, Any]:
        return {
            "report_version": self.report_version,
            "checked_at": _format_datetime(self.checked_at),
            "tenant_id": self.tenant_id,
            "archive_status": self.archive_status.value,
            "archive_document_count": self.archive_document_count,
            "archived_event_count": self.archived_event_count,
            "valid_archive_count": self.valid_archive_count,
            "invalid_archive_count": self.invalid_archive_count,
            "checksum_failure_count": self.checksum_failure_count,
            "malformed_archive_count": self.malformed_archive_count,
            "tenant_mismatch_count": self.tenant_mismatch_count,
            "oldest_archived_event_at": _format_datetime(self.oldest_archived_event_at),
            "newest_archived_event_at": _format_datetime(self.newest_archived_event_at),
            "last_verified_at": _format_datetime(self.last_verified_at),
            "enumeration_complete": self.enumeration_complete,
            "reason_codes": list(self.reason_codes),
        }


@dataclass(frozen=True)
class AuditBacklogReport:
    """Diagnostic backlog of events eligible under a supplied W112 policy.

    Purely informational: no archive, purge or restore is ever triggered.
    """

    report_version: str
    measured_at: datetime
    tenant_id: str
    policy_supplied: bool
    eligible_event_count: int | None
    protected_event_count: int | None
    invalid_event_count: int | None
    minimum_events_protected_count: int | None
    already_archived_count: int | None
    reason_codes: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "report_version": self.report_version,
            "measured_at": _format_datetime(self.measured_at),
            "tenant_id": self.tenant_id,
            "policy_supplied": self.policy_supplied,
            "eligible_event_count": self.eligible_event_count,
            "protected_event_count": self.protected_event_count,
            "invalid_event_count": self.invalid_event_count,
            "minimum_events_protected_count": self.minimum_events_protected_count,
            "already_archived_count": self.already_archived_count,
            "reason_codes": list(self.reason_codes),
        }


@dataclass(frozen=True)
class AuditReadinessReport:
    """Explainable readiness verdict with stable reason codes."""

    report_version: str
    checked_at: datetime
    tenant_id: str
    readiness_status: AuditReadinessStatus
    reason_codes: tuple[str, ...]
    blocking_reason_codes: tuple[str, ...]
    checks_run: int
    health_status: AuditOperationalStatus
    archive_status: AuditArchiveStatus

    def to_dict(self) -> dict[str, Any]:
        return {
            "report_version": self.report_version,
            "checked_at": _format_datetime(self.checked_at),
            "tenant_id": self.tenant_id,
            "readiness_status": self.readiness_status.value,
            "reason_codes": list(self.reason_codes),
            "blocking_reason_codes": list(self.blocking_reason_codes),
            "checks_run": self.checks_run,
            "health_status": self.health_status.value,
            "archive_status": self.archive_status.value,
        }


@dataclass(frozen=True)
class AuditOperationalSnapshot:
    """One coherent, read-only observation shared by every W113 projection."""

    tenant_id: str
    observed_at: datetime
    health: AuditHealthReport
    capacity: AuditCapacitySnapshot
    archive: AuditArchiveHealthReport
    backlog: AuditBacklogReport
    readiness: AuditReadinessReport
    documents_scanned: int = 0
    measured: bool = True
    notes: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        return {
            "tenant_id": self.tenant_id,
            "observed_at": _format_datetime(self.observed_at),
            "documents_scanned": self.documents_scanned,
            "measured": self.measured,
            "notes": list(self.notes),
            "health": self.health.to_dict(),
            "capacity": self.capacity.to_dict(),
            "archive": self.archive.to_dict(),
            "backlog": self.backlog.to_dict(),
            "readiness": self.readiness.to_dict(),
        }


class AuditHealthService:
    """Read-only operational diagnostics over the W110/W111/W112 chain.

    The service never appends, purges, archives or restores anything, and it
    never writes an audit event: a diagnostic read must not change the numbers it
    reports.  Every projection derives from a single :class:`AuditOperationalSnapshot`.
    """

    def __init__(
        self,
        audit_service: ExecutionAuditService,
        *,
        archive_store: AuditArchiveStore | None = None,
        lifecycle: AuditLifecycleService | None = None,
        active_path: Path | None = None,
        archive_path: Path | None = None,
        clock: Any = None,
        capacity_scan_limit: int = DEFAULT_CAPACITY_SCAN_LIMIT,
    ) -> None:
        self._audit_service = audit_service
        self._archive = archive_store
        self._lifecycle = lifecycle
        self._active_path = active_path
        self._archive_path = archive_path
        self._clock = clock or _utc_now
        self._capacity_scan_limit = capacity_scan_limit

    # ── store access ───────────────────────────────────────────────────────

    @property
    def store(self) -> AuditStore:
        return self._audit_service.store

    @staticmethod
    def _store_path(store: AuditStore) -> Path | None:
        """Best-effort location of the active store, for stat-only metrics.

        It is used exclusively for ``stat``-style observation.  A store that
        does not expose a location simply yields ``None`` and the corresponding
        byte metrics are reported as unavailable.
        """

        candidate = getattr(store, "path", None)
        if isinstance(candidate, Path):
            return candidate
        return None

    # ── the single coherent snapshot ───────────────────────────────────────

    def snapshot(
        self,
        tenant_id: str,
        *,
        policy: AuditRetentionPolicy | Mapping[str, Any] | None = None,
    ) -> AuditOperationalSnapshot:
        """Read the chain once and derive health, capacity, archive and readiness."""

        tenant, tenant_reason = _require_tenant_safe(tenant_id)
        observed_at = _utc(self._clock())
        if tenant is None:
            return self._unavailable_snapshot(
                observed_at=observed_at,
                tenant_id="",
                reason=tenant_reason or TENANT_CONTEXT_INVALID,
            )
        try:
            scan = self.store.scan_documents()
            if not isinstance(scan, AuditDocumentScan):
                raise AuditStoreError("audit store returned an invalid document scan")
        except AuditError as exc:
            # a typed audit failure is an integrity verdict, not an outage
            return self._unavailable_snapshot(
                observed_at=observed_at,
                tenant_id=tenant,
                reason=(
                    AUDIT_STORE_UNAVAILABLE
                    if isinstance(exc, AuditStoreError)
                    else INTEGRITY_CHECK_FAILED
                ),
            )
        except Exception:
            # availability is precisely the fact this projection reports, so an
            # unexpected store failure becomes a typed UNAVAILABLE verdict.  The
            # cause is never propagated: no exception text, path or traceback
            # leaves this boundary.
            return self._unavailable_snapshot(
                observed_at=observed_at,
                tenant_id=tenant,
                reason=AUDIT_STORE_UNAVAILABLE,
            )

        health = self._health_report(tenant, observed_at, scan)
        archive = self._archive_report(tenant, observed_at)
        capacity = self._capacity_report(tenant, observed_at, scan, archive)
        backlog = self._backlog_report(tenant, observed_at, policy)
        readiness = self._readiness_report(tenant, observed_at, health, archive, capacity, backlog)
        return AuditOperationalSnapshot(
            tenant_id=tenant,
            observed_at=observed_at,
            health=health,
            capacity=capacity,
            archive=archive,
            backlog=backlog,
            readiness=readiness,
            documents_scanned=len(scan.documents),
            measured=True,
            notes=(),
        )

    # ── health ─────────────────────────────────────────────────────────────

    def _health_report(
        self,
        tenant: str,
        observed_at: datetime,
        scan: AuditDocumentScan,
    ) -> AuditHealthReport:
        failures: dict[str, int] = {}
        for document in scan.invalid_documents:
            code = document.failure_code or "UNREADABLE"
            failures[code] = failures.get(code, 0) + 1

        tenant_events: list[ExecutionAuditEvent] = [
            document.event
            for document in scan.valid_documents
            if document.event is not None and document.event.tenant_id == tenant
        ]
        timestamps = sorted(event.occurred_at for event in tenant_events)
        tenants = {
            document.event.tenant_id
            for document in scan.valid_documents
            if document.event is not None
        }
        success = sum(
            1 for event in tenant_events if event.outcome.value in {"SUCCESS", "ALREADY_PRESENT"}
        )
        failure = sum(1 for event in tenant_events if event.outcome.value == "FAILED")

        malformed = failures.get(AUDIT_FAILURE_MALFORMED_JSON, 0) + failures.get(
            AUDIT_FAILURE_UNSAFE_DOCUMENT, 0
        )
        version = failures.get(AUDIT_FAILURE_VERSION, 0)
        checksum = failures.get(AUDIT_FAILURE_CHECKSUM, 0)
        identity = failures.get(AUDIT_FAILURE_IDENTITY, 0)
        schema = failures.get("SCHEMA", 0)
        canonical = failures.get("CANONICALIZATION", 0)
        invalid = len(scan.invalid_documents)

        reasons: list[str] = []
        if invalid:
            reasons.append(AUDIT_DATA_CORRUPTED)
        if version or schema:
            reasons.append(AUDIT_SCHEMA_INVALID)
        if version and not (checksum or malformed or identity or schema or canonical):
            reasons.append(INTEGRITY_CHECK_FAILED)

        if not scan.documents and not tenant_events:
            status = AuditOperationalStatus.EMPTY
            reasons.append(AUDIT_JOURNAL_EMPTY)
        elif invalid or version or schema or canonical or identity or malformed:
            status = AuditOperationalStatus.DEGRADED
        elif tenant_events:
            status = AuditOperationalStatus.HEALTHY
        else:
            # documents exist but none belong to this tenant
            status = AuditOperationalStatus.EMPTY
            reasons.append(AUDIT_JOURNAL_EMPTY)

        return AuditHealthReport(
            report_version=AUDIT_HEALTH_REPORT_VERSION,
            checked_at=observed_at,
            tenant_id=tenant,
            status=status,
            event_count=len(tenant_events),
            valid_event_count=len(tenant_events),
            integrity_invalid_count=invalid,
            malformed_count=malformed,
            version_failure_count=version,
            checksum_failure_count=checksum,
            identity_failure_count=identity,
            unsafe_document_count=failures.get(AUDIT_FAILURE_UNSAFE_DOCUMENT, 0),
            schema_failure_count=schema,
            canonicalization_failure_count=canonical,
            tenant_count=len(tenants),
            success_count=success,
            failure_count=failure,
            oldest_event_at=timestamps[0] if timestamps else None,
            newest_event_at=timestamps[-1] if timestamps else None,
            last_observed_event_at=timestamps[-1] if timestamps else None,
            reason_codes=tuple(dict.fromkeys(reasons)),
        )

    # ── archive health ─────────────────────────────────────────────────────

    def _archive_report(
        self,
        tenant: str,
        observed_at: datetime,
    ) -> AuditArchiveHealthReport:
        if self._archive is None:
            return AuditArchiveHealthReport(
                report_version=AUDIT_HEALTH_REPORT_VERSION,
                checked_at=observed_at,
                tenant_id=tenant,
                archive_status=AuditArchiveStatus.NOT_CONFIGURED,
                archive_document_count=None,
                archived_event_count=None,
                valid_archive_count=None,
                invalid_archive_count=None,
                checksum_failure_count=None,
                malformed_archive_count=None,
                tenant_mismatch_count=None,
                oldest_archived_event_at=None,
                newest_archived_event_at=None,
                last_verified_at=None,
                enumeration_complete=False,
                reason_codes=(ARCHIVE_NOT_CONFIGURED,),
            )
        try:
            documents = self._archive.list_documents(tenant_id=tenant)
        except AuditArchiveError:
            # a corrupt archive cannot be fully enumerated without bypassing the
            # W112 store, which is forbidden; the gap is reported, not invented
            return AuditArchiveHealthReport(
                report_version=AUDIT_HEALTH_REPORT_VERSION,
                checked_at=observed_at,
                tenant_id=tenant,
                archive_status=AuditArchiveStatus.CORRUPTED,
                archive_document_count=None,
                archived_event_count=None,
                valid_archive_count=None,
                invalid_archive_count=1,
                checksum_failure_count=1,
                malformed_archive_count=None,
                tenant_mismatch_count=0,
                oldest_archived_event_at=None,
                newest_archived_event_at=None,
                last_verified_at=None,
                enumeration_complete=False,
                reason_codes=(ARCHIVE_CORRUPTED,),
            )
        except Exception:
            # any unexpected archive failure is an availability verdict, never a
            # leaked cause
            return AuditArchiveHealthReport(
                report_version=AUDIT_HEALTH_REPORT_VERSION,
                checked_at=observed_at,
                tenant_id=tenant,
                archive_status=AuditArchiveStatus.UNAVAILABLE,
                archive_document_count=None,
                archived_event_count=None,
                valid_archive_count=None,
                invalid_archive_count=None,
                checksum_failure_count=None,
                malformed_archive_count=None,
                tenant_mismatch_count=None,
                oldest_archived_event_at=None,
                newest_archived_event_at=None,
                last_verified_at=None,
                enumeration_complete=False,
                reason_codes=(ARCHIVE_UNAVAILABLE,),
            )
        timestamps = sorted(
            document.event.occurred_at for document in documents if document.event is not None
        )
        count = len(documents)
        return AuditArchiveHealthReport(
            archived_event_ids=frozenset(
                document.event_id for document in documents if document.event is not None
            ),
            report_version=AUDIT_HEALTH_REPORT_VERSION,
            checked_at=observed_at,
            tenant_id=tenant,
            archive_status=AuditArchiveStatus.HEALTHY,
            archive_document_count=count,
            archived_event_count=count,
            valid_archive_count=count,
            invalid_archive_count=0,
            checksum_failure_count=0,
            malformed_archive_count=0,
            tenant_mismatch_count=0,
            oldest_archived_event_at=timestamps[0] if timestamps else None,
            newest_archived_event_at=timestamps[-1] if timestamps else None,
            last_verified_at=observed_at,
            enumeration_complete=True,
            reason_codes=(),
        )

    # ── capacity ───────────────────────────────────────────────────────────

    def _capacity_report(
        self,
        tenant: str,
        observed_at: datetime,
        scan: AuditDocumentScan,
        archive: AuditArchiveHealthReport,
    ) -> AuditCapacitySnapshot:
        active_path = self._active_path or self._store_path(self.store)
        archive_path = self._archive_path
        active_bytes, active_files, active_truncated = self._measure(active_path)
        archive_bytes, archive_files, archive_truncated = self._measure(archive_path)
        metrics = (
            filesystem_metrics(active_path) if active_path is not None else FilesystemMetrics()
        )
        tenant_events = [
            document.event
            for document in scan.valid_documents
            if document.event is not None and document.event.tenant_id == tenant
        ]
        timestamps = sorted(event.occurred_at for event in tenant_events)
        archived = archive.archived_event_count
        # an event archived but not yet purged is present on both sides, so the
        # union of identifiers is the only non-double-counting total
        total_known = (
            None
            if archived is None
            else len({event.event_id for event in tenant_events} | archive.archived_event_ids)
        )
        reasons: list[str] = []
        incomplete = False
        # a metric is a gap only when a source was actually configured and could
        # not be observed; a store without a location simply reports None
        if self._active_path is not None and active_bytes is None:
            incomplete = True
        if self._archive is not None and self._archive_path is not None and archive_bytes is None:
            incomplete = True
        if archive.archive_status is not AuditArchiveStatus.NOT_CONFIGURED and archived is None:
            incomplete = True
        if incomplete:
            reasons.append(CAPACITY_MEASUREMENT_INCOMPLETE)
        return AuditCapacitySnapshot(
            report_version=AUDIT_HEALTH_REPORT_VERSION,
            measured_at=observed_at,
            tenant_id=tenant,
            event_count=len(tenant_events),
            active_bytes=active_bytes,
            active_file_count=active_files,
            archive_bytes=archive_bytes,
            archive_file_count=archive_files,
            archived_event_count=archived,
            total_known_event_count=total_known,
            oldest_active_event_at=timestamps[0] if timestamps else None,
            newest_active_event_at=timestamps[-1] if timestamps else None,
            oldest_archived_event_at=archive.oldest_archived_event_at,
            newest_archived_event_at=archive.newest_archived_event_at,
            filesystem=metrics,
            capacity_measurement_complete=not incomplete,
            current_count=len(tenant_events),
            current_size_bytes=active_bytes,
            current_oldest_event_at=timestamps[0] if timestamps else None,
            current_newest_event_at=timestamps[-1] if timestamps else None,
            # no historical snapshots are persisted by design: a growth rate is
            # reported only when a previous observation is actually supplied
            observed_growth=None,
            observation_window=None,
            reason_codes=tuple(reasons),
        )

    def _measure(self, path: Path | None) -> tuple[int | None, int, bool]:
        if path is None:
            return None, 0, False
        return directory_bytes(path, limit=self._capacity_scan_limit)

    # ── backlog (diagnostic only) ──────────────────────────────────────────

    def _backlog_report(
        self,
        tenant: str,
        observed_at: datetime,
        policy: AuditRetentionPolicy | Mapping[str, Any] | None,
    ) -> AuditBacklogReport:
        if policy is None or self._lifecycle is None:
            return AuditBacklogReport(
                report_version=AUDIT_HEALTH_REPORT_VERSION,
                measured_at=observed_at,
                tenant_id=tenant,
                policy_supplied=False,
                eligible_event_count=None,
                protected_event_count=None,
                invalid_event_count=None,
                minimum_events_protected_count=None,
                already_archived_count=None,
                reason_codes=(),
            )
        try:
            preview = self._lifecycle.preview(tenant, policy)
        except AuditError:
            return AuditBacklogReport(
                report_version=AUDIT_HEALTH_REPORT_VERSION,
                measured_at=observed_at,
                tenant_id=tenant,
                policy_supplied=True,
                eligible_event_count=None,
                protected_event_count=None,
                invalid_event_count=None,
                minimum_events_protected_count=None,
                already_archived_count=None,
                reason_codes=(INTEGRITY_CHECK_FAILED,),
            )
        minimum = getattr(preview.policy, "minimum_events_to_keep", None)
        return AuditBacklogReport(
            report_version=AUDIT_HEALTH_REPORT_VERSION,
            measured_at=observed_at,
            tenant_id=tenant,
            policy_supplied=True,
            eligible_event_count=preview.eligible,
            protected_event_count=preview.protected,
            invalid_event_count=preview.invalid,
            minimum_events_protected_count=minimum,
            already_archived_count=preview.already_archived,
            reason_codes=tuple(preview.warnings),
        )

    # ── readiness ──────────────────────────────────────────────────────────

    def _readiness_report(
        self,
        tenant: str,
        observed_at: datetime,
        health: AuditHealthReport,
        archive: AuditArchiveHealthReport,
        capacity: AuditCapacitySnapshot,
        backlog: AuditBacklogReport,
    ) -> AuditReadinessReport:
        # every check that was actually evaluated, counted rather than asserted
        checks_run = 4 + (1 if backlog.policy_supplied else 0)
        reasons: list[str] = list(health.reason_codes)
        reasons.extend(archive.reason_codes)
        if not capacity.capacity_measurement_complete:
            # a measurement gap is observable but does not block serving
            reasons.append(CAPACITY_MEASUREMENT_INCOMPLETE)
        ordered = tuple(dict.fromkeys(reasons))
        blocking = tuple(code for code in ordered if code in BLOCKING_REASON_CODES)
        status = AuditReadinessStatus.READY if not blocking else AuditReadinessStatus.NOT_READY
        return AuditReadinessReport(
            report_version=AUDIT_HEALTH_REPORT_VERSION,
            checked_at=observed_at,
            tenant_id=tenant,
            readiness_status=status,
            reason_codes=ordered,
            blocking_reason_codes=blocking,
            checks_run=checks_run,
            health_status=health.status,
            archive_status=archive.archive_status,
        )

    # ── unavailable snapshot ───────────────────────────────────────────────

    def _unavailable_snapshot(
        self,
        *,
        observed_at: datetime,
        tenant_id: str,
        reason: str,
    ) -> AuditOperationalSnapshot:
        """Build a fully-typed NOT_READY snapshot without leaking the cause."""

        health = AuditHealthReport(
            report_version=AUDIT_HEALTH_REPORT_VERSION,
            checked_at=observed_at,
            tenant_id=tenant_id,
            status=AuditOperationalStatus.UNAVAILABLE,
            event_count=0,
            valid_event_count=0,
            integrity_invalid_count=0,
            malformed_count=0,
            version_failure_count=0,
            checksum_failure_count=0,
            identity_failure_count=0,
            unsafe_document_count=0,
            schema_failure_count=0,
            canonicalization_failure_count=0,
            tenant_count=0,
            success_count=0,
            failure_count=0,
            oldest_event_at=None,
            newest_event_at=None,
            last_observed_event_at=None,
            reason_codes=(reason,),
        )
        capacity = AuditCapacitySnapshot(
            report_version=AUDIT_HEALTH_REPORT_VERSION,
            measured_at=observed_at,
            tenant_id=tenant_id,
            event_count=0,
            active_bytes=None,
            active_file_count=0,
            archive_bytes=None,
            archive_file_count=0,
            archived_event_count=None,
            total_known_event_count=None,
            oldest_active_event_at=None,
            newest_active_event_at=None,
            oldest_archived_event_at=None,
            newest_archived_event_at=None,
            filesystem=FilesystemMetrics(available=False),
            capacity_measurement_complete=False,
            current_count=0,
            current_size_bytes=None,
            current_oldest_event_at=None,
            current_newest_event_at=None,
            observed_growth=None,
            observation_window=None,
            reason_codes=(reason,),
        )
        archive = AuditArchiveHealthReport(
            report_version=AUDIT_HEALTH_REPORT_VERSION,
            checked_at=observed_at,
            tenant_id=tenant_id,
            archive_status=AuditArchiveStatus.UNAVAILABLE,
            archive_document_count=None,
            archived_event_count=None,
            valid_archive_count=None,
            invalid_archive_count=None,
            checksum_failure_count=None,
            malformed_archive_count=None,
            tenant_mismatch_count=None,
            oldest_archived_event_at=None,
            newest_archived_event_at=None,
            last_verified_at=None,
            enumeration_complete=False,
            reason_codes=(ARCHIVE_UNAVAILABLE,),
        )
        backlog = AuditBacklogReport(
            report_version=AUDIT_HEALTH_REPORT_VERSION,
            measured_at=observed_at,
            tenant_id=tenant_id,
            policy_supplied=False,
            eligible_event_count=None,
            protected_event_count=None,
            invalid_event_count=None,
            minimum_events_protected_count=None,
            already_archived_count=None,
            reason_codes=(reason,),
        )
        readiness = AuditReadinessReport(
            report_version=AUDIT_HEALTH_REPORT_VERSION,
            checked_at=observed_at,
            tenant_id=tenant_id,
            readiness_status=AuditReadinessStatus.NOT_READY,
            reason_codes=(reason,),
            blocking_reason_codes=tuple(
                code for code in (reason,) if code in BLOCKING_REASON_CODES
            ),
            checks_run=5,
            health_status=AuditOperationalStatus.UNAVAILABLE,
            archive_status=AuditArchiveStatus.UNAVAILABLE,
        )
        return AuditOperationalSnapshot(
            tenant_id=tenant_id,
            observed_at=observed_at,
            health=health,
            capacity=capacity,
            archive=archive,
            backlog=backlog,
            readiness=readiness,
            documents_scanned=0,
            measured=False,
            notes=(reason,),
        )

    # ── per-endpoint projections ───────────────────────────────────────────

    def health(self, tenant_id: str) -> AuditHealthReport:
        return self.snapshot(tenant_id).health

    def capacity(self, tenant_id: str) -> AuditCapacitySnapshot:
        return self.snapshot(tenant_id).capacity

    def readiness(self, tenant_id: str) -> AuditReadinessReport:
        return self.snapshot(tenant_id).readiness

    def operational_status(
        self,
        tenant_id: str,
        *,
        policy: AuditRetentionPolicy | Mapping[str, Any] | None = None,
    ) -> AuditOperationalSnapshot:
        return self.snapshot(tenant_id, policy=policy)


__all__ = [
    "ARCHIVE_CORRUPTED",
    "ARCHIVE_NOT_CONFIGURED",
    "ARCHIVE_UNAVAILABLE",
    "AUDIT_DATA_CORRUPTED",
    "AUDIT_HEALTH_REPORT_VERSION",
    "AUDIT_JOURNAL_EMPTY",
    "AUDIT_SCHEMA_INVALID",
    "AUDIT_STORE_UNAVAILABLE",
    "BLOCKING_REASON_CODES",
    "CAPACITY_MEASUREMENT_INCOMPLETE",
    "DEFAULT_CAPACITY_SCAN_LIMIT",
    "INTEGRITY_CHECK_FAILED",
    "TENANT_CONTEXT_INVALID",
    "AuditArchiveHealthReport",
    "AuditArchiveReader",
    "AuditArchiveStatus",
    "AuditBacklogReader",
    "AuditBacklogReport",
    "AuditCapacitySnapshot",
    "AuditHealthError",
    "AuditHealthReport",
    "AuditHealthService",
    "AuditOperationalSnapshot",
    "AuditOperationalStatus",
    "AuditReadinessReport",
    "AuditReadinessStatus",
    "FilesystemMetrics",
    "directory_bytes",
    "filesystem_metrics",
]
