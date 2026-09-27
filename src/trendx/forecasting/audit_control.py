"""W111 — execution audit integrity, reconciliation and controlled export.

W111 is a strictly **read-only control plane** on top of the W110 audit trail.
It never opens an audit document itself: every read goes through the W110
:class:`~trendx.forecasting.audit.ExecutionAuditStore` abstraction (and the
:class:`~trendx.forecasting.audit.ExecutionAuditService` above it), so the W110
sanitation, checksum, tenant and idempotency contracts remain the single source
of truth.

Three capabilities are provided:

``ExecutionAuditIntegrityService``
    Deterministic, per-document integrity verdicts.  A corrupt document is
    reported as data (``INVALID``) and never silently skipped, repaired or
    hidden behind a generic failure.

``ExecutionAuditReconciliationService``
    Explicit, documented logical rules over the audit events themselves.  A rule
    that cannot be decided with the available events yields a warning, never an
    invented conclusion.

``ExecutionAuditExportService``
    Bounded, tenant-isolated, deterministic JSON/JSONL export with a
    reproducible SHA-256 ``export_checksum``.

Scope guard: W111 adds no backend, no scheduler, no worker, no retention, no
alerting and no new authentication or tenant logic.  It does not modify
``ExecutionRecord``, ``ExecutionHistoryRecord``, the W107 archive store, the
W108 recovery service or the W109 recovery API.
"""

from __future__ import annotations

import hashlib
from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import Enum
from typing import Any

from trendx.forecasting.audit import (
    AUDIT_FAILURE_CANONICAL,
    AUDIT_FAILURE_CHECKSUM,
    AUDIT_FAILURE_IDENTITY,
    AUDIT_FAILURE_MALFORMED_JSON,
    AUDIT_FAILURE_SCHEMA,
    AUDIT_FAILURE_UNREADABLE,
    AUDIT_FAILURE_UNSAFE_DOCUMENT,
    AUDIT_FAILURE_VERSION,
    MAX_AUDIT_OFFSET,
    MAX_AUDIT_QUERY_LIMIT,
    AuditDocumentScan,
    AuditError,
    AuditIntegrityError,
    AuditOperation,
    AuditOutcome,
    AuditQueryError,
    AuditStore,
    AuditStoreError,
    AuditValidationError,
    ExecutionAuditEvent,
    ExecutionAuditQuery,
    ExecutionAuditService,
    _canonical_json,
    _utc_now,
)

AUDIT_CONTROL_REPORT_VERSION = "1"
AUDIT_EXPORT_VERSION = "1"

#: Hard bounds for a single export.  They intentionally mirror the W110 query
#: bounds so a control-plane read can never be wider than a W110 read.
MAX_AUDIT_EXPORT_LIMIT = MAX_AUDIT_QUERY_LIMIT
DEFAULT_AUDIT_EXPORT_LIMIT = 100
MAX_AUDIT_EXPORT_OFFSET = MAX_AUDIT_OFFSET

#: Source recorded for a W111 operation if the host ever decides to audit the
#: control plane itself.  It exists to make the no-recursion rule explicit and
#: is not used by the default read path, which never appends an event.
AUDIT_CONTROL_SOURCE = "AUDIT_CONTROL_PLANE"

_REDACTED = "[REDACTED]"
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
_MAX_FINDINGS = 200


class AuditControlError(AuditError):
    """Base class for W111 control-plane errors."""


class IntegrityStatus(str, Enum):
    """Terminal state of an integrity evaluation."""

    VALID = "VALID"
    INVALID = "INVALID"
    EMPTY = "EMPTY"
    ERROR = "ERROR"


class ReconciliationStatus(str, Enum):
    """Terminal state of a reconciliation evaluation."""

    CONSISTENT = "CONSISTENT"
    INCONSISTENT = "INCONSISTENT"
    INSUFFICIENT_DATA = "INSUFFICIENT_DATA"
    ERROR = "ERROR"


class ExportFormat(str, Enum):
    """Serialisation formats allowed by W111."""

    JSON = "JSON"
    JSONL = "JSONL"


class FindingSeverity(str, Enum):
    """Severity of a reconciliation finding."""

    INCONSISTENCY = "INCONSISTENCY"
    WARNING = "WARNING"


def _utc(value: datetime | None) -> datetime:
    if value is None:
        return _utc_now()
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _format_datetime(value: datetime) -> str:
    return _utc(value).isoformat()


def _looks_sensitive(value: str) -> bool:
    lowered = value.casefold()
    return any(marker in lowered for marker in _SENSITIVE_MARKERS)


def _safe_identifier(value: str | None) -> str | None:
    """Fail closed on anything path-like, secret-like or oversized.

    W110 already refuses to persist such values; this is a defence-in-depth
    invariant for the control plane so a future writer cannot leak a path or a
    credential through an export.
    """

    if value is None:
        return None
    normalized = value.replace("\\", "/")
    if (
        "\x00" in value
        or "/" in normalized
        or "\n" in value
        or "\r" in value
        or "." * 2 in normalized
        or (len(normalized) >= 2 and normalized[1] == ":" and normalized[0].isalpha())
        or len(value) > 512
        or _looks_sensitive(value)
    ):
        return _REDACTED
    return value


def assert_export_safe(payload: Mapping[str, Any]) -> None:
    """Reject any export payload that could disclose a path or a credential.

    This is intentionally a hard invariant: it raises instead of redacting, so a
    leak attempt fails closed rather than shipping a partially redacted export.
    """

    for key, value in payload.items():
        if _looks_sensitive(str(key)):
            raise AuditControlError("export field is not permitted")
        if value is None:
            continue
        if not isinstance(value, str):
            continue
        if _safe_identifier(value) == _REDACTED:
            raise AuditControlError("export value is not permitted")
    for key in ("execution_id", "reference_key", "actor_id", "request_id", "source", "reason_code"):
        value = payload.get(key)
        if isinstance(value, str) and _safe_identifier(value) == _REDACTED:
            raise AuditControlError("export value is not permitted")


def _sanitized_event_dict(event: ExecutionAuditEvent) -> dict[str, Any]:
    """Project one validated W110 event onto the exportable contract.

    The field set is exactly the W110 durable event contract.  No credential,
    request body, archive content, traceback or filesystem path is ever added.
    """

    assert_export_safe(event.to_dict())
    return event.to_dict()


def _ordered_events(events: Iterable[ExecutionAuditEvent]) -> tuple[ExecutionAuditEvent, ...]:
    """Contractual export order: ``occurred_at`` ASC, then ``event_id`` ASC."""

    return tuple(sorted(events, key=lambda event: (event.occurred_at, event.event_id)))


@dataclass(frozen=True)
class IntegrityReport:
    """Structured, tenant-scoped integrity verdict."""

    report_version: str
    generated_at: datetime
    tenant_id: str
    events_scanned: int
    valid_events: int
    invalid_events: int
    duplicate_events: int
    checksum_failures: int
    schema_failures: int
    version_failures: int
    canonicalization_failures: int
    identity_failures: int
    unsafe_documents: int
    unreadable_documents: int
    malformed_documents: int
    first_failure: str | None
    integrity_status: IntegrityStatus
    documents_scanned: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "report_version": self.report_version,
            "generated_at": _format_datetime(self.generated_at),
            "tenant_id": self.tenant_id,
            "documents_scanned": self.documents_scanned,
            "events_scanned": self.events_scanned,
            "valid_events": self.valid_events,
            "invalid_events": self.invalid_events,
            "duplicate_events": self.duplicate_events,
            "checksum_failures": self.checksum_failures,
            "schema_failures": self.schema_failures,
            "version_failures": self.version_failures,
            "canonicalization_failures": self.canonicalization_failures,
            "identity_failures": self.identity_failures,
            "unsafe_documents": self.unsafe_documents,
            "unreadable_documents": self.unreadable_documents,
            "malformed_documents": self.malformed_documents,
            "first_failure": self.first_failure,
            "integrity_status": self.integrity_status.value,
        }


@dataclass(frozen=True)
class ReconciliationFinding:
    """One bounded, non-sensitive reconciliation finding."""

    code: str
    severity: FindingSeverity
    detail: str
    event_id: str | None = None
    operation: str | None = None
    request_id: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "code": self.code,
            "severity": self.severity.value,
            "detail": self.detail,
            "event_id": self.event_id,
            "operation": self.operation,
            "request_id": self.request_id,
        }


@dataclass(frozen=True)
class ReconciliationReport:
    """Structured, tenant-scoped reconciliation verdict."""

    report_version: str
    generated_at: datetime
    tenant_id: str
    events_scanned: int
    checks_run: int
    inconsistencies: int
    warnings: int
    correlation_chains: int
    first_failure: str | None
    reconciliation_status: ReconciliationStatus
    findings: tuple[ReconciliationFinding, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        return {
            "report_version": self.report_version,
            "generated_at": _format_datetime(self.generated_at),
            "tenant_id": self.tenant_id,
            "events_scanned": self.events_scanned,
            "checks_run": self.checks_run,
            "inconsistencies": self.inconsistencies,
            "warnings": self.warnings,
            "correlation_chains": self.correlation_chains,
            "first_failure": self.first_failure,
            "reconciliation_status": self.reconciliation_status.value,
            "findings": [finding.to_dict() for finding in self.findings],
        }


@dataclass(frozen=True)
class ExecutionAuditExport:
    """Deterministic export payload plus its reproducible SHA-256 checksum."""

    export_version: str
    export_format: ExportFormat
    tenant_id: str
    events: tuple[ExecutionAuditEvent, ...]
    total: int
    limit: int
    offset: int
    generated_at: datetime

    @property
    def count(self) -> int:
        return len(self.events)

    @property
    def has_more(self) -> bool:
        return self.offset + self.count < self.total

    @property
    def canonical_bytes(self) -> bytes:
        """Exact deterministic bytes covered by ``export_checksum``.

        ``generated_at`` is transport metadata and is deliberately excluded so
        two processes reading the same state produce byte-identical exports.
        """

        if self.export_format is ExportFormat.JSON:
            envelope = {
                "export_version": self.export_version,
                "format": self.export_format.value,
                "tenant_id": self.tenant_id,
                "event_count": self.count,
                "events": [_sanitized_event_dict(event) for event in self.events],
            }
            return _canonical_json(envelope) + b"\n"
        lines = [_canonical_json(_sanitized_event_dict(event)) for event in self.events]
        return b"".join(line + b"\n" for line in lines)

    @property
    def export_checksum(self) -> str:
        """SHA-256 of the canonical export bytes (never MD5)."""

        return hashlib.sha256(self.canonical_bytes).hexdigest()

    def to_dict(self, *, include_content: bool = True) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "export_version": self.export_version,
            "format": self.export_format.value,
            "tenant_id": self.tenant_id,
            "event_count": self.count,
            "total": self.total,
            "limit": self.limit,
            "offset": self.offset,
            "has_more": self.has_more,
            "export_checksum": self.export_checksum,
            "generated_at": _format_datetime(self.generated_at),
        }
        if include_content:
            payload["content"] = self.canonical_bytes.decode("utf-8")
        return payload


class ExecutionAuditControlService:
    """Shared, read-only W111 access to the W110 audit store.

    The class deliberately exposes **no** mutation primitive: it only forwards
    reads to the W110 store, so a W111 caller cannot write an audit event by
    construction.
    """

    def __init__(self, service: ExecutionAuditService) -> None:
        self._service = service

    @property
    def service(self) -> ExecutionAuditService:
        """The underlying W110 service, for composing sibling W111 controls."""

        return self._service

    @property
    def store(self) -> AuditStore:
        return self._service.store

    def scan(self) -> AuditDocumentScan:
        """Return the W110 per-document verdicts (read-only)."""

        scanner = getattr(self.store, "scan_documents", None)
        if not callable(scanner):
            msg = "audit store does not support a read-only document scan"
            raise AuditStoreError(msg)
        scan = scanner()
        if not isinstance(scan, AuditDocumentScan):
            msg = "audit store returned an invalid document scan"
            raise AuditStoreError(msg)
        return scan

    def query(self, query: ExecutionAuditQuery) -> Any:
        """Forward a bounded W110 query unchanged (read-only)."""

        return self._service.query(query)

    def events_for(
        self,
        query: ExecutionAuditQuery,
    ) -> tuple[ExecutionAuditEvent, ...]:
        """Return the **complete** tenant-scoped event set matching ``query``.

        A document that cannot be trusted is never silently dropped: it raises
        so the caller answers ``503`` instead of reporting a partial result.

        The match set is deliberately complete rather than windowed. A truncated
        view would make reconciliation emit false "not observable" warnings and
        would understate the export ``total``. Paging is applied by the caller,
        so the wire bound (``limit`` ≤ 1000) still limits any single response.
        """

        scan = self.scan()
        invalid = scan.invalid_documents
        if invalid:
            first = invalid[0]
            raise AuditIntegrityError(
                "audit source is not trustworthy: "
                f"{first.failure_code or 'UNKNOWN_DOCUMENT_FAILURE'}"
            )
        matching = [
            document.event
            for document in scan.valid_documents
            if document.event is not None and query.matches(document.event)
        ]
        return _ordered_events(matching)


def _validate_bounded(
    *,
    limit: int,
    offset: int,
    limit_max: int = MAX_AUDIT_EXPORT_LIMIT,
) -> None:
    if isinstance(limit, bool) or not isinstance(limit, int):
        raise AuditQueryError("limit must be an integer")
    if limit < 1 or limit > limit_max:
        raise AuditQueryError("limit is outside the allowed range")
    if isinstance(offset, bool) or not isinstance(offset, int):
        raise AuditQueryError("offset must be an integer")
    if offset < 0 or offset > MAX_AUDIT_EXPORT_OFFSET:
        raise AuditQueryError("offset is outside the allowed range")


def _require_tenant(tenant_id: str | None) -> str:
    if not isinstance(tenant_id, str) or not tenant_id.strip():
        raise AuditValidationError("tenant_id is required")
    tenant = tenant_id.strip()
    if len(tenant) > 256 or _safe_identifier(tenant) == _REDACTED:
        raise AuditValidationError("tenant_id is not permitted")
    return tenant


class ExecutionAuditIntegrityService(ExecutionAuditControlService):
    """Read-only integrity verification over the W110 audit documents.

    Verification is deterministic: for the same store state the report, every
    counter and ``first_failure`` are identical in any process.
    """

    def verify(
        self,
        tenant_id: str,
        *,
        query: ExecutionAuditQuery | None = None,
    ) -> IntegrityReport:
        tenant = _require_tenant(tenant_id)
        if query is not None:
            # the incoming bounds are still validated, but reconciliation always
            # evaluates the complete match set so a rule is never fed a
            # truncated view and emits a false "not observable" warning
            _validate_bounded(limit=query.limit, offset=query.offset)
        generated_at = _utc(None)
        try:
            scan = self.scan()
        except (AuditStoreError, OSError):
            raise
        except AuditError:
            return self._error_report(tenant, generated_at)
        try:
            return self._build_report(tenant, generated_at, scan, query)
        except Exception:  # pragma: no cover - defensive fail-closed branch
            return self._error_report(tenant, generated_at)

    def _error_report(self, tenant: str, generated_at: datetime) -> IntegrityReport:
        return IntegrityReport(
            report_version=AUDIT_CONTROL_REPORT_VERSION,
            generated_at=generated_at,
            tenant_id=tenant,
            events_scanned=0,
            valid_events=0,
            invalid_events=0,
            duplicate_events=0,
            checksum_failures=0,
            schema_failures=0,
            version_failures=0,
            canonicalization_failures=0,
            identity_failures=0,
            unsafe_documents=0,
            unreadable_documents=0,
            malformed_documents=0,
            first_failure="integrity_evaluation_failed",
            integrity_status=IntegrityStatus.ERROR,
            documents_scanned=0,
        )

    def _build_report(
        self,
        tenant: str,
        generated_at: datetime,
        scan: AuditDocumentScan,
        query: ExecutionAuditQuery | None = None,
    ) -> IntegrityReport:
        counters: dict[str, int] = {
            AUDIT_FAILURE_CHECKSUM: 0,
            AUDIT_FAILURE_SCHEMA: 0,
            AUDIT_FAILURE_VERSION: 0,
            AUDIT_FAILURE_CANONICAL: 0,
            AUDIT_FAILURE_IDENTITY: 0,
            AUDIT_FAILURE_UNSAFE_DOCUMENT: 0,
            AUDIT_FAILURE_UNREADABLE: 0,
            AUDIT_FAILURE_MALFORMED_JSON: 0,
        }
        first_failure: str | None = None
        for document in scan.invalid_documents:
            code = document.failure_code or AUDIT_FAILURE_UNREADABLE
            counters[code] = counters.get(code, 0) + 1
            if first_failure is None:
                first_failure = f"{code}:{document.detail or 'document_rejected'}"

        # Valid documents of *other* tenants are never counted, never named and
        # never exported; a corrupt document stays a store-level counter so an
        # operator is never told the store is clean when it is not.
        valid_events: list[ExecutionAuditEvent] = []
        tenant_scanned = 0
        for document in scan.valid_documents:
            event = document.event
            if event is None or event.tenant_id != tenant:
                continue
            if query is not None and not query.matches(event):
                continue
            tenant_scanned += 1
            event.verify_integrity()
            valid_events.append(event)

        duplicates = _count_duplicates(valid_events)
        if duplicates and first_failure is None:
            first_failure = "DUPLICATE_EVENT_IDENTITY"

        invalid_events = len(scan.invalid_documents)
        if scan.documents:
            status = (
                IntegrityStatus.INVALID if (invalid_events or duplicates) else IntegrityStatus.VALID
            )
        else:
            status = IntegrityStatus.EMPTY
        return IntegrityReport(
            report_version=AUDIT_CONTROL_REPORT_VERSION,
            generated_at=generated_at,
            tenant_id=tenant,
            events_scanned=tenant_scanned,
            valid_events=len(valid_events),
            invalid_events=invalid_events,
            duplicate_events=duplicates,
            checksum_failures=counters[AUDIT_FAILURE_CHECKSUM],
            schema_failures=counters[AUDIT_FAILURE_SCHEMA],
            version_failures=counters[AUDIT_FAILURE_VERSION],
            canonicalization_failures=counters[AUDIT_FAILURE_CANONICAL],
            identity_failures=counters[AUDIT_FAILURE_IDENTITY],
            unsafe_documents=counters[AUDIT_FAILURE_UNSAFE_DOCUMENT],
            unreadable_documents=counters[AUDIT_FAILURE_UNREADABLE],
            malformed_documents=counters[AUDIT_FAILURE_MALFORMED_JSON],
            first_failure=first_failure,
            integrity_status=status,
            documents_scanned=len(scan.documents),
        )


def _count_duplicates(events: list[ExecutionAuditEvent]) -> int:
    """Count events sharing an identity or an idempotency key with a different id."""

    duplicates = 0
    seen_ids: set[str] = set()
    for event in events:
        if event.event_id in seen_ids:
            duplicates += 1
        seen_ids.add(event.event_id)
    keys: dict[str, str] = {}
    for event in events:
        key = event.idempotency_key
        if not key:
            continue
        previous = keys.get(key)
        if previous is not None and previous != event.event_id:
            duplicates += 1
        keys[key] = event.event_id
    return duplicates


# ── Reconciliation rules ──────────────────────────────────────────────────
#
# Every rule below is explicit, bounded and derived from the reason codes that
# W107/W108/W109 actually write.  A rule only reports an inconsistency when the
# contradiction is provable from the events; an undecidable case becomes a
# warning, never an invented conclusion.

_NEGATIVE_REASON_MARKERS = (
    "failed",
    "failure",
    "forbidden",
    "refused",
    "rejected",
    "required",
    "missing",
    "unverified",
    "unavailable",
    "invalid",
    "inconsistent",
    "mismatch",
    "conflict",
    "changed",
    "protected",
    "dry_run",
    "tenant_forbidden",
    "retention_protected",
    "archive_required",
    "lifecycle_failed",
)

#: Reason codes W109 emits for a successful HTTP 200 restore.
_RECOVERY_API_SUCCESS_REASONS = frozenset({"record_restored", "record_already_present"})

#: Outcomes that describe a completed business action and therefore require an
#: execution identity.
_IDENTITY_REQUIRED_OUTCOMES = frozenset({AuditOutcome.SUCCESS, AuditOutcome.ALREADY_PRESENT})

_ARCHIVE_OPERATIONS = frozenset(
    {AuditOperation.ARCHIVE, AuditOperation.ARCHIVE_CREATE, AuditOperation.ARCHIVE_VERIFY}
)
_PURGE_OPERATIONS = frozenset(
    {AuditOperation.PURGE, AuditOperation.PURGE_PREVIEW, AuditOperation.PURGE_EXECUTE}
)
_RESTORE_OPERATIONS = frozenset(
    {AuditOperation.RESTORE, AuditOperation.RESTORE_VERIFY, AuditOperation.RESTORE_EXECUTE}
)
_CHAIN_ORDER: tuple[AuditOperation, ...] = (
    AuditOperation.RECOVERY_API,
    AuditOperation.RESTORE,
    AuditOperation.ARCHIVE,
)


def _chain_rank(operation: AuditOperation) -> int | None:
    for index, candidate in enumerate(_CHAIN_ORDER):
        if candidate is operation or (
            operation.value.startswith(candidate.value) and operation is not candidate
        ):
            return index
    return None


def _is_negative_reason(reason_code: str) -> bool:
    lowered = reason_code.casefold()
    return any(marker in lowered for marker in _NEGATIVE_REASON_MARKERS)


class ExecutionAuditReconciliationService(ExecutionAuditControlService):
    """Read-only logical reconciliation over the tenant's audit events."""

    def reconcile(
        self,
        tenant_id: str,
        *,
        query: ExecutionAuditQuery | None = None,
    ) -> ReconciliationReport:
        tenant = _require_tenant(tenant_id)
        generated_at = _utc(None)
        if query is not None:
            # the incoming bounds are still validated, but reconciliation always
            # evaluates the complete match set so a rule is never fed a
            # truncated view and emits a false "not observable" warning
            _validate_bounded(limit=query.limit, offset=query.offset)
            active_query = query
        else:
            active_query = ExecutionAuditQuery(tenant_id=tenant)
        try:
            events = self.events_for(active_query)
        except (AuditStoreError, AuditIntegrityError, OSError):
            raise
        except AuditError:
            return ReconciliationReport(
                report_version=AUDIT_CONTROL_REPORT_VERSION,
                generated_at=generated_at,
                tenant_id=tenant,
                events_scanned=0,
                checks_run=0,
                inconsistencies=0,
                warnings=1,
                correlation_chains=0,
                first_failure="reconciliation_evaluation_failed",
                reconciliation_status=ReconciliationStatus.ERROR,
                findings=(
                    ReconciliationFinding(
                        code="RECONCILIATION_EVALUATION_FAILED",
                        severity=FindingSeverity.WARNING,
                        detail="reconciliation could not be evaluated",
                    ),
                ),
            )
        try:
            return self._build_report(tenant, generated_at, events)
        except Exception:  # pragma: no cover - defensive fail-closed branch
            return ReconciliationReport(
                report_version=AUDIT_CONTROL_REPORT_VERSION,
                generated_at=generated_at,
                tenant_id=tenant,
                events_scanned=len(events),
                checks_run=0,
                inconsistencies=0,
                warnings=1,
                correlation_chains=0,
                first_failure="reconciliation_evaluation_failed",
                reconciliation_status=ReconciliationStatus.ERROR,
            )

    def _build_report(
        self,
        tenant: str,
        generated_at: datetime,
        events: tuple[ExecutionAuditEvent, ...],
    ) -> ReconciliationReport:
        findings: list[ReconciliationFinding] = []
        checks = 0
        warnings = 0
        chains = 0

        def add(
            code: str,
            severity: FindingSeverity,
            detail: str,
            event: ExecutionAuditEvent | None = None,
        ) -> None:
            nonlocal warnings
            if severity is FindingSeverity.WARNING:
                warnings += 1
            if len(findings) >= _MAX_FINDINGS:
                return
            findings.append(
                ReconciliationFinding(
                    code=code,
                    severity=severity,
                    detail=detail,
                    event_id=_safe_identifier(event.event_id) if event else None,
                    operation=event.operation.value if event else None,
                    request_id=_safe_identifier(event.request_id) if event else None,
                )
            )

        if not events:
            return ReconciliationReport(
                report_version=AUDIT_CONTROL_REPORT_VERSION,
                generated_at=generated_at,
                tenant_id=tenant,
                events_scanned=0,
                checks_run=0,
                inconsistencies=0,
                warnings=0,
                correlation_chains=0,
                first_failure=None,
                reconciliation_status=ReconciliationStatus.INSUFFICIENT_DATA,
            )

        for event in events:
            # R1 — an archived, purged or restored execution must be identified.
            checks += 1
            if (
                event.operation in _ARCHIVE_OPERATIONS | _PURGE_OPERATIONS | _RESTORE_OPERATIONS
                and event.outcome in _IDENTITY_REQUIRED_OUTCOMES
                and (not event.execution_id or not event.tenant_id)
            ):
                add(
                    "IDENTITY_REQUIRED",
                    FindingSeverity.INCONSISTENCY,
                    "a completed archive, purge or restore event has no execution identity",
                    event,
                )

            # R2 — a success may not carry an explicitly negative reason.
            checks += 1
            if event.outcome is AuditOutcome.SUCCESS and _is_negative_reason(event.reason_code):
                add(
                    "SUCCESS_WITH_NEGATIVE_REASON",
                    FindingSeverity.INCONSISTENCY,
                    "a successful outcome carries a refusal or failure reason",
                    event,
                )

            # R3 — a purge success may not claim a refusal, a dry run or a conflict.
            checks += 1
            if (
                event.operation in _PURGE_OPERATIONS
                and event.outcome is AuditOutcome.SUCCESS
                and (
                    event.reason_code == "dry_run"
                    or event.reason_code == "archive_required"
                    or event.reason_code == "record_changed"
                )
            ):
                add(
                    "PURGE_SUCCESS_CONTRADICTS_REASON",
                    FindingSeverity.INCONSISTENCY,
                    "purge reports success while the reason states a refusal or a conflict",
                    event,
                )

            # R4 — a restore success must identify what was restored.
            checks += 1
            if (
                event.operation in _RESTORE_OPERATIONS
                and event.outcome is AuditOutcome.SUCCESS
                and not event.execution_id
            ):
                add(
                    "RESTORE_SUCCESS_WITHOUT_EXECUTION",
                    FindingSeverity.INCONSISTENCY,
                    "a successful restore event has no execution id",
                    event,
                )

            # R5 — W109 HTTP outcomes stay distinguishable.
            checks += 1
            if event.operation is AuditOperation.RECOVERY_API:
                if (
                    event.outcome is AuditOutcome.SUCCESS
                    and event.reason_code not in _RECOVERY_API_SUCCESS_REASONS
                ):
                    add(
                        "RECOVERY_API_SUCCESS_NOT_A_200",
                        FindingSeverity.INCONSISTENCY,
                        "recovery api success does not use a documented 200 reason",
                        event,
                    )
                if (
                    event.outcome is not AuditOutcome.SUCCESS
                    and event.reason_code in _RECOVERY_API_SUCCESS_REASONS
                ):
                    add(
                        "RECOVERY_API_FAILURE_USES_200_REASON",
                        FindingSeverity.INCONSISTENCY,
                        "a non successful recovery api event uses a 200 reason",
                        event,
                    )

            # R6 — the correlation id must always be present.
            checks += 1
            if not event.request_id:
                add(
                    "REQUEST_ID_MISSING",
                    FindingSeverity.INCONSISTENCY,
                    "an audit event has no correlation id",
                    event,
                )

        # R7 — a recovery-api event must be correlatable with its restore event.
        checks += 1
        by_request: dict[str, list[ExecutionAuditEvent]] = {}
        by_execution: dict[str, list[ExecutionAuditEvent]] = {}
        for event in events:
            by_request.setdefault(event.request_id, []).append(event)
            if event.execution_id:
                by_execution.setdefault(f"{event.tenant_id}\x1f{event.execution_id}", []).append(
                    event
                )
        for event in events:
            if event.operation is not AuditOperation.RECOVERY_API or not event.execution_id:
                continue
            restores_same_request = [
                candidate
                for candidate in by_request.get(event.request_id, ())
                if candidate.operation in _RESTORE_OPERATIONS
            ]
            if restores_same_request:
                chains += 1
                continue
            siblings = by_execution.get(f"{event.tenant_id}\x1f{event.execution_id}", [])
            conflicting = [
                candidate
                for candidate in siblings
                if candidate.operation in _RESTORE_OPERATIONS
                and candidate.request_id != event.request_id
            ]
            checks += 1
            if conflicting:
                add(
                    "CORRELATION_ID_NOT_PRESERVED",
                    FindingSeverity.INCONSISTENCY,
                    "a restore exists for the same execution under a different request id",
                    event,
                )
            else:
                add(
                    "CORRELATION_NOT_OBSERVABLE",
                    FindingSeverity.WARNING,
                    "no restore event is observable for this recovery api event",
                    event,
                )

        # R8 — in a correlation chain ordered by its contractual rank
        # (RECOVERY_API -> RESTORE -> ARCHIVE), occurred_at must be non-decreasing.
        # A violation is a clock-skew warning, never proof of tampering.
        for request_id, group in by_request.items():
            ranked = [
                (rank, item)
                for rank, item in ((_chain_rank(item.operation), item) for item in group)
                if rank is not None
            ]
            if len(ranked) < 2:
                continue
            checks += 1
            ordered = [item for _, item in sorted(ranked, key=lambda entry: entry[0])]
            timestamps = [item.occurred_at for item in ordered]
            if any(
                timestamps[index] > timestamps[index + 1] for index in range(len(timestamps) - 1)
            ):
                add(
                    "CHAIN_CLOCK_SKEW",
                    FindingSeverity.WARNING,
                    f"occurred_at is not monotonic in request {request_id}",
                    None,
                )

        # R9 — one idempotency key may not describe two different events.
        checks += 1
        duplicates = _count_duplicates(list(events))
        if duplicates:
            add(
                "DUPLICATE_LOGICAL_EVENT",
                FindingSeverity.INCONSISTENCY,
                "an idempotency key or event id is shared by incompatible events",
                None,
            )

        inconsistencies = sum(
            1 for finding in findings if finding.severity is FindingSeverity.INCONSISTENCY
        )
        if inconsistencies:
            status = ReconciliationStatus.INCONSISTENT
        elif warnings or chains == 0:
            status = ReconciliationStatus.INSUFFICIENT_DATA
        else:
            status = ReconciliationStatus.CONSISTENT
        return ReconciliationReport(
            report_version=AUDIT_CONTROL_REPORT_VERSION,
            generated_at=generated_at,
            tenant_id=tenant,
            events_scanned=len(events),
            checks_run=checks,
            inconsistencies=inconsistencies,
            warnings=warnings,
            correlation_chains=chains,
            first_failure=findings[0].code if findings else None,
            reconciliation_status=status,
            findings=tuple(findings),
        )


class ExecutionAuditExportService(ExecutionAuditControlService):
    """Bounded, deterministic, tenant-isolated export of the audit trail."""

    def export(
        self,
        tenant_id: str,
        *,
        export_format: ExportFormat | str = ExportFormat.JSON,
        query: ExecutionAuditQuery | None = None,
        limit: int | None = None,
        offset: int = 0,
        generated_at: datetime | None = None,
    ) -> ExecutionAuditExport:
        tenant = _require_tenant(tenant_id)
        resolved_format = _coerce_format(export_format)
        active_limit = (
            query.limit
            if query is not None and limit is None
            else (limit if limit is not None else DEFAULT_AUDIT_EXPORT_LIMIT)
        )
        active_offset = query.offset if query is not None else offset
        _validate_bounded(limit=active_limit, offset=active_offset)
        if query is not None:
            tenant_scoped_query = ExecutionAuditQuery(
                tenant_id=tenant,
                operation=query.operation,
                outcome=query.outcome,
                execution_id=query.execution_id,
                reference_key=query.reference_key,
                request_id=query.request_id,
                actor_type=query.actor_type,
                source=query.source,
                reason_code=query.reason_code,
                occurred_at_from=query.occurred_at_from,
                occurred_at_to=query.occurred_at_to,
                limit=MAX_AUDIT_QUERY_LIMIT,
                offset=0,
            )
        else:
            tenant_scoped_query = ExecutionAuditQuery(tenant_id=tenant)
        events = self.events_for(tenant_scoped_query)
        # the match set is complete, so `total` is the true filtered count and
        # only the returned page is bounded
        total = len(events)
        selected = events[active_offset : active_offset + active_limit]
        return ExecutionAuditExport(
            export_version=AUDIT_EXPORT_VERSION,
            export_format=resolved_format,
            tenant_id=tenant,
            events=selected,
            total=total,
            limit=active_limit,
            offset=active_offset,
            generated_at=_utc(generated_at),
        )

    def checksum(
        self,
        tenant_id: str,
        *,
        export_format: ExportFormat | str = ExportFormat.JSON,
        query: ExecutionAuditQuery | None = None,
        limit: int | None = None,
        offset: int = 0,
        generated_at: datetime | None = None,
    ) -> dict[str, Any]:
        """Return only the reproducible checksum and its deterministic inputs."""

        export = self.export(
            tenant_id,
            export_format=export_format,
            query=query,
            limit=limit,
            offset=offset,
            generated_at=generated_at,
        )
        return {
            "export_version": export.export_version,
            "format": export.export_format.value,
            "tenant_id": export.tenant_id,
            "event_count": export.count,
            "total": export.total,
            "limit": export.limit,
            "offset": export.offset,
            "export_checksum": export.export_checksum,
        }


def _coerce_format(value: ExportFormat | str) -> ExportFormat:
    if isinstance(value, ExportFormat):
        return value
    try:
        return ExportFormat(str(value).upper())
    except (TypeError, ValueError) as exc:
        raise AuditValidationError("export format is unsupported") from exc


def iter_chains(events: Iterable[ExecutionAuditEvent]) -> Iterator[tuple[str, ...]]:
    """Reconstruct the observable ``RECOVERY_API -> RESTORE -> ARCHIVE`` chains.

    A chain is returned only when every link is actually present in the events;
    an incomplete chain yields nothing rather than a partial conclusion.
    """

    by_request: dict[str, set[AuditOperation]] = {}
    for event in events:
        by_request.setdefault(event.request_id, set()).add(event.operation)
    for request_id in sorted(by_request):
        operations = by_request[request_id]
        if AuditOperation.RECOVERY_API not in operations:
            continue
        if not any(operation in _RESTORE_OPERATIONS for operation in operations):
            continue
        if not any(operation in _ARCHIVE_OPERATIONS for operation in operations):
            continue
        yield (request_id,)


__all__ = [
    "AUDIT_CONTROL_REPORT_VERSION",
    "AUDIT_CONTROL_SOURCE",
    "AUDIT_EXPORT_VERSION",
    "DEFAULT_AUDIT_EXPORT_LIMIT",
    "MAX_AUDIT_EXPORT_LIMIT",
    "MAX_AUDIT_EXPORT_OFFSET",
    "AuditControlError",
    "ExecutionAuditControlService",
    "ExecutionAuditExport",
    "ExecutionAuditExportService",
    "ExecutionAuditIntegrityService",
    "ExecutionAuditReconciliationService",
    "ExportFormat",
    "FindingSeverity",
    "IntegrityReport",
    "IntegrityStatus",
    "ReconciliationFinding",
    "ReconciliationReport",
    "ReconciliationStatus",
    "assert_export_safe",
    "iter_chains",
]
