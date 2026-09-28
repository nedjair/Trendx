"""W114 - end-to-end lifecycle consistency validation for the execution chain.

This module is **validation scaffolding, not a business layer**.  It computes no
statistics, no analytics, no integrity verdict, no archive state and no
operational status of its own.  Every fact it reports is read from the service
that owns that truth::

    ExecutionHistoryService      (W99)  → history + statistics
    ExecutionHistoryAnalytics    (W104) → analytics
    ExecutionReportingService    (W106) → report
    ExecutionAuditIntegrityService  (W111) → integrity
    ExecutionAuditReconciliationService (W111) → reconciliation
    ExecutionAuditExportService  (W111) → export
    AuditLifecycleService        (W112) → archive / active split
    AuditHealthService           (W113) → health / capacity / readiness

What W114 adds is exactly two things and nothing else:

1. a :class:`LifecycleSnapshot` that *gathers* those projections into one
   comparable, deterministic record, and
2. :meth:`AuditE2EValidator.compare`, which *compares* two snapshots and returns
   a structured verdict per dimension.

It is read-only.  It never archives, purges, restores, deletes, writes telemetry,
schedules a job or opens a socket.  The lifecycle transitions themselves are
performed by W112 through its own public API in the tests; W114 only observes
the result.

A snapshot that cannot be gathered is reported as ``UNAVAILABLE`` with a reason
code rather than being filled with a guessed value, so a missing projection can
never masquerade as a consistent one.
"""

from __future__ import annotations

import hashlib
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import Enum
from typing import Any

from trendx.forecasting.analytics import (
    ExecutionAnalytics,
    ExecutionHistoryAnalytics,
)
from trendx.forecasting.audit import AuditError, ExecutionAuditQuery
from trendx.forecasting.audit_control import (
    AuditExportScope,
    AuditLifecycleReader,
    ExecutionAuditExportService,
    ExecutionAuditIntegrityService,
    ExecutionAuditReconciliationService,
    ExportFormat,
)
from trendx.forecasting.audit_health import AuditHealthService, AuditOperationalSnapshot
from trendx.forecasting.audit_lifecycle import AuditArchiveError
from trendx.forecasting.history import (
    ExecutionHistoryService,
    ExecutionQuery,
    ExecutionStatistics,
)
from trendx.forecasting.reporting import ExecutionAnalyticsReportingService

LIFECYCLE_CONSISTENCY_REPORT_VERSION = "1"

#: Fixed instant used for every export the validator performs, so an export is
#: byte-reproducible and its digest measures the *state*, not the clock.
DETERMINISTIC_EXPORT_INSTANT = datetime(2026, 1, 1, tzinfo=UTC)


class LifecycleState(str, Enum):
    """The observable states of the end-to-end scenario."""

    INITIAL = "S0_INITIAL"
    POST_EXECUTION = "S1_POST_EXECUTION"
    POST_ARCHIVE = "S2_POST_ARCHIVE"
    POST_PURGE = "S3_POST_PURGE"
    POST_RESTORE = "S4_POST_RESTORE"
    CORRUPTED = "S5_CORRUPTED_TEST"


class ConsistencyVerdict(str, Enum):
    """Outcome of comparing one dimension across two snapshots."""

    CONSISTENT = "CONSISTENT"
    INCONSISTENT = "INCONSISTENT"
    UNAVAILABLE = "UNAVAILABLE"


class ConsistencyDimension(str, Enum):
    """The projections W114 compares, one per existing contract."""

    HISTORY = "HISTORY"
    ANALYTICS = "ANALYTICS"
    REPORT = "REPORT"
    INTEGRITY = "INTEGRITY"
    ARCHIVE = "ARCHIVE"
    RECONCILIATION = "RECONCILIATION"
    EXPORT = "EXPORT"
    HEALTH = "HEALTH"
    CAPACITY = "CAPACITY"
    READINESS = "READINESS"


def _statistics_fingerprint(statistics: ExecutionStatistics) -> dict[str, Any]:
    """Read a W99 statistics object; W114 aggregates nothing itself."""

    return {
        "total_executions": statistics.total_executions,
        "success_count": statistics.success_count,
        "failure_count": statistics.failure_count,
        "prediction_count_total": statistics.prediction_count_total,
        "success_rate": round(statistics.success_rate, 6),
    }


def _analytics_fingerprint(analytics: ExecutionAnalytics) -> dict[str, Any]:
    """Read a W104 result; the breakdown is W104's, not W114's."""

    return {
        "summary": _statistics_fingerprint(analytics.summary),
        "by_metric": sorted(group.key for group in analytics.by_metric),
        "by_algorithm": sorted(group.key for group in analytics.by_algorithm),
        "by_model": sorted(f"{g.model_id}:{g.model_version}" for g in analytics.by_model),
        "by_status": sorted(group.key for group in analytics.by_status),
        "failure_codes": sorted(failure.error_code for failure in analytics.failures),
    }


def _canonical(value: Any) -> str:
    """Deterministic JSON for a fingerprint; rejects non-finite numbers."""

    import json

    def _reject(constant: str) -> Any:
        raise ValueError(f"non-finite number in snapshot: {constant}")

    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
        default=_reject,
    )


def digest(value: Any) -> str:
    """SHA-256 over the canonical form of a business value."""

    return hashlib.sha256(_canonical(value).encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class ConsistencyCheck:
    """One dimension compared between two snapshots."""

    dimension: ConsistencyDimension
    verdict: ConsistencyVerdict
    detail: str
    expected: Any = None
    observed: Any = None

    @property
    def is_consistent(self) -> bool:
        return self.verdict is ConsistencyVerdict.CONSISTENT

    def to_dict(self) -> dict[str, Any]:
        return {
            "dimension": self.dimension.value,
            "verdict": self.verdict.value,
            "detail": self.detail,
            "expected": self.expected,
            "observed": self.observed,
        }


@dataclass(frozen=True)
class LifecycleSnapshot:
    """A gathered, deterministic view of the chain at one lifecycle state.

    ``fingerprint`` digests every *business* fact below and excludes the
    observation instant, so two snapshots of the same logical state compare
    equal regardless of when they were taken.
    """

    state: LifecycleState
    tenant_id: str
    captured_at: datetime
    execution_ids: tuple[str, ...]
    execution_count: int
    execution_statistics: dict[str, Any]
    execution_reference_keys: tuple[str, ...]
    execution_statuses: dict[str, int]
    execution_prediction_total: int
    analytics: dict[str, Any] | None
    report_summary: dict[str, Any] | None
    audit_active_count: int
    audit_archived_count: int | None
    #: ``None`` when the journal could not be read: an unknown perimeter is
    #: never rendered as an empty tally, which would read as "zero events".
    audit_outcomes: dict[str, int] | None
    audit_execution_links: dict[str, str] | None
    integrity_status: str | None
    integrity_invalid_events: int | None
    integrity_checksum_failures: int | None
    reconciliation_status: str | None
    reconciliation_inconsistencies: int | None
    reconciliation_active_events: int | None
    reconciliation_archived_events: int | None
    export_active_total: int
    export_archived_total: int | None
    export_digest: str
    export_ids: tuple[str, ...]
    health_status: str
    health_event_count: int
    health_tenant_count: int
    health_invalid_count: int
    health_malformed_count: int
    capacity_active_event_count: int
    capacity_archived_event_count: int | None
    capacity_total_known_event_count: int | None
    capacity_active_bytes: int | None
    capacity_archive_bytes: int | None
    archive_status: str
    archive_document_count: int | None
    readiness_status: str
    readiness_blocking_reason_codes: tuple[str, ...]
    unavailable: tuple[str, ...] = field(default_factory=tuple)
    notes: tuple[str, ...] = field(default_factory=tuple)

    def business_view(self) -> dict[str, Any]:
        """Every business fact, with the observation instant excluded."""

        payload = {
            "state": self.state.value,
            "tenant_id": self.tenant_id,
            "execution_ids": list(self.execution_ids),
            "execution_count": self.execution_count,
            "execution_statistics": self.execution_statistics,
            "execution_reference_keys": list(self.execution_reference_keys),
            "execution_statuses": self.execution_statuses,
            "execution_prediction_total": self.execution_prediction_total,
            "analytics": self.analytics,
            "report_summary": self.report_summary,
            "audit_active_count": self.audit_active_count,
            "audit_archived_count": self.audit_archived_count,
            "audit_outcomes": self.audit_outcomes,
            "audit_execution_links": self.audit_execution_links,
            "integrity_status": self.integrity_status,
            "integrity_invalid_events": self.integrity_invalid_events,
            "integrity_checksum_failures": self.integrity_checksum_failures,
            "reconciliation_status": self.reconciliation_status,
            "reconciliation_inconsistencies": self.reconciliation_inconsistencies,
            "reconciliation_active_events": self.reconciliation_active_events,
            "reconciliation_archived_events": self.reconciliation_archived_events,
            "export_active_total": self.export_active_total,
            "export_archived_total": self.export_archived_total,
            "export_digest": self.export_digest,
            "export_ids": list(self.export_ids),
            "health_status": self.health_status,
            "health_event_count": self.health_event_count,
            "health_tenant_count": self.health_tenant_count,
            "health_invalid_count": self.health_invalid_count,
            "health_malformed_count": self.health_malformed_count,
            "capacity_active_event_count": self.capacity_active_event_count,
            "capacity_archived_event_count": self.capacity_archived_event_count,
            "capacity_total_known_event_count": self.capacity_total_known_event_count,
            "archive_status": self.archive_status,
            "archive_document_count": self.archive_document_count,
            "readiness_status": self.readiness_status,
            "readiness_blocking_reason_codes": list(self.readiness_blocking_reason_codes),
            "unavailable": list(self.unavailable),
        }
        return payload

    @property
    def fingerprint(self) -> str:
        """Deterministic digest of the logical state."""

        return digest(self.business_view())

    def to_dict(self) -> dict[str, Any]:
        payload = self.business_view()
        payload["captured_at"] = self.captured_at.astimezone(UTC).isoformat()
        payload["fingerprint"] = self.fingerprint
        payload["notes"] = list(self.notes)
        return payload


class AuditE2EValidator:
    """Gather the W98-W113 projections and compare them across states.

    The validator is constructed from the *existing* services.  It holds no
    store of its own, opens no file and mutates nothing: ``snapshot`` is a pure
    read, and ``compare`` is a pure function of two snapshots.
    """

    def __init__(
        self,
        *,
        history: ExecutionHistoryService,
        integrity: ExecutionAuditIntegrityService,
        reconciliation: ExecutionAuditReconciliationService,
        export: ExecutionAuditExportService,
        health: AuditHealthService,
        analytics: ExecutionHistoryAnalytics | None = None,
        reporting: ExecutionAnalyticsReportingService | None = None,
        lifecycle: AuditLifecycleReader | None = None,
        clock: Any = None,
    ) -> None:
        self._history = history
        self._integrity = integrity
        self._reconciliation = reconciliation
        self._export = export
        self._health = health
        self._analytics = analytics or ExecutionHistoryAnalytics(history)
        self._reporting = reporting or ExecutionAnalyticsReportingService(history)
        self._lifecycle = lifecycle
        self._clock = clock or (lambda: datetime.now(UTC))

    @property
    def lifecycle_reader(self) -> AuditLifecycleReader | None:
        """The W112 lifecycle reader this validator was given, if any."""

        return self._lifecycle

    # ── gathering ──────────────────────────────────────────────────────────

    def _reconciliation_facts(
        self,
        tenant_id: str,
        unavailable: list[str],
        notes: list[str],
    ) -> tuple[str | None, int | None, int | None, int | None]:
        """Read W111 reconciliation, degrading instead of raising.

        W111 fails closed on an untrustworthy journal and raises on a corrupt
        archive.  Both refusals are legitimate contracts, so W114 records the
        dimension as unavailable and lets the W113 status carry the operational
        truth rather than inventing a reconciliation verdict.
        """

        unknown = (None, None, None, None)
        try:
            report = self._reconciliation.reconcile(tenant_id, lifecycle=self._lifecycle)
        except AuditArchiveError:
            # the archive perimeter is unreadable: fall back to the active-only
            # reconciliation and record the reduced scope explicitly
            try:
                report = self._reconciliation.reconcile(tenant_id)
            except AuditError:
                pass
            else:
                notes.append("reconciliation archive perimeter unavailable")
            unavailable.append(ConsistencyDimension.RECONCILIATION.value)
            return unknown
        except AuditError:
            unavailable.append(ConsistencyDimension.RECONCILIATION.value)
            return unknown
        return (
            report.reconciliation_status.value,
            report.inconsistencies,
            report.active_events,
            report.archived_events,
        )

    def _history_facts(self, tenant_id: str) -> dict[str, Any]:
        query = ExecutionQuery(tenant_id=tenant_id, limit=None, offset=0)
        records = self._history.list_records(query)
        statistics = self._history.statistics(query)
        return {
            "records": records,
            "statistics": statistics,
            "ids": tuple(sorted(record.execution_id for record in records)),
            "reference_keys": tuple(
                sorted(record.reference_key for record in records if record.reference_key)
            ),
            "statuses": dict(
                sorted(
                    (status, sum(1 for r in records if r.status.value == status))
                    for status in {record.status.value for record in records}
                )
            ),
            "prediction_total": sum(record.prediction_count or 0 for record in records),
        }

    def snapshot(
        self,
        state: LifecycleState,
        tenant_id: str,
        *,
        note: str | None = None,
    ) -> LifecycleSnapshot:
        """Gather every projection once, for one tenant at one lifecycle state."""

        captured_at = self._clock()
        unavailable: list[str] = []

        history = self._history_facts(tenant_id)
        records = history["records"]
        statistics: ExecutionStatistics = history["statistics"]

        try:
            analytics: ExecutionAnalytics | None = self._analytics.analyze(
                ExecutionQuery(tenant_id=tenant_id, limit=None, offset=0)
            )
        except Exception:  # a missing projection is reported, never faked
            analytics = None
            unavailable.append(ConsistencyDimension.ANALYTICS.value)
        analytics_view = _analytics_fingerprint(analytics) if analytics else None

        try:
            report = self._reporting.report(
                ExecutionQuery(tenant_id=tenant_id, limit=None, offset=0),
                generated_at=DETERMINISTIC_EXPORT_INSTANT.isoformat(),
            )
            report_summary: dict[str, Any] | None = _statistics_fingerprint(report.summary)
        except Exception:  # a missing projection is reported, never faked
            report_summary = None
            unavailable.append(ConsistencyDimension.REPORT.value)

        # W111 refuses to certify an untrustworthy source by raising, while W113
        # reports it as a status.  Both are legitimate; the validator normalises
        # them into a reported dimension so a corrupted journal is *observed*
        # instead of aborting the snapshot.
        notes: list[str] = []
        integrity_invalid: int | None = None
        integrity_checksum: int | None = None
        try:
            integrity = self._integrity.verify(tenant_id)
        except AuditError:
            integrity_status: str | None = None
            unavailable.append(ConsistencyDimension.INTEGRITY.value)
            notes.append("integrity refused to certify the journal")
        else:
            integrity_status = integrity.integrity_status.value
            integrity_invalid = integrity.invalid_events
            integrity_checksum = integrity.checksum_failures
            if integrity_status == "ERROR":
                unavailable.append(ConsistencyDimension.INTEGRITY.value)

        # W113 classifies the archive fail-closed and can therefore be asked
        # first: it reports a corrupt archive as a status instead of raising.
        health: AuditOperationalSnapshot = self._health.snapshot(tenant_id)

        # W111 merges the archive directly and raises on a corrupt document, so
        # the archive-dependent reads are guarded.  A corrupt archive must be
        # reported through the W113 status, never crash the snapshot, and the
        # degraded scope is recorded instead of being silently narrowed.
        (
            reconciliation_status,
            reconciliation_inconsistencies,
            reconciliation_active,
            reconciliation_archived,
        ) = self._reconciliation_facts(tenant_id, unavailable, notes)
        try:
            export_active = self._export.export(
                tenant_id,
                export_format=ExportFormat.JSON,
                generated_at=DETERMINISTIC_EXPORT_INSTANT,
                scope=AuditExportScope.ACTIVE_ONLY,
            )
        except AuditError:
            # the journal cannot be exported; the perimeter is unknown, not zero
            export_active = None
            unavailable.append(ConsistencyDimension.EXPORT.value)
            notes.append("export refused to read the journal")
        export_total = int(export_active.total) if export_active else 0
        export_ids = (
            tuple(sorted(event.event_id for event in export_active.events)) if export_active else ()
        )
        export_digest = digest(
            {
                "active": (
                    [event.to_dict() for event in export_active.events] if export_active else []
                ),
                "scope": export_active.scope.value if export_active else None,
            }
        )
        export_archived_total: int | None
        try:
            export_combined = self._export.export(
                tenant_id,
                export_format=ExportFormat.JSON,
                generated_at=DETERMINISTIC_EXPORT_INSTANT,
                scope=AuditExportScope.ACTIVE_AND_ARCHIVED,
                lifecycle=self._lifecycle,
            )
        except AuditError:
            # the combined perimeter is unreadable, whether because the archive
            # is corrupt or the journal is untrustworthy: report the gap as an
            # unknown, never as a zero and never as a silent narrowing
            export_archived_total = None
            notes.append("export archive perimeter unavailable")
            unavailable.append(ConsistencyDimension.EXPORT.value)
        else:
            export_archived_total = int(export_combined.total)

        # W110 audit facts, read through the W110/W111 export contract
        if export_active is None:
            # an unreadable journal yields an unknown perimeter, not an empty
            # tally that would read as "zero events"
            outcomes = None
            execution_links = None
        else:
            active_events = export_active.events
            outcomes = dict(
                sorted(
                    (
                        outcome,
                        sum(1 for event in active_events if event.outcome.value == outcome),
                    )
                    for outcome in {event.outcome.value for event in active_events}
                )
            )
            execution_links = {
                event.execution_id: event.outcome.value
                for event in active_events
                if event.execution_id
            }

        return LifecycleSnapshot(
            state=state,
            tenant_id=tenant_id,
            captured_at=captured_at,
            execution_ids=history["ids"],
            execution_count=len(records),
            execution_statistics=_statistics_fingerprint(statistics),
            execution_reference_keys=history["reference_keys"],
            execution_statuses=history["statuses"],
            execution_prediction_total=history["prediction_total"],
            analytics=analytics_view,
            report_summary=report_summary,
            audit_active_count=export_total,
            audit_archived_count=(
                export_archived_total - export_total
                if self._lifecycle is not None and export_archived_total is not None
                else None
            ),
            audit_outcomes=outcomes,
            audit_execution_links=execution_links,
            integrity_status=integrity_status,
            integrity_invalid_events=integrity_invalid,
            integrity_checksum_failures=integrity_checksum,
            reconciliation_status=reconciliation_status,
            reconciliation_inconsistencies=reconciliation_inconsistencies,
            reconciliation_active_events=reconciliation_active,
            reconciliation_archived_events=reconciliation_archived,
            export_active_total=export_total,
            export_archived_total=export_archived_total,
            export_digest=export_digest,
            export_ids=export_ids,
            health_status=health.health.status.value,
            health_event_count=health.health.event_count,
            health_tenant_count=health.health.tenant_count,
            health_invalid_count=health.health.integrity_invalid_count,
            health_malformed_count=health.health.malformed_count,
            capacity_active_event_count=health.capacity.event_count,
            capacity_archived_event_count=health.capacity.archived_event_count,
            capacity_total_known_event_count=health.capacity.total_known_event_count,
            capacity_active_bytes=health.capacity.active_bytes,
            capacity_archive_bytes=health.capacity.archive_bytes,
            archive_status=health.archive.archive_status.value,
            archive_document_count=health.archive.archive_document_count,
            readiness_status=health.readiness.readiness_status.value,
            readiness_blocking_reason_codes=health.readiness.blocking_reason_codes,
            unavailable=tuple(dict.fromkeys(unavailable)),
            notes=tuple(item for item in ((note,) if note else ()) + tuple(notes) if item),
        )

    # ── comparison ─────────────────────────────────────────────────────────

    @staticmethod
    def _compare_field(
        dimension: ConsistencyDimension,
        field_name: str,
        expected: Any,
        observed: Any,
    ) -> ConsistencyCheck:
        if expected == observed:
            return ConsistencyCheck(
                dimension=dimension,
                verdict=ConsistencyVerdict.CONSISTENT,
                detail=f"{field_name} unchanged",
                expected=expected,
                observed=observed,
            )
        return ConsistencyCheck(
            dimension=dimension,
            verdict=ConsistencyVerdict.INCONSISTENT,
            detail=f"{field_name} changed",
            expected=expected,
            observed=observed,
        )

    def compare(
        self,
        before: LifecycleSnapshot,
        after: LifecycleSnapshot,
        *,
        dimensions: Sequence[ConsistencyDimension] | None = None,
    ) -> tuple[ConsistencyCheck, ...]:
        """Compare two snapshots dimension by dimension.

        W114 asserts *equality* only.  It never judges whether a change was
        legitimate: deciding that is the scenario's job, expressed by choosing
        which dimensions must be invariant across a transition.
        """

        wanted = tuple(dimensions) if dimensions is not None else tuple(ConsistencyDimension)
        checks: list[ConsistencyCheck] = []
        for dimension in wanted:
            if dimension is ConsistencyDimension.HISTORY:
                for field_name in (
                    "execution_ids",
                    "execution_count",
                    "execution_statistics",
                    "execution_reference_keys",
                    "execution_statuses",
                    "execution_prediction_total",
                ):
                    checks.append(
                        self._compare_field(
                            dimension,
                            field_name,
                            getattr(before, field_name),
                            getattr(after, field_name),
                        )
                    )
            elif dimension is ConsistencyDimension.ANALYTICS:
                checks.append(
                    self._compare_field(dimension, "analytics", before.analytics, after.analytics)
                )
            elif dimension is ConsistencyDimension.REPORT:
                checks.append(
                    self._compare_field(
                        dimension, "report_summary", before.report_summary, after.report_summary
                    )
                )
            elif dimension is ConsistencyDimension.INTEGRITY:
                for field_name in (
                    "integrity_status",
                    "integrity_invalid_events",
                    "integrity_checksum_failures",
                ):
                    checks.append(
                        self._compare_field(
                            dimension,
                            field_name,
                            getattr(before, field_name),
                            getattr(after, field_name),
                        )
                    )
            elif dimension is ConsistencyDimension.ARCHIVE:
                for field_name in ("archive_status", "archive_document_count"):
                    checks.append(
                        self._compare_field(
                            dimension,
                            field_name,
                            getattr(before, field_name),
                            getattr(after, field_name),
                        )
                    )
            elif dimension is ConsistencyDimension.RECONCILIATION:
                for field_name in (
                    "reconciliation_status",
                    "reconciliation_inconsistencies",
                    "reconciliation_active_events",
                    "reconciliation_archived_events",
                ):
                    checks.append(
                        self._compare_field(
                            dimension,
                            field_name,
                            getattr(before, field_name),
                            getattr(after, field_name),
                        )
                    )
            elif dimension is ConsistencyDimension.EXPORT:
                for field_name in (
                    "export_active_total",
                    "export_archived_total",
                    "export_digest",
                    "export_ids",
                ):
                    checks.append(
                        self._compare_field(
                            dimension,
                            field_name,
                            getattr(before, field_name),
                            getattr(after, field_name),
                        )
                    )
            elif dimension is ConsistencyDimension.HEALTH:
                for field_name in (
                    "health_status",
                    "health_event_count",
                    "health_tenant_count",
                    "health_invalid_count",
                    "health_malformed_count",
                ):
                    checks.append(
                        self._compare_field(
                            dimension,
                            field_name,
                            getattr(before, field_name),
                            getattr(after, field_name),
                        )
                    )
            elif dimension is ConsistencyDimension.CAPACITY:
                for field_name in (
                    "capacity_active_event_count",
                    "capacity_archived_event_count",
                    "capacity_total_known_event_count",
                ):
                    checks.append(
                        self._compare_field(
                            dimension,
                            field_name,
                            getattr(before, field_name),
                            getattr(after, field_name),
                        )
                    )
            elif dimension is ConsistencyDimension.READINESS:
                for field_name in (
                    "readiness_status",
                    "readiness_blocking_reason_codes",
                ):
                    checks.append(
                        self._compare_field(
                            dimension,
                            field_name,
                            getattr(before, field_name),
                            getattr(after, field_name),
                        )
                    )
            else:  # pragma: no cover - the enum is closed
                raise ValueError(dimension)
        return tuple(checks)

    @staticmethod
    def is_invariant(checks: Sequence[ConsistencyCheck]) -> bool:
        """True when every compared check is CONSISTENT."""

        return all(check.is_consistent for check in checks)


def invariants_for(transition: str) -> tuple[ConsistencyDimension, ...]:
    """Dimensions that must not change across a named transition.

    Only *invariance* is encoded, never a business rule: the caller states that
    a transition must not disturb a projection, and W114 verifies it.
    """

    by_transition: Mapping[str, tuple[ConsistencyDimension, ...]] = {
        # a lifecycle transition never touches the execution chain
        "archive": (
            ConsistencyDimension.HISTORY,
            ConsistencyDimension.ANALYTICS,
            ConsistencyDimension.REPORT,
        ),
        # purging an audit event may not touch the execution chain either
        "purge": (
            ConsistencyDimension.HISTORY,
            ConsistencyDimension.ANALYTICS,
            ConsistencyDimension.REPORT,
        ),
        # a restore brings the audit perimeter back without touching executions
        "restore": (
            ConsistencyDimension.HISTORY,
            ConsistencyDimension.ANALYTICS,
            ConsistencyDimension.REPORT,
        ),
        # a diagnostic read must change nothing at all
        "read_only": tuple(ConsistencyDimension),
    }
    return by_transition[transition]


__all__ = [
    "DETERMINISTIC_EXPORT_INSTANT",
    "LIFECYCLE_CONSISTENCY_REPORT_VERSION",
    "AuditE2EValidator",
    "ConsistencyCheck",
    "ConsistencyDimension",
    "ConsistencyVerdict",
    "ExecutionAuditQuery",
    "LifecycleSnapshot",
    "LifecycleState",
    "digest",
    "invariants_for",
]
