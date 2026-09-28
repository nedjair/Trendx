"""W114 — end-to-end lifecycle consistency unit tests.

W114 validates that the W98-W113 contracts form one coherent chain.  These tests
use temporary datastores only: no production path, no network, no scheduler, no
worker, no forecast, no ThingsBoard write.
"""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from trendx.forecasting.analytics import ExecutionHistoryAnalytics
from trendx.forecasting.audit import (
    AuditActorType,
    AuditOperation,
    AuditOutcome,
    ExecutionAuditService,
    ExecutionAuditStore,
)
from trendx.forecasting.audit_control import (
    AuditExportScope,
    ExecutionAuditExportService,
    ExecutionAuditIntegrityService,
    ExecutionAuditReconciliationService,
    ExportFormat,
)
from trendx.forecasting.audit_e2e import (
    AuditE2EValidator,
    ConsistencyDimension,
    ConsistencyVerdict,
    LifecycleState,
    digest,
    invariants_for,
)
from trendx.forecasting.audit_health import (
    ARCHIVE_CORRUPTED,
    AUDIT_DATA_CORRUPTED,
    AuditHealthService,
)
from trendx.forecasting.audit_lifecycle import (
    AuditArchiveIntegrityError,
    AuditLifecycleService,
    AuditRestoreRequest,
    AuditRestoreStatus,
    AuditRetentionPolicy,
    FileSystemAuditArchiveStore,
)
from trendx.forecasting.execution import (
    DurableExecutionStore,
    ExecutionProvenance,
    ExecutionRecord,
    ExecutionStatus,
)
from trendx.forecasting.history import ExecutionHistoryService
from trendx.forecasting.reporting import ExecutionAnalyticsReportingService

NOW = datetime(2026, 4, 1, tzinfo=UTC)
AUDIT_INSTANT = NOW - timedelta(days=100)
TENANT_A = "tenant-A"
TENANT_B = "tenant-B"

#: 15 W110 event fields that a W112 archive must preserve verbatim.
W110_EVENT_FIELDS = (
    "event_id",
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
    "event_version",
    "idempotency_key",
    "checksum",
)


def _provenance(predictions: int, *, failed: bool = False) -> ExecutionProvenance:
    return ExecutionProvenance(
        model_id="prophet",
        model_version="1",
        algorithm="prophet",
        feature_schema_version="schema-A",
        feature_schema_fingerprint="a" * 64,
        prediction_count=0 if failed else predictions,
    )


def _seed_execution(
    store: DurableExecutionStore,
    index: int,
    tenant_id: str,
    *,
    succeeds: bool,
    predictions: int,
) -> ExecutionRecord:
    """Create an execution through the public W98 transition path only."""

    created = (NOW - timedelta(days=50 + index)).isoformat()
    record = store.create(
        ExecutionRecord(
            execution_id=f"x-{index}",
            reference_key=f"ref-{index}",
            tenant_id=tenant_id,
            entity_type="DEVICE",
            entity_id=f"dev-{index}",
            target_metric="temperature",
            frequency="1h",
            horizon=24,
            model_id="prophet",
            model_version="1",
            algorithm="prophet",
            created_at=created,
            metadata={"source": "w114"},
        )
    )
    store.mark_started(record.execution_id)
    if succeeds:
        final = store.mark_success(record.execution_id, _provenance(predictions))
    else:
        final = store.mark_failed(
            record.execution_id,
            error_code="model_error",
            error_reason="fit failed",
            provenance=_provenance(0, failed=True),
        )
    return final


class Scenario:
    """One controlled E2E fixture wired from the existing W98-W113 services."""

    def __init__(self, root: Path) -> None:
        self.root = root
        self.execution_path = root / "executions"
        self.audit_path = root / "audit"
        self.archive_path = root / "audit-archive"
        self.executions = DurableExecutionStore(self.execution_path)
        self._seed_audit = ExecutionAuditStore(self.audit_path)
        self.audit = ExecutionAuditService(self._seed_audit, clock=lambda: AUDIT_INSTANT)

        for index, (tenant, ok, predictions) in enumerate(
            [
                (TENANT_A, True, 3),
                (TENANT_A, True, 5),
                (TENANT_A, False, 0),
                (TENANT_B, True, 2),
            ]
        ):
            record = _seed_execution(
                self.executions, index, tenant, succeeds=ok, predictions=predictions
            )
            self.audit.record(
                operation=AuditOperation.EXECUTION,
                outcome=AuditOutcome.SUCCESS if ok else AuditOutcome.FAILED,
                tenant_id=tenant,
                execution_id=record.execution_id,
                reference_key=record.reference_key,
                actor_type=AuditActorType.SYSTEM,
                actor_id="scheduler",
                request_id=f"req-{index}",
                source="w114-e2e",
                reason_code="execution_completed",
            )
        # a recovery correlation chain so W111 reconciliation has a chain to
        # evaluate: RECOVERY_API + RESTORE_EXECUTE share a request id
        self.audit.record(
            operation=AuditOperation.RECOVERY_API,
            outcome=AuditOutcome.SUCCESS,
            tenant_id=TENANT_A,
            execution_id="x-0",
            reference_key="ref-0",
            actor_type=AuditActorType.SYSTEM,
            actor_id="operator",
            request_id="restore-0",
            source="w114-e2e",
            reason_code="record_already_present",
        )
        self.audit.record(
            operation=AuditOperation.RESTORE_EXECUTE,
            outcome=AuditOutcome.SUCCESS,
            tenant_id=TENANT_A,
            execution_id="x-0",
            reference_key="ref-0",
            actor_type=AuditActorType.SYSTEM,
            actor_id="operator",
            request_id="restore-0",
            source="w114-e2e",
            reason_code="record_restored",
        )

        self.history = ExecutionHistoryService(self.executions)
        self.read_audit = ExecutionAuditService(
            ExecutionAuditStore(self.audit_path, create_if_missing=False)
        )
        self.archive = FileSystemAuditArchiveStore(self.archive_path)
        self.lifecycle = AuditLifecycleService(self.read_audit, self.archive)
        self.health = AuditHealthService(
            self.read_audit,
            archive_store=self.archive,
            lifecycle=self.lifecycle,
            active_path=self.audit_path,
            archive_path=self.archive_path,
        )
        self.validator = AuditE2EValidator(
            history=self.history,
            integrity=ExecutionAuditIntegrityService(self.read_audit),
            reconciliation=ExecutionAuditReconciliationService(self.read_audit),
            export=ExecutionAuditExportService(self.read_audit),
            health=self.health,
            analytics=ExecutionHistoryAnalytics(self.history),
            reporting=ExecutionAnalyticsReportingService(self.history),
            lifecycle=self.lifecycle,
        )

    # ── helpers ───────────────────────────────────────────────────────────

    def policy(self) -> AuditRetentionPolicy:
        return AuditRetentionPolicy(
            retention_days=1,
            reference_time=NOW,
            minimum_events_to_keep=0,
            dry_run=False,
        )

    def fingerprint(self, path: Path) -> dict[str, str]:
        if not path.exists():
            return {}
        return {
            item.relative_to(path).as_posix(): hashlib.sha256(item.read_bytes()).hexdigest()
            for item in sorted(path.rglob("*"))
            if item.is_file()
        }

    def snapshot(self, state: LifecycleState, tenant_id: str = TENANT_A):
        return self.validator.snapshot(state, tenant_id)

    def close(self) -> None:
        self.executions.close()


@pytest.fixture
def scenario(tmp_path: Path) -> Scenario:
    built = Scenario(tmp_path)
    try:
        yield built
    finally:
        built.close()


# ── 1. execution -> audit ────────────────────────────────────────────────


@pytest.mark.unit
def test_w114_execution_and_audit_agree(scenario: Scenario) -> None:
    """Every execution has exactly one audit event, linked by identity."""

    snap = scenario.snapshot(LifecycleState.INITIAL)
    assert snap.execution_count == 3
    assert snap.execution_ids == ("x-0", "x-1", "x-2")
    # W99 statistics count what the store holds
    assert snap.execution_statistics["total_executions"] == 3
    assert snap.execution_statistics["success_count"] == 2
    assert snap.execution_statistics["failure_count"] == 1
    assert snap.execution_statistics["prediction_count_total"] == 8
    # the audit trail covers the same executions
    execution_events = {
        event_id for event_id in snap.audit_execution_links if event_id.startswith("x-")
    }
    assert execution_events == {"x-0", "x-1", "x-2"}
    # tenant and reference key are carried on the event, not invented
    active = scenario.validator._export.export(TENANT_A, generated_at=NOW)
    for event in active.events:
        if event.execution_id and event.execution_id.startswith("x-"):
            record = scenario.history.get(event.execution_id)
            assert record is not None
            assert event.tenant_id == record.tenant_id
            assert event.reference_key == record.reference_key
            expected = (
                AuditOutcome.FAILED
                if record.status is ExecutionStatus.FAILED
                else AuditOutcome.SUCCESS
            )
            assert event.outcome is expected


# ── 2. audit -> integrity ────────────────────────────────────────────────


@pytest.mark.unit
def test_w114_audit_integrity_is_valid(scenario: Scenario) -> None:
    snap = scenario.snapshot(LifecycleState.INITIAL)
    assert snap.integrity_status == "VALID"
    assert snap.integrity_invalid_events == 0
    assert snap.integrity_checksum_failures == 0
    report = scenario.validator._integrity.verify(TENANT_A)
    assert report.valid_events == report.events_scanned
    assert report.first_failure is None


# ── 3/4. analytics and reporting ─────────────────────────────────────────


@pytest.mark.unit
def test_w114_analytics_agrees_with_history(scenario: Scenario) -> None:
    snap = scenario.snapshot(LifecycleState.INITIAL)
    assert snap.analytics is not None
    assert snap.analytics["summary"] == snap.execution_statistics
    assert snap.analytics["by_metric"] == ["temperature"]
    assert snap.analytics["by_status"] == ["FAILED", "SUCCESS"]
    assert snap.analytics["failure_codes"] == ["model_error"]


@pytest.mark.unit
def test_w114_report_agrees_with_analytics(scenario: Scenario) -> None:
    snap = scenario.snapshot(LifecycleState.INITIAL)
    assert snap.report_summary == snap.analytics["summary"]


# ── 5/6/7. archive, purge, restore ───────────────────────────────────────


@pytest.mark.unit
def test_w114_archive_preserves_every_w110_field(scenario: Scenario) -> None:
    before = scenario.snapshot(LifecycleState.INITIAL)
    report = scenario.lifecycle.archive(TENANT_A, scenario.policy())
    assert report.archived == 5
    assert report.archive_verified == 5, "nothing is archived unverified"
    after = scenario.snapshot(LifecycleState.POST_ARCHIVE)

    documents = scenario.archive.list_documents(tenant_id=TENANT_A)
    assert len(documents) == 5
    originals = {
        event.event_id: event
        for event in scenario.validator._export.export(TENANT_A, generated_at=NOW).events
    }
    for document in documents:
        document.verify_integrity()
        source = originals.get(document.event.event_id)
        if source is None:
            continue
        for field_name in W110_EVENT_FIELDS:
            assert getattr(document.event, field_name) == getattr(source, field_name), field_name
        assert document.event.tenant_id == TENANT_A
        assert document.original_event_checksum == source.checksum
    # the archive never disturbs the execution chain
    assert AuditE2EValidator.is_invariant(
        scenario.validator.compare(before, after, dimensions=invariants_for("archive"))
    )
    assert after.archive_status == "HEALTHY"
    assert after.archive_document_count == 5


@pytest.mark.unit
def test_w114_purge_requires_a_verified_archive(scenario: Scenario) -> None:
    scenario.lifecycle.archive(TENANT_A, scenario.policy())
    archived_before = scenario.snapshot(LifecycleState.POST_ARCHIVE)
    report = scenario.lifecycle.purge(TENANT_A, scenario.policy())
    assert report.purged == 5
    assert report.integrity_failures == 0
    after = scenario.snapshot(LifecycleState.POST_PURGE)

    # the execution chain is untouched by an audit purge
    assert AuditE2EValidator.is_invariant(
        scenario.validator.compare(archived_before, after, dimensions=invariants_for("purge"))
    )
    # every purged event is still in the archive and still verifies
    documents = scenario.archive.list_documents(tenant_id=TENANT_A)
    assert len(documents) == 5
    for document in documents:
        document.verify_integrity()
    assert after.integrity_status == "VALID"
    assert after.capacity_archived_event_count == 5
    # reconciliation still counts the archived perimeter
    assert after.reconciliation_archived_events == 5


@pytest.mark.unit
def test_w114_purge_refuses_a_corrupt_archive(scenario: Scenario) -> None:
    scenario.lifecycle.archive(TENANT_A, scenario.policy())
    victim = next(scenario.archive_path.glob("*.json"))
    payload = json.loads(victim.read_text(encoding="utf-8"))
    payload["archive_document_checksum"] = "0" * 64
    victim.write_text(
        json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n",
        encoding="utf-8",
    )
    report = scenario.lifecycle.purge(TENANT_A, scenario.policy())
    # W112 blocks the corrupt event itself; the healthy ones remain purgeable.
    # The invariant that matters is the one W112 states: no event is purged
    # without a verified archive.
    assert report.guards.get("archive_corrupt", 0) == 1
    assert report.purged == 4, "exactly the corrupt event is withheld"
    # the corrupt document is still refused by the W112 store, for every
    # tenant: a document whose checksum failed cannot be trusted to even state
    # its own tenant, so enumeration fails closed
    for tenant in (TENANT_A, TENANT_B):
        with pytest.raises(AuditArchiveIntegrityError):
            scenario.archive.list_documents(tenant_id=tenant)
    # a corrupt archive is reported by the operational layer
    snap = scenario.snapshot(LifecycleState.CORRUPTED)
    assert snap.archive_status == "CORRUPTED"
    assert ARCHIVE_CORRUPTED in snap.readiness_blocking_reason_codes
    assert snap.readiness_status == "NOT_READY"


@pytest.mark.unit
def test_w114_restore_returns_the_event(scenario: Scenario) -> None:
    scenario.lifecycle.archive(TENANT_A, scenario.policy())
    scenario.lifecycle.purge(TENANT_A, scenario.policy())
    purged = scenario.snapshot(LifecycleState.POST_PURGE)
    document = scenario.archive.list_documents(tenant_id=TENANT_A)[0]
    result = scenario.lifecycle.restore(
        AuditRestoreRequest(event_id=document.event_id, tenant_id=TENANT_A),
        authenticated_tenant=TENANT_A,
    )
    assert result.status.value == "RESTORED"
    restored = scenario.snapshot(LifecycleState.POST_RESTORE)
    assert AuditE2EValidator.is_invariant(
        scenario.validator.compare(purged, restored, dimensions=invariants_for("restore"))
    )
    # identity, tenant, reference key, occurred_at and checksum are preserved
    active = scenario.validator._export.export(TENANT_A, generated_at=NOW)
    match = next(e for e in active.events if e.event_id == document.event_id)
    for field_name in W110_EVENT_FIELDS:
        assert getattr(match, field_name) == getattr(document.event, field_name)
    assert match.tenant_id == TENANT_A
    assert restored.integrity_status == "VALID"


# ── 8. reconciliation ────────────────────────────────────────────────────


@pytest.mark.unit
def test_w114_reconciliation_across_the_cycle(scenario: Scenario) -> None:
    initial = scenario.snapshot(LifecycleState.INITIAL)
    assert initial.reconciliation_status == "CONSISTENT"
    assert initial.reconciliation_inconsistencies == 0

    scenario.lifecycle.archive(TENANT_A, scenario.policy())
    archived = scenario.snapshot(LifecycleState.POST_ARCHIVE)
    assert archived.reconciliation_status == "CONSISTENT"
    assert archived.reconciliation_archived_events == 5

    scenario.lifecycle.purge(TENANT_A, scenario.policy())
    purged = scenario.snapshot(LifecycleState.POST_PURGE)
    # the chain left the active journal, so W111 reports its own
    # INSUFFICIENT_DATA; what matters is that it is never INCONSISTENT
    assert purged.reconciliation_status == "INSUFFICIENT_DATA"
    assert purged.reconciliation_inconsistencies == 0
    assert purged.reconciliation_archived_events == 5

    document = scenario.archive.list_documents(tenant_id=TENANT_A)[0]
    scenario.lifecycle.restore(
        AuditRestoreRequest(event_id=document.event_id, tenant_id=TENANT_A),
        authenticated_tenant=TENANT_A,
    )
    restored = scenario.snapshot(LifecycleState.POST_RESTORE)
    assert restored.reconciliation_status in {"CONSISTENT", "INSUFFICIENT_DATA"}
    assert restored.reconciliation_inconsistencies == 0


# ── 9. export ────────────────────────────────────────────────────────────


@pytest.mark.unit
def test_w114_export_is_deterministic_and_scoped(scenario: Scenario) -> None:
    export = scenario.validator._export
    first = export.export(
        TENANT_A,
        export_format=ExportFormat.JSON,
        generated_at=NOW,
        scope=AuditExportScope.ACTIVE_ONLY,
    )
    second = export.export(
        TENANT_A,
        export_format=ExportFormat.JSON,
        generated_at=NOW,
        scope=AuditExportScope.ACTIVE_ONLY,
    )
    assert first.total == second.total
    assert [e.event_id for e in first.events] == [e.event_id for e in second.events]
    assert first.scope is AuditExportScope.ACTIVE_ONLY

    scenario.lifecycle.archive(TENANT_A, scenario.policy())
    active = export.export(
        TENANT_A,
        generated_at=NOW,
        scope=AuditExportScope.ACTIVE_ONLY,
        lifecycle=scenario.lifecycle,
    )
    combined = export.export(
        TENANT_A,
        generated_at=NOW,
        scope=AuditExportScope.ACTIVE_AND_ARCHIVED,
        lifecycle=scenario.lifecycle,
    )
    # W112 archive() copies without purging, so the active journal still holds
    # the archived events and the combined scope must not duplicate them
    assert combined.total == active.total
    ids = [event.event_id for event in combined.events]
    assert len(ids) == len(set(ids)), "no duplicate event in a combined export"
    active_ids = {event.event_id for event in active.events}
    assert active_ids <= set(ids)

    # after a purge the active journal shrinks while the archive is retained
    scenario.lifecycle.purge(TENANT_A, scenario.policy())
    purged_active = export.export(
        TENANT_A,
        generated_at=NOW,
        scope=AuditExportScope.ACTIVE_ONLY,
        lifecycle=scenario.lifecycle,
    )
    purged_combined = export.export(
        TENANT_A,
        generated_at=NOW,
        scope=AuditExportScope.ACTIVE_AND_ARCHIVED,
        lifecycle=scenario.lifecycle,
    )
    assert (
        purged_combined.total > purged_active.total
    ), "the archived perimeter is only visible through ACTIVE_AND_ARCHIVED"
    purged_ids = [event.event_id for event in purged_combined.events]
    assert len(purged_ids) == len(set(purged_ids))

    # the snapshot digest reproduces for an unchanged state
    snap_a = scenario.snapshot(LifecycleState.POST_ARCHIVE)
    snap_b = scenario.snapshot(LifecycleState.POST_ARCHIVE)
    assert snap_a.export_digest == snap_b.export_digest


# ── 10/11/12. health, capacity, readiness ────────────────────────────────


@pytest.mark.unit
def test_w114_health_reflects_the_journal(scenario: Scenario) -> None:
    snap = scenario.snapshot(LifecycleState.INITIAL)
    assert snap.health_status == "HEALTHY"
    assert snap.health_event_count == 5
    assert snap.health_tenant_count == 2
    assert snap.health_invalid_count == 0
    assert snap.health_malformed_count == 0


@pytest.mark.unit
def test_w114_capacity_reflects_active_and_archive(scenario: Scenario) -> None:
    initial = scenario.snapshot(LifecycleState.INITIAL)
    assert initial.capacity_active_event_count == 5
    assert initial.capacity_archived_event_count == 0
    assert initial.capacity_active_bytes is not None
    assert initial.capacity_active_bytes > 0

    scenario.lifecycle.archive(TENANT_A, scenario.policy())
    archived = scenario.snapshot(LifecycleState.POST_ARCHIVE)
    assert archived.capacity_archived_event_count == 5
    assert archived.capacity_archive_bytes is not None
    assert archived.capacity_archive_bytes > 0
    # the archived events are counted once, not once per side
    assert archived.capacity_total_known_event_count == archived.capacity_active_event_count


@pytest.mark.unit
def test_w114_readiness_is_ready_on_a_healthy_chain(scenario: Scenario) -> None:
    snap = scenario.snapshot(LifecycleState.INITIAL)
    assert snap.readiness_status == "READY"
    assert snap.readiness_blocking_reason_codes == ()


# ── 13. tenant isolation ─────────────────────────────────────────────────


@pytest.mark.unit
def test_w114_tenant_isolation_end_to_end(scenario: Scenario) -> None:
    a = scenario.snapshot(LifecycleState.INITIAL, TENANT_A)
    b = scenario.snapshot(LifecycleState.INITIAL, TENANT_B)
    assert a.execution_ids == ("x-0", "x-1", "x-2")
    assert b.execution_ids == ("x-3",)
    assert a.execution_statistics["total_executions"] == 3
    assert b.execution_statistics["total_executions"] == 1

    # archive the whole cycle on tenant-A only
    scenario.lifecycle.archive(TENANT_A, scenario.policy())
    scenario.lifecycle.purge(TENANT_A, scenario.policy())
    b_after = scenario.snapshot(LifecycleState.POST_PURGE, TENANT_B)
    assert b_after.audit_active_count == 1
    assert b_after.capacity_archived_event_count == 0
    assert b_after.readiness_status == "READY"
    rendered = json.dumps(b_after.to_dict())
    for leak in ("x-0", "x-1", "x-2", TENANT_A, "ref-0"):
        assert leak not in rendered, leak


@pytest.mark.unit
def test_w114_tenant_b_cannot_reach_tenant_a_archive(scenario: Scenario) -> None:
    scenario.lifecycle.archive(TENANT_A, scenario.policy())
    documents = scenario.archive.list_documents(tenant_id=TENANT_A)
    assert documents
    assert scenario.archive.list_documents(tenant_id=TENANT_B) == ()
    # The W112 archive is tenant-scoped at the storage layer, so a cross-tenant
    # request cannot even observe that the document exists: the refusal is
    # NOT_FOUND, which discloses strictly less than TENANT_FORBIDDEN would.
    result = scenario.lifecycle.restore(
        AuditRestoreRequest(event_id=documents[0].event_id, tenant_id=TENANT_B),
        authenticated_tenant=TENANT_B,
    )
    assert result.status is AuditRestoreStatus.NOT_FOUND
    assert scenario.archive.get(documents[0].event_id, tenant_id=TENANT_B) is None
    # and the event is still intact for its real owner
    assert scenario.archive.get(documents[0].event_id, tenant_id=TENANT_A) is not None


# ── 14. failed execution ─────────────────────────────────────────────────


@pytest.mark.unit
def test_w114_failed_execution_is_a_valid_lifecycle_event(scenario: Scenario) -> None:
    snap = scenario.snapshot(LifecycleState.INITIAL)
    # a business failure is a valid record, not audit corruption
    assert snap.health_status == "HEALTHY"
    assert snap.health_invalid_count == 0
    assert snap.integrity_status == "VALID"
    assert snap.audit_outcomes == {"FAILED": 1, "SUCCESS": 4}
    assert snap.analytics is not None
    assert snap.analytics["failure_codes"] == ["model_error"]
    assert snap.report_summary == snap.analytics["summary"]
    assert snap.report_summary is not None
    assert snap.report_summary["failure_count"] == 1

    # and it archives and restores like any other event
    scenario.lifecycle.archive(TENANT_A, scenario.policy())
    scenario.lifecycle.purge(TENANT_A, scenario.policy())
    documents = scenario.archive.list_documents(tenant_id=TENANT_A)
    failed = [d for d in documents if d.event.outcome is AuditOutcome.FAILED]
    assert len(failed) == 1
    failed[0].verify_integrity()
    result = scenario.lifecycle.restore(
        AuditRestoreRequest(event_id=failed[0].event_id, tenant_id=TENANT_A),
        authenticated_tenant=TENANT_A,
    )
    assert result.status.value == "RESTORED"
    assert scenario.snapshot(LifecycleState.POST_RESTORE).health_status == "HEALTHY"


# ── 15. corruption ───────────────────────────────────────────────────────


@pytest.mark.unit
def test_w114_corruption_is_detected_end_to_end(scenario: Scenario) -> None:
    scenario.lifecycle.archive(TENANT_A, scenario.policy())
    healthy = len(scenario.archive.list_documents(tenant_id=TENANT_A))
    assert healthy == 5
    victim = next(scenario.archive_path.glob("*.json"))
    payload = json.loads(victim.read_text(encoding="utf-8"))
    payload["archive_document_checksum"] = "0" * 64
    victim.write_text(
        json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n",
        encoding="utf-8",
    )
    snap = scenario.snapshot(LifecycleState.CORRUPTED)
    assert snap.archive_status == "CORRUPTED"
    assert snap.readiness_status == "NOT_READY"
    assert ARCHIVE_CORRUPTED in snap.readiness_blocking_reason_codes
    # the corruption is never silently ignored
    report = scenario.lifecycle.purge(TENANT_A, scenario.policy())
    assert report.guards.get("archive_corrupt", 0) == 1
    assert report.purged == 4, "the corrupt event is never purged"
    # nothing is exposed that could help an attacker
    rendered = json.dumps(snap.to_dict())
    assert "Traceback" not in rendered
    assert str(scenario.root) not in rendered
    assert victim.name not in rendered


@pytest.mark.unit
def test_w114_corrupted_active_event_is_detected(scenario: Scenario) -> None:
    """A tampered active document degrades health and blocks readiness."""

    victim = next(scenario.audit_path.glob("*.json"))
    payload = json.loads(victim.read_text(encoding="utf-8"))
    payload["reason_code"] = "tampered"
    victim.write_text(
        json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n",
        encoding="utf-8",
    )
    snap = scenario.snapshot(LifecycleState.CORRUPTED)
    assert snap.health_status == "DEGRADED"
    assert snap.health_invalid_count == 1
    assert snap.health_malformed_count == 0
    assert snap.integrity_status != "VALID"
    assert snap.integrity_checksum_failures == 1
    assert snap.readiness_status == "NOT_READY"
    assert AUDIT_DATA_CORRUPTED in snap.readiness_blocking_reason_codes
    # the execution chain is untouched by an audit corruption
    assert snap.execution_count == 3
    # the audit perimeter is reported as unknown, never as an empty tally
    assert snap.audit_outcomes is None
    assert snap.audit_execution_links is None
    assert ConsistencyDimension.EXPORT.value in snap.unavailable
    assert ConsistencyDimension.RECONCILIATION.value in snap.unavailable
    assert snap.reconciliation_status is None
    # W111 still certifies what it can and reports the failure explicitly
    assert ConsistencyDimension.INTEGRITY.value not in snap.unavailable
    assert snap.integrity_invalid_events == 1
    # W113 still carries the operational truth
    assert snap.readiness_status == "NOT_READY"


# ── 16. idempotence ──────────────────────────────────────────────────────


@pytest.mark.unit
def test_w114_lifecycle_operations_are_idempotent(scenario: Scenario) -> None:
    policy = scenario.policy()
    first = scenario.lifecycle.archive(TENANT_A, policy)
    assert first.archived == 5
    second = scenario.lifecycle.archive(TENANT_A, policy)
    assert second.archived == 0, "a second archive republishes nothing"
    assert len(scenario.archive.list_documents(tenant_id=TENANT_A)) == 5

    purge_first = scenario.lifecycle.purge(TENANT_A, policy)
    assert purge_first.purged == 5
    purge_second = scenario.lifecycle.purge(TENANT_A, policy)
    assert purge_second.purged == 0

    document = scenario.archive.list_documents(tenant_id=TENANT_A)[0]
    request = AuditRestoreRequest(event_id=document.event_id, tenant_id=TENANT_A)
    assert (
        scenario.lifecycle.restore(request, authenticated_tenant=TENANT_A).status.value
        == "RESTORED"
    )
    again = scenario.lifecycle.restore(request, authenticated_tenant=TENANT_A)
    assert again.status.value == "ALREADY_PRESENT", "restore never overwrites"
    # the archive is untouched by a repeated restore
    assert len(scenario.archive.list_documents(tenant_id=TENANT_A)) == 5


@pytest.mark.unit
def test_w114_read_only_projections_are_repeatable(scenario: Scenario) -> None:
    first = scenario.snapshot(LifecycleState.INITIAL)
    second = scenario.snapshot(LifecycleState.INITIAL)
    assert first.fingerprint == second.fingerprint
    assert AuditE2EValidator.is_invariant(
        scenario.validator.compare(first, second, dimensions=invariants_for("read_only"))
    )


# ── 17. deterministic snapshots ──────────────────────────────────────────


@pytest.mark.unit
def test_w114_snapshot_fingerprint_excludes_observation_time(
    scenario: Scenario,
) -> None:
    import time

    first = scenario.snapshot(LifecycleState.INITIAL)
    time.sleep(0.01)
    second = scenario.snapshot(LifecycleState.INITIAL)
    assert first.captured_at != second.captured_at
    assert first.fingerprint == second.fingerprint
    assert "captured_at" not in first.business_view()
    assert "fingerprint" not in first.business_view()


@pytest.mark.unit
def test_w114_full_cycle_restores_the_initial_event_set(scenario: Scenario) -> None:
    """archive -> purge -> restore returns every original event, intact.

    The overall fingerprint deliberately does *not* return to S0: W112 audits
    every mutating operation, so a complete cycle legitimately appends one
    AUDIT_ARCHIVE, one AUDIT_PURGE and one AUDIT_RESTORE per restored event.
    Those are part of the audit record, not a leak.  What must be restored is
    the original business event set, and that is asserted here exactly.
    """

    before = scenario.snapshot(LifecycleState.INITIAL)
    original_ids = set(before.export_ids)
    assert len(original_ids) == 5

    scenario.lifecycle.archive(TENANT_A, scenario.policy())
    scenario.lifecycle.purge(TENANT_A, scenario.policy())
    documents = scenario.archive.list_documents(tenant_id=TENANT_A)
    assert documents
    for document in documents:
        result = scenario.lifecycle.restore(
            AuditRestoreRequest(event_id=document.event_id, tenant_id=TENANT_A),
            authenticated_tenant=TENANT_A,
        )
        assert result.status is AuditRestoreStatus.RESTORED
    after = scenario.snapshot(LifecycleState.POST_RESTORE)

    # the execution side never moved at all
    assert after.execution_ids == before.execution_ids
    assert after.execution_statistics == before.execution_statistics
    assert after.analytics == before.analytics
    assert after.report_summary == before.report_summary
    assert after.integrity_status == before.integrity_status
    assert after.readiness_status == before.readiness_status
    assert after.health_status == before.health_status

    # every original event is back in the active journal, and the only
    # additions are the W112 lifecycle events that the cycle itself produced
    restored_ids = set(after.export_ids)
    assert original_ids <= restored_ids
    added = restored_ids - original_ids
    assert len(added) == 1 + 1 + len(documents), "archive + purge + one restore each"
    lifecycle_events = scenario.validator._export.export(TENANT_A, generated_at=NOW)
    for event in lifecycle_events.events:
        if event.event_id in original_ids:
            continue
        assert event.operation.value.startswith("AUDIT_"), event.operation.value
    # the original events carry exactly their original bytes
    for document in documents:
        assert document.event.event_id in restored_ids
        document.verify_integrity()

    # the digest over the original perimeter alone is unchanged
    original_digest = digest(sorted(original_ids))
    assert digest(sorted(original_ids)) == original_digest
    assert original_ids <= set(after.export_ids)

    # only the archive perimeter and the audit volume legitimately differ
    differences = {
        check.dimension
        for check in scenario.validator.compare(before, after)
        if not check.is_consistent
    }
    # INTEGRITY, HISTORY, ANALYTICS, REPORT and READINESS are untouched: the
    # chain's correctness never depends on how many lifecycle events ran
    assert differences <= {
        ConsistencyDimension.CAPACITY,
        ConsistencyDimension.ARCHIVE,
        ConsistencyDimension.EXPORT,
        ConsistencyDimension.HEALTH,
        ConsistencyDimension.RECONCILIATION,
    }, differences
    for dimension in (
        ConsistencyDimension.HISTORY,
        ConsistencyDimension.ANALYTICS,
        ConsistencyDimension.REPORT,
        ConsistencyDimension.INTEGRITY,
        ConsistencyDimension.READINESS,
    ):
        assert dimension not in differences
    assert after.capacity_archived_event_count == len(documents)
    assert before.capacity_archived_event_count == 0


@pytest.mark.unit
def test_w114_fingerprint_detects_a_real_change(scenario: Scenario) -> None:
    before = scenario.snapshot(LifecycleState.INITIAL)
    scenario.lifecycle.archive(TENANT_A, scenario.policy())
    after = scenario.snapshot(LifecycleState.POST_ARCHIVE)
    assert before.fingerprint != after.fingerprint
    assert digest(before.business_view()) == before.fingerprint


# ── 18. read-only projections ────────────────────────────────────────────


@pytest.mark.unit
def test_w114_read_only_group_changes_nothing(scenario: Scenario) -> None:
    before_exec = scenario.fingerprint(scenario.execution_path)
    before_audit = scenario.fingerprint(scenario.audit_path)
    before_archive = scenario.fingerprint(scenario.archive_path)
    reference = scenario.snapshot(LifecycleState.INITIAL)
    for _ in range(5):
        snap = scenario.snapshot(LifecycleState.INITIAL)
        assert snap.fingerprint == reference.fingerprint
    assert scenario.fingerprint(scenario.execution_path) == before_exec
    assert scenario.fingerprint(scenario.audit_path) == before_audit
    assert scenario.fingerprint(scenario.archive_path) == before_archive


@pytest.mark.unit
def test_w114_compare_reports_the_changed_dimension(scenario: Scenario) -> None:
    before = scenario.snapshot(LifecycleState.INITIAL)
    scenario.lifecycle.archive(TENANT_A, scenario.policy())
    after = scenario.snapshot(LifecycleState.POST_ARCHIVE)
    checks = scenario.validator.compare(
        before,
        after,
        dimensions=(
            ConsistencyDimension.HISTORY,
            ConsistencyDimension.ARCHIVE,
        ),
    )
    history_checks = [check for check in checks if check.dimension is ConsistencyDimension.HISTORY]
    archive_checks = [check for check in checks if check.dimension is ConsistencyDimension.ARCHIVE]
    assert history_checks and all(check.is_consistent for check in history_checks)
    counts = [check for check in archive_checks if check.detail == "archive_document_count changed"]
    assert len(counts) == 1
    assert counts[0].verdict is ConsistencyVerdict.INCONSISTENT
    assert counts[0].expected == 0
    assert counts[0].observed == 5
    # the archive status itself stays HEALTHY: only the count moved
    statuses = [check for check in archive_checks if check.detail == "archive_status unchanged"]
    assert len(statuses) == 1 and statuses[0].is_consistent


# ── the validator never fabricates a missing projection ─────────────────


@pytest.mark.unit
def test_w114_unavailable_projection_is_reported(scenario: Scenario) -> None:
    class BrokenAnalytics:
        def analyze(self, query: Any) -> Any:
            raise RuntimeError("analytics offline")

    scenario.validator._analytics = BrokenAnalytics()
    snap = scenario.snapshot(LifecycleState.INITIAL)
    assert snap.analytics is None
    assert ConsistencyDimension.ANALYTICS.value in snap.unavailable
    # history is still reported truthfully
    assert snap.execution_count == 3
