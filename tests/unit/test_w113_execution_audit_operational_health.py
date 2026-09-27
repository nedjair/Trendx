"""W113 — audit operational health, capacity and readiness unit tests."""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient
from pydantic import SecretStr
from trendx.config import settings
from trendx.forecasting.api import (
    TenantContext,
    get_execution_audit_health_service,
    get_tenant_context,
)
from trendx.forecasting.audit import (
    AuditActorType,
    AuditOperation,
    AuditOutcome,
    ExecutionAuditEvent,
    ExecutionAuditService,
    ExecutionAuditStore,
    MemoryExecutionAuditStore,
)
from trendx.forecasting.audit_health import (
    ARCHIVE_CORRUPTED,
    ARCHIVE_UNAVAILABLE,
    AUDIT_DATA_CORRUPTED,
    AUDIT_HEALTH_REPORT_VERSION,
    AUDIT_JOURNAL_EMPTY,
    AUDIT_SCHEMA_INVALID,
    AUDIT_STORE_UNAVAILABLE,
    BLOCKING_REASON_CODES,
    INTEGRITY_CHECK_FAILED,
    TENANT_CONTEXT_INVALID,
    AuditArchiveStatus,
    AuditHealthService,
    AuditOperationalStatus,
    AuditReadinessStatus,
    directory_bytes,
    filesystem_metrics,
)
from trendx.forecasting.audit_lifecycle import (
    AuditLifecycleService,
    AuditRetentionPolicy,
    FileSystemAuditArchiveStore,
)
from trendx.main import app

NOW = datetime(2026, 4, 1, tzinfo=UTC)
HEALTH_PATHS = (
    "/api/v1/forecast/executions/audit/health",
    "/api/v1/forecast/executions/audit/capacity",
    "/api/v1/forecast/executions/audit/readiness",
)


def _event(
    event_id: str,
    *,
    tenant_id: str = "tenant-A",
    outcome: AuditOutcome = AuditOutcome.SUCCESS,
    days_ago: int = 1,
    request_id: str = "request-A",
) -> ExecutionAuditEvent:
    return ExecutionAuditEvent(
        event_id=event_id,
        occurred_at=NOW - timedelta(days=days_ago),
        operation=AuditOperation.EXECUTION,
        outcome=outcome,
        tenant_id=tenant_id,
        execution_id=f"exec-{event_id}",
        reference_key=f"ref:{event_id}",
        actor_type=AuditActorType.SYSTEM,
        actor_id="system",
        request_id=request_id,
        source="w113-test",
        reason_code="record_restored",
    )


def _fingerprint(root: Path) -> dict[str, str]:
    return {
        item.relative_to(root).as_posix(): hashlib.sha256(item.read_bytes()).hexdigest()
        for item in sorted(root.rglob("*"))
        if item.is_file()
    }


def _seed(
    tmp_path: Path,
    *,
    count: int = 5,
    archived: int = 0,
    tenant_id: str = "tenant-A",
    with_archive: bool = True,
) -> AuditHealthService:
    store = ExecutionAuditStore(tmp_path / "audit")
    for index in range(count):
        store.append(
            _event(
                f"e-{index}",
                tenant_id=tenant_id,
                request_id=f"r{index}",
                days_ago=100 + index,
            )
        )
    service = ExecutionAuditService(ExecutionAuditStore(tmp_path / "audit"))
    archive_path = tmp_path / "archive"
    archive = None
    lifecycle = None
    if with_archive:
        archive = FileSystemAuditArchiveStore(archive_path)
        lifecycle = AuditLifecycleService(service, archive)
        if archived:
            # every seeded event is older than the cutoff, so protecting the
            # newest ``count - archived`` archives exactly ``archived`` documents
            lifecycle.archive(
                tenant_id,
                AuditRetentionPolicy(
                    retention_days=1,
                    reference_time=NOW,
                    minimum_events_to_keep=count - archived,
                    dry_run=False,
                ),
            )
    return AuditHealthService(
        service,
        archive_store=archive,
        lifecycle=lifecycle,
        active_path=tmp_path / "audit",
        archive_path=archive_path if with_archive else None,
    )


# ── 1. empty store ───────────────────────────────────────────────────────


@pytest.mark.unit
def test_w113_empty_store_is_empty_but_ready(tmp_path: Path) -> None:
    service = _seed(tmp_path, count=0, with_archive=False)
    snapshot = service.snapshot("tenant-A")
    assert snapshot.health.status is AuditOperationalStatus.EMPTY
    assert snapshot.health.event_count == 0
    assert snapshot.health.oldest_event_at is None
    assert snapshot.health.newest_event_at is None
    assert AUDIT_JOURNAL_EMPTY in snapshot.health.reason_codes
    # an empty journal is never automatically NOT_READY
    assert snapshot.readiness.readiness_status is AuditReadinessStatus.READY
    assert not snapshot.readiness.blocking_reason_codes


# ── 2. healthy store ─────────────────────────────────────────────────────


@pytest.mark.unit
def test_w113_healthy_store(tmp_path: Path) -> None:
    service = _seed(tmp_path, count=5)
    health = service.health("tenant-A")
    assert health.status is AuditOperationalStatus.HEALTHY
    assert health.event_count == 5
    assert health.valid_event_count == 5
    assert health.integrity_invalid_count == 0
    assert health.malformed_count == 0
    assert health.version_failure_count == 0
    assert health.checksum_failure_count == 0
    assert health.tenant_count == 1
    assert health.success_count == 5
    assert health.failure_count == 0
    assert health.oldest_event_at is not None
    assert health.newest_event_at is not None
    assert health.last_observed_event_at == health.newest_event_at
    assert health.report_version == AUDIT_HEALTH_REPORT_VERSION


@pytest.mark.unit
def test_w113_outcome_breakdown(tmp_path: Path) -> None:
    store = ExecutionAuditStore(tmp_path / "audit")
    store.append(_event("ok", request_id="r1"))
    store.append(_event("bad", outcome=AuditOutcome.FAILED, request_id="r2"))
    service = ExecutionAuditService(ExecutionAuditStore(tmp_path / "audit"))
    health = AuditHealthService(service, active_path=tmp_path / "audit").health("tenant-A")
    assert health.success_count == 1
    assert health.failure_count == 1


# ── 3. degraded store + 4/5/6 corrupt documents ──────────────────────────


@pytest.mark.unit
@pytest.mark.parametrize(
    ("mutate", "status", "reason", "counter"),
    [
        (
            "checksum",
            AuditOperationalStatus.DEGRADED,
            AUDIT_DATA_CORRUPTED,
            "checksum_failure_count",
        ),
        ("version", AuditOperationalStatus.DEGRADED, AUDIT_SCHEMA_INVALID, "version_failure_count"),
        ("schema", AuditOperationalStatus.DEGRADED, AUDIT_SCHEMA_INVALID, "schema_failure_count"),
        ("malformed", AuditOperationalStatus.DEGRADED, AUDIT_DATA_CORRUPTED, "malformed_count"),
        (
            "identity",
            AuditOperationalStatus.DEGRADED,
            AUDIT_DATA_CORRUPTED,
            "identity_failure_count",
        ),
        (
            "canonical",
            AuditOperationalStatus.DEGRADED,
            AUDIT_DATA_CORRUPTED,
            "canonicalization_failure_count",
        ),
        (
            "permission",
            AuditOperationalStatus.DEGRADED,
            AUDIT_DATA_CORRUPTED,
            "unsafe_document_count",
        ),
    ],
)
def test_w113_degraded_store_classifies_corruption(
    tmp_path: Path,
    mutate: str,
    status: AuditOperationalStatus,
    reason: str,
    counter: str,
) -> None:
    store = ExecutionAuditStore(tmp_path / "audit")
    store.append(_event("victim", request_id="r1"))
    path = next(store.path.glob("*.json"))
    payload = json.loads(path.read_text(encoding="utf-8"))
    if mutate == "checksum":
        payload["reason_code"] = "tampered"
    elif mutate == "version":
        payload["event_version"] = "999"
    elif mutate == "schema":
        payload["unexpected"] = "value"
    elif mutate == "malformed":
        path.write_text("{broken", encoding="utf-8")
    elif mutate == "identity":
        raw = path.read_text(encoding="utf-8")
        path.unlink()
        moved = store.path / ("0" * 64 + ".json")
        moved.write_text(raw, encoding="utf-8")
        moved.chmod(0o600)
    elif mutate == "canonical":
        path.write_text(json.dumps(payload, sort_keys=False) + "\n", encoding="utf-8")
        path.chmod(0o600)
    elif mutate == "permission":
        path.chmod(0o644)
    if mutate not in {"malformed", "identity", "canonical", "permission"}:
        path.write_text(
            json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n",
            encoding="utf-8",
        )
    service = ExecutionAuditService(ExecutionAuditStore(tmp_path / "audit"))
    health = AuditHealthService(service, active_path=tmp_path / "audit").health("tenant-A")
    assert health.status is status
    assert health.integrity_invalid_count == 1
    assert getattr(health, counter) == 1
    assert reason in health.reason_codes
    # the corruption is never hidden behind HEALTHY
    assert health.status is not AuditOperationalStatus.HEALTHY


@pytest.mark.unit
def test_w113_version_mismatch_is_schema_invalid_not_corruption(tmp_path: Path) -> None:
    """§6/§26.20: a schema/version mismatch is reported as its own class."""

    store = ExecutionAuditStore(tmp_path / "audit")
    store.append(_event("versioned", request_id="r1"))
    path = next(store.path.glob("*.json"))
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["event_version"] = "999"
    path.write_text(
        json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8"
    )
    service = ExecutionAuditService(ExecutionAuditStore(tmp_path / "audit"))
    snapshot = AuditHealthService(service, active_path=tmp_path / "audit").snapshot("tenant-A")
    assert snapshot.health.version_failure_count == 1
    assert AUDIT_SCHEMA_INVALID in snapshot.readiness.blocking_reason_codes
    assert snapshot.readiness.readiness_status is AuditReadinessStatus.NOT_READY


# ── 7/8. capacity + archive capacity ──────────────────────────────────────


@pytest.mark.unit
def test_w113_capacity_snapshot(tmp_path: Path) -> None:
    service = _seed(tmp_path, count=5, archived=2)
    capacity = service.capacity("tenant-A")
    # W112 archive() copies without purging, so the active store still holds the
    # archived events: 5 seeded + 1 W112 lifecycle event
    assert capacity.event_count == 6
    assert capacity.active_bytes is not None and capacity.active_bytes > 0
    assert capacity.active_file_count >= 6
    assert capacity.archive_bytes is not None and capacity.archive_bytes > 0
    assert capacity.archived_event_count == 2
    # the archived pair is present on both sides and must be counted once
    assert capacity.total_known_event_count == 6, "no double counting of archived events"
    assert capacity.total_known_event_count < (capacity.event_count + capacity.archived_event_count)
    assert capacity.oldest_active_event_at is not None
    assert capacity.newest_active_event_at is not None
    assert capacity.oldest_archived_event_at is not None
    assert capacity.newest_archived_event_at is not None
    assert capacity.capacity_measurement_complete is True
    assert capacity.current_count == 6
    assert capacity.current_size_bytes == capacity.active_bytes


@pytest.mark.unit
def test_w113_capacity_reports_growth_facts_without_inventing_a_rate(tmp_path: Path) -> None:
    service = _seed(tmp_path, count=4)
    capacity = service.capacity("tenant-A")
    # no historical snapshot is persisted, so no rate is fabricated
    assert capacity.observed_growth is None
    assert capacity.observation_window is None
    assert capacity.current_count == 4
    assert capacity.current_size_bytes == capacity.active_bytes
    assert capacity.current_oldest_event_at is not None
    assert capacity.current_newest_event_at is not None


@pytest.mark.unit
def test_w113_filesystem_metrics(tmp_path: Path) -> None:
    store = ExecutionAuditStore(tmp_path / "audit")
    store.append(_event("fs", request_id="r1"))
    metrics = filesystem_metrics(tmp_path / "audit")
    assert metrics.available is True
    assert metrics.filesystem_total_bytes is not None
    assert metrics.filesystem_free_bytes is not None
    assert metrics.filesystem_used_bytes is not None
    assert metrics.inode_total is not None
    rendered = json.dumps(metrics.to_dict())
    assert str(tmp_path) not in rendered


@pytest.mark.unit
def test_w113_filesystem_metrics_unavailable(tmp_path: Path) -> None:
    missing = filesystem_metrics(tmp_path / "does-not-exist")
    assert missing.available is False
    assert missing.detail == "filesystem_metrics_unavailable"
    assert missing.filesystem_free_bytes is None
    # unavailable is reported, never invented
    assert missing.to_dict()["filesystem_total_bytes"] is None


@pytest.mark.unit
def test_w113_directory_bytes_is_stat_only(tmp_path: Path) -> None:
    store = ExecutionAuditStore(tmp_path / "audit")
    store.append(_event("bytes", request_id="r1"))
    total, count, truncated = directory_bytes(tmp_path / "audit")
    assert total is not None and total > 0
    assert count == 1
    assert truncated is False
    # a missing or symlinked directory is a gap, not zero
    assert directory_bytes(tmp_path / "nope") == (None, 0, False)
    link = tmp_path / "link"
    link.symlink_to(tmp_path / "audit")
    assert directory_bytes(link)[0] is None


@pytest.mark.unit
def test_w113_capacity_without_paths_is_unavailable(tmp_path: Path) -> None:
    service = ExecutionAuditService(MemoryExecutionAuditStore())
    service.store.append(_event("mem", request_id="r1"))
    health = AuditHealthService(service)  # no paths configured
    capacity = health.capacity("tenant-A")
    assert capacity.active_bytes is None
    assert capacity.filesystem.available is False
    assert capacity.capacity_measurement_complete is True
    assert health.readiness("tenant-A").readiness_status is AuditReadinessStatus.READY


# ── 9. archive health ────────────────────────────────────────────────────


@pytest.mark.unit
def test_w113_archive_health_healthy(tmp_path: Path) -> None:
    service = _seed(tmp_path, count=5, archived=2)
    archive = service.snapshot("tenant-A").archive
    assert archive.archive_status is AuditArchiveStatus.HEALTHY
    assert archive.archive_document_count == 2
    assert archive.archived_event_count == 2
    assert archive.valid_archive_count == 2
    assert archive.invalid_archive_count == 0
    assert archive.checksum_failure_count == 0
    assert archive.tenant_mismatch_count == 0
    assert archive.enumeration_complete is True
    assert archive.last_verified_at is not None


@pytest.mark.unit
def test_w113_archive_health_corrupted(tmp_path: Path) -> None:
    service = _seed(tmp_path, count=4, archived=2)
    victim = next((tmp_path / "archive").glob("*.json"))
    payload = json.loads(victim.read_text(encoding="utf-8"))
    payload["archive_document_checksum"] = "0" * 64
    victim.write_text(
        json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8"
    )
    archive = service.snapshot("tenant-A").archive
    assert archive.archive_status is AuditArchiveStatus.CORRUPTED
    assert ARCHIVE_CORRUPTED in archive.reason_codes
    # the enumeration gap is reported, never invented
    assert archive.enumeration_complete is False
    assert archive.archive_document_count is None
    assert archive.invalid_archive_count == 1
    assert archive.checksum_failure_count == 1
    # a corrupted archive blocks readiness
    assert service.readiness("tenant-A").readiness_status is AuditReadinessStatus.NOT_READY


@pytest.mark.unit
def test_w113_archive_health_is_tenant_scoped(tmp_path: Path) -> None:
    """A tenant archive report must never count another tenant's documents."""

    store = ExecutionAuditStore(tmp_path / "audit")
    for index in range(4):
        store.append(_event(f"a-{index}", tenant_id="tenant-A", request_id=f"a{index}"))
    for index in range(3):
        store.append(_event(f"b-{index}", tenant_id="tenant-B", request_id=f"b{index}"))
    service = ExecutionAuditService(ExecutionAuditStore(tmp_path / "audit"))
    archive = FileSystemAuditArchiveStore(tmp_path / "archive")
    lifecycle = AuditLifecycleService(service, archive)
    for tenant in ("tenant-A", "tenant-B"):
        lifecycle.archive(
            tenant,
            AuditRetentionPolicy(retention_days=1, reference_time=NOW, dry_run=False),
        )
    diagnostics = AuditHealthService(
        service,
        archive_store=archive,
        lifecycle=lifecycle,
        active_path=tmp_path / "audit",
        archive_path=tmp_path / "archive",
    )
    report_a = diagnostics.snapshot("tenant-A").archive
    report_b = diagnostics.snapshot("tenant-B").archive
    # each side enumerates exactly its own documents, and never the other's
    assert report_a.archived_event_ids == frozenset({"a-0", "a-1", "a-2", "a-3"})
    assert report_b.archived_event_ids == frozenset({"b-0", "b-1", "b-2"})
    assert report_a.archived_event_count == 4
    assert report_b.archived_event_count == 3
    assert report_a.tenant_mismatch_count == 0
    assert report_b.tenant_mismatch_count == 0
    assert report_a.archive_status is AuditArchiveStatus.HEALTHY
    assert report_b.archive_status is AuditArchiveStatus.HEALTHY
    rendered_a = json.dumps(report_a.to_dict())
    rendered_b = json.dumps(report_b.to_dict())
    assert "b-" not in rendered_a
    assert "a-" not in rendered_b
    assert diagnostics.capacity("tenant-A").archived_event_count == 4
    assert diagnostics.capacity("tenant-B").archived_event_count == 3


@pytest.mark.unit
def test_w113_archive_not_configured_is_not_blocking(tmp_path: Path) -> None:
    service = _seed(tmp_path, count=3, with_archive=False)
    snapshot = service.snapshot("tenant-A")
    assert snapshot.archive.archive_status is AuditArchiveStatus.NOT_CONFIGURED
    assert snapshot.readiness.readiness_status is AuditReadinessStatus.READY


@pytest.mark.unit
def test_w113_archive_unavailable(tmp_path: Path) -> None:
    class DeadArchive:
        def list_documents(self, **_kwargs: Any) -> tuple[Any, ...]:
            raise OSError("gone")

    service = _seed(tmp_path, count=3)
    service._archive = DeadArchive()  # type: ignore[assignment]
    archive = service.snapshot("tenant-A").archive
    assert archive.archive_status is AuditArchiveStatus.UNAVAILABLE
    assert ARCHIVE_UNAVAILABLE in archive.reason_codes
    assert service.readiness("tenant-A").readiness_status is AuditReadinessStatus.NOT_READY


# ── 10/11. backlog + protection ──────────────────────────────────────────


@pytest.mark.unit
def test_w113_backlog_is_diagnostic_only(tmp_path: Path) -> None:
    service = _seed(tmp_path, count=6, archived=2)
    policy = AuditRetentionPolicy(
        retention_days=30, reference_time=NOW, minimum_events_to_keep=2, dry_run=True
    )
    before = _fingerprint(tmp_path / "audit")
    backlog = service.snapshot("tenant-A", policy=policy).backlog
    # active: 6 seeded events + 1 W112 lifecycle event, of which the 2 newest are
    # protected by minimum_events_to_keep
    assert backlog.policy_supplied is True
    assert backlog.eligible_event_count == 5
    assert backlog.protected_event_count == 2
    assert backlog.minimum_events_protected_count == 2
    assert backlog.invalid_event_count == 0
    assert backlog.already_archived_count == 2
    # the backlog check is counted only when a policy actually supplied one
    assert service.readiness("tenant-A").checks_run == 4
    # a backlog must never trigger an action
    assert _fingerprint(tmp_path / "audit") == before
    assert len(list((tmp_path / "archive").glob("*.json"))) == 2


@pytest.mark.unit
def test_w113_backlog_absent_without_policy(tmp_path: Path) -> None:
    service = _seed(tmp_path, count=3)
    backlog = service.snapshot("tenant-A").backlog
    assert backlog.policy_supplied is False
    assert backlog.eligible_event_count is None
    assert backlog.protected_event_count is None


# ── 12/13/14. readiness and stable reason codes ──────────────────────────


@pytest.mark.unit
def test_w113_readiness_ready_on_healthy_store(tmp_path: Path) -> None:
    service = _seed(tmp_path, count=4, archived=1)
    readiness = service.readiness("tenant-A")
    assert readiness.readiness_status is AuditReadinessStatus.READY
    assert readiness.blocking_reason_codes == ()
    assert readiness.checks_run == 4, "no policy supplied, so no backlog check ran"
    assert readiness.health_status is AuditOperationalStatus.HEALTHY
    assert readiness.archive_status is AuditArchiveStatus.HEALTHY


@pytest.mark.unit
def test_w113_readiness_not_ready_on_corruption(tmp_path: Path) -> None:
    store = ExecutionAuditStore(tmp_path / "audit")
    store.append(_event("broken", request_id="r1"))
    path = next(store.path.glob("*.json"))
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["reason_code"] = "tampered"
    path.write_text(
        json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8"
    )
    service = ExecutionAuditService(ExecutionAuditStore(tmp_path / "audit"))
    readiness = AuditHealthService(service, active_path=tmp_path / "audit").readiness("tenant-A")
    assert readiness.readiness_status is AuditReadinessStatus.NOT_READY
    assert AUDIT_DATA_CORRUPTED in readiness.blocking_reason_codes


@pytest.mark.unit
def test_w113_readiness_not_ready_when_store_unavailable(tmp_path: Path) -> None:
    class DeadStore:
        def scan_documents(self) -> Any:
            raise RuntimeError("boom")

    service = ExecutionAuditService(MemoryExecutionAuditStore())
    service._store = DeadStore()  # type: ignore[assignment]
    snapshot = AuditHealthService(service).snapshot("tenant-A")
    assert snapshot.health.status is AuditOperationalStatus.UNAVAILABLE
    assert snapshot.readiness.readiness_status is AuditReadinessStatus.NOT_READY
    assert AUDIT_STORE_UNAVAILABLE in snapshot.readiness.blocking_reason_codes
    # the cause is a reported code, never an exception
    assert "boom" not in json.dumps(snapshot.to_dict())
    assert snapshot.measured is False


@pytest.mark.unit
def test_w113_reason_codes_are_stable_and_documented() -> None:
    assert BLOCKING_REASON_CODES == frozenset(
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
    # informational codes must never block readiness
    assert AUDIT_JOURNAL_EMPTY not in BLOCKING_REASON_CODES


@pytest.mark.unit
def test_w113_tenant_context_invalid_is_reported_not_raised(tmp_path: Path) -> None:
    service = _seed(tmp_path, count=2)
    for bad in ("", "   ", "tenant/B", "..", "a" * 300):
        snapshot = service.snapshot(bad)
        assert snapshot.readiness.readiness_status is AuditReadinessStatus.NOT_READY
        assert TENANT_CONTEXT_INVALID in snapshot.readiness.blocking_reason_codes
        assert snapshot.health.status is AuditOperationalStatus.UNAVAILABLE


# ── 6. tenant isolation ──────────────────────────────────────────────────


@pytest.mark.unit
def test_w113_tenant_isolation(tmp_path: Path) -> None:
    store = ExecutionAuditStore(tmp_path / "audit")
    for index in range(4):
        store.append(_event(f"a-{index}", tenant_id="tenant-A", request_id=f"a{index}"))
    for index in range(2):
        store.append(_event(f"b-{index}", tenant_id="tenant-B", request_id=f"b{index}"))
    service = ExecutionAuditService(ExecutionAuditStore(tmp_path / "audit"))
    diagnostics = AuditHealthService(service, active_path=tmp_path / "audit")
    a = diagnostics.health("tenant-A")
    b = diagnostics.health("tenant-B")
    assert a.event_count == 4
    assert b.event_count == 2
    assert a.tenant_count == 2, "the store-wide tenant count is an observable fact"
    rendered_a = json.dumps(a.to_dict())
    assert "b-0" not in rendered_a
    assert "tenant-B" not in rendered_a
    assert diagnostics.capacity("tenant-A").event_count == 4
    assert diagnostics.capacity("tenant-B").event_count == 2


# ── 15. determinism ──────────────────────────────────────────────────────


@pytest.mark.unit
def test_w113_projection_is_deterministic(tmp_path: Path) -> None:
    service = _seed(tmp_path, count=5, archived=2)
    first = service.snapshot("tenant-A").to_dict()
    second = service.snapshot("tenant-A").to_dict()
    assert _strip_volatile(first) == _strip_volatile(second)


def _strip_volatile(payload: dict[str, Any]) -> dict[str, Any]:
    """Drop observation timestamps so the comparison is functional."""

    clone = json.loads(json.dumps(payload))
    for section, key in (
        (None, "observed_at"),
        ("health", "checked_at"),
        ("capacity", "measured_at"),
        ("archive", "checked_at"),
        ("archive", "last_verified_at"),
        ("backlog", "measured_at"),
        ("readiness", "checked_at"),
    ):
        target = clone if section is None else clone[section]
        target.pop(key, None)
    return clone


@pytest.mark.unit
def test_w113_deterministic_clock_is_injectable(tmp_path: Path) -> None:
    store = ExecutionAuditStore(tmp_path / "audit")
    store.append(_event("clock", request_id="r1"))
    service = ExecutionAuditService(ExecutionAuditStore(tmp_path / "audit"))
    fixed = AuditHealthService(service, active_path=tmp_path / "audit", clock=lambda: NOW)
    snapshot = fixed.snapshot("tenant-A")
    assert snapshot.observed_at == NOW
    assert snapshot.health.checked_at == NOW
    assert snapshot.capacity.measured_at == NOW


# ── 16. read-only proof + 18. sanitization ────────────────────────────────


@pytest.mark.unit
def test_w113_read_only_proof(tmp_path: Path) -> None:
    service = _seed(tmp_path, count=5, archived=2)
    policy = AuditRetentionPolicy(
        retention_days=30, reference_time=NOW, minimum_events_to_keep=1, dry_run=True
    )
    before_audit = _fingerprint(tmp_path / "audit")
    before_archive = _fingerprint(tmp_path / "archive")
    service.health("tenant-A")
    service.capacity("tenant-A")
    service.readiness("tenant-A")
    service.snapshot("tenant-A", policy=policy)
    assert _fingerprint(tmp_path / "audit") == before_audit
    assert _fingerprint(tmp_path / "archive") == before_archive


@pytest.mark.unit
def test_w113_creates_no_audit_event(tmp_path: Path) -> None:
    service = _seed(tmp_path, count=4, archived=1)
    store = ExecutionAuditStore(tmp_path / "audit")
    before = len(store.scan_documents().events)
    for _ in range(5):
        service.health("tenant-A")
        service.capacity("tenant-A")
        service.readiness("tenant-A")
    after = len(store.scan_documents().events)
    assert after == before, "a diagnostic read must never create an audit event"


@pytest.mark.unit
def test_w113_reports_never_expose_paths_or_secrets(tmp_path: Path) -> None:
    store = ExecutionAuditStore(tmp_path / "audit")
    store.append(_event("leak", request_id="r1"))
    path = next(store.path.glob("*.json"))
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["reason_code"] = "tampered"
    path.write_text(
        json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8"
    )
    service = ExecutionAuditService(ExecutionAuditStore(tmp_path / "audit"))
    snapshot = AuditHealthService(
        service,
        archive_store=FileSystemAuditArchiveStore(tmp_path / "archive"),
        active_path=tmp_path / "audit",
        archive_path=tmp_path / "archive",
    ).snapshot("tenant-A")
    rendered = json.dumps(snapshot.to_dict())
    assert str(tmp_path) not in rendered
    assert path.name not in rendered
    assert ".json" not in rendered
    assert ".lock" not in rendered
    assert "Traceback" not in rendered
    assert "RuntimeError" not in rendered
    assert "boom" not in rendered


# ── 17. no recursion ─────────────────────────────────────────────────────


@pytest.mark.unit
def test_w113_diagnostics_do_not_feed_themselves(tmp_path: Path) -> None:
    """health -> count must not create an event that changes the count."""

    service = _seed(tmp_path, count=6, archived=2)
    store = ExecutionAuditStore(tmp_path / "audit")
    first = AuditHealthService(service, active_path=tmp_path / "audit").health("tenant-A")
    baseline = len(store.scan_documents().events)
    for _ in range(20):
        AuditHealthService(service, active_path=tmp_path / "audit").health("tenant-A")
    last = AuditHealthService(service, active_path=tmp_path / "audit").health("tenant-A")
    assert first.event_count == last.event_count
    assert (
        len(store.scan_documents().events) == baseline
    ), "20 further diagnostic reads must add no audit event"


# ── HTTP contract ────────────────────────────────────────────────────────


def _auth_headers() -> dict[str, str]:
    return {"Authorization": f"Bearer {settings.trendx_api_token.get_secret_value()}"}


@pytest.mark.unit
def test_w113_api_requires_authentication() -> None:
    with TestClient(app) as client:
        for path in HEALTH_PATHS:
            assert client.get(path).status_code == 401
            assert client.get(path, headers={"Authorization": "Bearer wrong"}).status_code == 401


@pytest.mark.unit
def test_w113_api_endpoints(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr(settings, "trendx_api_token", SecretStr("w113-unit-token"))
    service = _seed(tmp_path, count=5, archived=2)
    previous = app.state.execution_audit_health_service_factory
    app.state.execution_audit_health_service_factory = lambda: service
    app.dependency_overrides[get_execution_audit_health_service] = lambda: service
    app.dependency_overrides[get_tenant_context] = lambda: TenantContext("tenant-A")
    try:
        with TestClient(app) as client:
            before = _fingerprint(tmp_path / "audit")
            health = client.get(HEALTH_PATHS[0], headers=_auth_headers())
            assert health.status_code == 200
            assert health.json()["status"] == "HEALTHY"
            assert health.json()["event_count"] == 6

            capacity = client.get(HEALTH_PATHS[1], headers=_auth_headers())
            assert capacity.status_code == 200
            assert capacity.json()["archived_event_count"] == 2
            assert capacity.json()["total_known_event_count"] == 6
            assert capacity.json()["active_bytes"] > 0
            assert capacity.json()["filesystem"]["available"] is True

            readiness = client.get(HEALTH_PATHS[2], headers=_auth_headers())
            assert readiness.status_code == 200
            assert readiness.json()["readiness_status"] == "READY"
            assert readiness.json()["blocking_reason_codes"] == []
            # read-only proof over HTTP
            assert _fingerprint(tmp_path / "audit") == before
            for response in (health, capacity, readiness):
                assert settings.trendx_api_token.get_secret_value() not in response.text
                assert str(tmp_path) not in response.text
                assert "Traceback" not in response.text
    finally:
        app.state.execution_audit_health_service_factory = previous
        app.dependency_overrides.pop(get_execution_audit_health_service, None)
        app.dependency_overrides.pop(get_tenant_context, None)


@pytest.mark.unit
def test_w113_api_degraded_is_not_an_http_error(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(settings, "trendx_api_token", SecretStr("w113-degraded-token"))
    store = ExecutionAuditStore(tmp_path / "audit")
    store.append(_event("bad", request_id="r1"))
    path = next(store.path.glob("*.json"))
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["reason_code"] = "tampered"
    path.write_text(
        json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8"
    )
    service = ExecutionAuditService(ExecutionAuditStore(tmp_path / "audit"))
    diagnostics = AuditHealthService(service, active_path=tmp_path / "audit")
    previous = app.state.execution_audit_health_service_factory
    app.state.execution_audit_health_service_factory = lambda: diagnostics
    app.dependency_overrides[get_execution_audit_health_service] = lambda: diagnostics
    app.dependency_overrides[get_tenant_context] = lambda: TenantContext("tenant-A")
    try:
        with TestClient(app) as client:
            health = client.get(HEALTH_PATHS[0], headers=_auth_headers())
            assert health.status_code == 200, "DEGRADED is a diagnostic, not an HTTP error"
            assert health.json()["status"] == "DEGRADED"
            readiness = client.get(HEALTH_PATHS[2], headers=_auth_headers())
            assert readiness.status_code == 200
            assert readiness.json()["readiness_status"] == "NOT_READY"
    finally:
        app.state.execution_audit_health_service_factory = previous
        app.dependency_overrides.pop(get_execution_audit_health_service, None)
        app.dependency_overrides.pop(get_tenant_context, None)


@pytest.mark.unit
def test_w113_api_tenant_isolation(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr(settings, "trendx_api_token", SecretStr("w113-tenant-token"))
    store = ExecutionAuditStore(tmp_path / "audit")
    for index in range(3):
        store.append(_event(f"a-{index}", tenant_id="tenant-A", request_id=f"a{index}"))
    store.append(_event("b-0", tenant_id="tenant-B", request_id="b0"))
    service = ExecutionAuditService(ExecutionAuditStore(tmp_path / "audit"))
    diagnostics = AuditHealthService(service, active_path=tmp_path / "audit")
    previous = app.state.execution_audit_health_service_factory
    app.state.execution_audit_health_service_factory = lambda: diagnostics
    app.dependency_overrides[get_execution_audit_health_service] = lambda: diagnostics
    app.dependency_overrides[get_tenant_context] = lambda: TenantContext("tenant-A")
    try:
        with TestClient(app) as client:
            for path in HEALTH_PATHS:
                body = client.get(path, headers=_auth_headers()).text
                assert "b-0" not in body
                assert "tenant-B" not in body
            capacity = client.get(HEALTH_PATHS[1], headers=_auth_headers())
            assert capacity.json()["event_count"] == 3
    finally:
        app.state.execution_audit_health_service_factory = previous
        app.dependency_overrides.pop(get_execution_audit_health_service, None)
        app.dependency_overrides.pop(get_tenant_context, None)


@pytest.mark.unit
def test_w113_api_unavailable_backend(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(settings, "trendx_api_token", SecretStr("w113-unavailable-token"))
    previous = app.state.execution_audit_health_service_factory
    app.state.execution_audit_health_service_factory = lambda: None
    app.dependency_overrides[get_tenant_context] = lambda: TenantContext("tenant-A")
    try:
        with TestClient(app) as client:
            for path in HEALTH_PATHS:
                response = client.get(path, headers=_auth_headers())
                assert response.status_code == 503, path
                assert response.json()["detail"] == "audit_health_unavailable"
    finally:
        app.state.execution_audit_health_service_factory = previous
        app.dependency_overrides.pop(get_tenant_context, None)


@pytest.mark.unit
def test_w113_api_auth_not_configured(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(settings, "trendx_api_token", SecretStr("CHANGE_ME"))
    with TestClient(app) as client:
        response = client.get(HEALTH_PATHS[0], headers={"Authorization": "Bearer CHANGE_ME"})
        assert response.status_code == 503
        assert response.json()["detail"] == "audit_authentication_not_configured"


@pytest.mark.unit
def test_w113_api_is_get_only_and_openapi_documented() -> None:
    schema = app.openapi()
    for path in HEALTH_PATHS:
        verbs = {verb.upper() for verb in schema["paths"][path]}
        assert verbs == {"GET"}, (path, verbs)
        operation = schema["paths"][path]["get"]
        assert set(operation["responses"]) >= {"200", "401", "403", "422", "503"}
        assert operation["security"] == [{"BearerAuth": []}, {"ApiKeyAuth": []}]
    assert "AuditHealthOut" in schema["components"]["schemas"]
    assert "AuditCapacityOut" in schema["components"]["schemas"]
    assert "AuditReadinessOut" in schema["components"]["schemas"]
    # no DELETE/POST anywhere on the health surface
    for path in HEALTH_PATHS:
        assert "delete" not in {verb.lower() for verb in schema["paths"][path]}
        assert "post" not in {verb.lower() for verb in schema["paths"][path]}


@pytest.mark.unit
def test_w113_health_routes_do_not_duplicate_the_audit_api() -> None:
    schema = app.openapi()
    paths = {path for path in schema["paths"] if "/audit" in path}
    health = {path for path in paths if path.endswith(("/health", "/capacity", "/readiness"))}
    assert health == set(HEALTH_PATHS)
    # a consolidated /operational-status route is deliberately not added: the
    # three projections are served from one snapshot, and a fourth route would
    # be redundant
    assert not any(path.endswith("/operational-status") for path in paths)
