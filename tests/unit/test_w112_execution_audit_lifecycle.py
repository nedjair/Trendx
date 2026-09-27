"""W112 — audit lifecycle, retention and archive unit tests."""

from __future__ import annotations

import hashlib
import json
import stat
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient
from pydantic import SecretStr
from trendx.config import settings
from trendx.forecasting.api import (
    TenantContext,
    get_execution_audit_lifecycle_service,
    get_tenant_context,
)
from trendx.forecasting.audit import (
    AUDIT_EVENT_VERSION,
    AuditActorType,
    AuditDeleteOutcome,
    AuditError,
    AuditOperation,
    AuditOutcome,
    ExecutionAuditEvent,
    ExecutionAuditService,
    ExecutionAuditStore,
    MemoryExecutionAuditStore,
)
from trendx.forecasting.audit_control import (
    AuditExportScope,
    ExecutionAuditControlService,
    ExecutionAuditExportService,
    ExecutionAuditIntegrityService,
    ExecutionAuditReconciliationService,
)
from trendx.forecasting.audit_lifecycle import (
    AUDIT_ARCHIVE_VERSION,
    AUDIT_LIFECYCLE_SOURCE,
    AuditArchiveDocument,
    AuditArchiveError,
    AuditArchiveIntegrityError,
    AuditLifecycleService,
    AuditRestoreRequest,
    AuditRestoreStatus,
    AuditRetentionPolicy,
    AuditRetentionPolicyError,
    FileSystemAuditArchiveStore,
    LifecycleRecursionError,
    LifecycleStatus,
)
from trendx.main import app

NOW = datetime(2026, 4, 1, tzinfo=UTC)
ROOT = Path(__file__).resolve().parents[2]
LIFECYCLE_PATHS = (
    "/api/v1/forecast/executions/audit/lifecycle/preview",
    "/api/v1/forecast/executions/audit/lifecycle/archive",
    "/api/v1/forecast/executions/audit/lifecycle/purge",
    "/api/v1/forecast/executions/audit/lifecycle/restore",
)


def _event(
    event_id: str,
    *,
    tenant_id: str = "tenant-A",
    execution_id: str = "exec-A",
    request_id: str = "request-A",
    occurred_at: datetime = NOW,
    operation: AuditOperation = AuditOperation.EXECUTION,
    outcome: AuditOutcome = AuditOutcome.SUCCESS,
    reason_code: str = "record_restored",
) -> ExecutionAuditEvent:
    return ExecutionAuditEvent(
        event_id=event_id,
        occurred_at=occurred_at,
        operation=operation,
        outcome=outcome,
        tenant_id=tenant_id,
        execution_id=execution_id,
        reference_key=f"ref:{execution_id}",
        actor_type=AuditActorType.SYSTEM,
        actor_id="system",
        request_id=request_id,
        source="w112-test",
        reason_code=reason_code,
    )


def _policy(
    *,
    retention_days: int = 30,
    reference_time: datetime = NOW,
    minimum_events_to_keep: int = 0,
    archive_before_purge: bool = True,
    dry_run: bool = False,
) -> AuditRetentionPolicy:
    return AuditRetentionPolicy(
        retention_days=retention_days,
        reference_time=reference_time,
        minimum_events_to_keep=minimum_events_to_keep,
        archive_before_purge=archive_before_purge,
        dry_run=dry_run,
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
    old: int = 5,
    new: int = 3,
    tenant_id: str = "tenant-A",
) -> tuple[ExecutionAuditStore, FileSystemAuditArchiveStore, AuditLifecycleService]:
    store = ExecutionAuditStore(tmp_path / "audit")
    for index in range(old):
        store.append(
            _event(
                f"old-{index}",
                tenant_id=tenant_id,
                request_id=f"old-r{index}",
                occurred_at=NOW - timedelta(days=100 + index),
            )
        )
    for index in range(new):
        store.append(
            _event(
                f"new-{index}",
                tenant_id=tenant_id,
                request_id=f"new-r{index}",
                occurred_at=NOW - timedelta(days=1),
            )
        )
    service = ExecutionAuditService(ExecutionAuditStore(tmp_path / "audit"))
    archive = FileSystemAuditArchiveStore(tmp_path / "archive")
    return store, archive, AuditLifecycleService(service, archive)


# ── A. Retention policy ──────────────────────────────────────────────────


@pytest.mark.unit
def test_w112_retention_policy_contract() -> None:
    policy = _policy()
    assert policy.retention_days == 30
    assert policy.reference_time == NOW
    assert policy.reference_time.tzinfo is UTC
    assert policy.eligible_at == NOW - timedelta(days=30)
    assert policy.archive_before_purge is True
    assert policy.minimum_events_to_keep == 0


@pytest.mark.unit
def test_w112_retention_policy_defaults_to_dry_run() -> None:
    policy = AuditRetentionPolicy(retention_days=1, reference_time=NOW)
    assert policy.dry_run is True


@pytest.mark.unit
@pytest.mark.parametrize(
    "kwargs",
    [
        {"retention_days": 0},
        {"retention_days": -1},
        {"retention_days": True},
        {"retention_days": 1, "minimum_events_to_keep": -1},
        {"retention_days": 1, "minimum_events_to_keep": True},
        {"retention_days": 1, "dry_run": "yes"},
        {"retention_days": 1, "archive_before_purge": 1},
    ],
)
def test_w112_retention_policy_rejects_invalid(kwargs: dict[str, Any]) -> None:
    base = {"reference_time": NOW, **kwargs}
    with pytest.raises(AuditRetentionPolicyError):
        AuditRetentionPolicy(**base)  # type: ignore[arg-type]


@pytest.mark.unit
def test_w112_retention_policy_requires_reference_time() -> None:
    with pytest.raises(TypeError):
        AuditRetentionPolicy(retention_days=1)  # type: ignore[call-arg]


@pytest.mark.unit
def test_w112_retention_policy_normalises_to_utc() -> None:
    local = datetime(2026, 4, 1, 12, 0, 0)  # naive, no implicit local time
    policy = AuditRetentionPolicy(retention_days=1, reference_time=local)
    assert policy.reference_time == datetime(2026, 4, 1, 12, 0, tzinfo=UTC)
    offset = datetime(2026, 4, 1, 12, 0, tzinfo=timezone(timedelta(hours=5)))
    policy_offset = AuditRetentionPolicy(retention_days=1, reference_time=offset)
    assert policy_offset.reference_time == datetime(2026, 4, 1, 7, 0, tzinfo=UTC)


@pytest.mark.unit
def test_w112_eligibility_boundary_is_inclusive() -> None:
    policy = _policy(retention_days=30, reference_time=NOW)
    boundary = policy.eligible_at
    assert policy.is_eligible(boundary) is True
    assert policy.is_eligible(boundary - timedelta(microseconds=1)) is True
    assert policy.is_eligible(boundary + timedelta(microseconds=1)) is False


@pytest.mark.unit
def test_w112_retention_policy_round_trip() -> None:
    policy = _policy(minimum_events_to_keep=2)
    restored = AuditRetentionPolicy.from_dict(policy.to_dict())
    assert restored.to_dict() == policy.to_dict()


@pytest.mark.unit
def test_w112_retention_policy_rejects_unknown_fields() -> None:
    with pytest.raises(AuditRetentionPolicyError):
        AuditRetentionPolicy.from_dict(
            {"retention_days": 1, "reference_time": NOW.isoformat(), "unknown": 1}
        )


# ── B. Preview (read-only) ───────────────────────────────────────────────


@pytest.mark.unit
def test_w112_preview_is_read_only(tmp_path: Path) -> None:
    _, _, service = _seed(tmp_path)
    before = _fingerprint(tmp_path / "audit")
    archive_before = _fingerprint(tmp_path / "archive")
    report = service.preview("tenant-A", _policy(minimum_events_to_keep=3, dry_run=True))
    assert report.status is LifecycleStatus.DRY_RUN
    assert report.scanned == 8
    assert report.eligible == 5
    assert report.protected == 3
    assert report.invalid == 0
    assert report.already_archived == 0
    assert report.archive_candidates == 5
    assert report.purge_candidates == 5
    assert _fingerprint(tmp_path / "audit") == before
    assert _fingerprint(tmp_path / "archive") == archive_before
    assert report.to_dict()["policy"]["dry_run"] is True


@pytest.mark.unit
def test_w112_preview_reports_invalid_documents(tmp_path: Path) -> None:
    store, _, service = _seed(tmp_path)
    victim = next(store.path.glob("*.json"))
    payload = json.loads(victim.read_text(encoding="utf-8"))
    payload["reason_code"] = "tampered"
    victim.write_text(
        json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8"
    )
    report = service.preview("tenant-A", _policy(dry_run=True))
    assert report.invalid == 1
    assert report.status is LifecycleStatus.DRY_RUN


@pytest.mark.unit
def test_w112_minimum_events_to_keep_protects_newest(tmp_path: Path) -> None:
    _, _, service = _seed(tmp_path, old=5, new=3)
    report = service.preview("tenant-A", _policy(minimum_events_to_keep=3, dry_run=True))
    assert report.protected == 3
    assert report.eligible == 5
    # a keep larger than the trail protects everything
    everything = service.preview("tenant-A", _policy(minimum_events_to_keep=100, dry_run=True))
    assert everything.protected == 8
    assert everything.eligible == 0


@pytest.mark.unit
def test_w112_preview_warns_when_minimum_exceeds_trail(tmp_path: Path) -> None:
    _, _, service = _seed(tmp_path, old=1, new=1)
    report = service.preview("tenant-A", _policy(minimum_events_to_keep=50, dry_run=True))
    assert "minimum_exceeds_available" in report.warnings


# ── C. Archive ───────────────────────────────────────────────────────────


@pytest.mark.unit
def test_w112_archive_preserves_the_full_w110_contract(tmp_path: Path) -> None:
    store, archive, service = _seed(tmp_path, old=3, new=0)
    report = service.archive("tenant-A", _policy(dry_run=False))
    assert report.status is LifecycleStatus.SUCCESS
    assert report.archived == 3
    assert report.archive_verified == 3
    documents = archive.list_documents(tenant_id="tenant-A")
    assert len(documents) == 3
    for document in documents:
        payload = document.to_dict()
        assert payload["archive_version"] == AUDIT_ARCHIVE_VERSION
        assert "archived_at" in payload
        assert "original_event_checksum" in payload
        assert "archive_document_checksum" in payload
        event = payload["event"]
        for field in (
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
        ):
            assert field in event, field
        assert event["event_version"] == AUDIT_EVENT_VERSION
        assert event["checksum"] == payload["original_event_checksum"]
    assert store is not None


@pytest.mark.unit
def test_w112_archive_is_idempotent(tmp_path: Path) -> None:
    _, archive, service = _seed(tmp_path, old=3, new=0)
    first = service.archive("tenant-A", _policy(dry_run=False))
    assert first.archived == 3
    second = service.archive("tenant-A", _policy(dry_run=False))
    assert second.archived == 0
    assert second.skipped == 3
    assert len(archive.list_documents()) == 3


@pytest.mark.unit
def test_w112_archive_document_is_canonical_and_checksummed(tmp_path: Path) -> None:
    _, archive, service = _seed(tmp_path, old=2, new=0)
    service.archive("tenant-A", _policy(dry_run=False))
    document = archive.list_documents()[0]
    assert document is not None
    raw = (archive.path / archive._filename(document.event_id)).read_text(encoding="utf-8")
    assert raw.endswith("\n")
    parsed = json.loads(raw)
    assert list(parsed) == sorted(parsed), "archive document must use canonical key order"
    assert (
        document.archive_document_checksum
        == hashlib.sha256(document.canonical_payload()).hexdigest()
    )
    assert len(document.archive_document_checksum) == 64


@pytest.mark.unit
def test_w112_archive_permissions_are_tight(tmp_path: Path) -> None:
    _, archive, _ = _seed(tmp_path, old=1, new=0)
    assert stat.S_IMODE(archive.path.stat().st_mode) == 0o700
    for path in archive.path.glob("*.json"):
        assert stat.S_IMODE(path.stat().st_mode) == 0o600


@pytest.mark.unit
def test_w112_archive_is_immutable_after_publication(tmp_path: Path) -> None:
    store, archive, service = _seed(tmp_path, old=1, new=0)
    service.archive("tenant-A", _policy(dry_run=False))
    path = next(archive.path.glob("*.json"))
    before_bytes = path.read_bytes()
    before_stat = path.stat()
    # a second archive run must not rewrite the document
    service.archive("tenant-A", _policy(dry_run=False))
    assert path.read_bytes() == before_bytes
    assert path.stat().st_mtime_ns == before_stat.st_mtime_ns
    assert store is not None


@pytest.mark.unit
def test_w112_archive_temporary_files_are_never_valid(tmp_path: Path) -> None:
    _, archive, service = _seed(tmp_path, old=1, new=0)
    stray = archive.path / ".deadbeef.tmp"
    stray.write_text("{not json", encoding="utf-8")
    assert list(archive.path.glob("*.json")) == []
    # a stray temp file is ignored by listing and cannot be read as a document
    assert archive.list_documents() == ()
    assert service.archive("tenant-A", _policy(dry_run=False)).archived == 1
    # the store leaves no temporary of its own behind
    assert not [p for p in archive.path.glob("*.tmp") if p.name != ".deadbeef.tmp"]


@pytest.mark.unit
def test_w112_archive_rejects_conflicting_content(tmp_path: Path) -> None:
    store = ExecutionAuditStore(tmp_path / "audit")
    store.append(_event("shared", request_id="r1"))
    archive = FileSystemAuditArchiveStore(tmp_path / "archive")
    event = store.scan_documents().events[0]
    archive.put(
        AuditArchiveDocument(
            event=event,
            archive_version=AUDIT_ARCHIVE_VERSION,
            archived_at=NOW,
            original_event_checksum=event.checksum or "",
        )
    )
    # corrupt the published document, then republish the honest one
    path = archive.path / archive._filename(event.event_id)
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["event"]["reason_code"] = "corrupted"
    path.write_text(
        json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8"
    )
    with pytest.raises(AuditArchiveError):
        archive.put(
            AuditArchiveDocument(
                event=event,
                archive_version=AUDIT_ARCHIVE_VERSION,
                archived_at=NOW + timedelta(days=1),
                original_event_checksum=event.checksum or "",
            )
        )


# ── D. Archive integrity ─────────────────────────────────────────────────


@pytest.mark.unit
@pytest.mark.parametrize(
    "mutate",
    ["checksum", "canonical", "event_checksum", "missing_field", "extra_field", "version"],
)
def test_w112_archive_corruption_is_refused(tmp_path: Path, mutate: str) -> None:
    store, archive, service = _seed(tmp_path, old=1, new=0)
    service.archive("tenant-A", _policy(dry_run=False))
    path = next(archive.path.glob("*.json"))
    payload = json.loads(path.read_text(encoding="utf-8"))
    if mutate == "checksum":
        payload["archive_document_checksum"] = "0" * 64
    elif mutate == "canonical":
        path.write_text(json.dumps(payload, sort_keys=False) + "\n", encoding="utf-8")
        path.chmod(0o600)
        with pytest.raises(AuditArchiveIntegrityError):
            archive.list_documents()
        return
    elif mutate == "event_checksum":
        payload["event"]["checksum"] = "1" * 64
    elif mutate == "missing_field":
        payload.pop("original_event_checksum")
    elif mutate == "extra_field":
        payload["unexpected"] = "value"
    elif mutate == "version":
        payload["archive_version"] = 99
    path.write_text(
        json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8"
    )
    with pytest.raises(AuditArchiveError):
        archive.list_documents()


@pytest.mark.unit
def test_w112_archive_verify_reports_failure_codes(tmp_path: Path) -> None:
    _, archive, service = _seed(tmp_path, old=1, new=0)
    service.archive("tenant-A", _policy(dry_run=False))
    event_id = archive.list_documents()[0].event_id
    assert archive.verify(event_id).verified is True
    assert archive.verify("does-not-exist").error_code == "archive_missing"
    assert archive.verify(event_id, tenant_id="tenant-B").error_code == "tenant_mismatch"
    path = next(archive.path.glob("*.json"))
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["archive_document_checksum"] = "0" * 64
    path.write_text(
        json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8"
    )
    verdict = archive.verify(event_id)
    assert verdict.verified is False
    assert verdict.error_code == "archive_corrupt"


@pytest.mark.unit
def test_w112_archive_symlink_and_permissions_are_refused(tmp_path: Path) -> None:
    _, archive, service = _seed(tmp_path, old=1, new=0)
    service.archive("tenant-A", _policy(dry_run=False))
    path = next(archive.path.glob("*.json"))
    outside = tmp_path / "outside.json"
    outside.write_text(path.read_text(encoding="utf-8"), encoding="utf-8")
    path.unlink()
    path.symlink_to(outside)
    with pytest.raises(AuditArchiveIntegrityError):
        archive.list_documents()
    path.unlink()
    path.write_text(outside.read_text(encoding="utf-8"), encoding="utf-8")
    path.chmod(0o644)
    with pytest.raises(AuditArchiveIntegrityError):
        archive.list_documents()


@pytest.mark.unit
def test_w112_archive_directory_symlink_is_refused(tmp_path: Path) -> None:
    real = tmp_path / "real"
    real.mkdir()
    link = tmp_path / "link"
    link.symlink_to(real)
    with pytest.raises(AuditArchiveError):
        FileSystemAuditArchiveStore(link)


# ── E. Purge safety ──────────────────────────────────────────────────────


@pytest.mark.unit
def test_w112_purge_archives_verifies_then_deletes(tmp_path: Path) -> None:
    store, archive, service = _seed(tmp_path, old=4, new=2)
    purged_ids = {f"old-{index}" for index in range(4)}
    report = service.purge("tenant-A", _policy(dry_run=False))
    assert report.status is LifecycleStatus.SUCCESS
    assert report.purged == 4
    assert report.archived == 4
    assert report.archive_verified == 4
    assert len(archive.list_documents()) == 4
    remaining = {event.event_id for event in store.scan_documents().events}
    assert {"new-0", "new-1"} <= remaining
    assert not (purged_ids & remaining)


@pytest.mark.unit
def test_w112_purge_without_verified_archive_never_deletes(tmp_path: Path) -> None:
    store, archive, service = _seed(tmp_path, old=3, new=0)

    class BlindArchive(FileSystemAuditArchiveStore):
        def verify(self, *args: Any, **kwargs: Any) -> Any:
            return type(
                "V",
                (),
                {
                    "verified": False,
                    "error_code": "archive_corrupt",
                    "event_id": "",
                    "tenant_id": None,
                    "checksum": None,
                },
            )()

    blind = BlindArchive(tmp_path / "archive")
    blind_service = AuditLifecycleService(
        ExecutionAuditService(ExecutionAuditStore(tmp_path / "audit")), blind
    )
    report = blind_service.purge("tenant-A", _policy(dry_run=False))
    assert report.purged == 0
    assert report.integrity_failures == 3
    # the three business events survive; only the purge audit event was added
    survivors = {e.event_id for e in store.scan_documents().events}
    assert {"old-0", "old-1", "old-2"} <= survivors
    assert archive is not None


@pytest.mark.unit
def test_w112_purge_refused_when_archive_before_purge_disabled(tmp_path: Path) -> None:
    store, _, service = _seed(tmp_path, old=3, new=0)
    report = service.purge("tenant-A", _policy(archive_before_purge=False, dry_run=False))
    assert report.purged == 0
    assert report.guards.get("archive_missing") == 3
    survivors = {e.event_id for e in store.scan_documents().events}
    assert {"old-0", "old-1", "old-2"} <= survivors
    assert list(FileSystemAuditArchiveStore(tmp_path / "archive").path.glob("*.json")) == []


@pytest.mark.unit
def test_w112_purge_dry_run_is_read_only(tmp_path: Path) -> None:
    store, archive, service = _seed(tmp_path, old=4, new=0)
    before = _fingerprint(tmp_path / "audit")
    report = service.purge("tenant-A", _policy(dry_run=True))
    assert report.status is LifecycleStatus.DRY_RUN
    assert report.purged == 0
    assert _fingerprint(tmp_path / "audit") == before
    assert list(archive.path.glob("*.json")) == []


@pytest.mark.unit
def test_w112_purge_skips_invalid_events_and_never_removes_them(tmp_path: Path) -> None:
    store, _, service = _seed(tmp_path, old=3, new=0)
    victim = next(store.path.glob("*.json"))
    tampered_id = json.loads(victim.read_text(encoding="utf-8"))["event_id"]
    payload = json.loads(victim.read_text(encoding="utf-8"))
    payload["reason_code"] = "tampered"
    victim.write_text(
        json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8"
    )
    report = service.purge("tenant-A", _policy(dry_run=False))
    # the corrupt document is skipped, the healthy ones are purged
    assert report.integrity_failures == 0
    assert report.purged == 2
    remaining = list(store.path.glob("*.json"))
    assert len(remaining) == 1
    assert json.loads(remaining[0].read_text(encoding="utf-8"))["event_id"] == tampered_id


# ── F. Compare-and-delete ────────────────────────────────────────────────


@pytest.mark.unit
def test_w112_compare_and_delete_removes_only_the_expected_event(tmp_path: Path) -> None:
    store = ExecutionAuditStore(tmp_path / "audit")
    store.append(_event("target", request_id="r1"))
    store.append(_event("bystander", request_id="r2"))
    expected = store.get("target")
    assert expected is not None
    result = store.delete_if_unchanged(expected)
    assert result.outcome is AuditDeleteOutcome.DELETED
    assert result.deleted is True
    assert store.get("target") is None
    assert store.get("bystander") is not None, "only the compared event may be removed"


@pytest.mark.unit
def test_w112_compare_and_delete_detects_concurrent_modification(tmp_path: Path) -> None:
    store = ExecutionAuditStore(tmp_path / "audit")
    store.append(_event("racy", request_id="r1"))
    expected = store.get("racy")
    assert expected is not None
    # another writer replaces the very same document with different content
    path = store._event_path("racy")
    replacement = ExecutionAuditEvent(
        event_id="racy",
        occurred_at=NOW,
        operation=AuditOperation.EXECUTION,
        outcome=AuditOutcome.SUCCESS,
        tenant_id="tenant-A",
        execution_id="exec-A",
        reference_key="ref:exec-A",
        actor_type=AuditActorType.SYSTEM,
        actor_id="system",
        request_id="request-A",
        source="w112-test",
        reason_code="swapped",
    ).with_checksum()
    store._atomic_write(path, replacement)
    result = store.delete_if_unchanged(expected)
    assert result.outcome is AuditDeleteOutcome.CONFLICT
    assert store.get("racy") is not None, "a conflicting event must never be removed"


@pytest.mark.unit
def test_w112_compare_and_delete_reports_not_found(tmp_path: Path) -> None:
    store = ExecutionAuditStore(tmp_path / "audit")
    missing = _event("ghost", request_id="r1").with_checksum()
    result = store.delete_if_unchanged(missing)
    assert result.outcome is AuditDeleteOutcome.NOT_FOUND


@pytest.mark.unit
def test_w112_compare_and_delete_rejects_unsigned_event(tmp_path: Path) -> None:
    store = ExecutionAuditStore(tmp_path / "audit")
    store.append(_event("unsigned", request_id="r1"))
    unsigned = ExecutionAuditEvent(
        event_id="unsigned",
        occurred_at=NOW,
        operation=AuditOperation.EXECUTION,
        outcome=AuditOutcome.SUCCESS,
        tenant_id="tenant-A",
        execution_id="exec-A",
        reference_key="ref:a",
        actor_type=AuditActorType.SYSTEM,
        actor_id="system",
        request_id="r1",
        source="w112",
        reason_code="ok",
    )
    with pytest.raises(AuditError):
        store.delete_if_unchanged(unsigned)


@pytest.mark.unit
def test_w112_purge_conflict_leaves_the_event(tmp_path: Path) -> None:
    store, _, _ = _seed(tmp_path, old=2, new=0)
    # build a service that uses THIS store object so the patch is effective
    service = AuditLifecycleService(
        ExecutionAuditService(store), FileSystemAuditArchiveStore(tmp_path / "archive")
    )
    original_delete = store.delete_if_unchanged
    calls = {"n": 0}
    assert original_delete is not None

    def conflicting(expected: ExecutionAuditEvent) -> Any:
        calls["n"] += 1
        return type(
            "R",
            (),
            {
                "outcome": AuditDeleteOutcome.CONFLICT,
                "event_id": expected.event_id,
                "tenant_id": expected.tenant_id,
                "to_dict": lambda self: {},
            },
        )()

    object.__setattr__(store, "delete_if_unchanged", conflicting)
    report = service.purge("tenant-A", _policy(dry_run=False))
    assert calls["n"] == 2
    assert report.purged == 0
    assert report.conflicts == 2
    assert report.status is LifecycleStatus.CONFLICT
    survivors = {e.event_id for e in store.scan_documents().events}
    assert {"old-0", "old-1"} <= survivors, "a conflicting event must survive"


# ── G. Restore ───────────────────────────────────────────────────────────


@pytest.mark.unit
def test_w112_restore_round_trip(tmp_path: Path) -> None:
    store, archive, service = _seed(tmp_path, old=3, new=1)
    service.purge("tenant-A", _policy(dry_run=False))
    archived_id = archive.list_documents()[0].event_id
    assert store.get(archived_id) is None
    result = service.restore(
        AuditRestoreRequest(event_id=archived_id, tenant_id="tenant-A"),
        authenticated_tenant="tenant-A",
    )
    assert result.status is AuditRestoreStatus.RESTORED
    restored = store.get(archived_id)
    assert restored is not None
    assert restored.tenant_id == "tenant-A"


@pytest.mark.unit
def test_w112_restore_already_present_and_conflict(tmp_path: Path) -> None:
    store, archive, service = _seed(tmp_path, old=1, new=0)
    service.purge("tenant-A", _policy(dry_run=False))
    archived_id = archive.list_documents()[0].event_id
    first = service.restore(
        AuditRestoreRequest(event_id=archived_id, tenant_id="tenant-A"),
        authenticated_tenant="tenant-A",
    )
    assert first.status is AuditRestoreStatus.RESTORED
    again = service.restore(
        AuditRestoreRequest(event_id=archived_id, tenant_id="tenant-A"),
        authenticated_tenant="tenant-A",
    )
    assert again.status is AuditRestoreStatus.ALREADY_PRESENT
    assert again.reason_code == "record_already_present"
    # a different active event with the same id is a conflict, never an overwrite
    other = ExecutionAuditStore(tmp_path / "audit")
    document = archive.get(archived_id)
    assert document is not None
    conflict_event = ExecutionAuditEvent(
        event_id=document.event.event_id,
        occurred_at=document.event.occurred_at,
        operation=document.event.operation,
        outcome=document.event.outcome,
        tenant_id=document.event.tenant_id,
        execution_id="exec-different",
        reference_key=document.event.reference_key,
        actor_type=document.event.actor_type,
        actor_id=document.event.actor_id,
        request_id=document.event.request_id,
        source=document.event.source,
        reason_code=document.event.reason_code,
    ).with_checksum()
    path = other._event_path(archived_id)
    other._atomic_write(path, conflict_event)
    conflict = service.restore(
        AuditRestoreRequest(event_id=archived_id, tenant_id="tenant-A"),
        authenticated_tenant="tenant-A",
    )
    assert conflict.status is AuditRestoreStatus.CONFLICT
    assert other.get(archived_id) is not None
    assert other.get(archived_id).execution_id == "exec-different"  # type: ignore[union-attr]


@pytest.mark.unit
def test_w112_restore_missing_and_corrupt(tmp_path: Path) -> None:
    _, archive, service = _seed(tmp_path, old=1, new=0)
    service.archive("tenant-A", _policy(dry_run=False))
    missing = service.restore(
        AuditRestoreRequest(event_id="nope", tenant_id="tenant-A"),
        authenticated_tenant="tenant-A",
    )
    assert missing.status is AuditRestoreStatus.NOT_FOUND
    document = archive.list_documents()[0]
    path = archive.path / archive._filename(document.event_id)
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["archive_document_checksum"] = "0" * 64
    path.write_text(
        json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8"
    )
    event_id = payload["event"]["event_id"]
    corrupt = service.restore(
        AuditRestoreRequest(event_id=event_id, tenant_id="tenant-A"),
        authenticated_tenant="tenant-A",
    )
    assert corrupt.status in {AuditRestoreStatus.INTEGRITY_FAILURE, AuditRestoreStatus.NOT_FOUND}


# ── H. Tenant isolation ──────────────────────────────────────────────────


@pytest.mark.unit
def test_w112_tenant_isolation_on_every_operation(tmp_path: Path) -> None:
    store, archive, service = _seed(tmp_path, old=3, new=0)
    service.archive("tenant-A", _policy(dry_run=False))
    tenant_b = ExecutionAuditStore(tmp_path / "audit")
    tenant_b.append(_event("b-only", tenant_id="tenant-B", execution_id="exec-B", request_id="b1"))
    service_b = AuditLifecycleService(
        ExecutionAuditService(ExecutionAuditStore(tmp_path / "audit")), archive
    )
    # A cannot see or act on B
    report_a = service.preview("tenant-A", _policy(dry_run=True))
    assert report_a.eligible == 3, "tenant A sees only its own eligible events"
    assert "b-only" not in json.dumps(report_a.to_dict())
    report_b = service_b.preview("tenant-B", _policy(dry_run=True))
    assert report_b.scanned == 1
    b_event = archive.list_documents(tenant_id="tenant-B")
    assert b_event == ()
    denied = service.restore(
        AuditRestoreRequest(event_id="b-only", tenant_id="tenant-B"),
        authenticated_tenant="tenant-A",
    )
    assert denied.status is AuditRestoreStatus.TENANT_FORBIDDEN
    assert store.get("b-only") is not None
    assert "tenant-B" not in json.dumps(archive.list_documents(tenant_id="tenant-A")[0].to_dict())


@pytest.mark.unit
def test_w112_tenant_validation_rejects_escapes(tmp_path: Path) -> None:
    _, _, service = _seed(tmp_path, old=1, new=0)
    for bad in ("", "   ", "tenant/B", "..", "a/../b", "a" * 300):
        with pytest.raises(AuditError):
            service.preview(bad, _policy(dry_run=True))


# ── I. W110 / W111 compatibility ─────────────────────────────────────────


@pytest.mark.unit
def test_w110_query_never_returns_a_purged_event(tmp_path: Path) -> None:
    store, _, service = _seed(tmp_path, old=4, new=2)
    purged_ids = {f"old-{index}" for index in range(4)}
    service.purge("tenant-A", _policy(dry_run=False))
    from_api = store.query().events
    assert not purged_ids & {event.event_id for event in from_api}
    for event in from_api:
        assert store.get(event.event_id) is not None


@pytest.mark.unit
def test_w111_integrity_stays_valid_after_archive_and_purge(tmp_path: Path) -> None:
    _, _, service = _seed(tmp_path, old=4, new=2)
    audit_service = ExecutionAuditService(ExecutionAuditStore(tmp_path / "audit"))
    service.purge("tenant-A", _policy(dry_run=False))
    control = ExecutionAuditControlService(
        ExecutionAuditService(ExecutionAuditStore(tmp_path / "audit"))
    )
    report = ExecutionAuditIntegrityService(control).verify("tenant-A")
    assert report.integrity_status.value == "VALID"
    assert report.invalid_events == 0
    assert audit_service is not None


@pytest.mark.unit
def test_w111_export_scope_is_explicit_and_deterministic(tmp_path: Path) -> None:
    _, archive, service = _seed(tmp_path, old=4, new=2)
    service.purge("tenant-A", _policy(dry_run=False))
    control = ExecutionAuditControlService(
        ExecutionAuditService(ExecutionAuditStore(tmp_path / "audit"))
    )
    exporter = ExecutionAuditExportService(control)
    active = exporter.export("tenant-A", scope=AuditExportScope.ACTIVE_ONLY)
    both = exporter.export(
        "tenant-A",
        scope=AuditExportScope.ACTIVE_AND_ARCHIVED,
        lifecycle=service,
    )
    assert active.scope is AuditExportScope.ACTIVE_ONLY
    assert both.scope is AuditExportScope.ACTIVE_AND_ARCHIVED
    assert both.count == active.count + len(archive.list_documents(tenant_id="tenant-A"))
    assert active.export_checksum != both.export_checksum
    assert json.loads(active.canonical_bytes)["scope"] == "ACTIVE_ONLY"
    assert json.loads(both.canonical_bytes)["scope"] == "ACTIVE_AND_ARCHIVED"
    repeat = exporter.export(
        "tenant-A", scope=AuditExportScope.ACTIVE_AND_ARCHIVED, lifecycle=service
    )
    assert repeat.canonical_bytes == both.canonical_bytes


@pytest.mark.unit
def test_w111_reconciliation_distinguishes_active_and_archived(tmp_path: Path) -> None:
    _, _, service = _seed(tmp_path, old=4, new=2)
    service.purge("tenant-A", _policy(dry_run=False))
    control = ExecutionAuditControlService(
        ExecutionAuditService(ExecutionAuditStore(tmp_path / "audit"))
    )
    without = ExecutionAuditReconciliationService(control).reconcile("tenant-A")
    assert without.lifecycle_reader is False
    assert without.archived_events == 0
    with_reader = ExecutionAuditReconciliationService(control).reconcile(
        "tenant-A", lifecycle=service
    )
    assert with_reader.lifecycle_reader is True
    assert with_reader.archived_events == 4
    assert with_reader.active_events == with_reader.events_scanned
    assert with_reader.reconciliation_status is not None


@pytest.mark.unit
def test_w111_export_never_duplicates_an_event(tmp_path: Path) -> None:
    _, _, service = _seed(tmp_path, old=3, new=2)
    service.archive("tenant-A", _policy(dry_run=False))
    control = ExecutionAuditControlService(
        ExecutionAuditService(ExecutionAuditStore(tmp_path / "audit"))
    )
    both = ExecutionAuditExportService(control).export(
        "tenant-A", scope=AuditExportScope.ACTIVE_AND_ARCHIVED, lifecycle=service
    )
    ids = [item["event_id"] for item in json.loads(both.canonical_bytes)["events"]]
    assert len(ids) == len(set(ids)), "an event present in both stores must appear once"


# ── J. Audit of the lifecycle + recursion guard ──────────────────────────


@pytest.mark.unit
def test_w112_records_its_own_operations_as_w110_events(tmp_path: Path) -> None:
    store, _, service = _seed(tmp_path, old=2, new=1)
    service.preview("tenant-A", _policy(dry_run=True))
    service.archive("tenant-A", _policy(dry_run=False))
    service.purge("tenant-A", _policy(dry_run=False))
    operations = {event.operation for event in store.scan_documents().events}
    assert AuditOperation.AUDIT_ARCHIVE in operations
    assert AuditOperation.AUDIT_PURGE in operations
    # preview and dry runs are read-only probes and are deliberately NOT
    # self-audited: writing an event would mutate the store being inspected
    assert AuditOperation.AUDIT_LIFECYCLE_PREVIEW not in operations
    for event in store.scan_documents().events:
        if event.operation.value.startswith("AUDIT_"):
            assert event.source == AUDIT_LIFECYCLE_SOURCE
            assert event.tenant_id == "tenant-A"


@pytest.mark.unit
def test_w112_restore_is_audited(tmp_path: Path) -> None:
    store, archive, service = _seed(tmp_path, old=1, new=0)
    service.purge("tenant-A", _policy(dry_run=False))
    service.restore(
        AuditRestoreRequest(event_id=archive.list_documents()[0].event_id, tenant_id="tenant-A"),
        authenticated_tenant="tenant-A",
    )
    operations = {event.operation for event in store.scan_documents().events}
    assert AuditOperation.AUDIT_RESTORE in operations


@pytest.mark.unit
def test_w112_no_recursive_lifecycle(tmp_path: Path) -> None:
    """A lifecycle audit write must never start another lifecycle run."""

    store, archive, service = _seed(tmp_path, old=2, new=0)
    before = len(store.scan_documents().events)
    service.archive("tenant-A", _policy(dry_run=False))
    service.purge("tenant-A", _policy(dry_run=False))
    after = len(store.scan_documents().events)
    # only the explicit lifecycle events were added, no runaway chain
    assert after - before <= 4
    assert archive is not None


@pytest.mark.unit
def test_w112_recursion_guard_refuses_a_nested_lifecycle(tmp_path: Path) -> None:
    """A lifecycle started while another one runs is refused, not chained."""

    _, _, service = _seed(tmp_path, old=1, new=0)
    with service._guarded():
        # inside an active run, any lifecycle entry point must be refused
        for call in (
            lambda: service.preview("tenant-A", _policy(dry_run=True)),
            lambda: service.archive("tenant-A", _policy(dry_run=False)),
            lambda: service.purge("tenant-A", _policy(dry_run=False)),
        ):
            with pytest.raises(LifecycleRecursionError):
                call()
    # the guard is released again once the run completes
    assert service.preview("tenant-A", _policy(dry_run=True)).status is LifecycleStatus.DRY_RUN


# ── K. Concurrency ───────────────────────────────────────────────────────


@pytest.mark.unit
def test_w112_concurrent_archive_is_idempotent(tmp_path: Path) -> None:
    store = ExecutionAuditStore(tmp_path / "audit")
    for index in range(6):
        store.append(
            _event(f"c-{index}", request_id=f"c{index}", occurred_at=NOW - timedelta(days=90))
        )
    services = [
        AuditLifecycleService(
            ExecutionAuditService(ExecutionAuditStore(tmp_path / "audit")),
            FileSystemAuditArchiveStore(tmp_path / "archive"),
        )
        for _ in range(4)
    ]

    def run(target: AuditLifecycleService) -> int:
        return target.archive("tenant-A", _policy(dry_run=False)).archived

    with ThreadPoolExecutor(max_workers=4) as executor:
        created = sum(executor.map(run, services))
    documents = FileSystemAuditArchiveStore(tmp_path / "archive").list_documents()
    assert created == 6, "exactly one effective archive per event"
    assert len(documents) == 6


@pytest.mark.unit
def test_w112_concurrent_purge_removes_once(tmp_path: Path) -> None:
    store = ExecutionAuditStore(tmp_path / "audit")
    for index in range(4):
        store.append(
            _event(f"p-{index}", request_id=f"p{index}", occurred_at=NOW - timedelta(days=90))
        )
    services = [
        AuditLifecycleService(
            ExecutionAuditService(ExecutionAuditStore(tmp_path / "audit")),
            FileSystemAuditArchiveStore(tmp_path / "archive"),
        )
        for _ in range(4)
    ]

    def run(target: AuditLifecycleService) -> int:
        return target.purge("tenant-A", _policy(dry_run=False)).purged

    with ThreadPoolExecutor(max_workers=4) as executor:
        purged = sum(executor.map(run, services))
    assert purged == 4, "each business event is removed exactly once"
    remaining = store.scan_documents().events
    assert not {e.event_id for e in remaining} & {f"p-{i}" for i in range(4)}
    # only the four legitimate AUDIT_PURGE events of the runs may remain
    assert {e.operation for e in remaining} == {AuditOperation.AUDIT_PURGE}
    assert len(FileSystemAuditArchiveStore(tmp_path / "archive").list_documents()) == 4


@pytest.mark.unit
def test_w112_multiprocess_archive_and_purge(tmp_path: Path) -> None:
    store = ExecutionAuditStore(tmp_path / "audit")
    for index in range(5):
        store.append(
            _event(f"m-{index}", request_id=f"m{index}", occurred_at=NOW - timedelta(days=90))
        )
    script = (
        "import sys\n"
        f"sys.path.insert(0, {str(ROOT / 'src')!r})\n"
        "from datetime import datetime, timezone\n"
        "from trendx.forecasting.audit import ExecutionAuditService, ExecutionAuditStore\n"
        "from trendx.forecasting.audit_lifecycle import (AuditLifecycleService,"
        " AuditRetentionPolicy, FileSystemAuditArchiveStore)\n"
        f"service = ExecutionAuditService(ExecutionAuditStore({str(tmp_path / 'audit')!r}))\n"
        f"archive = FileSystemAuditArchiveStore({str(tmp_path / 'archive')!r})\n"
        "lifecycle = AuditLifecycleService(service, archive)\n"
        "policy = AuditRetentionPolicy(retention_days=1, reference_time=datetime.now(timezone.utc), dry_run=False)\n"
        "print(lifecycle.purge('tenant-A', policy).purged)\n"
    )
    results = []
    for _ in range(2):
        completed = subprocess.run(
            [sys.executable, "-c", script],
            capture_output=True,
            text=True,
            check=True,
            cwd=str(ROOT),
        )
        results.append(int(completed.stdout.strip().splitlines()[-1]))
    assert sum(results) == 5, "purges across processes must total exactly the trail"
    remaining = {e.event_id for e in store.scan_documents().events}
    assert not remaining & {f"m-{i}" for i in range(5)}
    assert len(FileSystemAuditArchiveStore(tmp_path / "archive").list_documents()) == 5


# ── L. Security / sanitization ───────────────────────────────────────────


@pytest.mark.unit
@pytest.mark.parametrize(
    "event_id",
    ["../../etc/passwd", "/absolute/path", "a\x00b", "x" * 400, "..", "a\\b"],
)
def test_w112_archive_rejects_unsafe_event_ids(tmp_path: Path, event_id: str) -> None:
    archive = FileSystemAuditArchiveStore(tmp_path / "archive")
    with pytest.raises(AuditError):
        archive.get(event_id)
    with pytest.raises(AuditError):
        archive.exists(event_id)


@pytest.mark.unit
def test_w112_reports_never_expose_paths_or_secrets(tmp_path: Path) -> None:
    store, archive, service = _seed(tmp_path, old=2, new=1)
    report = service.purge("tenant-A", _policy(dry_run=False))
    rendered = json.dumps(report.to_dict())
    assert str(tmp_path) not in rendered
    assert str(archive.path) not in rendered
    assert ".json" not in rendered
    assert "Traceback" not in rendered
    assert ".lock" not in rendered
    assert store is not None


@pytest.mark.unit
def test_w112_archive_document_rejects_foreign_shapes() -> None:
    for payload in (
        {"archive_version": 1},
        {
            "archive_version": 1,
            "archived_at": NOW.isoformat(),
            "original_event_checksum": "0" * 64,
            "event": {},
        },
        {
            "archive_version": 1,
            "archived_at": NOW.isoformat(),
            "original_event_checksum": "0" * 64,
            "event": {},
            "archive_document_checksum": "0" * 64,
            "extra": 1,
        },
    ):
        with pytest.raises(AuditArchiveIntegrityError):
            AuditArchiveDocument.from_dict(payload)


# ── M. HTTP contract ─────────────────────────────────────────────────────


def _auth_headers() -> dict[str, str]:
    return {"Authorization": f"Bearer {settings.trendx_api_token.get_secret_value()}"}


def _policy_body(**overrides: Any) -> dict[str, Any]:
    body = {
        "retention_days": 30,
        "reference_time": NOW.isoformat(),
        "minimum_events_to_keep": 2,
        "dry_run": True,
    }
    body.update(overrides)
    return body


@pytest.mark.unit
def test_w112_api_requires_authentication() -> None:
    with TestClient(app) as client:
        for path in LIFECYCLE_PATHS:
            assert client.post(path, json={}).status_code == 401
            assert (
                client.post(path, json={}, headers={"Authorization": "Bearer bad"}).status_code
                == 401
            )


@pytest.mark.unit
def test_w112_api_preview_archive_purge_restore(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(settings, "trendx_api_token", SecretStr("w112-unit-token"))
    store, archive, service = _seed(tmp_path, old=5, new=3)
    previous = app.state.execution_audit_lifecycle_service_factory
    app.state.execution_audit_lifecycle_service_factory = lambda: service
    app.dependency_overrides[get_execution_audit_lifecycle_service] = lambda: service
    app.dependency_overrides[get_tenant_context] = lambda: TenantContext("tenant-A")
    try:
        with TestClient(app) as client:
            before = _fingerprint(tmp_path / "audit")
            preview = client.post(
                "/api/v1/forecast/executions/audit/lifecycle/preview",
                json=_policy_body(),
                headers=_auth_headers(),
            )
            assert preview.status_code == 200
            assert preview.json()["status"] == "DRY_RUN"
            assert preview.json()["eligible"] == 5
            assert preview.json()["protected"] == 2
            assert _fingerprint(tmp_path / "audit") == before

            archive_response = client.post(
                "/api/v1/forecast/executions/audit/lifecycle/archive",
                json=_policy_body(dry_run=False),
                headers=_auth_headers(),
            )
            assert archive_response.status_code == 200
            assert archive_response.json()["archived"] == 5
            assert archive_response.json()["archive_verified"] == 5

            purge = client.post(
                "/api/v1/forecast/executions/audit/lifecycle/purge",
                json=_policy_body(dry_run=False),
                headers=_auth_headers(),
            )
            assert purge.status_code == 200
            assert purge.json()["purged"] == 5
            assert purge.json()["archive_verified"] == 5

            archived_id = archive.list_documents()[0].event_id
            restore = client.post(
                "/api/v1/forecast/executions/audit/lifecycle/restore",
                json={"event_id": archived_id, "tenant_id": "tenant-A"},
                headers=_auth_headers(),
            )
            assert restore.status_code == 200
            assert restore.json()["status"] == "RESTORED"
            assert restore.json()["http_status"] == 200
            assert store.get(archived_id) is not None
            for response in (preview, archive_response, purge, restore):
                assert _no_leak(response.text)
    finally:
        app.state.execution_audit_lifecycle_service_factory = previous
        app.dependency_overrides.pop(get_execution_audit_lifecycle_service, None)
        app.dependency_overrides.pop(get_tenant_context, None)


def _no_leak(text: str) -> bool:
    """True when a response body carries no token, traceback or lock path."""

    return (
        settings.trendx_api_token.get_secret_value() not in text
        and "Traceback" not in text
        and ".lock" not in text
    )


@pytest.mark.unit
def test_w112_api_rejects_invalid_policy(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr(settings, "trendx_api_token", SecretStr("w112-invalid-token"))
    _, _, service = _seed(tmp_path, old=1, new=1)
    previous = app.state.execution_audit_lifecycle_service_factory
    app.state.execution_audit_lifecycle_service_factory = lambda: service
    app.dependency_overrides[get_tenant_context] = lambda: TenantContext("tenant-A")
    try:
        with TestClient(app) as client:
            for body in (
                _policy_body(retention_days=0),
                _policy_body(retention_days=-5),
                _policy_body(reference_time="not-a-date"),
                _policy_body(reference_time=""),
                _policy_body(minimum_events_to_keep=-1),
                {**_policy_body(), "unknown": "x"},
            ):
                response = client.post(
                    "/api/v1/forecast/executions/audit/lifecycle/preview",
                    json=body,
                    headers=_auth_headers(),
                )
                assert response.status_code == 422, body
                assert "Traceback" not in response.text
    finally:
        app.state.execution_audit_lifecycle_service_factory = previous
        app.dependency_overrides.pop(get_tenant_context, None)


@pytest.mark.unit
def test_w112_api_restore_status_codes(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr(settings, "trendx_api_token", SecretStr("w112-restore-token"))
    _, archive, service = _seed(tmp_path, old=2, new=0)
    service.purge("tenant-A", _policy(dry_run=False))
    archived_id = archive.list_documents()[0].event_id
    previous = app.state.execution_audit_lifecycle_service_factory
    app.state.execution_audit_lifecycle_service_factory = lambda: service
    app.dependency_overrides[get_tenant_context] = lambda: TenantContext("tenant-A")
    try:
        with TestClient(app) as client:
            assert (
                client.post(
                    "/api/v1/forecast/executions/audit/lifecycle/restore",
                    json={"event_id": "missing", "tenant_id": "tenant-A"},
                    headers=_auth_headers(),
                ).status_code
                == 404
            )
            assert (
                client.post(
                    "/api/v1/forecast/executions/audit/lifecycle/restore",
                    json={"event_id": archived_id, "tenant_id": "tenant-A"},
                    headers=_auth_headers(),
                ).status_code
                == 200
            )
            assert (
                client.post(
                    "/api/v1/forecast/executions/audit/lifecycle/restore",
                    json={"event_id": archived_id, "tenant_id": "tenant-A"},
                    headers=_auth_headers(),
                ).status_code
                == 200
            )
            forbidden = client.post(
                "/api/v1/forecast/executions/audit/lifecycle/restore",
                json={"event_id": archived_id, "tenant_id": "tenant-B"},
                headers=_auth_headers(),
            )
            assert forbidden.status_code == 403
            assert "tenant-B" not in forbidden.text
            assert (
                client.post(
                    "/api/v1/forecast/executions/audit/lifecycle/restore",
                    json={"event_id": "../../etc/passwd", "tenant_id": "tenant-A"},
                    headers=_auth_headers(),
                ).status_code
                == 422
            )
    finally:
        app.state.execution_audit_lifecycle_service_factory = previous
        app.dependency_overrides.pop(get_tenant_context, None)


@pytest.mark.unit
def test_w112_api_unavailable_backend(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(settings, "trendx_api_token", SecretStr("w112-unavailable-token"))
    previous = app.state.execution_audit_lifecycle_service_factory
    app.state.execution_audit_lifecycle_service_factory = lambda: None
    app.dependency_overrides[get_tenant_context] = lambda: TenantContext("tenant-A")
    try:
        with TestClient(app) as client:
            for path in LIFECYCLE_PATHS:
                response = client.post(path, json={}, headers=_auth_headers())
                assert response.status_code == 503, path
                assert response.json()["detail"] == "audit_lifecycle_unavailable"
    finally:
        app.state.execution_audit_lifecycle_service_factory = previous
        app.dependency_overrides.pop(get_tenant_context, None)


@pytest.mark.unit
def test_w112_api_auth_not_configured(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(settings, "trendx_api_token", SecretStr("CHANGE_ME"))
    with TestClient(app) as client:
        response = client.post(
            "/api/v1/forecast/executions/audit/lifecycle/preview",
            json=_policy_body(),
            headers={"Authorization": "Bearer CHANGE_ME"},
        )
        assert response.status_code == 503
        assert response.json()["detail"] == "audit_authentication_not_configured"


@pytest.mark.unit
def test_w112_api_has_no_direct_delete_route() -> None:
    schema = app.openapi()
    for path in LIFECYCLE_PATHS:
        verbs = {verb.upper() for verb in schema["paths"][path]}
        assert verbs == {"POST"}, (path, verbs)
    delete_routes = [
        path
        for path, item in schema["paths"].items()
        if "delete" in {verb.lower() for verb in item} and "audit" in path
    ]
    assert delete_routes == [], "there must be no DELETE route on the audit surface"


@pytest.mark.unit
def test_w112_openapi_contract() -> None:
    schema = app.openapi()
    for path in LIFECYCLE_PATHS:
        operation = schema["paths"][path]["post"]
        assert set(operation["responses"]) >= {"200", "401", "403", "409", "422", "503"}
        assert operation["security"] == [{"BearerAuth": []}, {"ApiKeyAuth": []}]
    assert "AuditLifecyclePreviewOut" in schema["components"]["schemas"]
    assert "AuditLifecycleOut" in schema["components"]["schemas"]
    assert "AuditRestoreOut" in schema["components"]["schemas"]


# ── N. Sanity on the store contract ──────────────────────────────────────


@pytest.mark.unit
def test_w112_memory_store_compare_and_delete() -> None:
    store = MemoryExecutionAuditStore()
    stored = store.append(_event("mem", request_id="r1"))
    assert store.delete_if_unchanged(stored).outcome is AuditDeleteOutcome.DELETED
    assert store.get("mem") is None
    assert store.delete_if_unchanged(stored).outcome is AuditDeleteOutcome.NOT_FOUND
