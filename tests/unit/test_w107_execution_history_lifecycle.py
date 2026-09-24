"""W107 — durable execution-history lifecycle, archive, and purge contracts."""

from __future__ import annotations

import hashlib
import json
import stat
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from loguru import logger
from trendx.forecasting.analytics import analyze_execution_history
from trendx.forecasting.execution import (
    DurableExecutionStore,
    ExecutionProvenance,
    ExecutionRecord,
    ExecutionStoreError,
    MemoryExecutionStore,
)
from trendx.forecasting.history import ExecutionHistoryService, ExecutionQuery
from trendx.forecasting.lifecycle import (
    ArchiveConflictError,
    ArchiveError,
    ArchiveIntegrityError,
    ArchiveVerification,
    ExecutionHistoryLifecycle,
    ExecutionRetentionPolicy,
    FileSystemArchiveStore,
    InvalidRetentionPolicyError,
    LifecycleTenantError,
    _document_checksum,
)
from trendx.forecasting.reporting import ExecutionAnalyticsReportingService

BASE_TIME = datetime(2026, 1, 1, tzinfo=UTC)
NOW = BASE_TIME + timedelta(days=60)
POLICY_DAYS = 30


@dataclass(frozen=True)
class Spec:
    execution_id: str
    tenant: str = "tenant-A"
    status: str = "SUCCESS"
    created_at: str = "2026-01-01T00:00:00Z"
    completed_at: str = "2099-01-01T00:01:00Z"
    error_code: str = "controlled_failure"
    metadata: dict[str, Any] | None = None
    result: dict[str, Any] | None = None


def _record(spec: Spec) -> ExecutionRecord:
    return ExecutionRecord(
        execution_id=spec.execution_id,
        reference_key=f"w107:{spec.execution_id}",
        tenant_id=spec.tenant,
        entity_type="DEVICE",
        entity_id=f"device-{spec.execution_id}",
        target_metric="temperature",
        frequency="1h",
        horizon=24,
        model_id="model-w107",
        model_version="1",
        algorithm="Fourier",
        feature_schema_version="schema-w107",
        feature_schema_fingerprint="fingerprint-w107",
        artifact_uri="file:///w107/model.bin",
        created_at=spec.created_at,
        metadata=spec.metadata if spec.metadata is not None else {"source": "w107"},
        result=spec.result if spec.result is not None else {"values": [1.0, 2.0]},
    )


def _provenance(record: ExecutionRecord) -> ExecutionProvenance:
    return ExecutionProvenance(
        model_id=record.model_id,
        model_version=record.model_version,
        algorithm=record.algorithm,
        feature_schema_version=record.feature_schema_version,
        feature_schema_fingerprint=record.feature_schema_fingerprint,
        artifact_uri=record.artifact_uri,
        prediction_count=2,
        metadata={"source": "w107"},
        result=record.result,
    )


def _seed(store: MemoryExecutionStore | DurableExecutionStore, specs: tuple[Spec, ...]) -> None:
    for spec in specs:
        record = _record(spec)
        store.create(record)
        if spec.status == "PENDING":
            continue
        store.mark_started(record.execution_id)
        if spec.status == "RUNNING":
            continue
        if spec.status == "FAILED":
            store.mark_failed(
                record.execution_id,
                error_code=spec.error_code,
                error_reason="W107 controlled failure",
                provenance=_provenance(record),
                completed_at=spec.completed_at,
            )
        elif spec.status == "SUCCESS":
            store.mark_success(
                record.execution_id,
                _provenance(record),
                completed_at=spec.completed_at,
            )
        else:
            msg = f"unsupported W107 fixture status: {spec.status}"
            raise ValueError(msg)


def _memory(specs: tuple[Spec, ...]) -> MemoryExecutionStore:
    store = MemoryExecutionStore()
    _seed(store, specs)
    return store


def _durable(tmp_path: Path, specs: tuple[Spec, ...]) -> DurableExecutionStore:
    store = DurableExecutionStore(tmp_path / "active-history.json")
    _seed(store, specs)
    return store


def _archive(tmp_path: Path) -> FileSystemArchiveStore:
    return FileSystemArchiveStore(tmp_path / "archive")


def _lifecycle(
    store: MemoryExecutionStore | DurableExecutionStore,
    archive: FileSystemArchiveStore,
) -> ExecutionHistoryLifecycle:
    return ExecutionHistoryLifecycle(store, archive, lambda: NOW)  # type: ignore[arg-type]


def _policy(*, dry_run: bool, archive_before_purge: bool = True, minimum: int = 0):
    return ExecutionRetentionPolicy(
        retention_days=POLICY_DAYS,
        archive_before_purge=archive_before_purge,
        minimum_records_to_keep=minimum,
        dry_run=dry_run,
    )


def _ids(records: tuple[ExecutionRecord, ...]) -> set[str]:
    return {record.execution_id for record in records}


def _active_hash(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


@pytest.mark.unit
def test_w107_policy_contract() -> None:
    policy = ExecutionRetentionPolicy(retention_days=POLICY_DAYS)
    assert policy.archive_before_purge is True
    assert policy.minimum_records_to_keep == 0
    assert policy.dry_run is True
    assert policy.to_dict() == {
        "retention_days": 30,
        "archive_before_purge": True,
        "minimum_records_to_keep": 0,
        "dry_run": True,
    }
    with pytest.raises(InvalidRetentionPolicyError):
        ExecutionRetentionPolicy(retention_days=-1)
    with pytest.raises(InvalidRetentionPolicyError):
        ExecutionRetentionPolicy(retention_days=True)  # type: ignore[arg-type]
    with pytest.raises(InvalidRetentionPolicyError):
        ExecutionRetentionPolicy(retention_days=30, minimum_records_to_keep=-1)
    with pytest.raises(InvalidRetentionPolicyError):
        ExecutionRetentionPolicy(retention_days=10**20)
    with pytest.raises(InvalidRetentionPolicyError):
        ExecutionRetentionPolicy(retention_days=30, dry_run=1)  # type: ignore[arg-type]


@pytest.mark.unit
def test_w107_boundary_time(tmp_path: Path) -> None:
    exact = NOW - timedelta(days=POLICY_DAYS)
    specs = (
        Spec("exact", created_at=exact.isoformat()),
        Spec("before", created_at=(exact + timedelta(microseconds=1)).isoformat()),
        Spec("after", created_at=(exact - timedelta(microseconds=1)).isoformat()),
    )
    store = _memory(specs)
    report = _lifecycle(store, _archive(tmp_path)).preview("tenant-A", _policy(dry_run=True))
    assert report.eligible == 2
    assert report.protected == 1
    assert set(report.archive_candidates) == {"exact", "after"}
    assert "before" in report.protected_ids
    assert {item.execution_id: item.reason for item in report.decisions} == {
        "exact": "retention_eligible",
        "before": "within_retention_window",
        "after": "retention_eligible",
    }


@pytest.mark.unit
def test_w107_dry_run(tmp_path: Path) -> None:
    path = tmp_path / "active-history.json"
    store = _durable(
        tmp_path,
        (
            Spec("old-success", created_at="2025-12-01T00:00:00Z"),
            Spec("recent-success", created_at="2026-02-15T00:00:00Z"),
        ),
    )
    archive = _archive(tmp_path)
    lifecycle = _lifecycle(store, archive)
    before_hash = _active_hash(path)
    before_records = tuple(record.to_dict() for record in store.list())
    lock_path = path.with_name(f".{path.name}.lock")
    lock_mtime = lock_path.stat().st_mtime_ns
    try:
        report = lifecycle.preview("tenant-A", _policy(dry_run=True))
        assert report.scanned == 2
        assert report.eligible == 1
        assert report.protected == 1
        assert report.archived == 0
        assert report.purged == 0
        assert report.dry_run is True
        assert report.archive_candidates == ("old-success",)
        assert _active_hash(path) == before_hash
        assert tuple(record.to_dict() for record in store.list()) == before_records
        assert lock_path.stat().st_mtime_ns == lock_mtime
        assert not any(archive.root.glob("*.json"))
    finally:
        store.close()


@pytest.mark.unit
def test_w107_preview_does_not_create_archive_root(tmp_path: Path) -> None:
    store = _memory((Spec("no-root", created_at="2025-01-01T00:00:00Z"),))
    archive_root = tmp_path / "missing-archive-root"
    _lifecycle(store, FileSystemArchiveStore(archive_root)).preview(
        "tenant-A", _policy(dry_run=True)
    )
    assert not archive_root.exists()


@pytest.mark.unit
def test_w107_archive_integrity(tmp_path: Path) -> None:
    record = _record(Spec("archive-ok"))
    archive = _archive(tmp_path)
    policy = _policy(dry_run=False)
    receipt = archive.archive((record,), policy=policy, archived_at=NOW)[0]
    archived = archive.read(record.execution_id)
    assert receipt.created is True
    assert receipt.record_checksum == archived.metadata.record_checksum
    assert archive.verify(record.execution_id, expected_record=record, expected_policy=policy).valid
    assert archived.record.to_dict() == record.to_dict()
    assert archived.metadata.reason == "retention"
    assert archived.metadata.policy.to_dict() == policy.to_dict()
    file_path = archive._path_for(record.execution_id)
    assert stat.S_IMODE(file_path.stat().st_mode) == 0o600
    assert stat.S_IMODE(archive.root.stat().st_mode) == 0o700
    first_bytes = file_path.read_bytes()
    second = archive.archive((record,), policy=policy, archived_at=NOW + timedelta(days=1))[0]
    assert second.created is False
    assert file_path.read_bytes() == first_bytes
    payload = json.loads(file_path.read_text(encoding="utf-8"))
    assert payload["archive_format_version"] == 1
    assert (
        payload["content_checksum"] == archived.metadata.record_checksum
        or len(payload["content_checksum"]) == 64
    )


@pytest.mark.unit
def test_w107_archive_rejects_incomplete_or_noncanonical_record(tmp_path: Path) -> None:
    record = _record(Spec("strict-record"))
    archive = _archive(tmp_path)
    archive.archive((record,), policy=_policy(dry_run=False), archived_at=NOW)
    file_path = archive._path_for(record.execution_id)
    original = json.loads(file_path.read_text(encoding="utf-8"))
    del original["record"]["model_id"]
    original["content_checksum"] = _document_checksum(original["metadata"], original["record"])
    file_path.write_text(json.dumps(original, sort_keys=True), encoding="utf-8")
    assert archive.verify(record.execution_id).valid is False

    second = _record(Spec("noncanonical-record"))
    archive.archive((second,), policy=_policy(dry_run=False), archived_at=NOW)
    second_path = archive._path_for(second.execution_id)
    payload = json.loads(second_path.read_text(encoding="utf-8"))
    payload["record"]["prediction_count"] = 1.0
    payload["content_checksum"] = _document_checksum(payload["metadata"], payload["record"])
    second_path.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")
    assert archive.verify(second.execution_id).valid is False

    third = _record(Spec("invalid-version"))
    archive.archive((third,), policy=_policy(dry_run=False), archived_at=NOW)
    third_path = archive._path_for(third.execution_id)
    versioned = json.loads(third_path.read_text(encoding="utf-8"))
    versioned["archive_format_version"] = True
    third_path.write_text(json.dumps(versioned, sort_keys=True), encoding="utf-8")
    assert archive.verify(third.execution_id).valid is False


@pytest.mark.unit
def test_w107_archive_corruption(tmp_path: Path) -> None:
    record = _record(Spec("archive-corrupt"))
    store = _memory((Spec("archive-corrupt"),))
    archive = _archive(tmp_path)
    archive.archive((record,), policy=_policy(dry_run=False), archived_at=NOW)
    file_path = archive._path_for(record.execution_id)
    original_text = file_path.read_text(encoding="utf-8")
    file_path.write_text(original_text + "corrupt", encoding="utf-8")
    verification = archive.verify(record.execution_id)
    assert verification.valid is False
    payload = json.loads(original_text)
    payload["metadata"]["archived_at"] = "2026-03-03T00:00:00+00:00"
    file_path.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")
    assert archive.verify(record.execution_id).error_code == "archive_content_checksum_mismatch"
    with pytest.raises(ArchiveIntegrityError):
        archive.read(record.execution_id)
    report = _lifecycle(store, archive).purge_eligible("tenant-A", _policy(dry_run=False))
    assert report.purged == 0
    assert report.failed == 1
    assert store.get(record.execution_id) is not None


@pytest.mark.unit
def test_w107_archive_rejects_insecure_permissions(tmp_path: Path) -> None:
    record = _record(Spec("insecure-permissions"))
    store = _memory((Spec("insecure-permissions"),))
    archive = _archive(tmp_path)
    archive.archive((record,), policy=_policy(dry_run=False), archived_at=NOW)
    archive._path_for(record.execution_id).chmod(0o644)
    assert archive.verify(record.execution_id).valid is False
    report = _lifecycle(store, archive).purge_eligible("tenant-A", _policy(dry_run=False))
    assert report.purged == 0
    assert store.get(record.execution_id) is not None


@pytest.mark.unit
def test_w107_archive_idempotence(tmp_path: Path) -> None:
    record = _record(Spec("archive-idempotent"))
    archive = _archive(tmp_path)
    policy = _policy(dry_run=False)
    first = archive.archive((record,), policy=policy, archived_at=NOW)[0]
    file_path = archive._path_for(record.execution_id)
    original = file_path.read_bytes()
    second = archive.archive((record,), policy=policy, archived_at=NOW + timedelta(hours=1))[0]
    assert first.created is True
    assert second.created is False
    assert file_path.read_bytes() == original
    changed = replace(record, model_id="different-model")
    with pytest.raises(ArchiveConflictError):
        archive.archive((changed,), policy=policy, archived_at=NOW)


@pytest.mark.unit
def test_w107_purge_safety(tmp_path: Path) -> None:
    store = _durable(
        tmp_path,
        (
            Spec("old-success", created_at="2025-12-01T00:00:00Z"),
            Spec("old-failure", status="FAILED", created_at="2025-12-02T00:00:00Z"),
            Spec("recent-success", created_at="2026-02-15T00:00:00Z"),
            Spec("recent-failure", status="FAILED", created_at="2026-02-14T00:00:00Z"),
            Spec("running", status="RUNNING", created_at="2025-01-01T00:00:00Z"),
            Spec("pending", status="PENDING", created_at="2025-01-01T00:00:00Z"),
        ),
    )
    archive = _archive(tmp_path)
    lifecycle = _lifecycle(store, archive)
    try:
        report = lifecycle.purge_eligible("tenant-A", _policy(dry_run=False))
        assert report.scanned == 6
        assert report.eligible == 2
        assert report.archived == 2
        assert report.purged == 2
        assert report.failed == 0
        assert _ids(store.list()) == {"recent-success", "recent-failure", "running", "pending"}
        assert archive.exists("old-success")
        assert archive.exists("old-failure")
        second = lifecycle.purge_eligible("tenant-A", _policy(dry_run=False))
        assert second.purged == 0
        assert second.archived == 0
        assert _ids(store.list()) == {"recent-success", "recent-failure", "running", "pending"}
    finally:
        store.close()


@pytest.mark.unit
def test_w107_protected_statuses(tmp_path: Path) -> None:
    store = _memory(
        (
            Spec("pending", status="PENDING", created_at="2020-01-01T00:00:00Z"),
            Spec("running", status="RUNNING", created_at="2020-01-01T00:00:00Z"),
            Spec("success", created_at="2020-01-01T00:00:00Z"),
        )
    )
    report = _lifecycle(store, _archive(tmp_path)).preview("tenant-A", _policy(dry_run=True))
    assert report.protected == 2
    assert report.eligible == 1
    assert set(report.protected_ids) == {"pending", "running"}
    assert {item.execution_id: item.reason for item in report.decisions}[
        "pending"
    ] == "status_pending"
    assert {item.execution_id: item.reason for item in report.decisions}[
        "running"
    ] == "status_running"


@pytest.mark.unit
def test_w107_invalid_created_at_is_skipped(tmp_path: Path) -> None:
    store = _memory((Spec("invalid-created", created_at="not-a-timestamp"),))
    report = _lifecycle(store, _archive(tmp_path)).preview("tenant-A", _policy(dry_run=True))
    assert report.eligible == 0
    assert report.skipped == 1
    assert report.decisions[0].reason == "invalid_created_at"
    assert store.get("invalid-created") is not None


@pytest.mark.unit
def test_w107_scan_failure_is_non_mutating(tmp_path: Path) -> None:
    class BrokenStore(MemoryExecutionStore):
        def list(self, **kwargs: Any) -> tuple[ExecutionRecord, ...]:  # type: ignore[override]
            raise ExecutionStoreError("controlled store read failure")

    report = _lifecycle(BrokenStore(), _archive(tmp_path)).purge_eligible(
        "tenant-A", _policy(dry_run=False)
    )
    assert report.scanned == 0
    assert report.failed == 1
    assert report.purged == 0
    assert report.failures[0].stage == "scan"
    assert report.failures[0].execution_id is None


@pytest.mark.unit
def test_w107_unknown_status_is_skipped(tmp_path: Path) -> None:
    class UnknownStatusStore(MemoryExecutionStore):
        def list(self, **kwargs: Any) -> tuple[ExecutionRecord, ...]:  # type: ignore[override]
            return tuple(replace(record, status="UNKNOWN") for record in super().list(**kwargs))

    store = UnknownStatusStore()
    _seed(store, (Spec("unknown"),))
    report = _lifecycle(store, _archive(tmp_path)).preview("tenant-A", _policy(dry_run=True))
    assert report.eligible == 0
    assert report.protected == 0
    assert report.skipped == 1


@pytest.mark.unit
def test_w107_tenant_isolation(tmp_path: Path) -> None:
    store = _memory(
        (
            Spec("a-old", tenant="tenant-A", created_at="2025-01-01T00:00:00Z"),
            Spec("b-old", tenant="tenant-B", created_at="2025-01-01T00:00:00Z"),
        )
    )
    archive = _archive(tmp_path)
    lifecycle = _lifecycle(store, archive)
    report = lifecycle.purge_eligible("tenant-A", _policy(dry_run=False))
    assert report.purged == 1
    assert _ids(store.list()) == {"b-old"}
    assert archive.exists("a-old")
    assert not archive.exists("b-old")
    with pytest.raises(LifecycleTenantError):
        lifecycle.archive_execution("b-old", "tenant-A", _policy(dry_run=False))


@pytest.mark.unit
def test_w107_failure_to_archive_keeps_record() -> None:
    class FailingArchiveStore:
        def archive(self, records: Any, **_: Any) -> tuple[Any, ...]:
            raise OSError("controlled filesystem failure")

        def exists(self, execution_id: str) -> bool:
            return False

        def read(self, execution_id: str) -> Any:
            raise ArchiveError("controlled failure")

        def verify(self, execution_id: str, **_: Any) -> Any:
            raise ArchiveError("controlled failure")

    store = _memory((Spec("keep-on-failure"),))
    report = _lifecycle(store, FailingArchiveStore()).purge_eligible(  # type: ignore[arg-type]
        "tenant-A", _policy(dry_run=False)
    )
    assert report.failed == 1
    assert report.archived == 0
    assert report.purged == 0
    assert store.get("keep-on-failure") is not None


@pytest.mark.unit
def test_w107_adapter_error_codes_are_sanitized(tmp_path: Path) -> None:
    class LeakyVerifyStore:
        def archive(self, records: Any, **_: Any) -> tuple[Any, ...]:
            raise AssertionError("preview must not archive")

        def exists(self, execution_id: str) -> bool:
            return True

        def read(self, execution_id: str) -> Any:
            raise AssertionError("preview must not read")

        def verify(self, execution_id: str, **_: Any) -> ArchiveVerification:
            return ArchiveVerification(
                valid=False,
                error_code="/private/archive/path-and-secret",
            )

    store = _memory((Spec("adapter-leak", created_at="2025-01-01T00:00:00Z"),))
    report = _lifecycle(store, LeakyVerifyStore()).preview(  # type: ignore[arg-type]
        "tenant-A", _policy(dry_run=True)
    )
    assert report.failed == 1
    assert report.failures[0].error_code == "archive_invalid"
    assert "/private/archive/path-and-secret" not in json.dumps(report.to_dict())


@pytest.mark.unit
def test_w107_archive_required_cannot_be_bypassed(tmp_path: Path) -> None:
    store = _memory((Spec("no-bypass", created_at="2025-01-01T00:00:00Z"),))
    archive = _archive(tmp_path)
    report = _lifecycle(store, archive).purge_eligible(
        "tenant-A", _policy(dry_run=False, archive_before_purge=False)
    )
    assert report.failed == 1
    assert report.archived == 0
    assert report.purged == 0
    assert store.get("no-bypass") is not None
    assert not archive.exists("no-bypass")


@pytest.mark.unit
def test_w107_atomic_archive_interruption(tmp_path: Path) -> None:
    record = _record(Spec("interrupted"))
    archive = FileSystemArchiveStore(
        tmp_path / "interrupted-archive",
        replace=lambda source, target: (_ for _ in ()).throw(OSError("simulated interruption")),
    )
    with pytest.raises(ArchiveError):
        archive.archive((record,), policy=_policy(dry_run=False), archived_at=NOW)
    assert not archive.exists(record.execution_id)
    assert not list(archive.root.glob("*.tmp"))


@pytest.mark.unit
def test_w107_directory_fsync_failure_keeps_source(tmp_path: Path) -> None:
    def fail_sync(path: Path) -> None:
        raise OSError("simulated directory sync failure")

    store = _memory((Spec("sync-failure", created_at="2025-01-01T00:00:00Z"),))
    archive = FileSystemArchiveStore(tmp_path / "sync-archive", sync_directory=fail_sync)
    report = _lifecycle(store, archive).purge_eligible("tenant-A", _policy(dry_run=False))
    assert report.failed == 1
    assert report.purged == 0
    assert store.get("sync-failure") is not None
    assert archive.exists("sync-failure")


@pytest.mark.unit
def test_w107_archive_root_rejects_symlink(tmp_path: Path) -> None:
    target = tmp_path / "target"
    target.mkdir()
    link = tmp_path / "archive-link"
    link.symlink_to(target, target_is_directory=True)
    with pytest.raises(ArchiveError):
        FileSystemArchiveStore(link)


@pytest.mark.unit
@pytest.mark.parametrize(
    "execution_id",
    ["../tenant-escape", "/absolute/path", "a\x00b", "tenant/../../escape"],
)
def test_w107_archive_path_is_hashed(tmp_path: Path, execution_id: str) -> None:
    archive = FileSystemArchiveStore(tmp_path / "path-check")
    record = _record(Spec(execution_id, created_at="2025-01-01T00:00:00Z"))
    archive.archive((record,), policy=_policy(dry_run=False), archived_at=NOW)
    assert archive._path_for(execution_id).parent == archive.root
    assert archive.exists(execution_id)
    assert archive.read(execution_id).record.execution_id == execution_id


@pytest.mark.unit
def test_w107_lifecycle_report_is_serializable_and_sanitized(tmp_path: Path) -> None:
    secret = "not-for-lifecycle-report"
    store = _memory(
        (
            Spec(
                "report",
                metadata={"credential": secret, "result": [1, 2]},
                created_at="2025-01-01T00:00:00Z",
            ),
        )
    )
    report = _lifecycle(store, _archive(tmp_path)).preview("tenant-A", _policy(dry_run=True))
    payload = report.to_dict()
    assert json.loads(json.dumps(payload, sort_keys=True)) == payload
    assert set(payload) == {
        "tenant_id",
        "policy",
        "generated_at",
        "scanned",
        "eligible",
        "protected",
        "archived",
        "purged",
        "already_purged",
        "skipped",
        "failed",
        "dry_run",
        "archive_candidates",
        "already_archived",
        "archived_ids",
        "purged_ids",
        "already_purged_ids",
        "protected_ids",
        "skipped_ids",
        "decisions",
        "failures",
    }
    assert secret not in json.dumps(payload)


@pytest.mark.unit
def test_w107_observability_is_sanitized(tmp_path: Path) -> None:
    secret = "credential-must-not-be-logged"
    store = _memory((Spec("observed", metadata={"token": secret}),))
    events: list[str] = []
    sink_id = logger.add(events.append, format="{message}")
    try:
        _lifecycle(store, _archive(tmp_path)).preview("tenant-A", _policy(dry_run=True))
    finally:
        logger.remove(sink_id)
    output = "\\n".join(events)
    assert "operation=lifecycle" in output
    assert "outcome=preview" in output
    assert secret not in output
    assert str(tmp_path) not in output


@pytest.mark.unit
def test_w107_history_read_during_archive_is_safe(tmp_path: Path) -> None:
    store = _durable(tmp_path, (Spec("concurrent-read", created_at="2025-01-01T00:00:00Z"),))
    history = ExecutionHistoryService(store)
    failures: list[Exception] = []

    def read_history() -> None:
        try:
            for _ in range(20):
                page = history.query(ExecutionQuery(tenant_id="tenant-A"))
                assert page.total == 1
        except Exception as exc:  # pragma: no cover - assertion below exposes failures
            failures.append(exc)

    try:
        with ThreadPoolExecutor(max_workers=2) as executor:
            reader = executor.submit(read_history)
            _lifecycle(store, _archive(tmp_path)).archive_eligible(
                "tenant-A", _policy(dry_run=False)
            )
            reader.result()
        assert failures == []
    finally:
        store.close()


@pytest.mark.unit
def test_w107_w99_compatibility(tmp_path: Path) -> None:
    store = _durable(
        tmp_path,
        (
            Spec("old", created_at="2025-01-01T00:00:00Z"),
            Spec("recent", created_at="2026-02-15T00:00:00Z"),
        ),
    )
    archive = _archive(tmp_path)
    try:
        history = ExecutionHistoryService(store)
        query = ExecutionQuery(tenant_id="tenant-A")
        assert history.count(query) == 2
        _lifecycle(store, archive).purge_eligible("tenant-A", _policy(dry_run=False))
        assert history.count(query) == 1
        assert history.get("old") is None
        assert archive.read("old").record.execution_id == "old"
    finally:
        store.close()


@pytest.mark.unit
def test_w107_w104_compatibility(tmp_path: Path) -> None:
    store = _durable(
        tmp_path,
        (
            Spec("old", created_at="2025-01-01T00:00:00Z"),
            Spec("recent", created_at="2026-02-15T00:00:00Z"),
        ),
    )
    archive = _archive(tmp_path)
    try:
        history = ExecutionHistoryService(store)
        query = ExecutionQuery(tenant_id="tenant-A")
        before = analyze_execution_history(history, query)
        _lifecycle(store, archive).purge_eligible("tenant-A", _policy(dry_run=False))
        after = analyze_execution_history(history, query)
        assert before.summary.total_executions == 2
        assert after.summary.total_executions == 1
        assert after.by_entity[0].entity_id == "device-recent"
    finally:
        store.close()


@pytest.mark.unit
def test_w107_w106_compatibility(tmp_path: Path) -> None:
    store = _durable(
        tmp_path,
        (
            Spec("old", created_at="2025-01-01T00:00:00Z"),
            Spec("recent", created_at="2026-02-15T00:00:00Z"),
        ),
    )
    archive = _archive(tmp_path)
    try:
        service = ExecutionHistoryService(store)
        query = ExecutionQuery(tenant_id="tenant-A")
        reporting = ExecutionAnalyticsReportingService(service)
        before = reporting.report(query)
        _lifecycle(store, archive).purge_eligible("tenant-A", _policy(dry_run=False))
        after = reporting.report(query)
        assert before.summary.total_executions == 2
        assert after.summary.total_executions == 1
        assert after.tenant_id == "tenant-A"
    finally:
        store.close()


@pytest.mark.unit
def test_w107_read_only_preview(tmp_path: Path) -> None:
    path = tmp_path / "active-history.json"
    store = _durable(tmp_path, (Spec("read-only", created_at="2025-01-01T00:00:00Z"),))
    before_hash = _active_hash(path)
    before_records = tuple(record.to_dict() for record in store.list())
    try:
        _lifecycle(store, _archive(tmp_path)).preview("tenant-A", _policy(dry_run=True))
        assert _active_hash(path) == before_hash
        assert tuple(record.to_dict() for record in store.list()) == before_records
    finally:
        store.close()


@pytest.mark.unit
def test_w107_minimum_records_to_keep(tmp_path: Path) -> None:
    store = _memory(
        (
            Spec("oldest", created_at="2025-01-01T00:00:00Z"),
            Spec("middle", created_at="2025-02-01T00:00:00Z"),
            Spec("newest", created_at="2025-03-01T00:00:00Z"),
        )
    )
    report = _lifecycle(store, _archive(tmp_path)).preview(
        "tenant-A", _policy(dry_run=True, minimum=1)
    )
    assert report.eligible == 2
    assert report.protected == 1
    assert report.protected_ids == ("newest",)


@pytest.mark.unit
@pytest.mark.parametrize("changed_value", [1.0, True, {"nested": [1.0]}])
def test_w107_compare_and_delete_preserves_json_type_distinctions(changed_value: Any) -> None:
    store = MemoryExecutionStore()
    record = replace(_record(Spec("cas-types")), metadata={"value": 1})
    store.create(record)
    current = store.get("cas-types")
    assert current is not None
    store._records["cas-types"] = replace(current, metadata={"value": changed_value})
    assert store.delete_if_unchanged("cas-types", current) is False
    assert store.get("cas-types") is not None


@pytest.mark.unit
def test_w107_durable_compare_and_delete_preserves_json_type_distinctions(
    tmp_path: Path,
) -> None:
    path = tmp_path / "durable-cas.json"
    store = DurableExecutionStore(path)
    store.create(_record(Spec("durable-cas")))
    expected = store.get("durable-cas")
    assert expected is not None
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["records"]["durable-cas"]["metadata"] = {"value": 1.0}
    path.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")
    try:
        assert store.delete_if_unchanged("durable-cas", expected) is False
        assert store.get("durable-cas") is not None
    finally:
        store.close()


@pytest.mark.unit
def test_w107_atomic_compare_and_delete_does_not_delete_changed_record() -> None:
    store = _memory((Spec("race"),))
    original = store.get("race")
    assert original is not None
    changed = replace(original, metadata={"changed": True})
    store._records["race"] = changed
    assert store.delete_if_unchanged("race", original) is False
    assert store.get("race") == changed


@pytest.mark.unit
def test_w107_empty_dataset(tmp_path: Path) -> None:
    store = MemoryExecutionStore()
    report = _lifecycle(store, _archive(tmp_path)).preview("tenant-A", _policy(dry_run=True))
    assert report.scanned == 0
    assert report.eligible == 0
    assert report.protected == 0
    assert report.archived == 0
    assert report.purged == 0
    assert report.failures == ()
