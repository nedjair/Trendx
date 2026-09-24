"""W108 — explicit archive verification and historical restore contracts."""

from __future__ import annotations

import hashlib
import json
import stat
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient
from loguru import logger
from trendx.forecasting.analytics import analyze_execution_history
from trendx.forecasting.execution import (
    DurableExecutionStore,
    ExecutionProvenance,
    ExecutionRecord,
    ExecutionStatus,
    MemoryExecutionStore,
    RestoreStoreStatus,
)
from trendx.forecasting.history import ExecutionHistoryService, ExecutionQuery
from trendx.forecasting.lifecycle import (
    ArchiveVerification,
    ExecutionRetentionPolicy,
    FileSystemArchiveStore,
    _document_checksum,
    _record_checksum,
)
from trendx.forecasting.recovery import (
    ExecutionRestoreOutcome,
    ExecutionRestoreRequest,
    ExecutionRestoreResult,
    ExecutionRestoreStatus,
    RecoveryService,
    RestoreConflictPolicy,
)
from trendx.forecasting.reporting import ExecutionAnalyticsReportingService
from trendx.main import app

NOW = datetime(2026, 3, 2, tzinfo=UTC)
POLICY = ExecutionRetentionPolicy(retention_days=30, dry_run=False)


def _terminal_record(
    execution_id: str = "restore-me",
    tenant_id: str = "tenant-A",
) -> ExecutionRecord:
    store = MemoryExecutionStore()
    record = ExecutionRecord(
        execution_id=execution_id,
        reference_key=f"w108:{execution_id}",
        tenant_id=tenant_id,
        entity_type="DEVICE",
        entity_id=f"device-{execution_id}",
        target_metric="temperature",
        frequency="1h",
        horizon=24,
        model_id="model-w108",
        model_version="1",
        algorithm="Fourier",
        feature_schema_version="schema-w108",
        feature_schema_fingerprint="fingerprint-w108",
        artifact_uri="file:///w108/model.bin",
        created_at="2025-01-01T00:00:00Z",
        metadata={"source": "w108", "sequence": 3},
        result={"values": [1.0, 2.0, 3.0]},
    )
    store.create(record)
    store.mark_started(execution_id)
    provenance = ExecutionProvenance(
        model_id=record.model_id,
        model_version=record.model_version,
        algorithm=record.algorithm,
        feature_schema_version=record.feature_schema_version,
        feature_schema_fingerprint=record.feature_schema_fingerprint,
        artifact_uri=record.artifact_uri,
        prediction_count=3,
        metadata={"source": "w108"},
        result=record.result,
    )
    restored = store.mark_success(
        execution_id,
        provenance,
        completed_at="2099-01-01T00:01:00Z",
    )
    return restored


def _archive(
    record: ExecutionRecord,
    tmp_path: Path,
) -> tuple[FileSystemArchiveStore, str]:
    archive = FileSystemArchiveStore(tmp_path / "archive")
    archive.archive((record,), policy=POLICY, archived_at=NOW)
    checksum = archive.verify(record.execution_id).checksum
    assert checksum is not None
    return archive, checksum


def _request(
    record: ExecutionRecord,
    checksum: str,
    tenant_id: str | None = None,
) -> ExecutionRestoreRequest:
    return ExecutionRestoreRequest(
        execution_id=record.execution_id,
        tenant_id=tenant_id or record.tenant_id,
        expected_archive_checksum=checksum,
    )


def _service(
    store: MemoryExecutionStore | DurableExecutionStore,
    archive: FileSystemArchiveStore,
) -> RecoveryService:
    return RecoveryService(store, archive, lambda: NOW)


@pytest.mark.unit
def test_w108_restore_contract() -> None:
    request = ExecutionRestoreRequest(
        execution_id="contract",
        tenant_id="tenant-A",
        expected_archive_checksum="a" * 64,
        conflict_policy="FAIL_IF_EXISTS",
    )
    assert request.conflict_policy is RestoreConflictPolicy.FAIL_IF_EXISTS
    assert request.to_dict()["conflict_policy"] == "FAIL_IF_EXISTS"
    assert ExecutionRestoreStatus.RESTORED.value == "RESTORED"
    assert ExecutionRestoreOutcome.NO_CHANGE.value == "NO_CHANGE"
    with pytest.raises(ValueError):
        ExecutionRestoreRequest("", "tenant-A", "a" * 64)
    with pytest.raises(ValueError):
        ExecutionRestoreRequest("id", "", "a" * 64)
    with pytest.raises(ValueError):
        ExecutionRestoreRequest("id", "tenant-A", "")
    with pytest.raises(ValueError):
        ExecutionRestoreRequest("id", "tenant-A", "a" * 64, "FORCE_OVERWRITE")  # type: ignore[arg-type]


@pytest.mark.unit
def test_w108_verify_archive(tmp_path: Path) -> None:
    record = _terminal_record()
    archive, checksum = _archive(record, tmp_path)
    target = MemoryExecutionStore()
    result = _service(target, archive).verify_archive(_request(record, checksum))
    assert isinstance(result, ExecutionRestoreResult)
    assert result.status is ExecutionRestoreStatus.VERIFIED
    assert result.outcome is ExecutionRestoreOutcome.NO_CHANGE
    assert result.archive_verified is True
    assert result.record_restored is False
    assert result.metadata is not None
    assert result.metadata.archive_checksum == checksum
    assert target.list() == ()


@pytest.mark.unit
def test_w108_restore_success(tmp_path: Path) -> None:
    record = _terminal_record()
    archive, checksum = _archive(record, tmp_path)
    target = MemoryExecutionStore()
    result = _service(target, archive).restore(_request(record, checksum))
    assert result.status is ExecutionRestoreStatus.RESTORED
    assert result.outcome is ExecutionRestoreOutcome.RESTORED
    assert result.record_restored is True
    assert target.get(record.execution_id) == record
    payload = result.to_dict()
    assert payload["status"] == "RESTORED"
    assert payload["archive_verified"] is True
    assert payload["restore_metadata"]["operation"] == "restore"
    assert "record" not in payload
    assert "archive_path" not in payload


@pytest.mark.unit
def test_w108_durable_restore_survives_reopen(tmp_path: Path) -> None:
    record = _terminal_record("durable-reopen")
    archive, checksum = _archive(record, tmp_path)
    active_path = tmp_path / "active.json"
    first_store = DurableExecutionStore(active_path)
    try:
        result = _service(first_store, archive).restore(_request(record, checksum))
        assert result.status is ExecutionRestoreStatus.RESTORED
    finally:
        first_store.close()
    reopened = DurableExecutionStore(active_path)
    try:
        assert reopened.get(record.execution_id) == record
        second = _service(reopened, archive).restore(_request(record, checksum))
        assert second.status is ExecutionRestoreStatus.ALREADY_PRESENT
    finally:
        reopened.close()


@pytest.mark.unit
def test_w108_restore_preserves_identity(tmp_path: Path) -> None:
    record = _terminal_record()
    archive, checksum = _archive(record, tmp_path)
    target = MemoryExecutionStore()
    _service(target, archive).restore(_request(record, checksum))
    restored = target.get(record.execution_id)
    assert restored is not None
    for field in (
        "execution_id",
        "tenant_id",
        "reference_key",
        "entity_type",
        "entity_id",
        "target_metric",
        "status",
        "model_id",
        "model_version",
        "algorithm",
    ):
        assert getattr(restored, field) == getattr(record, field)


@pytest.mark.unit
def test_w108_restore_preserves_historical_timestamps(tmp_path: Path) -> None:
    record = _terminal_record()
    archive, checksum = _archive(record, tmp_path)
    target = MemoryExecutionStore()
    result = _service(target, archive).restore(_request(record, checksum), now=NOW)
    restored = target.get(record.execution_id)
    assert restored is not None
    assert restored.created_at == record.created_at
    assert restored.started_at == record.started_at
    assert restored.completed_at == record.completed_at
    assert result.metadata is not None
    assert result.metadata.restore_timestamp == NOW.isoformat()
    assert result.metadata.restore_timestamp != restored.created_at


@pytest.mark.unit
def test_w108_restore_does_not_modify_archive(tmp_path: Path) -> None:
    record = _terminal_record()
    archive, checksum = _archive(record, tmp_path)
    archive_path = archive._path_for(record.execution_id)  # type: ignore[attr-defined]
    before_hash = hashlib.sha256(archive_path.read_bytes()).hexdigest()
    before_mtime = archive_path.stat().st_mtime_ns
    before_mode = stat.S_IMODE(archive_path.stat().st_mode)
    _service(MemoryExecutionStore(), archive).restore(_request(record, checksum))
    after = archive._path_for(record.execution_id)  # type: ignore[attr-defined]
    assert hashlib.sha256(after.read_bytes()).hexdigest() == before_hash
    assert after.stat().st_mtime_ns == before_mtime
    assert stat.S_IMODE(after.stat().st_mode) == before_mode


@pytest.mark.unit
def test_w108_restore_idempotence(tmp_path: Path) -> None:
    record = _terminal_record()
    archive, checksum = _archive(record, tmp_path)
    target = MemoryExecutionStore()
    service = _service(target, archive)
    first = service.restore(_request(record, checksum))
    second = service.restore(_request(record, checksum))
    assert first.status is ExecutionRestoreStatus.RESTORED
    assert second.status is ExecutionRestoreStatus.ALREADY_PRESENT
    assert second.already_present is True
    assert target.list() == (record,)


@pytest.mark.unit
def test_w108_restore_identical_existing_record(tmp_path: Path) -> None:
    record = _terminal_record()
    archive, checksum = _archive(record, tmp_path)
    target = MemoryExecutionStore()
    assert target.restore_if_absent(record).status is RestoreStoreStatus.INSERTED
    result = _service(target, archive).restore(_request(record, checksum))
    assert result.status is ExecutionRestoreStatus.ALREADY_PRESENT
    assert result.record_restored is False
    assert target.get(record.execution_id) == record


@pytest.mark.unit
def test_w108_restore_conflicting_existing_record(tmp_path: Path) -> None:
    record = _terminal_record()
    archive, checksum = _archive(record, tmp_path)
    target = MemoryExecutionStore()
    conflicting = replace(record, model_id="different-model")
    assert target.restore_if_absent(conflicting).status is RestoreStoreStatus.INSERTED
    result = _service(target, archive).restore(_request(record, checksum))
    assert result.status is ExecutionRestoreStatus.CONFLICT
    assert result.conflict is True
    assert result.record_restored is False
    assert target.get(record.execution_id) == conflicting


@pytest.mark.unit
def test_w108_checksum_mismatch(tmp_path: Path) -> None:
    record = _terminal_record()
    archive, _ = _archive(record, tmp_path)
    target = MemoryExecutionStore()
    result = _service(target, archive).restore(_request(record, "0" * 64))
    assert result.status is ExecutionRestoreStatus.INTEGRITY_FAILURE
    assert result.record_restored is False
    assert target.list() == ()


@pytest.mark.unit
def test_w108_corrupt_archive(tmp_path: Path) -> None:
    record = _terminal_record()
    archive, checksum = _archive(record, tmp_path)
    path = archive._path_for(record.execution_id)  # type: ignore[attr-defined]
    path.write_text(path.read_text(encoding="utf-8") + "corrupt", encoding="utf-8")
    target = MemoryExecutionStore()
    result = _service(target, archive).restore(_request(record, checksum))
    assert result.status in {
        ExecutionRestoreStatus.INTEGRITY_FAILURE,
        ExecutionRestoreStatus.INVALID_ARCHIVE,
    }
    assert result.record_restored is False
    assert target.list() == ()


@pytest.mark.unit
def test_w108_missing_archive(tmp_path: Path) -> None:
    record = _terminal_record()
    archive = FileSystemArchiveStore(tmp_path / "missing-archive")
    target = MemoryExecutionStore()
    result = _service(target, archive).restore(_request(record, "a" * 64))
    assert result.status is ExecutionRestoreStatus.ARCHIVE_NOT_FOUND
    assert result.record_restored is False
    assert target.list() == ()


@pytest.mark.unit
def test_w108_non_terminal_archive_is_rejected(tmp_path: Path) -> None:
    source = _terminal_record("terminal-source")
    record = replace(
        source,
        execution_id="pending-archive",
        reference_key="w108:pending-archive",
        status=ExecutionStatus.PENDING,
        started_at="",
        completed_at="",
        prediction_count=None,
        result=None,
    )
    archive, checksum = _archive(record, tmp_path)
    target = MemoryExecutionStore()
    result = _service(target, archive).restore(_request(record, checksum))
    assert result.status is ExecutionRestoreStatus.INVALID_ARCHIVE
    assert result.reason == "non_terminal_archive"
    assert target.list() == ()


@pytest.mark.unit
def test_w108_tenant_isolation(tmp_path: Path) -> None:
    record = _terminal_record(tenant_id="tenant-A")
    archive, checksum = _archive(record, tmp_path)
    target = MemoryExecutionStore()
    result = _service(target, archive).restore(_request(record, checksum, "tenant-B"))
    assert result.status is ExecutionRestoreStatus.TENANT_FORBIDDEN
    assert result.record_restored is False
    assert target.list() == ()


@pytest.mark.unit
def test_w108_cross_tenant_execution_collision_is_rejected(tmp_path: Path) -> None:
    record = _terminal_record("cross-tenant", tenant_id="tenant-A")
    archive, checksum = _archive(record, tmp_path)
    target = MemoryExecutionStore()
    other_tenant = replace(record, tenant_id="tenant-B")
    assert target.restore_if_absent(other_tenant).status is RestoreStoreStatus.INSERTED
    result = _service(target, archive).restore(_request(record, checksum))
    assert result.status is ExecutionRestoreStatus.CONFLICT
    assert result.reason == "execution_id_conflict"
    assert target.get(record.execution_id) == other_tenant
    assert target.get(record.execution_id).tenant_id == "tenant-B"


@pytest.mark.unit
def test_w108_protected_identity(tmp_path: Path) -> None:
    record = _terminal_record()
    archive, checksum = _archive(record, tmp_path)
    target = MemoryExecutionStore()
    request = _request(record, checksum)
    result = _service(target, archive).restore(request)
    restored = target.get(record.execution_id)
    assert result.status is ExecutionRestoreStatus.RESTORED
    assert restored is not None
    assert restored.reference_key == record.reference_key
    assert restored.created_at == record.created_at
    assert restored.completed_at == record.completed_at
    assert not hasattr(request, "reference_key")
    assert not hasattr(request, "created_at")


@pytest.mark.unit
def test_w108_concurrent_restore(tmp_path: Path) -> None:
    record = _terminal_record()
    archive, checksum = _archive(record, tmp_path)
    target = MemoryExecutionStore()
    services = [_service(target, archive), _service(target, archive)]
    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(
            executor.map(
                lambda service: service.restore(_request(record, checksum)),
                services,
            )
        )
    assert sorted(result.status.value for result in results) == [
        "ALREADY_PRESENT",
        "RESTORED",
    ]
    assert target.list() == (record,)


@pytest.mark.unit
def test_w108_w99_compatibility(tmp_path: Path) -> None:
    record = _terminal_record()
    archive, checksum = _archive(record, tmp_path)
    target = MemoryExecutionStore()
    history = ExecutionHistoryService(target)
    query = ExecutionQuery(
        tenant_id=record.tenant_id,
        entity_type=record.entity_type,
        target_metric=record.target_metric,
    )
    assert history.count(query) == 0
    _service(target, archive).restore(_request(record, checksum))
    assert history.count(query) == 1
    assert history.get(record.execution_id) == record
    assert history.list_records(query)[0].reference_key == record.reference_key


@pytest.mark.unit
def test_w108_w104_compatibility(tmp_path: Path) -> None:
    record = _terminal_record()
    archive, checksum = _archive(record, tmp_path)
    target = MemoryExecutionStore()
    service = ExecutionHistoryService(target)
    query = ExecutionQuery(tenant_id=record.tenant_id)
    before = analyze_execution_history(service, query)
    _service(target, archive).restore(_request(record, checksum))
    after = analyze_execution_history(service, query)
    assert before.summary.total_executions == 0
    assert after.summary.total_executions == 1
    assert after.summary.prediction_count_total == 3
    assert after.by_status[0].key == "SUCCESS"


@pytest.mark.unit
def test_w108_w106_compatibility(tmp_path: Path) -> None:
    record = _terminal_record()
    archive, checksum = _archive(record, tmp_path)
    target = MemoryExecutionStore()
    service = ExecutionHistoryService(target)
    query = ExecutionQuery(tenant_id=record.tenant_id)
    reporting = ExecutionAnalyticsReportingService(service)
    before = reporting.report(query)
    _service(target, archive).restore(_request(record, checksum))
    after = reporting.report(query)
    assert before.summary.total_executions == 0
    assert after.summary.total_executions == 1
    assert after.tenant_id == record.tenant_id
    assert after.summary.prediction_count_total == 3


@pytest.mark.unit
def test_w108_read_only_verification(tmp_path: Path) -> None:
    record = _terminal_record()
    archive, checksum = _archive(record, tmp_path)
    target = MemoryExecutionStore()
    archive_path = archive._path_for(record.execution_id)  # type: ignore[attr-defined]
    before_archive = archive_path.read_bytes()
    before_records = target.list()
    result = _service(target, archive).verify_archive(_request(record, checksum))
    assert result.status is ExecutionRestoreStatus.VERIFIED
    assert archive_path.read_bytes() == before_archive
    assert target.list() == before_records


@pytest.mark.unit
def test_w108_archive_immutability(tmp_path: Path) -> None:
    record = _terminal_record()
    archive, checksum = _archive(record, tmp_path)
    path = archive._path_for(record.execution_id)  # type: ignore[attr-defined]
    before = (hashlib.sha256(path.read_bytes()).hexdigest(), path.stat().st_mtime_ns)
    target = MemoryExecutionStore()
    _service(target, archive).restore(_request(record, checksum))
    after = (hashlib.sha256(path.read_bytes()).hexdigest(), path.stat().st_mtime_ns)
    assert before == after


@pytest.mark.unit
@pytest.mark.parametrize(
    "execution_id", ["../escape", "/absolute/path", "a\x00b", "tenant/../../escape"]
)
def test_w108_path_security(tmp_path: Path, execution_id: str) -> None:
    record = _terminal_record(execution_id)
    archive, checksum = _archive(record, tmp_path)
    target = MemoryExecutionStore()
    result = _service(target, archive).restore(_request(record, checksum))
    assert result.status is ExecutionRestoreStatus.RESTORED
    path = archive._path_for(execution_id)  # type: ignore[attr-defined]
    assert path.parent == archive.root


@pytest.mark.unit
def test_w108_archive_security_variants(tmp_path: Path) -> None:
    def reject_case(
        case: str,
        *,
        transform: Any = None,
        symlink: bool = False,
        mode: int | None = None,
    ) -> None:
        record = _terminal_record(case)
        archive, checksum = _archive(record, tmp_path / case)
        path = archive._path_for(record.execution_id)  # type: ignore[attr-defined]
        if transform is not None:
            transform(path)
        if symlink:
            outside = tmp_path / f"{case}-outside.json"
            outside.write_bytes(path.read_bytes())
            path.unlink()
            path.symlink_to(outside)
        if mode is not None:
            path.chmod(mode)
        target = MemoryExecutionStore()
        result = _service(target, archive).restore(_request(record, checksum))
        assert result.record_restored is False
        assert target.list() == ()
        assert result.status in {
            ExecutionRestoreStatus.ARCHIVE_NOT_FOUND,
            ExecutionRestoreStatus.INVALID_ARCHIVE,
            ExecutionRestoreStatus.INTEGRITY_FAILURE,
        }

    def malformed(path: Path) -> None:
        path.write_text("{not-json", encoding="utf-8")

    def extra_field(path: Path) -> None:
        payload = json.loads(path.read_text(encoding="utf-8"))
        payload["extra"] = True
        path.write_text(json.dumps(payload), encoding="utf-8")

    def missing_field(path: Path) -> None:
        payload = json.loads(path.read_text(encoding="utf-8"))
        del payload["record"]["model_id"]
        path.write_text(json.dumps(payload), encoding="utf-8")

    def unknown_version(path: Path) -> None:
        payload = json.loads(path.read_text(encoding="utf-8"))
        payload["archive_format_version"] = 999
        path.write_text(json.dumps(payload), encoding="utf-8")

    def nan_value(path: Path) -> None:
        payload = json.loads(path.read_text(encoding="utf-8"))
        payload["record"]["metadata"]["nan"] = float("nan")
        path.write_text(json.dumps(payload), encoding="utf-8")

    def infinity_value(path: Path) -> None:
        payload = json.loads(path.read_text(encoding="utf-8"))
        payload["record"]["metadata"]["infinity"] = float("inf")
        path.write_text(json.dumps(payload), encoding="utf-8")

    reject_case("malformed", transform=malformed)
    reject_case("extra", transform=extra_field)
    reject_case("missing", transform=missing_field)
    reject_case("version", transform=unknown_version)
    reject_case("nan", transform=nan_value)
    reject_case("infinity", transform=infinity_value)
    reject_case("symlink", symlink=True)
    reject_case("permissions", mode=0o644)


@pytest.mark.unit
def test_w108_execution_identity_collision_is_rejected(tmp_path: Path) -> None:
    record = _terminal_record("identity")
    archive, _ = _archive(record, tmp_path)
    path = archive._path_for(record.execution_id)  # type: ignore[attr-defined]
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["record"]["execution_id"] = "forged"
    payload["metadata"]["execution_id"] = "forged"
    forged = ExecutionRecord.from_dict(payload["record"])
    payload["metadata"]["record_checksum"] = _record_checksum(forged)
    payload["content_checksum"] = _document_checksum(payload["metadata"], payload["record"])
    path.write_text(json.dumps(payload), encoding="utf-8")
    checksum = archive.verify(record.execution_id).checksum
    assert checksum is not None
    target = MemoryExecutionStore()
    result = _service(target, archive).restore(_request(record, checksum))
    assert result.status is ExecutionRestoreStatus.INVALID_ARCHIVE
    assert result.reason == "execution_id_mismatch"
    assert target.list() == ()


@pytest.mark.unit
def test_w108_archive_metadata_tenant_mismatch_is_rejected(tmp_path: Path) -> None:
    record = _terminal_record("metadata-tenant")
    archive, checksum = _archive(record, tmp_path)
    path = archive._path_for(record.execution_id)  # type: ignore[attr-defined]
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["metadata"]["tenant_id"] = "tenant-forged"
    payload["content_checksum"] = _document_checksum(payload["metadata"], payload["record"])
    path.write_text(json.dumps(payload), encoding="utf-8")
    target = MemoryExecutionStore()
    result = _service(target, archive).restore(_request(record, checksum))
    assert result.status is ExecutionRestoreStatus.INVALID_ARCHIVE
    assert result.record_restored is False
    assert target.list() == ()


@pytest.mark.unit
def test_w108_reference_key_collision_is_rejected(tmp_path: Path) -> None:
    record = _terminal_record("reference-collision")
    archive, checksum = _archive(record, tmp_path)
    target = MemoryExecutionStore()
    other = replace(record, execution_id="other-execution")
    assert target.restore_if_absent(other).status is RestoreStoreStatus.INSERTED
    result = _service(target, archive).restore(_request(record, checksum))
    assert result.status is ExecutionRestoreStatus.CONFLICT
    assert result.reason == "reference_key_conflict"
    assert target.get(other.execution_id) == other
    assert target.get(record.execution_id) is None


@pytest.mark.unit
def test_w108_store_failure_is_controlled(tmp_path: Path) -> None:
    class FailingStore(MemoryExecutionStore):
        def restore_if_absent(self, record: ExecutionRecord) -> Any:
            raise RuntimeError("backend unavailable")

    record = _terminal_record("store-failure")
    archive, checksum = _archive(record, tmp_path)
    target = FailingStore()
    result = _service(target, archive).restore(_request(record, checksum))
    assert result.status is ExecutionRestoreStatus.RESTORE_FAILED
    assert result.record_restored is False
    assert target.list() == ()


@pytest.mark.unit
def test_w108_openapi() -> None:
    with TestClient(app) as client:
        response = client.get("/openapi.json")
    assert response.status_code == 200
    paths = response.json()["paths"]
    assert "/api/v1/forecast/executions/restore" not in paths
    assert "delete" not in paths.get("/api/v1/forecast/executions/restore", {})
    assert "get" not in paths.get("/api/v1/forecast/executions/restore", {})


@pytest.mark.unit
def test_w108_error_sanitization(tmp_path: Path) -> None:
    secret_path = "/private/archive/secret-path"

    class LeakyArchiveStore:
        def exists(self, execution_id: str) -> bool:
            return True

        def verify(self, execution_id: str, **_: Any) -> ArchiveVerification:
            return ArchiveVerification(valid=False, error_code=secret_path)

        def read(self, execution_id: str) -> Any:
            raise AssertionError("read must not occur")

        def archive(self, records: Any, **_: Any) -> Any:
            raise AssertionError("restore must not archive")

    record = _terminal_record()
    target = MemoryExecutionStore()
    result = _service(target, LeakyArchiveStore()).restore(  # type: ignore[arg-type]
        _request(record, "a" * 64)
    )
    serialized = json.dumps(result.to_dict())
    assert result.status is ExecutionRestoreStatus.INVALID_ARCHIVE
    assert secret_path not in serialized


@pytest.mark.unit
def test_w108_archive_adapter_outputs_are_validated(tmp_path: Path) -> None:
    record = _terminal_record("adapter")
    request = _request(record, "a" * 64)

    class MissingVerification:
        def exists(self, execution_id: str) -> bool:
            return True

        def verify(self, execution_id: str, **_: Any) -> ArchiveVerification:
            return ArchiveVerification(valid=False, error_code="missing_archive")

        def read(self, execution_id: str) -> Any:
            raise AssertionError("read must not occur")

        def archive(self, records: Any, **_: Any) -> Any:
            raise AssertionError("archive must not occur")

    result = _service(MemoryExecutionStore(), MissingVerification()).restore(request)  # type: ignore[arg-type]
    assert result.status is ExecutionRestoreStatus.ARCHIVE_NOT_FOUND

    class InvalidChecksum:
        def exists(self, execution_id: str) -> bool:
            return True

        def verify(self, execution_id: str, **_: Any) -> ArchiveVerification:
            return ArchiveVerification(valid=True, checksum="not-a-sha256")

        def read(self, execution_id: str) -> Any:
            raise AssertionError("read must not occur")

        def archive(self, records: Any, **_: Any) -> Any:
            raise AssertionError("archive must not occur")

    result = _service(MemoryExecutionStore(), InvalidChecksum()).restore(request)  # type: ignore[arg-type]
    assert result.status is ExecutionRestoreStatus.INTEGRITY_FAILURE
    assert result.archive_verified is False

    secret = "adapter-secret-payload"

    class LeakyRead:
        def exists(self, execution_id: str) -> bool:
            return True

        def verify(self, execution_id: str, **_: Any) -> ArchiveVerification:
            return ArchiveVerification(valid=True, checksum="a" * 64)

        def read(self, execution_id: str) -> Any:
            raise RuntimeError(secret)

        def archive(self, records: Any, **_: Any) -> Any:
            raise AssertionError("archive must not occur")

    result = _service(MemoryExecutionStore(), LeakyRead()).restore(request)  # type: ignore[arg-type]
    assert result.status is ExecutionRestoreStatus.INVALID_ARCHIVE
    assert secret not in json.dumps(result.to_dict())


@pytest.mark.unit
def test_w108_observability(tmp_path: Path) -> None:
    record = _terminal_record()
    archive, checksum = _archive(record, tmp_path)
    events: list[str] = []
    sink_id = logger.add(events.append, format="{message}")
    try:
        _service(MemoryExecutionStore(), archive).restore(_request(record, checksum))
    finally:
        logger.remove(sink_id)
    output = "\n".join(events)
    assert "operation=restore" in output
    assert "outcome=success" in output
    assert str(tmp_path) not in output
    for forbidden in ("authorization", "token", "api key", "payload", "traceback"):
        assert forbidden not in output.lower()
