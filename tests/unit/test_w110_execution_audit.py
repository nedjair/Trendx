"""W110 — durable, tenant-safe execution operational audit tests."""

from __future__ import annotations

import hashlib
import json
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient
from pydantic import SecretStr
from trendx.config import settings
from trendx.forecasting.api import (
    TenantContext,
    get_execution_audit_service,
    get_execution_recovery_service,
    get_tenant_context,
)
from trendx.forecasting.audit import (
    AUDIT_EVENT_VERSION,
    AuditActorType,
    AuditEventVersionError,
    AuditFailurePolicy,
    AuditIdempotencyConflictError,
    AuditIntegrityError,
    AuditOperation,
    AuditOutcome,
    AuditStoreError,
    ExecutionAuditEvent,
    ExecutionAuditQuery,
    ExecutionAuditService,
    ExecutionAuditStore,
    MemoryExecutionAuditStore,
)
from trendx.forecasting.execution import (
    DurableExecutionStore,
    ExecutionProvenance,
    ExecutionRecord,
    ExecutionStatus,
)
from trendx.forecasting.lifecycle import (
    ExecutionHistoryLifecycle,
    ExecutionRetentionPolicy,
    FileSystemArchiveStore,
)
from trendx.forecasting.recovery import (
    ExecutionRestoreOutcome,
    ExecutionRestoreResult,
    ExecutionRestoreStatus,
)
from trendx.main import app

NOW = datetime(2026, 4, 1, tzinfo=UTC)


def _event(
    event_id: str,
    *,
    tenant_id: str = "tenant-A",
    execution_id: str = "exec-A",
    operation: AuditOperation = AuditOperation.EXECUTION,
    outcome: AuditOutcome = AuditOutcome.SUCCESS,
    request_id: str = "request-A",
    occurred_at: datetime = NOW,
    reference_key: str | None = "ref:exec-A",
) -> ExecutionAuditEvent:
    return ExecutionAuditEvent(
        event_id=event_id,
        occurred_at=occurred_at,
        operation=operation,
        outcome=outcome,
        tenant_id=tenant_id,
        execution_id=execution_id,
        reference_key=reference_key,
        actor_type=AuditActorType.SYSTEM,
        actor_id="system",
        request_id=request_id,
        source="w110-test",
        reason_code="test_event",
    )


def _record(execution_id: str = "exec-A", tenant_id: str = "tenant-A") -> ExecutionRecord:
    return ExecutionRecord(
        execution_id=execution_id,
        reference_key=f"ref:{execution_id}",
        tenant_id=tenant_id,
        entity_type="DEVICE",
        entity_id=f"device-{execution_id}",
        target_metric="temperature",
        frequency="1h",
        horizon=1,
        status=ExecutionStatus.SUCCESS,
        created_at="2020-01-01T00:00:00+00:00",
        started_at="2020-01-01T00:00:01+00:00",
        completed_at="2020-01-01T00:00:02+00:00",
        model_id="model-A",
        model_version="1",
        algorithm="Fourier",
    )


def _auth_headers() -> dict[str, str]:
    return {"Authorization": f"Bearer {settings.trendx_api_token.get_secret_value()}"}


@pytest.mark.unit
def test_w110_audit_event_contract() -> None:
    event = _event("event-contract")
    assert event.event_version == AUDIT_EVENT_VERSION == "1"
    assert event.operation is AuditOperation.EXECUTION
    assert event.outcome is AuditOutcome.SUCCESS
    assert event.tenant_id == "tenant-A"
    assert event.execution_id == "exec-A"
    assert event.reference_key == "ref:exec-A"
    assert event.actor_type is AuditActorType.SYSTEM
    assert event.actor_id == "system"
    assert event.request_id == "request-A"
    assert event.source == "w110-test"
    assert event.reason_code == "test_event"


@pytest.mark.unit
def test_w110_event_version() -> None:
    with pytest.raises(AuditEventVersionError):
        ExecutionAuditEvent(
            event_id="bad-version",
            occurred_at=NOW,
            operation=AuditOperation.EXECUTION,
            outcome=AuditOutcome.SUCCESS,
            tenant_id="tenant-A",
            execution_id=None,
            reference_key=None,
            actor_type=AuditActorType.SYSTEM,
            actor_id="system",
            request_id="request-A",
            source="test",
            reason_code="test",
            event_version="2",
        )


@pytest.mark.unit
def test_w110_store_missing_and_symlink_are_rejected(tmp_path: Path) -> None:
    missing = tmp_path / "missing-audit"
    with pytest.raises(AuditStoreError):
        ExecutionAuditStore(missing, create_if_missing=False)
    assert not missing.exists()
    target = tmp_path / "target-audit"
    target.mkdir()
    linked = tmp_path / "linked-audit"
    linked.symlink_to(target, target_is_directory=True)
    with pytest.raises(AuditStoreError):
        ExecutionAuditStore(linked)


@pytest.mark.unit
def test_w110_append(tmp_path: Path) -> None:
    store = ExecutionAuditStore(tmp_path / "audit")
    persisted = store.append(_event("append-1"))
    assert persisted.checksum is not None
    assert len(persisted.checksum) == 64
    assert store.get("append-1") == persisted


@pytest.mark.unit
def test_w110_get(tmp_path: Path) -> None:
    store = ExecutionAuditStore(tmp_path / "audit")
    event = store.append(_event("get-1"))
    assert store.get(event.event_id, tenant_id="tenant-A") == event
    assert store.get("missing", tenant_id="tenant-A") is None


@pytest.mark.unit
def test_w110_query(tmp_path: Path) -> None:
    store = ExecutionAuditStore(tmp_path / "audit")
    store.append(_event("query-1", occurred_at=NOW))
    store.append(
        _event(
            "query-2",
            outcome=AuditOutcome.FAILED,
            operation=AuditOperation.RESTORE,
            execution_id="exec-B",
            request_id="request-B",
            occurred_at=NOW + timedelta(seconds=1),
        )
    )
    page = store.query(
        ExecutionAuditQuery(
            tenant_id="tenant-A",
            operation=AuditOperation.RESTORE,
            limit=1,
            offset=0,
        )
    )
    assert page.total == 1
    assert page.events[0].event_id == "query-2"
    assert page.has_more is False


@pytest.mark.unit
def test_w110_statistics(tmp_path: Path) -> None:
    store = ExecutionAuditStore(tmp_path / "audit")
    store.append(_event("stats-1", request_id="stats-request-1"))
    store.append(
        _event(
            "stats-2",
            outcome=AuditOutcome.CONFLICT,
            request_id="stats-request-2",
        )
    )
    store.append(
        _event(
            "stats-3",
            tenant_id="tenant-B",
            execution_id="exec-B",
            outcome=AuditOutcome.FORBIDDEN,
            request_id="stats-request-3",
        )
    )
    stats = store.statistics(ExecutionAuditQuery(tenant_id="tenant-A"))
    assert stats.total_events == 2
    assert stats.successful_events == 1
    assert stats.conflicts == 1
    assert stats.by_tenant == {"tenant-A": 2}
    assert stats.by_operation == {"EXECUTION": 2}


@pytest.mark.unit
def test_w110_idempotence(tmp_path: Path) -> None:
    store = ExecutionAuditStore(tmp_path / "audit")
    first = store.append(_event("idem-1", request_id="same-request"))
    second = store.append(_event("idem-1", request_id="same-request"))
    assert first == second
    assert store.query().total == 1
    changed = replace(_event("idem-1", request_id="same-request"), reason_code="changed")
    with pytest.raises(AuditIdempotencyConflictError):
        store.append(changed)


@pytest.mark.unit
def test_w110_tenant_isolation(tmp_path: Path) -> None:
    store = ExecutionAuditStore(tmp_path / "audit")
    store.append(_event("tenant-event", tenant_id="tenant-B", execution_id="exec-B"))
    assert store.get("tenant-event", tenant_id="tenant-A") is None
    assert store.query(ExecutionAuditQuery(tenant_id="tenant-A")).total == 0
    assert store.query(ExecutionAuditQuery(tenant_id="tenant-B")).total == 1


@pytest.mark.unit
def test_w110_integrity(tmp_path: Path) -> None:
    store = ExecutionAuditStore(tmp_path / "audit")
    event = store.append(_event("integrity-1"))
    event.verify_integrity()
    payload = event.to_dict()
    payload["reason_code"] = "tampered"
    with pytest.raises(AuditIntegrityError):
        ExecutionAuditEvent.from_dict(payload)


@pytest.mark.unit
def test_w110_corrupted_event(tmp_path: Path) -> None:
    store = ExecutionAuditStore(tmp_path / "audit")
    store.append(_event("corrupt-1"))
    event_path = next(store.path.glob("*.json"))
    event_path.write_text("{broken", encoding="utf-8")
    with pytest.raises(AuditIntegrityError):
        store.query()


@pytest.mark.unit
def test_w110_cached_query_rechecks_integrity_and_identity(tmp_path: Path) -> None:
    store = ExecutionAuditStore(tmp_path / "audit")
    event = store.append(_event("cache-1"))
    assert store.query().total == 1
    event_path = next(store.path.glob("*.json"))
    event_path.chmod(0o644)
    with pytest.raises(AuditIntegrityError):
        store.query()
    event_path.chmod(0o600)
    event_path.write_text(
        json.dumps(event.to_dict(), sort_keys=False) + "\n",
        encoding="utf-8",
    )
    with pytest.raises(AuditIntegrityError):
        store.query()


@pytest.mark.unit
def test_w110_get_rejects_symlink_and_filename_mismatch(tmp_path: Path) -> None:
    store = ExecutionAuditStore(tmp_path / "audit")
    event = store.append(_event("symlink-1"))
    event_path = next(store.path.glob("*.json"))
    event_path.unlink()
    event_path.symlink_to(tmp_path / "outside.json")
    (tmp_path / "outside.json").write_text("{}", encoding="utf-8")
    with pytest.raises(AuditIntegrityError):
        store.get(event.event_id)
    event_path.unlink()
    mismatched = store.path / ("0" * 64 + ".json")
    mismatched.write_text(
        json.dumps(event.to_dict(), separators=(",", ":")) + "\n",
        encoding="utf-8",
    )
    with pytest.raises(AuditIntegrityError):
        store.query()


@pytest.mark.unit
def test_w110_unknown_version(tmp_path: Path) -> None:
    store = ExecutionAuditStore(tmp_path / "audit")
    store.append(_event("version-1"))
    event_path = next(store.path.glob("*.json"))
    payload = json.loads(event_path.read_text(encoding="utf-8"))
    payload["event_version"] = "999"
    event_path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(AuditEventVersionError):
        store.query()


@pytest.mark.unit
def test_w110_secret_sanitization(tmp_path: Path) -> None:
    store = ExecutionAuditStore(tmp_path / "audit")
    service = ExecutionAuditService(store)
    service.record(
        operation=AuditOperation.RESTORE,
        outcome=AuditOutcome.FAILED,
        tenant_id="tenant-A",
        execution_id="../../outside",
        reference_key="/opt/trendx/private/archive.json",
        actor_id="api-key-context",
        request_id="request-A",
        source="w110-test",
        reason_code="Authorization Bearer super-secret-token",
    )
    raw = next(store.path.glob("*.json")).read_text(encoding="utf-8")
    assert "super-secret-token" not in raw
    assert "/opt/trendx" not in raw
    assert "outside" not in raw
    assert "Authorization" not in raw


@pytest.mark.unit
def test_w110_read_only_query(tmp_path: Path) -> None:
    store = ExecutionAuditStore(tmp_path / "audit")
    store.append(_event("readonly-1", request_id="readonly-request-1"))
    store.append(
        _event(
            "readonly-2",
            execution_id="exec-B",
            request_id="readonly-request-2",
            operation=AuditOperation.RESTORE,
            outcome=AuditOutcome.FAILED,
        )
    )
    before = {
        path.name: hashlib.sha256(path.read_bytes()).hexdigest()
        for path in store.path.glob("*.json")
    }
    store.query(
        ExecutionAuditQuery(
            tenant_id="tenant-A",
            operation=AuditOperation.RESTORE,
            limit=1,
            offset=0,
        )
    )
    store.query(ExecutionAuditQuery(tenant_id="tenant-A", limit=1, offset=1))
    store.statistics(ExecutionAuditQuery(tenant_id="tenant-A"))
    after = {
        path.name: hashlib.sha256(path.read_bytes()).hexdigest()
        for path in store.path.glob("*.json")
    }
    assert before == after


def _restore_result(status: ExecutionRestoreStatus, reason: str) -> ExecutionRestoreResult:
    return ExecutionRestoreResult(
        execution_id="exec-A",
        tenant_id="tenant-A",
        status=status,
        outcome=ExecutionRestoreOutcome.RESTORED,
        archive_verified=True,
        record_restored=status is ExecutionRestoreStatus.RESTORED,
        already_present=status is ExecutionRestoreStatus.ALREADY_PRESENT,
        conflict=status is ExecutionRestoreStatus.CONFLICT,
        reason=reason,
    )


@pytest.mark.unit
def test_w110_restore_audit(tmp_path: Path) -> None:
    service = ExecutionAuditService(ExecutionAuditStore(tmp_path / "audit"))
    service.record_restore_result(
        _restore_result(ExecutionRestoreStatus.RESTORED, "record_restored"),
        request_id="restore-request",
    )
    page = service.query(ExecutionAuditQuery(operation=AuditOperation.RESTORE))
    assert page.total == 1
    assert page.events[0].outcome is AuditOutcome.SUCCESS


@pytest.mark.unit
def test_w110_w109_failure_statuses_are_audited(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(settings, "trendx_api_token", SecretStr("w110-status-token"))
    audit = ExecutionAuditService(MemoryExecutionAuditStore())

    class StatusRecovery:
        def __init__(self, status: ExecutionRestoreStatus, reason: str) -> None:
            self.status = status
            self.reason = reason

        def restore(self, _request: Any) -> ExecutionRestoreResult:
            return ExecutionRestoreResult(
                execution_id="audit-http-failure",
                tenant_id="tenant-A",
                status=self.status,
                outcome=ExecutionRestoreOutcome.REJECTED,
                archive_verified=False,
                record_restored=False,
                already_present=False,
                conflict=False,
                reason=self.reason,
            )

    previous_factory = app.state.execution_audit_service_factory
    app.state.execution_audit_service_factory = lambda: audit
    app.dependency_overrides[get_execution_audit_service] = lambda: audit
    app.dependency_overrides[get_tenant_context] = lambda: TenantContext("tenant-A")
    app.dependency_overrides[get_execution_recovery_service] = lambda: StatusRecovery(
        ExecutionRestoreStatus.ARCHIVE_NOT_FOUND,
        "archive_not_found",
    )
    try:
        with TestClient(app) as client:
            payload = {
                "execution_id": "audit-http-failure",
                "tenant_id": "tenant-A",
                "expected_archive_checksum": "0" * 64,
                "conflict_policy": "FAIL_IF_EXISTS",
            }
            response = client.post(
                "/api/v1/forecast/executions/restore",
                json=payload,
                headers=_auth_headers(),
            )
            assert response.status_code == 404
            app.dependency_overrides[get_execution_recovery_service] = lambda: StatusRecovery(
                ExecutionRestoreStatus.RESTORE_FAILED,
                "active_store_write_failed",
            )
            response = client.post(
                "/api/v1/forecast/executions/restore",
                json=payload,
                headers=_auth_headers(),
            )
            assert response.status_code == 503
    finally:
        app.state.execution_audit_service_factory = previous_factory
        app.dependency_overrides.clear()

    events = audit.store.query(ExecutionAuditQuery(operation=AuditOperation.RECOVERY_API)).events
    assert {event.outcome for event in events} == {
        AuditOutcome.REJECTED,
        AuditOutcome.FAILED,
    }


@pytest.mark.unit
def test_w110_conflict_audit(tmp_path: Path) -> None:
    service = ExecutionAuditService(ExecutionAuditStore(tmp_path / "audit"))
    service.record_restore_result(
        _restore_result(ExecutionRestoreStatus.CONFLICT, "execution_id_conflict"),
        request_id="restore-request",
    )
    assert service.query(ExecutionAuditQuery(outcome=AuditOutcome.CONFLICT)).total == 1


@pytest.mark.unit
def test_w110_forbidden_audit(tmp_path: Path) -> None:
    service = ExecutionAuditService(ExecutionAuditStore(tmp_path / "audit"))
    service.record_restore_result(
        _restore_result(ExecutionRestoreStatus.TENANT_FORBIDDEN, "tenant_forbidden"),
        request_id="restore-request",
    )
    assert service.query(ExecutionAuditQuery(outcome=AuditOutcome.FORBIDDEN)).total == 1


@pytest.mark.unit
def test_w110_execution_transition_audit(tmp_path: Path) -> None:
    from trendx.forecasting.execution import MemoryExecutionStore

    audit = ExecutionAuditService(ExecutionAuditStore(tmp_path / "audit"))
    history = MemoryExecutionStore(audit_service=audit)
    pending = replace(_record(), status=ExecutionStatus.PENDING, started_at="", completed_at="")
    history.create(pending)
    history.mark_started(pending.execution_id)
    history.mark_success(
        pending.execution_id,
        ExecutionProvenance(
            model_id="model-A",
            model_version="1",
            algorithm="Fourier",
        ),
    )
    assert audit.query(ExecutionAuditQuery(operation=AuditOperation.EXECUTION)).total == 1


@pytest.mark.unit
def test_w110_archive_audit(tmp_path: Path) -> None:
    history_path = tmp_path / "history.json"
    history = DurableExecutionStore(history_path)
    record = _record()
    history.restore_if_absent(record)
    archive = FileSystemArchiveStore(tmp_path / "archive")
    audit = ExecutionAuditService(ExecutionAuditStore(tmp_path / "audit"))
    lifecycle = ExecutionHistoryLifecycle(history, archive, lambda: NOW, audit_service=audit)
    receipt = lifecycle.archive_execution(
        record.execution_id,
        record.tenant_id,
        ExecutionRetentionPolicy(retention_days=0, dry_run=False),
        now=NOW,
        request_id="archive-request",
    )
    assert receipt.created is True
    assert audit.query(ExecutionAuditQuery(operation=AuditOperation.ARCHIVE)).total >= 1


@pytest.mark.unit
def test_w110_purge_audit(tmp_path: Path) -> None:
    history_path = tmp_path / "history.json"
    history = DurableExecutionStore(history_path)
    record = _record()
    history.restore_if_absent(record)
    archive = FileSystemArchiveStore(tmp_path / "archive")
    audit = ExecutionAuditService(ExecutionAuditStore(tmp_path / "audit"))
    lifecycle = ExecutionHistoryLifecycle(history, archive, lambda: NOW, audit_service=audit)
    report = lifecycle.purge_eligible(
        record.tenant_id,
        ExecutionRetentionPolicy(retention_days=0, dry_run=False),
        now=NOW,
        request_id="purge-request",
    )
    assert report.purged == 1
    assert audit.query(ExecutionAuditQuery(operation=AuditOperation.PURGE)).total >= 1


@pytest.mark.unit
def test_w110_process_isolation(tmp_path: Path) -> None:
    root = tmp_path / "audit"
    store = ExecutionAuditStore(root)
    store.append(_event("process-1"))
    code = (
        "from trendx.forecasting.audit import ExecutionAuditStore, ExecutionAuditQuery;"
        "import sys;"
        "s=ExecutionAuditStore(sys.argv[1], create_if_missing=False);"
        "print(s.query(ExecutionAuditQuery(tenant_id='tenant-A')).total)"
    )
    result = subprocess.run(
        [sys.executable, "-c", code, str(root)],
        check=True,
        capture_output=True,
        text=True,
        env={"PYTHONPATH": str(Path(__file__).parents[2] / "src")},
    )
    assert result.stdout.strip().endswith("1")


@pytest.mark.unit
def test_w110_concurrent_append(tmp_path: Path) -> None:
    service = ExecutionAuditService(ExecutionAuditStore(tmp_path / "audit"))
    with ThreadPoolExecutor(max_workers=8) as executor:
        list(
            executor.map(
                lambda index: service.record(
                    operation=AuditOperation.EXECUTION,
                    outcome=AuditOutcome.SUCCESS,
                    tenant_id="tenant-A",
                    execution_id=f"exec-{index}",
                    request_id=f"request-{index}",
                    source="w110-test",
                    reason_code="concurrent",
                ),
                range(32),
            )
        )
    assert service.query(ExecutionAuditQuery(limit=1000)).total == 32


@pytest.mark.unit
def test_w110_audit_failure_policy(tmp_path: Path) -> None:
    class FailingStore:
        def append(self, event: ExecutionAuditEvent) -> ExecutionAuditEvent:
            raise RuntimeError("private backend detail")

    best_effort = ExecutionAuditService(FailingStore())
    assert (
        best_effort.record(
            operation=AuditOperation.RESTORE,
            outcome=AuditOutcome.SUCCESS,
            tenant_id="tenant-A",
            request_id="failure-request",
        )
        is None
    )
    mandatory = ExecutionAuditService(FailingStore(), failure_policy=AuditFailurePolicy.MANDATORY)
    with pytest.raises(RuntimeError):
        mandatory.record(
            operation=AuditOperation.RESTORE,
            outcome=AuditOutcome.SUCCESS,
            tenant_id="tenant-A",
            request_id="failure-request-2",
        )


@pytest.mark.unit
def test_w110_restore_api_audit_and_read_only_endpoint(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(settings, "trendx_api_token", SecretStr("w110-unit-token"))
    audit_store = MemoryExecutionAuditStore()
    audit = ExecutionAuditService(audit_store)
    audit.record(
        operation=AuditOperation.EXECUTION,
        outcome=AuditOutcome.SUCCESS,
        tenant_id="tenant-A",
        execution_id="exec-A",
        request_id="existing-request",
        source="w110-test",
        reason_code="existing",
    )
    audit.record(
        operation=AuditOperation.EXECUTION,
        outcome=AuditOutcome.FAILED,
        tenant_id="tenant-A",
        execution_id="exec-B",
        request_id="existing-request-2",
        source="w110-test",
        reason_code="existing-failure",
    )
    app.dependency_overrides[get_execution_audit_service] = lambda: audit
    app.dependency_overrides[get_tenant_context] = lambda: TenantContext("tenant-A")
    try:
        with TestClient(app) as client:
            response = client.get(
                "/api/v1/forecast/executions/audit",
                params={"limit": 1, "offset": 0},
                headers=_auth_headers(),
            )
            assert response.status_code == 200
            assert response.json()["total"] == 2
            assert len(response.json()["items"]) == 1
            assert response.json()["has_more"] is True
            assert response.headers["X-Request-ID"]
            assert client.get("/api/v1/forecast/executions/audit").status_code == 401
            assert (
                client.get(
                    "/api/v1/forecast/executions/audit",
                    params={"tenant_id": "tenant-B"},
                    headers=_auth_headers(),
                ).status_code
                == 403
            )
            assert (
                client.get(
                    "/api/v1/forecast/executions/audit",
                    params={"limit": 1001},
                    headers=_auth_headers(),
                ).status_code
                == 422
            )
    finally:
        app.dependency_overrides.clear()


@pytest.mark.unit
def test_w110_audit_placeholder_auth_fails_closed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(settings, "trendx_api_token", SecretStr("CHANGE_ME"))
    audit = ExecutionAuditService(ExecutionAuditStore(tmp_path / "audit"))
    previous_factory = app.state.execution_audit_service_factory
    app.state.execution_audit_service_factory = lambda: audit
    app.dependency_overrides[get_tenant_context] = lambda: TenantContext("tenant-A")
    try:
        with TestClient(app) as client:
            response = client.get(
                "/api/v1/forecast/executions/audit",
                headers={"Authorization": "Bearer CHANGE_ME"},
            )
            assert response.status_code == 503
            assert response.json()["detail"] == "audit_authentication_not_configured"
    finally:
        app.state.execution_audit_service_factory = previous_factory
        app.dependency_overrides.clear()


@pytest.mark.unit
def test_w110_openapi_audit_contract() -> None:
    with TestClient(app) as client:
        document = client.get("/openapi.json").json()
    path = document["paths"]["/api/v1/forecast/executions/audit"]
    assert set(path) == {"get"}
    assert {"401", "403", "422", "503"} <= set(path["get"]["responses"])
    assert path["get"]["security"] == [{"BearerAuth": []}, {"ApiKeyAuth": []}]
    assert "/api/v1/forecast/executions/audit/statistics" in document["paths"]
    assert document["components"]["securitySchemes"] == {
        "BearerAuth": {"type": "http", "scheme": "bearer"},
        "ApiKeyAuth": {"type": "apiKey", "in": "header", "name": "X-API-Key"},
    }
