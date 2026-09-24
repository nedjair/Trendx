"""W109 — authenticated HTTP recovery API contract and security tests."""

from __future__ import annotations

import hashlib
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient
from loguru import logger
from pydantic import SecretStr
from trendx.config import settings
from trendx.forecasting.api import (
    TenantContext,
    get_execution_history_service,
    get_execution_recovery_service,
    get_tenant_context,
)
from trendx.forecasting.execution import (
    DurableExecutionStore,
    ExecutionRecord,
    ExecutionStatus,
    ExecutionStoreError,
    RestoreStoreStatus,
)
from trendx.forecasting.history import ExecutionHistoryService
from trendx.forecasting.lifecycle import ExecutionRetentionPolicy, FileSystemArchiveStore
from trendx.forecasting.recovery import (
    ExecutionRestoreOutcome,
    ExecutionRestoreResult,
    ExecutionRestoreStatus,
    RecoveryService,
)
from trendx.main import _execution_recovery_service_factory, app

NOW = datetime(2026, 3, 2, tzinfo=UTC)
POLICY = ExecutionRetentionPolicy(retention_days=30, dry_run=False)


def _record(
    execution_id: str = "http-restore",
    tenant_id: str = "tenant-A",
) -> ExecutionRecord:
    return ExecutionRecord(
        execution_id=execution_id,
        reference_key=f"w109:{execution_id}",
        tenant_id=tenant_id,
        entity_type="DEVICE",
        entity_id=f"device-{execution_id}",
        target_metric="temperature",
        frequency="1h",
        horizon=24,
        model_id="model-w109",
        model_version="1",
        algorithm="Fourier",
        feature_schema_version="schema-w109",
        feature_schema_fingerprint="fingerprint-w109",
        artifact_uri="file:///w109/model.bin",
        status=ExecutionStatus.SUCCESS,
        created_at="2025-01-01T00:00:00Z",
        started_at="2025-01-01T00:00:01Z",
        completed_at="2025-01-01T00:00:02Z",
        prediction_count=3,
        metadata={"source": "w109", "sequence": 7},
        result={"values": [1.0, 2.0, 3.0]},
    )


def _archive(
    root: Path,
    record: ExecutionRecord,
) -> tuple[FileSystemArchiveStore, str]:
    store = FileSystemArchiveStore(root)
    store.archive((record,), policy=POLICY, archived_at=NOW)
    checksum = store.verify(record.execution_id).checksum
    assert checksum is not None
    return store, checksum


def _body(
    record: ExecutionRecord,
    checksum: str,
    *,
    tenant_id: str | None = None,
) -> dict[str, str]:
    return {
        "execution_id": record.execution_id,
        "tenant_id": tenant_id or record.tenant_id,
        "expected_archive_checksum": checksum,
        "conflict_policy": "FAIL_IF_EXISTS",
    }


def _auth_headers() -> dict[str, str]:
    return {"Authorization": f"Bearer {settings.trendx_api_token.get_secret_value()}"}


@pytest.fixture
def recovery_client(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> Iterator[
    tuple[TestClient, DurableExecutionStore, ExecutionRecord, FileSystemArchiveStore, str]
]:
    monkeypatch.setattr(settings, "trendx_api_token", SecretStr("w109-unit-valid-token"))
    record = _record()
    archive, checksum = _archive(tmp_path / "archive", record)
    store = DurableExecutionStore(tmp_path / "history.json")
    service = RecoveryService(store, archive, lambda: NOW)
    history_service = ExecutionHistoryService(store)
    app.dependency_overrides[get_execution_recovery_service] = lambda: service
    app.dependency_overrides[get_execution_history_service] = lambda: history_service
    app.dependency_overrides[get_tenant_context] = lambda: TenantContext("tenant-A")
    try:
        with TestClient(app) as client:
            yield client, store, record, archive, checksum
    finally:
        app.dependency_overrides.clear()
        store.close()


def _post(client: TestClient, body: dict[str, Any]) -> Any:
    return client.post(
        "/api/v1/forecast/executions/restore",
        json=body,
        headers=_auth_headers(),
    )


@pytest.mark.unit
def test_w109_restore_endpoint_exists(
    recovery_client: tuple[
        TestClient, DurableExecutionStore, ExecutionRecord, FileSystemArchiveStore, str
    ],
) -> None:
    client, _, record, _, checksum = recovery_client
    response = _post(client, _body(record, checksum))
    assert response.status_code == 200
    assert response.json()["status"] == "RESTORED"


@pytest.mark.unit
def test_w109_auth_required(
    recovery_client: tuple[
        TestClient, DurableExecutionStore, ExecutionRecord, FileSystemArchiveStore, str
    ],
) -> None:
    client, _, record, _, checksum = recovery_client
    response = client.post(
        "/api/v1/forecast/executions/restore",
        json=_body(record, checksum),
    )
    assert response.status_code == 401
    invalid = client.post(
        "/api/v1/forecast/executions/restore",
        json=_body(record, checksum),
        headers={"Authorization": "Bearer invalid-w109-token"},
    )
    assert invalid.status_code == 401
    assert "invalid-w109-token" not in invalid.text
    api_key = client.post(
        "/api/v1/forecast/executions/restore",
        json=_body(record, checksum),
        headers={"X-API-Key": settings.trendx_api_token.get_secret_value()},
    )
    assert api_key.status_code == 200


@pytest.mark.unit
def test_w109_placeholder_auth_fails_closed(
    recovery_client: tuple[
        TestClient, DurableExecutionStore, ExecutionRecord, FileSystemArchiveStore, str
    ],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, store, record, _, checksum = recovery_client
    monkeypatch.setattr(settings, "trendx_api_token", SecretStr("CHANGE_ME"))
    response = _post(client, _body(record, checksum))
    assert response.status_code == 503
    assert response.json()["detail"] == "recovery_authentication_not_configured"
    assert store.list() == ()


@pytest.mark.unit
def test_w109_tenant_context(
    recovery_client: tuple[
        TestClient, DurableExecutionStore, ExecutionRecord, FileSystemArchiveStore, str
    ],
) -> None:
    client, store, record, _, checksum = recovery_client
    response = _post(client, _body(record, checksum, tenant_id="tenant-B"))
    assert response.status_code == 403
    assert response.json()["detail"] == "tenant_forbidden"
    assert store.get(record.execution_id) is None


@pytest.mark.unit
def test_w109_tenant_mismatch_precedes_backend_factory(
    recovery_client: tuple[
        TestClient, DurableExecutionStore, ExecutionRecord, FileSystemArchiveStore, str
    ],
) -> None:
    client, store, record, _, checksum = recovery_client
    app.dependency_overrides.pop(get_execution_recovery_service, None)
    original_factory = app.state.execution_recovery_service_factory
    called = False

    def unexpected_factory() -> Any:
        nonlocal called
        called = True
        raise AssertionError("recovery backend must not be constructed")

    app.state.execution_recovery_service_factory = unexpected_factory
    try:
        response = _post(client, _body(record, checksum, tenant_id="tenant-B"))
    finally:
        app.state.execution_recovery_service_factory = original_factory
    assert response.status_code == 403
    assert response.json()["detail"] == "tenant_forbidden"
    assert called is False
    assert store.list() == ()


@pytest.mark.unit
def test_w109_restore_success(
    recovery_client: tuple[
        TestClient, DurableExecutionStore, ExecutionRecord, FileSystemArchiveStore, str
    ],
) -> None:
    client, store, record, _, checksum = recovery_client
    response = _post(client, _body(record, checksum))
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "RESTORED"
    assert body["outcome"] == "RESTORED"
    assert body["archive_verified"] is True
    assert body["record_restored"] is True
    assert body["already_present"] is False
    assert body["conflict"] is False
    assert body["reason"] == "record_restored"
    assert store.get(record.execution_id) == record


@pytest.mark.unit
def test_w109_restore_already_present(
    recovery_client: tuple[
        TestClient, DurableExecutionStore, ExecutionRecord, FileSystemArchiveStore, str
    ],
) -> None:
    client, _, record, _, checksum = recovery_client
    assert _post(client, _body(record, checksum)).status_code == 200
    response = _post(client, _body(record, checksum))
    assert response.status_code == 200
    assert response.json()["status"] == "ALREADY_PRESENT"
    assert response.json()["record_restored"] is False


@pytest.mark.unit
def test_w109_restore_conflict(
    recovery_client: tuple[
        TestClient, DurableExecutionStore, ExecutionRecord, FileSystemArchiveStore, str
    ],
) -> None:
    client, store, record, _, checksum = recovery_client
    conflicting = replace(record, model_id="different-model")
    assert store.restore_if_absent(conflicting).status is RestoreStoreStatus.INSERTED
    response = _post(client, _body(record, checksum))
    assert response.status_code == 409
    assert response.json()["status"] == "CONFLICT"
    assert store.get(record.execution_id) == conflicting


@pytest.mark.unit
def test_w109_archive_not_found(
    recovery_client: tuple[
        TestClient, DurableExecutionStore, ExecutionRecord, FileSystemArchiveStore, str
    ],
) -> None:
    client, store, record, _, _ = recovery_client
    response = _post(
        client,
        {
            "execution_id": "missing-http-archive",
            "tenant_id": record.tenant_id,
            "expected_archive_checksum": "0" * 64,
            "conflict_policy": "FAIL_IF_EXISTS",
        },
    )
    assert response.status_code == 404
    assert response.json()["status"] == "ARCHIVE_NOT_FOUND"
    assert store.list() == ()


@pytest.mark.unit
def test_w109_checksum_failure(
    recovery_client: tuple[
        TestClient, DurableExecutionStore, ExecutionRecord, FileSystemArchiveStore, str
    ],
) -> None:
    client, store, record, _, _ = recovery_client
    response = _post(client, _body(record, "0" * 64))
    assert response.status_code == 422
    assert response.json()["status"] == "INTEGRITY_FAILURE"
    assert store.list() == ()


@pytest.mark.unit
def test_w109_invalid_archive(
    recovery_client: tuple[
        TestClient, DurableExecutionStore, ExecutionRecord, FileSystemArchiveStore, str
    ],
) -> None:
    client, store, record, archive, checksum = recovery_client
    path = archive._path_for(record.execution_id)  # type: ignore[attr-defined]
    path.write_text("{not-json", encoding="utf-8")
    response = _post(client, _body(record, checksum))
    assert response.status_code == 422
    assert response.json()["status"] in {"INVALID_ARCHIVE", "INTEGRITY_FAILURE"}
    assert store.list() == ()


@pytest.mark.unit
def test_w109_insecure_archive_permissions_fail_closed(
    recovery_client: tuple[
        TestClient, DurableExecutionStore, ExecutionRecord, FileSystemArchiveStore, str
    ],
) -> None:
    client, store, record, archive, checksum = recovery_client
    archive._path_for(record.execution_id).chmod(0o644)  # type: ignore[attr-defined]
    response = _post(client, _body(record, checksum))
    assert response.status_code == 422
    assert response.json()["status"] == "INTEGRITY_FAILURE"
    assert store.list() == ()


@pytest.mark.unit
def test_w109_cross_tenant_rejected(
    tmp_path: Path,
    recovery_client: tuple[
        TestClient, DurableExecutionStore, ExecutionRecord, FileSystemArchiveStore, str
    ],
) -> None:
    client, store, _, _, _ = recovery_client
    record_b = _record("tenant-b-archive", tenant_id="tenant-B")
    archive_b, checksum_b = _archive(tmp_path / "archive-b", record_b)
    service_b = RecoveryService(store, archive_b, lambda: NOW)
    app.dependency_overrides[get_execution_recovery_service] = lambda: service_b
    try:
        response = _post(
            client,
            {
                "execution_id": record_b.execution_id,
                "tenant_id": "tenant-A",
                "expected_archive_checksum": checksum_b,
                "conflict_policy": "FAIL_IF_EXISTS",
            },
        )
    finally:
        app.dependency_overrides[get_execution_recovery_service] = lambda: RecoveryService(
            store,
            recovery_client[3],
            lambda: NOW,
        )
    assert response.status_code == 403
    assert response.json()["status"] == "TENANT_FORBIDDEN"
    assert store.get(record_b.execution_id) is None


@pytest.mark.unit
def test_w109_active_cross_tenant_collision_is_not_exposed(
    recovery_client: tuple[
        TestClient, DurableExecutionStore, ExecutionRecord, FileSystemArchiveStore, str
    ],
) -> None:
    client, store, record, _, checksum = recovery_client
    tenant_b_record = replace(record, tenant_id="tenant-B")
    assert store.restore_if_absent(tenant_b_record).status is RestoreStoreStatus.INSERTED
    response = _post(client, _body(record, checksum))
    assert response.status_code == 409
    assert response.json()["tenant_id"] == "tenant-A"
    assert "tenant-B" not in response.text
    assert store.get(record.execution_id) == tenant_b_record


@pytest.mark.unit
def test_w109_error_sanitization(
    recovery_client: tuple[
        TestClient, DurableExecutionStore, ExecutionRecord, FileSystemArchiveStore, str
    ],
) -> None:
    client, store, record, _, checksum = recovery_client
    unknown = _post(client, {**_body(record, checksum), "archive_path": "/tmp/secret-path"})
    assert unknown.status_code == 422
    assert unknown.json() == {"detail": "invalid_request"}
    assert "/tmp/secret-path" not in unknown.text
    malformed = client.post(
        "/api/v1/forecast/executions/restore",
        content=b"{not-json",
        headers={**_auth_headers(), "Content-Type": "application/json"},
    )
    assert malformed.status_code == 422
    assert "traceback" not in malformed.text.lower()
    assert store.list() == ()


@pytest.mark.unit
def test_w109_openapi(
    recovery_client: tuple[
        TestClient, DurableExecutionStore, ExecutionRecord, FileSystemArchiveStore, str
    ],
) -> None:
    client, _, _, _, _ = recovery_client
    response = client.get("/openapi.json")
    assert response.status_code == 200
    paths = response.json()["paths"]
    path = paths["/api/v1/forecast/executions/restore"]
    assert set(path) == {"post"}
    operation = path["post"]
    assert {"401", "403", "404", "409", "422", "503"} <= set(operation["responses"])
    assert "anyOf" in operation["responses"]["403"]["content"]["application/json"]["schema"]
    assert "anyOf" in operation["responses"]["422"]["content"]["application/json"]["schema"]
    assert "anyOf" in operation["responses"]["503"]["content"]["application/json"]["schema"]
    assert operation["security"] == [{"BearerAuth": []}, {"ApiKeyAuth": []}]
    document = response.json()
    assert document["components"]["securitySchemes"] == {
        "BearerAuth": {"type": "http", "scheme": "bearer"},
        "ApiKeyAuth": {"type": "apiKey", "in": "header", "name": "X-API-Key"},
    }
    schema = operation["requestBody"]["content"]["application/json"]["schema"]
    if "$ref" in schema:
        schema = document["components"]["schemas"][schema["$ref"].split("/")[-1]]
    assert {"execution_id", "tenant_id", "expected_archive_checksum", "conflict_policy"} <= set(
        schema["properties"]
    )
    assert "archive_path" not in schema["properties"]
    policy_schema = schema["properties"]["conflict_policy"]
    assert policy_schema.get("const") == "FAIL_IF_EXISTS" or policy_schema.get("enum") == [
        "FAIL_IF_EXISTS"
    ]
    assert (
        "ExecutionRestoreResultOut"
        in operation["responses"]["200"]["content"]["application/json"]["schema"]["$ref"]
    )


@pytest.mark.unit
def test_w109_no_get_restore(
    recovery_client: tuple[
        TestClient, DurableExecutionStore, ExecutionRecord, FileSystemArchiveStore, str
    ],
) -> None:
    client, _, _, _, _ = recovery_client
    response = client.get(
        "/api/v1/forecast/executions/restore",
        headers=_auth_headers(),
    )
    assert response.status_code == 405


@pytest.mark.unit
def test_w109_no_delete_restore(
    recovery_client: tuple[
        TestClient, DurableExecutionStore, ExecutionRecord, FileSystemArchiveStore, str
    ],
) -> None:
    client, _, _, _, _ = recovery_client
    response = client.delete(
        "/api/v1/forecast/executions/restore",
        headers=_auth_headers(),
    )
    assert response.status_code == 405


@pytest.mark.unit
def test_w109_no_other_restore_methods(
    recovery_client: tuple[
        TestClient, DurableExecutionStore, ExecutionRecord, FileSystemArchiveStore, str
    ],
) -> None:
    client, _, _, _, _ = recovery_client
    for method in ("put", "patch", "head"):
        response = getattr(client, method)(
            "/api/v1/forecast/executions/restore",
            headers=_auth_headers(),
        )
        assert response.status_code in {404, 405}


@pytest.mark.unit
def test_w109_w99_after_restore(
    recovery_client: tuple[
        TestClient, DurableExecutionStore, ExecutionRecord, FileSystemArchiveStore, str
    ],
) -> None:
    client, _, record, _, checksum = recovery_client
    assert _post(client, _body(record, checksum)).status_code == 200
    response = client.get(
        f"/api/v1/forecast/executions/{record.execution_id}",
        headers=_auth_headers(),
    )
    assert response.status_code == 200
    body = response.json()
    assert body["execution_id"] == record.execution_id
    assert body["tenant_id"] == record.tenant_id
    assert body["entity_id"] == record.entity_id
    assert body["target_metric"] == record.target_metric
    assert body["status"] == "SUCCESS"
    assert body["created_at"] == record.created_at
    assert body["completed_at"] == record.completed_at
    assert body["model_id"] == record.model_id
    assert body["model_version"] == record.model_version
    assert body["metadata"] == record.metadata
    assert body["result"] == record.result


@pytest.mark.unit
def test_w109_w104_after_restore(
    recovery_client: tuple[
        TestClient, DurableExecutionStore, ExecutionRecord, FileSystemArchiveStore, str
    ],
) -> None:
    client, _, record, _, checksum = recovery_client
    before = client.get("/api/v1/forecast/executions/analytics", headers=_auth_headers())
    assert before.status_code == 200
    assert before.json()["summary"]["total"] == 0
    assert _post(client, _body(record, checksum)).status_code == 200
    after = client.get("/api/v1/forecast/executions/analytics", headers=_auth_headers())
    assert after.status_code == 200
    body = after.json()
    assert body["summary"]["total"] == 1
    assert body["summary"]["total_prediction_count"] == 3
    assert body["by_metric"][0]["key"] == record.target_metric
    assert body["by_entity"][0]["entity_id"] == record.entity_id
    assert body["by_model"][0]["model_id"] == record.model_id
    assert body["by_status"][0]["key"] == "SUCCESS"


@pytest.mark.unit
def test_w109_w106_after_restore(
    recovery_client: tuple[
        TestClient, DurableExecutionStore, ExecutionRecord, FileSystemArchiveStore, str
    ],
) -> None:
    client, _, record, _, checksum = recovery_client
    before = client.get("/api/v1/forecast/executions/report", headers=_auth_headers())
    assert before.status_code == 200
    assert before.json()["summary"]["total"] == 0
    assert _post(client, _body(record, checksum)).status_code == 200
    after = client.get("/api/v1/forecast/executions/report", headers=_auth_headers())
    assert after.status_code == 200
    body = after.json()
    assert body["tenant_id"] == record.tenant_id
    assert body["summary"]["total"] == 1
    assert body["summary"]["total_prediction_count"] == 3
    assert body["dimensions"]["by_metric"][0]["key"] == record.target_metric
    assert body["dimensions"]["by_status"][0]["key"] == "SUCCESS"
    assert body["failures"] == []


@pytest.mark.unit
def test_w109_failed_record_reaches_analytics_and_reporting(
    tmp_path: Path,
    recovery_client: tuple[
        TestClient, DurableExecutionStore, ExecutionRecord, FileSystemArchiveStore, str
    ],
) -> None:
    client, _, record, _, _ = recovery_client
    failed = replace(
        record,
        execution_id="failed-http",
        status=ExecutionStatus.FAILED,
        error_code="MODEL_ERROR",
        error_reason="model failed",
        prediction_count=0,
    )
    archive, checksum = _archive(tmp_path / "failed-archive", failed)
    store = DurableExecutionStore(tmp_path / "failed-history.json")
    service = RecoveryService(store, archive, lambda: NOW)
    app.dependency_overrides[get_execution_recovery_service] = lambda: service
    app.dependency_overrides[get_execution_history_service] = lambda: ExecutionHistoryService(store)
    try:
        response = _post(client, _body(failed, checksum))
        assert response.status_code == 200
        assert response.json()["status"] == "RESTORED"
        analytics = client.get("/api/v1/forecast/executions/analytics", headers=_auth_headers())
        report = client.get("/api/v1/forecast/executions/report", headers=_auth_headers())
        assert analytics.status_code == 200
        assert report.status_code == 200
        assert analytics.json()["failures"][0]["error_code"] == "MODEL_ERROR"
        assert report.json()["failures"][0]["error_code"] == "MODEL_ERROR"
    finally:
        store.close()


@pytest.mark.unit
def test_w109_idempotence(
    recovery_client: tuple[
        TestClient, DurableExecutionStore, ExecutionRecord, FileSystemArchiveStore, str
    ],
) -> None:
    client, store, record, archive, checksum = recovery_client
    archive_path = archive._path_for(record.execution_id)  # type: ignore[attr-defined]
    before_hash = hashlib.sha256(archive_path.read_bytes()).hexdigest()
    first = _post(client, _body(record, checksum))
    active_after_first = store.get(record.execution_id)
    second = _post(client, _body(record, checksum))
    assert first.status_code == 200
    assert second.status_code == 200
    assert first.json()["status"] == "RESTORED"
    assert second.json()["status"] == "ALREADY_PRESENT"
    assert store.get(record.execution_id) == active_after_first == record
    assert hashlib.sha256(archive_path.read_bytes()).hexdigest() == before_hash


@pytest.mark.unit
def test_w109_concurrent_restore(
    recovery_client: tuple[
        TestClient, DurableExecutionStore, ExecutionRecord, FileSystemArchiveStore, str
    ],
) -> None:
    client, store, record, _, checksum = recovery_client
    with TestClient(app) as second_client:

        def send(test_client: TestClient) -> Any:
            return _post(test_client, _body(record, checksum))

        with ThreadPoolExecutor(max_workers=2) as executor:
            responses = list(executor.map(send, (client, second_client)))
    assert sorted(response.status_code for response in responses) == [200, 200]
    assert sorted(response.json()["status"] for response in responses) == [
        "ALREADY_PRESENT",
        "RESTORED",
    ]
    assert store.get(record.execution_id) == record


@pytest.mark.unit
def test_w109_no_arbitrary_archive_path(
    recovery_client: tuple[
        TestClient, DurableExecutionStore, ExecutionRecord, FileSystemArchiveStore, str
    ],
    tmp_path: Path,
) -> None:
    client, store, record, _, checksum = recovery_client
    outside = tmp_path / "outside.txt"
    outside.write_text("do-not-read", encoding="utf-8")
    before = outside.read_bytes()
    path_attempt = _post(
        client,
        {
            **_body(record, checksum),
            "archive_path": "/etc/passwd",
        },
    )
    assert path_attempt.status_code == 422
    assert "/etc/passwd" not in path_attempt.text
    traversal = _post(
        client,
        {
            "execution_id": "../../outside",
            "tenant_id": record.tenant_id,
            "expected_archive_checksum": checksum,
            "conflict_policy": "FAIL_IF_EXISTS",
        },
    )
    assert traversal.status_code == 404
    assert "../../outside" not in traversal.text
    assert outside.read_bytes() == before
    assert store.list() == ()


@pytest.mark.unit
def test_w109_observability_sanitized(
    recovery_client: tuple[
        TestClient, DurableExecutionStore, ExecutionRecord, FileSystemArchiveStore, str
    ],
) -> None:
    client, _, record, _, checksum = recovery_client
    events: list[str] = []
    sink_id = logger.add(events.append, format="{message}")
    try:
        response = _post(client, _body(record, checksum))
    finally:
        logger.remove(sink_id)
    output = "\n".join(events)
    assert response.status_code == 200
    assert "operation=restore" in output
    assert "outcome=success" in output
    assert "status_code=200" in output
    assert checksum not in output
    assert "Authorization" not in output
    assert "traceback" not in output.lower()


@pytest.mark.unit
def test_w109_invalid_request_fields(
    recovery_client: tuple[
        TestClient, DurableExecutionStore, ExecutionRecord, FileSystemArchiveStore, str
    ],
) -> None:
    client, store, record, _, checksum = recovery_client
    invalid_policy = _post(client, {**_body(record, checksum), "conflict_policy": "FORCE"})
    assert invalid_policy.status_code == 422
    missing = client.post(
        "/api/v1/forecast/executions/restore",
        json={"execution_id": record.execution_id},
        headers=_auth_headers(),
    )
    assert missing.status_code == 422
    oversized = _post(client, _body(record, checksum) | {"execution_id": "x" * 513})
    assert oversized.status_code == 422
    blank_execution = _post(client, _body(record, checksum) | {"execution_id": " "})
    assert blank_execution.status_code == 422
    blank_tenant = _post(client, _body(record, checksum) | {"tenant_id": " "})
    assert blank_tenant.status_code == 422
    null_byte = _post(client, _body(record, checksum) | {"execution_id": "bad\x00id"})
    assert null_byte.status_code == 422
    assert "\x00" not in null_byte.text
    without_policy = {
        key: value for key, value in _body(record, checksum).items() if key != "conflict_policy"
    }
    missing_policy = _post(client, without_policy)
    assert missing_policy.status_code == 422
    unknown_query = client.post(
        "/api/v1/forecast/executions/restore",
        params={"archive_path": "/etc/passwd"},
        json=_body(record, checksum),
        headers=_auth_headers(),
    )
    assert unknown_query.status_code == 422
    assert "/etc/passwd" not in unknown_query.text
    assert store.list() == ()


@pytest.mark.unit
def test_w109_reference_key_conflict(
    recovery_client: tuple[
        TestClient, DurableExecutionStore, ExecutionRecord, FileSystemArchiveStore, str
    ],
) -> None:
    client, store, record, _, checksum = recovery_client
    other = replace(record, execution_id="other-reference")
    assert store.restore_if_absent(other).status is RestoreStoreStatus.INSERTED
    response = _post(client, _body(record, checksum))
    assert response.status_code == 409
    assert response.json()["status"] == "CONFLICT"
    assert store.get(record.execution_id) is None
    assert store.get(other.execution_id) == other


@pytest.mark.unit
def test_w109_verify_only_backend_is_not_success(
    recovery_client: tuple[
        TestClient, DurableExecutionStore, ExecutionRecord, FileSystemArchiveStore, str
    ],
) -> None:
    client, store, record, archive, checksum = recovery_client
    service = RecoveryService(store, archive, lambda: NOW)

    class VerifyOnlyService:
        def restore(self, request: Any) -> Any:
            return service.verify_archive(request)

    app.dependency_overrides[get_execution_recovery_service] = lambda: VerifyOnlyService()
    response = _post(client, _body(record, checksum))
    assert response.status_code == 503
    assert response.json()["status"] == "VERIFIED"
    assert response.json()["outcome"] == "NO_CHANGE"
    assert store.list() == ()


@pytest.mark.unit
def test_w109_restore_failed_status_maps_to_503(
    recovery_client: tuple[
        TestClient, DurableExecutionStore, ExecutionRecord, FileSystemArchiveStore, str
    ],
) -> None:
    client, store, record, _, checksum = recovery_client

    class FailedService:
        def restore(self, request: Any) -> ExecutionRestoreResult:
            return ExecutionRestoreResult(
                execution_id=record.execution_id,
                tenant_id=record.tenant_id,
                status=ExecutionRestoreStatus.RESTORE_FAILED,
                outcome=ExecutionRestoreOutcome.REJECTED,
                archive_verified=True,
                record_restored=False,
                already_present=False,
                conflict=False,
                reason="active_store_write_failed",
            )

    app.dependency_overrides[get_execution_recovery_service] = lambda: FailedService()
    response = _post(client, _body(record, checksum))
    assert response.status_code == 503
    assert response.json()["status"] == "RESTORE_FAILED"
    assert store.list() == ()


@pytest.mark.unit
def test_w109_missing_history_is_not_created(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    history_path = tmp_path / "missing-history.json"
    archive_path = tmp_path / "archive"
    archive_path.mkdir()
    monkeypatch.setattr(settings, "trendx_execution_history_path", str(history_path))
    monkeypatch.setattr(settings, "trendx_execution_archive_path", str(archive_path))
    with pytest.raises(ExecutionStoreError):
        _execution_recovery_service_factory()
    assert not history_path.exists()
    direct_path = tmp_path / "direct-missing.json"
    with pytest.raises(ExecutionStoreError):
        DurableExecutionStore(direct_path, create_if_missing=False)
    assert not direct_path.exists()


@pytest.mark.unit
def test_w109_symlink_history_is_rejected(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    real_history = tmp_path / "real-history.json"
    store = DurableExecutionStore(real_history)
    store.close()
    linked_history = tmp_path / "linked-history.json"
    linked_history.symlink_to(real_history)
    archive_path = tmp_path / "archive"
    archive_path.mkdir()
    monkeypatch.setattr(settings, "trendx_execution_history_path", str(linked_history))
    monkeypatch.setattr(settings, "trendx_execution_archive_path", str(archive_path))
    with pytest.raises(ExecutionStoreError):
        _execution_recovery_service_factory()
    assert linked_history.is_symlink()


@pytest.mark.unit
def test_w109_backend_unavailable(
    recovery_client: tuple[
        TestClient, DurableExecutionStore, ExecutionRecord, FileSystemArchiveStore, str
    ],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, _, _, _, _ = recovery_client
    app.dependency_overrides.pop(get_execution_recovery_service, None)
    original_factory = app.state.execution_recovery_service_factory

    def failing_factory() -> Any:
        raise ExecutionStoreError("backend unavailable")

    monkeypatch.setattr(app.state, "execution_recovery_service_factory", failing_factory)
    try:
        response = _post(client, _body(_record("backend-test"), "a" * 64))
    finally:
        app.state.execution_recovery_service_factory = original_factory
    assert response.status_code == 503
    assert response.json()["detail"] == "recovery_service_unavailable"
    assert "backend unavailable" not in response.text.lower()
