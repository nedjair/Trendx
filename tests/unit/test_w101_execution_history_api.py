"""W101 — operational read-only execution history API.

The tests use the existing FastAPI app, authentication middleware, dependency
overrides, and a temporary W100 DurableExecutionStore.  No production service,
scheduler, worker, ThingsBoard, MLflow, or database is started.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient
from trendx.config import settings
from trendx.forecasting.api import (
    TenantContext,
    get_execution_history_service,
    get_tenant_context,
)
from trendx.forecasting.execution import (
    DurableExecutionStore,
    ExecutionProvenance,
    ExecutionRecord,
)
from trendx.forecasting.history import ExecutionHistoryService
from trendx.main import app

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src"
BASE_TIME = datetime(2026, 1, 1, tzinfo=UTC)


def _timestamp(minutes: int) -> str:
    return (BASE_TIME + timedelta(minutes=minutes)).isoformat()


def _record(
    execution_id: str,
    *,
    tenant_id: str = "tenant-A",
    entity_id: str = "entity-A",
    target_metric: str = "temperature",
    model_id: str = "model-A",
    model_version: str = "1",
    created_minutes: int = 0,
    metadata: dict[str, Any] | None = None,
) -> ExecutionRecord:
    return ExecutionRecord(
        execution_id=execution_id,
        reference_key=f"ref-{execution_id}",
        tenant_id=tenant_id,
        entity_type="DEVICE",
        entity_id=entity_id,
        target_metric=target_metric,
        frequency="1h",
        horizon=24,
        model_id=model_id,
        model_version=model_version,
        algorithm="Fourier",
        feature_schema_version="S1",
        feature_schema_fingerprint="fingerprint-S1",
        artifact_uri="file:///artifacts/model-A/artifact.bin",
        created_at=_timestamp(created_minutes),
        metadata=metadata if metadata is not None else {"source": "w101"},
    )


def _provenance(
    *,
    model_id: str = "model-A",
    model_version: str = "1",
    fingerprint: str = "fingerprint-S1",
    metadata: dict[str, Any] | None = None,
    result: dict[str, Any] | None = None,
) -> ExecutionProvenance:
    return ExecutionProvenance(
        model_id=model_id,
        model_version=model_version,
        algorithm="Fourier",
        feature_schema_version="S1",
        feature_schema_fingerprint=fingerprint,
        artifact_uri=f"file:///artifacts/{model_id}/artifact.bin",
        prediction_count=3,
        metadata=metadata if metadata is not None else {"source": "w101"},
        result=result if result is not None else {"values": [1.0, 2.0, 3.0]},
    )


def _success(
    store: DurableExecutionStore,
    record: ExecutionRecord,
    *,
    provenance: ExecutionProvenance | None = None,
) -> ExecutionRecord:
    store.create(record)
    store.mark_started(record.execution_id)
    return store.mark_success(
        record.execution_id,
        provenance or _provenance(),
        completed_at=None,
    )


def _failed(
    store: DurableExecutionStore,
    record: ExecutionRecord,
    *,
    error_code: str = "feature_schema_mismatch",
) -> ExecutionRecord:
    store.create(record)
    store.mark_started(record.execution_id)
    return store.mark_failed(
        record.execution_id,
        error_code=error_code,
        error_reason="controlled fixture failure",
        provenance=_provenance(fingerprint="fingerprint-S2"),
        completed_at=None,
    )


def _auth_headers() -> dict[str, str]:
    return {"Authorization": f"Bearer {settings.trendx_api_token.get_secret_value()}"}


def _seed_store(tmp_path: Path) -> DurableExecutionStore:
    path = tmp_path / "execution-history.json"
    store = DurableExecutionStore(path)
    _success(
        store,
        _record("E-api-success", created_minutes=30),
        provenance=_provenance(
            metadata={"safe": "visible", "api_key": "must-not-leak"},
            result={"values": [1, 2, 3], "token": "must-not-leak"},
        ),
    )
    _success(
        store,
        _record(
            "E-api-humidity",
            target_metric="humidity",
            model_version="2",
            created_minutes=20,
        ),
        provenance=_provenance(model_version="2"),
    )
    _failed(
        store,
        _record("E-api-failed", created_minutes=10),
    )
    _success(
        store,
        _record(
            "E-api-entity-b",
            entity_id="entity-B",
            target_metric="solar_power",
            model_id="model-C",
            created_minutes=5,
        ),
        provenance=_provenance(model_id="model-C"),
    )
    _success(
        store,
        _record(
            "E-api-tenant-b",
            tenant_id="tenant-B",
            created_minutes=1,
        ),
        provenance=_provenance(model_id="model-D"),
    )
    return store


@pytest.fixture
def api_client(tmp_path: Path) -> Iterator[tuple[TestClient, DurableExecutionStore]]:
    store = _seed_store(tmp_path)
    service = ExecutionHistoryService(store)
    app.dependency_overrides[get_execution_history_service] = lambda: service
    app.dependency_overrides[get_tenant_context] = lambda: TenantContext("tenant-A")
    with TestClient(app) as client:
        yield client, store
    app.dependency_overrides.clear()
    store.close()


def _get(client: TestClient, path: str, **params: Any) -> Any:
    return client.get(path, params=params or None, headers=_auth_headers())


@pytest.mark.unit
def test_a_get_execution_by_id(api_client: tuple[TestClient, DurableExecutionStore]) -> None:
    client, _ = api_client
    response = _get(client, "/api/v1/forecast/executions/E-api-success")
    assert response.status_code == 200
    body = response.json()
    assert body["execution_id"] == "E-api-success"
    assert body["status"] == "SUCCESS"
    assert body["model_id"] == "model-A"
    assert body["prediction_count"] == 3


@pytest.mark.unit
def test_b_get_execution_by_reference(api_client: tuple[TestClient, DurableExecutionStore]) -> None:
    client, _ = api_client
    response = _get(client, "/api/v1/forecast/executions/reference/ref-E-api-success")
    assert response.status_code == 200
    assert response.json()["execution_id"] == "E-api-success"


@pytest.mark.unit
def test_c_list_executions(api_client: tuple[TestClient, DurableExecutionStore]) -> None:
    client, _ = api_client
    response = _get(client, "/api/v1/forecast/executions")
    assert response.status_code == 200
    body = response.json()
    assert body["total"] == 4
    assert body["limit"] == 100
    assert body["offset"] == 0
    assert body["has_more"] is False
    assert [item["execution_id"] for item in body["items"]] == [
        "E-api-success",
        "E-api-humidity",
        "E-api-failed",
        "E-api-entity-b",
    ]


@pytest.mark.unit
def test_d_filter_tenant(api_client: tuple[TestClient, DurableExecutionStore]) -> None:
    client, _ = api_client
    response = _get(client, "/api/v1/forecast/executions", tenant_id="tenant-A")
    assert response.status_code == 200
    assert {item["tenant_id"] for item in response.json()["items"]} == {"tenant-A"}


@pytest.mark.unit
def test_e_filter_entity(api_client: tuple[TestClient, DurableExecutionStore]) -> None:
    client, _ = api_client
    response = _get(client, "/api/v1/forecast/executions", entity_id="entity-B")
    assert response.status_code == 200
    assert [item["execution_id"] for item in response.json()["items"]] == ["E-api-entity-b"]


@pytest.mark.unit
def test_f_filter_metric(api_client: tuple[TestClient, DurableExecutionStore]) -> None:
    client, _ = api_client
    response = _get(client, "/api/v1/forecast/executions", target_metric="humidity")
    assert response.status_code == 200
    assert response.json()["items"][0]["target_metric"] == "humidity"


@pytest.mark.unit
def test_g_filter_model_and_version(api_client: tuple[TestClient, DurableExecutionStore]) -> None:
    client, _ = api_client
    model = _get(client, "/api/v1/forecast/executions", model_id="model-A")
    version = _get(
        client,
        "/api/v1/forecast/executions",
        model_id="model-A",
        model_version="2",
    )
    assert model.status_code == version.status_code == 200
    assert len(model.json()["items"]) == 3
    assert version.json()["items"][0]["model_version"] == "2"


@pytest.mark.unit
def test_h_filter_algorithm(api_client: tuple[TestClient, DurableExecutionStore]) -> None:
    client, _ = api_client
    response = _get(client, "/api/v1/forecast/executions", algorithm="FOURIER")
    assert response.status_code == 200
    assert response.json()["total"] == 4


@pytest.mark.unit
def test_i_filter_schema_fingerprint(api_client: tuple[TestClient, DurableExecutionStore]) -> None:
    client, _ = api_client
    response = _get(
        client,
        "/api/v1/forecast/executions",
        feature_schema_fingerprint="fingerprint-S2",
    )
    assert response.status_code == 200
    assert [item["execution_id"] for item in response.json()["items"]] == ["E-api-failed"]


@pytest.mark.unit
def test_j_filter_status(api_client: tuple[TestClient, DurableExecutionStore]) -> None:
    client, _ = api_client
    success = _get(client, "/api/v1/forecast/executions", status="SUCCESS")
    failed = _get(client, "/api/v1/forecast/executions", status="FAILED")
    assert success.status_code == failed.status_code == 200
    assert success.json()["total"] == 3
    assert failed.json()["total"] == 1


@pytest.mark.unit
def test_k_created_at_range_filter(api_client: tuple[TestClient, DurableExecutionStore]) -> None:
    client, _ = api_client
    response = _get(
        client,
        "/api/v1/forecast/executions",
        created_at_from="2026-01-01T00:15:00Z",
        created_at_to="2026-01-01T00:35:00Z",
    )
    assert response.status_code == 200
    assert {item["execution_id"] for item in response.json()["items"]} == {
        "E-api-success",
        "E-api-humidity",
    }


@pytest.mark.unit
def test_l_completed_at_range_filter(api_client: tuple[TestClient, DurableExecutionStore]) -> None:
    client, _ = api_client
    now = datetime.now(UTC)
    response = _get(
        client,
        "/api/v1/forecast/executions",
        completed_at_from=(now - timedelta(minutes=1)).isoformat(),
        completed_at_to=(now + timedelta(minutes=1)).isoformat(),
        status="FAILED",
    )
    assert response.status_code == 200
    assert [item["execution_id"] for item in response.json()["items"]] == ["E-api-failed"]


@pytest.mark.unit
def test_m_pagination_and_stable_order(
    api_client: tuple[TestClient, DurableExecutionStore],
) -> None:
    client, _ = api_client
    first = _get(client, "/api/v1/forecast/executions", limit=2, offset=0)
    second = _get(client, "/api/v1/forecast/executions", limit=2, offset=2)
    assert first.status_code == second.status_code == 200
    assert first.json()["has_more"] is True
    assert second.json()["has_more"] is False
    assert [item["execution_id"] for item in first.json()["items"]] == [
        "E-api-success",
        "E-api-humidity",
    ]
    assert [item["execution_id"] for item in second.json()["items"]] == [
        "E-api-failed",
        "E-api-entity-b",
    ]


@pytest.mark.unit
def test_n_statistics_filters(api_client: tuple[TestClient, DurableExecutionStore]) -> None:
    client, _ = api_client
    response = _get(client, "/api/v1/forecast/executions/statistics", tenant_id="tenant-A")
    assert response.status_code == 200
    body = response.json()
    assert body["total"] == 4
    assert body["success_count"] == 3
    assert body["failed_count"] == 1
    assert body["success_rate"] == 0.75
    assert body["total_prediction_count"] == 12
    assert body["duration_sample_count"] == 4
    assert body["total_duration"] is not None
    assert body["average_duration"] is not None


@pytest.mark.unit
def test_o_diagnostic_success_has_no_failure(
    api_client: tuple[TestClient, DurableExecutionStore],
) -> None:
    client, _ = api_client
    response = _get(client, "/api/v1/forecast/executions/E-api-success/diagnostic")
    assert response.status_code == 200
    assert response.json() == {
        "execution_id": "E-api-success",
        "status": "SUCCESS",
        "failure": None,
    }


@pytest.mark.unit
def test_p_diagnostic_failed_preserves_provenance(
    api_client: tuple[TestClient, DurableExecutionStore],
) -> None:
    client, _ = api_client
    response = _get(client, "/api/v1/forecast/executions/E-api-failed/diagnostic")
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "FAILED"
    assert body["failure"]["error_code"] == "feature_schema_mismatch"
    assert body["failure"]["feature_schema_fingerprint"] == "fingerprint-S2"
    assert body["failure"]["model_id"] == "model-A"
    assert body["failure"]["completed_at"]


@pytest.mark.unit
def test_q_unknown_execution_is_not_found(
    api_client: tuple[TestClient, DurableExecutionStore],
) -> None:
    client, _ = api_client
    response = _get(client, "/api/v1/forecast/executions/E-does-not-exist")
    assert response.status_code == 404
    assert response.json()["detail"] == "execution_not_found"


@pytest.mark.unit
def test_r_invalid_query_and_pagination(
    api_client: tuple[TestClient, DurableExecutionStore],
) -> None:
    client, _ = api_client
    bad_date = _get(
        client,
        "/api/v1/forecast/executions",
        created_at_from="not-a-date",
    )
    bad_limit = _get(client, "/api/v1/forecast/executions", limit=0)
    bad_offset = _get(client, "/api/v1/forecast/executions", offset=-1)
    assert bad_date.status_code == 422
    assert bad_date.json()["detail"] == "invalid_query"
    assert bad_limit.status_code == 422
    assert bad_limit.json()["detail"] == "invalid_pagination"
    assert bad_offset.status_code == 422
    assert bad_offset.json()["detail"] == "invalid_pagination"


@pytest.mark.unit
def test_s_authentication_is_required(api_client: tuple[TestClient, DurableExecutionStore]) -> None:
    client, _ = api_client
    response = client.get("/api/v1/forecast/executions")
    assert response.status_code == 401


@pytest.mark.unit
def test_t_tenant_isolation_and_forged_tenant_filter(
    api_client: tuple[TestClient, DurableExecutionStore],
) -> None:
    client, _ = api_client
    forbidden = _get(client, "/api/v1/forecast/executions", tenant_id="tenant-B")
    assert forbidden.status_code == 403
    assert forbidden.json()["detail"] == "tenant_forbidden"

    hidden = _get(client, "/api/v1/forecast/executions/E-api-tenant-b")
    assert hidden.status_code == 404

    app.dependency_overrides[get_tenant_context] = lambda: TenantContext("tenant-B")
    try:
        visible = _get(client, "/api/v1/forecast/executions/E-api-tenant-b")
        assert visible.status_code == 200
        assert visible.json()["tenant_id"] == "tenant-B"
    finally:
        app.dependency_overrides[get_tenant_context] = lambda: TenantContext("tenant-A")


@pytest.mark.unit
def test_u_no_mutation_after_reads(api_client: tuple[TestClient, DurableExecutionStore]) -> None:
    client, store = api_client
    before = store.path.read_bytes()
    _get(client, "/api/v1/forecast/executions")
    _get(client, "/api/v1/forecast/executions/E-api-success")
    _get(client, "/api/v1/forecast/executions/statistics")
    _get(client, "/api/v1/forecast/executions/E-api-failed/diagnostic")
    assert store.path.read_bytes() == before


@pytest.mark.unit
def test_v_metadata_and_result_secret_keys_are_redacted(
    api_client: tuple[TestClient, DurableExecutionStore],
) -> None:
    client, _ = api_client
    response = _get(client, "/api/v1/forecast/executions/E-api-success")
    assert response.status_code == 200
    body = response.json()
    assert body["metadata"]["safe"] == "visible"
    assert body["metadata"]["api_key"] == "[REDACTED]"
    assert body["result"]["values"] == [1, 2, 3]
    assert body["result"]["token"] == "[REDACTED]"


@pytest.mark.unit
def test_w_openapi_documents_read_only_endpoints(
    api_client: tuple[TestClient, DurableExecutionStore],
) -> None:
    client, _ = api_client
    response = client.get("/openapi.json")
    assert response.status_code == 200
    paths = response.json()["paths"]
    expected = {
        "/api/v1/forecast/executions",
        "/api/v1/forecast/executions/statistics",
        "/api/v1/forecast/executions/reference/{reference_key}",
        "/api/v1/forecast/executions/{execution_id}",
        "/api/v1/forecast/executions/{execution_id}/diagnostic",
    }
    assert expected <= set(paths)
    for path in expected:
        assert set(paths[path]) == {"get"}
        operation = paths[path]["get"]
        assert {"401", "403", "404", "422", "503"} <= set(operation["responses"])


@pytest.mark.unit
def test_y_missing_datastore_is_unavailable_and_not_created(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "missing-history.json"
    monkeypatch.setattr(settings, "trendx_execution_history_path", str(path))
    app.dependency_overrides[get_tenant_context] = lambda: TenantContext("tenant-A")
    try:
        with TestClient(app) as client:
            response = _get(client, "/api/v1/forecast/executions")
    finally:
        app.dependency_overrides.clear()
    assert response.status_code == 503
    assert response.json()["detail"] == "execution_history_unavailable"
    assert not path.exists()


@pytest.mark.unit
def test_z_corrupted_datastore_is_sanitized(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "corrupted-history.json"
    path.write_text("{not-json", encoding="utf-8")
    monkeypatch.setattr(settings, "trendx_execution_history_path", str(path))
    app.dependency_overrides[get_tenant_context] = lambda: TenantContext("tenant-A")
    try:
        with TestClient(app) as client:
            response = _get(client, "/api/v1/forecast/executions")
    finally:
        app.dependency_overrides.clear()
    assert response.status_code == 503
    assert response.json()["detail"] == "execution_history_unavailable"
    assert "path" not in response.text.lower()
    assert "traceback" not in response.text.lower()


@pytest.mark.unit
def test_x_new_process_api_reads_durable_store(tmp_path: Path) -> None:
    path = tmp_path / "execution-history.json"
    store = DurableExecutionStore(path)
    _success(store, _record("E-process-api", created_minutes=1))
    _failed(store, _record("E-process-api-failed", created_minutes=2))
    store.close()

    child = r"""
import json
import sys
from fastapi.testclient import TestClient
from trendx.config import settings
from trendx.main import app

headers = {"Authorization": f"Bearer {settings.trendx_api_token.get_secret_value()}"}
with TestClient(app) as client:
    by_id = client.get("/api/v1/forecast/executions/E-process-api", headers=headers)
    listing = client.get("/api/v1/forecast/executions", headers=headers)
    stats = client.get("/api/v1/forecast/executions/statistics", headers=headers)
    diagnostic = client.get(
        "/api/v1/forecast/executions/E-process-api-failed/diagnostic",
        headers=headers,
    )
    print(json.dumps({
        "by_id": by_id.json(),
        "list": listing.json(),
        "stats": stats.json(),
        "diagnostic": diagnostic.json(),
    }))
"""
    env = os.environ.copy()
    env["PYTHONPATH"] = str(SRC) + os.pathsep + env.get("PYTHONPATH", "")
    env.update(
        {
            "PYTHONDONTWRITEBYTECODE": "1",
            "TRENDX_EXECUTION_HISTORY_PATH": str(path),
            "TRENDX_DEFAULT_TENANT_ID": "tenant-A",
            "TRENDX_SCHEDULER_FORECAST_ENABLED": "false",
            "TRENDX_INGEST_ENABLED": "false",
            "TRENDX_WORKER_INGESTION_ENABLED": "false",
            "TB_WRITEBACK_ENABLED": "false",
            "TB_ALARMS_ENABLED": "false",
        }
    )
    completed = subprocess.run(
        [sys.executable, "-c", child],
        cwd=ROOT,
        env=env,
        text=True,
        capture_output=True,
        check=True,
    )
    evidence = json.loads(completed.stdout.strip().splitlines()[-1])
    assert evidence["by_id"]["execution_id"] == "E-process-api"
    assert evidence["by_id"]["status"] == "SUCCESS"
    assert evidence["list"]["total"] == 2
    assert evidence["list"]["items"][0]["execution_id"] == "E-process-api-failed"
    assert evidence["stats"]["total"] == 2
    assert evidence["stats"]["failed_count"] == 1
    assert evidence["diagnostic"]["failure"]["error_code"] == "feature_schema_mismatch"
