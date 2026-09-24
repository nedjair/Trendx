"""W103 — stable execution-history contract and observability checks."""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient
from loguru import logger
from trendx.config import settings
from trendx.forecasting.api import (
    ExecutionDiagnosticOut,
    ExecutionErrorOut,
    ExecutionOut,
    FailureDiagnosticOut,
    TenantContext,
    get_execution_history_service,
    get_tenant_context,
)
from trendx.forecasting.execution import (
    DurableExecutionStore,
    ExecutionProvenance,
    ExecutionRecord,
    ExecutionStatus,
)
from trendx.forecasting.history import ExecutionHistoryService
from trendx.main import app

BASE_TIME = datetime(2026, 1, 1, tzinfo=UTC)


def _timestamp(minutes: int) -> str:
    return (BASE_TIME + timedelta(minutes=minutes)).isoformat()


def _record(
    execution_id: str,
    *,
    tenant_id: str = "tenant-A",
    entity_id: str = "entity-A",
    metric: str = "temperature",
) -> ExecutionRecord:
    return ExecutionRecord(
        execution_id=execution_id,
        reference_key=f"ref:{execution_id}",
        tenant_id=tenant_id,
        entity_type="DEVICE",
        entity_id=entity_id,
        target_metric=metric,
        frequency="1h",
        horizon=24,
        model_id="model-A",
        model_version="1",
        algorithm="Fourier",
        feature_schema_version="schema-A",
        feature_schema_fingerprint="fingerprint-A",
        artifact_uri="file:///w103/model-A.bin",
        created_at=_timestamp(10 if execution_id.endswith("success") else 5),
        metadata={"source": "w103"},
    )


def _provenance(*, predictions: int = 2) -> ExecutionProvenance:
    return ExecutionProvenance(
        model_id="model-A",
        model_version="1",
        algorithm="Fourier",
        feature_schema_version="schema-A",
        feature_schema_fingerprint="fingerprint-A",
        artifact_uri="file:///w103/model-A.bin",
        prediction_count=predictions,
        metadata={"source": "w103"},
        result={"values": [1.0] * predictions},
    )


def _write_fixture(path: Path) -> DurableExecutionStore:
    store = DurableExecutionStore(path)
    for record in (
        _record("w103-a-success"),
        _record("w103-a-failed", metric="humidity"),
        _record("w103-b-success", tenant_id="tenant-B", entity_id="entity-B", metric="energy"),
    ):
        store.create(record)
        store.mark_started(record.execution_id)
        if record.execution_id.endswith("success"):
            store.mark_success(record.execution_id, _provenance())
        else:
            store.mark_failed(
                record.execution_id,
                error_code="feature_schema_mismatch",
                error_reason="w103 fixture",
                provenance=_provenance(predictions=0),
            )
    store.close()
    return DurableExecutionStore(path)


def _auth_headers() -> dict[str, str]:
    return {"Authorization": f"Bearer {settings.trendx_api_token.get_secret_value()}"}


def _get(client: TestClient, path: str, **params: Any) -> Any:
    return client.get(path, params=params or None, headers=_auth_headers())


@pytest.fixture
def api_client(tmp_path: Path) -> Iterator[tuple[TestClient, DurableExecutionStore, list[str]]]:
    store = _write_fixture(tmp_path / "execution-history.json")
    service = ExecutionHistoryService(store)
    events: list[str] = []

    def sink(message: Any) -> None:
        events.append(str(message.record["message"]))

    sink_id = logger.add(sink, level="INFO")
    app.dependency_overrides[get_execution_history_service] = lambda: service
    app.dependency_overrides[get_tenant_context] = lambda: TenantContext("tenant-A")
    try:
        with TestClient(app) as client:
            yield client, store, events
    finally:
        app.dependency_overrides.clear()
        logger.remove(sink_id)
        store.close()


def _has_event(events: list[str], operation: str, outcome: str, status_code: int) -> bool:
    expected = (
        f"execution_history operation={operation} outcome={outcome} status_code={status_code}"
    )
    return any(expected in event for event in events)


@pytest.mark.unit
def test_w103_openapi_and_dto_contract(
    api_client: tuple[TestClient, DurableExecutionStore, list[str]],
) -> None:
    client, _, _ = api_client
    response = client.get("/openapi.json")
    assert response.status_code == 200
    document = response.json()
    paths = document["paths"]
    expected = {
        "/api/v1/forecast/executions",
        "/api/v1/forecast/executions/{execution_id}",
        "/api/v1/forecast/executions/reference/{reference_key}",
        "/api/v1/forecast/executions/{execution_id}/diagnostic",
        "/api/v1/forecast/executions/statistics",
    }
    assert expected <= set(paths)
    for path in expected:
        operation = paths[path]["get"]
        assert set(paths[path]) == {"get"}
        assert {"401", "403", "404", "422", "503"} <= set(operation["responses"])

    list_parameters = {
        parameter["name"]
        for parameter in paths["/api/v1/forecast/executions"]["get"]["parameters"]
        if parameter["in"] == "query"
    }
    assert {
        "tenant_id",
        "entity_type",
        "entity_id",
        "target_metric",
        "execution_id",
        "reference_key",
        "model_id",
        "model_version",
        "algorithm",
        "feature_schema_version",
        "feature_schema_fingerprint",
        "status",
        "created_at_from",
        "created_at_to",
        "completed_at_from",
        "completed_at_to",
        "limit",
        "offset",
    } <= list_parameters

    schemas = document["components"]["schemas"]
    assert {
        "ExecutionOut",
        "ExecutionListOut",
        "ExecutionStatsOut",
        "ExecutionDiagnosticOut",
        "FailureDiagnosticOut",
        "ExecutionErrorOut",
    } <= set(schemas)
    assert set(ExecutionOut.model_json_schema()["required"]) == {
        "execution_id",
        "reference_key",
        "tenant_id",
        "entity_type",
        "entity_id",
        "target_metric",
        "frequency",
        "horizon",
        "model_id",
        "model_version",
        "algorithm",
        "feature_schema_version",
        "feature_schema_fingerprint",
        "artifact_uri",
        "created_at",
        "started_at",
        "completed_at",
        "status",
        "error_code",
        "error_reason",
    }
    assert set(ExecutionErrorOut.model_json_schema()["required"]) == {"detail"}
    assert "failure" in ExecutionDiagnosticOut.model_json_schema()["properties"]
    assert "error_code" in FailureDiagnosticOut.model_json_schema()["properties"]


@pytest.mark.unit
def test_w103_observability_outcomes_and_read_only(
    api_client: tuple[TestClient, DurableExecutionStore, list[str]],
) -> None:
    client, store, events = api_client
    before = store.path.read_bytes()

    assert _get(client, "/api/v1/forecast/executions").status_code == 200
    assert _get(client, "/api/v1/forecast/executions/unknown").status_code == 404
    assert _get(client, "/api/v1/forecast/executions", created_at_from="bad").status_code == 422
    assert _get(client, "/api/v1/forecast/executions", tenant_id="tenant-B").status_code == 403
    assert _get(client, "/api/v1/forecast/executions/w103-a-failed/diagnostic").status_code == 200

    assert _has_event(events, "list", "success", 200)
    assert _has_event(events, "get", "not_found", 404)
    assert _has_event(events, "query", "invalid_request", 422)
    assert _has_event(events, "query", "tenant_rejected", 403)
    assert _has_event(events, "diagnostic", "success", 200)
    assert store.path.read_bytes() == before


@pytest.mark.unit
def test_w103_tenant_isolation_on_all_read_endpoints(
    api_client: tuple[TestClient, DurableExecutionStore, list[str]],
) -> None:
    client, _, _ = api_client
    assert _get(client, "/api/v1/forecast/executions").json()["total"] == 2
    assert _get(client, "/api/v1/forecast/executions/w103-b-success").status_code == 404
    assert (
        _get(client, "/api/v1/forecast/executions/reference/ref:w103-b-success").status_code == 404
    )
    assert _get(client, "/api/v1/forecast/executions/w103-b-success/diagnostic").status_code == 404
    assert _get(client, "/api/v1/forecast/executions/statistics").json()["total"] == 2

    app.dependency_overrides[get_tenant_context] = lambda: TenantContext("tenant-B")
    try:
        assert _get(client, "/api/v1/forecast/executions/w103-b-success").status_code == 200
        assert (
            _get(client, "/api/v1/forecast/executions/reference/ref:w103-b-success").status_code
            == 200
        )
        assert (
            _get(client, "/api/v1/forecast/executions/w103-b-success/diagnostic").status_code == 200
        )
        assert _get(client, "/api/v1/forecast/executions/statistics").json()["total"] == 1
    finally:
        app.dependency_overrides[get_tenant_context] = lambda: TenantContext("tenant-A")


@pytest.mark.unit
def test_w103_statistics_subsets_and_empty_result(
    api_client: tuple[TestClient, DurableExecutionStore, list[str]],
) -> None:
    client, _, _ = api_client
    mixed = _get(client, "/api/v1/forecast/executions/statistics").json()
    success = _get(client, "/api/v1/forecast/executions/statistics", status="SUCCESS").json()
    failed = _get(client, "/api/v1/forecast/executions/statistics", status="FAILED").json()
    empty = _get(client, "/api/v1/forecast/executions/statistics", target_metric="absent").json()

    assert mixed["total"] == 2
    assert mixed["success_count"] == 1
    assert mixed["failed_count"] == 1
    assert mixed["success_rate"] == 0.5
    assert success["total"] == 1 and success["success_rate"] == 1.0
    assert failed["total"] == 1 and failed["success_rate"] == 0.0
    assert empty["total"] == 0
    assert empty["success_rate"] == 0.0
    assert empty["total_duration"] is None
    assert empty["average_duration"] is None


@pytest.mark.unit
def test_w103_error_sanitization_and_diagnostic_unavailable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    corrupt = tmp_path / "corrupt.json"
    corrupt.write_text("{broken", encoding="utf-8")
    monkeypatch.setattr(settings, "trendx_execution_history_path", str(corrupt))
    app.dependency_overrides.pop(get_execution_history_service, None)
    app.dependency_overrides[get_tenant_context] = lambda: TenantContext("tenant-A")
    events: list[str] = []

    def sink(message: Any) -> None:
        events.append(str(message.record["message"]))

    sink_id = logger.add(sink, level="INFO")
    try:
        with TestClient(app) as client:
            corrupt_response = _get(client, "/api/v1/forecast/executions")
            assert corrupt_response.status_code == 503
            assert corrupt_response.json() == {"detail": "execution_history_unavailable"}
            assert "traceback" not in corrupt_response.text.lower()
            assert "broken" not in corrupt_response.text.lower()

            class NoDiagnosticService:
                def get(self, execution_id: str) -> ExecutionRecord | None:
                    return ExecutionRecord(
                        execution_id="diagnostic-unavailable",
                        reference_key="ref:diagnostic-unavailable",
                        tenant_id="tenant-A",
                        entity_type="DEVICE",
                        entity_id="entity-A",
                        target_metric="temperature",
                        frequency="1h",
                        horizon=24,
                        status=ExecutionStatus.FAILED,
                        created_at=_timestamp(1),
                        error_code="feature_schema_mismatch",
                        error_reason="w103 fixture",
                    )

                def failure_diagnostics(self, query: Any) -> tuple[Any, ...]:
                    return ()

            app.dependency_overrides[get_execution_history_service] = lambda: NoDiagnosticService()
            unavailable = _get(
                client,
                "/api/v1/forecast/executions/diagnostic-unavailable/diagnostic",
            )
            assert unavailable.status_code == 503
            assert unavailable.json() == {"detail": "diagnostic_unavailable"}
    finally:
        logger.remove(sink_id)
        app.dependency_overrides.clear()

    assert _has_event(events, "service", "datastore_unavailable", 503)
    assert _has_event(events, "diagnostic", "diagnostic_unavailable", 503)


@pytest.mark.unit
def test_w103_diagnostics_are_read_only_and_known_codes(
    api_client: tuple[TestClient, DurableExecutionStore, list[str]],
) -> None:
    client, store, _ = api_client
    before = store.path.read_bytes()
    response = _get(client, "/api/v1/forecast/executions/w103-a-failed/diagnostic")
    assert response.status_code == 200
    body = response.json()
    assert body["failure"]["error_code"] == "feature_schema_mismatch"
    assert body["failure"]["execution_id"] == "w103-a-failed"
    assert store.path.read_bytes() == before
