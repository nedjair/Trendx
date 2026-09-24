"""W104 — deterministic operational analytics over Execution History."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from collections.abc import Iterator
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient
from loguru import logger
from trendx.config import settings
from trendx.forecasting.analytics import ExecutionHistoryAnalytics, analyze_execution_history
from trendx.forecasting.api import (
    TenantContext,
    get_execution_history_service,
    get_tenant_context,
)
from trendx.forecasting.execution import (
    DurableExecutionStore,
    ExecutionProvenance,
    ExecutionRecord,
    MemoryExecutionStore,
)
from trendx.forecasting.history import (
    ExecutionHistoryPage,
    ExecutionHistoryService,
    ExecutionQuery,
    ExecutionStatistics,
)
from trendx.main import app

BASE_TIME = datetime(2026, 1, 1, tzinfo=UTC)
ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src"


def _timestamp(minutes: int) -> str:
    return (BASE_TIME + timedelta(minutes=minutes)).isoformat()


def _provenance(
    *,
    model: str,
    version: str,
    algorithm: str,
    predictions: int,
    fingerprint: str = "fingerprint-A",
) -> ExecutionProvenance:
    return ExecutionProvenance(
        model_id=model,
        model_version=version,
        algorithm=algorithm,
        feature_schema_version="schema-A",
        feature_schema_fingerprint=fingerprint,
        artifact_uri=f"file:///w104/{model}-{version}.bin",
        prediction_count=predictions,
        metadata={"source": "w104"},
        result={"values": [1.0] * predictions},
    )


def _record(
    execution_id: str,
    *,
    tenant: str = "tenant-A",
    entity_type: str = "DEVICE",
    entity: str = "entity-A",
    metric: str = "temperature",
    model: str = "model-A",
    version: str = "1",
    algorithm: str = "Fourier",
    created: int = 10,
) -> ExecutionRecord:
    return ExecutionRecord(
        execution_id=execution_id,
        reference_key=f"ref:{execution_id}",
        tenant_id=tenant,
        entity_type=entity_type,
        entity_id=entity,
        target_metric=metric,
        frequency="1h",
        horizon=24,
        model_id=model,
        model_version=version,
        algorithm=algorithm,
        feature_schema_version="schema-A",
        feature_schema_fingerprint="fingerprint-A",
        artifact_uri=f"file:///w104/{model}-{version}.bin",
        created_at=_timestamp(created),
        metadata={"source": "w104"},
    )


def _seed(path: Path) -> DurableExecutionStore:
    store = DurableExecutionStore(path)
    fixtures = (
        (
            _record("A-temp", metric="temperature", model="model-A", created=50),
            "model-A",
            "1",
            "Fourier",
            4,
            None,
        ),
        (
            _record("A-humidity", metric="humidity", model="model-A", version="2", created=40),
            "model-A",
            "2",
            "Fourier",
            3,
            None,
        ),
        (
            _record("A-energy-failed", metric="energy_consumption", model="model-B", created=30),
            "model-B",
            "1",
            "Prophet",
            0,
            "feature_schema_mismatch",
        ),
        (
            _record(
                "A-pressure-failed",
                entity="entity-A2",
                metric="pressure",
                model="model-C",
                created=20,
            ),
            "model-C",
            "1",
            "Fourier",
            0,
            "no-compatible-champion",
        ),
        (
            _record(
                "A-solar",
                metric="solar_power",
                model="model-D",
                version="3",
                algorithm="ARIMA",
                created=10,
            ),
            "model-D",
            "3",
            "ARIMA",
            5,
            None,
        ),
        (
            _record(
                "B-energy",
                tenant="tenant-B",
                entity="entity-B",
                metric="energy_consumption",
                model="model-B",
                created=60,
            ),
            "model-B",
            "1",
            "Fourier",
            6,
            None,
        ),
        (
            _record(
                "B-temp-failed",
                tenant="tenant-B",
                entity="entity-B",
                metric="temperature",
                model="model-E",
                created=5,
            ),
            "model-E",
            "1",
            "Prophet",
            0,
            "artifact-corrupted",
        ),
    )
    for record, model, version, algorithm, predictions, error_code in fixtures:
        store.create(record)
        store.mark_started(record.execution_id)
        if error_code is None:
            store.mark_success(
                record.execution_id,
                _provenance(
                    model=model,
                    version=version,
                    algorithm=algorithm,
                    predictions=predictions,
                ),
            )
        else:
            store.mark_failed(
                record.execution_id,
                error_code=error_code,
                error_reason="w104 synthetic failure",
                provenance=_provenance(
                    model=model,
                    version=version,
                    algorithm=algorithm,
                    predictions=0,
                ),
            )
    store.close()
    return DurableExecutionStore(path)


def _auth_headers() -> dict[str, str]:
    return {"Authorization": f"Bearer {settings.trendx_api_token.get_secret_value()}"}


def _get(client: TestClient, path: str, **params: Any) -> Any:
    return client.get(path, params=params or None, headers=_auth_headers())


class _QueryOnlyHistoryService(ExecutionHistoryService):
    """Test double proving analytics derives all values from one query snapshot."""

    def __init__(self, records: tuple[ExecutionRecord, ...]) -> None:
        self._records = records
        self.query_calls = 0

    def query(self, query: ExecutionQuery | None = None) -> ExecutionHistoryPage:
        self.query_calls += 1
        return ExecutionHistoryPage(
            records=self._records,
            total=len(self._records),
            limit=None,
            offset=0,
        )

    def statistics(self, query: ExecutionQuery | None = None) -> ExecutionStatistics:
        raise AssertionError("analytics must not rescan the mutable history service")


@pytest.fixture
def analytics_client(tmp_path: Path) -> Iterator[tuple[TestClient, DurableExecutionStore]]:
    store = _seed(tmp_path / "execution-history.json")
    service = ExecutionHistoryService(store)
    app.dependency_overrides[get_execution_history_service] = lambda: service
    app.dependency_overrides[get_tenant_context] = lambda: TenantContext("tenant-A")
    try:
        with TestClient(app) as client:
            yield client, store
    finally:
        app.dependency_overrides.clear()
        store.close()


@pytest.mark.unit
def test_w104_analytics_dimensions_are_generic_and_deterministic(tmp_path: Path) -> None:
    store = _seed(tmp_path / "history.json")
    try:
        service = ExecutionHistoryService(store)
        query = ExecutionQuery(tenant_id="tenant-A")
        first = analyze_execution_history(service, query).to_dict()
        second = analyze_execution_history(service, query).to_dict()
        assert first == second
        assert first["summary"] == service.statistics(query).to_dict()
        assert first["tenant_id"] == "tenant-A"
        assert first["summary"]["total_executions"] == 5
        assert first["summary"]["success_count"] == 3
        assert first["summary"]["failure_count"] == 2
        assert first["summary"]["success_rate"] == 0.6
        assert first["summary"]["prediction_count_total"] == 12
        assert first["summary"]["duration_sample_count"] == 5
        assert first["summary"]["total_duration_seconds"] is not None
        assert first["summary"]["average_duration_seconds"] is not None
        assert [item["key"] for item in first["by_metric"]] == [
            "energy_consumption",
            "humidity",
            "pressure",
            "solar_power",
            "temperature",
        ]
        assert [item["entity_id"] for item in first["by_entity"]] == [
            "entity-A",
            "entity-A2",
        ]
        assert [item["key"] for item in first["by_algorithm"]] == ["ARIMA", "Fourier", "Prophet"]
        assert [(item["model_id"], item["model_version"]) for item in first["by_model"]] == [
            ("model-A", "1"),
            ("model-A", "2"),
            ("model-B", "1"),
            ("model-C", "1"),
            ("model-D", "3"),
        ]
        assert {item["key"] for item in first["by_status"]} == {"FAILED", "SUCCESS"}
        assert first["failures"] == [
            {"count": 1, "error_code": "feature_schema_mismatch", "proportion": 0.5},
            {"count": 1, "error_code": "no-compatible-champion", "proportion": 0.5},
        ]
        assert first["trend_contract"] == {
            "dimension": "created_at",
            "bucket": "exact_timestamp",
            "timezone": "UTC",
            "inclusivity": "inclusive",
            "ordering": "ascending",
        }
        assert len(first["created_at_trend"]) == 5
    finally:
        store.close()


@pytest.mark.unit
def test_w104_analytics_uses_one_history_snapshot(tmp_path: Path) -> None:
    store = _seed(tmp_path / "history.json")
    records = tuple(record for record in store.list() if record.tenant_id == "tenant-A")
    store.close()
    service = _QueryOnlyHistoryService(records)
    result = analyze_execution_history(service, ExecutionQuery(tenant_id="tenant-A"))
    assert service.query_calls == 1
    assert result.summary.total_executions == 5
    assert len(result.by_metric) == 5


@pytest.mark.unit
def test_w104_analytics_filters_and_inclusive_windows(tmp_path: Path) -> None:
    store = _seed(tmp_path / "history.json")
    try:
        service = ExecutionHistoryService(store)
        analytics = ExecutionHistoryAnalytics(service)
        empty = analytics.analyze(
            ExecutionQuery(tenant_id="tenant-A", created_at_from="2026-01-02T00:00:00Z")
        )
        assert empty.summary.total_executions == 0
        assert empty.by_metric == ()
        assert empty.failures == ()
        assert empty.created_at_trend == ()

        exact = analytics.analyze(
            ExecutionQuery(
                tenant_id="tenant-A",
                created_at_from="2026-01-01T00:10:00Z",
                created_at_to="2026-01-01T00:10:00Z",
            )
        )
        assert exact.summary.total_executions == 1
        assert exact.by_metric[0].key == "solar_power"

        metric = analytics.analyze(ExecutionQuery(tenant_id="tenant-A", target_metric="humidity"))
        assert metric.summary.total_executions == 1
        assert metric.by_status[0].key == "SUCCESS"

        paged = analytics.analyze(ExecutionQuery(tenant_id="tenant-A", limit=1, offset=1))
        assert paged.summary.total_executions == 5
        assert len(paged.by_metric) == 5
    finally:
        store.close()


@pytest.mark.unit
def test_w104_analytics_normalizes_equivalent_offsets_into_one_exact_bucket() -> None:
    store = MemoryExecutionStore()
    first = replace(
        _record("offset-a", created=0),
        created_at="2026-01-01T01:00:00+01:00",
        started_at="2026-01-01T00:00:00Z",
        completed_at="2026-01-01T00:00:10Z",
    )
    second = replace(
        _record("offset-b", created=0),
        created_at="2025-12-31T19:00:00-05:00",
        started_at="2026-01-01T01:00:00+01:00",
        completed_at="2026-01-01T01:00:10+01:00",
    )
    store.create(first)
    store.create(second)
    result = analyze_execution_history(
        ExecutionHistoryService(store),
        ExecutionQuery(
            tenant_id="tenant-A",
            created_at_from="2025-12-31T19:00:00-05:00",
            created_at_to="2026-01-01T01:00:00+01:00",
        ),
    )
    assert result.summary.total_executions == 2
    assert len(result.created_at_trend) == 1
    assert result.created_at_trend[0].bucket == "2026-01-01T00:00:00+00:00"
    completed = analyze_execution_history(
        ExecutionHistoryService(store),
        ExecutionQuery(
            tenant_id="tenant-A",
            completed_at_from="2026-01-01T00:00:10Z",
            completed_at_to="2026-01-01T00:00:10Z",
        ),
    )
    assert completed.summary.total_executions == 2
    assert completed.summary.duration_sample_count == 2


@pytest.mark.unit
def test_w104_analytics_requires_tenant_scope_and_handles_empty_store(tmp_path: Path) -> None:
    path = tmp_path / "history.json"
    store = DurableExecutionStore(path)
    try:
        service = ExecutionHistoryService(store)
        with pytest.raises(ValueError, match="tenant"):
            analyze_execution_history(service, ExecutionQuery())
        result = analyze_execution_history(service, ExecutionQuery(tenant_id="tenant-A"))
        assert result.summary.total_executions == 0
        assert result.summary.success_count == 0
        assert result.summary.failure_count == 0
        assert result.summary.success_rate == 0.0
        assert result.summary.prediction_count_total == 0
        assert result.summary.total_duration_seconds is None
        assert result.summary.average_duration_seconds is None
    finally:
        store.close()


@pytest.mark.unit
def test_w104_analytics_api_contract_and_tenant_scope(
    analytics_client: tuple[TestClient, DurableExecutionStore],
) -> None:
    client, store = analytics_client
    before = tuple(record.to_dict() for record in store.list())
    response = _get(client, "/api/v1/forecast/executions/analytics")
    assert response.status_code == 200
    body = response.json()
    assert body["tenant_id"] == "tenant-A"
    assert body["summary"]["total"] == 5
    assert body["summary"]["success_count"] == 3
    assert body["summary"]["failed_count"] == 2
    assert body["summary"]["success_rate"] == 0.6
    assert body["trend_contract"]["bucket"] == "exact_timestamp"
    assert {item["key"] for item in body["by_metric"]} >= {
        "temperature",
        "humidity",
        "energy_consumption",
        "solar_power",
    }
    assert tuple(record.to_dict() for record in store.list()) == before

    openapi = client.get("/openapi.json").json()
    analytics_operation = openapi["paths"]["/api/v1/forecast/executions/analytics"]["get"]
    assert set(analytics_operation["responses"]) >= {"200", "401", "403", "404", "422", "503"}
    assert all(
        parameter["name"] not in {"limit", "offset"}
        for parameter in analytics_operation["parameters"]
        if parameter["in"] == "query"
    )
    assert set(openapi["paths"]["/api/v1/forecast/executions/analytics"]) == {"get"}

    assert (
        _get(client, "/api/v1/forecast/executions/analytics", tenant_id="tenant-B").status_code
        == 403
    )
    app.dependency_overrides[get_tenant_context] = lambda: TenantContext("tenant-B")
    try:
        tenant_b = _get(client, "/api/v1/forecast/executions/analytics")
        assert tenant_b.status_code == 200
        assert tenant_b.json()["tenant_id"] == "tenant-B"
        assert tenant_b.json()["summary"]["total"] == 2
    finally:
        app.dependency_overrides[get_tenant_context] = lambda: TenantContext("tenant-A")


@pytest.mark.unit
def test_w104_analytics_api_error_cases_and_logs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    missing = tmp_path / "missing.json"
    monkeypatch.setattr(settings, "trendx_execution_history_path", str(missing))
    app.dependency_overrides.pop(get_execution_history_service, None)
    app.dependency_overrides[get_tenant_context] = lambda: TenantContext("tenant-A")
    events: list[str] = []

    def sink(message: Any) -> None:
        events.append(str(message.record["message"]))

    sink_id = logger.add(sink, level="INFO")
    try:
        with TestClient(app) as client:
            response = _get(client, "/api/v1/forecast/executions/analytics")
            assert response.status_code == 503
            assert response.json() == {"detail": "execution_history_unavailable"}
            assert not missing.exists()
            invalid = _get(client, "/api/v1/forecast/executions/analytics", created_at_from="bad")
            assert invalid.status_code == 422
            assert invalid.json() == {"detail": "invalid_query"}

            corrupt = tmp_path / "corrupt.json"
            corrupt.write_text("{broken", encoding="utf-8")
            monkeypatch.setattr(settings, "trendx_execution_history_path", str(corrupt))
            corrupted = _get(client, "/api/v1/forecast/executions/analytics")
            assert corrupted.status_code == 503
            assert corrupted.json() == {"detail": "execution_history_unavailable"}
            assert "broken" not in corrupted.text.lower()

            invalid_created = tmp_path / "invalid-created.json"
            invalid_store = DurableExecutionStore(invalid_created)
            invalid_record = replace(_record("invalid-created"), created_at="not-a-timestamp")
            invalid_store.create(invalid_record)
            invalid_store.mark_started(invalid_record.execution_id)
            invalid_store.mark_success(
                invalid_record.execution_id,
                _provenance(model="model-A", version="1", algorithm="Fourier", predictions=1),
            )
            invalid_store.close()
            monkeypatch.setattr(settings, "trendx_execution_history_path", str(invalid_created))
            invalid_history = _get(client, "/api/v1/forecast/executions/analytics")
            assert invalid_history.status_code == 503
            assert invalid_history.json() == {"detail": "execution_history_unavailable"}
            assert "not-a-timestamp" not in invalid_history.text.lower()

            class ReaderFailure:
                def query(self, query: ExecutionQuery | None = None) -> Any:
                    raise OSError("private-reader-details")

            app.dependency_overrides[get_execution_history_service] = lambda: ReaderFailure()
            reader_failure = _get(client, "/api/v1/forecast/executions/analytics")
            assert reader_failure.status_code == 503
            assert reader_failure.json() == {"detail": "execution_history_unavailable"}
            assert "private-reader-details" not in reader_failure.text.lower()
    finally:
        logger.remove(sink_id)
        app.dependency_overrides.clear()
    assert any(
        "operation=service outcome=datastore_unavailable status_code=503" in event
        for event in events
    )
    assert any(
        "operation=analytics_query outcome=invalid_request status_code=422" in event
        for event in events
    )
    joined_events = "\n".join(events).casefold()
    for forbidden in ("broken", "not-a-timestamp", "traceback", str(missing).casefold()):
        assert forbidden not in joined_events
    token = settings.trendx_api_token.get_secret_value().casefold()
    if token:
        assert token not in joined_events


@pytest.mark.unit
def test_w104_process_a_b_analytics_results_are_identical(tmp_path: Path) -> None:
    path = tmp_path / "history.json"
    store = _seed(path)
    store.close()
    child = r"""
import json
import sys
from trendx.forecasting.analytics import analyze_execution_history
from trendx.forecasting.execution import DurableExecutionStore
from trendx.forecasting.history import ExecutionHistoryService, ExecutionQuery

store = DurableExecutionStore(sys.argv[1])
result = analyze_execution_history(
    ExecutionHistoryService(store),
    ExecutionQuery(tenant_id="tenant-A"),
)
print(json.dumps(result.to_dict(), sort_keys=True))
store.close()
"""
    env = os.environ.copy()
    env["PYTHONPATH"] = str(SRC) + os.pathsep + env.get("PYTHONPATH", "")
    env.update(
        {
            "PYTHONDONTWRITEBYTECODE": "1",
            "TRENDX_SCHEDULER_FORECAST_ENABLED": "false",
            "TRENDX_INGEST_ENABLED": "false",
            "TRENDX_WORKER_INGESTION_ENABLED": "false",
            "TB_WRITEBACK_ENABLED": "false",
            "TB_ALARMS_ENABLED": "false",
        }
    )
    first = subprocess.run(
        [sys.executable, "-c", child, str(path)],
        cwd=ROOT,
        env=env,
        text=True,
        capture_output=True,
        check=True,
        timeout=30,
    )
    second = subprocess.run(
        [sys.executable, "-c", child, str(path)],
        cwd=ROOT,
        env=env,
        text=True,
        capture_output=True,
        check=True,
        timeout=30,
    )
    assert first.stdout.strip() == second.stdout.strip()


@pytest.mark.unit
def test_w104_process_a_b_api_analytics_results_are_identical(tmp_path: Path) -> None:
    path = tmp_path / "history.json"
    store = _seed(path)
    store.close()
    child = r"""
import json
from fastapi.testclient import TestClient
from trendx.config import settings
from trendx.main import app

headers = {"Authorization": f"Bearer {settings.trendx_api_token.get_secret_value()}"}
with TestClient(app) as client:
    response = client.get("/api/v1/forecast/executions/analytics", headers=headers)
    print(json.dumps({"status_code": response.status_code, "body": response.json()}, sort_keys=True))
"""
    env = os.environ.copy()
    env["PYTHONPATH"] = str(SRC) + os.pathsep + env.get("PYTHONPATH", "")
    env.update(
        {
            "PYTHONDONTWRITEBYTECODE": "1",
            "TRENDX_EXECUTION_HISTORY_PATH": str(path),
            "TRENDX_DEFAULT_TENANT_ID": "tenant-A",
            "TRENDX_SCHEDULER_ENABLED": "false",
            "TRENDX_SCHEDULER_FORECAST_ENABLED": "false",
            "TRENDX_INGEST_ENABLED": "false",
            "TRENDX_WORKER_INGESTION_ENABLED": "false",
            "TB_WRITEBACK_ENABLED": "false",
            "TB_ALARMS_ENABLED": "false",
            "ANOMALY_DETECTION_ENABLED": "false",
            "MLFLOW_TRACKING_URI": "file:///tmp/trendx-w104-process-api",
        }
    )
    first = subprocess.run(
        [sys.executable, "-c", child],
        cwd=ROOT,
        env=env,
        text=True,
        capture_output=True,
        check=True,
        timeout=30,
    )
    second = subprocess.run(
        [sys.executable, "-c", child],
        cwd=ROOT,
        env=env,
        text=True,
        capture_output=True,
        check=True,
        timeout=30,
    )
    first_line = first.stdout.splitlines()[-1]
    second_line = second.stdout.splitlines()[-1]
    assert first_line == second_line
    first_evidence = json.loads(first_line)
    assert first_evidence["status_code"] == 200
    assert first_evidence["body"]["summary"]["total"] == 5


@pytest.mark.unit
def test_w104_analytics_scales_to_a_larger_durable_snapshot(tmp_path: Path) -> None:
    path = tmp_path / "history.json"
    store = DurableExecutionStore(path)
    for index in range(120):
        record = _record(
            f"large-{index}",
            entity=f"entity-{index % 3}",
            metric=f"metric-{index % 4}",
            created=index,
        )
        store.create(record)
        store.mark_started(record.execution_id)
        store.mark_success(
            record.execution_id,
            _provenance(
                model="model-A",
                version="1",
                algorithm="Fourier",
                predictions=index % 5,
            ),
        )
    try:
        service = ExecutionHistoryService(store)
        first = analyze_execution_history(service, ExecutionQuery(tenant_id="tenant-A"))
        second = analyze_execution_history(service, ExecutionQuery(tenant_id="tenant-A"))
        assert first.summary.total_executions == 120
        assert len(first.by_entity) == 3
        assert len(first.by_metric) == 4
        assert first.to_dict() == second.to_dict()
    finally:
        store.close()
