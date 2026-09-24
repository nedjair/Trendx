"""W106 — real-process operational validation of execution-history reporting."""

from __future__ import annotations

import hashlib
import math
import os
import socket
import subprocess
import sys
import time
import tracemalloc
from collections.abc import Iterable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import IO, Any

import httpx
from trendx.forecasting.analytics import (
    TREND_CONTRACT,
    ExecutionAnalytics,
    analyze_execution_history,
)
from trendx.forecasting.execution import (
    DurableExecutionStore,
    ExecutionProvenance,
    ExecutionRecord,
)
from trendx.forecasting.history import ExecutionHistoryPage, ExecutionHistoryService, ExecutionQuery
from trendx.forecasting.reporting import ExecutionAnalyticsReportingService

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src"
TOKEN = "w106-test-token"
BASE_TIME = datetime(2026, 1, 1, tzinfo=UTC)
TEST_FLAGS = {
    "TRENDX_SCHEDULER_ENABLED": "false",
    "TRENDX_SCHEDULER_FORECAST_ENABLED": "false",
    "TRENDX_INGEST_ENABLED": "false",
    "TRENDX_WORKER_INGESTION_ENABLED": "false",
    "TB_WRITEBACK_ENABLED": "false",
    "TB_ALARMS_ENABLED": "false",
    "ANOMALY_DETECTION_ENABLED": "false",
}

# Self-contained tests: intentionally not marked ``integration`` because the
# repository's integration marker requires an external catalog database.  They
# are collected by the full suite and excluded from ``-m unit``.


@dataclass(frozen=True)
class RecordSpec:
    execution_id: str
    tenant: str
    entity_type: str
    entity_id: str
    metric: str
    model: str
    version: str
    algorithm: str
    schema: str
    fingerprint: str
    created_at: str
    status: str = "SUCCESS"
    error_code: str = ""
    predictions: int = 1
    completed_at: str = "2099-01-01T00:00:10Z"


@dataclass
class LiveServer:
    process: subprocess.Popen[str]
    url: str
    log_path: Path
    log_handle: IO[str]


def _record(spec: RecordSpec, artifact_root: Path) -> ExecutionRecord:
    return ExecutionRecord(
        execution_id=spec.execution_id,
        reference_key=f"w106:{spec.execution_id}",
        tenant_id=spec.tenant,
        entity_type=spec.entity_type,
        entity_id=spec.entity_id,
        target_metric=spec.metric,
        frequency="1h",
        horizon=24,
        model_id=spec.model,
        model_version=spec.version,
        algorithm=spec.algorithm,
        feature_schema_version=spec.schema,
        feature_schema_fingerprint=spec.fingerprint,
        artifact_uri=(artifact_root / f"{spec.execution_id}.bin").as_uri(),
        created_at=spec.created_at,
        metadata={"source": "w106"},
    )


def _provenance(record: ExecutionRecord, predictions: int) -> ExecutionProvenance:
    return ExecutionProvenance(
        model_id=record.model_id,
        model_version=record.model_version,
        algorithm=record.algorithm,
        feature_schema_version=record.feature_schema_version,
        feature_schema_fingerprint=record.feature_schema_fingerprint,
        artifact_uri=record.artifact_uri,
        prediction_count=predictions,
        metadata={"source": "w106"},
        result={"values": [1.0] * predictions},
    )


def _seed(path: Path, specs: Iterable[RecordSpec], artifact_root: Path) -> None:
    store = DurableExecutionStore(path)
    try:
        for spec in specs:
            record = _record(spec, artifact_root)
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
                    error_reason="W106 controlled failure",
                    provenance=_provenance(record, 0),
                    completed_at=spec.completed_at,
                )
            elif spec.status == "SUCCESS":
                store.mark_success(
                    record.execution_id,
                    _provenance(record, spec.predictions),
                    completed_at=spec.completed_at,
                )
            else:
                msg = f"Unsupported W106 fixture status: {spec.status}"
                raise ValueError(msg)
    finally:
        store.close()


def _base_specs() -> tuple[RecordSpec, ...]:
    return (
        RecordSpec(
            "a-success",
            "tenant-A",
            "DEVICE",
            "device-a",
            "temperature",
            "model-a",
            "1",
            "Fourier",
            "schema-a",
            "fingerprint-a",
            "2026-01-01T00:10:00Z",
            predictions=3,
        ),
        RecordSpec(
            "a-failure-schema",
            "tenant-A",
            "ASSET",
            "asset-a",
            "energy_consumption",
            "model-b",
            "1",
            "Prophet",
            "schema-b",
            "fingerprint-b",
            "2026-01-01T00:20:00+00:00",
            status="FAILED",
            error_code="feature_schema_mismatch",
        ),
        RecordSpec(
            "a-failure-champion",
            "tenant-A",
            "ASSET",
            "asset-a",
            "energy_consumption",
            "model-b",
            "1",
            "Prophet",
            "schema-b",
            "fingerprint-b",
            "2026-01-01T00:30:00Z",
            status="FAILED",
            error_code="no-compatible-champion",
        ),
        RecordSpec(
            "a-failure-artifact",
            "tenant-A",
            "DEVICE",
            "device-b",
            "pressure",
            "model-c",
            "2",
            "ARIMA",
            "schema-c",
            "fingerprint-c",
            "2026-01-01T00:40:00Z",
            status="FAILED",
            error_code="artifact-missing",
        ),
        RecordSpec(
            "a-failure-payload",
            "tenant-A",
            "DEVICE",
            "device-b",
            "flow_rate",
            "model-c",
            "2",
            "ARIMA",
            "schema-c",
            "fingerprint-c",
            "2026-01-01T00:50:00Z",
            status="FAILED",
            error_code="payload_invalid",
        ),
        RecordSpec(
            "a-failure-corrupted",
            "tenant-A",
            "DEVICE",
            "device-b",
            "solar_power",
            "model-c",
            "2",
            "ARIMA",
            "schema-c",
            "fingerprint-c",
            "2026-01-01T01:00:00Z",
            status="FAILED",
            error_code="artifact-corrupted",
        ),
        RecordSpec(
            "a-pending",
            "tenant-A",
            "DEVICE",
            "device-c",
            "temperature",
            "model-a",
            "1",
            "Fourier",
            "schema-a",
            "fingerprint-a",
            "2026-01-01T01:10:00Z",
            status="PENDING",
        ),
        RecordSpec(
            "a-running",
            "tenant-A",
            "DEVICE",
            "device-c",
            "humidity",
            "model-d",
            "3",
            "Prophet",
            "schema-d",
            "fingerprint-d",
            "2026-01-01T01:20:00Z",
            status="RUNNING",
        ),
        RecordSpec(
            "b-success",
            "tenant-B",
            "DEVICE",
            "device-only-b",
            "temperature",
            "model-b",
            "9",
            "Fourier",
            "schema-b",
            "fingerprint-b",
            "2026-01-01T01:30:00Z",
            predictions=4,
        ),
    )


def _large_specs(count: int, prefix: str) -> tuple[RecordSpec, ...]:
    algorithms = ("Fourier", "Prophet", "ARIMA")
    return tuple(
        RecordSpec(
            f"{prefix}-{index}",
            "tenant-A",
            "DEVICE" if index % 2 else "ASSET",
            f"entity-{index % 7}",
            f"metric-{index % 5}",
            f"model-{index % 4}",
            str(index % 2 + 1),
            algorithms[index % len(algorithms)],
            f"schema-{index % 3}",
            f"fingerprint-{index % 4}",
            (BASE_TIME + timedelta(minutes=2000 + index)).isoformat(),
            predictions=index % 6,
        )
        for index in range(count)
    )


def _direct_report(path: Path, query: ExecutionQuery) -> tuple[ExecutionAnalytics, Any]:
    store = DurableExecutionStore(path)
    try:
        service = ExecutionHistoryService(store)
        analytics = analyze_execution_history(service, query)
        report = ExecutionAnalyticsReportingService(service).report(query)
        return analytics, report
    finally:
        store.close()


def _w104_api_payload(analytics: ExecutionAnalytics) -> dict[str, Any]:
    summary = analytics.summary
    return {
        "summary": {
            "total": summary.total_executions,
            "success_count": summary.success_count,
            "failed_count": summary.failure_count,
            "success_rate": summary.success_rate,
            "total_prediction_count": summary.prediction_count_total,
            "duration_sample_count": summary.duration_sample_count,
            "total_duration": summary.total_duration_seconds,
            "average_duration": summary.average_duration_seconds,
        },
        "dimensions": {
            "by_metric": [item.to_dict() for item in analytics.by_metric],
            "by_entity": [item.to_dict() for item in analytics.by_entity],
            "by_algorithm": [item.to_dict() for item in analytics.by_algorithm],
            "by_model": [item.to_dict() for item in analytics.by_model],
            "by_status": [item.to_dict() for item in analytics.by_status],
        },
        "temporal": {
            "created_at_trend": [item.to_dict() for item in analytics.created_at_trend],
            "trend_contract": dict(TREND_CONTRACT),
        },
        "failures": [item.to_dict() for item in analytics.failures],
    }


def _expected_http(path: Path, query: ExecutionQuery) -> dict[str, Any]:
    analytics, report = _direct_report(path, query)
    return {
        "contract_version": report.contract_version,
        "generated_at": report.generated_at,
        "tenant_id": report.tenant_id,
        "tenant_context": report.tenant_context.to_dict(),
        "applied_filters": report.applied_filters.to_dict(),
        "pagination": report.pagination.to_dict(),
        **_w104_api_payload(analytics),
    }


def _records(path: Path) -> tuple[dict[str, Any], ...]:
    store = DurableExecutionStore(path)
    try:
        return tuple(record.to_dict() for record in store.list())
    finally:
        store.close()


def _hash(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _headers() -> dict[str, str]:
    return {"Authorization": f"Bearer {TOKEN}"}


def _get(
    client: httpx.Client,
    path: str,
    *,
    authenticated: bool = True,
    params: dict[str, str] | None = None,
) -> httpx.Response:
    return client.get(
        path,
        params=params,
        headers=_headers() if authenticated else None,
    )


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _environment(path: Path, workdir: Path, tenant: str) -> dict[str, str]:
    env = os.environ.copy()
    env["PYTHONPATH"] = str(SRC) + os.pathsep + env.get("PYTHONPATH", "")
    env.update(
        {
            "PYTHONDONTWRITEBYTECODE": "1",
            "TRENDX_API_TOKEN": TOKEN,
            "TRENDX_EXECUTION_HISTORY_PATH": str(path),
            "TRENDX_DEFAULT_TENANT_ID": tenant,
            "TRENDX_DISK_MIN_FREE_GB": "0",
            "TRENDX_LOG_LEVEL": "INFO",
            "TB_BASE_URL": "http://127.0.0.1:1",
            "TRENDX_DB_HOST": "127.0.0.1",
            "TRENDX_DB_PORT": "1",
            "MLFLOW_TRACKING_URI": f"file://{workdir / 'mlruns'}",
            **TEST_FLAGS,
        }
    )
    return env


def _start_server(path: Path, workdir: Path, label: str, tenant: str = "tenant-A") -> LiveServer:
    port = _free_port()
    log_path = workdir / f"{label}.log"
    log_handle = log_path.open("w+", encoding="utf-8")
    process = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "uvicorn",
            "trendx.main:app",
            "--host",
            "127.0.0.1",
            "--port",
            str(port),
            "--log-level",
            "warning",
        ],
        cwd=ROOT,
        env=_environment(path, workdir, tenant),
        text=True,
        stdout=log_handle,
        stderr=subprocess.STDOUT,
    )
    server = LiveServer(process, f"http://127.0.0.1:{port}", log_path, log_handle)
    deadline = time.monotonic() + 30
    try:
        with httpx.Client(trust_env=False, timeout=2.0) as client:
            while time.monotonic() < deadline:
                if process.poll() is not None:
                    log_handle.flush()
                    raise RuntimeError(log_path.read_text(encoding="utf-8")[-2000:])
                try:
                    if client.get(f"{server.url}/health").status_code == 200:
                        return server
                except httpx.HTTPError:
                    pass
                time.sleep(0.1)
        raise TimeoutError("W106 Uvicorn startup timed out")
    except BaseException:
        _stop_server(server)
        raise


def _stop_server(server: LiveServer) -> str:
    if server.process.poll() is None:
        server.process.terminate()
    try:
        server.process.wait(timeout=15)
    except subprocess.TimeoutExpired:
        server.process.kill()
        server.process.wait(timeout=15)
    server.log_handle.flush()
    server.log_handle.close()
    return server.log_path.read_text(encoding="utf-8")


def _query_for_filter(name: str, value: str) -> ExecutionQuery:
    values: dict[str, Any] = {name: value}
    values["tenant_id"] = value if name == "tenant_id" else "tenant-A"
    return ExecutionQuery(**values)


def _filter_cases() -> dict[str, str]:
    return {
        "tenant_id": "tenant-A",
        "entity_type": "ASSET",
        "entity_id": "device-b",
        "target_metric": "energy_consumption",
        "execution_id": "a-success",
        "reference_key": "w106:a-success",
        "model_id": "model-b",
        "model_version": "1",
        "algorithm": "PROPHET",
        "feature_schema_version": "schema-b",
        "feature_schema_fingerprint": "fingerprint-b",
        "status": "FAILED",
        "created_at_from": "2026-01-01T00:10:00Z",
        "created_at_to": "2026-01-01T01:20:00Z",
        "completed_at_from": "2099-01-01T00:00:00Z",
        "completed_at_to": "2099-01-01T00:00:10Z",
    }


class _CachedService:
    def __init__(self, expected_query: ExecutionQuery, page: ExecutionHistoryPage) -> None:
        self.expected_query = expected_query
        self.page = page

    def query(self, requested: ExecutionQuery) -> ExecutionHistoryPage:
        assert requested == self.expected_query
        return self.page


def _partitioned(body: dict[str, Any]) -> int:
    total = body["summary"]["total"]
    assert sum(item["total"] for item in body["dimensions"]["by_status"]) == total
    assert sum(item["total"] for item in body["dimensions"]["by_metric"]) == total
    assert sum(item["total"] for item in body["dimensions"]["by_entity"]) == total
    assert sum(item["total"] for item in body["dimensions"]["by_model"]) == total
    assert sum(item["total"] for item in body["dimensions"]["by_algorithm"]) == total
    assert sum(item["total"] for item in body["temporal"]["created_at_trend"]) == total
    return total


def test_w106_openapi(tmp_path: Path) -> None:
    path = tmp_path / "history.json"
    artifacts = tmp_path / "artifacts"
    _seed(path, _base_specs(), artifacts)
    before_hash = _hash(path)
    before_records = _records(path)
    server = _start_server(path, tmp_path, "openapi")
    logs = ""
    try:
        with httpx.Client(base_url=server.url, trust_env=False, timeout=10.0) as client:
            assert _get(client, "/health", authenticated=False).status_code == 200
            openapi_response = _get(client, "/openapi.json", authenticated=False)
            assert openapi_response.status_code == 200
            assert (
                _get(client, "/api/v1/forecast/executions/report", authenticated=False).status_code
                == 401
            )
            response = _get(client, "/api/v1/forecast/executions/report")
            assert response.status_code == 200
            body = response.json()
            assert body == _expected_http(path, ExecutionQuery(tenant_id="tenant-A"))
            assert body["contract_version"] == "1"
            assert body["generated_at"] is None
            assert body["tenant_context"] == {"tenant_id": "tenant-A"}
            assert body["pagination"] == {"applied": False, "reason": "aggregate_report"}
            assert body["applied_filters"]["tenant_id"] == "tenant-A"
            assert _partitioned(body) == 8
            assert {item["error_code"] for item in body["failures"]} == {
                "feature_schema_mismatch",
                "no-compatible-champion",
                "artifact-missing",
                "payload_invalid",
                "artifact-corrupted",
            }
            assert body["temporal"]["trend_contract"] == {
                "dimension": "created_at",
                "bucket": "exact_timestamp",
                "timezone": "UTC",
                "inclusivity": "inclusive",
                "ordering": "ascending",
            }

            operation = openapi_response.json()["paths"]["/api/v1/forecast/executions/report"][
                "get"
            ]
            query_parameters = {
                parameter["name"]
                for parameter in operation["parameters"]
                if parameter["in"] == "query"
            }
            assert query_parameters == set(_filter_cases())
            assert "limit" not in query_parameters
            assert "offset" not in query_parameters
            assert "requestBody" not in operation
            assert {"200", "401", "403", "422", "503"} <= set(operation["responses"])
            assert "ExecutionAnalyticsReportOut" in openapi_response.json()["components"]["schemas"]
            assert "ExecutionErrorOut" in openapi_response.json()["components"]["schemas"]

            for name, value in _filter_cases().items():
                filtered = _get(
                    client,
                    "/api/v1/forecast/executions/report",
                    params={name: value},
                )
                assert filtered.status_code == 200
                assert filtered.json() == _expected_http(path, _query_for_filter(name, value))

            combined = _get(
                client,
                "/api/v1/forecast/executions/report",
                params={
                    "entity_type": "ASSET",
                    "target_metric": "energy_consumption",
                    "status": "FAILED",
                },
            )
            assert combined.status_code == 200
            assert combined.json()["summary"]["failed_count"] == 2
            assert combined.json()["applied_filters"]["entity_type"] == "ASSET"

            exact = _get(
                client,
                "/api/v1/forecast/executions/report",
                params={
                    "created_at_from": "2026-01-01T00:10:00Z",
                    "created_at_to": "2026-01-01T00:10:00Z",
                },
            )
            assert exact.status_code == 200
            assert exact.json()["summary"]["total"] == 1
    finally:
        logs = _stop_server(server)

    assert _hash(path) == before_hash
    assert _records(path) == before_records
    assert not artifacts.exists()
    assert "execution_history operation=report outcome=success status_code=200" in logs
    sanitized = logs.casefold()
    for forbidden in (
        TOKEN.casefold(),
        str(path).casefold(),
        "authorization",
        "api_key",
        "traceback",
    ):
        assert forbidden not in sanitized


def test_w106_process_a_b(tmp_path: Path) -> None:
    path = tmp_path / "history.json"
    artifacts = tmp_path / "artifacts"
    _seed(path, _base_specs(), artifacts)
    before_hash = _hash(path)
    first_server = _start_server(path, tmp_path, "process-a")
    try:
        with httpx.Client(base_url=first_server.url, trust_env=False, timeout=10.0) as client:
            first = _get(client, "/api/v1/forecast/executions/report")
    finally:
        _stop_server(first_server)
    second_server = _start_server(path, tmp_path, "process-b")
    try:
        with httpx.Client(base_url=second_server.url, trust_env=False, timeout=10.0) as client:
            second = _get(client, "/api/v1/forecast/executions/report")
    finally:
        _stop_server(second_server)
    assert first.status_code == second.status_code == 200
    assert first.text == second.text
    assert first.json()["contract_version"] == "1"
    assert first.json()["generated_at"] is None
    assert _hash(path) == before_hash
    assert not artifacts.exists()


def test_w106_concurrent_reads(tmp_path: Path) -> None:
    path = tmp_path / "history.json"
    artifacts = tmp_path / "artifacts"
    initial_count = 120
    added_count = 12
    _seed(path, _large_specs(initial_count, "initial"), artifacts)
    server = _start_server(path, tmp_path, "concurrency")
    marker = tmp_path / "writer-start"
    writer_code = r"""
import sys
import time
from pathlib import Path
from trendx.forecasting.execution import DurableExecutionStore, ExecutionProvenance, ExecutionRecord

path = Path(sys.argv[1])
marker = Path(sys.argv[2])
start = int(sys.argv[3])
count = int(sys.argv[4])
while not marker.exists():
    time.sleep(0.01)
store = DurableExecutionStore(path)
for index in range(count):
    execution_id = f"added-{start + index}"
    record = ExecutionRecord(
        execution_id=execution_id,
        reference_key=f"w106:{execution_id}",
        tenant_id="tenant-A",
        entity_type="DEVICE",
        entity_id=f"added-entity-{index % 3}",
        target_metric=f"added-metric-{index % 2}",
        frequency="1h",
        horizon=24,
        model_id="added-model",
        model_version="1",
        algorithm="Fourier",
        feature_schema_version="added-schema",
        feature_schema_fingerprint="added-fingerprint",
        artifact_uri=f"file:///w106/{execution_id}.bin",
        created_at=f"2026-02-01T00:00:{index:02d}+00:00",
    )
    store.create(record)
    store.mark_started(execution_id)
    store.mark_success(
        execution_id,
        ExecutionProvenance(
            model_id="added-model",
            model_version="1",
            algorithm="Fourier",
            feature_schema_version="added-schema",
            feature_schema_fingerprint="added-fingerprint",
            artifact_uri=record.artifact_uri,
            prediction_count=1,
        ),
        completed_at="2099-01-01T00:00:00Z",
    )
store.close()
"""
    writer = subprocess.Popen(
        [sys.executable, "-c", writer_code, str(path), str(marker), "0", str(added_count)],
        cwd=ROOT,
        env=_environment(path, tmp_path, "tenant-A"),
        text=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
    )
    observed: list[dict[str, Any]] = []
    try:
        with httpx.Client(base_url=server.url, trust_env=False, timeout=15.0) as client:
            with ThreadPoolExecutor(max_workers=8) as executor:
                futures = [
                    executor.submit(
                        _get,
                        client,
                        "/api/v1/forecast/executions/report",
                    )
                    for _ in range(16)
                ]
                time.sleep(0.05)
                marker.write_text("start", encoding="utf-8")
                stderr = writer.communicate(timeout=45)[1]
                assert writer.returncode == 0, stderr
                for future in futures:
                    result = future.result(timeout=20)
                    assert result.status_code == 200
                    observed.append(result.json())
        with httpx.Client(base_url=server.url, trust_env=False, timeout=10.0) as client:
            final_first = _get(client, "/api/v1/forecast/executions/report")
            final_second = _get(client, "/api/v1/forecast/executions/report")
    finally:
        if writer.poll() is None:
            writer.kill()
            writer.wait(timeout=10)
        _stop_server(server)

    for body in observed:
        assert body["tenant_id"] == "tenant-A"
        total = _partitioned(body)
        assert initial_count <= total <= initial_count + added_count
        assert body["failures"] == []
    assert final_first.status_code == final_second.status_code == 200
    assert final_first.text == final_second.text
    expected = _expected_http(path, ExecutionQuery(tenant_id="tenant-A"))
    assert final_first.json() == expected
    assert expected["summary"]["total"] == initial_count + added_count
    assert not artifacts.exists()


def test_w106_error_sanitization(tmp_path: Path) -> None:
    seeded = tmp_path / "seeded.json"
    artifacts = tmp_path / "artifacts"
    _seed(seeded, _base_specs(), artifacts)
    empty = tmp_path / "empty.json"
    DurableExecutionStore(empty).close()
    missing = tmp_path / "missing.json"
    corrupt = tmp_path / "corrupt.json"
    corrupt.write_text("{broken", encoding="utf-8")
    logs: list[str] = []

    empty_server = _start_server(empty, tmp_path, "empty")
    try:
        with httpx.Client(base_url=empty_server.url, trust_env=False, timeout=10.0) as client:
            response = _get(client, "/api/v1/forecast/executions/report")
            assert response.status_code == 200
            body = response.json()
            assert body["summary"] == {
                "total": 0,
                "success_count": 0,
                "failed_count": 0,
                "success_rate": 0.0,
                "total_prediction_count": 0,
                "duration_sample_count": 0,
                "total_duration": None,
                "average_duration": None,
            }
            assert body["dimensions"] == {
                "by_metric": [],
                "by_entity": [],
                "by_algorithm": [],
                "by_model": [],
                "by_status": [],
            }
            assert body["failures"] == []
            assert body["temporal"]["created_at_trend"] == []
            invalid = _get(
                client,
                "/api/v1/forecast/executions/report",
                params={"created_at_from": "not-a-date"},
            )
            assert invalid.status_code == 422
            assert invalid.json() == {"detail": "invalid_query"}
            forced = _get(
                client,
                "/api/v1/forecast/executions/report",
                params={"tenant_id": "tenant-B"},
            )
            assert forced.status_code == 403
            assert forced.json() == {"detail": "tenant_forbidden"}
    finally:
        logs.append(_stop_server(empty_server))

    tenant_b_server = _start_server(seeded, tmp_path, "tenant-b", tenant="tenant-B")
    try:
        with httpx.Client(base_url=tenant_b_server.url, trust_env=False, timeout=10.0) as client:
            response = _get(client, "/api/v1/forecast/executions/report")
            assert response.status_code == 200
            assert response.json() == _expected_http(seeded, ExecutionQuery(tenant_id="tenant-B"))
            assert response.json()["tenant_id"] == "tenant-B"
            assert response.json()["summary"]["total"] == 1
    finally:
        logs.append(_stop_server(tenant_b_server))

    for label, path, tenant, status, detail in (
        ("missing", missing, "tenant-A", 503, "execution_history_unavailable"),
        ("corrupt", corrupt, "tenant-A", 503, "execution_history_unavailable"),
        ("no-tenant", seeded, "", 403, "tenant_context_unavailable"),
    ):
        server = _start_server(path, tmp_path, label, tenant=tenant)
        try:
            with httpx.Client(base_url=server.url, trust_env=False, timeout=10.0) as client:
                assert _get(client, "/health", authenticated=False).status_code == 200
                response = _get(client, "/api/v1/forecast/executions/report")
                assert response.status_code == status
                assert response.json() == {"detail": detail}
                assert str(path).casefold() not in response.text.casefold()
        finally:
            logs.append(_stop_server(server))
    assert not missing.exists()
    joined = "\n".join(logs)
    for event in (
        "execution_history operation=report outcome=success status_code=200",
        "execution_history operation=report outcome=invalid_request status_code=422",
        "execution_history operation=report outcome=tenant_rejected status_code=403",
        "execution_history operation=tenant outcome=tenant_rejected status_code=403",
        "execution_history operation=service outcome=datastore_unavailable status_code=503",
    ):
        assert event in joined
    sanitized = joined.casefold()
    for forbidden in (
        TOKEN.casefold(),
        str(seeded).casefold(),
        str(missing).casefold(),
        "authorization",
        "api_key",
        "traceback",
        "broken",
    ):
        assert forbidden not in sanitized


def test_w106_performance_and_read_only(tmp_path: Path) -> None:
    measurements: list[tuple[int, float, float, int]] = []
    for count in (10, 100, 500):
        path = tmp_path / f"volume-{count}.json"
        artifacts = tmp_path / f"artifacts-{count}"
        _seed(path, _large_specs(count, f"volume-{count}"), artifacts)
        store = DurableExecutionStore(path)
        service = ExecutionHistoryService(store)
        query = ExecutionQuery(tenant_id="tenant-A")
        read_started = time.perf_counter()
        page = service.query(query)
        read_elapsed = time.perf_counter() - read_started

        cached = _CachedService(query, page)
        tracemalloc.start()
        build_started = time.perf_counter()
        report = ExecutionAnalyticsReportingService(cached).report(query)  # type: ignore[arg-type]
        build_elapsed = time.perf_counter() - build_started
        _, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        store.close()
        assert report.summary.total_executions == count
        assert math.isfinite(read_elapsed)
        assert math.isfinite(build_elapsed)
        assert peak > 0
        measurements.append((count, read_elapsed, build_elapsed, peak))
        assert not artifacts.exists()

    api_path = tmp_path / "api-volume.json"
    api_artifacts = tmp_path / "api-artifacts"
    _seed(api_path, _large_specs(100, "api-volume"), api_artifacts)
    before_hash = _hash(api_path)
    before_records = _records(api_path)
    server = _start_server(api_path, tmp_path, "volume-api")
    try:
        with httpx.Client(base_url=server.url, trust_env=False, timeout=15.0) as client:
            started = time.perf_counter()
            response = _get(client, "/api/v1/forecast/executions/report")
            http_elapsed = time.perf_counter() - started
            assert response.status_code == 200
            assert response.json()["summary"]["total"] == 100
    finally:
        _stop_server(server)
    assert math.isfinite(http_elapsed)
    assert _hash(api_path) == before_hash
    assert _records(api_path) == before_records
    assert not api_artifacts.exists()
    mlruns = tmp_path / "mlruns"
    assert not mlruns.exists() or not any(path.is_file() for path in mlruns.rglob("*"))
