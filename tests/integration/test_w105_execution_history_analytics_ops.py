"""W105 — operational validation of Execution History Analytics.

The tests use temporary W100 datastores and real Uvicorn processes.  The
application under test is never replaced by TestClient, and every process is
started with the business side-effect flags disabled.
"""

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
from trendx.forecasting.analytics import analyze_execution_history
from trendx.forecasting.execution import (
    DurableExecutionStore,
    ExecutionProvenance,
    ExecutionRecord,
)
from trendx.forecasting.history import ExecutionHistoryService, ExecutionQuery

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src"
TOKEN = "w105-test-token"
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

# These tests are intentionally self-contained: they do not require the
# external catalog database used by the repository's traditional integration
# tests.  They remain unselected by ``-m unit`` and are collected by the full
# suite, while the real-process checks can run locally without a database.


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
    completed_at: str | None = None


@dataclass
class LiveServer:
    process: subprocess.Popen[str]
    url: str
    log_path: Path
    log_handle: IO[str]


def _artifact_uri(artifact_root: Path, execution_id: str) -> str:
    return (artifact_root / f"{execution_id}.bin").as_uri()


def _record(spec: RecordSpec, artifact_root: Path) -> ExecutionRecord:
    return ExecutionRecord(
        execution_id=spec.execution_id,
        reference_key=f"w105:{spec.execution_id}",
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
        artifact_uri=_artifact_uri(artifact_root, spec.execution_id),
        created_at=spec.created_at,
        metadata={"source": "w105"},
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
        metadata={"source": "w105"},
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
            completed_at = (
                spec.completed_at or (datetime.now(UTC) + timedelta(seconds=30)).isoformat()
            )
            if spec.status == "FAILED":
                if not spec.error_code:
                    msg = "W105 FAILED fixture requires an error code"
                    raise ValueError(msg)
                store.mark_failed(
                    record.execution_id,
                    error_code=spec.error_code,
                    error_reason="W105 controlled failure",
                    provenance=_provenance(record, 0),
                    completed_at=completed_at,
                )
            elif spec.status == "SUCCESS":
                store.mark_success(
                    record.execution_id,
                    _provenance(record, spec.predictions),
                    completed_at=completed_at,
                )
            else:
                msg = f"Unsupported W105 fixture status: {spec.status}"
                raise ValueError(msg)
    finally:
        store.close()


def _base_specs() -> tuple[RecordSpec, ...]:
    return (
        RecordSpec(
            "a-temp",
            "tenant-A",
            "DEVICE",
            "device-a",
            "temperature",
            "model-a",
            "1",
            "Fourier",
            "S1",
            "fp-a",
            "2026-01-01T00:10:00Z",
            predictions=3,
            completed_at="2099-01-01T00:00:10Z",
        ),
        RecordSpec(
            "a-humidity",
            "tenant-A",
            "DEVICE",
            "device-a",
            "humidity",
            "model-a",
            "2",
            "Prophet",
            "S1",
            "fp-a",
            "2026-01-01T00:20:00+00:00",
            predictions=2,
            completed_at="2099-01-01T00:00:20Z",
        ),
        RecordSpec(
            "a-energy-fail-1",
            "tenant-A",
            "ASSET",
            "asset-a",
            "energy_consumption",
            "model-b",
            "1",
            "ARIMA",
            "S2",
            "fp-b",
            "2026-01-01T00:30:00Z",
            status="FAILED",
            error_code="no-compatible-champion",
            completed_at="2099-01-01T00:00:30Z",
        ),
        RecordSpec(
            "a-energy-fail-2",
            "tenant-A",
            "ASSET",
            "asset-a",
            "energy_consumption",
            "model-b",
            "1",
            "ARIMA",
            "S2",
            "fp-b",
            "2026-01-01T00:40:00Z",
            status="FAILED",
            error_code="no-compatible-champion",
            completed_at="2099-01-01T00:00:30Z",
        ),
        RecordSpec(
            "a-schema-fail",
            "tenant-A",
            "DEVICE",
            "device-b",
            "pressure",
            "model-c",
            "3",
            "Fourier",
            "S3",
            "fp-c",
            "2026-01-01T00:50:00Z",
            status="FAILED",
            error_code="feature_schema_mismatch",
            completed_at="2099-01-01T00:00:30Z",
        ),
        RecordSpec(
            "a-artifact-missing",
            "tenant-A",
            "DEVICE",
            "device-b",
            "solar_power",
            "model-d",
            "1",
            "Prophet",
            "S1",
            "fp-a",
            "2026-01-01T01:00:00Z",
            status="FAILED",
            error_code="artifact-missing",
            completed_at="2099-01-01T00:00:30Z",
        ),
        RecordSpec(
            "a-artifact-corrupt",
            "tenant-A",
            "DEVICE",
            "device-b",
            "wind_speed",
            "model-d",
            "1",
            "Prophet",
            "S1",
            "fp-a",
            "2026-01-01T01:05:00Z",
            status="FAILED",
            error_code="artifact-corrupted",
            completed_at="2099-01-01T00:00:30Z",
        ),
        RecordSpec(
            "a-payload-fail",
            "tenant-A",
            "DEVICE",
            "device-c",
            "flow_rate",
            "model-e",
            "1",
            "Fourier",
            "S2",
            "fp-b",
            "2026-01-01T01:10:00Z",
            status="FAILED",
            error_code="payload_invalid",
            completed_at="2099-01-01T00:00:30Z",
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
            "S1",
            "fp-a",
            "2026-01-01T01:20:00Z",
            status="PENDING",
        ),
        RecordSpec(
            "a-running",
            "tenant-A",
            "DEVICE",
            "device-d",
            "humidity",
            "model-b",
            "2",
            "Prophet",
            "S1",
            "fp-a",
            "2026-01-01T01:30:00Z",
            status="RUNNING",
        ),
        RecordSpec(
            "b-temperature",
            "tenant-B",
            "DEVICE",
            "device-b",
            "temperature",
            "model-f",
            "4",
            "ARIMA",
            "S4",
            "fp-d",
            "2026-01-01T01:40:00Z",
            predictions=4,
            completed_at="2099-01-01T00:00:40Z",
        ),
        RecordSpec(
            "b-failure",
            "tenant-B",
            "ASSET",
            "asset-b",
            "energy_consumption",
            "model-g",
            "1",
            "Fourier",
            "S4",
            "fp-d",
            "2026-01-01T01:50:00Z",
            status="FAILED",
            error_code="artifact-corrupted",
            completed_at="2099-01-01T00:00:50Z",
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
            f"S{index % 3}",
            f"fp-{index % 4}",
            (BASE_TIME + timedelta(minutes=2000 + index)).isoformat(),
            predictions=index % 6,
            completed_at="2099-01-01T00:00:00Z",
        )
        for index in range(count)
    )


def _direct_projection(path: Path, query: ExecutionQuery) -> dict[str, Any]:
    store = DurableExecutionStore(path)
    try:
        return analyze_execution_history(ExecutionHistoryService(store), query).to_dict()
    finally:
        store.close()


def _api_projection(direct: dict[str, Any]) -> dict[str, Any]:
    summary = direct["summary"]
    return {
        "tenant_id": direct["tenant_id"],
        "summary": {
            "total": summary["total_executions"],
            "success_count": summary["success_count"],
            "failed_count": summary["failure_count"],
            "success_rate": summary["success_rate"],
            "total_prediction_count": summary["prediction_count_total"],
            "duration_sample_count": summary["duration_sample_count"],
            "total_duration": summary["total_duration_seconds"],
            "average_duration": summary["average_duration_seconds"],
        },
        "by_metric": direct["by_metric"],
        "by_entity": direct["by_entity"],
        "by_algorithm": direct["by_algorithm"],
        "by_model": direct["by_model"],
        "by_status": direct["by_status"],
        "failures": direct["failures"],
        "created_at_trend": direct["created_at_trend"],
        "trend_contract": direct["trend_contract"],
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
    server = LiveServer(
        process=process,
        url=f"http://127.0.0.1:{port}",
        log_path=log_path,
        log_handle=log_handle,
    )
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
        raise TimeoutError("W105 Uvicorn startup timed out")
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
        "entity_id": "asset-a",
        "target_metric": "energy_consumption",
        "execution_id": "a-temp",
        "reference_key": "w105:a-temp",
        "model_id": "model-a",
        "model_version": "1",
        "algorithm": "FOURIER",
        "feature_schema_version": "S1",
        "feature_schema_fingerprint": "fp-a",
        "status": "SUCCESS",
        "created_at_from": "2026-01-01T00:10:00Z",
        "created_at_to": "2026-01-01T01:30:00Z",
        "completed_at_from": "2099-01-01T00:00:00Z",
        "completed_at_to": "2099-01-01T23:59:59Z",
    }


def _assert_partitioned(body: dict[str, Any]) -> int:
    total = body["summary"]["total"]
    assert sum(item["total"] for item in body["by_status"]) == total
    assert sum(item["total"] for item in body["by_metric"]) == total
    assert sum(item["total"] for item in body["created_at_trend"]) == total
    return total


def test_w105_real_http_contract_dimensions_filters_and_read_only(tmp_path: Path) -> None:
    path = tmp_path / "history.json"
    artifact_root = tmp_path / "artifacts"
    _seed(path, _base_specs(), artifact_root)
    before_hash = _hash(path)
    before_records = _records(path)
    server = _start_server(path, tmp_path, "real-http")
    logs = ""
    try:
        with httpx.Client(base_url=server.url, trust_env=False, timeout=10.0) as client:
            assert _get(client, "/health", authenticated=False).status_code == 200
            openapi_response = _get(client, "/openapi.json", authenticated=False)
            assert openapi_response.status_code == 200
            assert (
                _get(
                    client, "/api/v1/forecast/executions/analytics", authenticated=False
                ).status_code
                == 401
            )

            response = _get(client, "/api/v1/forecast/executions/analytics")
            assert response.status_code == 200
            direct = _direct_projection(path, ExecutionQuery(tenant_id="tenant-A"))
            body = response.json()
            assert body == _api_projection(direct)
            assert {item["key"] for item in body["by_metric"]} == {
                "temperature",
                "humidity",
                "energy_consumption",
                "pressure",
                "solar_power",
                "wind_speed",
                "flow_rate",
            }
            assert {item["key"] for item in body["by_algorithm"]} == {"Fourier", "Prophet", "ARIMA"}
            assert {(item["model_id"], item["model_version"]) for item in body["by_model"]} == {
                ("model-a", "1"),
                ("model-a", "2"),
                ("model-b", "1"),
                ("model-b", "2"),
                ("model-c", "3"),
                ("model-d", "1"),
                ("model-e", "1"),
            }
            assert {item["key"] for item in body["by_status"]} == {
                "SUCCESS",
                "FAILED",
                "PENDING",
                "RUNNING",
            }
            assert {(item["entity_type"], item["entity_id"]) for item in body["by_entity"]} == {
                ("DEVICE", "device-a"),
                ("ASSET", "asset-a"),
                ("DEVICE", "device-b"),
                ("DEVICE", "device-c"),
                ("DEVICE", "device-d"),
            }
            assert {item["error_code"]: item["count"] for item in body["failures"]} == {
                "no-compatible-champion": 2,
                "feature_schema_mismatch": 1,
                "artifact-missing": 1,
                "artifact-corrupted": 1,
                "payload_invalid": 1,
            }
            assert body["trend_contract"] == {
                "dimension": "created_at",
                "bucket": "exact_timestamp",
                "timezone": "UTC",
                "inclusivity": "inclusive",
                "ordering": "ascending",
            }
            assert body["created_at_trend"][0]["bucket"] == "2026-01-01T00:10:00+00:00"
            _assert_partitioned(body)

            operation = openapi_response.json()["paths"]["/api/v1/forecast/executions/analytics"][
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
            assert {"200", "401", "403", "404", "422", "503"} <= set(operation["responses"])
            assert operation["responses"]["200"]["content"]["application/json"]["schema"]
            for status in ("401", "403", "404", "422", "503"):
                assert operation["responses"][status]["content"]["application/json"]["schema"]
            assert "ExecutionAnalyticsOut" in openapi_response.json()["components"]["schemas"]
            assert "ExecutionErrorOut" in openapi_response.json()["components"]["schemas"]

            for name, value in _filter_cases().items():
                filtered = _get(
                    client,
                    "/api/v1/forecast/executions/analytics",
                    params={name: value},
                )
                assert filtered.status_code == 200
                expected = _direct_projection(path, _query_for_filter(name, value))
                assert filtered.json() == _api_projection(expected)

            exact_created = _get(
                client,
                "/api/v1/forecast/executions/analytics",
                params={
                    "created_at_from": "2026-01-01T00:10:00Z",
                    "created_at_to": "2026-01-01T00:10:00Z",
                },
            )
            assert exact_created.status_code == 200
            assert exact_created.json()["summary"]["total"] == 1
            exact_completed = _get(
                client,
                "/api/v1/forecast/executions/analytics",
                params={
                    "completed_at_from": "2099-01-01T00:00:10Z",
                    "completed_at_to": "2099-01-01T00:00:10Z",
                },
            )
            assert exact_completed.status_code == 200
            assert exact_completed.json()["summary"]["total"] == 1

            combined = _get(
                client,
                "/api/v1/forecast/executions/analytics",
                params={
                    "entity_type": "ASSET",
                    "target_metric": "energy_consumption",
                    "status": "FAILED",
                },
            )
            assert combined.status_code == 200
            combined_expected = _direct_projection(
                path,
                ExecutionQuery(
                    tenant_id="tenant-A",
                    entity_type="ASSET",
                    target_metric="energy_consumption",
                    status="FAILED",
                ),
            )
            assert combined.json() == _api_projection(combined_expected)

            assert _get(client, "/api/v1/forecast/executions/statistics").status_code == 200
            assert (
                _get(
                    client,
                    "/api/v1/forecast/executions/a-energy-fail-1/diagnostic",
                ).status_code
                == 200
            )
    finally:
        logs = _stop_server(server)

    assert _hash(path) == before_hash
    assert _records(path) == before_records
    assert not artifact_root.exists()
    assert "execution_history operation=analytics outcome=success status_code=200" in logs
    sanitized = logs.casefold()
    for forbidden in (
        TOKEN.casefold(),
        str(path).casefold(),
        "authorization",
        "api_key",
        "traceback",
    ):
        assert forbidden not in sanitized


def test_w105_process_a_b_real_http_is_exact(tmp_path: Path) -> None:
    path = tmp_path / "history.json"
    artifact_root = tmp_path / "artifacts"
    _seed(path, _base_specs(), artifact_root)
    before_hash = _hash(path)

    first_server = _start_server(path, tmp_path, "process-a")
    try:
        with httpx.Client(base_url=first_server.url, trust_env=False, timeout=10.0) as client:
            first = _get(client, "/api/v1/forecast/executions/analytics")
    finally:
        _stop_server(first_server)
    assert first.status_code == 200

    second_server = _start_server(path, tmp_path, "process-b")
    try:
        with httpx.Client(base_url=second_server.url, trust_env=False, timeout=10.0) as client:
            second = _get(client, "/api/v1/forecast/executions/analytics")
    finally:
        _stop_server(second_server)
    assert second.status_code == 200
    assert first.text == second.text
    assert first.json() == _api_projection(
        _direct_projection(path, ExecutionQuery(tenant_id="tenant-A"))
    )
    assert _hash(path) == before_hash
    assert not artifact_root.exists()


def test_w105_concurrent_reads_are_coherent_snapshots(tmp_path: Path) -> None:
    path = tmp_path / "history.json"
    artifact_root = tmp_path / "artifacts"
    initial_count = 120
    added_count = 12
    _seed(path, _large_specs(initial_count, "initial"), artifact_root)
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
        reference_key=f"w105:{execution_id}",
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
        artifact_uri=f"file:///w105/{execution_id}.bin",
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
                        "/api/v1/forecast/executions/analytics",
                    )
                    for _ in range(16)
                ]
                time.sleep(0.05)
                marker.write_text("start", encoding="utf-8")
                stderr = writer.communicate(timeout=45)[1]
                assert writer.returncode == 0, stderr
                for future in futures:
                    response = future.result(timeout=20)
                    assert response.status_code == 200
                    observed.append(response.json())
        with httpx.Client(base_url=server.url, trust_env=False, timeout=10.0) as client:
            final_first = _get(client, "/api/v1/forecast/executions/analytics")
            final_second = _get(client, "/api/v1/forecast/executions/analytics")
    finally:
        if writer.poll() is None:
            writer.kill()
            writer.wait(timeout=10)
        _stop_server(server)

    for body in observed:
        total = _assert_partitioned(body)
        assert initial_count <= total <= initial_count + added_count
    assert final_first.status_code == 200
    assert final_second.status_code == 200
    assert final_first.text == final_second.text
    expected = _direct_projection(path, ExecutionQuery(tenant_id="tenant-A"))
    assert final_first.json() == _api_projection(expected)
    assert expected["summary"]["total_executions"] == initial_count + added_count
    assert not artifact_root.exists()


def test_w105_empty_errors_tenant_isolation_and_observability(tmp_path: Path) -> None:
    seeded = tmp_path / "seeded.json"
    artifact_root = tmp_path / "artifacts"
    _seed(seeded, _base_specs(), artifact_root)
    empty = tmp_path / "empty.json"
    DurableExecutionStore(empty).close()
    missing = tmp_path / "missing.json"
    corrupt = tmp_path / "corrupt.json"
    corrupt.write_text("{broken", encoding="utf-8")
    logs: list[str] = []

    empty_server = _start_server(empty, tmp_path, "empty")
    try:
        with httpx.Client(base_url=empty_server.url, trust_env=False, timeout=10.0) as client:
            response = _get(client, "/api/v1/forecast/executions/analytics")
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
            assert all(
                not body[name]
                for name in (
                    "by_metric",
                    "by_entity",
                    "by_algorithm",
                    "by_model",
                    "by_status",
                    "failures",
                    "created_at_trend",
                )
            )
            invalid = _get(
                client,
                "/api/v1/forecast/executions/analytics",
                params={"created_at_from": "not-a-date"},
            )
            assert invalid.status_code == 422
            assert invalid.json() == {"detail": "invalid_query"}
            forced = _get(
                client,
                "/api/v1/forecast/executions/analytics",
                params={"tenant_id": "tenant-B"},
            )
            assert forced.status_code == 403
            assert forced.json() == {"detail": "tenant_forbidden"}
    finally:
        logs.append(_stop_server(empty_server))

    tenant_b_server = _start_server(seeded, tmp_path, "tenant-b", tenant="tenant-B")
    try:
        with httpx.Client(base_url=tenant_b_server.url, trust_env=False, timeout=10.0) as client:
            response = _get(client, "/api/v1/forecast/executions/analytics")
            assert response.status_code == 200
            assert response.json() == _api_projection(
                _direct_projection(seeded, ExecutionQuery(tenant_id="tenant-B"))
            )
    finally:
        logs.append(_stop_server(tenant_b_server))

    for label, path, tenant, expected_detail in (
        ("missing", missing, "tenant-A", "execution_history_unavailable"),
        ("corrupt", corrupt, "tenant-A", "execution_history_unavailable"),
        ("no-tenant", seeded, "", "tenant_context_unavailable"),
    ):
        server = _start_server(path, tmp_path, label, tenant=tenant)
        try:
            with httpx.Client(base_url=server.url, trust_env=False, timeout=10.0) as client:
                assert _get(client, "/health", authenticated=False).status_code == 200
                response = _get(client, "/api/v1/forecast/executions/analytics")
                expected_status = 503 if expected_detail == "execution_history_unavailable" else 403
                assert response.status_code == expected_status
                assert response.json() == {"detail": expected_detail}
                assert str(path).casefold() not in response.text.casefold()
        finally:
            logs.append(_stop_server(server))
    assert not missing.exists()
    assert "broken" not in "\n".join(logs).casefold()

    joined = "\n".join(logs)
    for event in (
        "execution_history operation=analytics outcome=success status_code=200",
        "execution_history operation=analytics_query outcome=invalid_request status_code=422",
        "execution_history operation=analytics_query outcome=tenant_rejected status_code=403",
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
    ):
        assert forbidden not in sanitized


def test_w105_volume_is_representative_and_has_no_external_side_effect(tmp_path: Path) -> None:
    measurements: list[tuple[int, float, int]] = []
    for count in (10, 100, 500):
        path = tmp_path / f"volume-{count}.json"
        artifact_root = tmp_path / f"artifacts-{count}"
        _seed(path, _large_specs(count, f"volume-{count}"), artifact_root)
        store = DurableExecutionStore(path)
        service = ExecutionHistoryService(store)
        tracemalloc.start()
        started = time.perf_counter()
        result = analyze_execution_history(service, ExecutionQuery(tenant_id="tenant-A"))
        elapsed = time.perf_counter() - started
        _, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        store.close()
        assert result.summary.total_executions == count
        assert math.isfinite(elapsed)
        assert peak > 0
        measurements.append((count, elapsed, peak))
        assert not artifact_root.exists()

    assert [count for count, _, _ in measurements] == [10, 100, 500]
    assert all(
        math.isfinite(elapsed) and math.isfinite(float(peak)) for _, elapsed, peak in measurements
    )

    api_path = tmp_path / "api-volume.json"
    api_artifact_root = tmp_path / "api-artifacts"
    _seed(api_path, _large_specs(100, "api-volume"), api_artifact_root)
    server = _start_server(api_path, tmp_path, "volume-api")
    try:
        with httpx.Client(base_url=server.url, trust_env=False, timeout=15.0) as client:
            started = time.perf_counter()
            response = _get(client, "/api/v1/forecast/executions/analytics")
            api_elapsed = time.perf_counter() - started
            assert response.status_code == 200
            assert response.json()["summary"]["total"] == 100
    finally:
        _stop_server(server)
    assert math.isfinite(api_elapsed)
    assert not api_artifact_root.exists()
    mlruns = tmp_path / "mlruns"
    assert not mlruns.exists() or not any(path.is_file() for path in mlruns.rglob("*"))


def test_w105_analytics_source_has_no_external_side_effect_path() -> None:
    source = (ROOT / "src/trendx/forecasting/analytics.py").read_text(encoding="utf-8").casefold()
    for forbidden in (
        "mlflow",
        "thingsboard",
        "registry",
        "artifactstore",
        "scheduler",
        "worker",
        ".lock",
    ):
        assert forbidden not in source
    assert "durableexecutionstore" not in source
    assert "json" not in source
    assert all(value == "false" for value in TEST_FLAGS.values())
