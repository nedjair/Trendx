"""W110 — real Uvicorn/HTTPX operational checks for the audit sidecar.

These tests are self-contained local-process checks, like the W109 operational
suite; they do not require the external catalog database.
"""

from __future__ import annotations

import hashlib
import os
import socket
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import IO, Any

import httpx
from trendx.forecasting.audit import (
    AuditActorType,
    AuditOperation,
    AuditOutcome,
    ExecutionAuditEvent,
    ExecutionAuditStore,
)
from trendx.forecasting.execution import DurableExecutionStore, ExecutionRecord, ExecutionStatus
from trendx.forecasting.lifecycle import ExecutionRetentionPolicy, FileSystemArchiveStore

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src"
TOKEN = "w110-operational-test-token"
NOW = datetime(2026, 4, 1, tzinfo=UTC)
POLICY = ExecutionRetentionPolicy(retention_days=30, dry_run=False)
TEST_FLAGS = {
    "TRENDX_SCHEDULER_ENABLED": "false",
    "TRENDX_SCHEDULER_FORECAST_ENABLED": "false",
    "TRENDX_INGEST_ENABLED": "false",
    "TRENDX_WORKER_INGESTION_ENABLED": "false",
    "TB_WRITEBACK_ENABLED": "false",
    "TB_ALARMS_ENABLED": "false",
    "ANOMALY_DETECTION_ENABLED": "false",
}


@dataclass
class LiveServer:
    process: subprocess.Popen[str]
    url: str
    log_path: Path
    log_handle: IO[str]


def _record(execution_id: str, tenant_id: str = "tenant-A") -> ExecutionRecord:
    return ExecutionRecord(
        execution_id=execution_id,
        reference_key=f"w110:{execution_id}",
        tenant_id=tenant_id,
        entity_type="DEVICE",
        entity_id=f"device-{execution_id}",
        target_metric="temperature",
        frequency="1h",
        horizon=24,
        model_id="model-w110",
        model_version="1",
        algorithm="Fourier",
        feature_schema_version="schema-w110",
        feature_schema_fingerprint="fingerprint-w110",
        artifact_uri="file:///w110/model.bin",
        status=ExecutionStatus.SUCCESS,
        created_at="2025-01-01T00:00:00Z",
        started_at="2025-01-01T00:00:01Z",
        completed_at="2025-01-01T00:00:02Z",
        prediction_count=1,
        metadata={"source": "w110-ops"},
        result={"values": [1.0]},
    )


def _body(record: ExecutionRecord, checksum: str, tenant_id: str | None = None) -> dict[str, str]:
    return {
        "execution_id": record.execution_id,
        "tenant_id": tenant_id or record.tenant_id,
        "expected_archive_checksum": checksum,
        "conflict_policy": "FAIL_IF_EXISTS",
    }


def _archive(root: Path, record: ExecutionRecord) -> str:
    store = FileSystemArchiveStore(root)
    store.archive((record,), policy=POLICY, archived_at=NOW)
    checksum = store.verify(record.execution_id).checksum
    assert checksum is not None
    return checksum


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _environment(
    history_path: Path,
    archive_path: Path,
    audit_path: Path,
    workdir: Path,
) -> dict[str, str]:
    environment = os.environ.copy()
    environment["PYTHONPATH"] = str(SRC) + os.pathsep + environment.get("PYTHONPATH", "")
    environment.update(
        {
            "PYTHONDONTWRITEBYTECODE": "1",
            "TRENDX_API_TOKEN": TOKEN,
            "TRENDX_EXECUTION_HISTORY_PATH": str(history_path),
            "TRENDX_EXECUTION_ARCHIVE_PATH": str(archive_path),
            "TRENDX_EXECUTION_AUDIT_PATH": str(audit_path),
            "TRENDX_DEFAULT_TENANT_ID": "tenant-A",
            "TRENDX_DISK_MIN_FREE_GB": "0",
            "TRENDX_LOG_LEVEL": "INFO",
            "TRENDX_ENV": "test",
            "MLFLOW_TRACKING_URI": f"file://{workdir / 'mlruns'}",
            **TEST_FLAGS,
        }
    )
    return environment


def _start_server(
    history_path: Path,
    archive_path: Path,
    audit_path: Path,
    workdir: Path,
    label: str,
) -> LiveServer:
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
        env=_environment(history_path, archive_path, audit_path, workdir),
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
        raise TimeoutError("W110 Uvicorn startup timed out")
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


def _headers(request_id: str | None = None) -> dict[str, str]:
    headers = {"Authorization": f"Bearer {TOKEN}"}
    if request_id is not None:
        headers["X-Request-ID"] = request_id
    return headers


def _seed(history_path: Path) -> None:
    store = DurableExecutionStore(history_path)
    store.close()


def test_w110_real_restore_audit_query_statistics_and_errors(tmp_path: Path) -> None:
    history_path = tmp_path / "history.json"
    archive_path = tmp_path / "archive"
    audit_path = tmp_path / "audit"
    _seed(history_path)
    record = _record("real-audit")
    checksum = _archive(archive_path, record)
    server = _start_server(history_path, archive_path, audit_path, tmp_path, "query")
    try:
        with httpx.Client(base_url=server.url, trust_env=False, timeout=15.0) as client:
            assert client.get("/api/v1/forecast/executions/audit").status_code == 401
            request_id = "w110-request-001"
            restored = client.post(
                "/api/v1/forecast/executions/restore",
                json=_body(record, checksum),
                headers=_headers(request_id),
            )
            assert restored.status_code == 200
            assert restored.headers["X-Request-ID"] == request_id
            audit = client.get(
                "/api/v1/forecast/executions/audit",
                params={"operation": "RECOVERY_API", "limit": 10},
                headers=_headers(),
            )
            assert audit.status_code == 200
            body = audit.json()
            assert body["total"] == 1
            assert body["items"][0]["request_id"] == request_id
            assert body["items"][0]["actor_type"] == "API"
            assert body["items"][0]["actor_id"] == "api-key-context"
            restore_events = client.get(
                "/api/v1/forecast/executions/audit",
                params={"operation": "RESTORE", "request_id": request_id},
                headers=_headers(),
            )
            assert restore_events.status_code == 200
            assert restore_events.json()["total"] == 1
            assert restore_events.json()["items"][0]["request_id"] == request_id
            assert "archive_path" not in audit.text
            assert TOKEN not in audit.text
            stats = client.get(
                "/api/v1/forecast/executions/audit/statistics",
                headers=_headers(),
            )
            assert stats.status_code == 200
            assert stats.json()["total_events"] >= 2
            assert (
                client.get(
                    "/api/v1/forecast/executions/audit",
                    params={"tenant_id": "tenant-B"},
                    headers=_headers(),
                ).status_code
                == 403
            )
            assert (
                client.get(
                    "/api/v1/forecast/executions/audit",
                    params={"limit": 1001},
                    headers=_headers(),
                ).status_code
                == 422
            )
            assert (
                client.get(
                    "/api/v1/forecast/executions/audit",
                    params={"unknown": "value"},
                    headers=_headers(),
                ).status_code
                == 422
            )
    finally:
        logs = _stop_server(server)
    assert TOKEN not in logs
    assert str(archive_path) not in logs
    assert str(audit_path) not in logs
    assert "operation=restore" in logs
    assert "request_id=w110-request-001" in logs


def test_w110_real_process_isolation_and_concurrent_restore(tmp_path: Path) -> None:
    history_path = tmp_path / "history.json"
    archive_path = tmp_path / "archive"
    audit_path = tmp_path / "audit"
    _seed(history_path)
    record = _record("process-audit")
    checksum = _archive(archive_path, record)
    server_a = _start_server(history_path, archive_path, audit_path, tmp_path, "process-a")
    server_b = _start_server(history_path, archive_path, audit_path, tmp_path, "process-b")
    try:
        with (
            httpx.Client(base_url=server_a.url, trust_env=False, timeout=15.0) as client_a,
            httpx.Client(base_url=server_b.url, trust_env=False, timeout=15.0) as client_b,
        ):
            with ThreadPoolExecutor(max_workers=2) as executor:
                responses = list(
                    executor.map(
                        lambda client: client.post(
                            "/api/v1/forecast/executions/restore",
                            json=_body(record, checksum),
                            headers=_headers(),
                        ),
                        (client_a, client_b),
                    )
                )
            assert sorted(response.status_code for response in responses) == [200, 200]
            assert sorted(response.json()["status"] for response in responses) == [
                "ALREADY_PRESENT",
                "RESTORED",
            ]
            visible = client_a.get(
                "/api/v1/forecast/executions/audit",
                params={"operation": "RECOVERY_API", "limit": 10},
                headers=_headers(),
            )
            assert visible.status_code == 200
            assert visible.json()["total"] == 2
    finally:
        logs_a = _stop_server(server_a)
        logs_b = _stop_server(server_b)
    assert TOKEN not in logs_a + logs_b
    assert str(audit_path) not in logs_a + logs_b


def test_w110_real_read_only_and_performance_sizes(tmp_path: Path) -> None:
    history_path = tmp_path / "history.json"
    archive_path = tmp_path / "archive"
    audit_path = tmp_path / "audit"
    _seed(history_path)
    record = _record("performance-seed")
    _archive(archive_path, record)
    store = ExecutionAuditStore(audit_path)
    for index in range(1000):
        store.append(
            ExecutionAuditEvent(
                event_id=f"perf-{index}",
                occurred_at=NOW,
                operation=AuditOperation.EXECUTION,
                outcome=AuditOutcome.SUCCESS,
                tenant_id="tenant-A",
                execution_id=f"perf-exec-{index}",
                reference_key=None,
                actor_type=AuditActorType.SYSTEM,
                actor_id="system",
                request_id=f"perf-request-{index}",
                source="w110-ops",
                reason_code="performance_fixture",
            )
        )
    before = {
        path.name: hashlib.sha256(path.read_bytes()).hexdigest()
        for path in audit_path.glob("*.json")
    }
    server = _start_server(history_path, archive_path, audit_path, tmp_path, "performance")
    observations: list[dict[str, Any]] = []
    try:
        with httpx.Client(base_url=server.url, trust_env=False, timeout=30.0) as client:
            for size in (10, 100, 500, 1000):
                started = time.perf_counter()
                response = client.get(
                    "/api/v1/forecast/executions/audit",
                    params={"limit": min(size, 1000)},
                    headers=_headers(),
                )
                elapsed = time.perf_counter() - started
                assert response.status_code == 200
                assert len(response.json()["items"]) == min(size, 1000)
                observations.append({"requested": size, "seconds": elapsed})
            stats = client.get(
                "/api/v1/forecast/executions/audit/statistics",
                headers=_headers(),
            )
            assert stats.status_code == 200
            assert stats.json()["total_events"] == 1000
    finally:
        _stop_server(server)
    after = {
        path.name: hashlib.sha256(path.read_bytes()).hexdigest()
        for path in audit_path.glob("*.json")
    }
    assert before == after
    assert [item["requested"] for item in observations] == [10, 100, 500, 1000]
    assert all(item["seconds"] >= 0 for item in observations)


def test_w110_real_openapi_and_sanitized_audit_errors(tmp_path: Path) -> None:
    history_path = tmp_path / "history.json"
    archive_path = tmp_path / "archive"
    audit_path = tmp_path / "audit"
    _seed(history_path)
    record = _record("openapi-audit")
    _archive(archive_path, record)
    server = _start_server(history_path, archive_path, audit_path, tmp_path, "openapi")
    try:
        with httpx.Client(base_url=server.url, trust_env=False, timeout=15.0) as client:
            document = client.get("/openapi.json").json()
            path = document["paths"]["/api/v1/forecast/executions/audit"]
            assert set(path) == {"get"}
            assert {"401", "403", "422", "503"} <= set(path["get"]["responses"])
            assert path["get"]["security"] == [
                {"BearerAuth": []},
                {"ApiKeyAuth": []},
            ]
            assert (
                client.get(
                    "/api/v1/forecast/executions/audit",
                    params={"limit": "bad"},
                    headers=_headers(),
                ).status_code
                == 422
            )
            assert (
                client.post("/api/v1/forecast/executions/audit", headers=_headers()).status_code
                == 405
            )
            assert (
                client.get(
                    "/api/v1/forecast/executions/audit",
                    params={"operation": "SECRET"},
                    headers=_headers(),
                ).status_code
                == 422
            )
            assert TOKEN not in client.get("/api/v1/forecast/executions/audit").text
    finally:
        logs = _stop_server(server)
    assert TOKEN not in logs
    assert str(archive_path) not in logs
    assert "traceback" not in logs.lower()
