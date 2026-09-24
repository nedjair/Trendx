"""W109 — real Uvicorn/httpx validation of authenticated recovery API."""

from __future__ import annotations

import os
import socket
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from threading import Barrier
from typing import IO, Any

import httpx
from trendx.forecasting.execution import DurableExecutionStore, ExecutionRecord, ExecutionStatus
from trendx.forecasting.lifecycle import ExecutionRetentionPolicy, FileSystemArchiveStore

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src"
TOKEN = "w109-operational-test-token"
NOW = datetime(2026, 3, 2, tzinfo=UTC)
POLICY = ExecutionRetentionPolicy(retention_days=30, dry_run=False)
# These tests are self-contained local Uvicorn/HTTPX checks.  They intentionally
# remain outside the repository's external-catalog integration marker, like the
# W105/W106 operational suites.
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
        prediction_count=1,
        metadata={"source": "w109-ops"},
        result={"values": [1.0]},
    )


def _body(record: ExecutionRecord, checksum: str, tenant_id: str | None = None) -> dict[str, str]:
    return {
        "execution_id": record.execution_id,
        "tenant_id": tenant_id or record.tenant_id,
        "expected_archive_checksum": checksum,
        "conflict_policy": "FAIL_IF_EXISTS",
    }


def _archive(root: Path, records: tuple[ExecutionRecord, ...]) -> dict[str, str]:
    store = FileSystemArchiveStore(root)
    store.archive(records, policy=POLICY, archived_at=NOW)
    checksums: dict[str, str] = {}
    for record in records:
        checksum = store.verify(record.execution_id).checksum
        assert checksum is not None
        checksums[record.execution_id] = checksum
    return checksums


def _seed_empty_store(path: Path) -> None:
    store = DurableExecutionStore(path)
    store.close()


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _environment(
    history_path: Path,
    archive_path: Path,
    workdir: Path,
    tenant: str = "tenant-A",
) -> dict[str, str]:
    environment = os.environ.copy()
    environment["PYTHONPATH"] = str(SRC) + os.pathsep + environment.get("PYTHONPATH", "")
    environment.update(
        {
            "PYTHONDONTWRITEBYTECODE": "1",
            "TRENDX_API_TOKEN": TOKEN,
            "TRENDX_EXECUTION_HISTORY_PATH": str(history_path),
            "TRENDX_EXECUTION_ARCHIVE_PATH": str(archive_path),
            "TRENDX_DEFAULT_TENANT_ID": tenant,
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
    workdir: Path,
    label: str,
    tenant: str = "tenant-A",
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
        env=_environment(history_path, archive_path, workdir, tenant),
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
        raise TimeoutError("W109 Uvicorn startup timed out")
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


def _headers() -> dict[str, str]:
    return {"Authorization": f"Bearer {TOKEN}"}


def _post(
    client: httpx.Client,
    body: dict[str, Any],
    *,
    authenticated: bool = True,
) -> httpx.Response:
    return client.post(
        "/api/v1/forecast/executions/restore",
        json=body,
        headers=_headers() if authenticated else None,
    )


def test_w109_real_post_auth_tenant_w99_w104_w106(tmp_path: Path) -> None:
    history_path = tmp_path / "history.json"
    archive_path = tmp_path / "archive"
    _seed_empty_store(history_path)
    record = _record("real-http")
    checksum = _archive(archive_path, (record,))[record.execution_id]
    server = _start_server(history_path, archive_path, tmp_path, "real-http")
    try:
        with httpx.Client(base_url=server.url, trust_env=False, timeout=15.0) as client:
            assert _post(client, _body(record, checksum), authenticated=False).status_code == 401
            invalid = client.post(
                "/api/v1/forecast/executions/restore",
                json=_body(record, checksum),
                headers={"Authorization": "Bearer invalid"},
            )
            assert invalid.status_code == 401
            forbidden = _post(client, _body(record, checksum, "tenant-B"))
            assert forbidden.status_code == 403
            assert forbidden.json()["detail"] == "tenant_forbidden"

            first = _post(client, _body(record, checksum))
            assert first.status_code == 200
            assert first.json()["status"] == "RESTORED"
            second = _post(client, _body(record, checksum))
            assert second.status_code == 200
            assert second.json()["status"] == "ALREADY_PRESENT"

            w99 = client.get(
                f"/api/v1/forecast/executions/{record.execution_id}",
                headers=_headers(),
            )
            assert w99.status_code == 200
            assert w99.json()["tenant_id"] == "tenant-A"
            assert w99.json()["status"] == "SUCCESS"
            assert w99.json()["result"] == record.result

            analytics = client.get(
                "/api/v1/forecast/executions/analytics",
                headers=_headers(),
            )
            report = client.get(
                "/api/v1/forecast/executions/report",
                headers=_headers(),
            )
            assert analytics.status_code == 200
            assert report.status_code == 200
            assert analytics.json()["summary"]["total"] == 1
            assert report.json()["summary"]["total"] == 1
    finally:
        logs = _stop_server(server)
    archive_file = next(archive_path.glob("*.json"))
    assert archive_file.read_bytes()
    assert TOKEN not in logs
    assert str(archive_path) not in logs
    assert "operation=restore" in logs
    assert "status_code=200" in logs


def test_w109_real_process_a_b_concurrent_restore(tmp_path: Path) -> None:
    history_path = tmp_path / "history.json"
    archive_path = tmp_path / "archive"
    _seed_empty_store(history_path)
    record = _record("process-http")
    checksum = _archive(archive_path, (record,))[record.execution_id]
    archive_file = next(archive_path.glob("*.json"))
    archive_before = archive_file.read_bytes()
    server_a = _start_server(history_path, archive_path, tmp_path, "process-a")
    server_b = _start_server(history_path, archive_path, tmp_path, "process-b")
    try:
        with (
            httpx.Client(base_url=server_a.url, trust_env=False, timeout=15.0) as client_a,
            httpx.Client(base_url=server_b.url, trust_env=False, timeout=15.0) as client_b,
        ):
            with ThreadPoolExecutor(max_workers=2) as executor:
                responses = list(
                    executor.map(
                        lambda client: _post(client, _body(record, checksum)),
                        (client_a, client_b),
                    )
                )
        assert sorted(response.status_code for response in responses) == [200, 200]
        assert sorted(response.json()["status"] for response in responses) == [
            "ALREADY_PRESENT",
            "RESTORED",
        ]
    finally:
        logs_a = _stop_server(server_a)
        logs_b = _stop_server(server_b)
    store = DurableExecutionStore(history_path)
    try:
        assert store.get(record.execution_id) == record
    finally:
        store.close()
    assert "status_code=409" not in logs_a + logs_b
    assert archive_file.read_bytes() == archive_before
    assert TOKEN not in logs_a + logs_b


def test_w109_real_openapi_and_sanitized_errors(tmp_path: Path) -> None:
    history_path = tmp_path / "history.json"
    archive_path = tmp_path / "archive"
    _seed_empty_store(history_path)
    record = _record("error-http")
    checksums = _archive(archive_path, (record,))
    checksum = checksums[record.execution_id]
    server = _start_server(history_path, archive_path, tmp_path, "errors")
    try:
        with httpx.Client(base_url=server.url, trust_env=False, timeout=15.0) as client:
            openapi = client.get("/openapi.json")
            assert openapi.status_code == 200
            document = openapi.json()
            path = document["paths"]["/api/v1/forecast/executions/restore"]
            assert set(path) == {"post"}
            assert "get" not in path
            assert "delete" not in path
            assert path["post"]["security"] == [
                {"BearerAuth": []},
                {"ApiKeyAuth": []},
            ]
            assert set(document["components"]["securitySchemes"]) == {
                "BearerAuth",
                "ApiKeyAuth",
            }

            missing = _post(
                client,
                {
                    "execution_id": "missing-http",
                    "tenant_id": "tenant-A",
                    "expected_archive_checksum": "0" * 64,
                    "conflict_policy": "FAIL_IF_EXISTS",
                },
            )
            assert missing.status_code == 404
            assert missing.json()["status"] == "ARCHIVE_NOT_FOUND"

            archive_file = next(archive_path.glob("*.json"))
            archive_file.write_text("{broken", encoding="utf-8")
            corrupt = _post(client, _body(record, checksum))
            assert corrupt.status_code == 422
            assert corrupt.json()["status"] in {"INVALID_ARCHIVE", "INTEGRITY_FAILURE"}
            assert str(archive_path) not in corrupt.text
            assert "traceback" not in corrupt.text.lower()

            malformed = client.post(
                "/api/v1/forecast/executions/restore",
                content=b"{broken",
                headers={**_headers(), "Content-Type": "application/json"},
            )
            assert malformed.status_code == 422
            assert malformed.json() == {"detail": "invalid_request"}
    finally:
        logs = _stop_server(server)
    assert TOKEN not in logs
    assert "operation=restore" in logs


def test_w109_real_post_and_w99_read_concurrent(tmp_path: Path) -> None:
    history_path = tmp_path / "history.json"
    archive_path = tmp_path / "archive"
    _seed_empty_store(history_path)
    record = _record("concurrent-read")
    checksum = _archive(archive_path, (record,))[record.execution_id]
    archive_file = next(archive_path.glob("*.json"))
    archive_before = archive_file.read_bytes()
    server = _start_server(history_path, archive_path, tmp_path, "concurrent-read")
    try:
        with httpx.Client(base_url=server.url, trust_env=False, timeout=15.0) as client:
            barrier = Barrier(2)

            def restore() -> httpx.Response:
                barrier.wait()
                return _post(client, _body(record, checksum))

            def read() -> httpx.Response:
                barrier.wait()
                return client.get(
                    f"/api/v1/forecast/executions/{record.execution_id}",
                    headers=_headers(),
                )

            with ThreadPoolExecutor(max_workers=2) as executor:
                restore_future = executor.submit(restore)
                read_future = executor.submit(read)
                restore_response = restore_future.result()
                read_response = read_future.result()

            assert restore_response.status_code == 200
            assert restore_response.json()["status"] == "RESTORED"
            assert read_response.status_code in {200, 404}
            final_read = client.get(
                f"/api/v1/forecast/executions/{record.execution_id}",
                headers=_headers(),
            )
            assert final_read.status_code == 200
            assert final_read.json()["tenant_id"] == "tenant-A"
    finally:
        _stop_server(server)
    assert archive_file.read_bytes() == archive_before


def test_w109_real_performance_10_100_500(tmp_path: Path) -> None:
    history_path = tmp_path / "history.json"
    archive_path = tmp_path / "archive"
    _seed_empty_store(history_path)
    all_records: list[ExecutionRecord] = []
    for size in (10, 100, 500):
        all_records.extend(_record(f"perf-http-{size}-{index}") for index in range(size))
    checksums = _archive(archive_path, tuple(all_records))
    server = _start_server(history_path, archive_path, tmp_path, "performance")
    observations: list[dict[str, Any]] = []
    try:
        with httpx.Client(base_url=server.url, trust_env=False, timeout=30.0) as client:
            offset = 0
            for size in (10, 100, 500):
                started = time.perf_counter()
                statuses: list[str] = []
                for record in all_records[offset : offset + size]:
                    response = _post(
                        client,
                        _body(record, checksums[record.execution_id]),
                    )
                    assert response.status_code == 200
                    statuses.append(response.json()["status"])
                offset += size
                latency = time.perf_counter() - started
                read_started = time.perf_counter()
                read_response = client.get(
                    "/api/v1/forecast/executions/analytics",
                    headers=_headers(),
                )
                read_latency = time.perf_counter() - read_started
                assert read_response.status_code == 200
                assert read_response.json()["summary"]["total"] == offset
                assert statuses.count("RESTORED") == size
                observations.append(
                    {
                        "records": size,
                        "http_restore_seconds": latency,
                        "w99_read_seconds": read_latency,
                    }
                )
    finally:
        _stop_server(server)
    assert [item["records"] for item in observations] == [10, 100, 500]
    assert all(item["http_restore_seconds"] >= 0 for item in observations)
    assert all(item["w99_read_seconds"] >= 0 for item in observations)
