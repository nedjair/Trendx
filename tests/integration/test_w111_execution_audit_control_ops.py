"""W111 — real Uvicorn/HTTPX operational checks for the audit control plane.

These tests are self-contained local-process checks, like the W110 operational
suite; they do not require the external catalog database and they never start a
scheduler, worker, ingest, forecast or ThingsBoard write path.
"""

from __future__ import annotations

import hashlib
import json
import os
import socket
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import IO

import httpx
from trendx.forecasting.audit import (
    AuditActorType,
    AuditOperation,
    AuditOutcome,
    ExecutionAuditEvent,
    ExecutionAuditService,
    ExecutionAuditStore,
)
from trendx.forecasting.audit_control import (
    ExecutionAuditExportService,
    ExecutionAuditIntegrityService,
    ExecutionAuditReconciliationService,
)

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src"
TOKEN = "w111-operational-test-token"
NOW = datetime(2026, 4, 1, tzinfo=UTC)
PATHS = {
    "integrity": "/api/v1/forecast/executions/audit/integrity",
    "reconciliation": "/api/v1/forecast/executions/audit/reconciliation",
    "export": "/api/v1/forecast/executions/audit/export",
    "checksum": "/api/v1/forecast/executions/audit/export/checksum",
}

TEST_FLAGS = {
    "TRENDX_SCHEDULER_ENABLED": "false",
    "TRENDX_SCHEDULER_FORECAST_ENABLED": "false",
    "TRENDX_INGEST_ENABLED": "false",
    "TRENDX_WORKER_INGESTION_ENABLED": "false",
    "TRENDX_WORKER_FORECAST_ENABLED": "false",
    "TB_WRITEBACK_ENABLED": "false",
    "TB_ALARMS_ENABLED": "false",
    "ANOMALY_DETECTION_ENABLED": "false",
    "TRENDX_MLFLOW_ENABLED": "false",
}


@dataclass
class LiveServer:
    process: subprocess.Popen[str]
    url: str
    log_path: Path
    log_handle: IO[str]


def _event(
    event_id: str,
    *,
    tenant_id: str = "tenant-A",
    execution_id: str = "exec-A",
    operation: AuditOperation = AuditOperation.EXECUTION,
    outcome: AuditOutcome = AuditOutcome.SUCCESS,
    reason_code: str = "record_restored",
    request_id: str = "request-A",
    occurred_at: datetime = NOW,
) -> ExecutionAuditEvent:
    return ExecutionAuditEvent(
        event_id=event_id,
        occurred_at=occurred_at,
        operation=operation,
        outcome=outcome,
        tenant_id=tenant_id,
        execution_id=execution_id,
        reference_key=f"ref:{execution_id}",
        actor_type=AuditActorType.SYSTEM,
        actor_id="system",
        request_id=request_id,
        source="w111-ops",
        reason_code=reason_code,
    )


def _seed_chain(audit_path: Path, *, tenant_id: str = "tenant-A") -> None:
    store = ExecutionAuditStore(audit_path)
    store.append(
        _event(
            "ops-api",
            tenant_id=tenant_id,
            operation=AuditOperation.RECOVERY_API,
            reason_code="record_restored",
            request_id="w111-request-1",
        )
    )
    store.append(
        _event(
            "ops-restore",
            tenant_id=tenant_id,
            operation=AuditOperation.RESTORE,
            reason_code="record_restored",
            request_id="w111-request-1",
            occurred_at=NOW + timedelta(seconds=1),
        )
    )
    store.append(
        _event(
            "ops-archive",
            tenant_id=tenant_id,
            operation=AuditOperation.ARCHIVE,
            reason_code="archive_verified",
            request_id="w111-request-1",
            occurred_at=NOW + timedelta(seconds=2),
        )
    )
    store.close()


def _fingerprint(path: Path) -> dict[str, str]:
    return {
        item.name: hashlib.sha256(item.read_bytes()).hexdigest()
        for item in sorted(path.rglob("*"))
        if item.is_file()
    }


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _environment(audit_path: Path, workdir: Path) -> dict[str, str]:
    environment = os.environ.copy()
    environment["PYTHONPATH"] = str(SRC) + os.pathsep + environment.get("PYTHONPATH", "")
    environment.update(
        {
            "PYTHONDONTWRITEBYTECODE": "1",
            "TRENDX_API_TOKEN": TOKEN,
            "TRENDX_EXECUTION_AUDIT_PATH": str(audit_path),
            "TRENDX_EXECUTION_HISTORY_PATH": "",
            "TRENDX_EXECUTION_ARCHIVE_PATH": "",
            "TRENDX_DEFAULT_TENANT_ID": "tenant-A",
            "TRENDX_DISK_MIN_FREE_GB": "0",
            "TRENDX_LOG_LEVEL": "INFO",
            "TRENDX_ENV": "test",
            "MLFLOW_TRACKING_URI": f"file://{workdir / 'mlruns'}",
            **TEST_FLAGS,
        }
    )
    return environment


def _start_server(audit_path: Path, workdir: Path, label: str) -> LiveServer:
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
        env=_environment(audit_path, workdir),
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
        raise TimeoutError("W111 Uvicorn startup timed out")
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


def test_w111_ops_endpoints_read_only_auth_deterministic_and_tenant_safe(
    tmp_path: Path,
) -> None:
    audit_path = tmp_path / "audit"
    _seed_chain(audit_path)
    store = ExecutionAuditStore(audit_path)
    store.append(
        _event(
            "ops-other-tenant",
            tenant_id="tenant-B",
            execution_id="exec-B",
            request_id="request-B",
            reason_code="record_restored",
        )
    )
    before = _fingerprint(audit_path)
    server = _start_server(audit_path, tmp_path, "control")
    try:
        with httpx.Client(base_url=server.url, trust_env=False, timeout=20.0) as client:
            # no auth -> 401
            for path in PATHS.values():
                assert client.get(path).status_code == 401
            # bad auth -> 401
            assert (
                client.get(PATHS["export"], headers={"Authorization": "Bearer nope"}).status_code
                == 401
            )
            # integrity
            integrity = client.get(PATHS["integrity"], headers=_headers())
            assert integrity.status_code == 200
            assert integrity.json()["integrity_status"] == "VALID"
            assert integrity.json()["valid_events"] == 3
            assert integrity.json()["tenant_id"] == "tenant-A"
            assert "tenant-B" not in integrity.text
            # reconciliation
            reconciliation = client.get(PATHS["reconciliation"], headers=_headers())
            assert reconciliation.status_code == 200
            assert reconciliation.json()["reconciliation_status"] == "CONSISTENT"
            assert reconciliation.json()["correlation_chains"] == 1
            # export determinism
            first = client.get(PATHS["export"], headers=_headers())
            second = client.get(PATHS["export"], headers=_headers())
            assert first.status_code == second.status_code == 200
            assert first.json()["export_checksum"] == second.json()["export_checksum"]
            assert first.json()["content"] == second.json()["content"]
            payload = json.loads(first.json()["content"])
            assert [item["event_id"] for item in payload["events"]] == [
                "ops-api",
                "ops-restore",
                "ops-archive",
            ]
            assert (
                first.json()["export_checksum"]
                == hashlib.sha256(first.json()["content"].encode("utf-8")).hexdigest()
            )
            # checksum endpoint
            checksum = client.get(PATHS["checksum"], headers=_headers())
            assert checksum.status_code == 200
            assert checksum.json()["export_checksum"] == first.json()["export_checksum"]
            assert "content" not in checksum.json()
            # jsonl
            jsonl = client.get(PATHS["export"], params={"format": "JSONL"}, headers=_headers())
            assert jsonl.status_code == 200
            assert len(jsonl.json()["content"].splitlines()) == 3
            # tenant isolation
            for path in PATHS.values():
                assert (
                    client.get(
                        path, params={"tenant_id": "tenant-B"}, headers=_headers()
                    ).status_code
                    == 403
                )
            # bounds and unknown fields
            for params in ({"limit": 1001}, {"limit": 0}, {"offset": -1}, {"unknown": "x"}):
                assert (
                    client.get(PATHS["integrity"], params=params, headers=_headers()).status_code
                    == 422
                )
            assert (
                client.get(
                    PATHS["export"], params={"format": "CSV"}, headers=_headers()
                ).status_code
                == 422
            )
            # no write verbs
            for path in PATHS.values():
                for verb in ("POST", "PUT", "PATCH", "DELETE"):
                    assert client.request(verb, path, headers=_headers()).status_code == 405
            # request correlation
            correlated = client.get(PATHS["export"], headers=_headers("w111-request-1"))
            assert correlated.headers["X-Request-ID"] == "w111-request-1"
            # openapi
            schema = client.get("/openapi.json").json()
            for path in PATHS.values():
                operation = schema["paths"][path]["get"]
                assert set(operation["responses"]) >= {"200", "401", "403", "422", "503"}
                assert operation["security"] == [{"BearerAuth": []}, {"ApiKeyAuth": []}]
                verbs = {verb.upper() for verb in schema["paths"][path]}
                assert verbs == {"GET"}
            # no leakage
            for path in PATHS.values():
                body = client.get(path, headers=_headers()).text
                assert TOKEN not in body
                assert str(audit_path) not in body
                assert "Traceback" not in body
                assert "tenant-B" not in body
    finally:
        logs = _stop_server(server)
    after = _fingerprint(audit_path)
    assert before == after, "W111 must not mutate the audit datastore"
    assert TOKEN not in logs
    assert str(audit_path) not in logs
    assert "Traceback" not in logs
    assert "operation=audit_integrity" in logs
    assert "operation=audit_export" in logs


def test_w111_ops_process_a_b_and_thread_concurrency(tmp_path: Path) -> None:
    audit_path = tmp_path / "audit"
    _seed_chain(audit_path)
    before = _fingerprint(audit_path)
    server_a = _start_server(audit_path, tmp_path, "process-a")
    server_b = _start_server(audit_path, tmp_path, "process-b")
    try:
        with (
            httpx.Client(base_url=server_a.url, trust_env=False, timeout=30.0) as client_a,
            httpx.Client(base_url=server_b.url, trust_env=False, timeout=30.0) as client_b,
        ):
            export_a = client_a.get(PATHS["export"], headers=_headers())
            export_b = client_b.get(PATHS["export"], headers=_headers())
            assert export_a.status_code == export_b.status_code == 200
            assert export_a.json()["export_checksum"] == export_b.json()["export_checksum"]
            assert export_a.json()["content"] == export_b.json()["content"]

            def call(client: httpx.Client) -> tuple[str, str, str, bool]:
                integrity = client.get(PATHS["integrity"], headers=_headers())
                export = client.get(PATHS["export"], headers=_headers())
                checksum = client.get(PATHS["checksum"], headers=_headers())
                reconciliation = client.get(PATHS["reconciliation"], headers=_headers())
                return (
                    integrity.json()["integrity_status"],
                    export.json()["export_checksum"],
                    reconciliation.json()["reconciliation_status"],
                    checksum.json()["export_checksum"] == export.json()["export_checksum"],
                )

            with ThreadPoolExecutor(max_workers=6) as executor:
                futures = [
                    executor.submit(call, client_a if index % 2 else client_b)
                    for index in range(12)
                ]
                results = [future.result() for future in futures]
            assert len(set(results)) == 1
            assert results[0] == (
                "VALID",
                export_a.json()["export_checksum"],
                "CONSISTENT",
                True,
            )
    finally:
        logs_a = _stop_server(server_a)
        logs_b = _stop_server(server_b)
    assert before == _fingerprint(audit_path)
    assert TOKEN not in logs_a and TOKEN not in logs_b


def test_w111_ops_corruption_contract(tmp_path: Path) -> None:
    audit_path = tmp_path / "audit"
    _seed_chain(audit_path)
    store = ExecutionAuditStore(audit_path)
    victim = next(store.path.glob("*.json"))
    payload = json.loads(victim.read_text(encoding="utf-8"))
    payload["reason_code"] = "tampered"
    victim.write_text(
        json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n",
        encoding="utf-8",
    )
    server = _start_server(audit_path, tmp_path, "corrupt")
    try:
        with httpx.Client(base_url=server.url, trust_env=False, timeout=20.0) as client:
            integrity = client.get(PATHS["integrity"], headers=_headers())
            assert integrity.status_code == 200
            body = integrity.json()
            assert body["integrity_status"] == "INVALID"
            assert body["checksum_failures"] == 1
            assert body["first_failure"].startswith("CHECKSUM")
            assert str(audit_path) not in integrity.text
            for path in (PATHS["export"], PATHS["checksum"], PATHS["reconciliation"]):
                response = client.get(path, headers=_headers())
                assert response.status_code == 503, path
                assert response.json()["detail"] == "audit_service_unavailable"
                assert "Traceback" not in response.text
                assert str(audit_path) not in response.text
    finally:
        logs = _stop_server(server)
    assert "Traceback" not in logs


def test_w111_ops_unavailable_store(tmp_path: Path) -> None:
    audit_path = tmp_path / "audit"
    audit_path.mkdir(parents=True)
    audit_path.chmod(0o755)
    server = _start_server(audit_path, tmp_path, "unavailable")
    try:
        with httpx.Client(base_url=server.url, trust_env=False, timeout=20.0) as client:
            for path in PATHS.values():
                response = client.get(path, headers=_headers())
                assert response.status_code == 503, path
                assert response.json()["detail"] == "audit_service_unavailable"
    finally:
        _stop_server(server)


def test_w111_ops_performance_observed_durations(tmp_path: Path) -> None:
    """Observed durations only. W111 deliberately defines no SLA threshold."""

    observations: dict[int, dict[str, float]] = {}
    for size in (10, 100, 500, 1000):
        root = tmp_path / f"perf-{size}"
        audit_path = root / "audit"
        store = ExecutionAuditStore(audit_path)
        for index in range(size):
            store.append(
                _event(
                    f"perf-{size}-{index}",
                    operation=AuditOperation.EXECUTION,
                    reason_code="record_restored",
                    request_id=f"perf-{index}",
                    occurred_at=NOW + timedelta(seconds=index),
                )
            )
        service = ExecutionAuditService(ExecutionAuditStore(audit_path, create_if_missing=False))
        started = time.perf_counter()
        integrity = ExecutionAuditIntegrityService(service).verify("tenant-A")
        integrity_seconds = time.perf_counter() - started
        started = time.perf_counter()
        reconciliation = ExecutionAuditReconciliationService(service).reconcile("tenant-A")
        reconciliation_seconds = time.perf_counter() - started
        started = time.perf_counter()
        export = ExecutionAuditExportService(service).export("tenant-A", limit=1000)
        export_seconds = time.perf_counter() - started
        started = time.perf_counter()
        checksum = ExecutionAuditExportService(service).checksum("tenant-A", limit=1000)
        checksum_seconds = time.perf_counter() - started

        assert integrity.events_scanned == size
        assert integrity.integrity_status.value == "VALID"
        assert export.count == size
        assert reconciliation.events_scanned == size
        assert checksum["export_checksum"] == export.export_checksum
        observations[size] = {
            "events": float(size),
            "integrity_s": round(integrity_seconds, 4),
            "reconciliation_s": round(reconciliation_seconds, 4),
            "export_s": round(export_seconds, 4),
            "checksum_s": round(checksum_seconds, 4),
        }
    assert set(observations) == {10, 100, 500, 1000}
    print("\nW111 observed durations (no SLA defined):")
    for size, values in observations.items():
        assert values["events"] == float(size)
        print(f"  {size:5d} events -> {values}")


def test_w111_ops_no_side_effect_flags(tmp_path: Path) -> None:
    """Every W111 route is served with all side-effect flags disabled."""

    audit_path = tmp_path / "audit"
    _seed_chain(audit_path)
    before = _fingerprint(audit_path)
    server = _start_server(audit_path, tmp_path, "flags")
    try:
        with httpx.Client(base_url=server.url, trust_env=False, timeout=20.0) as client:
            for path in PATHS.values():
                assert client.get(path, headers=_headers()).status_code == 200
    finally:
        logs = _stop_server(server)
    lowered = logs.lower()
    for flag, value in TEST_FLAGS.items():
        assert value == "false", flag
    # unambiguous side-effect markers must be absent from the server log
    for marker in (
        "prophet",
        "operation=forecast",
        "model training",
        "writeback",
        "send_telemetry",
        "alarm created",
        "operation=ingest",
    ):
        assert marker not in lowered, marker
    # no MLflow tracking artefact was created by a read-only control-plane call
    assert not (tmp_path / "mlruns").exists()
    assert before == _fingerprint(audit_path)
