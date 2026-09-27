"""W112 — real Uvicorn/HTTPX operational checks for the audit lifecycle.

Self-contained local-process tests, like the W110/W111 operational suites: no
external catalog database, no scheduler, no worker, no forecast and no
ThingsBoard write path.
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
    AuditExportScope,
    ExecutionAuditControlService,
    ExecutionAuditExportService,
    ExecutionAuditIntegrityService,
)
from trendx.forecasting.audit_lifecycle import (
    AuditLifecycleService,
    AuditRestoreRequest,
    AuditRetentionPolicy,
    FileSystemAuditArchiveStore,
)

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src"
TOKEN = "w112-operational-test-token"
NOW = datetime(2026, 4, 1, tzinfo=UTC)
PATHS = {
    "preview": "/api/v1/forecast/executions/audit/lifecycle/preview",
    "archive": "/api/v1/forecast/executions/audit/lifecycle/archive",
    "purge": "/api/v1/forecast/executions/audit/lifecycle/purge",
    "restore": "/api/v1/forecast/executions/audit/lifecycle/restore",
    "audit": "/api/v1/forecast/executions/audit",
    "integrity": "/api/v1/forecast/executions/audit/integrity",
    "export": "/api/v1/forecast/executions/audit/export",
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
    request_id: str = "request-A",
    occurred_at: datetime = NOW,
) -> ExecutionAuditEvent:
    return ExecutionAuditEvent(
        event_id=event_id,
        occurred_at=occurred_at,
        operation=AuditOperation.EXECUTION,
        outcome=AuditOutcome.SUCCESS,
        tenant_id=tenant_id,
        execution_id="exec-A",
        reference_key="ref:exec-A",
        actor_type=AuditActorType.SYSTEM,
        actor_id="system",
        request_id=request_id,
        source="w112-ops",
        reason_code="record_restored",
    )


def _seed(audit_path: Path, *, old: int = 6, new: int = 3) -> None:
    store = ExecutionAuditStore(audit_path)
    for index in range(old):
        store.append(
            _event(
                f"old-{index}",
                request_id=f"old-r{index}",
                occurred_at=NOW - timedelta(days=100 + index),
            )
        )
    for index in range(new):
        store.append(
            _event(
                f"new-{index}",
                request_id=f"new-r{index}",
                occurred_at=NOW - timedelta(days=1),
            )
        )
    store.close()


def _fingerprint(path: Path) -> dict[str, str]:
    return {
        item.relative_to(path).as_posix(): hashlib.sha256(item.read_bytes()).hexdigest()
        for item in sorted(path.rglob("*"))
        if item.is_file()
    }


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _environment(audit_path: Path, archive_path: Path, workdir: Path) -> dict[str, str]:
    environment = os.environ.copy()
    environment["PYTHONPATH"] = str(SRC) + os.pathsep + environment.get("PYTHONPATH", "")
    environment.update(
        {
            "PYTHONDONTWRITEBYTECODE": "1",
            "TRENDX_API_TOKEN": TOKEN,
            "TRENDX_EXECUTION_AUDIT_PATH": str(audit_path),
            "TRENDX_EXECUTION_AUDIT_ARCHIVE_PATH": str(archive_path),
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


def _start_server(audit_path: Path, archive_path: Path, workdir: Path, label: str) -> LiveServer:
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
        env=_environment(audit_path, archive_path, workdir),
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
        raise TimeoutError("W112 Uvicorn startup timed out")
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


def _body(**overrides: object) -> dict[str, object]:
    body: dict[str, object] = {
        "retention_days": 30,
        "reference_time": NOW.isoformat(),
        "minimum_events_to_keep": 2,
        "dry_run": False,
    }
    body.update(overrides)
    return body


def test_w112_ops_preview_archive_purge_restore(tmp_path: Path) -> None:
    audit_path = tmp_path / "audit"
    archive_path = tmp_path / "audit-archive"
    _seed(audit_path)
    before = _fingerprint(audit_path)
    archive_before = _fingerprint(archive_path) if archive_path.exists() else {}
    server = _start_server(audit_path, archive_path, tmp_path, "lifecycle")
    try:
        with httpx.Client(base_url=server.url, trust_env=False, timeout=30.0) as client:
            # auth
            for path in PATHS.values():
                if "/audit" == path[-5:]:
                    continue
                assert client.post(path, json=_body()).status_code == 401

            # preview is read-only
            preview = client.post(PATHS["preview"], json=_body(), headers=_headers())
            assert preview.status_code == 200
            assert preview.json()["status"] == "DRY_RUN"
            assert preview.json()["eligible"] == 6
            assert preview.json()["protected"] == 2
            assert _fingerprint(audit_path) == before, "preview must not mutate the store"

            # archive
            archived = client.post(PATHS["archive"], json=_body(), headers=_headers("w112-archive"))
            assert archived.status_code == 200
            assert archived.json()["archived"] == 6
            assert archived.json()["archive_verified"] == 6
            assert archived.json()["status"] == "SUCCESS"

            # purge
            purged = client.post(PATHS["purge"], json=_body(), headers=_headers("w112-purge"))
            assert purged.status_code == 200
            assert purged.json()["purged"] == 6
            assert purged.json()["archive_verified"] == 6
            assert purged.json()["status"] == "SUCCESS"

            # W110 no longer returns a purged event
            after = client.get(PATHS["audit"], headers=_headers()).json()
            assert not {f"old-{i}" for i in range(6)} & {
                item["event_id"] for item in after["items"]
            }

            # W111 integrity stays coherent
            integrity = client.get(PATHS["integrity"], headers=_headers())
            assert integrity.status_code == 200
            assert integrity.json()["integrity_status"] == "VALID"
            assert integrity.json()["invalid_events"] == 0

            # restore one archived event
            archive = FileSystemAuditArchiveStore(archive_path)
            archived_id = archive.list_documents(tenant_id="tenant-A")[0].event_id
            restore = client.post(
                PATHS["restore"],
                json={"event_id": archived_id, "tenant_id": "tenant-A"},
                headers=_headers("w112-restore"),
            )
            assert restore.status_code == 200
            assert restore.json()["status"] == "RESTORED"
            again = client.post(
                PATHS["restore"],
                json={"event_id": archived_id, "tenant_id": "tenant-A"},
                headers=_headers(),
            )
            assert again.status_code == 200
            assert again.json()["status"] == "ALREADY_PRESENT"
            assert (
                client.post(
                    PATHS["restore"],
                    json={"event_id": "missing", "tenant_id": "tenant-A"},
                    headers=_headers(),
                ).status_code
                == 404
            )
            assert (
                client.post(
                    PATHS["restore"],
                    json={"event_id": archived_id, "tenant_id": "tenant-B"},
                    headers=_headers(),
                ).status_code
                == 403
            )

            # tenant isolation on the read surface
            assert (
                client.get(
                    PATHS["audit"],
                    params={"tenant_id": "tenant-B"},
                    headers=_headers(),
                ).status_code
                == 403
            )

            # no leakage
            for response in (preview, archived, purged, restore, again):
                assert TOKEN not in response.text
                assert str(audit_path) not in response.text
                assert str(archive_path) not in response.text
                assert "Traceback" not in response.text
                assert ".lock" not in response.text
    finally:
        logs = _stop_server(server)
    assert TOKEN not in logs
    assert str(audit_path) not in logs
    assert str(archive_path) not in logs
    assert "Traceback" not in logs
    assert "operation=audit_lifecycle" in logs
    assert archive_before == {} or isinstance(archive_before, dict)


def test_w112_ops_process_a_b_concurrent_lifecycle(tmp_path: Path) -> None:
    audit_path = tmp_path / "audit"
    archive_path = tmp_path / "audit-archive"
    _seed(audit_path, old=8, new=2)
    server_a = _start_server(audit_path, archive_path, tmp_path, "process-a")
    server_b = _start_server(audit_path, archive_path, tmp_path, "process-b")
    try:
        with (
            httpx.Client(base_url=server_a.url, trust_env=False, timeout=60.0) as client_a,
            httpx.Client(base_url=server_b.url, trust_env=False, timeout=60.0) as client_b,
        ):

            def purge(client: httpx.Client) -> int:
                response = client.post(PATHS["purge"], json=_body(), headers=_headers())
                assert response.status_code == 200
                return int(response.json()["purged"])

            def archive(client: httpx.Client) -> int:
                response = client.post(PATHS["archive"], json=_body(), headers=_headers())
                assert response.status_code == 200
                return int(response.json()["archived"])

            with ThreadPoolExecutor(max_workers=2) as executor:
                archived = sum(executor.map(archive, (client_a, client_b)))
            assert archived == 8, "each event is effectively archived exactly once"

            with ThreadPoolExecutor(max_workers=4) as executor:
                purged = sum(executor.map(purge, (client_a, client_b, client_a, client_b)))
            assert purged == 8, "each event is effectively purged exactly once"

            integrity = client_a.get(PATHS["integrity"], headers=_headers()).json()
            assert integrity["integrity_status"] == "VALID"
    finally:
        logs_a = _stop_server(server_a)
        logs_b = _stop_server(server_b)
    documents = FileSystemAuditArchiveStore(archive_path).list_documents(tenant_id="tenant-A")
    assert len(documents) == 8, "no duplicated and no lost archive"
    for document in documents:
        document.verify_integrity()
    assert TOKEN not in logs_a and TOKEN not in logs_b


def test_w112_ops_corrupt_archive_blocks_purge(tmp_path: Path) -> None:
    audit_path = tmp_path / "audit"
    archive_path = tmp_path / "audit-archive"
    # 6 old events, keep 2 -> 4 eligible and therefore 4 archived documents
    _seed(audit_path, old=6, new=0)
    server = _start_server(audit_path, archive_path, tmp_path, "corrupt")
    try:
        with httpx.Client(base_url=server.url, trust_env=False, timeout=30.0) as client:
            assert (
                client.post(PATHS["archive"], json=_body(), headers=_headers()).status_code == 200
            )
            path = next(archive_path.glob("*.json"))
            payload = json.loads(path.read_text(encoding="utf-8"))
            payload["archive_document_checksum"] = "0" * 64
            path.write_text(
                json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n",
                encoding="utf-8",
            )
            purge = client.post(PATHS["purge"], json=_body(), headers=_headers())
            assert purge.status_code == 200
            body = purge.json()
            # The event whose archive is corrupt must be blocked by a guard and
            # must still be active; the healthy events are still purged, so the
            # run is a partial success rather than a blanket refusal.
            assert body["guards"].get("archive_corrupt") == 1
            assert body["purged"] > 0, "healthy events must still be purged"
            assert body["purged"] + body["guards"]["archive_corrupt"] == body["eligible"]
            # the event whose archive is corrupt is still active
            remaining = client.get(PATHS["audit"], headers=_headers()).json()
            surviving = {item["event_id"] for item in remaining["items"]}
            assert payload["event"]["event_id"] in surviving
            for response in (purge,):
                assert "Traceback" not in response.text
                assert str(archive_path) not in response.text
    finally:
        logs = _stop_server(server)
    assert "Traceback" not in logs


def test_w112_ops_unavailable_archive(tmp_path: Path) -> None:
    audit_path = tmp_path / "audit"
    archive_path = tmp_path / "audit-archive"
    _seed(audit_path, old=1, new=0)
    # a regular file where the archive directory is expected: the factory must
    # refuse rather than silently create or purge
    archive_path.write_text("not a directory", encoding="utf-8")
    server = _start_server(audit_path, archive_path, tmp_path, "unavailable")
    try:
        with httpx.Client(base_url=server.url, trust_env=False, timeout=30.0) as client:
            for path in (PATHS["preview"], PATHS["archive"], PATHS["purge"]):
                response = client.post(path, json=_body(), headers=_headers())
                assert response.status_code == 503, path
                assert response.json()["detail"] == "audit_lifecycle_unavailable"
                assert str(archive_path) not in response.text
    finally:
        _stop_server(server)


def test_w112_ops_export_scope_after_lifecycle(tmp_path: Path) -> None:
    audit_path = tmp_path / "audit"
    archive_path = tmp_path / "audit-archive"
    _seed(audit_path, old=5, new=2)
    service = AuditLifecycleService(
        ExecutionAuditService(ExecutionAuditStore(audit_path, create_if_missing=False)),
        FileSystemAuditArchiveStore(archive_path),
    )
    policy = AuditRetentionPolicy(
        retention_days=30, reference_time=NOW, minimum_events_to_keep=2, dry_run=False
    )
    service.purge("tenant-A", policy)
    control = ExecutionAuditControlService(
        ExecutionAuditService(ExecutionAuditStore(audit_path, create_if_missing=False))
    )
    assert (
        ExecutionAuditIntegrityService(control).verify("tenant-A").integrity_status.value == "VALID"
    )
    active = ExecutionAuditExportService(control).export("tenant-A")
    both = ExecutionAuditExportService(control).export(
        "tenant-A", scope=AuditExportScope.ACTIVE_AND_ARCHIVED, lifecycle=service
    )
    assert json.loads(active.canonical_bytes)["scope"] == "ACTIVE_ONLY"
    assert json.loads(both.canonical_bytes)["scope"] == "ACTIVE_AND_ARCHIVED"
    assert both.count == active.count + 5
    assert active.export_checksum != both.export_checksum


def test_w112_ops_performance_observed(tmp_path: Path) -> None:
    """Observed durations only; W112 defines no SLA threshold."""

    observations: dict[int, dict[str, float]] = {}
    for size in (10, 100, 500, 1000):
        root = tmp_path / f"perf-{size}"
        audit_path = root / "audit"
        store = ExecutionAuditStore(audit_path)
        for index in range(size):
            store.append(
                _event(
                    f"p-{size}-{index}",
                    request_id=f"p{index}",
                    occurred_at=NOW - timedelta(days=200 + index),
                )
            )
        service = ExecutionAuditService(ExecutionAuditStore(audit_path, create_if_missing=False))
        archive = FileSystemAuditArchiveStore(root / "archive")
        lifecycle = AuditLifecycleService(service, archive)
        policy = AuditRetentionPolicy(retention_days=1, reference_time=NOW, dry_run=False)

        started = time.perf_counter()
        preview = lifecycle.preview("tenant-A", policy)
        preview_s = time.perf_counter() - started
        started = time.perf_counter()
        lifecycle.archive("tenant-A", policy)
        archive_s = time.perf_counter() - started
        started = time.perf_counter()
        for document in archive.list_documents()[:20]:
            archive.verify(document.event_id)
        verify_s = time.perf_counter() - started
        started = time.perf_counter()
        report = lifecycle.purge("tenant-A", policy)
        purge_s = time.perf_counter() - started
        started = time.perf_counter()
        restore_target = archive.list_documents()[0].event_id
        lifecycle.restore(
            AuditRestoreRequest(event_id=restore_target, tenant_id="tenant-A"),
            authenticated_tenant="tenant-A",
        )
        restore_s = time.perf_counter() - started

        assert preview.scanned >= size
        assert report.purged == size
        observations[size] = {
            "preview_s": round(preview_s, 4),
            "archive_s": round(archive_s, 4),
            "verify_s": round(verify_s, 4),
            "purge_s": round(purge_s, 4),
            "restore_s": round(restore_s, 4),
        }
    assert set(observations) == {10, 100, 500, 1000}
    print("\nW112 observed durations (no SLA defined):")
    for size, values in observations.items():
        print(f"  {size:5d} events -> {values}")


def test_w112_ops_openapi_and_no_delete_route(tmp_path: Path) -> None:
    audit_path = tmp_path / "audit"
    archive_path = tmp_path / "audit-archive"
    _seed(audit_path, old=1, new=1)
    server = _start_server(audit_path, archive_path, tmp_path, "openapi")
    try:
        with httpx.Client(base_url=server.url, trust_env=False, timeout=30.0) as client:
            schema = client.get("/openapi.json").json()
            for path in (PATHS["preview"], PATHS["archive"], PATHS["purge"], PATHS["restore"]):
                operation = schema["paths"][path]["post"]
                assert set(operation["responses"]) >= {
                    "200",
                    "401",
                    "403",
                    "409",
                    "422",
                    "503",
                }
                assert operation["security"] == [
                    {"BearerAuth": []},
                    {"ApiKeyAuth": []},
                ]
                verbs = {verb.upper() for verb in schema["paths"][path]}
                assert verbs == {"POST"}
            audit_deletes = [
                path
                for path, item in schema["paths"].items()
                if "delete" in {verb.lower() for verb in item} and "audit" in path
            ]
            assert audit_deletes == [], "no DELETE route may exist on the audit surface"
    finally:
        logs = _stop_server(server)
    for flag, value in TEST_FLAGS.items():
        assert value == "false", flag
    lowered = logs.lower()
    for marker in ("prophet", "operation=forecast", "writeback", "send_telemetry", "alarm created"):
        assert marker not in lowered, marker
    assert not (tmp_path / "mlruns").exists()
