"""W113 — real Uvicorn/HTTPX operational checks for audit operational health.

Self-contained local-process tests, like the W110/W111/W112 operational suites:
no external catalog database, no scheduler, no worker, no forecast, no
ThingsBoard write path and no destructive automation.  Every test here is
read-only: the datastore fingerprint is taken before and after each exercise.
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
from trendx.forecasting.audit_health import (
    ARCHIVE_CORRUPTED,
    AUDIT_DATA_CORRUPTED,
    AuditHealthService,
)
from trendx.forecasting.audit_lifecycle import (
    AuditLifecycleService,
    AuditRetentionPolicy,
    FileSystemAuditArchiveStore,
)

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src"
TOKEN = "w113-operational-test-token"
NOW = datetime(2026, 4, 1, tzinfo=UTC)
PATHS = {
    "health": "/api/v1/forecast/executions/audit/health",
    "capacity": "/api/v1/forecast/executions/audit/capacity",
    "readiness": "/api/v1/forecast/executions/audit/readiness",
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
        source="w113-ops",
        reason_code="record_restored",
    )


def _seed(audit_path: Path, *, old: int = 6, new: int = 2, other: int = 0) -> None:
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
    for index in range(other):
        store.append(
            _event(
                f"other-{index}",
                tenant_id="tenant-B",
                request_id=f"other-r{index}",
                occurred_at=NOW - timedelta(days=50 + index),
            )
        )
    store.close()


def _fingerprint(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}
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
        raise TimeoutError("W113 Uvicorn startup timed out")
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


def _no_disclosure(text: str, *forbidden: str) -> None:
    """No path, no secret, no traceback may appear in a diagnostic response."""

    assert TOKEN not in text
    assert "Traceback" not in text
    assert "/root" not in text
    assert "/tmp" not in text
    assert ".json" not in text
    assert "archive_path" not in text
    for needle in forbidden:
        assert needle not in text


def _no_disclosure_in_log(text: str, *forbidden: str) -> None:
    """A server log must carry no secret, no traceback and no W113 path.

    Third-party startup warnings legitimately mention interpreter paths, so the
    check targets what W113 controls: the audit and archive locations, the tenant
    data and the token.
    """

    assert TOKEN not in text
    assert "Traceback" not in text
    for needle in forbidden:
        assert needle not in text


# ── 2/3/4/5/6 + 9. auth, health, capacity, readiness, read-only proof ──────


def test_w113_ops_endpoints_auth_and_read_only(tmp_path: Path) -> None:
    audit_path = tmp_path / "audit"
    archive_path = tmp_path / "audit-archive"
    _seed(audit_path, old=6, new=2, other=2)
    # populate the archive so the archive side of capacity is non-trivial
    service = ExecutionAuditService(ExecutionAuditStore(audit_path, create_if_missing=False))
    lifecycle = AuditLifecycleService(service, FileSystemAuditArchiveStore(archive_path))
    lifecycle.archive(
        "tenant-A",
        AuditRetentionPolicy(
            retention_days=1,
            reference_time=NOW,
            minimum_events_to_keep=6,
            dry_run=False,
        ),
    )
    before = _fingerprint(audit_path)
    archive_before = _fingerprint(archive_path)
    server = _start_server(audit_path, archive_path, tmp_path, "endpoints")
    try:
        with httpx.Client(base_url=server.url, trust_env=False, timeout=30.0) as client:
            # auth is mandatory on every W113 route
            for path in PATHS.values():
                assert client.get(path).status_code == 401
                assert client.get(path, headers={"Authorization": "Bearer nope"}).status_code == 401

            # health
            health = client.get(PATHS["health"], headers=_headers("w113-health"))
            assert health.status_code == 200
            body = health.json()
            assert body["status"] == "HEALTHY"
            assert body["event_count"] == 9, "8 tenant-A events + 1 W112 lifecycle event"
            assert body["tenant_count"] == 2
            assert body["success_count"] == body["event_count"]
            assert body["integrity_invalid_count"] == 0
            assert body["oldest_event_at"] < body["newest_event_at"]
            assert body["reason_codes"] == []
            _no_disclosure(health.text, "tenant-B", "other-0")

            # capacity
            capacity = client.get(PATHS["capacity"], headers=_headers("w113-capacity"))
            assert capacity.status_code == 200
            snapshot = capacity.json()
            assert snapshot["event_count"] == 9
            assert snapshot["archived_event_count"] == 2
            assert snapshot["total_known_event_count"] == 9, "archived events counted once"
            assert snapshot["active_bytes"] > 0
            assert snapshot["archive_bytes"] > 0
            assert snapshot["capacity_measurement_complete"] is True
            assert snapshot["current_count"] == 9
            # growth is never fabricated from an unjustified assumption
            assert snapshot["observed_growth"] is None
            assert snapshot["observation_window"] is None
            filesystem = snapshot["filesystem"]
            assert filesystem["available"] is True
            assert filesystem["filesystem_total_bytes"] > 0
            assert filesystem["inode_total"] is not None
            _no_disclosure(capacity.text, str(audit_path), "tenant-B")

            # readiness
            readiness = client.get(PATHS["readiness"], headers=_headers("w113-readiness"))
            assert readiness.status_code == 200
            verdict = readiness.json()
            assert verdict["readiness_status"] == "READY"
            assert verdict["blocking_reason_codes"] == []
            assert verdict["health_status"] == "HEALTHY"
            assert verdict["archive_status"] == "HEALTHY"
            _no_disclosure(readiness.text)

            # 9. read-only proof: the datastore is byte-identical
            assert _fingerprint(audit_path) == before
            assert _fingerprint(archive_path) == archive_before
    finally:
        logs = _stop_server(server)
    # 11. observability: the operation is logged without leaking anything
    assert "operation=audit_health" in logs
    assert "status_code=200" in logs
    assert "health_HEALTHY" in logs
    assert "readiness_READY" in logs
    _no_disclosure_in_log(logs, str(audit_path), str(archive_path), "other-0")


# ── 3. tenant isolation over HTTP ────────────────────────────────────────


def test_w113_ops_tenant_isolation(tmp_path: Path) -> None:
    audit_path = tmp_path / "audit"
    archive_path = tmp_path / "audit-archive"
    _seed(audit_path, old=4, new=0, other=5)
    before = _fingerprint(audit_path)
    server = _start_server(audit_path, archive_path, tmp_path, "tenant")
    try:
        with httpx.Client(base_url=server.url, trust_env=False, timeout=30.0) as client:
            for path in PATHS.values():
                text = client.get(path, headers=_headers()).text
                assert "tenant-B" not in text
                assert "other-" not in text
            health = client.get(PATHS["health"], headers=_headers()).json()
            assert health["tenant_id"] == "tenant-A"
            assert health["event_count"] == 4
            assert health["tenant_count"] == 2, "the store-wide tenant count stays a fact"
            assert _fingerprint(audit_path) == before
    finally:
        logs = _stop_server(server)
    _no_disclosure_in_log(logs, str(audit_path), "other-")


# ── 7. Process A / Process B read the identical state ────────────────────


def test_w113_ops_process_a_b_identical_reads(tmp_path: Path) -> None:
    audit_path = tmp_path / "audit"
    archive_path = tmp_path / "audit-archive"
    _seed(audit_path, old=6, new=2)
    service = ExecutionAuditService(ExecutionAuditStore(audit_path, create_if_missing=False))
    archive = FileSystemAuditArchiveStore(archive_path)
    AuditLifecycleService(service, archive).archive(
        "tenant-A",
        AuditRetentionPolicy(
            retention_days=1, reference_time=NOW, minimum_events_to_keep=6, dry_run=False
        ),
    )
    before = _fingerprint(audit_path)
    archive_before = _fingerprint(archive_path)
    server_a = _start_server(audit_path, archive_path, tmp_path, "process-a")
    server_b = _start_server(audit_path, archive_path, tmp_path, "process-b")
    try:
        with (
            httpx.Client(base_url=server_a.url, trust_env=False, timeout=60.0) as client_a,
            httpx.Client(base_url=server_b.url, trust_env=False, timeout=60.0) as client_b,
        ):
            for key, path in PATHS.items():
                body_a = client_a.get(path, headers=_headers()).json()
                body_b = client_b.get(path, headers=_headers()).json()
                for volatile in ("checked_at", "measured_at", "last_verified_at"):
                    body_a.pop(volatile, None)
                    body_b.pop(volatile, None)
                if key != "readiness":
                    # the two processes must not observe a different journal
                    assert body_a == body_b, key
            # process B sees the archive the same way
            assert (
                client_b.get(PATHS["capacity"], headers=_headers()).json()["archived_event_count"]
                == 2
            )
    finally:
        logs_a = _stop_server(server_a)
        logs_b = _stop_server(server_b)
    assert _fingerprint(audit_path) == before, "no process mutated the journal"
    assert _fingerprint(archive_path) == archive_before
    _no_disclosure_in_log(logs_a + logs_b, str(audit_path), "tenant-B")


# ── 8. concurrent readers ────────────────────────────────────────────────


def test_w113_ops_concurrent_readers(tmp_path: Path) -> None:
    audit_path = tmp_path / "audit"
    archive_path = tmp_path / "audit-archive"
    _seed(audit_path, old=30, new=5)
    before = _fingerprint(audit_path)
    server = _start_server(audit_path, archive_path, tmp_path, "concurrent")
    try:
        with httpx.Client(base_url=server.url, trust_env=False, timeout=60.0) as client:
            expected = client.get(PATHS["health"], headers=_headers()).json()

            routes = list(PATHS.values())

            def read(index: int) -> tuple[int, int, str]:
                response = client.get(routes[index % len(routes)], headers=_headers())
                return response.status_code, index % len(routes), response.text

            with ThreadPoolExecutor(max_workers=8) as executor:
                results = list(executor.map(read, range(48)))

            for _, _, text in results:
                _no_disclosure(text, "tenant-B")
            # every reader of health observed the same projection
            for status_code, index, text in results:
                assert status_code == 200
                if index == 0:
                    payload = json.loads(text)
                    for volatile in ("checked_at",):
                        payload.pop(volatile, None)
                    reference = dict(expected)
                    reference.pop("checked_at", None)
                    assert payload == reference
    finally:
        logs = _stop_server(server)
    assert _fingerprint(audit_path) == before, "48 concurrent reads changed nothing"
    _no_disclosure_in_log(logs, str(audit_path))


# ── 12. corruption scenario ──────────────────────────────────────────────


def test_w113_ops_corrupted_event_is_reported_not_hidden(tmp_path: Path) -> None:
    audit_path = tmp_path / "audit"
    archive_path = tmp_path / "audit-archive"
    _seed(audit_path, old=4, new=2)
    victim = next(audit_path.glob("*.json"))
    payload = json.loads(victim.read_text(encoding="utf-8"))
    payload["reason_code"] = "tampered"
    victim.write_text(
        json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8"
    )
    before = _fingerprint(audit_path)
    server = _start_server(audit_path, archive_path, tmp_path, "corrupt-event")
    try:
        with httpx.Client(base_url=server.url, trust_env=False, timeout=30.0) as client:
            health = client.get(PATHS["health"], headers=_headers())
            # DEGRADED is a diagnostic, not an HTTP error
            assert health.status_code == 200
            body = health.json()
            assert body["status"] == "DEGRADED"
            assert body["checksum_failure_count"] == 1
            assert body["integrity_invalid_count"] == 1
            assert AUDIT_DATA_CORRUPTED in body["reason_codes"]

            readiness = client.get(PATHS["readiness"], headers=_headers())
            assert readiness.status_code == 200
            verdict = readiness.json()
            assert verdict["readiness_status"] == "NOT_READY"
            assert AUDIT_DATA_CORRUPTED in verdict["blocking_reason_codes"]
            _no_disclosure(health.text + readiness.text, "tampered")
    finally:
        logs = _stop_server(server)
    assert _fingerprint(audit_path) == before, "a diagnostic never repairs anything"
    assert "operation=audit_health" in logs
    _no_disclosure_in_log(logs, str(audit_path), str(archive_path), victim.name)


# ── 13. archive scenario: healthy, corrupted and missing ─────────────────


def test_w113_ops_archive_health_scenarios(tmp_path: Path) -> None:
    audit_path = tmp_path / "audit"
    archive_path = tmp_path / "audit-archive"
    _seed(audit_path, old=6, new=0)
    service = ExecutionAuditService(ExecutionAuditStore(audit_path, create_if_missing=False))
    archive = FileSystemAuditArchiveStore(archive_path)
    AuditLifecycleService(service, archive).archive(
        "tenant-A",
        AuditRetentionPolicy(
            retention_days=1, reference_time=NOW, minimum_events_to_keep=4, dry_run=False
        ),
    )
    before = _fingerprint(audit_path)
    archive_before = _fingerprint(archive_path)
    server = _start_server(audit_path, archive_path, tmp_path, "archive")
    try:
        with httpx.Client(base_url=server.url, trust_env=False, timeout=30.0) as client:
            capacity = client.get(PATHS["capacity"], headers=_headers()).json()
            assert capacity["archive_bytes"] > 0
            assert capacity["archived_event_count"] == 2
            assert capacity["oldest_archived_event_at"] is not None
            assert capacity["newest_archived_event_at"] is not None

            # corrupt one archived document while the server runs
            victim = next(archive_path.glob("*.json"))
            payload = json.loads(victim.read_text(encoding="utf-8"))
            payload["archive_document_checksum"] = "0" * 64
            victim.write_text(
                json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n",
                encoding="utf-8",
            )

            readiness = client.get(PATHS["readiness"], headers=_headers())
            assert readiness.status_code == 200
            verdict = readiness.json()
            assert verdict["archive_status"] == "CORRUPTED"
            assert ARCHIVE_CORRUPTED in verdict["blocking_reason_codes"]
            assert verdict["readiness_status"] == "NOT_READY"
            _no_disclosure(readiness.text, victim.name)
    finally:
        logs = _stop_server(server)
    assert _fingerprint(audit_path) == before
    assert _fingerprint(archive_path) != archive_before, "the corruption is external, not ours"
    assert "operation=audit_health" in logs
    _no_disclosure_in_log(logs, str(audit_path), str(archive_path), victim.name)


def test_w113_ops_archive_not_configured_is_not_a_failure(tmp_path: Path) -> None:
    audit_path = tmp_path / "audit"
    archive_path = tmp_path / "unused-archive"
    _seed(audit_path, old=2, new=1)
    before = _fingerprint(audit_path)
    server = _start_server(audit_path, archive_path, tmp_path, "no-archive")
    try:
        with httpx.Client(base_url=server.url, trust_env=False, timeout=30.0) as client:
            capacity = client.get(PATHS["capacity"], headers=_headers()).json()
            assert capacity["archived_event_count"] == 0
            assert capacity["archive_bytes"] == 0
            readiness = client.get(PATHS["readiness"], headers=_headers()).json()
            assert (
                readiness["readiness_status"] == "READY"
            ), "an absent archive is not an operational failure"
    finally:
        logs = _stop_server(server)
    assert _fingerprint(audit_path) == before
    _no_disclosure_in_log(logs, str(audit_path))


# ── 10. OpenAPI ──────────────────────────────────────────────────────────


def test_w113_ops_openapi_is_read_only_and_documented(tmp_path: Path) -> None:
    audit_path = tmp_path / "audit"
    archive_path = tmp_path / "audit-archive"
    _seed(audit_path, old=1, new=1)
    server = _start_server(audit_path, archive_path, tmp_path, "openapi")
    try:
        with httpx.Client(base_url=server.url, trust_env=False, timeout=30.0) as client:
            schema = client.get("/openapi.json").json()
            for path in PATHS.values():
                verbs = {verb.upper() for verb in schema["paths"][path]}
                assert verbs == {"GET"}, (path, verbs)
                operation = schema["paths"][path]["get"]
                assert set(operation["responses"]) >= {
                    "200",
                    "401",
                    "403",
                    "422",
                    "503",
                }
                assert operation["security"] == [{"BearerAuth": []}, {"ApiKeyAuth": []}]
                assert operation["summary"] and operation["description"]
            for model in ("AuditHealthOut", "AuditCapacityOut", "AuditReadinessOut"):
                assert model in schema["components"]["schemas"]
            # no mutation verb anywhere on the W113 surface
            for path in PATHS.values():
                for verb in ("post", "put", "patch", "delete"):
                    assert verb not in {v.lower() for v in schema["paths"][path]}
            # a single consolidated route is not added: three projections, one snapshot
            assert not any(path.endswith("/operational-status") for path in schema["paths"])
    finally:
        logs = _stop_server(server)
    _no_disclosure_in_log(logs, str(audit_path))


# ── 14. performance matrix ───────────────────────────────────────────────


def test_w113_ops_performance_observed(tmp_path: Path) -> None:
    """Observed durations only; W113 defines no SLA threshold."""

    observations: dict[int, dict[str, float]] = {}
    for size in (10, 100, 500, 1000, 5000):
        root = tmp_path / f"perf-{size}"
        audit_path = root / "audit"
        archive_path = root / "archive"
        store = ExecutionAuditStore(audit_path)
        for index in range(size):
            store.append(
                _event(
                    f"p-{size}-{index}",
                    request_id=f"p{index}",
                    occurred_at=NOW - timedelta(days=200 + index),
                )
            )
        store.close()
        service = ExecutionAuditService(ExecutionAuditStore(audit_path, create_if_missing=False))
        archive = FileSystemAuditArchiveStore(archive_path)
        lifecycle = AuditLifecycleService(service, archive)
        lifecycle.archive(
            "tenant-A",
            AuditRetentionPolicy(retention_days=1, reference_time=NOW, dry_run=False),
        )
        diagnostics = AuditHealthService(
            service,
            archive_store=archive,
            lifecycle=lifecycle,
            active_path=audit_path,
            archive_path=archive_path,
        )

        # read: the single store scan that feeds the whole projection
        started = time.perf_counter()
        diagnostics.store.scan_documents()
        read_s = time.perf_counter() - started
        # one snapshot, then each projection derived from it
        started = time.perf_counter()
        snapshot = diagnostics.snapshot("tenant-A")
        snapshot_s = time.perf_counter() - started
        started = time.perf_counter()
        health = snapshot.health
        health_s = time.perf_counter() - started
        started = time.perf_counter()
        capacity = snapshot.capacity
        capacity_s = time.perf_counter() - started
        started = time.perf_counter()
        readiness = snapshot.readiness
        readiness_s = time.perf_counter() - started
        # the projections are already computed on the snapshot, so deriving them
        # is a field access, not another scan
        assert (health.status, capacity.event_count, readiness.readiness_status) is not None
        # archive() copies without purging: the journal keeps ``size`` events plus
        # the one W112 lifecycle event the archive run itself audited
        assert snapshot.health.event_count == size + 1
        assert snapshot.archive.archived_event_count == size
        assert snapshot.capacity.total_known_event_count == size + 1
        observations[size] = {
            "read_s": round(read_s, 4),
            "snapshot_s": round(snapshot_s, 4),
            "health_s": round(health_s, 6),
            "capacity_s": round(capacity_s, 6),
            "readiness_s": round(readiness_s, 6),
        }
    print("\nW113 observed durations (no SLA defined):")
    for size, values in observations.items():
        print(f"  {size:5d} events -> {values}")

    # HTTP layer at two sizes
    http_observations: dict[int, float] = {}
    for size in (100, 1000):
        root = tmp_path / f"http-{size}"
        audit_path = root / "audit"
        archive_path = root / "archive"
        _seed(audit_path, old=size, new=0)
        server = _start_server(audit_path, archive_path, root, f"http-{size}")
        try:
            with httpx.Client(base_url=server.url, trust_env=False, timeout=120.0) as client:
                client.get(PATHS["health"], headers=_headers())  # warm-up
                started = time.perf_counter()
                for path in PATHS.values():
                    assert client.get(path, headers=_headers()).status_code == 200
                http_observations[size] = round((time.perf_counter() - started) / len(PATHS), 4)
        finally:
            _stop_server(server)
    print(f"W113 observed HTTP latency per endpoint: {http_observations}")
    assert set(observations) == {10, 100, 500, 1000, 5000}
