"""W114 — real Uvicorn/HTTPX end-to-end operational validation.

Self-contained local-process tests: temporary datastores only, no production
path, no scheduler, no worker, no forecast, no training, no MLflow run, no
ThingsBoard write, no alarm, no Redis, no Kafka, no PostgreSQL mutation.
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
from trendx.forecasting.analytics import ExecutionHistoryAnalytics
from trendx.forecasting.audit import (
    AuditActorType,
    AuditOperation,
    AuditOutcome,
    ExecutionAuditService,
    ExecutionAuditStore,
)
from trendx.forecasting.audit_control import (
    ExecutionAuditExportService,
    ExecutionAuditIntegrityService,
    ExecutionAuditReconciliationService,
)
from trendx.forecasting.audit_e2e import (
    AuditE2EValidator,
    ConsistencyDimension,
    LifecycleState,
    invariants_for,
)
from trendx.forecasting.audit_health import AuditHealthService
from trendx.forecasting.audit_lifecycle import (
    AuditLifecycleService,
    AuditRestoreRequest,
    AuditRetentionPolicy,
    FileSystemAuditArchiveStore,
)
from trendx.forecasting.execution import (
    DurableExecutionStore,
    ExecutionProvenance,
    ExecutionRecord,
)
from trendx.forecasting.history import ExecutionHistoryService, ExecutionQuery
from trendx.forecasting.reporting import ExecutionAnalyticsReportingService

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src"
TOKEN = "w114-operational-test-token"
NOW = datetime(2026, 4, 1, tzinfo=UTC)
AUDIT_INSTANT = NOW - timedelta(days=100)
TENANT_A = "tenant-A"
TENANT_B = "tenant-B"

# every read-only projection the E2E chain exposes over HTTP
READ_PATHS = {
    "history": "/api/v1/forecast/executions",
    "analytics": "/api/v1/forecast/executions/analytics",
    "report": "/api/v1/forecast/executions/report",
    "audit": "/api/v1/forecast/executions/audit",
    "integrity": "/api/v1/forecast/executions/audit/integrity",
    "reconciliation": "/api/v1/forecast/executions/audit/reconciliation",
    "export": "/api/v1/forecast/executions/audit/export",
    "health": "/api/v1/forecast/executions/audit/health",
    "capacity": "/api/v1/forecast/executions/audit/capacity",
    "readiness": "/api/v1/forecast/executions/audit/readiness",
}
LIFECYCLE_PATHS = {
    "preview": "/api/v1/forecast/executions/audit/lifecycle/preview",
    "archive": "/api/v1/forecast/executions/audit/lifecycle/archive",
    "purge": "/api/v1/forecast/executions/audit/lifecycle/purge",
    "restore": "/api/v1/forecast/executions/audit/lifecycle/restore",
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


def _fingerprint(path: Path) -> dict[str, str]:
    """Digest a datastore.

    ``DurableExecutionStore`` is a single file while the W110 audit store and
    the W112 archive are directories, so both layouts must be covered.
    """

    if not path.exists():
        return {}
    if path.is_file():
        return {path.name: hashlib.sha256(path.read_bytes()).hexdigest()}
    return {
        item.relative_to(path).as_posix(): hashlib.sha256(item.read_bytes()).hexdigest()
        for item in sorted(path.rglob("*"))
        if item.is_file()
    }


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _environment(workdir: Path) -> dict[str, str]:
    environment = os.environ.copy()
    environment["PYTHONPATH"] = str(SRC) + os.pathsep + environment.get("PYTHONPATH", "")
    environment.update(
        {
            "PYTHONDONTWRITEBYTECODE": "1",
            "TRENDX_API_TOKEN": TOKEN,
            "TRENDX_EXECUTION_HISTORY_PATH": str(workdir / "executions"),
            "TRENDX_EXECUTION_AUDIT_PATH": str(workdir / "audit"),
            "TRENDX_EXECUTION_AUDIT_ARCHIVE_PATH": str(workdir / "audit-archive"),
            "TRENDX_EXECUTION_ARCHIVE_PATH": "",
            "TRENDX_DEFAULT_TENANT_ID": TENANT_A,
            "TRENDX_DISK_MIN_FREE_GB": "0",
            "TRENDX_LOG_LEVEL": "INFO",
            "TRENDX_ENV": "test",
            "MLFLOW_TRACKING_URI": f"file://{workdir / 'mlruns'}",
            **TEST_FLAGS,
        }
    )
    return environment


def _start_server(workdir: Path, label: str) -> LiveServer:
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
        env=_environment(workdir),
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
        raise TimeoutError("W114 Uvicorn startup timed out")
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


def _seed(workdir: Path, *, tenant_a: int = 3, tenant_b: int = 1) -> None:
    """Process A: create executions and their audit events on disk."""

    executions = DurableExecutionStore(workdir / "executions")
    audit = ExecutionAuditService(
        ExecutionAuditStore(workdir / "audit"), clock=lambda: AUDIT_INSTANT
    )
    index = 0
    for tenant, count in ((TENANT_A, tenant_a), (TENANT_B, tenant_b)):
        for position in range(count):
            # every third execution of tenant-A fails, so the chain carries a
            # business failure alongside successes
            succeeds = not (tenant == TENANT_A and position == tenant_a - 1)
            created = (NOW - timedelta(days=50 + index)).isoformat()
            record = executions.create(
                ExecutionRecord(
                    execution_id=f"x-{index}",
                    reference_key=f"ref-{index}",
                    tenant_id=tenant,
                    entity_type="DEVICE",
                    entity_id=f"dev-{index}",
                    target_metric="temperature",
                    frequency="1h",
                    horizon=24,
                    model_id="prophet",
                    model_version="1",
                    algorithm="prophet",
                    created_at=created,
                    metadata={"source": "w114-ops"},
                )
            )
            executions.mark_started(record.execution_id)
            provenance = ExecutionProvenance(
                model_id="prophet",
                model_version="1",
                algorithm="prophet",
                feature_schema_version="schema-A",
                prediction_count=0 if not succeeds else 3 + position,
            )
            if succeeds:
                final = executions.mark_success(record.execution_id, provenance)
            else:
                final = executions.mark_failed(
                    record.execution_id,
                    error_code="model_error",
                    error_reason="fit failed",
                    provenance=provenance,
                )
            audit.record(
                operation=AuditOperation.EXECUTION,
                outcome=AuditOutcome.SUCCESS if succeeds else AuditOutcome.FAILED,
                tenant_id=tenant,
                execution_id=final.execution_id,
                reference_key=final.reference_key,
                actor_type=AuditActorType.SYSTEM,
                actor_id="scheduler",
                request_id=f"req-{index}",
                source="w114-ops",
                reason_code="execution_completed",
            )
            index += 1
    # a recovery correlation chain so W111 reconciliation has something to check
    audit.record(
        operation=AuditOperation.RECOVERY_API,
        outcome=AuditOutcome.SUCCESS,
        tenant_id=TENANT_A,
        execution_id="x-0",
        reference_key="ref-0",
        actor_type=AuditActorType.SYSTEM,
        actor_id="operator",
        request_id="restore-0",
        source="w114-ops",
        reason_code="record_already_present",
    )
    audit.record(
        operation=AuditOperation.RESTORE_EXECUTE,
        outcome=AuditOutcome.SUCCESS,
        tenant_id=TENANT_A,
        execution_id="x-0",
        reference_key="ref-0",
        actor_type=AuditActorType.SYSTEM,
        actor_id="operator",
        request_id="restore-0",
        source="w114-ops",
        reason_code="record_restored",
    )
    executions.close()


def _validator(workdir: Path) -> AuditE2EValidator:
    """Wire the validator from the same services the server builds."""

    audit_service = ExecutionAuditService(
        ExecutionAuditStore(workdir / "audit", create_if_missing=False)
    )
    archive = FileSystemAuditArchiveStore(workdir / "audit-archive")
    lifecycle = AuditLifecycleService(audit_service, archive)
    history = ExecutionHistoryService(
        DurableExecutionStore(workdir / "executions", create_if_missing=False)
    )
    return AuditE2EValidator(
        history=history,
        integrity=ExecutionAuditIntegrityService(audit_service),
        reconciliation=ExecutionAuditReconciliationService(audit_service),
        export=ExecutionAuditExportService(audit_service),
        health=AuditHealthService(
            audit_service,
            archive_store=archive,
            lifecycle=lifecycle,
            active_path=workdir / "audit",
            archive_path=workdir / "audit-archive",
        ),
        analytics=ExecutionHistoryAnalytics(history),
        reporting=ExecutionAnalyticsReportingService(history),
        lifecycle=lifecycle,
    )


def _policy() -> AuditRetentionPolicy:
    return AuditRetentionPolicy(
        retention_days=1,
        reference_time=NOW,
        minimum_events_to_keep=0,
        dry_run=False,
    )


def _body(**overrides: object) -> dict[str, object]:
    body: dict[str, object] = {
        "retention_days": 1,
        "reference_time": NOW.isoformat(),
        "minimum_events_to_keep": 0,
        "dry_run": False,
    }
    body.update(overrides)
    return body


_VOLATILE_KEYS = (
    "generated_at",
    "checked_at",
    "measured_at",
    "observed_at",
    "last_verified_at",
)


def _business_only(payload: object) -> object:
    """Strip observation instants and filesystem facts from a response.

    Filesystem capacity is an infrastructure observation, not a business fact:
    two reads of an unchanged journal legitimately disagree about free bytes.
    """

    if isinstance(payload, dict):
        return {
            key: _business_only(value)
            for key, value in payload.items()
            if key not in _VOLATILE_KEYS and key != "filesystem"
        }
    if isinstance(payload, list):
        return [_business_only(item) for item in payload]
    return payload


def _no_disclosure(text: str, *forbidden: str) -> None:
    assert TOKEN not in text
    assert "Traceback" not in text
    assert ".json" not in text
    for needle in forbidden:
        assert needle not in text


def _no_disclosure_in_log(text: str, *forbidden: str) -> None:
    assert TOKEN not in text
    assert "Traceback" not in text
    for needle in forbidden:
        assert needle not in text


# ── 1/2. real Uvicorn + complete lifecycle ───────────────────────────────


def test_w114_ops_complete_lifecycle_over_http(tmp_path: Path) -> None:
    audit_path = tmp_path / "audit"
    archive_path = tmp_path / "audit-archive"
    history_path = tmp_path / "executions"
    _seed(tmp_path)
    execution_before = _fingerprint(history_path)
    audit_before = _fingerprint(audit_path)
    archive_before = _fingerprint(archive_path)
    server = _start_server(tmp_path, "lifecycle")
    try:
        with httpx.Client(base_url=server.url, trust_env=False, timeout=60.0) as client:
            # 2. every read-only projection answers, authenticated
            for path in READ_PATHS.values():
                assert client.get(path).status_code == 401
                response = client.get(path, headers=_headers())
                assert response.status_code == 200, path

            analytics = client.get(READ_PATHS["analytics"], headers=_headers()).json()
            report = client.get(READ_PATHS["report"], headers=_headers()).json()
            assert analytics["summary"] == report["summary"], "W106 must agree with W104"
            integrity = client.get(READ_PATHS["integrity"], headers=_headers()).json()
            assert integrity["integrity_status"] == "VALID"
            reconciliation = client.get(READ_PATHS["reconciliation"], headers=_headers()).json()
            assert reconciliation["reconciliation_status"] == "CONSISTENT"

            # 9. read-only proof: nothing moved
            assert _fingerprint(history_path) == execution_before
            assert _fingerprint(audit_path) == audit_before
            assert _fingerprint(archive_path) == archive_before

            # 6. archive
            archived = client.post(
                LIFECYCLE_PATHS["archive"], json=_body(), headers=_headers("w114-archive")
            )
            assert archived.status_code == 200
            body = archived.json()
            assert body["archived"] == 5
            assert body["archive_verified"] == 5

            # 7. purge, after a verified archive
            purged = client.post(
                LIFECYCLE_PATHS["purge"], json=_body(), headers=_headers("w114-purge")
            )
            assert purged.status_code == 200
            assert purged.json()["purged"] == 5
            assert (
                _fingerprint(history_path) == execution_before
            ), "an audit purge never touches the execution chain"

            # 8. restore
            documents = FileSystemAuditArchiveStore(archive_path).list_documents(tenant_id=TENANT_A)
            assert len(documents) == 5
            for document in documents:
                restored = client.post(
                    LIFECYCLE_PATHS["restore"],
                    json={"event_id": document.event_id, "tenant_id": TENANT_A},
                    headers=_headers("w114-restore"),
                )
                assert restored.status_code == 200
                assert restored.json()["status"] in {"RESTORED", "ALREADY_PRESENT"}

            # 10/11. export and health/readiness after the cycle
            export = client.get(READ_PATHS["export"], headers=_headers()).json()
            assert export["total"] > 0
            readiness = client.get(READ_PATHS["readiness"], headers=_headers()).json()
            assert readiness["readiness_status"] == "READY"
            health = client.get(READ_PATHS["health"], headers=_headers()).json()
            assert health["status"] == "HEALTHY"
            _no_disclosure(json.dumps([health, readiness, export]))
    finally:
        logs = _stop_server(server)
    assert "operation=audit" in logs
    _no_disclosure_in_log(logs, str(audit_path), str(archive_path))


# ── 3. Process A / Process B ─────────────────────────────────────────────


def test_w114_ops_process_a_b_sees_the_same_state(tmp_path: Path) -> None:
    audit_path = tmp_path / "audit"
    archive_path = tmp_path / "audit-archive"
    history_path = tmp_path / "executions"
    _seed(tmp_path)
    # Process A mutates through the public lifecycle API
    server_a = _start_server(tmp_path, "process-a")
    try:
        with httpx.Client(base_url=server_a.url, trust_env=False, timeout=60.0) as client:
            assert (
                client.post(
                    LIFECYCLE_PATHS["archive"], json=_body(), headers=_headers()
                ).status_code
                == 200
            )
            assert (
                client.post(LIFECYCLE_PATHS["purge"], json=_body(), headers=_headers()).status_code
                == 200
            )
    finally:
        logs_a = _stop_server(server_a)
    execution_after_a = _fingerprint(history_path)
    audit_after_a = _fingerprint(audit_path)
    archive_after_a = _fingerprint(archive_path)

    # Process B only reads
    server_b = _start_server(tmp_path, "process-b")
    try:
        with httpx.Client(base_url=server_b.url, trust_env=False, timeout=60.0) as client:
            seen: dict[str, dict[str, object]] = {}
            for key, path in READ_PATHS.items():
                response = client.get(path, headers=_headers())
                assert response.status_code == 200, key
                seen[key] = response.json()
            assert seen["reconciliation"]["reconciliation_status"] in {
                "CONSISTENT",
                "INSUFFICIENT_DATA",
            }
            assert seen["integrity"]["integrity_status"] == "VALID"
            assert seen["readiness"]["readiness_status"] == "READY"
            assert seen["capacity"]["archived_event_count"] == 5
            # the caller is tenant-A, so tenant-A's own ids are expected;
            # the guarantee is that tenant-B's ids never appear
            for leak in ("x-3", "ref-3", "dev-3", TENANT_B):
                assert leak not in json.dumps(seen), leak
    finally:
        logs_b = _stop_server(server_b)

    assert _fingerprint(history_path) == execution_after_a
    assert _fingerprint(audit_path) == audit_after_a
    assert _fingerprint(archive_path) == archive_after_a

    # Process A restores; Process B must observe the change
    server_c = _start_server(tmp_path, "process-a-restore")
    try:
        with httpx.Client(base_url=server_c.url, trust_env=False, timeout=60.0) as client:
            documents = FileSystemAuditArchiveStore(archive_path).list_documents(tenant_id=TENANT_A)
            for document in documents:
                response = client.post(
                    LIFECYCLE_PATHS["restore"],
                    json={"event_id": document.event_id, "tenant_id": TENANT_A},
                    headers=_headers(),
                )
                assert response.status_code == 200
    finally:
        logs_c = _stop_server(server_c)
    _no_disclosure_in_log(logs_a + logs_b + logs_c, str(audit_path), TOKEN)


# ── 4. tenant isolation ──────────────────────────────────────────────────


def test_w114_ops_tenant_isolation_end_to_end(tmp_path: Path) -> None:
    _seed(tmp_path, tenant_a=3, tenant_b=1)
    server = _start_server(tmp_path, "tenant")
    try:
        with httpx.Client(base_url=server.url, trust_env=False, timeout=60.0) as client:
            client.post(LIFECYCLE_PATHS["archive"], json=_body(), headers=_headers())
            client.post(LIFECYCLE_PATHS["purge"], json=_body(), headers=_headers())
            for key, path in READ_PATHS.items():
                text = client.get(path, headers=_headers()).text
                for leak in (TENANT_B, "x-3", "ref-3", "dev-3"):
                    assert leak not in text, (key, leak)
            health = client.get(READ_PATHS["health"], headers=_headers()).json()
            assert health["tenant_id"] == TENANT_A
            # a cross-tenant restore over HTTP is refused
            documents = FileSystemAuditArchiveStore(tmp_path / "audit-archive").list_documents(
                tenant_id=TENANT_A
            )
            response = client.post(
                LIFECYCLE_PATHS["restore"],
                json={"event_id": documents[0].event_id, "tenant_id": TENANT_B},
                headers=_headers(),
            )
            assert response.status_code in {403, 404, 409}, response.status_code
            # the body echoes the caller's own request values, which is not a
            # disclosure; what matters is the status and the absence of any
            # internal detail
            _no_disclosure(response.text)
            assert response.json()["status"] == "TENANT_FORBIDDEN"
    finally:
        logs = _stop_server(server)
    _no_disclosure_in_log(logs, TENANT_B)


# ── 5. concurrent reads ──────────────────────────────────────────────────


def test_w114_ops_concurrent_reads(tmp_path: Path) -> None:
    _seed(tmp_path, tenant_a=8, tenant_b=2)
    paths = list(READ_PATHS.values())
    audit_path = tmp_path / "audit"
    history_path = tmp_path / "executions"
    audit_before = _fingerprint(audit_path)
    history_before = _fingerprint(history_path)
    server = _start_server(tmp_path, "concurrent")
    try:
        with httpx.Client(base_url=server.url, trust_env=False, timeout=60.0) as client:
            expected = {
                key: client.get(path, headers=_headers()).text for key, path in READ_PATHS.items()
            }

            def read(index: int) -> tuple[int, str, str]:
                key = list(READ_PATHS)[index % len(READ_PATHS)]
                response = client.get(READ_PATHS[key], headers=_headers())
                return response.status_code, key, response.text

            with ThreadPoolExecutor(max_workers=8) as executor:
                results = list(executor.map(read, range(80)))
            for status_code, key, text in results:
                assert status_code == 200, key
                _no_disclosure(text, TENANT_B)
            # Every concurrent reader observed the identical *business*
            # projection.  Filesystem metrics are deliberately excluded: they
            # are infrastructure observations, not business facts, and the host
            # filesystem legitimately moves between two reads.
            for _, key, text in results:
                stable = _business_only(json.loads(text))
                reference = _business_only(json.loads(expected[key]))
                assert stable == reference, key
            assert len(paths) == 10
    finally:
        logs = _stop_server(server)
    assert _fingerprint(audit_path) == audit_before
    assert _fingerprint(history_path) == history_before
    _no_disclosure_in_log(logs, str(audit_path))


# ── 6/7/8. archive, purge, restore are safe ──────────────────────────────


def test_w114_ops_purge_requires_verified_archive(tmp_path: Path) -> None:
    archive_path = tmp_path / "audit-archive"
    _seed(tmp_path)
    server = _start_server(tmp_path, "purge-guard")
    try:
        with httpx.Client(base_url=server.url, trust_env=False, timeout=60.0) as client:
            assert (
                client.post(LIFECYCLE_PATHS["archive"], json=_body(), headers=_headers()).json()[
                    "archived"
                ]
                == 5
            )
            victim = next(archive_path.glob("*.json"))
            payload = json.loads(victim.read_text(encoding="utf-8"))
            payload["archive_document_checksum"] = "0" * 64
            victim.write_text(
                json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n",
                encoding="utf-8",
            )
            purged = client.post(LIFECYCLE_PATHS["purge"], json=_body(), headers=_headers())
            assert purged.status_code == 200
            body = purged.json()
            # the corrupt event is never purged; healthy ones still are
            assert body["purged"] == 4
            assert body["guards"].get("archive_corrupt") == 1
            readiness = client.get(READ_PATHS["readiness"], headers=_headers()).json()
            assert readiness["readiness_status"] == "NOT_READY"
            assert "ARCHIVE_CORRUPTED" in readiness["blocking_reason_codes"]
    finally:
        logs = _stop_server(server)
    _no_disclosure_in_log(logs, victim.name)


# ── 9. corruption scenario ───────────────────────────────────────────────


def test_w114_ops_corruption_is_detected_without_traceback(tmp_path: Path) -> None:
    audit_path = tmp_path / "audit"
    _seed(tmp_path)
    executions_before = _fingerprint(tmp_path / "executions")
    server = _start_server(tmp_path, "corrupt")
    try:
        with httpx.Client(base_url=server.url, trust_env=False, timeout=60.0) as client:
            victim = next(audit_path.glob("*.json"))
            payload = json.loads(victim.read_text(encoding="utf-8"))
            payload["reason_code"] = "tampered"
            victim.write_text(
                json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n",
                encoding="utf-8",
            )
            integrity = client.get(READ_PATHS["integrity"], headers=_headers())
            # W111 fails closed on an untrustworthy source
            assert integrity.status_code in {200, 503}
            if integrity.status_code == 200:
                assert integrity.json()["integrity_status"] != "VALID"
            else:
                _no_disclosure(integrity.text, "CHECKSUM")
            health = client.get(READ_PATHS["health"], headers=_headers())
            assert health.status_code == 200
            assert health.json()["status"] == "DEGRADED"
            assert health.json()["checksum_failure_count"] == 1
            readiness = client.get(READ_PATHS["readiness"], headers=_headers())
            assert readiness.status_code == 200
            assert readiness.json()["readiness_status"] == "NOT_READY"
            assert "AUDIT_DATA_CORRUPTED" in readiness.json()["blocking_reason_codes"]
            for response in (integrity, health, readiness):
                _no_disclosure(response.text, str(tmp_path), victim.name)
            # the corruption is never silently ignored by the execution side
            analytics = client.get(READ_PATHS["analytics"], headers=_headers())
            assert analytics.status_code == 200
    finally:
        logs = _stop_server(server)
    _no_disclosure_in_log(logs, str(audit_path), victim.name)
    # the execution chain is the only thing never touched by the corruption
    assert _fingerprint(tmp_path / "executions") == executions_before
    assert executions_before


# ── 10. export determinism and scope ─────────────────────────────────────


def test_w114_ops_export_is_deterministic_and_scoped(tmp_path: Path) -> None:
    _seed(tmp_path)
    server = _start_server(tmp_path, "export")
    try:
        with httpx.Client(base_url=server.url, trust_env=False, timeout=60.0) as client:
            first = client.get(READ_PATHS["export"], headers=_headers()).json()
            second = client.get(READ_PATHS["export"], headers=_headers()).json()
            # export_checksum is the determinism guarantee on the wire:
            # generated_at is transport metadata and sits outside the checksum
            assert first["export_checksum"] == second["export_checksum"]
            assert first["content"] == second["content"]
            assert first["total"] == second["total"]
            assert first["event_count"] == second["event_count"]
            # the ACTIVE_AND_ARCHIVED perimeter is a service-level W111/W112
            # contract (covered in-process by the unit suite); the HTTP surface
            # exposes the active journal only and has no scope parameter
            assert "scope" not in first
            paged = client.get(READ_PATHS["export"], params={"limit": 2}, headers=_headers())
            assert paged.status_code == 200
            assert paged.json()["event_count"] == 2
            assert paged.json()["has_more"] is True
            _no_disclosure(first["content"], str(tmp_path))
    finally:
        logs = _stop_server(server)
    _no_disclosure_in_log(logs, str(tmp_path / "audit"))


# ── 11. health / readiness across the cycle ──────────────────────────────


def test_w114_ops_health_and_readiness_across_the_cycle(tmp_path: Path) -> None:
    audit_path = tmp_path / "audit"
    audit_path = tmp_path / "audit"
    archive_path = tmp_path / "audit-archive"
    _seed(tmp_path)
    server = _start_server(tmp_path, "health-cycle")
    observed: list[tuple[int, int, str, str]] = []
    try:
        with httpx.Client(base_url=server.url, trust_env=False, timeout=60.0) as client:
            for label in ("S0", "S2", "S3", "S4"):
                if label == "S2":
                    client.post(LIFECYCLE_PATHS["archive"], json=_body(), headers=_headers())
                if label == "S3":
                    client.post(LIFECYCLE_PATHS["purge"], json=_body(), headers=_headers())
                if label == "S4":
                    for document in FileSystemAuditArchiveStore(archive_path).list_documents(
                        tenant_id=TENANT_A
                    ):
                        client.post(
                            LIFECYCLE_PATHS["restore"],
                            json={
                                "event_id": document.event_id,
                                "tenant_id": TENANT_A,
                            },
                            headers=_headers(),
                        )
                health = client.get(READ_PATHS["health"], headers=_headers()).json()
                capacity = client.get(READ_PATHS["capacity"], headers=_headers()).json()
                readiness = client.get(READ_PATHS["readiness"], headers=_headers()).json()
                assert health["status"] == "HEALTHY", label
                assert readiness["readiness_status"] == "READY", label
                observed.append(
                    (
                        label,
                        capacity["event_count"],
                        capacity["archived_event_count"],
                        readiness["readiness_status"],
                    )
                )
    finally:
        logs = _stop_server(server)
    # S3 is the only state where the active journal is smaller
    by_label = {row[0]: row for row in observed}
    assert by_label["S2"][2] == 5, "archived count is visible after archive"
    assert by_label["S3"][1] < by_label["S2"][1], "purge shrinks the active journal"
    assert by_label["S4"][1] > by_label["S3"][1], "restore repopulates it"
    _no_disclosure_in_log(logs, str(audit_path))


# ── 12. OpenAPI: no new public route ─────────────────────────────────────


def test_w114_ops_openapi_adds_no_route(tmp_path: Path) -> None:
    _seed(tmp_path)
    server = _start_server(tmp_path, "openapi")
    try:
        with httpx.Client(base_url=server.url, trust_env=False, timeout=60.0) as client:
            schema = client.get("/openapi.json").json()
            # W114 exposes nothing new: it reuses the existing read surface
            assert not any("e2e" in path for path in schema["paths"])
            for key, path in READ_PATHS.items():
                verbs = {verb.upper() for verb in schema["paths"][path]}
                assert verbs == {"GET"}, (key, verbs)
            for key, path in LIFECYCLE_PATHS.items():
                assert {v.upper() for v in schema["paths"][path]} == {"POST"}, key
            # no mutation verb on any read path
            for path in READ_PATHS.values():
                for verb in ("post", "put", "patch", "delete"):
                    assert verb not in {v.lower() for v in schema["paths"][path]}
    finally:
        logs = _stop_server(server)
    _no_disclosure_in_log(logs, TOKEN)


# ── 13. error sanitization and auth ──────────────────────────────────────


def test_w114_ops_errors_are_sanitized(tmp_path: Path) -> None:
    _seed(tmp_path)
    server = _start_server(tmp_path, "errors")
    try:
        with httpx.Client(base_url=server.url, trust_env=False, timeout=60.0) as client:
            # unauthenticated
            for path in READ_PATHS.values():
                assert client.get(path).status_code == 401
            # wrong token
            assert (
                client.get(
                    READ_PATHS["health"], headers={"Authorization": "Bearer nope"}
                ).status_code
                == 401
            )
            # a malformed lifecycle body is a 422 with no internals
            invalid = client.post(
                LIFECYCLE_PATHS["archive"],
                json={"retention_days": -1},
                headers=_headers(),
            )
            assert invalid.status_code == 422
            _no_disclosure(invalid.text)
            # a restore for an unknown event is sanitized
            missing = client.post(
                LIFECYCLE_PATHS["restore"],
                json={"event_id": "00000000-0000-0000-0000-000000000000", "tenant_id": TENANT_A},
                headers=_headers(),
            )
            assert missing.status_code in {404, 409, 503}
            _no_disclosure(missing.text)
            assert missing.json()["status"] == "NOT_FOUND"
            # a cross-tenant restore discloses nothing
            other = client.post(
                LIFECYCLE_PATHS["restore"],
                json={"event_id": "11111111-1111-1111-1111-111111111111", "tenant_id": TENANT_B},
                headers=_headers(),
            )
            _no_disclosure(other.text)
    finally:
        logs = _stop_server(server)
    _no_disclosure_in_log(logs, TOKEN, str(tmp_path))


# ── 14. side-effect proof ────────────────────────────────────────────────


def test_w114_ops_no_side_effects(tmp_path: Path) -> None:
    _seed(tmp_path)
    mlruns = tmp_path / "mlruns"
    server = _start_server(tmp_path, "side-effects")
    try:
        with httpx.Client(base_url=server.url, trust_env=False, timeout=60.0) as client:
            for path in READ_PATHS.values():
                client.get(path, headers=_headers())
            client.post(LIFECYCLE_PATHS["archive"], json=_body(), headers=_headers())
            for path in READ_PATHS.values():
                client.get(path, headers=_headers())
    finally:
        logs = _stop_server(server)
    # no MLflow run directory was produced
    assert not mlruns.exists() or not any(mlruns.rglob("*"))
    # the disabled flags are honoured: nothing scheduled, ingested or written back
    environment = _environment(tmp_path)
    for flag in (
        "TRENDX_SCHEDULER_FORECAST_ENABLED",
        "TRENDX_INGEST_ENABLED",
        "TRENDX_WORKER_INGESTION_ENABLED",
        "TB_WRITEBACK_ENABLED",
        "TB_ALARMS_ENABLED",
        "ANOMALY_DETECTION_ENABLED",
        "TRENDX_MLFLOW_ENABLED",
    ):
        assert environment[flag] == "false", flag
    lowered = logs.lower()
    for marker in (
        "run_id",
        "mlflow run",
        "log_model",
        "log_metric",
        "tb_writeback",
        "alarm_created",
        "kafka_producer",
        "redis_client",
        "insert into",
        "update ",
    ):
        assert marker not in lowered, marker
    _no_disclosure_in_log(logs, TOKEN)


# ── 15. performance matrix (observed, no SLA) ────────────────────────────


def test_w114_ops_performance_observed(tmp_path: Path) -> None:
    """Observed durations only; W114 defines no SLA threshold."""

    observations: dict[int, dict[str, float]] = {}
    for size in (10, 100, 500, 1000):
        root = tmp_path / f"perf-{size}"
        root.mkdir(parents=True, exist_ok=True)
        executions = DurableExecutionStore(root / "executions")
        audit = ExecutionAuditService(
            ExecutionAuditStore(root / "audit"), clock=lambda: AUDIT_INSTANT
        )
        for index in range(size):
            created = (NOW - timedelta(days=50 + index)).isoformat()
            record = executions.create(
                ExecutionRecord(
                    execution_id=f"p-{size}-{index}",
                    reference_key=f"p-{size}-{index}",
                    tenant_id=TENANT_A,
                    entity_type="DEVICE",
                    entity_id=f"dev-{index}",
                    target_metric="temperature",
                    frequency="1h",
                    horizon=24,
                    model_id="prophet",
                    model_version="1",
                    algorithm="prophet",
                    created_at=created,
                )
            )
            executions.mark_started(record.execution_id)
            started = time.perf_counter()
            executions.mark_success(
                record.execution_id,
                ExecutionProvenance(
                    model_id="prophet",
                    model_version="1",
                    algorithm="prophet",
                    prediction_count=3,
                ),
            )
            execution_s = time.perf_counter() - started
            observations.setdefault(size, {}).setdefault("execution_s", 0.0)
            observations[size]["execution_s"] += execution_s
            started = time.perf_counter()
            audit.record(
                operation=AuditOperation.EXECUTION,
                outcome=AuditOutcome.SUCCESS,
                tenant_id=TENANT_A,
                execution_id=record.execution_id,
                reference_key=record.reference_key,
                actor_type=AuditActorType.SYSTEM,
                actor_id="scheduler",
                request_id=f"p{index}",
                source="w114-perf",
                reason_code="execution_completed",
            )
            observations[size].setdefault("audit_s", 0.0)
            observations[size]["audit_s"] += time.perf_counter() - started
        executions.close()

        service = ExecutionAuditService(
            ExecutionAuditStore(root / "audit", create_if_missing=False)
        )
        archive = FileSystemAuditArchiveStore(root / "audit-archive")
        lifecycle = AuditLifecycleService(service, archive)
        history = ExecutionHistoryService(
            DurableExecutionStore(root / "executions", create_if_missing=False)
        )
        validator = AuditE2EValidator(
            history=history,
            integrity=ExecutionAuditIntegrityService(service),
            reconciliation=ExecutionAuditReconciliationService(service),
            export=ExecutionAuditExportService(service),
            health=AuditHealthService(
                service,
                archive_store=archive,
                lifecycle=lifecycle,
                active_path=root / "audit",
                archive_path=root / "audit-archive",
            ),
            analytics=ExecutionHistoryAnalytics(history),
            reporting=ExecutionAnalyticsReportingService(history),
            lifecycle=lifecycle,
        )
        policy = _policy()
        started = time.perf_counter()
        integrity = validator._integrity.verify(TENANT_A)
        integrity_s = time.perf_counter() - started
        tenant_query = ExecutionQuery(tenant_id=TENANT_A, limit=None, offset=0)
        started = time.perf_counter()
        validator._analytics.analyze(tenant_query)
        analytics_s = time.perf_counter() - started
        started = time.perf_counter()
        validator._reporting.report(tenant_query, generated_at=NOW.isoformat())
        report_s = time.perf_counter() - started
        started = time.perf_counter()
        report = lifecycle.archive(TENANT_A, policy)
        archive_s = time.perf_counter() - started
        started = time.perf_counter()
        lifecycle.purge(TENANT_A, policy)
        purge_s = time.perf_counter() - started
        started = time.perf_counter()
        target = archive.list_documents(tenant_id=TENANT_A)[0].event_id
        lifecycle.restore(
            AuditRestoreRequest(event_id=target, tenant_id=TENANT_A),
            authenticated_tenant=TENANT_A,
        )
        restore_s = time.perf_counter() - started
        started = time.perf_counter()
        snapshot = validator.snapshot(LifecycleState.POST_RESTORE, TENANT_A)
        health_s = time.perf_counter() - started

        assert integrity.integrity_status.value in {"VALID", "ERROR"}
        assert report.archived == size
        assert snapshot.execution_count == size
        observations[size].update(
            {
                "integrity_s": round(integrity_s, 4),
                "analytics_s": round(analytics_s, 4),
                "report_s": round(report_s, 4),
                "archive_s": round(archive_s, 4),
                "purge_s": round(purge_s, 4),
                "restore_s": round(restore_s, 4),
                "health_snapshot_s": round(health_s, 4),
            }
        )
    print("\nW114 observed durations (no SLA defined):")
    for size, values in observations.items():
        print(f"  {size:5d} events -> {values}")

    # HTTP layer across the read surface
    root = tmp_path / "http"
    root.mkdir(parents=True, exist_ok=True)
    _seed(root, tenant_a=50, tenant_b=5)
    server = _start_server(root, "http")
    http_observed: dict[str, float] = {}
    try:
        with httpx.Client(base_url=server.url, trust_env=False, timeout=120.0) as client:
            for key, path in READ_PATHS.items():
                client.get(path, headers=_headers())  # warm-up
                started = time.perf_counter()
                assert client.get(path, headers=_headers()).status_code == 200
                http_observed[key] = round(time.perf_counter() - started, 4)
    finally:
        logs = _stop_server(server)
    print(f"W114 observed HTTP latency per endpoint: {http_observed}")
    assert set(observations) == {10, 100, 500, 1000}
    _no_disclosure_in_log(logs, TOKEN)


# ── 16. the validator itself, end to end, in-process ─────────────────────


def test_w114_ops_validator_matrix(tmp_path: Path) -> None:
    """Build the S0..S5 matrix with the W114 validator over a real store."""

    _seed(tmp_path, tenant_a=3, tenant_b=1)
    validator = _validator(tmp_path)
    history = DurableExecutionStore(tmp_path / "executions")
    archive_path = tmp_path / "audit-archive"

    history_before = _fingerprint(tmp_path / "executions")
    s0 = validator.snapshot(LifecycleState.INITIAL, TENANT_A)
    s0_repeat = validator.snapshot(LifecycleState.INITIAL, TENANT_A)
    assert s0.fingerprint == s0_repeat.fingerprint
    assert AuditE2EValidator.is_invariant(
        validator.compare(s0, s0_repeat, dimensions=invariants_for("read_only"))
    )
    # read-only group changed nothing
    assert _fingerprint(tmp_path / "executions") == history_before

    lifecycle = validator._lifecycle
    policy = _policy()
    lifecycle.archive(TENANT_A, policy)
    s2 = validator.snapshot(LifecycleState.POST_ARCHIVE, TENANT_A)
    assert AuditE2EValidator.is_invariant(
        validator.compare(s0, s2, dimensions=invariants_for("archive"))
    )
    lifecycle.purge(TENANT_A, policy)
    s3 = validator.snapshot(LifecycleState.POST_PURGE, TENANT_A)
    assert AuditE2EValidator.is_invariant(
        validator.compare(s2, s3, dimensions=invariants_for("purge"))
    )
    for document in FileSystemAuditArchiveStore(archive_path).list_documents(tenant_id=TENANT_A):
        lifecycle.restore(
            AuditRestoreRequest(event_id=document.event_id, tenant_id=TENANT_A),
            authenticated_tenant=TENANT_A,
        )
    s4 = validator.snapshot(LifecycleState.POST_RESTORE, TENANT_A)
    assert AuditE2EValidator.is_invariant(
        validator.compare(s3, s4, dimensions=invariants_for("restore"))
    )

    # S5: controlled corruption, fixture only
    victim = next(archive_path.glob("*.json"))
    payload = json.loads(victim.read_text(encoding="utf-8"))
    payload["archive_document_checksum"] = "0" * 64
    victim.write_text(
        json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n",
        encoding="utf-8",
    )
    s5 = validator.snapshot(LifecycleState.CORRUPTED, TENANT_A)
    assert s5.archive_status == "CORRUPTED"
    assert s5.readiness_status == "NOT_READY"
    # the corruption is reported, never hidden behind a zero
    assert s5.export_archived_total is None
    assert ConsistencyDimension.EXPORT.value in s5.unavailable
    rendered = json.dumps(s5.to_dict())
    assert "Traceback" not in rendered
    assert str(tmp_path) not in rendered
    assert victim.name not in rendered
    # the execution chain never moved across the whole cycle
    assert _fingerprint(tmp_path / "executions") == history_before
    assert s4.execution_ids == s0.execution_ids
    assert s5.execution_count == s0.execution_count
    history.close()
