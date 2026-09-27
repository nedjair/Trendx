"""W111 — audit integrity, reconciliation and controlled export unit tests."""

from __future__ import annotations

import hashlib
import json
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient
from pydantic import SecretStr
from trendx.config import settings
from trendx.forecasting.api import (
    TenantContext,
    get_execution_audit_control_service,
    get_tenant_context,
)
from trendx.forecasting.audit import (
    AuditActorType,
    AuditIntegrityError,
    AuditOperation,
    AuditOutcome,
    AuditQueryError,
    AuditStoreError,
    AuditValidationError,
    ExecutionAuditEvent,
    ExecutionAuditQuery,
    ExecutionAuditService,
    ExecutionAuditStore,
    MemoryExecutionAuditStore,
)
from trendx.forecasting.audit_control import (
    AuditControlError,
    ExecutionAuditControlService,
    ExecutionAuditExportService,
    ExecutionAuditIntegrityService,
    ExecutionAuditReconciliationService,
    ExportFormat,
    IntegrityStatus,
    ReconciliationStatus,
    assert_export_safe,
    iter_chains,
)
from trendx.main import app

NOW = datetime(2026, 4, 1, tzinfo=UTC)
ROOT = Path(__file__).resolve().parents[2]

W111_PATHS = (
    "/api/v1/forecast/executions/audit/integrity",
    "/api/v1/forecast/executions/audit/reconciliation",
    "/api/v1/forecast/executions/audit/export",
    "/api/v1/forecast/executions/audit/export/checksum",
)


def _event(
    event_id: str,
    *,
    tenant_id: str = "tenant-A",
    execution_id: str | None = "exec-A",
    operation: AuditOperation = AuditOperation.EXECUTION,
    outcome: AuditOutcome = AuditOutcome.SUCCESS,
    reason_code: str = "test_event",
    request_id: str = "request-A",
    occurred_at: datetime = NOW,
    reference_key: str | None = "ref:exec-A",
) -> ExecutionAuditEvent:
    return ExecutionAuditEvent(
        event_id=event_id,
        occurred_at=occurred_at,
        operation=operation,
        outcome=outcome,
        tenant_id=tenant_id,
        execution_id=execution_id,
        reference_key=reference_key,
        actor_type=AuditActorType.SYSTEM,
        actor_id="system",
        request_id=request_id,
        source="w111-test",
        reason_code=reason_code,
    )


def _file_service(tmp_path: Path) -> ExecutionAuditService:
    return ExecutionAuditService(ExecutionAuditStore(tmp_path / "audit"))


def _auth_headers() -> dict[str, str]:
    return {"Authorization": f"Bearer {settings.trendx_api_token.get_secret_value()}"}


def _store_fingerprint(path: Path) -> dict[str, str]:
    """Hash every audit artefact so a read-only proof is objective."""

    fingerprint: dict[str, str] = {}
    for item in sorted(path.rglob("*")):
        if item.is_file():
            fingerprint[item.name] = hashlib.sha256(item.read_bytes()).hexdigest()
    return fingerprint


def _seed_chain(store: ExecutionAuditStore) -> None:
    """Seed one coherent RECOVERY_API -> RESTORE -> ARCHIVE chain."""

    store.append(
        _event(
            "chain-api",
            operation=AuditOperation.RECOVERY_API,
            outcome=AuditOutcome.SUCCESS,
            reason_code="record_restored",
            request_id="req-chain",
        )
    )
    store.append(
        _event(
            "chain-restore",
            operation=AuditOperation.RESTORE,
            outcome=AuditOutcome.SUCCESS,
            reason_code="record_restored",
            request_id="req-chain",
            occurred_at=NOW + timedelta(seconds=1),
        )
    )
    store.append(
        _event(
            "chain-archive",
            operation=AuditOperation.ARCHIVE,
            outcome=AuditOutcome.SUCCESS,
            reason_code="archive_verified",
            request_id="req-chain",
            occurred_at=NOW + timedelta(seconds=2),
        )
    )


# ── A. Event validation and W110 reuse ───────────────────────────────────


@pytest.mark.unit
def test_w111_uses_w110_event_contract_without_change() -> None:
    event = _event("contract")
    assert event.event_version == "1"
    assert set(event.to_dict()) == {
        "event_id",
        "event_version",
        "occurred_at",
        "operation",
        "outcome",
        "tenant_id",
        "execution_id",
        "reference_key",
        "actor_type",
        "actor_id",
        "request_id",
        "source",
        "reason_code",
        "idempotency_key",
        "checksum",
    }
    event.with_checksum().verify_integrity()


@pytest.mark.unit
def test_w111_control_service_exposes_no_mutation() -> None:
    store = MemoryExecutionAuditStore()
    control = ExecutionAuditControlService(ExecutionAuditService(store))
    for forbidden in ("append", "record", "delete", "purge", "write", "update"):
        assert not hasattr(control, forbidden)
    assert not any(
        name for name in dir(type(control)) if name in {"append", "record", "delete", "purge"}
    )


@pytest.mark.unit
def test_w111_read_only_store_never_appends(tmp_path: Path) -> None:
    inner = ExecutionAuditStore(tmp_path / "audit")
    inner.append(_event("seed-1"))
    calls: list[Any] = []

    class WatchedStore:
        """Delegating proxy that records any mutation attempt."""

        def __getattr__(self, name: str) -> Any:
            attribute = getattr(inner, name)
            if name == "append":

                def tracked(event: ExecutionAuditEvent) -> ExecutionAuditEvent:
                    calls.append(event)
                    return attribute(event)

                return tracked
            return attribute

    control = ExecutionAuditControlService(ExecutionAuditService(cast_store(WatchedStore())))
    before = _store_fingerprint(tmp_path / "audit")
    ExecutionAuditIntegrityService(control).verify("tenant-A")
    ExecutionAuditReconciliationService(control).reconcile("tenant-A")
    ExecutionAuditExportService(control).export("tenant-A")
    ExecutionAuditExportService(control).checksum("tenant-A")
    after = _store_fingerprint(tmp_path / "audit")
    assert before == after
    assert calls == []


def cast_store(store: Any) -> Any:
    return store


# ── B. Integrity ─────────────────────────────────────────────────────────


@pytest.mark.unit
def test_w111_integrity_valid_store(tmp_path: Path) -> None:
    service = _file_service(tmp_path)
    store = service.store
    assert isinstance(store, ExecutionAuditStore)
    _seed_chain(store)
    report = ExecutionAuditIntegrityService(service).verify("tenant-A")
    assert report.integrity_status is IntegrityStatus.VALID
    assert report.valid_events == 3
    assert report.invalid_events == 0
    assert report.duplicate_events == 0
    assert report.first_failure is None
    assert report.report_version == "1"
    assert report.tenant_id == "tenant-A"
    assert report.generated_at.tzinfo is UTC


@pytest.mark.unit
def test_w111_integrity_empty_store(tmp_path: Path) -> None:
    report = ExecutionAuditIntegrityService(_file_service(tmp_path)).verify("tenant-A")
    assert report.integrity_status is IntegrityStatus.EMPTY
    assert report.events_scanned == 0
    assert report.valid_events == 0


@pytest.mark.unit
def test_w111_integrity_is_tenant_scoped(tmp_path: Path) -> None:
    service = _file_service(tmp_path)
    store = service.store
    assert isinstance(store, ExecutionAuditStore)
    _seed_chain(store)
    store.append(
        _event(
            "other-tenant",
            tenant_id="tenant-B",
            execution_id="exec-B",
            request_id="request-B",
            operation=AuditOperation.RESTORE,
            reason_code="record_restored",
        )
    )
    report_a = ExecutionAuditIntegrityService(service).verify("tenant-A")
    report_b = ExecutionAuditIntegrityService(service).verify("tenant-B")
    assert report_a.valid_events == 3
    assert report_b.valid_events == 1
    assert report_a.documents_scanned == 4
    assert report_b.documents_scanned == 4


@pytest.mark.unit
@pytest.mark.parametrize(
    ("mutate", "counter", "code"),
    [
        ("checksum", "checksum_failures", "CHECKSUM"),
        ("version", "version_failures", "VERSION"),
        ("malformed", "malformed_documents", "MALFORMED_JSON"),
        ("identity", "identity_failures", "IDENTITY"),
        ("schema", "schema_failures", "SCHEMA"),
        ("canonical", "canonicalization_failures", "CANONICALIZATION"),
        ("permission", "unsafe_documents", "UNSAFE_DOCUMENT"),
    ],
)
def test_w111_integrity_classifies_each_failure(
    tmp_path: Path,
    mutate: str,
    counter: str,
    code: str,
) -> None:
    service = _file_service(tmp_path)
    store = service.store
    assert isinstance(store, ExecutionAuditStore)
    store.append(_event("target"))
    path = next(store.path.glob("*.json"))
    payload = json.loads(path.read_text(encoding="utf-8"))

    if mutate == "checksum":
        payload["reason_code"] = "tampered"
        path.write_text(
            json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n",
            encoding="utf-8",
        )
    elif mutate == "version":
        payload["event_version"] = "999"
        path.write_text(
            json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n",
            encoding="utf-8",
        )
    elif mutate == "malformed":
        path.write_text("{broken", encoding="utf-8")
    elif mutate == "identity":
        raw = path.read_text(encoding="utf-8")
        path.unlink()
        moved = store.path / ("0" * 64 + ".json")
        moved.write_text(raw, encoding="utf-8")
        moved.chmod(0o600)
    elif mutate == "schema":
        payload["unexpected"] = "value"
        path.write_text(
            json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n",
            encoding="utf-8",
        )
    elif mutate == "canonical":
        path.write_text(json.dumps(payload, sort_keys=False) + "\n", encoding="utf-8")
        path.chmod(0o600)
    elif mutate == "permission":
        path.chmod(0o644)

    report = ExecutionAuditIntegrityService(service).verify("tenant-A")
    assert report.integrity_status is IntegrityStatus.INVALID
    assert report.invalid_events == 1
    assert getattr(report, counter) == 1
    assert report.first_failure is not None
    assert report.first_failure.startswith(code)
    # a corrupt document is never masked behind a generic failure
    assert "Traceback" not in str(report.to_dict())


@pytest.mark.unit
def test_w111_integrity_never_exposes_path_or_filename(tmp_path: Path) -> None:
    service = _file_service(tmp_path)
    store = service.store
    assert isinstance(store, ExecutionAuditStore)
    store.append(_event("leaky"))
    path = next(store.path.glob("*.json"))
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["reason_code"] = "tampered"
    path.write_text(
        json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n",
        encoding="utf-8",
    )
    rendered = json.dumps(ExecutionAuditIntegrityService(service).verify("tenant-A").to_dict())
    assert str(tmp_path) not in rendered
    assert path.name not in rendered
    assert ".json" not in rendered


@pytest.mark.unit
def test_w111_integrity_duplicate_identity(tmp_path: Path) -> None:
    service = ExecutionAuditService(MemoryExecutionAuditStore())
    store = service.store
    event = _event("dup-1")
    store.append(event)
    clone = ExecutionAuditEvent(
        event_id="dup-2",
        occurred_at=event.occurred_at,
        operation=event.operation,
        outcome=event.outcome,
        tenant_id=event.tenant_id,
        execution_id=event.execution_id,
        reference_key=event.reference_key,
        actor_type=event.actor_type,
        actor_id=event.actor_id,
        request_id=event.request_id,
        source=event.source,
        reason_code=event.reason_code,
        idempotency_key=event.idempotency_key,
    )
    store._events[clone.event_id] = clone.with_checksum()  # deliberate private tamper
    report = ExecutionAuditIntegrityService(service).verify("tenant-A")
    assert report.duplicate_events == 1
    assert report.integrity_status is IntegrityStatus.INVALID
    assert report.first_failure == "DUPLICATE_EVENT_IDENTITY"


@pytest.mark.unit
def test_w111_integrity_rejects_bad_tenant(tmp_path: Path) -> None:
    service = _file_service(tmp_path)
    for bad in ("", "   ", "../escape", "tenant/B", "a" * 300, None):
        with pytest.raises(AuditValidationError):
            ExecutionAuditIntegrityService(service).verify(cast_tenant(bad))


def cast_tenant(value: Any) -> str:
    return value if isinstance(value, str) else ""


# ── C. Reconciliation ────────────────────────────────────────────────────


@pytest.mark.unit
def test_w111_reconciliation_consistent_chain(tmp_path: Path) -> None:
    service = _file_service(tmp_path)
    store = service.store
    assert isinstance(store, ExecutionAuditStore)
    _seed_chain(store)
    report = ExecutionAuditReconciliationService(service).reconcile("tenant-A")
    assert report.reconciliation_status is ReconciliationStatus.CONSISTENT
    assert report.inconsistencies == 0
    assert report.warnings == 0
    assert report.correlation_chains == 1
    assert report.checks_run > 0
    assert report.first_failure is None


@pytest.mark.unit
def test_w111_reconciliation_insufficient_data_on_empty_store(tmp_path: Path) -> None:
    report = ExecutionAuditReconciliationService(_file_service(tmp_path)).reconcile("tenant-A")
    assert report.reconciliation_status is ReconciliationStatus.INSUFFICIENT_DATA
    assert report.events_scanned == 0
    assert report.findings == ()


@pytest.mark.unit
def test_w111_reconciliation_flags_success_with_refusal_reason(tmp_path: Path) -> None:
    service = _file_service(tmp_path)
    store = service.store
    assert isinstance(store, ExecutionAuditStore)
    store.append(
        _event(
            "purge-refused",
            operation=AuditOperation.PURGE,
            outcome=AuditOutcome.SUCCESS,
            reason_code="archive_required",
        )
    )
    report = ExecutionAuditReconciliationService(service).reconcile("tenant-A")
    assert report.reconciliation_status is ReconciliationStatus.INCONSISTENT
    codes = {finding.code for finding in report.findings}
    assert "SUCCESS_WITH_NEGATIVE_REASON" in codes
    assert "PURGE_SUCCESS_CONTRADICTS_REASON" in codes
    assert report.inconsistencies >= 2


@pytest.mark.unit
def test_w111_reconciliation_flags_restore_without_execution(tmp_path: Path) -> None:
    service = _file_service(tmp_path)
    store = service.store
    assert isinstance(store, ExecutionAuditStore)
    store.append(
        _event(
            "restore-anonymous",
            execution_id=None,
            operation=AuditOperation.RESTORE,
            outcome=AuditOutcome.SUCCESS,
            reason_code="record_restored",
            reference_key=None,
        )
    )
    report = ExecutionAuditReconciliationService(service).reconcile("tenant-A")
    codes = {finding.code for finding in report.findings}
    assert report.reconciliation_status is ReconciliationStatus.INCONSISTENT
    assert "IDENTITY_REQUIRED" in codes
    assert "RESTORE_SUCCESS_WITHOUT_EXECUTION" in codes


@pytest.mark.unit
def test_w111_reconciliation_recovery_api_status_distinguishable(tmp_path: Path) -> None:
    service = _file_service(tmp_path)
    store = service.store
    assert isinstance(store, ExecutionAuditStore)
    store.append(
        _event(
            "api-503",
            operation=AuditOperation.RECOVERY_API,
            outcome=AuditOutcome.SUCCESS,
            reason_code="backend_unavailable",
        )
    )
    store.append(
        _event(
            "api-200-on-404",
            operation=AuditOperation.RECOVERY_API,
            outcome=AuditOutcome.REJECTED,
            reason_code="record_restored",
            request_id="request-B",
        )
    )
    report = ExecutionAuditReconciliationService(service).reconcile("tenant-A")
    codes = {finding.code for finding in report.findings}
    assert report.reconciliation_status is ReconciliationStatus.INCONSISTENT
    assert "RECOVERY_API_SUCCESS_NOT_A_200" in codes
    assert "RECOVERY_API_FAILURE_USES_200_REASON" in codes


@pytest.mark.unit
def test_w111_reconciliation_detects_request_id_not_preserved(tmp_path: Path) -> None:
    service = _file_service(tmp_path)
    store = service.store
    assert isinstance(store, ExecutionAuditStore)
    store.append(
        _event(
            "api-one",
            operation=AuditOperation.RECOVERY_API,
            outcome=AuditOutcome.SUCCESS,
            reason_code="record_restored",
            request_id="req-one",
        )
    )
    store.append(
        _event(
            "restore-two",
            operation=AuditOperation.RESTORE,
            outcome=AuditOutcome.SUCCESS,
            reason_code="record_restored",
            request_id="req-two",
        )
    )
    report = ExecutionAuditReconciliationService(service).reconcile("tenant-A")
    codes = {finding.code for finding in report.findings}
    assert "CORRELATION_ID_NOT_PRESERVED" in codes
    assert report.reconciliation_status is ReconciliationStatus.INCONSISTENT


@pytest.mark.unit
def test_w111_reconciliation_never_invents_a_missing_event(tmp_path: Path) -> None:
    """A recovery api event without an observable restore is a warning only."""

    service = _file_service(tmp_path)
    store = service.store
    assert isinstance(store, ExecutionAuditStore)
    store.append(
        _event(
            "api-only",
            operation=AuditOperation.RECOVERY_API,
            outcome=AuditOutcome.SUCCESS,
            reason_code="record_restored",
        )
    )
    report = ExecutionAuditReconciliationService(service).reconcile("tenant-A")
    assert report.reconciliation_status is ReconciliationStatus.INSUFFICIENT_DATA
    assert report.inconsistencies == 0
    assert report.warnings == 1
    assert [f.code for f in report.findings] == ["CORRELATION_NOT_OBSERVABLE"]


@pytest.mark.unit
def test_w111_reconciliation_warns_on_chain_clock_skew(tmp_path: Path) -> None:
    service = _file_service(tmp_path)
    store = service.store
    assert isinstance(store, ExecutionAuditStore)
    store.append(
        _event(
            "skew-api",
            operation=AuditOperation.RECOVERY_API,
            outcome=AuditOutcome.SUCCESS,
            reason_code="record_restored",
            request_id="req-skew",
            occurred_at=NOW + timedelta(seconds=10),
        )
    )
    store.append(
        _event(
            "skew-restore",
            operation=AuditOperation.RESTORE,
            outcome=AuditOutcome.SUCCESS,
            reason_code="record_restored",
            request_id="req-skew",
            occurred_at=NOW,
        )
    )
    report = ExecutionAuditReconciliationService(service).reconcile("tenant-A")
    codes = {finding.code for finding in report.findings}
    assert "CHAIN_CLOCK_SKEW" in codes
    assert report.inconsistencies == 0
    assert report.correlation_chains == 1
    assert report.reconciliation_status is ReconciliationStatus.INSUFFICIENT_DATA


@pytest.mark.unit
def test_w111_reconciliation_refuses_corrupt_source(tmp_path: Path) -> None:
    service = _file_service(tmp_path)
    store = service.store
    assert isinstance(store, ExecutionAuditStore)
    store.append(_event("corrupt-source"))
    path = next(store.path.glob("*.json"))
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["reason_code"] = "tampered"
    path.write_text(
        json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n",
        encoding="utf-8",
    )
    with pytest.raises(AuditIntegrityError):
        ExecutionAuditReconciliationService(service).reconcile("tenant-A")


@pytest.mark.unit
def test_w111_iter_chains_only_returns_complete_chains(tmp_path: Path) -> None:
    service = _file_service(tmp_path)
    store = service.store
    assert isinstance(store, ExecutionAuditStore)
    _seed_chain(store)
    store.append(
        _event(
            "orphan-api",
            operation=AuditOperation.RECOVERY_API,
            outcome=AuditOutcome.SUCCESS,
            reason_code="record_restored",
            request_id="req-orphan",
        )
    )
    events = service.query(ExecutionAuditQuery(tenant_id="tenant-A")).events
    chains = list(iter_chains(events))
    assert chains == [("req-chain",)]


# ── D. Controlled export and determinism ─────────────────────────────────


@pytest.mark.unit
def test_w111_export_is_deterministic_and_checksummed(tmp_path: Path) -> None:
    service = _file_service(tmp_path)
    store = service.store
    assert isinstance(store, ExecutionAuditStore)
    _seed_chain(store)
    exporter = ExecutionAuditExportService(service)
    first = exporter.export("tenant-A")
    second = exporter.export("tenant-A")
    assert first.canonical_bytes == second.canonical_bytes
    assert first.export_checksum == second.export_checksum
    assert first.export_checksum == hashlib.sha256(first.canonical_bytes).hexdigest()
    assert len(first.export_checksum) == 64
    # generated_at is transport metadata and never changes the checksum
    later = exporter.export("tenant-A", generated_at=NOW + timedelta(days=365))
    assert later.export_checksum == first.export_checksum
    assert later.generated_at != first.generated_at


@pytest.mark.unit
def test_w111_export_orders_occurred_at_then_event_id_ascending() -> None:
    service = ExecutionAuditService(MemoryExecutionAuditStore())
    store = service.store
    store.append(_event("zzz", occurred_at=NOW, request_id="r1"))
    store.append(_event("aaa", occurred_at=NOW, request_id="r2"))
    store.append(_event("mmm", occurred_at=NOW - timedelta(seconds=1), request_id="r3"))
    export = ExecutionAuditExportService(service).export("tenant-A")
    parsed = json.loads(export.canonical_bytes)
    assert [item["event_id"] for item in parsed["events"]] == ["mmm", "aaa", "zzz"]


@pytest.mark.unit
def test_w111_export_checksum_changes_when_one_event_changes() -> None:
    service = ExecutionAuditService(MemoryExecutionAuditStore())
    store = service.store
    store.append(_event("stable-1", occurred_at=NOW, request_id="r1"))
    store.append(_event("stable-2", occurred_at=NOW + timedelta(seconds=1), request_id="r2"))
    exporter = ExecutionAuditExportService(service)
    before = exporter.checksum("tenant-A")["export_checksum"]
    store.append(_event("stable-3", occurred_at=NOW + timedelta(seconds=2), request_id="r3"))
    after = exporter.checksum("tenant-A")["export_checksum"]
    assert before != after


@pytest.mark.unit
def test_w111_export_jsonl_is_line_delimited(tmp_path: Path) -> None:
    service = _file_service(tmp_path)
    store = service.store
    assert isinstance(store, ExecutionAuditStore)
    _seed_chain(store)
    export = ExecutionAuditExportService(service).export(
        "tenant-A",
        export_format=ExportFormat.JSONL,
    )
    lines = export.canonical_bytes.decode("utf-8").splitlines()
    assert len(lines) == 3
    assert all(json.loads(line)["event_version"] == "1" for line in lines)
    assert export.canonical_bytes.endswith(b"\n")
    json_export = ExecutionAuditExportService(service).export("tenant-A")
    assert export.export_checksum != json_export.export_checksum


@pytest.mark.unit
def test_w111_export_is_tenant_isolated(tmp_path: Path) -> None:
    service = _file_service(tmp_path)
    store = service.store
    assert isinstance(store, ExecutionAuditStore)
    _seed_chain(store)
    store.append(
        _event(
            "tenant-b-only",
            tenant_id="tenant-B",
            execution_id="exec-B",
            request_id="request-B",
            operation=AuditOperation.EXECUTION,
        )
    )
    export_a = ExecutionAuditExportService(service).export("tenant-A")
    export_b = ExecutionAuditExportService(service).export("tenant-B")
    assert export_a.total == 3
    assert export_b.total == 1
    assert b"tenant-B" not in export_a.canonical_bytes
    assert b"exec-B" not in export_a.canonical_bytes
    assert b"tenant-A" not in export_b.canonical_bytes
    assert export_a.export_checksum != export_b.export_checksum


@pytest.mark.unit
def test_w111_export_is_paginated_and_bounded(tmp_path: Path) -> None:
    service = ExecutionAuditService(MemoryExecutionAuditStore())
    store = service.store
    for index in range(5):
        store.append(
            _event(
                f"page-{index}", occurred_at=NOW + timedelta(seconds=index), request_id=f"r{index}"
            )
        )
    exporter = ExecutionAuditExportService(service)
    page = exporter.export("tenant-A", limit=2)
    assert page.count == 2
    assert page.total == 5
    assert page.has_more is True
    tail = exporter.export("tenant-A", limit=2, offset=4)
    assert tail.count == 1
    assert tail.has_more is False
    for bad in ({"limit": 0}, {"limit": -1}, {"limit": 1001}, {"offset": -1}):
        with pytest.raises(AuditQueryError):
            exporter.export("tenant-A", **bad)


@pytest.mark.unit
def test_w111_export_refuses_corrupt_source(tmp_path: Path) -> None:
    service = _file_service(tmp_path)
    store = service.store
    assert isinstance(store, ExecutionAuditStore)
    store.append(_event("corrupt"))
    path = next(store.path.glob("*.json"))
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["reason_code"] = "tampered"
    path.write_text(
        json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n",
        encoding="utf-8",
    )
    with pytest.raises(AuditIntegrityError):
        ExecutionAuditExportService(service).export("tenant-A")
    with pytest.raises(AuditIntegrityError):
        ExecutionAuditExportService(service).checksum("tenant-A")


@pytest.mark.unit
def test_w111_export_filters_reuse_w110_contract(tmp_path: Path) -> None:
    service = _file_service(tmp_path)
    store = service.store
    assert isinstance(store, ExecutionAuditStore)
    _seed_chain(store)
    store.append(
        _event(
            "filtered-out",
            operation=AuditOperation.PURGE,
            outcome=AuditOutcome.FAILED,
            reason_code="operation_failed",
            request_id="request-B",
        )
    )
    export = ExecutionAuditExportService(service).export(
        "tenant-A",
        query=ExecutionAuditQuery(operation=AuditOperation.RESTORE),
    )
    assert export.count == 1
    assert json.loads(export.canonical_bytes)["events"][0]["operation"] == "RESTORE"


@pytest.mark.unit
def test_w111_export_total_and_paging_exceed_one_page_window() -> None:
    """A trail larger than the page bound must still page and count exactly."""

    service = ExecutionAuditService(MemoryExecutionAuditStore())
    store = service.store
    total = 1000 + 25
    for index in range(total):
        store.append(
            _event(
                f"big-{index:05d}",
                request_id=f"big-{index}",
                occurred_at=NOW + timedelta(seconds=index),
            )
        )
    exporter = ExecutionAuditExportService(service)
    first_page = exporter.export("tenant-A", limit=1000, offset=0)
    assert first_page.total == total
    assert first_page.count == 1000
    assert first_page.has_more is True
    last_page = exporter.export("tenant-A", limit=1000, offset=1000)
    assert last_page.count == 25
    assert last_page.has_more is False
    # the two pages together cover the whole trail exactly once, in order
    combined = (
        json.loads(first_page.canonical_bytes)["events"]
        + json.loads(last_page.canonical_bytes)["events"]
    )
    assert [item["event_id"] for item in combined] == sorted(item["event_id"] for item in combined)
    assert len({item["event_id"] for item in combined}) == total


@pytest.mark.unit
def test_w111_reconciliation_evaluates_the_whole_match_set() -> None:
    """A trailing correlated restore beyond one page must still be detected."""

    service = ExecutionAuditService(MemoryExecutionAuditStore())
    store = service.store
    for index in range(1000 + 5):
        store.append(
            _event(
                f"noise-{index:05d}",
                request_id=f"noise-{index}",
                occurred_at=NOW + timedelta(seconds=index),
            )
        )
    store.append(
        _event(
            "far-api",
            operation=AuditOperation.RECOVERY_API,
            outcome=AuditOutcome.SUCCESS,
            reason_code="record_restored",
            request_id="far-request",
            occurred_at=NOW + timedelta(seconds=2000),
        )
    )
    store.append(
        _event(
            "far-restore",
            operation=AuditOperation.RESTORE,
            outcome=AuditOutcome.SUCCESS,
            reason_code="record_restored",
            request_id="far-request",
            occurred_at=NOW + timedelta(seconds=2001),
        )
    )
    report = ExecutionAuditReconciliationService(service).reconcile("tenant-A")
    assert report.events_scanned == 1007
    assert report.correlation_chains == 1
    assert report.reconciliation_status is ReconciliationStatus.CONSISTENT


@pytest.mark.unit
def test_w111_export_rejects_unsafe_payload() -> None:
    for payload in (
        {"execution_id": "/opt/trendx/archive.json"},
        {"reference_key": "../../etc/passwd"},
        {"actor_id": "Bearer secret-token"},
        {"request_id": "C:\\Windows\\system32"},
        {"source": "password=value"},
        {"reason_code": "api_key=abc"},
        {"event_id": "a" * 600},
    ):
        with pytest.raises(AuditControlError):
            assert_export_safe(payload)


@pytest.mark.unit
def test_w111_export_content_has_no_secret_path_or_authorization(tmp_path: Path) -> None:
    service = _file_service(tmp_path)
    store = service.store
    assert isinstance(store, ExecutionAuditStore)
    _seed_chain(store)
    export = ExecutionAuditExportService(service).export("tenant-A")
    text = export.canonical_bytes.decode("utf-8")
    for forbidden in (
        "Authorization",
        "Bearer",
        "password",
        "api_key",
        "Traceback",
        "/tmp",
        ".json",
        "lock",
        settings.trendx_api_token.get_secret_value(),
    ):
        assert forbidden not in text


# ── E. Error sanitization and backend unavailability ─────────────────────


@pytest.mark.unit
def test_w111_missing_store_is_refused_at_construction(tmp_path: Path) -> None:
    """A store that does not exist is refused, never silently created on a read."""

    with pytest.raises(AuditStoreError):
        ExecutionAuditStore(tmp_path / "missing", create_if_missing=False)


@pytest.mark.unit
def test_w111_deleted_store_is_reported_not_masked(tmp_path: Path) -> None:
    service = _file_service(tmp_path)
    store = service.store
    assert isinstance(store, ExecutionAuditStore)
    store.append(_event("present"))
    for item in store.path.rglob("*"):
        if item.is_file():
            item.unlink()
    store.path.rmdir()
    for control in (
        ExecutionAuditIntegrityService(service),
        ExecutionAuditReconciliationService(service),
        ExecutionAuditExportService(service),
    ):
        with pytest.raises(AuditStoreError):
            if isinstance(control, ExecutionAuditIntegrityService):
                control.verify("tenant-A")
            elif isinstance(control, ExecutionAuditReconciliationService):
                control.reconcile("tenant-A")
            else:
                control.export("tenant-A")


@pytest.mark.unit
def test_w111_symlinked_store_is_rejected(tmp_path: Path) -> None:
    real = tmp_path / "real"
    real.mkdir()
    link = tmp_path / "link"
    link.symlink_to(real)
    with pytest.raises(AuditStoreError):
        ExecutionAuditStore(link)


@pytest.mark.unit
def test_w111_backend_exception_never_escapes_to_the_caller() -> None:
    """An exploding backend surfaces as a sanitized typed error, never a cause."""

    class ExplodingStore:
        def scan_documents(self) -> Any:
            raise AuditStoreError("audit store is unavailable")

    control = ExecutionAuditControlService(ExecutionAuditService(cast_store(ExplodingStore())))
    for control_service in (
        ExecutionAuditExportService(control),
        ExecutionAuditReconciliationService(control),
    ):
        with pytest.raises(AuditStoreError) as caught:
            if isinstance(control_service, ExecutionAuditExportService):
                control_service.export("tenant-A")
            else:
                control_service.reconcile("tenant-A")
        assert str(caught.value) == "audit store is unavailable"
        assert caught.value.__cause__ is None


# ── F. Concurrency ───────────────────────────────────────────────────────


@pytest.mark.unit
def test_w111_concurrent_reads_are_stable(tmp_path: Path) -> None:
    service = _file_service(tmp_path)
    store = service.store
    assert isinstance(store, ExecutionAuditStore)
    _seed_chain(store)

    def worker() -> tuple[str, str, str]:
        integrity = ExecutionAuditIntegrityService(service).verify("tenant-A")
        export = ExecutionAuditExportService(service).export("tenant-A")
        reconciliation = ExecutionAuditReconciliationService(service).reconcile("tenant-A")
        return (
            integrity.integrity_status.value,
            export.export_checksum,
            reconciliation.reconciliation_status.value,
        )

    with ThreadPoolExecutor(max_workers=8) as executor:
        results = list(executor.map(lambda _: worker(), range(16)))
    assert len(set(results)) == 1
    assert results[0][0] == IntegrityStatus.VALID.value


@pytest.mark.unit
def test_w111_multiprocess_export_is_byte_identical(tmp_path: Path) -> None:
    service = _file_service(tmp_path)
    store = service.store
    assert isinstance(store, ExecutionAuditStore)
    _seed_chain(store)
    expected = ExecutionAuditExportService(service).export("tenant-A").export_checksum
    script = (
        "import sys\n"
        f"sys.path.insert(0, {str(ROOT / 'src')!r})\n"
        "from trendx.forecasting.audit import ExecutionAuditService, ExecutionAuditStore\n"
        "from trendx.forecasting.audit_control import ExecutionAuditExportService\n"
        f"service = ExecutionAuditService(ExecutionAuditStore({str(tmp_path / 'audit')!r},"
        " create_if_missing=False))\n"
        "print(ExecutionAuditExportService(service).export('tenant-A').export_checksum)\n"
    )
    digests = []
    for _ in range(2):
        completed = subprocess.run(
            [sys.executable, "-c", script],
            capture_output=True,
            text=True,
            check=True,
            cwd=str(ROOT),
        )
        digests.append(completed.stdout.strip())
    assert digests == [expected, expected]


# ── G. HTTP contract ─────────────────────────────────────────────────────


@pytest.mark.unit
def test_w111_api_requires_authentication() -> None:
    with TestClient(app) as client:
        for path in W111_PATHS:
            assert client.get(path).status_code == 401
            assert client.get(path, headers={"Authorization": "Bearer wrong"}).status_code == 401


@pytest.mark.unit
def test_w111_api_integrity_reconciliation_export_and_checksum(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(settings, "trendx_api_token", SecretStr("w111-unit-token"))
    service = _file_service(tmp_path)
    store = service.store
    assert isinstance(store, ExecutionAuditStore)
    _seed_chain(store)
    previous_factory = app.state.execution_audit_service_factory
    previous_override = app.dependency_overrides.get(get_execution_audit_control_service)
    app.state.execution_audit_service_factory = lambda: service
    app.dependency_overrides[get_tenant_context] = lambda: TenantContext("tenant-A")
    try:
        with TestClient(app) as client:
            integrity = client.get(
                "/api/v1/forecast/executions/audit/integrity",
                headers=_auth_headers(),
            )
            assert integrity.status_code == 200
            assert integrity.json()["integrity_status"] == "VALID"
            assert integrity.json()["valid_events"] == 3

            reconciliation = client.get(
                "/api/v1/forecast/executions/audit/reconciliation",
                headers=_auth_headers(),
            )
            assert reconciliation.status_code == 200
            assert reconciliation.json()["reconciliation_status"] == "CONSISTENT"
            assert reconciliation.json()["correlation_chains"] == 1

            export = client.get(
                "/api/v1/forecast/executions/audit/export",
                headers=_auth_headers(),
            )
            assert export.status_code == 200
            body = export.json()
            assert body["event_count"] == 3
            assert (
                body["export_checksum"]
                == hashlib.sha256(body["content"].encode("utf-8")).hexdigest()
            )

            checksum = client.get(
                "/api/v1/forecast/executions/audit/export/checksum",
                headers=_auth_headers(),
            )
            assert checksum.status_code == 200
            assert checksum.json()["export_checksum"] == body["export_checksum"]
            assert "content" not in checksum.json()

            jsonl = client.get(
                "/api/v1/forecast/executions/audit/export",
                params={"format": "JSONL"},
                headers=_auth_headers(),
            )
            assert jsonl.status_code == 200
            assert jsonl.json()["format"] == "JSONL"
            assert len(jsonl.json()["content"].splitlines()) == 3
    finally:
        app.state.execution_audit_service_factory = previous_factory
        if previous_override is None:
            app.dependency_overrides.pop(get_execution_audit_control_service, None)
        else:
            app.dependency_overrides[get_execution_audit_control_service] = previous_override
        app.dependency_overrides.pop(get_tenant_context, None)


@pytest.mark.unit
def test_w111_api_tenant_isolation_and_filters(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(settings, "trendx_api_token", SecretStr("w111-tenant-token"))
    service = _file_service(tmp_path)
    store = service.store
    assert isinstance(store, ExecutionAuditStore)
    _seed_chain(store)
    store.append(
        _event(
            "tenant-b-event",
            tenant_id="tenant-B",
            execution_id="exec-B",
            request_id="request-B",
            operation=AuditOperation.EXECUTION,
        )
    )
    previous_factory = app.state.execution_audit_service_factory
    app.state.execution_audit_service_factory = lambda: service
    app.dependency_overrides[get_tenant_context] = lambda: TenantContext("tenant-A")
    try:
        with TestClient(app) as client:
            for path in W111_PATHS:
                response = client.get(
                    path,
                    params={"tenant_id": "tenant-B"},
                    headers=_auth_headers(),
                )
                assert response.status_code == 403, path
                assert "tenant-B" not in response.text
            export = client.get(
                "/api/v1/forecast/executions/audit/export",
                headers=_auth_headers(),
            )
            assert "tenant-B" not in export.text
            assert "exec-B" not in export.text
            filtered = client.get(
                "/api/v1/forecast/executions/audit/export",
                params={"operation": "RESTORE"},
                headers=_auth_headers(),
            )
            assert filtered.json()["event_count"] == 1
            # unknown field, oversized pagination
            for params in ({"unknown": "x"}, {"limit": 1001}, {"limit": 0}, {"offset": -1}):
                response = client.get(
                    "/api/v1/forecast/executions/audit/integrity",
                    params=params,
                    headers=_auth_headers(),
                )
                assert response.status_code == 422, params
            # reverse direction: tenant B asking for tenant A is also refused
            app.dependency_overrides[get_tenant_context] = lambda: TenantContext("tenant-B")
            for path in W111_PATHS:
                response = client.get(
                    path,
                    params={"tenant_id": "tenant-A"},
                    headers=_auth_headers(),
                )
                assert response.status_code == 403, path
                assert "tenant-A" not in response.text
            export_b = client.get(
                "/api/v1/forecast/executions/audit/export",
                headers=_auth_headers(),
            )
            assert export_b.json()["event_count"] == 1
            assert "tenant-A" not in export_b.text
    finally:
        app.state.execution_audit_service_factory = previous_factory
        app.dependency_overrides.pop(get_tenant_context, None)


@pytest.mark.unit
def test_w111_api_reports_corruption_and_refuses_export(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(settings, "trendx_api_token", SecretStr("w111-corrupt-token"))
    service = _file_service(tmp_path)
    store = service.store
    assert isinstance(store, ExecutionAuditStore)
    store.append(_event("will-break"))
    path = next(store.path.glob("*.json"))
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["reason_code"] = "tampered"
    path.write_text(
        json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n",
        encoding="utf-8",
    )
    previous_factory = app.state.execution_audit_service_factory
    app.state.execution_audit_service_factory = lambda: service
    app.dependency_overrides[get_tenant_context] = lambda: TenantContext("tenant-A")
    try:
        with TestClient(app) as client:
            integrity = client.get(
                "/api/v1/forecast/executions/audit/integrity",
                headers=_auth_headers(),
            )
            assert integrity.status_code == 200
            assert integrity.json()["integrity_status"] == "INVALID"
            assert integrity.json()["checksum_failures"] == 1
            assert str(tmp_path) not in integrity.text
            export = client.get(
                "/api/v1/forecast/executions/audit/export",
                headers=_auth_headers(),
            )
            assert export.status_code == 503
            assert export.json()["detail"] == "audit_service_unavailable"
            assert str(tmp_path) not in export.text
            assert "Traceback" not in export.text
    finally:
        app.state.execution_audit_service_factory = previous_factory
        app.dependency_overrides.pop(get_tenant_context, None)


@pytest.mark.unit
def test_w111_api_unavailable_backend(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(settings, "trendx_api_token", SecretStr("w111-unavailable-token"))
    previous_factory = app.state.execution_audit_service_factory
    app.state.execution_audit_service_factory = lambda: None
    app.dependency_overrides[get_tenant_context] = lambda: TenantContext("tenant-A")
    try:
        with TestClient(app) as client:
            for path in W111_PATHS:
                response = client.get(path, headers=_auth_headers())
                assert response.status_code == 503, path
                assert response.json()["detail"] == "audit_service_unavailable"
    finally:
        app.state.execution_audit_service_factory = previous_factory
        app.dependency_overrides.pop(get_tenant_context, None)


@pytest.mark.unit
def test_w111_api_auth_not_configured(monkeypatch: pytest.MonkeyPatch) -> None:
    """A configured placeholder token is refused by the W110 auth policy."""

    monkeypatch.setattr(settings, "trendx_api_token", SecretStr("CHANGE_ME"))
    with TestClient(app) as client:
        response = client.get(
            "/api/v1/forecast/executions/audit/integrity",
            headers={"Authorization": "Bearer CHANGE_ME"},
        )
        assert response.status_code == 503
        assert response.json()["detail"] == "audit_authentication_not_configured"


@pytest.mark.unit
def test_w111_api_is_read_only_over_http() -> None:
    schema = app.openapi()
    for path in W111_PATHS:
        verbs = {verb.upper() for verb in schema["paths"][path]}
        assert verbs == {"GET"}, (path, verbs)


@pytest.mark.unit
def test_w111_openapi_contract() -> None:
    schema = app.openapi()
    for path in W111_PATHS:
        operation = schema["paths"][path]["get"]
        assert set(operation["responses"]) >= {"200", "401", "403", "422", "503"}
        assert operation["security"] == [{"BearerAuth": []}, {"ApiKeyAuth": []}]
        names = {parameter["name"] for parameter in operation["parameters"]}
        assert "X-Request-ID" in names
        assert {"limit", "offset", "tenant_id", "execution_id"} <= names
    assert "ExecutionAuditIntegrityOut" in schema["components"]["schemas"]
    assert "ExecutionAuditReconciliationOut" in schema["components"]["schemas"]
    assert "ExecutionAuditExportOut" in schema["components"]["schemas"]


@pytest.mark.unit
def test_w111_api_preserves_request_id(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(settings, "trendx_api_token", SecretStr("w111-request-token"))
    service = _file_service(tmp_path)
    store = service.store
    assert isinstance(store, ExecutionAuditStore)
    _seed_chain(store)
    previous_factory = app.state.execution_audit_service_factory
    app.state.execution_audit_service_factory = lambda: service
    app.dependency_overrides[get_tenant_context] = lambda: TenantContext("tenant-A")
    try:
        with TestClient(app) as client:
            response = client.get(
                "/api/v1/forecast/executions/audit/export",
                headers={**_auth_headers(), "X-Request-ID": "w111-request-1"},
            )
            assert response.headers["X-Request-ID"] == "w111-request-1"
    finally:
        app.state.execution_audit_service_factory = previous_factory
        app.dependency_overrides.pop(get_tenant_context, None)
