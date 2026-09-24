"""W99 — read-only forecast execution history and observability.

The fixtures are W98 ExecutionRecords and a local snapshot store.  The
new-process proof passes a serialized fixture to a new Python process; it
does not claim that W98's in-memory store persists across processes.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from trendx.forecasting.execution import ExecutionRecord, ExecutionStatus, MemoryExecutionStore
from trendx.forecasting.history import (
    HISTORY_ORDER,
    MAX_HISTORY_PAGE_SIZE,
    ExecutionHistoryPage,
    ExecutionHistoryService,
    ExecutionQuery,
    FailureDiagnostic,
)

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src"
BASE_TIME = datetime(2026, 1, 1, tzinfo=UTC)


def _timestamp(minutes: int = 0) -> str:
    return (BASE_TIME + timedelta(minutes=minutes)).isoformat()


def _record(
    execution_id: str,
    *,
    tenant_id: str = "tenant-A",
    entity_id: str = "entity-A",
    target_metric: str = "temperature",
    status: ExecutionStatus = ExecutionStatus.SUCCESS,
    model_id: str = "model-A",
    model_version: str = "1",
    algorithm: str = "Fourier",
    feature_schema_version: str = "S1",
    feature_schema_fingerprint: str = "fingerprint-S1",
    artifact_uri: str = "file:///artifacts/model-A/artifact.bin",
    created_minutes: int = 0,
    started_minutes: int | None = 1,
    completed_minutes: int | None = 5,
    prediction_count: int | None = 3,
    error_code: str = "",
    error_reason: str = "",
    reference_key: str | None = None,
) -> ExecutionRecord:
    return ExecutionRecord(
        execution_id=execution_id,
        reference_key=reference_key or f"ref-{execution_id}",
        tenant_id=tenant_id,
        entity_type="DEVICE",
        entity_id=entity_id,
        target_metric=target_metric,
        frequency="1h",
        horizon=24,
        model_id=model_id,
        model_version=model_version,
        algorithm=algorithm,
        feature_schema_version=feature_schema_version,
        feature_schema_fingerprint=feature_schema_fingerprint,
        artifact_uri=artifact_uri,
        status=status,
        created_at=_timestamp(created_minutes),
        started_at=_timestamp(started_minutes) if started_minutes is not None else "",
        completed_at=(_timestamp(completed_minutes) if completed_minutes is not None else ""),
        error_code=error_code,
        error_reason=error_reason,
        prediction_count=prediction_count,
        metadata={"source": "w99-fixture"},
        result=(
            {"result": {"values": [1.0] * prediction_count}}
            if status is ExecutionStatus.SUCCESS and prediction_count is not None
            else None
        ),
    )


def _default_records() -> tuple[ExecutionRecord, ...]:
    return (
        _record("E-temp-success", created_minutes=10, completed_minutes=20),
        _record(
            "E-humidity-success",
            target_metric="humidity",
            model_version="2",
            artifact_uri="file:///artifacts/model-A-v2/artifact.bin",
            created_minutes=30,
            started_minutes=35,
            completed_minutes=40,
            prediction_count=5,
        ),
        _record(
            "E-schema-failed",
            status=ExecutionStatus.FAILED,
            model_id="model-B",
            feature_schema_version="S2",
            feature_schema_fingerprint="fingerprint-S2",
            artifact_uri="file:///artifacts/model-B/artifact.bin",
            created_minutes=20,
            started_minutes=23,
            completed_minutes=25,
            prediction_count=None,
            error_code="feature_schema_mismatch",
            error_reason="fingerprint mismatch",
        ),
        _record(
            "E-artifact-failed",
            entity_id="entity-B",
            target_metric="solar_power",
            status=ExecutionStatus.FAILED,
            model_id="model-A",
            created_minutes=40,
            started_minutes=41,
            completed_minutes=42,
            prediction_count=None,
            error_code="artifact-missing",
            error_reason="artifact was not found",
        ),
        _record(
            "E-no-champion-failed",
            entity_id="entity-B",
            target_metric="energy_consumption",
            status=ExecutionStatus.FAILED,
            model_id="",
            model_version="",
            artifact_uri="",
            created_minutes=45,
            started_minutes=46,
            completed_minutes=47,
            prediction_count=None,
            error_code="no-compatible-champion",
            error_reason="no compatible champion",
        ),
        _record(
            "E-entity-b-success",
            entity_id="entity-B",
            target_metric="X",
            model_id="model-C",
            artifact_uri="file:///artifacts/model-C/artifact.bin",
            created_minutes=50,
            started_minutes=52,
            completed_minutes=55,
            prediction_count=7,
        ),
        _record(
            "E-tenant-b-success",
            tenant_id="tenant-B",
            model_id="model-D",
            artifact_uri="file:///artifacts/model-D/artifact.bin",
            created_minutes=60,
            started_minutes=62,
            completed_minutes=65,
            prediction_count=9,
        ),
        _record(
            "E-pending",
            status=ExecutionStatus.PENDING,
            model_id="",
            model_version="",
            artifact_uri="",
            created_minutes=70,
            started_minutes=None,
            completed_minutes=None,
            prediction_count=None,
        ),
    )


@dataclass
class _SnapshotStore:
    """Read-only test reader with an explicit mutation call log."""

    records: tuple[ExecutionRecord, ...]
    mutation_calls: list[str] = field(default_factory=list)

    def get(self, execution_id: str) -> ExecutionRecord | None:
        return next(
            (record for record in self.records if record.execution_id == execution_id),
            None,
        )

    def get_by_reference_key(self, reference_key: str) -> ExecutionRecord | None:
        matches = [record for record in self.records if record.reference_key == reference_key]
        return matches[-1] if matches else None

    def list(
        self,
        *,
        reference_key: str | None = None,
        status: ExecutionStatus | None = None,
    ) -> tuple[ExecutionRecord, ...]:
        records = list(self.records)
        if reference_key is not None:
            records = [record for record in records if record.reference_key == reference_key]
        if status is not None:
            records = [record for record in records if record.status is status]
        return tuple(records)

    def create(self, record: ExecutionRecord) -> ExecutionRecord:
        self.mutation_calls.append("create")
        raise AssertionError("history service attempted a write")

    def mark_started(self, execution_id: str) -> ExecutionRecord:
        self.mutation_calls.append("mark_started")
        raise AssertionError("history service attempted a write")

    def mark_success(self, execution_id: str, provenance: Any) -> ExecutionRecord:
        self.mutation_calls.append("mark_success")
        raise AssertionError("history service attempted a write")

    def mark_failed(self, execution_id: str, **kwargs: Any) -> ExecutionRecord:
        self.mutation_calls.append("mark_failed")
        raise AssertionError("history service attempted a write")


def _service(
    records: tuple[ExecutionRecord, ...] | None = None,
) -> tuple[ExecutionHistoryService, _SnapshotStore]:
    store = _SnapshotStore(records if records is not None else _default_records())
    return ExecutionHistoryService(store), store


def _ids(page: ExecutionHistoryPage) -> list[str]:
    return [record.execution_id for record in page.records]


@pytest.mark.unit
def test_a_query_empty_history() -> None:
    service, _ = _service(())
    page = service.query()
    assert page.records == ()
    assert page.total == 0
    assert service.statistics().success_rate == 0.0

    memory_store = MemoryExecutionStore()
    pending = _record("E-memory", status=ExecutionStatus.PENDING)
    memory_store.create(pending)
    memory_service = ExecutionHistoryService(memory_store)
    assert memory_service.query().records[0].execution_id == "E-memory"


@pytest.mark.unit
def test_b_query_all_returns_records_without_filter() -> None:
    service, _ = _service()
    page = service.query()
    assert page.total == len(_default_records())
    assert len(page.records) == page.total
    assert {record.execution_id for record in page} == {
        record.execution_id for record in _default_records()
    }


@pytest.mark.unit
def test_c_filter_tenant() -> None:
    service, _ = _service()
    page = service.query(ExecutionQuery(tenant_id="tenant-A"))
    assert page.records
    assert {record.tenant_id for record in page} == {"tenant-A"}


@pytest.mark.unit
def test_d_filter_entity() -> None:
    service, _ = _service()
    page = service.query(ExecutionQuery(entity_id="entity-B"))
    assert {record.entity_id for record in page} == {"entity-B"}


@pytest.mark.unit
def test_e_filter_metric() -> None:
    service, _ = _service()
    page = service.query(ExecutionQuery(target_metric="humidity"))
    assert _ids(page) == ["E-humidity-success"]


@pytest.mark.unit
def test_f_filter_execution_id() -> None:
    service, _ = _service()
    page = service.query(ExecutionQuery(execution_id="E-temp-success"))
    assert _ids(page) == ["E-temp-success"]


@pytest.mark.unit
def test_g_filter_reference_key() -> None:
    service, _ = _service()
    page = service.query(ExecutionQuery(reference_key="ref-E-schema-failed"))
    assert _ids(page) == ["E-schema-failed"]


@pytest.mark.unit
def test_h_filter_model_id() -> None:
    service, _ = _service()
    page = service.query(ExecutionQuery(model_id="model-A"))
    assert {record.model_id for record in page} == {"model-A"}
    assert len(page.records) == 3


@pytest.mark.unit
def test_i_filter_model_version() -> None:
    service, _ = _service()
    page = service.query(ExecutionQuery(model_id="model-A", model_version="2"))
    assert _ids(page) == ["E-humidity-success"]
    assert page.records[0].artifact_uri.endswith("model-A-v2/artifact.bin")


@pytest.mark.unit
def test_j_filter_status_success() -> None:
    service, _ = _service()
    page = service.query(ExecutionQuery(status=ExecutionStatus.SUCCESS))
    assert page.records
    assert {record.status for record in page} == {ExecutionStatus.SUCCESS}


@pytest.mark.unit
def test_k_filter_status_failed() -> None:
    service, _ = _service()
    page = service.query(ExecutionQuery(status="FAILED"))
    assert {record.status for record in page} == {ExecutionStatus.FAILED}
    assert len(page.records) == 3


@pytest.mark.unit
def test_l_combined_and_filters() -> None:
    service, _ = _service()
    page = service.query(
        ExecutionQuery(
            tenant_id="tenant-A",
            entity_id="entity-A",
            target_metric="temperature",
            status="SUCCESS",
        )
    )
    assert _ids(page) == ["E-temp-success"]


@pytest.mark.unit
def test_m_order_is_created_desc_then_execution_id_desc() -> None:
    records = (
        _record("E-b", created_minutes=10),
        _record("E-c", created_minutes=10),
        _record("E-a", created_minutes=20),
    )
    service, _ = _service(records)
    first = service.query()
    second = service.query()
    assert _ids(first) == ["E-a", "E-c", "E-b"]
    assert _ids(first) == _ids(second)
    assert first.to_dict()["order"] == HISTORY_ORDER


@pytest.mark.unit
def test_n_time_range_filters_are_inclusive() -> None:
    service, _ = _service()
    page = service.query(
        ExecutionQuery(
            created_at_from=_timestamp(20),
            created_at_to=_timestamp(40),
        )
    )
    assert {record.execution_id for record in page} == {
        "E-humidity-success",
        "E-schema-failed",
        "E-artifact-failed",
    }
    completed = service.query(
        ExecutionQuery(
            completed_at_from="2026-01-01T00:25:00+00:00",
            completed_at_to="2026-01-01T00:42:00+00:00",
        )
    )
    assert {record.execution_id for record in completed} == {
        "E-humidity-success",
        "E-schema-failed",
        "E-artifact-failed",
    }


@pytest.mark.unit
def test_o_success_count() -> None:
    service, _ = _service()
    statistics = service.statistics(ExecutionQuery(status=ExecutionStatus.SUCCESS))
    assert statistics.success_count == 4
    assert statistics.failure_count == 0
    assert statistics.total_executions == 4


@pytest.mark.unit
def test_p_failed_count() -> None:
    service, _ = _service()
    statistics = service.statistics(ExecutionQuery(status=ExecutionStatus.FAILED))
    assert statistics.failure_count == 3
    assert statistics.failed_count == 3
    assert statistics.success_count == 0


@pytest.mark.unit
def test_q_success_rate_and_empty_total() -> None:
    service, _ = _service()
    statistics = service.statistics()
    assert statistics.total_executions == 8
    assert statistics.success_count == 4
    assert statistics.success_rate == 0.5
    empty, _ = _service(())
    assert empty.statistics().success_rate == 0.0


@pytest.mark.unit
def test_r_prediction_count_and_duration_aggregation() -> None:
    service, _ = _service()
    statistics = service.statistics(ExecutionQuery(status=ExecutionStatus.SUCCESS))
    assert statistics.prediction_count_total == 24
    assert statistics.duration_sample_count == 4
    assert statistics.total_duration_seconds == 1800.0
    assert statistics.average_duration_seconds == 450.0


@pytest.mark.unit
def test_s_failure_diagnostics_preserve_existing_codes() -> None:
    records = (
        *_default_records(),
        _record(
            "E-artifact-corrupt",
            status=ExecutionStatus.FAILED,
            created_minutes=80,
            started_minutes=81,
            completed_minutes=82,
            prediction_count=None,
            error_code="artifact-corrupted",
            error_reason="artifact integrity check failed",
        ),
        _record(
            "E-payload-invalid",
            status=ExecutionStatus.FAILED,
            created_minutes=85,
            started_minutes=86,
            completed_minutes=87,
            prediction_count=None,
            error_code="payload_invalid",
            error_reason="invalid forecast payload",
        ),
    )
    service, _ = _service(records)
    diagnostics = service.failure_diagnostics()
    assert {diagnostic.error_code for diagnostic in diagnostics} == {
        "feature_schema_mismatch",
        "artifact-missing",
        "artifact-corrupted",
        "payload_invalid",
        "no-compatible-champion",
    }
    schema = next(d for d in diagnostics if d.error_code == "feature_schema_mismatch")
    assert isinstance(schema, FailureDiagnostic)
    assert schema.status is ExecutionStatus.FAILED
    assert schema.feature_schema_fingerprint == "fingerprint-S2"
    assert schema.created_at == _timestamp(20)
    assert schema.completed_at == _timestamp(25)


@pytest.mark.unit
def test_t_model_provenance_lookup() -> None:
    service, _ = _service()
    page = service.find_by_model("model-A")
    assert {record.model_id for record in page} == {"model-A"}
    record = service.get("E-temp-success")
    assert record is not None
    assert record.model_version == "1"
    assert record.algorithm == "Fourier"
    assert record.feature_schema_fingerprint == "fingerprint-S1"
    assert record.artifact_uri.endswith("model-A/artifact.bin")


@pytest.mark.unit
def test_u_model_versions_and_artifacts_are_distinct() -> None:
    service, _ = _service()
    v1 = service.find_by_model("model-A", model_version="1")
    v2 = service.find_by_model("model-A", model_version="2")
    assert v1.records[0].model_version == "1"
    assert v2.records[0].model_version == "2"
    assert v1.records[0].artifact_uri != v2.records[0].artifact_uri


@pytest.mark.unit
def test_v_tenant_isolation_and_tenant_entity_combination() -> None:
    service, _ = _service()
    tenant_a = service.query(ExecutionQuery(tenant_id="tenant-A", entity_id="entity-A"))
    assert {record.tenant_id for record in tenant_a} == {"tenant-A"}
    assert all(record.entity_id == "entity-A" for record in tenant_a)
    tenant_b = service.query(ExecutionQuery(tenant_id="tenant-B", entity_id="entity-A"))
    assert {record.execution_id for record in tenant_b} == {"E-tenant-b-success"}


@pytest.mark.unit
def test_w_multi_entity_and_multi_metric_history() -> None:
    service, _ = _service()
    entity_a = service.query(ExecutionQuery(entity_id="entity-A"))
    entity_b = service.query(ExecutionQuery(entity_id="entity-B"))
    assert {record.entity_id for record in entity_a} == {"entity-A"}
    assert {record.entity_id for record in entity_b} == {"entity-B"}
    for metric in ("temperature", "humidity", "energy_consumption", "solar_power"):
        page = service.query(ExecutionQuery(target_metric=metric))
        assert page.records
        assert {record.target_metric for record in page} == {metric}
        assert service.statistics(ExecutionQuery(target_metric=metric)).total_executions == len(
            page.records
        )


@pytest.mark.unit
def test_x_history_read_only_and_repeated_query_is_idempotent() -> None:
    records = _default_records()
    store = _SnapshotStore(records)
    service = ExecutionHistoryService(store)
    before = store.records
    first = service.query()
    first_stats = service.statistics()
    first_diagnostics = service.failure_diagnostics()
    second = service.query()
    assert _ids(first) == _ids(second)
    assert first_stats == service.statistics()
    assert first_diagnostics == service.failure_diagnostics()
    assert store.records == before
    assert store.mutation_calls == []
    assert len(first.records) == len(before)


@pytest.mark.unit
def test_y_fourier_algorithm_filter() -> None:
    service, _ = _service()
    page = service.query(ExecutionQuery(algorithm="FOURIER"))
    assert page.records
    assert {record.algorithm.casefold() for record in page} == {"fourier"}


@pytest.mark.unit
def test_z_external_feature_fingerprint_history_and_diagnostic() -> None:
    service, _ = _service()
    s1 = service.query(ExecutionQuery(feature_schema_fingerprint="fingerprint-S1"))
    s2 = service.query(ExecutionQuery(feature_schema_fingerprint="fingerprint-S2"))
    s2_version = service.query(ExecutionQuery(feature_schema_version="S2"))
    assert s1.records
    assert _ids(s2) == ["E-schema-failed"]
    assert _ids(s2_version) == ["E-schema-failed"]
    assert s2.records[0].status is ExecutionStatus.FAILED
    assert s2.records[0].error_code == "feature_schema_mismatch"


@pytest.mark.unit
def test_aa_pagination_is_stable_bounded_and_validated() -> None:
    service, _ = _service()
    first = service.query(ExecutionQuery(limit=4, offset=0))
    second = service.query(ExecutionQuery(limit=4, offset=4))
    assert first.total == second.total == 8
    assert first.has_more is True
    assert not set(_ids(first)) & set(_ids(second))
    assert _ids(first) + _ids(second) == _ids(service.query())
    with pytest.raises(ValueError):
        ExecutionQuery(limit=0)
    with pytest.raises(ValueError):
        ExecutionQuery(limit=MAX_HISTORY_PAGE_SIZE + 1)
    with pytest.raises(ValueError):
        ExecutionQuery(offset=-1)


@pytest.mark.unit
def test_ac_query_and_page_serialization_is_deterministic() -> None:
    service, _ = _service()
    query = ExecutionQuery(
        tenant_id="tenant-A",
        target_metric="temperature",
        status="SUCCESS",
        created_at_from="2026-01-01T00:00:00Z",
        limit=2,
        offset=0,
    )
    assert ExecutionQuery.from_dict(query.to_dict()) == query
    first = service.query(query).to_dict()
    second = service.query(query).to_dict()
    assert first == second
    required = {
        "execution_id",
        "reference_key",
        "tenant_id",
        "entity_id",
        "target_metric",
        "status",
        "model_id",
        "model_version",
        "algorithm",
        "feature_schema_fingerprint",
        "artifact_uri",
        "created_at",
        "completed_at",
    }
    assert required.issubset(first["records"][0])
    assert first["order"] == HISTORY_ORDER


def test_ab_new_process_history_service_uses_reconstructed_snapshot() -> None:
    records = _default_records()
    child = r"""
import json
import sys

from trendx.forecasting.execution import ExecutionRecord
from trendx.forecasting.history import ExecutionHistoryService, ExecutionQuery

class SnapshotStore:
    def __init__(self, records):
        self.records = tuple(records)

    def get(self, execution_id):
        return next((r for r in self.records if r.execution_id == execution_id), None)

    def get_by_reference_key(self, reference_key):
        matches = [r for r in self.records if r.reference_key == reference_key]
        return matches[-1] if matches else None

    def list(self, *, reference_key=None, status=None):
        records = list(self.records)
        if reference_key is not None:
            records = [r for r in records if r.reference_key == reference_key]
        if status is not None:
            records = [r for r in records if r.status is status]
        return tuple(records)

payload = json.load(sys.stdin)
records = [ExecutionRecord.from_dict(item) for item in payload]
service = ExecutionHistoryService(SnapshotStore(records))
page = service.query(ExecutionQuery(tenant_id="tenant-A", status="FAILED"))
stats = service.statistics(ExecutionQuery(model_id="model-A"))
print(json.dumps({"ids": [r.execution_id for r in page], "stats": stats.to_dict()}))
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
    completed = subprocess.run(
        [sys.executable, "-c", child],
        cwd=ROOT,
        env=env,
        input=json.dumps([record.to_dict() for record in records]),
        text=True,
        capture_output=True,
        check=True,
    )
    evidence = json.loads(completed.stdout.strip().splitlines()[-1])
    assert set(evidence["ids"]) == {
        "E-schema-failed",
        "E-artifact-failed",
        "E-no-champion-failed",
    }
    assert evidence["stats"]["total_executions"] == 3
    assert evidence["stats"]["failure_count"] == 1
