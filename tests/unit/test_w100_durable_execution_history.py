"""W100 — durable forecast execution history persistence.

Every datastore is a temporary local JSON file.  The new-process proof passes
only the datastore path to process B; no ExecutionRecord or Python object is
serialized as a test fixture or shared through memory.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path
from threading import Barrier
from typing import Any

import numpy as np
import pandas as pd
import pytest
from trendx.forecasting.base import ForecastResult
from trendx.forecasting.contract import Algorithm, ForecastRequest
from trendx.forecasting.dataset import DatasetBuilder
from trendx.forecasting.execution import (
    DurableExecutionStore,
    ExecutionProvenance,
    ExecutionRecord,
    ExecutionStatus,
    ExecutionStoreError,
    ExecutionStoreSerializationError,
    InvalidExecutionTransitionError,
    MemoryExecutionStore,
)
from trendx.forecasting.history import ExecutionHistoryService, ExecutionQuery
from trendx.forecasting.pipeline import ForecastPipeline
from trendx.forecasting.registry import ModelRole, ModelStatus, RegisteredModel
from trendx.forecasting.resolution import FeatureResolver

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src"
BASE_TIME = datetime(2026, 1, 1, tzinfo=UTC)


def _timestamp(minutes: int = 0) -> str:
    return (BASE_TIME + timedelta(minutes=minutes)).isoformat()


def _record(
    execution_id: str,
    *,
    reference_key: str | None = None,
    tenant_id: str = "tenant-A",
    entity_id: str = "entity-A",
    target_metric: str = "temperature",
    status: ExecutionStatus = ExecutionStatus.PENDING,
    model_id: str = "",
    model_version: str = "",
    algorithm: str = "",
    feature_schema_version: str = "S1",
    feature_schema_fingerprint: str = "fingerprint-S1",
    artifact_uri: str = "",
    created_minutes: int = 0,
    prediction_count: int | None = None,
    metadata: dict[str, Any] | None = None,
    result: dict[str, Any] | None = None,
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
        metadata=metadata if metadata is not None else {"fixture": "w100"},
        result=result,
    )


def _provenance(
    *,
    model_id: str = "model-A",
    model_version: str = "1",
    algorithm: str = "Fourier",
    feature_schema_version: str = "S1",
    fingerprint: str = "fingerprint-S1",
    artifact_uri: str = "file:///artifacts/model-A/artifact.bin",
    prediction_count: int = 3,
    metadata: dict[str, Any] | None = None,
    result: dict[str, Any] | None = None,
) -> ExecutionProvenance:
    return ExecutionProvenance(
        model_id=model_id,
        model_version=model_version,
        algorithm=algorithm,
        feature_schema_version=feature_schema_version,
        feature_schema_fingerprint=fingerprint,
        artifact_uri=artifact_uri,
        prediction_count=prediction_count,
        metadata=metadata if metadata is not None else {"source": "w100"},
        result=result,
    )


def _success(
    store: DurableExecutionStore,
    record: ExecutionRecord,
    provenance: ExecutionProvenance | None = None,
) -> ExecutionRecord:
    store.create(record)
    store.mark_started(record.execution_id)
    return store.mark_success(
        record.execution_id,
        provenance or _provenance(),
        completed_at=record.completed_at or None,
    )


def _failed(
    store: DurableExecutionStore,
    record: ExecutionRecord,
    *,
    code: str = "payload_invalid",
    reason: str = "controlled failure",
    provenance: ExecutionProvenance | None = None,
) -> ExecutionRecord:
    store.create(record)
    store.mark_started(record.execution_id)
    return store.mark_failed(
        record.execution_id,
        error_code=code,
        error_reason=reason,
        provenance=provenance,
        completed_at=record.completed_at or None,
    )


def _ids(records: tuple[ExecutionRecord, ...]) -> list[str]:
    return [record.execution_id for record in records]


@pytest.mark.unit
def test_a_durable_create_and_read(tmp_path: Path) -> None:
    path = tmp_path / "execution-history.json"
    store = DurableExecutionStore(path)
    record = _record("E-a")
    store.create(record)
    store.close()

    reopened = DurableExecutionStore(path)
    restored = reopened.get("E-a")
    assert restored == record
    assert restored.status is ExecutionStatus.PENDING
    reopened.close()


@pytest.mark.unit
def test_b_durable_success_is_read_after_restart(tmp_path: Path) -> None:
    path = tmp_path / "execution-history.json"
    store = DurableExecutionStore(path)
    record = _success(store, _record("E-success"))
    assert record.status is ExecutionStatus.SUCCESS
    store.close()

    reopened = DurableExecutionStore(path)
    restored = reopened.get("E-success")
    assert restored is not None
    assert restored.status is ExecutionStatus.SUCCESS
    assert restored.model_id == "model-A"
    assert restored.artifact_uri.endswith("model-A/artifact.bin")
    assert restored.prediction_count == 3
    reopened.close()


@pytest.mark.unit
def test_c_durable_failed_is_read_after_restart(tmp_path: Path) -> None:
    path = tmp_path / "execution-history.json"
    store = DurableExecutionStore(path)
    record = _failed(store, _record("E-failed"), code="no-compatible-champion")
    assert record.status is ExecutionStatus.FAILED
    store.close()

    reopened = DurableExecutionStore(path)
    restored = reopened.get("E-failed")
    assert restored is not None
    assert restored.status is ExecutionStatus.FAILED
    assert restored.error_code == "no-compatible-champion"
    assert restored.error_reason == "controlled failure"
    reopened.close()


@pytest.mark.unit
def test_d_execution_id_lookup(tmp_path: Path) -> None:
    store = DurableExecutionStore(tmp_path / "execution-history.json")
    _success(store, _record("E-lookup"))
    assert store.get("E-lookup") is not None
    assert store.get("missing") is None
    store.close()


@pytest.mark.unit
def test_e_reference_key_lookup(tmp_path: Path) -> None:
    store = DurableExecutionStore(tmp_path / "execution-history.json")
    _success(store, _record("E-ref", reference_key="dispatch-ref"))
    record = store.get_by_reference_key("dispatch-ref")
    assert record is not None
    assert record.execution_id == "E-ref"
    assert store.get_by_reference_key("unknown") is None
    store.close()


@pytest.mark.unit
def test_f_unique_execution_id_is_enforced(tmp_path: Path) -> None:
    store = DurableExecutionStore(tmp_path / "execution-history.json")
    record = _record("E-unique")
    store.create(record)
    with pytest.raises(ExecutionStoreError, match="already exists"):
        store.create(record)
    assert len(store.list()) == 1
    store.close()


@pytest.mark.unit
def test_g_reference_idempotence_is_durable(tmp_path: Path) -> None:
    path = tmp_path / "execution-history.json"
    store = DurableExecutionStore(path)
    _success(store, _record("E-first", reference_key="same-ref"))
    store.close()

    reopened = DurableExecutionStore(path)
    with pytest.raises(ExecutionStoreError, match="reference_key"):
        reopened.create(_record("E-second", reference_key="same-ref"))
    assert len(reopened.list(reference_key="same-ref")) == 1
    existing = reopened.get_by_reference_key("same-ref")
    assert existing is not None
    assert existing.status is ExecutionStatus.SUCCESS
    reopened.close()


@pytest.mark.unit
def test_h_state_transitions_and_invalid_transitions(tmp_path: Path) -> None:
    store = DurableExecutionStore(tmp_path / "execution-history.json")
    store.create(_record("E-transition"))
    with pytest.raises(InvalidExecutionTransitionError):
        store.mark_success("E-transition", _provenance())
    store.mark_started("E-transition")
    with pytest.raises(InvalidExecutionTransitionError):
        store.mark_started("E-transition")
    store.mark_success("E-transition", _provenance())
    with pytest.raises(InvalidExecutionTransitionError):
        store.mark_failed("E-transition", error_code="late", error_reason="late")
    store.close()


@pytest.mark.unit
def test_gb_failed_reference_can_be_retried_with_new_execution_id(tmp_path: Path) -> None:
    path = tmp_path / "execution-history.json"
    store = DurableExecutionStore(path)
    _failed(store, _record("E-failed-retry", reference_key="retry-ref"), code="artifact-corrupted")
    store.close()

    reopened = DurableExecutionStore(path)
    retry_record = _record("E-retry-success", reference_key="retry-ref")
    reopened.create(retry_record)
    reopened.mark_started(retry_record.execution_id)
    reopened.mark_success(retry_record.execution_id, _provenance())
    history = ExecutionHistoryService(reopened)
    records = history.query(ExecutionQuery(reference_key="retry-ref")).records
    assert [record.execution_id for record in records] == [
        "E-retry-success",
        "E-failed-retry",
    ]
    assert records[0].status is ExecutionStatus.SUCCESS
    assert records[1].status is ExecutionStatus.FAILED
    reopened.close()


@pytest.mark.unit
def test_gc_concurrent_execution_id_creation_has_one_winner(tmp_path: Path) -> None:
    path = tmp_path / "execution-history.json"
    DurableExecutionStore(path).close()
    barrier = Barrier(2)

    def attempt() -> str:
        store = DurableExecutionStore(path)
        try:
            barrier.wait(timeout=10)
            store.create(_record("E-race", reference_key="race-ref"))
            return "created"
        except ExecutionStoreError:
            return "rejected"
        finally:
            store.close()

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(lambda _index: attempt(), range(2)))
    assert sorted(results) == ["created", "rejected"]

    reopened = DurableExecutionStore(path)
    assert len(reopened.list()) == 1
    assert reopened.get("E-race") is not None
    reopened.close()


@pytest.mark.unit
def test_i_tenant_isolation_after_restart(tmp_path: Path) -> None:
    path = tmp_path / "execution-history.json"
    store = DurableExecutionStore(path)
    _success(store, _record("E-tenant-a", tenant_id="tenant-A"))
    _success(store, _record("E-tenant-b", tenant_id="tenant-B"))
    store.close()

    history = ExecutionHistoryService(DurableExecutionStore(path))
    tenant_a = history.query(ExecutionQuery(tenant_id="tenant-A"))
    assert _ids(tuple(tenant_a.records)) == ["E-tenant-a"]
    assert all(record.tenant_id == "tenant-A" for record in tenant_a)


@pytest.mark.unit
def test_j_entity_isolation_after_restart(tmp_path: Path) -> None:
    path = tmp_path / "execution-history.json"
    store = DurableExecutionStore(path)
    _success(store, _record("E-entity-a", entity_id="entity-A"))
    _success(store, _record("E-entity-b", entity_id="entity-B"))
    store.close()

    history = ExecutionHistoryService(DurableExecutionStore(path))
    page = history.query(ExecutionQuery(entity_id="entity-B"))
    assert _ids(tuple(page.records)) == ["E-entity-b"]


@pytest.mark.unit
def test_k_metric_filter_after_restart(tmp_path: Path) -> None:
    path = tmp_path / "execution-history.json"
    store = DurableExecutionStore(path)
    for metric in ("temperature", "humidity", "energy_consumption", "solar_power"):
        _success(store, _record(f"E-{metric}", target_metric=metric))
    store.close()

    history = ExecutionHistoryService(DurableExecutionStore(path))
    for metric in ("temperature", "humidity", "energy_consumption", "solar_power"):
        page = history.query(ExecutionQuery(target_metric=metric))
        assert _ids(tuple(page.records)) == [f"E-{metric}"]


@pytest.mark.unit
def test_l_model_version_and_artifact_provenance(tmp_path: Path) -> None:
    path = tmp_path / "execution-history.json"
    store = DurableExecutionStore(path)
    _success(
        store,
        _record("E-model-v1"),
        _provenance(
            model_id="model-A",
            model_version="1",
            artifact_uri="file:///artifacts/model-A-v1.bin",
        ),
    )
    _success(
        store,
        _record("E-model-v2"),
        _provenance(
            model_id="model-A",
            model_version="2",
            artifact_uri="file:///artifacts/model-A-v2.bin",
        ),
    )
    store.close()

    history = ExecutionHistoryService(DurableExecutionStore(path))
    v1 = history.query(ExecutionQuery(model_id="model-A", model_version="1"))
    v2 = history.query(ExecutionQuery(model_id="model-A", model_version="2"))
    assert v1.records[0].artifact_uri != v2.records[0].artifact_uri
    assert {record.model_version for record in v1.records} == {"1"}
    assert {record.model_version for record in v2.records} == {"2"}


@pytest.mark.unit
def test_m_feature_fingerprint_persistence(tmp_path: Path) -> None:
    path = tmp_path / "execution-history.json"
    store = DurableExecutionStore(path)
    _success(
        store,
        _record("E-s1"),
        _provenance(
            feature_schema_version="S1",
            fingerprint="fingerprint-S1",
        ),
    )
    _failed(
        store,
        _record("E-s2"),
        code="feature_schema_mismatch",
        provenance=_provenance(
            feature_schema_version="S2",
            fingerprint="fingerprint-S2",
        ),
    )
    store.close()

    history = ExecutionHistoryService(DurableExecutionStore(path))
    s1 = history.query(ExecutionQuery(feature_schema_fingerprint="fingerprint-S1"))
    s2 = history.query(ExecutionQuery(feature_schema_fingerprint="fingerprint-S2"))
    assert _ids(tuple(s1.records)) == ["E-s1"]
    assert _ids(tuple(s2.records)) == ["E-s2"]
    assert s2.records[0].error_code == "feature_schema_mismatch"


@pytest.mark.unit
def test_n_pagination_after_restart(tmp_path: Path) -> None:
    path = tmp_path / "execution-history.json"
    store = DurableExecutionStore(path)
    for index in range(5):
        _success(store, _record(f"E-page-{index}", created_minutes=index))
    store.close()

    history = ExecutionHistoryService(DurableExecutionStore(path))
    first = history.query(ExecutionQuery(limit=2, offset=0))
    second = history.query(ExecutionQuery(limit=2, offset=2))
    final = history.query(ExecutionQuery(limit=2, offset=4))
    empty = history.query(ExecutionQuery(limit=2, offset=10))
    assert first.total == second.total == 5
    assert first.has_more is True
    assert second.has_more is True
    assert final.has_more is False
    assert len(final.records) == 1
    assert empty.records == ()
    assert not set(_ids(tuple(first.records))) & set(_ids(tuple(second.records)))


@pytest.mark.unit
def test_o_stable_ordering_after_restart(tmp_path: Path) -> None:
    path = tmp_path / "execution-history.json"
    store = DurableExecutionStore(path)
    _success(store, _record("E-a", created_minutes=1))
    _success(store, _record("E-z", created_minutes=1))
    _success(store, _record("E-m", created_minutes=2))
    store.close()

    history = ExecutionHistoryService(DurableExecutionStore(path))
    assert _ids(tuple(history.query().records)) == ["E-m", "E-z", "E-a"]
    assert _ids(tuple(history.query().records)) == ["E-m", "E-z", "E-a"]


@pytest.mark.unit
def test_p_statistics_from_durable_store_after_restart(tmp_path: Path) -> None:
    path = tmp_path / "execution-history.json"
    store = DurableExecutionStore(path)
    _success(store, _record("E-stat-1"), _provenance(prediction_count=4))
    _success(store, _record("E-stat-2"), _provenance(prediction_count=6))
    _failed(store, _record("E-stat-failed"), code="artifact-corrupted")
    store.close()

    history = ExecutionHistoryService(DurableExecutionStore(path))
    stats = history.statistics()
    assert stats.total_executions == 3
    assert stats.success_count == 2
    assert stats.failure_count == 1
    assert stats.success_rate == 2 / 3
    assert stats.prediction_count_total == 10
    assert stats.duration_sample_count == 3


@pytest.mark.unit
def test_q_timestamp_filter_preserves_utc_semantics(tmp_path: Path) -> None:
    path = tmp_path / "execution-history.json"
    store = DurableExecutionStore(path)
    _success(store, _record("E-time-1", created_minutes=10))
    _success(store, _record("E-time-2", created_minutes=20))
    store.close()

    history = ExecutionHistoryService(DurableExecutionStore(path))
    page = history.query(
        ExecutionQuery(
            created_at_from="2026-01-01T00:15:00Z",
            created_at_to="2026-01-01T00:25:00Z",
        )
    )
    assert _ids(tuple(page.records)) == ["E-time-2"]
    assert page.records[0].created_at.endswith("+00:00")


@pytest.mark.unit
def test_r_strict_json_serialization_and_rejection(tmp_path: Path) -> None:
    path = tmp_path / "execution-history.json"
    store = DurableExecutionStore(path)
    result = {"values": [1, 2.5, True, None], "label": "ok", "nested": {"x": 1}}
    metadata = {"list": [1, "two", False, None], "mapping": {"a": 1}}
    _success(
        store,
        _record("E-json", metadata=metadata),
        _provenance(metadata=metadata, result=result),
    )
    store.close()

    reopened = DurableExecutionStore(path)
    record = reopened.get("E-json")
    assert record is not None
    assert record.metadata["list"] == [1, "two", False, None]
    assert record.result == {"values": [1, 2.5, True, None], "label": "ok", "nested": {"x": 1}}

    invalid = _record("E-invalid", metadata={"opaque": object()})
    with pytest.raises(ExecutionStoreSerializationError):
        reopened.create(invalid)
    assert reopened.get("E-invalid") is None
    reopened.close()


@pytest.mark.unit
def test_s_artifact_uri_is_durable(tmp_path: Path) -> None:
    path = tmp_path / "execution-history.json"
    store = DurableExecutionStore(path)
    uri = "file:///opt/trendx/data/model-A/artifact.bin"
    _success(store, _record("E-artifact"), _provenance(artifact_uri=uri))
    store.close()

    reopened = DurableExecutionStore(path)
    record = reopened.get("E-artifact")
    assert record is not None
    assert record.artifact_uri == uri
    reopened.close()


@pytest.mark.unit
def test_t_multi_metric_durable_history(tmp_path: Path) -> None:
    path = tmp_path / "execution-history.json"
    store = DurableExecutionStore(path)
    for metric in ("temperature", "humidity", "energy_consumption", "solar_power"):
        _success(store, _record(f"E-metric-{metric}", target_metric=metric))
    store.close()

    history = ExecutionHistoryService(DurableExecutionStore(path))
    assert (
        history.query(ExecutionQuery(target_metric="solar_power")).records[0].target_metric
        == "solar_power"
    )
    assert history.statistics(ExecutionQuery(target_metric="humidity")).total_executions == 1


@pytest.mark.unit
def test_u_multi_entity_durable_history(tmp_path: Path) -> None:
    path = tmp_path / "execution-history.json"
    store = DurableExecutionStore(path)
    _success(store, _record("E-entity-A", entity_id="entity-A"))
    _success(store, _record("E-entity-B", entity_id="entity-B"))
    _success(store, _record("E-tenant-B-entity-A", tenant_id="tenant-B", entity_id="entity-A"))
    store.close()

    history = ExecutionHistoryService(DurableExecutionStore(path))
    assert _ids(tuple(history.query(ExecutionQuery(entity_id="entity-A")).records)) == [
        "E-tenant-B-entity-A",
        "E-entity-A",
    ]
    assert _ids(
        tuple(history.query(ExecutionQuery(tenant_id="tenant-A", entity_id="entity-A")).records)
    ) == ["E-entity-A"]


@pytest.mark.unit
def test_v_fourier_provenance_durable(tmp_path: Path) -> None:
    path = tmp_path / "execution-history.json"
    store = DurableExecutionStore(path)
    _success(
        store,
        _record("E-fourier"),
        _provenance(
            algorithm="Fourier",
            artifact_uri="file:///artifacts/fourier/artifact.bin",
        ),
    )
    store.close()

    history = ExecutionHistoryService(DurableExecutionStore(path))
    page = history.query(ExecutionQuery(algorithm="FOURIER"))
    assert _ids(tuple(page.records)) == ["E-fourier"]
    assert page.records[0].algorithm == "Fourier"
    assert page.records[0].model_id == "model-A"
    assert page.records[0].artifact_uri.endswith("fourier/artifact.bin")


@pytest.mark.unit
def test_w_external_feature_provenance_durable(tmp_path: Path) -> None:
    path = tmp_path / "execution-history.json"
    store = DurableExecutionStore(path)
    _success(
        store,
        _record("E-external-s1"),
        _provenance(
            feature_schema_version="external-S1",
            fingerprint="external-fingerprint-S1",
        ),
    )
    _failed(
        store,
        _record("E-external-s2"),
        code="feature_schema_mismatch",
        provenance=_provenance(
            feature_schema_version="external-S2",
            fingerprint="external-fingerprint-S2",
        ),
    )
    store.close()

    history = ExecutionHistoryService(DurableExecutionStore(path))
    page = history.query(ExecutionQuery(feature_schema_version="external-S2"))
    assert _ids(tuple(page.records)) == ["E-external-s2"]
    assert page.records[0].feature_schema_fingerprint == "external-fingerprint-S2"
    assert page.records[0].status is ExecutionStatus.FAILED


@pytest.mark.unit
def test_x_real_new_process_durable_persistence(tmp_path: Path) -> None:
    path = tmp_path / "execution-history.json"
    store = DurableExecutionStore(path)
    _success(store, _record("E-process-success", created_minutes=1))
    _failed(
        store,
        _record("E-process-failed", created_minutes=2),
        code="feature_schema_mismatch",
    )
    store.close()

    child = r"""
import json
import sys
from trendx.forecasting.execution import DurableExecutionStore
from trendx.forecasting.history import ExecutionHistoryService, ExecutionQuery

path = sys.argv[1]
store = DurableExecutionStore(path)
service = ExecutionHistoryService(store)
by_id = service.get("E-process-success")
by_reference = service.get_by_reference_key("ref-E-process-success")
page = service.query(ExecutionQuery(tenant_id="tenant-A", status="SUCCESS", limit=1, offset=0))
stats = service.statistics()
print(json.dumps({
    "by_id": by_id.to_dict() if by_id else None,
    "by_reference": by_reference.execution_id if by_reference else None,
    "page_ids": [record.execution_id for record in page.records],
    "page_total": page.total,
    "stats": stats.to_dict(),
}))
store.close()
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
        [sys.executable, "-c", child, str(path)],
        cwd=ROOT,
        env=env,
        text=True,
        capture_output=True,
        check=True,
    )
    evidence = json.loads(completed.stdout.strip().splitlines()[-1])
    assert evidence["by_id"]["status"] == "SUCCESS"
    assert evidence["by_id"]["model_id"] == "model-A"
    assert evidence["by_reference"] == "E-process-success"
    assert evidence["page_ids"] == ["E-process-success"]
    assert evidence["page_total"] == 1
    assert evidence["stats"]["total_executions"] == 2
    assert evidence["stats"]["failure_count"] == 1


@pytest.mark.unit
def test_y_restart_preserves_running_without_recovery(tmp_path: Path) -> None:
    path = tmp_path / "execution-history.json"
    store = DurableExecutionStore(path)
    store.create(_record("E-running"))
    store.mark_started("E-running")
    store.close()

    reopened = DurableExecutionStore(path)
    record = reopened.get("E-running")
    assert record is not None
    assert record.status is ExecutionStatus.RUNNING
    assert record.completed_at == ""
    reopened.close()


@pytest.mark.unit
def test_z_corrupt_store_is_rejected_without_overwrite(tmp_path: Path) -> None:
    path = tmp_path / "execution-history.json"
    store = DurableExecutionStore(path)
    _success(store, _record("E-before-corruption"))
    store.close()
    path.write_text("{not-json", encoding="utf-8")

    with pytest.raises(ExecutionStoreSerializationError):
        DurableExecutionStore(path)
    assert path.read_text(encoding="utf-8") == "{not-json"


@pytest.mark.unit
def test_aa_pipeline_can_use_durable_store(tmp_path: Path) -> None:
    path = tmp_path / "execution-history.json"
    store = DurableExecutionStore(path)
    request = ForecastRequest(
        tenant_id="tenant-A",
        entity_type="DEVICE",
        entity_id="entity-A",
        target_metric="temperature",
        frequency="1h",
        horizon=4,
        algorithm=Algorithm.FOURIER,
    )
    index = pd.date_range("2026-01-01", periods=48, freq="30min", tz="UTC")
    frame = pd.DataFrame(
        {"ts": index, "value": [20.0 + (i % 24) * 0.25 for i in range(len(index))]}
    )
    frames = {(request.entity_id, request.target_metric): frame}
    resolver = FeatureResolver(available=frames.keys())
    model = RegisteredModel(
        model_id="pipeline-model",
        tenant_id=request.tenant_id,
        entity_type=request.entity_type,
        entity_id=request.entity_id,
        target_metric=request.target_metric,
        algorithm=Algorithm.FOURIER,
        feature_schema_version="v1",
        feature_schema_fingerprint=request.feature_schema().fingerprint(),
        frequency=request.frequency,
        horizon=request.horizon,
        status=ModelStatus.READY,
        role=ModelRole.CHAMPION,
        model_uri="file:///artifacts/pipeline-model/artifact.bin",
    )

    class Engine:
        def predict(self, horizon: int) -> ForecastResult:
            values = np.arange(float(horizon), dtype=float)
            return ForecastResult(
                values=values,
                lower_bound=values - 1.0,
                upper_bound=values + 1.0,
                model_name="fixture-engine",
            )

    pipeline = ForecastPipeline(
        resolver=resolver,
        builder=DatasetBuilder(),
        models=[model],
        model_loader=lambda _model: Engine(),
        execution_store=store,
    )
    outcome = pipeline.run(
        request,
        frames,
        execution_id="E-pipeline",
        reference_key="ref-pipeline",
    )
    assert outcome.status == "ok"
    assert outcome.execution_record is not None
    store.close()

    history = ExecutionHistoryService(DurableExecutionStore(path))
    record = history.get("E-pipeline")
    assert record is not None
    assert record.status is ExecutionStatus.SUCCESS
    assert record.model_id == "pipeline-model"
    assert record.prediction_count == 4


@pytest.mark.unit
def test_ab_memory_store_remains_available_for_fast_fixtures() -> None:
    store = MemoryExecutionStore()
    record = _record("E-memory")
    store.create(record)
    assert store.get("E-memory") == record
    assert store.get("E-memory") is not None
