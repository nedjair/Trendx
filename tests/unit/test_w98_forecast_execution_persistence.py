"""W98 — forecast execution persistence and provenance.

All executions are synthetic/local.  The tests use a memory ExecutionStore,
temporary Fourier artifacts, and the thin forecast worker adapter.  No
production scheduler, worker, queue, database, ThingsBoard, or MLflow service
is started.
"""

from __future__ import annotations

import gc
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pytest
from trendx.forecasting.artifact import FileSystemArtifactStore
from trendx.forecasting.contract import (
    Algorithm,
    FeatureCategory,
    FeatureDefinition,
    ForecastRequest,
)
from trendx.forecasting.dataset import DatasetBuilder, ForecastDataset
from trendx.forecasting.evaluation import CandidateEvaluation, SelectionResult
from trendx.forecasting.execution import (
    ExecutionProvenance,
    ExecutionRecord,
    ExecutionStatus,
    InvalidExecutionTransitionError,
    MemoryExecutionStore,
)
from trendx.forecasting.pipeline import ForecastPipeline
from trendx.forecasting.registry import (
    ChampionRegistry,
    ModelRole,
    ModelStatus,
    RegisteredModel,
)
from trendx.forecasting.resolution import FeatureResolver
from trendx.forecasting.training import ModelTrainer, TrainingRequest
from trendx.scheduler.discovery import (
    ForecastPolicy,
    MetricPolicy,
    discover,
    plan_tick,
)
from trendx.scheduler.jobs import JobSpec
from trendx.scheduler.registry import SchedulerRegistry
from trendx.services.forecast_worker import (
    ForecastTaskError,
    build_forecast_task_handler,
    execute_forecast_task,
)

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src"


def _frame(base: float = 20.0, periods: int = 72, freq: str = "30min") -> pd.DataFrame:
    index = pd.date_range("2026-01-01", periods=periods, freq=freq, tz="UTC")
    return pd.DataFrame(
        {
            "ts": index,
            "value": [base + (i % 24) * 0.5 for i in range(periods)],
        }
    )


def _request(
    target: str = "temperature",
    *,
    entity: str = "entity-A",
    features: tuple[FeatureDefinition, ...] = (),
    frequency: str = "1h",
    horizon: int = 24,
) -> ForecastRequest:
    return ForecastRequest(
        tenant_id="tenant-1",
        entity_type="DEVICE",
        entity_id=entity,
        target_metric=target,
        features=features,
        frequency=frequency,
        horizon=horizon,
        algorithm=Algorithm.FOURIER,
    )


def _frames(
    request: ForecastRequest,
    *,
    target_base: float = 20.0,
    feature_base: float = 30.0,
) -> dict[tuple[str, str], pd.DataFrame]:
    result: dict[tuple[str, str], pd.DataFrame] = {
        (request.entity_id, request.target_metric): _frame(target_base)
    }
    for feature in request.features:
        result[(feature.entity_scope or request.entity_id, feature.metric)] = _frame(feature_base)
    return result


def _resolver(frames: dict[tuple[str, str], pd.DataFrame]) -> FeatureResolver:
    return FeatureResolver(
        available=frames.keys(),
        external_providers={"fixture-weather"},
    )


def _dataset(
    request: ForecastRequest,
    frames: dict[tuple[str, str], pd.DataFrame] | None = None,
) -> ForecastDataset:
    frames = frames or _frames(request)
    resolved = _resolver(frames).resolve(request)
    assert resolved.ok
    return DatasetBuilder().build(request, resolved, frames=frames)


def _fourier_model() -> Any:
    from trendx.forecasting import create_model

    frame = pd.DataFrame(
        {
            "ds": pd.date_range("2026-01-01", periods=120, freq="1h"),
            "y": 20.0 + 0.05 * np.arange(120) + 5.0 * np.sin(np.arange(120) / 12.0),
        }
    )
    return create_model("Fourier").fit(frame)


def _candidate(model_id: str, request: ForecastRequest) -> RegisteredModel:
    return RegisteredModel(
        model_id=model_id,
        tenant_id=request.tenant_id,
        entity_type=request.entity_type,
        entity_id=request.entity_id,
        target_metric=request.target_metric,
        algorithm=Algorithm.FOURIER,
        feature_schema_version="v1",
        feature_schema_fingerprint=request.feature_schema().fingerprint(),
        frequency=request.frequency,
        horizon=request.horizon,
        training_window="90d",
        status=ModelStatus.READY,
        role=ModelRole.CHALLENGER,
    )


def _selection(model: RegisteredModel) -> SelectionResult:
    return SelectionResult(
        winner_model_id=model.model_id,
        reason="best-score",
        criterion="mae",
        evaluations=(
            CandidateEvaluation(
                model_id=model.model_id,
                algorithm=model.algorithm.value,
                folds=(),
                mean_scores={"mae": 1.0},
                valid_folds=1,
                status="ok",
            ),
        ),
    )


def _train_promoted(
    store: FileSystemArtifactStore,
    registry: ChampionRegistry,
    model_id: str,
    request: ForecastRequest,
    *,
    target_base: float = 20.0,
) -> tuple[RegisteredModel, ForecastDataset, dict[tuple[str, str], pd.DataFrame]]:
    frames = _frames(request, target_base=target_base)
    dataset = _dataset(request, frames)
    candidate = _candidate(model_id, request)
    outcome = ModelTrainer(
        model_factory=lambda _algorithm: _fourier_model(),
        artifact_store=store,
        registry=registry,
    ).train(
        _selection(candidate),
        dataset,
        TrainingRequest(
            tenant_id=request.tenant_id,
            entity_type=request.entity_type,
            entity_id=request.entity_id,
            target_metric=request.target_metric,
            frequency=request.frequency,
            horizon=request.horizon,
            training_window="90d",
            algorithm=Algorithm.FOURIER,
        ),
        {candidate.model_id: candidate},
    )
    assert outcome.status == "ok"
    assert outcome.model is not None
    registry.promote(outcome.model.model_id, dataset, artifact_store=store)
    return outcome.model, dataset, frames


def _pipeline(
    registry: ChampionRegistry,
    store: FileSystemArtifactStore,
    frames: dict[tuple[str, str], pd.DataFrame],
    execution_store: MemoryExecutionStore | None = None,
) -> ForecastPipeline:
    return ForecastPipeline(
        resolver=_resolver(frames),
        builder=DatasetBuilder(),
        champion_registry=registry,
        artifact_store=store,
        execution_store=execution_store,
    )


def _scheduler_payload(
    request: ForecastRequest,
    *,
    window: str = "w98",
) -> dict[str, Any]:
    policy = MetricPolicy(
        frequency=request.frequency,
        horizon=request.horizon,
        algorithm=request.algorithm.value,
        features=request.features,
    )
    eligible, rejected = discover(
        [
            {
                "tenant_id": request.tenant_id,
                "entity_type": request.entity_type,
                "entity_id": request.entity_id,
                "metric_name": request.target_metric,
            }
        ],
        ForecastPolicy(overrides={request.target_metric: policy}),
    )
    assert eligible and not rejected
    plan = plan_tick(eligible, job_type="trendx_forecast", window=window)
    assert len(plan.payloads) == 1
    return dict(plan.payloads[0])


def _run_task(
    payload: dict[str, Any],
    pipeline: ForecastPipeline,
    frames: dict[tuple[str, str], pd.DataFrame],
) -> dict[str, Any]:
    handler = build_forecast_task_handler(pipeline, frames)
    return handler(payload, "task-w98", "worker-execution-w98")


def _record(
    request: ForecastRequest,
    *,
    execution_id: str = "E1",
    reference_key: str = "ref-E1",
) -> ExecutionRecord:
    schema = request.feature_schema()
    return ExecutionRecord(
        execution_id=execution_id,
        reference_key=reference_key,
        tenant_id=request.tenant_id,
        entity_type=request.entity_type,
        entity_id=request.entity_id,
        target_metric=request.target_metric,
        frequency=request.frequency,
        horizon=request.horizon,
        algorithm=request.algorithm.value,
        feature_schema_version=schema.schema_version,
        feature_schema_fingerprint=schema.fingerprint(),
        created_at="2026-01-01T00:00:00+00:00",
    )


def _provenance(
    model: RegisteredModel, *, result: dict[str, Any] | None = None
) -> ExecutionProvenance:
    return ExecutionProvenance(
        model_id=model.model_id,
        model_version=model.model_version,
        algorithm=model.algorithm.value,
        feature_schema_version=model.feature_schema_version,
        feature_schema_fingerprint=model.feature_schema_fingerprint,
        artifact_uri=model.model_uri,
        prediction_count=24,
        metadata={"source": "test"},
        result=result,
    )


def _artifact_dir(uri: str) -> Path:
    assert uri.startswith("file://")
    return Path(uri[7:])


@pytest.mark.unit
def test_a_execution_record_creation() -> None:
    request = _request()
    store = MemoryExecutionStore()
    record = store.create(_record(request))
    assert record.status is ExecutionStatus.PENDING
    assert store.get("E1") == record


@pytest.mark.unit
def test_b_pending_state_is_explicit() -> None:
    request = _request()
    record = _record(request)
    assert record.status.value == "PENDING"
    assert record.started_at == ""
    assert record.completed_at == ""


@pytest.mark.unit
def test_c_running_transition() -> None:
    store = MemoryExecutionStore()
    store.create(_record(_request()))
    record = store.mark_started("E1")
    assert record.status is ExecutionStatus.RUNNING
    assert record.started_at


@pytest.mark.unit
def test_d_success_transition_and_prediction_count() -> None:
    request = _request()
    model = _candidate("m1", request)
    store = MemoryExecutionStore()
    store.create(_record(request))
    store.mark_started("E1")
    record = store.mark_success("E1", _provenance(model))
    assert record.status is ExecutionStatus.SUCCESS
    assert record.completed_at
    assert record.prediction_count == 24
    assert record.error_code == ""


@pytest.mark.unit
def test_e_failed_transition() -> None:
    request = _request()
    store = MemoryExecutionStore()
    store.create(_record(request))
    store.mark_started("E1")
    record = store.mark_failed(
        "E1",
        error_code="artifact-missing",
        error_reason="artifact was not found",
    )
    assert record.status is ExecutionStatus.FAILED
    assert record.error_code == "artifact-missing"
    assert record.error_reason == "artifact was not found"
    assert record.completed_at


@pytest.mark.unit
def test_f_invalid_transitions_are_refused() -> None:
    request = _request()
    store = MemoryExecutionStore()
    store.create(_record(request))
    with pytest.raises(InvalidExecutionTransitionError):
        store.mark_success("E1", _provenance(_candidate("m1", request)))
    store.mark_started("E1")
    with pytest.raises(InvalidExecutionTransitionError):
        store.mark_started("E1")
    store.mark_success("E1", _provenance(_candidate("m1", request)))
    with pytest.raises(InvalidExecutionTransitionError):
        store.mark_failed("E1", error_code="late", error_reason="late failure")


@pytest.mark.unit
def test_g_model_identity_provenance_is_persisted() -> None:
    request = _request()
    model = _candidate("model-identity", request)
    store = MemoryExecutionStore()
    store.create(_record(request))
    store.mark_started("E1")
    record = store.mark_success("E1", _provenance(model))
    assert record.model_id == model.model_id
    assert record.model_version == model.model_version
    assert record.algorithm == model.algorithm.value


@pytest.mark.unit
def test_h_artifact_uri_provenance_is_persisted(tmp_path: Path) -> None:
    request = _request()
    model, _dataset, _frames = _train_promoted(
        FileSystemArtifactStore(tmp_path / "artifacts"),
        ChampionRegistry(),
        "artifact-provenance",
        request,
    )
    store = MemoryExecutionStore()
    store.create(_record(request))
    store.mark_started("E1")
    record = store.mark_success("E1", _provenance(model))
    assert record.artifact_uri == model.model_uri
    assert record.artifact_uri.startswith("file://")


@pytest.mark.unit
def test_i_feature_fingerprint_provenance_is_persisted() -> None:
    request = _request(features=(FeatureDefinition(name="humidity", metric="humidity"),))
    model = _candidate("fingerprint", request)
    store = MemoryExecutionStore()
    store.create(_record(request))
    store.mark_started("E1")
    record = store.mark_success("E1", _provenance(model))
    assert record.feature_schema_fingerprint == request.feature_schema().fingerprint()
    assert record.feature_schema_version == "v1"


@pytest.mark.unit
def test_j_execution_and_reference_ids_are_preserved() -> None:
    request = _request()
    store = MemoryExecutionStore()
    record = store.create(_record(request, execution_id="E-42", reference_key="ref-42"))
    assert record.execution_id == "E-42"
    assert record.reference_key == "ref-42"
    assert store.get_by_reference_key("ref-42") == record


@pytest.mark.unit
def test_k_execution_record_serialization_round_trip() -> None:
    request = _request()
    record = _record(request)
    restored = ExecutionRecord.from_dict(record.to_dict())
    assert restored == record
    assert restored.to_dict()["status"] == "PENDING"


@pytest.mark.unit
def test_l_duplicate_reference_is_not_executed_twice(tmp_path: Path) -> None:
    artifact_store = FileSystemArtifactStore(tmp_path / "artifacts")
    registry = ChampionRegistry()
    request = _request()
    model, _dataset, frames = _train_promoted(artifact_store, registry, "duplicate", request)
    execution_store = MemoryExecutionStore()
    pipeline = _pipeline(registry, artifact_store, frames, execution_store)
    payload = _scheduler_payload(request)

    first = _run_task(payload, pipeline, frames)
    second = _run_task(payload, pipeline, frames)

    assert first == second
    records = execution_store.list(reference_key=payload["reference_key"])
    assert len(records) == 1
    assert records[0].status is ExecutionStatus.SUCCESS
    assert records[0].model_id == model.model_id


@pytest.mark.unit
def test_m_retry_keeps_failed_history_and_uses_new_execution_id(tmp_path: Path) -> None:
    artifact_store = FileSystemArtifactStore(tmp_path / "artifacts")
    registry = ChampionRegistry()
    request = _request()
    model, _dataset, frames = _train_promoted(artifact_store, registry, "retry", request)
    execution_store = MemoryExecutionStore()
    pipeline = _pipeline(registry, artifact_store, frames, execution_store)
    payload = _scheduler_payload(request)
    failed_payload = dict(payload)
    failed_payload["execution_id"] = "E-failed"
    failed_payload["reference_key"] = payload["reference_key"] + "-retry-case"
    artifact_path = _artifact_dir(model.model_uri) / "model.bin"
    original_artifact = artifact_path.read_bytes()
    artifact_path.unlink()
    with pytest.raises(RuntimeError, match="artifact-missing"):
        _run_task(failed_payload, pipeline, frames)
    failed = execution_store.get("E-failed")
    assert failed is not None and failed.status is ExecutionStatus.FAILED
    artifact_path.write_bytes(original_artifact)

    success_payload = dict(payload)
    success_payload["execution_id"] = "E-retry"
    success_payload["reference_key"] = failed_payload["reference_key"]
    result = _run_task(success_payload, pipeline, frames)
    assert result["model_id"] == model.model_id
    history = execution_store.list(reference_key=failed_payload["reference_key"])
    assert [r.status for r in history] == [ExecutionStatus.FAILED, ExecutionStatus.SUCCESS]
    assert {r.execution_id for r in history} == {"E-failed", "E-retry"}


@pytest.mark.unit
def test_n_no_champion_marks_execution_failed_without_training(tmp_path: Path) -> None:
    base = tmp_path / "artifacts"
    artifact_store = FileSystemArtifactStore(base)
    execution_store = MemoryExecutionStore()
    request = _request()
    payload = _scheduler_payload(request)
    pipeline = _pipeline(ChampionRegistry(), artifact_store, _frames(request), execution_store)

    with pytest.raises(RuntimeError, match="no-compatible-champion"):
        _run_task(payload, pipeline, _frames(request))

    record = execution_store.get_by_reference_key(payload["reference_key"])
    assert record is not None
    assert record.status is ExecutionStatus.FAILED
    assert record.error_code == "no-compatible-champion"
    assert list(base.iterdir()) == []


@pytest.mark.unit
def test_o_incompatible_champion_marks_execution_failed(tmp_path: Path) -> None:
    artifact_store = FileSystemArtifactStore(tmp_path / "artifacts")
    registry = ChampionRegistry()
    trained_request = _request()
    model, _dataset, _trained_frames = _train_promoted(
        artifact_store, registry, "incompatible", trained_request
    )
    serving_request = _request(frequency="15m")
    frames = _frames(serving_request)
    execution_store = MemoryExecutionStore()
    pipeline = _pipeline(registry, artifact_store, frames, execution_store)
    payload = _scheduler_payload(serving_request)

    with pytest.raises(RuntimeError, match="frequency_mismatch"):
        _run_task(payload, pipeline, frames)

    record = execution_store.get_by_reference_key(payload["reference_key"])
    assert record is not None
    assert record.status is ExecutionStatus.FAILED
    assert record.error_code == "frequency_mismatch"
    champion = registry.get(model.model_id)
    assert champion is not None
    assert champion.role is ModelRole.CHAMPION


@pytest.mark.unit
@pytest.mark.parametrize("mode", ["missing", "corrupt"])
def test_p_artifact_failure_marks_execution_failed(tmp_path: Path, mode: str) -> None:
    artifact_store = FileSystemArtifactStore(tmp_path / "artifacts")
    registry = ChampionRegistry()
    request = _request()
    model, _dataset, frames = _train_promoted(artifact_store, registry, f"artifact-{mode}", request)
    if mode == "missing":
        (_artifact_dir(model.model_uri) / "model.bin").unlink()
    else:
        with (_artifact_dir(model.model_uri) / "model.bin").open("ab") as handle:
            handle.write(b"corrupt")
    execution_store = MemoryExecutionStore()
    pipeline = _pipeline(registry, artifact_store, frames, execution_store)
    payload = _scheduler_payload(request)

    with pytest.raises(RuntimeError, match="artifact"):
        _run_task(payload, pipeline, frames)

    record = execution_store.get_by_reference_key(payload["reference_key"])
    assert record is not None
    assert record.status is ExecutionStatus.FAILED
    assert record.model_id == model.model_id
    assert record.model_version == model.model_version
    assert record.artifact_uri == model.model_uri
    champion = registry.get(model.model_id)
    assert champion is not None
    assert champion.role is ModelRole.CHAMPION


@pytest.mark.unit
def test_q_multi_metric_execution_records_are_generic(tmp_path: Path) -> None:
    artifact_store = FileSystemArtifactStore(tmp_path / "artifacts")
    registry = ChampionRegistry()
    execution_store = MemoryExecutionStore()
    for target in ("temperature", "humidity", "energy_consumption", "solar_power"):
        request = _request(target)
        model, _dataset, frames = _train_promoted(
            artifact_store, registry, f"metric-{target}", request
        )
        payload = _scheduler_payload(request)
        pipeline = _pipeline(registry, artifact_store, frames, execution_store)
        result = _run_task(payload, pipeline, frames)
        record = execution_store.get_by_reference_key(payload["reference_key"])
        assert result["target_metric"] == target
        assert record is not None
        assert record.status is ExecutionStatus.SUCCESS
        assert record.target_metric == target
        assert record.model_id == model.model_id
        assert record.feature_schema_fingerprint == request.feature_schema().fingerprint()


@pytest.mark.unit
def test_r_multi_entity_execution_records_are_isolated(tmp_path: Path) -> None:
    artifact_store = FileSystemArtifactStore(tmp_path / "artifacts")
    registry = ChampionRegistry()
    execution_store = MemoryExecutionStore()
    request_a = _request("X", entity="entity-A")
    request_b = _request("X", entity="entity-B")
    model_a, _dataset_a, frames_a = _train_promoted(
        artifact_store, registry, "entity-A", request_a, target_base=20.0
    )
    model_b, _dataset_b, frames_b = _train_promoted(
        artifact_store, registry, "entity-B", request_b, target_base=80.0
    )
    payload_a = _scheduler_payload(request_a)
    payload_b = _scheduler_payload(request_b)
    pipeline_a = _pipeline(registry, artifact_store, frames_a, execution_store)
    pipeline_b = _pipeline(registry, artifact_store, frames_b, execution_store)
    _run_task(payload_a, pipeline_a, frames_a)
    _run_task(payload_b, pipeline_b, frames_b)
    record_a = execution_store.get_by_reference_key(payload_a["reference_key"])
    record_b = execution_store.get_by_reference_key(payload_b["reference_key"])
    assert record_a is not None and record_b is not None
    assert record_a.execution_id != record_b.execution_id
    assert record_a.reference_key != record_b.reference_key
    assert record_a.entity_id == "entity-A"
    assert record_b.entity_id == "entity-B"
    assert record_a.model_id == model_a.model_id
    assert record_b.model_id == model_b.model_id
    assert record_a.artifact_uri != record_b.artifact_uri


@pytest.mark.unit
def test_s_fourier_success_record_contains_model_provenance(tmp_path: Path) -> None:
    artifact_store = FileSystemArtifactStore(tmp_path / "artifacts")
    registry = ChampionRegistry()
    request = _request()
    model, _dataset, frames = _train_promoted(artifact_store, registry, "fourier-record", request)
    execution_store = MemoryExecutionStore()
    pipeline = _pipeline(registry, artifact_store, frames, execution_store)
    payload = _scheduler_payload(request)

    result = _run_task(payload, pipeline, frames)

    record = execution_store.get_by_reference_key(payload["reference_key"])
    assert result["model_id"] == model.model_id
    assert record is not None
    assert record.status is ExecutionStatus.SUCCESS
    assert record.algorithm == Algorithm.FOURIER.value
    assert record.model_id == model.model_id
    assert record.model_version == model.model_version
    assert record.artifact_uri == model.model_uri
    assert (_artifact_dir(model.model_uri) / "train_history.csv").exists()


@pytest.mark.unit
def test_t_external_feature_success_and_failure_records(tmp_path: Path) -> None:
    artifact_store = FileSystemArtifactStore(tmp_path / "artifacts")
    registry = ChampionRegistry()
    feature_s1 = (
        FeatureDefinition(
            name="weather_signal",
            metric="weather_signal",
            source="fixture-weather",
            category=FeatureCategory.EXTERNAL_FORECAST,
        ),
    )
    feature_s2 = (
        *feature_s1,
        FeatureDefinition(
            name="weather_pressure",
            metric="weather_pressure",
            source="fixture-weather",
            category=FeatureCategory.EXTERNAL_FORECAST,
        ),
    )
    trained_request = _request(features=feature_s1)
    model, _dataset, _trained_frames = _train_promoted(
        artifact_store, registry, "external-record", trained_request
    )
    execution_store = MemoryExecutionStore()

    success_request = _request(features=feature_s1)
    success_frames = _frames(success_request)
    success_payload = _scheduler_payload(success_request)
    success_pipeline = _pipeline(registry, artifact_store, success_frames, execution_store)
    success_result = _run_task(success_payload, success_pipeline, success_frames)
    success_record = execution_store.get_by_reference_key(success_payload["reference_key"])
    assert success_result["model_id"] == model.model_id
    assert success_record is not None and success_record.status is ExecutionStatus.SUCCESS

    failure_request = _request(features=feature_s2)
    failure_frames = _frames(failure_request)
    failure_payload = _scheduler_payload(failure_request, window="w98-feature-failure")
    failure_pipeline = _pipeline(registry, artifact_store, failure_frames, execution_store)
    with pytest.raises(RuntimeError, match="feature_schema_mismatch"):
        _run_task(failure_payload, failure_pipeline, failure_frames)
    failure_record = execution_store.get_by_reference_key(failure_payload["reference_key"])
    assert failure_record is not None
    assert failure_record.status is ExecutionStatus.FAILED
    assert failure_record.error_code == "feature_schema_mismatch"
    assert (
        failure_record.feature_schema_fingerprint == failure_request.feature_schema().fingerprint()
    )


@pytest.mark.unit
def test_u_no_auto_promotion_and_champion_preserved_after_success_and_failure(
    tmp_path: Path,
) -> None:
    artifact_store = FileSystemArtifactStore(tmp_path / "artifacts")
    registry = ChampionRegistry()
    request = _request()
    model, _dataset, frames = _train_promoted(artifact_store, registry, "preserve", request)
    champion = registry.get(model.model_id)
    assert champion is not None
    before = champion.to_dict()
    execution_store = MemoryExecutionStore()
    pipeline = _pipeline(registry, artifact_store, frames, execution_store)
    payload = _scheduler_payload(request)
    _run_task(payload, pipeline, frames)
    current = registry.get(model.model_id)
    assert current is not None
    assert current.to_dict() == before

    bad_payload = dict(payload)
    bad_payload["execution_id"] = "E-bad"
    bad_payload["reference_key"] = payload["reference_key"] + "-bad"
    bad_payload["frequency"] = "15m"
    bad_frames = _frames(_request(frequency="15m"))
    bad_pipeline = _pipeline(registry, artifact_store, bad_frames, execution_store)
    with pytest.raises(RuntimeError):
        _run_task(bad_payload, bad_pipeline, bad_frames)
    current = registry.get(model.model_id)
    assert current is not None
    assert current.to_dict() == before


@pytest.mark.unit
def test_v_new_process_scheduler_worker_pipeline_execution_record(tmp_path: Path) -> None:
    artifact_store = FileSystemArtifactStore(tmp_path / "artifacts")
    registry = ChampionRegistry()
    request = _request()
    model, _dataset, _frames = _train_promoted(artifact_store, registry, "process-record", request)
    persisted = registry.get(model.model_id)
    assert persisted is not None
    model_payload = persisted.to_dict()

    del model
    del registry
    gc.collect()

    child = r"""
import json
import sys

import pandas as pd

from trendx.forecasting.artifact import FileSystemArtifactStore
from trendx.forecasting.contract import Algorithm
from trendx.forecasting.execution import MemoryExecutionStore
from trendx.forecasting.pipeline import ForecastPipeline
from trendx.forecasting.registry import ChampionRegistry, ModelRole, ModelStatus, RegisteredModel
from trendx.scheduler.discovery import ForecastPolicy, MetricCandidate, MetricPolicy, discover, plan_tick
from trendx.services.forecast_worker import build_forecast_task_handler

payload = json.load(sys.stdin)
model_data = dict(payload["model"])
model_data["algorithm"] = Algorithm(model_data["algorithm"])
model_data["status"] = ModelStatus(model_data["status"])
model_data["role"] = ModelRole(model_data["role"])
registry = ChampionRegistry()
registry.register(RegisteredModel(**model_data))
request_data = {
    "tenant_id": payload["tenant_id"],
    "entity_type": payload["entity_type"],
    "entity_id": payload["entity_id"],
    "metric_name": payload["target_metric"],
}
policy = MetricPolicy(
    frequency=payload["frequency"],
    horizon=payload["horizon"],
    algorithm=payload["algorithm"],
)
eligible, rejected = discover(
    [request_data], ForecastPolicy(overrides={payload["target_metric"]: policy})
)
assert eligible and not rejected
task = dict(plan_tick(eligible, job_type="trendx_forecast", window="w98-process").payloads[0])
index = pd.date_range("2026-01-01", periods=72, freq="30min", tz="UTC")
frames = {
    (payload["entity_id"], payload["target_metric"]): pd.DataFrame(
        {"ts": index, "value": [20.0 + (i % 24) * 0.5 for i in range(len(index))]}
    )
}
execution_store = MemoryExecutionStore()
pipeline = ForecastPipeline(
    champion_registry=registry,
    artifact_store=FileSystemArtifactStore(payload["base_path"]),
    execution_store=execution_store,
)
handler = build_forecast_task_handler(pipeline, frames)
result = handler(task, "task-process-b", "worker-process-b")
record = execution_store.get_by_reference_key(task["reference_key"])
assert record is not None and record.status.value == "SUCCESS"
print(json.dumps({"task": task, "result": result, "record": record.to_dict()}))
"""
    payload = {
        "model": model_payload,
        "base_path": str(tmp_path / "artifacts"),
        "tenant_id": "tenant-1",
        "entity_type": "DEVICE",
        "entity_id": "entity-A",
        "target_metric": "temperature",
        "frequency": "1h",
        "horizon": 24,
        "algorithm": "Fourier",
    }
    env = os.environ.copy()
    env["PYTHONPATH"] = str(SRC) + os.pathsep + env.get("PYTHONPATH", "")
    env.update(
        {
            "PYTHONDONTWRITEBYTECODE": "1",
            "TRENDX_SCHEDULER_ENABLED": "false",
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
        input=json.dumps(payload),
        text=True,
        capture_output=True,
        check=True,
    )
    evidence = json.loads(completed.stdout.strip().splitlines()[-1])
    assert evidence["result"]["model_id"] == model_payload["model_id"]
    assert evidence["record"]["status"] == "SUCCESS"
    assert evidence["record"]["execution_id"] == evidence["task"]["execution_id"]
    assert evidence["record"]["reference_key"] == evidence["task"]["reference_key"]
    assert evidence["record"]["model_id"] == model_payload["model_id"]
    assert evidence["record"]["artifact_uri"] == model_payload["model_uri"]
    assert len(evidence["result"]["result"]["values"]) == 24


@pytest.mark.unit
def test_w_invalid_payload_does_not_create_false_success(tmp_path: Path) -> None:
    execution_store = MemoryExecutionStore()
    pipeline = ForecastPipeline(execution_store=execution_store)
    with pytest.raises(ForecastTaskError):
        execute_forecast_task({}, pipeline, {})
    assert execution_store.list() == ()


@pytest.mark.unit
def test_x_scheduler_idempotence_still_prevents_duplicate_dispatch(tmp_path: Path) -> None:
    artifact_store = FileSystemArtifactStore(tmp_path / "artifacts")
    registry = ChampionRegistry()
    request = _request()
    _model, _dataset, frames = _train_promoted(
        artifact_store, registry, "schedule-idempotence", request
    )
    execution_store = MemoryExecutionStore()
    payload = _scheduler_payload(request)
    calls: list[dict[str, Any]] = []

    def handler(data: dict[str, Any]) -> dict[str, Any]:
        calls.append(dict(data))
        return _run_task(data, _pipeline(registry, artifact_store, frames, execution_store), frames)

    scheduler = SchedulerRegistry(
        specs=(JobSpec("forecast-run", "trendx_forecast", "1h", "test"),),
    )
    scheduler.register_handler("trendx_forecast", handler)
    first = scheduler.trigger("forecast-run", payload, idempotency_key=payload["reference_key"])
    second = scheduler.trigger("forecast-run", payload, idempotency_key=payload["reference_key"])
    assert first["status"] == "ok"
    assert second["deduplicated"] is True
    assert len(calls) == 1
    assert len(execution_store.list()) == 1


@pytest.mark.unit
def test_y_execution_store_does_not_depend_on_database() -> None:
    store = MemoryExecutionStore()
    assert isinstance(store, MemoryExecutionStore)
    assert not hasattr(store, "session")
    assert not hasattr(store, "engine")


@pytest.mark.unit
def test_z_worker_reexecution_uses_existing_reference_without_new_success(tmp_path: Path) -> None:
    artifact_store = FileSystemArtifactStore(tmp_path / "artifacts")
    registry = ChampionRegistry()
    request = _request()
    model, _dataset, frames = _train_promoted(artifact_store, registry, "reexecution", request)
    execution_store = MemoryExecutionStore()
    payload = _scheduler_payload(request)
    pipeline = _pipeline(registry, artifact_store, frames, execution_store)
    first = _run_task(payload, pipeline, frames)
    duplicate_payload = dict(payload)
    duplicate_payload["execution_id"] = "E-duplicate"
    second = _run_task(duplicate_payload, pipeline, frames)
    assert first["model_id"] == second["model_id"] == model.model_id
    assert len(execution_store.list()) == 1
