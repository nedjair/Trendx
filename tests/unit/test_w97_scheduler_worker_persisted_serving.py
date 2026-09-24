"""W97 — scheduler task → worker adapter → W96 persisted serving.

All execution is synthetic and in-process except the explicit new-process
proof.  The test never imports the production worker module, starts a queue,
opens a database, contacts ThingsBoard, or activates a scheduler.
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
    MetricCandidate,
    MetricPolicy,
    discover,
    plan_tick,
)
from trendx.scheduler.fanout import forecast_reference_key
from trendx.scheduler.jobs import JobSpec
from trendx.scheduler.registry import SchedulerRegistry
from trendx.services.forecast_worker import (
    ForecastTaskError,
    _run_generic_forecast,
    build_forecast_task_handler,
    execute_forecast_task,
    payload_to_request,
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
        key = (feature.entity_scope or request.entity_id, feature.metric)
        result[key] = _frame(feature_base)
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
    promote: bool = True,
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
    if promote:
        registry.promote(outcome.model.model_id, dataset, artifact_store=store)
    return outcome.model, dataset, frames


class _RecordingArtifactStore(FileSystemArtifactStore):
    def __init__(self, base_path: str | Path) -> None:
        super().__init__(base_path)
        self.loaded_uris: list[str] = []

    def load(self, uri: str, algorithm: str) -> Any:
        self.loaded_uris.append(uri)
        return super().load(uri, algorithm)


class _CountingPipeline(ForecastPipeline):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.run_calls = 0

    def run(self, *args: Any, **kwargs: Any) -> Any:
        self.run_calls += 1
        return super().run(*args, **kwargs)


def _pipeline(
    registry: ChampionRegistry,
    store: FileSystemArtifactStore,
    frames: dict[tuple[str, str], pd.DataFrame],
    *,
    counting: bool = False,
) -> ForecastPipeline:
    kwargs: dict[str, Any] = {
        "resolver": _resolver(frames),
        "builder": DatasetBuilder(),
        "champion_registry": registry,
        "artifact_store": store,
    }
    return (_CountingPipeline if counting else ForecastPipeline)(**kwargs)


def _scheduler_payload(
    request: ForecastRequest,
    *,
    window: str = "w97",
) -> dict[str, Any]:
    candidate = MetricCandidate(
        tenant_id=request.tenant_id,
        entity_type=request.entity_type,
        entity_id=request.entity_id,
        metric_name=request.target_metric,
        policy=MetricPolicy(
            frequency=request.frequency,
            horizon=request.horizon,
            algorithm=request.algorithm.value,
            features=request.features,
        ),
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
        ForecastPolicy(overrides={request.target_metric: candidate.policy}),
    )
    assert eligible and not rejected
    plan = plan_tick(eligible, job_type="trendx_forecast", window=window)
    assert len(plan.payloads) == 1
    return dict(plan.payloads[0])


def _run_worker_task(
    payload: dict[str, Any],
    pipeline: ForecastPipeline,
    frames: dict[tuple[str, str], pd.DataFrame],
) -> dict[str, Any]:
    handler = build_forecast_task_handler(pipeline, frames)
    return handler(payload, "task-w97", "worker-execution-w97")


def _artifact_dir(uri: str) -> Path:
    assert uri.startswith("file://")
    return Path(uri[7:])


@pytest.mark.unit
def test_a_scheduler_creates_generic_forecast_task():
    request = _request("temperature")
    payload = _scheduler_payload(request)

    assert payload["job_type"] == "trendx_forecast"
    assert payload["tenant_id"] == request.tenant_id
    assert payload["entity_id"] == request.entity_id
    assert payload["metric_name"] == request.target_metric
    assert payload["target_metric"] == request.target_metric
    assert payload["frequency"] == request.frequency
    assert payload["horizon"] == request.horizon
    assert payload["features"] == []
    assert payload["execution_id"]
    assert payload["reference_key"] == forecast_reference_key(
        tenant_id=request.tenant_id,
        entity_id=request.entity_id,
        metric_name=request.target_metric,
        job_type="trendx_forecast",
        window="w97",
    )
    forbidden = {"model", "ForecastModel", "registry", "ChampionRegistry", "artifact_store"}
    assert forbidden.isdisjoint(payload)


@pytest.mark.unit
def test_b_worker_reconstructs_forecast_request():
    request = _request("humidity", horizon=48)
    payload = _scheduler_payload(request)
    rebuilt = payload_to_request(payload)

    assert rebuilt.tenant_id == request.tenant_id
    assert rebuilt.entity_type == request.entity_type
    assert rebuilt.entity_id == request.entity_id
    assert rebuilt.target_metric == request.target_metric
    assert rebuilt.horizon == request.horizon
    assert rebuilt.frequency == request.frequency
    assert rebuilt.algorithm is request.algorithm


@pytest.mark.unit
def test_c_worker_invokes_pipeline_once(tmp_path):
    store = FileSystemArtifactStore(tmp_path / "artifacts")
    registry = ChampionRegistry()
    request = _request("temperature")
    model, _dataset, frames = _train_promoted(store, registry, "worker-once", request)
    pipeline = _pipeline(registry, store, frames, counting=True)
    payload = _scheduler_payload(request)

    result = _run_generic_forecast(
        payload,
        "task-w97",
        "worker-execution-w97",
        pipeline=pipeline,
        frames=frames,
    )

    assert pipeline.run_calls == 1
    assert result["model_id"] == model.model_id


@pytest.mark.unit
def test_d_persisted_champion_is_loaded_and_envelope_succeeds(tmp_path):
    store = _RecordingArtifactStore(tmp_path / "artifacts")
    registry = ChampionRegistry()
    request = _request("temperature")
    model, _dataset, frames = _train_promoted(store, registry, "worker-load", request)
    payload = _scheduler_payload(request)

    result = _run_worker_task(payload, _pipeline(registry, store, frames), frames)

    assert store.loaded_uris == [model.model_uri]
    assert result["model_id"] == model.model_id
    assert result["model_version"] == model.model_version
    assert result["target_metric"] == request.target_metric
    assert len(result["result"]["values"]) == request.horizon


@pytest.mark.unit
def test_e_no_champion_is_controlled_failure_without_training(tmp_path):
    base = tmp_path / "artifacts"
    store = FileSystemArtifactStore(base)
    registry = ChampionRegistry()
    request = _request("temperature")
    payload = _scheduler_payload(request)

    with pytest.raises(RuntimeError, match="no-compatible-champion"):
        _run_worker_task(payload, _pipeline(registry, store, _frames(request)), _frames(request))

    assert list(base.iterdir()) == []
    assert registry.champions() == []


@pytest.mark.unit
def test_f_incompatible_champion_is_controlled_failure(tmp_path):
    store = FileSystemArtifactStore(tmp_path / "artifacts")
    registry = ChampionRegistry()
    trained_request = _request("temperature")
    model, _dataset, _trained_frames = _train_promoted(
        store, registry, "worker-incompatible", trained_request
    )
    serving_request = _request("temperature", frequency="15m")
    frames = _frames(serving_request)
    payload = _scheduler_payload(serving_request)

    with pytest.raises(RuntimeError, match="frequency_mismatch"):
        _run_worker_task(payload, _pipeline(registry, store, frames), frames)

    assert registry.get(model.model_id).role is ModelRole.CHAMPION


@pytest.mark.unit
def test_g_missing_artifact_is_controlled_failure(tmp_path):
    store = FileSystemArtifactStore(tmp_path / "artifacts")
    registry = ChampionRegistry()
    request = _request("temperature")
    model, _dataset, frames = _train_promoted(store, registry, "worker-missing", request)
    (_artifact_dir(model.model_uri) / "model.bin").unlink()
    payload = _scheduler_payload(request)

    with pytest.raises(RuntimeError, match="artifact-missing"):
        _run_worker_task(payload, _pipeline(registry, store, frames), frames)


@pytest.mark.unit
def test_h_corrupted_artifact_is_controlled_failure(tmp_path):
    store = FileSystemArtifactStore(tmp_path / "artifacts")
    registry = ChampionRegistry()
    request = _request("temperature")
    model, _dataset, frames = _train_promoted(store, registry, "worker-corrupt", request)
    with (_artifact_dir(model.model_uri) / "model.bin").open("ab") as handle:
        handle.write(b"corrupt")
    payload = _scheduler_payload(request)

    with pytest.raises(RuntimeError, match="artifact-corrupted"):
        _run_worker_task(payload, _pipeline(registry, store, frames), frames)


@pytest.mark.unit
def test_i_invalid_payload_never_reaches_pipeline(tmp_path):
    calls: list[bool] = []

    class _NeverPipeline(ForecastPipeline):
        def run(self, *args: Any, **kwargs: Any) -> Any:
            calls.append(True)
            return super().run(*args, **kwargs)

    with pytest.raises(ForecastTaskError):
        execute_forecast_task({}, _NeverPipeline(), {})
    assert calls == []


@pytest.mark.unit
def test_j_no_auto_promotion_and_champion_preservation(tmp_path):
    store = FileSystemArtifactStore(tmp_path / "artifacts")
    registry = ChampionRegistry()
    request = _request("temperature")
    model, _dataset, frames = _train_promoted(store, registry, "champion-A", request)
    before = registry.get(model.model_id).to_dict()
    payload = _scheduler_payload(request)

    result = _run_worker_task(payload, _pipeline(registry, store, frames), frames)

    assert result["model_id"] == model.model_id
    assert registry.get(model.model_id).to_dict() == before
    assert registry.get(model.model_id).role is ModelRole.CHAMPION
    assert registry.get(model.model_id).status is ModelStatus.READY


@pytest.mark.unit
def test_k_scheduler_duplicate_reference_is_deduplicated():
    request = _request("temperature")
    candidate = MetricCandidate(
        request.tenant_id,
        request.entity_type,
        request.entity_id,
        request.target_metric,
        MetricPolicy(
            frequency=request.frequency,
            horizon=request.horizon,
            algorithm=request.algorithm.value,
        ),
    )
    eligible, _ = discover(
        [
            {
                "tenant_id": request.tenant_id,
                "entity_type": request.entity_type,
                "entity_id": request.entity_id,
                "metric_name": request.target_metric,
            }
        ]
    )
    dispatched: set[str] = set()
    first = plan_tick([candidate], job_type="trendx_forecast", window="w97", dispatched=dispatched)
    second = plan_tick([candidate], job_type="trendx_forecast", window="w97", dispatched=dispatched)
    third = plan_tick([candidate], job_type="trendx_forecast", window="w97", dispatched=dispatched)
    assert len(first.payloads) == 1
    assert len(second.payloads) == 0
    assert second.skipped_duplicates == (first.payloads[0]["reference_key"],)
    assert len(third.skipped_duplicates) == 1
    assert eligible


@pytest.mark.unit
def test_l_execution_id_is_preserved_from_scheduler_payload(tmp_path):
    store = FileSystemArtifactStore(tmp_path / "artifacts")
    registry = ChampionRegistry()
    request = _request("temperature")
    _model, _dataset, frames = _train_promoted(store, registry, "execution-id", request)
    payload = _scheduler_payload(request)
    pipeline = _pipeline(registry, store, frames)
    handler = build_forecast_task_handler(pipeline, frames)

    result = handler(payload, "different-task", "different-worker-execution")

    assert result["execution_id"] == payload["execution_id"]


@pytest.mark.unit
def test_m_reference_key_is_preserved_by_worker_result(tmp_path):
    store = FileSystemArtifactStore(tmp_path / "artifacts")
    registry = ChampionRegistry()
    request = _request("temperature")
    _model, _dataset, frames = _train_promoted(store, registry, "reference-key", request)
    payload = _scheduler_payload(request)

    result = _run_worker_task(payload, _pipeline(registry, store, frames), frames)

    assert result["reference_key"] == payload["reference_key"]


@pytest.mark.unit
def test_n_scheduler_registry_duplicate_dispatch_runs_handler_once(tmp_path):
    store = FileSystemArtifactStore(tmp_path / "artifacts")
    registry = ChampionRegistry()
    request = _request("temperature")
    model, _dataset, frames = _train_promoted(store, registry, "registry-idempotence", request)
    payload = _scheduler_payload(request)
    calls: list[dict[str, Any]] = []

    def handler(data: dict[str, Any]) -> dict[str, Any]:
        calls.append(dict(data))
        return _run_worker_task(data, _pipeline(registry, store, frames), frames)

    scheduler = SchedulerRegistry(
        specs=(JobSpec("forecast-run", "trendx_forecast", "1h", "test"),),
    )
    scheduler.register_handler("trendx_forecast", handler)
    first = scheduler.trigger("forecast-run", payload, idempotency_key=payload["reference_key"])
    second = scheduler.trigger("forecast-run", payload, idempotency_key=payload["reference_key"])

    assert first["model_id"] == model.model_id
    assert second["deduplicated"] is True
    assert len(calls) == 1


@pytest.mark.unit
def test_o_multi_metric_scheduler_worker_path(tmp_path):
    store = FileSystemArtifactStore(tmp_path / "artifacts")
    registry = ChampionRegistry()
    for target in ("temperature", "humidity", "energy_consumption", "solar_power"):
        request = _request(target)
        model, _dataset, frames = _train_promoted(store, registry, f"multi-{target}", request)
        payload = _scheduler_payload(request)
        result = _run_worker_task(payload, _pipeline(registry, store, frames), frames)
        assert result["model_id"] == model.model_id
        assert result["target_metric"] == target


@pytest.mark.unit
def test_p_multi_entity_task_and_artifact_isolation(tmp_path):
    store = _RecordingArtifactStore(tmp_path / "artifacts")
    registry = ChampionRegistry()
    request_a = _request("X", entity="entity-A")
    request_b = _request("X", entity="entity-B")
    model_a, _dataset_a, frames_a = _train_promoted(
        store, registry, "entity-A", request_a, target_base=20.0
    )
    model_b, _dataset_b, frames_b = _train_promoted(
        store, registry, "entity-B", request_b, target_base=80.0
    )

    result_a = _run_worker_task(
        _scheduler_payload(request_a), _pipeline(registry, store, frames_a), frames_a
    )
    result_b = _run_worker_task(
        _scheduler_payload(request_b), _pipeline(registry, store, frames_b), frames_b
    )

    assert result_a["entity_id"] == "entity-A"
    assert result_b["entity_id"] == "entity-B"
    assert result_a["model_id"] == model_a.model_id
    assert result_b["model_id"] == model_b.model_id
    assert set(store.loaded_uris) == {model_a.model_uri, model_b.model_uri}
    assert store.metadata(model_a.model_uri).entity_id == "entity-A"
    assert store.metadata(model_b.model_uri).entity_id == "entity-B"


@pytest.mark.unit
def test_q_fourier_worker_reload_restores_history(tmp_path):
    store = FileSystemArtifactStore(tmp_path / "artifacts")
    registry = ChampionRegistry()
    request = _request("temperature")
    model, _dataset, frames = _train_promoted(store, registry, "fourier-worker", request)
    payload = _scheduler_payload(request)

    result = _run_worker_task(payload, _pipeline(registry, store, frames), frames)

    assert result["model_id"] == model.model_id
    assert (_artifact_dir(model.model_uri) / "train_history.csv").exists()
    assert len(result["result"]["values"]) == request.horizon


@pytest.mark.unit
def test_r_external_features_are_reconstructed_by_worker(tmp_path):
    store = FileSystemArtifactStore(tmp_path / "artifacts")
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
    trained_request = _request("temperature", features=feature_s1)
    model, _dataset, _frames = _train_promoted(store, registry, "external-worker", trained_request)

    serving_request = _request("temperature", features=feature_s1)
    frames = _frames_for(serving_request)
    payload = _scheduler_payload(serving_request)
    result = _run_worker_task(payload, _pipeline(registry, store, frames), frames)
    assert result["model_id"] == model.model_id
    assert payload_to_request(payload).features == feature_s1

    mismatched_request = _request("temperature", features=feature_s2)
    mismatched_frames = _frames_for(mismatched_request)
    mismatched_payload = _scheduler_payload(mismatched_request)
    with pytest.raises(RuntimeError, match="feature_schema_mismatch"):
        _run_worker_task(
            mismatched_payload,
            _pipeline(registry, store, mismatched_frames),
            mismatched_frames,
        )


def _frames_for(request: ForecastRequest) -> dict[tuple[str, str], pd.DataFrame]:
    return _frames(request)


@pytest.mark.unit
def test_s_new_process_scheduler_worker_pipeline_serving(tmp_path):
    store = FileSystemArtifactStore(tmp_path / "artifacts")
    registry = ChampionRegistry()
    request = _request("temperature")
    model, _dataset, _frames = _train_promoted(store, registry, "process-model", request)
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
plan = plan_tick(eligible, job_type="trendx_forecast", window="w97-process")
task = dict(plan.payloads[0])
index = pd.date_range("2026-01-01", periods=72, freq="30min", tz="UTC")
frames = {
    (payload["entity_id"], payload["target_metric"]): pd.DataFrame(
        {"ts": index, "value": [20.0 + (i % 24) * 0.5 for i in range(len(index))]}
    )
}
pipeline = ForecastPipeline(
    champion_registry=registry,
    artifact_store=FileSystemArtifactStore(payload["base_path"]),
)
handler = build_forecast_task_handler(pipeline, frames)
result = handler(task, "task-process-b", "worker-process-b")
assert result["model_id"] == payload["model"]["model_id"]
print(json.dumps({"task": task, "result": result}))
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
    assert evidence["task"]["job_type"] == "trendx_forecast"
    assert evidence["result"]["model_id"] == model_payload["model_id"]
    assert evidence["result"]["execution_id"] == evidence["task"]["execution_id"]
    assert evidence["result"]["reference_key"] == evidence["task"]["reference_key"]
    assert len(evidence["result"]["result"]["values"]) == 24
