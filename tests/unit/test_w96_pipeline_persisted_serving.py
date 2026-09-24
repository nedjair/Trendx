"""W96 — ForecastPipeline integration with persisted champion serving.

The tests keep the W88 injected-engine mode intact and exercise the new
W96 mode through the complete request -> resolver -> dataset -> champion ->
artifact -> reload -> prediction path.
"""

from __future__ import annotations

import gc
import json
import os
import subprocess
import sys
from dataclasses import replace
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pytest
from numpy.testing import assert_allclose
from trendx.forecasting.artifact import FileSystemArtifactStore
from trendx.forecasting.base import ForecastResult
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


def _make_frames(
    request: ForecastRequest,
    *,
    target_base: float = 20.0,
    feature_base: float = 30.0,
) -> dict[tuple[str, str], pd.DataFrame]:
    frames: dict[tuple[str, str], pd.DataFrame] = {
        (request.entity_id, request.target_metric): _frame(target_base)
    }
    for feature in request.features:
        key = (feature.entity_scope or request.entity_id, feature.metric)
        frames[key] = _frame(feature_base)
    return frames


def _resolver(frames: dict[tuple[str, str], pd.DataFrame]) -> FeatureResolver:
    return FeatureResolver(
        available=frames.keys(),
        external_providers={"fixture-weather"},
    )


def _dataset(
    request: ForecastRequest,
    frames: dict[tuple[str, str], pd.DataFrame] | None = None,
) -> ForecastDataset:
    frames = frames or _make_frames(request)
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


def _candidate(
    model_id: str,
    request: ForecastRequest,
) -> RegisteredModel:
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


def _train_register_promote(
    store: FileSystemArtifactStore,
    registry: ChampionRegistry,
    model_id: str,
    request: ForecastRequest,
    *,
    target_base: float = 20.0,
    promote: bool = True,
) -> tuple[RegisteredModel, ForecastDataset, dict[tuple[str, str], pd.DataFrame]]:
    frames = _make_frames(request, target_base=target_base)
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


def _pipeline(
    registry: ChampionRegistry,
    store: FileSystemArtifactStore,
    frames: dict[tuple[str, str], pd.DataFrame],
    *,
    model_loader: Any = None,
) -> ForecastPipeline:
    return ForecastPipeline(
        resolver=_resolver(frames),
        builder=DatasetBuilder(),
        champion_registry=registry,
        artifact_store=store,
        model_loader=model_loader,
    )


def _artifact_dir(uri: str) -> Path:
    assert uri.startswith("file://")
    return Path(uri[7:])


class _RecordingArtifactStore(FileSystemArtifactStore):
    def __init__(self, base_path: str | Path) -> None:
        super().__init__(base_path)
        self.loaded_uris: list[str] = []

    def load(self, uri: str, algorithm: str) -> Any:
        self.loaded_uris.append(uri)
        return super().load(uri, algorithm)


@pytest.mark.unit
def test_a_persisted_champion_is_served_by_pipeline(tmp_path):
    store = FileSystemArtifactStore(tmp_path / "artifacts")
    registry = ChampionRegistry()
    request = _request("temperature")
    model, _dataset, frames = _train_register_promote(store, registry, "champ-A", request)
    serving_request = replace(request, algorithm=Algorithm.AUTO)

    outcome = _pipeline(registry, store, frames).run(serving_request, frames, execution_id="w96-a")

    assert outcome.status == "ok"
    assert outcome.reason == "forecast"
    assert outcome.compatibility is not None
    assert outcome.envelope is not None
    assert outcome.envelope.model_id == model.model_id
    assert outcome.envelope.execution_id == "w96-a"
    assert len(outcome.envelope.result.values) == 24


@pytest.mark.unit
def test_b_injected_model_mode_remains_supported():
    request = _request("temperature")
    frames = _make_frames(request)
    model = RegisteredModel(
        model_id="injected-A",
        tenant_id=request.tenant_id,
        entity_type=request.entity_type,
        entity_id=request.entity_id,
        target_metric=request.target_metric,
        algorithm=Algorithm.FOURIER,
        feature_schema_fingerprint=request.feature_schema().fingerprint(),
        frequency=request.frequency,
        horizon=request.horizon,
        model_uri="memory://injected-A",
    )
    calls: list[str] = []

    class _InjectedModel:
        def predict(self, horizon: int) -> ForecastResult:
            calls.append("predict")
            return ForecastResult(
                values=np.ones(horizon),
                lower_bound=np.zeros(horizon),
                upper_bound=np.ones(horizon) * 2,
            )

    outcome = ForecastPipeline(
        resolver=_resolver(frames),
        builder=DatasetBuilder(),
        models=[model],
        model_loader=lambda _model: _InjectedModel(),
    ).run(request, frames)

    assert outcome.status == "ok"
    assert outcome.envelope.model_id == "injected-A"
    assert calls == ["predict"]


@pytest.mark.unit
def test_c_missing_champion_does_not_fallback_to_challenger_or_training(tmp_path):
    store = FileSystemArtifactStore(tmp_path / "artifacts")
    registry = ChampionRegistry()
    request = _request("temperature")
    _model, _dataset, frames = _train_register_promote(
        store, registry, "challenger-only", request, promote=False
    )
    loader_calls: list[str] = []

    def _loader(_model: RegisteredModel) -> Any:
        loader_calls.append("called")
        raise AssertionError("persisted serving must not invoke a training fallback")

    outcome = _pipeline(registry, store, frames, model_loader=_loader).run(request, frames)
    assert outcome.status == "refused"
    assert outcome.reason == "no-compatible-champion"
    assert outcome.envelope is None
    assert loader_calls == []
    assert registry.champions() == []


@pytest.mark.unit
def test_d_missing_artifact_is_a_controlled_pipeline_refusal(tmp_path):
    store = FileSystemArtifactStore(tmp_path / "artifacts")
    registry = ChampionRegistry()
    request = _request("temperature")
    model, _dataset, frames = _train_register_promote(store, registry, "missing", request)
    (_artifact_dir(model.model_uri) / "model.bin").unlink()

    outcome = _pipeline(registry, store, frames).run(request, frames)

    assert outcome.status == "refused"
    assert outcome.reason == "artifact-missing"
    assert outcome.envelope is None


@pytest.mark.unit
def test_e_corrupted_artifact_is_a_controlled_pipeline_refusal(tmp_path):
    store = FileSystemArtifactStore(tmp_path / "artifacts")
    registry = ChampionRegistry()
    request = _request("temperature")
    model, _dataset, frames = _train_register_promote(store, registry, "corrupt", request)
    with (_artifact_dir(model.model_uri) / "model.bin").open("ab") as handle:
        handle.write(b"corrupt")

    outcome = _pipeline(registry, store, frames).run(request, frames)

    assert outcome.status == "refused"
    assert outcome.reason == "artifact-corrupted"
    assert outcome.envelope is None


@pytest.mark.unit
def test_f_schema_mismatch_is_refused_without_metric_branching(tmp_path):
    store = FileSystemArtifactStore(tmp_path / "artifacts")
    registry = ChampionRegistry()
    feature_s1 = (FeatureDefinition(name="humidity", metric="humidity"),)
    feature_s2 = (
        *feature_s1,
        FeatureDefinition(name="pressure", metric="pressure"),
    )
    trained_request = _request("temperature", features=feature_s1)
    model, _dataset, _frames = _train_register_promote(
        store, registry, "schema-S1", trained_request
    )
    serving_request = _request("temperature", features=feature_s2)
    frames = _make_frames(serving_request)

    outcome = _pipeline(registry, store, frames).run(serving_request, frames)

    assert outcome.status == "refused"
    assert outcome.reason == "feature_schema_mismatch"
    assert registry.get(model.model_id).role is ModelRole.CHAMPION


@pytest.mark.unit
def test_g_fingerprint_mismatch_is_refused(tmp_path):
    store = FileSystemArtifactStore(tmp_path / "artifacts")
    registry = ChampionRegistry()
    request = _request("temperature")
    model, _dataset, frames = _train_register_promote(store, registry, "fingerprint", request)
    bad_registry = ChampionRegistry()
    bad_model = replace(registry.get(model.model_id), feature_schema_fingerprint="wrong")
    bad_registry.register(bad_model)

    outcome = _pipeline(bad_registry, store, frames).run(request, frames)

    assert outcome.status == "refused"
    assert outcome.reason == "feature_schema_mismatch"
    assert outcome.envelope is None


@pytest.mark.unit
@pytest.mark.parametrize(
    ("request_kwargs", "reason"),
    [
        ({"frequency": "15m"}, "frequency_mismatch"),
        ({"horizon": 48}, "horizon_mismatch"),
    ],
)
def test_h_frequency_and_horizon_mismatch_are_refused(tmp_path, request_kwargs, reason):
    store = FileSystemArtifactStore(tmp_path / "artifacts")
    registry = ChampionRegistry()
    trained_request = _request("temperature")
    model, _dataset, _frames = _train_register_promote(
        store, registry, "frequency-horizon", trained_request
    )
    serving_request = _request("temperature", **request_kwargs)
    frames = _make_frames(serving_request)

    outcome = _pipeline(registry, store, frames).run(serving_request, frames)

    assert outcome.status == "refused"
    assert outcome.reason == reason
    assert registry.get(model.model_id).role is ModelRole.CHAMPION


@pytest.mark.unit
def test_i_target_and_entity_mismatch_are_refused(tmp_path):
    store = FileSystemArtifactStore(tmp_path / "artifacts")
    registry = ChampionRegistry()
    trained_request = _request("temperature", entity="entity-A")
    model, _dataset, _frames = _train_register_promote(
        store, registry, "target-entity", trained_request
    )

    target_request = _request("humidity", entity="entity-A")
    target_frames = _make_frames(target_request)
    target_outcome = _pipeline(registry, store, target_frames).run(target_request, target_frames)
    assert target_outcome.status == "refused"
    assert target_outcome.reason == "target_mismatch"

    entity_request = _request("temperature", entity="entity-B")
    entity_frames = _make_frames(entity_request)
    entity_outcome = _pipeline(registry, store, entity_frames).run(entity_request, entity_frames)
    assert entity_outcome.status == "refused"
    assert entity_outcome.reason == "entity_mismatch"
    assert registry.get(model.model_id).role is ModelRole.CHAMPION


@pytest.mark.unit
def test_j_challenger_serving_never_auto_promotes(tmp_path):
    store = FileSystemArtifactStore(tmp_path / "artifacts")
    registry = ChampionRegistry()
    request_a = _request("temperature", entity="entity-A")
    model_a, _dataset_a, frames_a = _train_register_promote(store, registry, "champ-A", request_a)
    before_a = registry.get(model_a.model_id).to_dict()

    request_b = _request("temperature", entity="entity-B")
    model_b, dataset_b, frames_b = _train_register_promote(
        store, registry, "challenger-B", request_b, promote=False
    )
    before_b = registry.get(model_b.model_id).to_dict()
    refused = _pipeline(registry, store, frames_b).run(request_b, frames_b)
    assert refused.status == "refused"
    assert refused.reason == "entity_mismatch"
    assert registry.get(model_a.model_id).to_dict() == before_a
    assert registry.get(model_b.model_id).to_dict() == before_b

    registry.promote(model_b.model_id, dataset_b, artifact_store=store)
    served = _pipeline(registry, store, frames_b).run(request_b, frames_b)
    assert served.status == "ok"
    assert served.envelope.model_id == model_b.model_id
    assert registry.get(model_b.model_id).role is ModelRole.CHAMPION
    assert registry.get(model_a.model_id).role is ModelRole.CHAMPION
    assert registry.get(model_a.model_id).to_dict()["status"] == ModelStatus.READY.value


@pytest.mark.unit
def test_k_champion_lookup_preserves_registry_state(tmp_path):
    store = FileSystemArtifactStore(tmp_path / "artifacts")
    registry = ChampionRegistry()
    request = _request("temperature")
    model, _dataset, frames = _train_register_promote(store, registry, "champ-A", request)
    before = registry.get(model.model_id).to_dict()

    first = _pipeline(registry, store, frames).run(request, frames)
    second = _pipeline(registry, store, frames).run(request, frames)

    assert first.status == "ok"
    assert second.status == "ok"
    assert second.envelope.model_id == model.model_id
    assert registry.get(model.model_id).to_dict() == before


@pytest.mark.unit
def test_l_multi_metric_pipeline_uses_one_generic_path(tmp_path):
    store = FileSystemArtifactStore(tmp_path / "artifacts")
    registry = ChampionRegistry()
    uris: set[str] = set()
    for target in ("temperature", "humidity", "energy_consumption", "solar_power"):
        request = _request(target)
        model, _dataset, frames = _train_register_promote(
            store, registry, f"model-{target}", request
        )
        outcome = _pipeline(registry, store, frames).run(request, frames)
        assert outcome.status == "ok"
        assert outcome.envelope.target_metric == target
        assert outcome.envelope.model_id == model.model_id
        uris.add(model.model_uri)
    assert len(uris) == 4


@pytest.mark.unit
def test_m_multi_entity_pipeline_isolates_lookup_artifact_and_prediction(tmp_path):
    store = _RecordingArtifactStore(tmp_path / "artifacts")
    registry = ChampionRegistry()
    model_a, _dataset_a, frames_a = _train_register_promote(
        store,
        registry,
        "entity-A",
        _request("X", entity="entity-A"),
        target_base=20.0,
    )
    model_b, _dataset_b, frames_b = _train_register_promote(
        store,
        registry,
        "entity-B",
        _request("X", entity="entity-B"),
        target_base=80.0,
    )
    assert model_a.model_uri != model_b.model_uri
    assert store.metadata(model_a.model_uri).entity_id == "entity-A"
    assert store.metadata(model_b.model_uri).entity_id == "entity-B"

    outcome_a = _pipeline(registry, store, frames_a).run(_request("X", entity="entity-A"), frames_a)
    outcome_b = _pipeline(registry, store, frames_b).run(_request("X", entity="entity-B"), frames_b)
    assert outcome_a.status == "ok"
    assert outcome_b.status == "ok"
    assert outcome_a.envelope.model_id == model_a.model_id
    assert outcome_b.envelope.model_id == model_b.model_id
    assert set(store.loaded_uris) == {model_a.model_uri, model_b.model_uri}


@pytest.mark.unit
def test_n_external_feature_schema_is_pipeline_isolated(tmp_path):
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
    model, _dataset, _frames = _train_register_promote(
        store, registry, "external-S1", trained_request
    )
    serving_request = _request("temperature", features=feature_s1)
    frames = _make_frames(serving_request)
    compatible = _pipeline(registry, store, frames).run(serving_request, frames)
    assert compatible.status == "ok"
    assert compatible.envelope.model_id == model.model_id

    mismatched_request = _request("temperature", features=feature_s2)
    mismatched_frames = _make_frames(mismatched_request)
    mismatched = _pipeline(registry, store, mismatched_frames).run(
        mismatched_request, mismatched_frames
    )
    assert mismatched.status == "refused"
    assert mismatched.reason == "feature_schema_mismatch"


@pytest.mark.unit
def test_o_fourier_pipeline_reload_restores_history(tmp_path):
    store = FileSystemArtifactStore(tmp_path / "artifacts")
    registry = ChampionRegistry()
    request = _request("temperature")
    model, _dataset, frames = _train_register_promote(store, registry, "fourier", request)
    expected = store.load(model.model_uri, "Fourier").predict(24).values

    outcome = _pipeline(registry, store, frames).run(request, frames)

    assert outcome.status == "ok"
    assert_allclose(outcome.envelope.result.values, expected, rtol=1e-9, atol=1e-9)
    artifact_dir = _artifact_dir(model.model_uri)
    assert (artifact_dir / "train_history.csv").exists()
    assert (artifact_dir / "checksum.sha256").exists()
    assert (artifact_dir / "metadata.json").exists()


@pytest.mark.unit
def test_p_new_process_pipeline_reloads_persisted_champion(tmp_path):
    store = FileSystemArtifactStore(tmp_path / "artifacts")
    registry = ChampionRegistry()
    request = _request("temperature")
    model, _dataset, _frames = _train_register_promote(store, registry, "process-A", request)
    persisted_model = registry.get(model.model_id)
    assert persisted_model is not None
    model_payload = persisted_model.to_dict()

    del model
    del registry
    gc.collect()

    child = r"""
import json
import sys

import pandas as pd

from trendx.forecasting.artifact import FileSystemArtifactStore
from trendx.forecasting.contract import Algorithm, ForecastRequest
from trendx.forecasting.pipeline import ForecastPipeline
from trendx.forecasting.registry import ChampionRegistry, ModelRole, ModelStatus, RegisteredModel

payload = json.load(sys.stdin)
model_data = dict(payload["model"])
model_data["algorithm"] = Algorithm(model_data["algorithm"])
model_data["status"] = ModelStatus(model_data["status"])
model_data["role"] = ModelRole(model_data["role"])
registered = RegisteredModel(**model_data)
registry = ChampionRegistry()
registry.register(registered)
index = pd.date_range("2026-01-01", periods=72, freq="30min", tz="UTC")
frames = {
    (payload["entity_id"], payload["target_metric"]): pd.DataFrame(
        {"ts": index, "value": [20.0 + (i % 24) * 0.5 for i in range(len(index))]}
    )
}
request = ForecastRequest(
    tenant_id=payload["tenant_id"],
    entity_type=payload["entity_type"],
    entity_id=payload["entity_id"],
    target_metric=payload["target_metric"],
    frequency=payload["frequency"],
    horizon=payload["horizon"],
    algorithm=Algorithm.FOURIER,
)
pipeline = ForecastPipeline(
    champion_registry=registry,
    artifact_store=FileSystemArtifactStore(payload["base_path"]),
)
outcome = pipeline.run(request, frames, execution_id="w96-process-b")
assert outcome.status == "ok", outcome.reason
assert outcome.envelope is not None
print(json.dumps(outcome.envelope.to_dict()))
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
    }
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
        input=json.dumps(payload),
        text=True,
        capture_output=True,
        check=True,
    )
    envelope = json.loads(completed.stdout.strip().splitlines()[-1])
    assert envelope["model_id"] == model_payload["model_id"]
    assert envelope["model_version"] == model_payload["model_version"]
    assert envelope["execution_id"] == "w96-process-b"
    assert len(envelope["result"]["values"]) == 24
