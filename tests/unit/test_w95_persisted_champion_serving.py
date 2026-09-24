"""W95 — serving a persisted champion from a new Python context.

The central test launches a second Python process after training, registration,
and promotion.  The second process receives only the registered identity and
artifact URI; it never receives the training-time model instance.
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
from trendx.forecasting.artifact import (
    ArtifactError,
    ArtifactIntegrityError,
    FileSystemArtifactStore,
)
from trendx.forecasting.contract import (
    Algorithm,
    FeatureDefinition,
    FeatureSchema,
    ForecastRequest,
)
from trendx.forecasting.dataset import DatasetBuilder, ForecastDataset
from trendx.forecasting.evaluation import CandidateEvaluation, SelectionResult
from trendx.forecasting.registry import (
    ChampionRegistry,
    IncompatibilityReason,
    ModelRole,
    ModelStatus,
    RegisteredModel,
)
from trendx.forecasting.resolution import FeatureResolver
from trendx.forecasting.serving import ServingError, serve_registered_model
from trendx.forecasting.training import ModelTrainer, TrainingRequest

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src"


def _frame(base: float = 20.0, periods: int = 72, freq: str = "30min") -> pd.DataFrame:
    index = pd.date_range("2026-01-01", periods=periods, freq=freq, tz="UTC")
    return pd.DataFrame({"ts": index, "value": [base + (i % 24) * 0.5 for i in range(periods)]})


def _request(
    target: str = "temperature",
    features: tuple[FeatureDefinition, ...] = (),
    entity: str = "e1",
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
    )


def _fingerprint(features: tuple[FeatureDefinition, ...]) -> str:
    return FeatureSchema(features=features).fingerprint()


def _dataset(
    target: str = "temperature",
    features: tuple[FeatureDefinition, ...] = (),
    entity: str = "e1",
    frequency: str = "1h",
    horizon: int = 24,
) -> ForecastDataset:
    request = _request(target, features, entity, frequency, horizon)
    frames = {(entity, target): _frame()}
    for feature in features:
        frames[(entity, feature.metric or target)] = _frame(30.0)
    resolved = FeatureResolver(available=set(frames)).resolve(
        request, FeatureSchema(features=features)
    )
    assert resolved.ok
    return DatasetBuilder(frame_provider=lambda e, m: frames[(e, m)].copy()).build(
        request, resolved
    )


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
    target: str = "temperature",
    entity: str = "e1",
    features: tuple[FeatureDefinition, ...] = (),
    frequency: str = "1h",
    horizon: int = 24,
) -> RegisteredModel:
    return RegisteredModel(
        model_id=model_id,
        tenant_id="tenant-1",
        entity_type="DEVICE",
        entity_id=entity,
        target_metric=target,
        algorithm=Algorithm.PROPHET,
        feature_schema_version="v1",
        feature_schema_fingerprint=_fingerprint(features),
        frequency=frequency,
        horizon=horizon,
        training_window="90d",
        status=ModelStatus.READY,
        role=ModelRole.CHALLENGER,
        model_uri="",
    )


def _selection(model: RegisteredModel) -> SelectionResult:
    evaluation = CandidateEvaluation(
        model_id=model.model_id,
        algorithm=model.algorithm.value,
        folds=(),
        mean_scores={"mae": 1.0},
        valid_folds=1,
        status="ok",
    )
    return SelectionResult(
        winner_model_id=model.model_id,
        reason="best-score",
        criterion="mae",
        evaluations=(evaluation,),
    )


def _train_register_promote(
    store: FileSystemArtifactStore,
    registry: ChampionRegistry,
    model_id: str,
    *,
    target: str = "temperature",
    entity: str = "e1",
    features: tuple[FeatureDefinition, ...] = (),
    frequency: str = "1h",
    horizon: int = 24,
    promote: bool = True,
) -> tuple[RegisteredModel, ForecastDataset]:
    dataset = _dataset(target, features, entity, frequency, horizon)
    candidate = _candidate(model_id, target, entity, features, frequency, horizon)
    outcome = ModelTrainer(
        model_factory=lambda _algorithm: _fourier_model(),
        artifact_store=store,
        registry=registry,
    ).train(
        _selection(candidate),
        dataset,
        TrainingRequest(
            tenant_id="tenant-1",
            entity_type="DEVICE",
            entity_id=entity,
            target_metric=target,
            frequency=frequency,
            horizon=horizon,
            training_window="90d",
        ),
        {candidate.model_id: candidate},
    )
    assert outcome.status == "ok"
    assert outcome.model is not None
    if promote:
        registry.promote(outcome.model.model_id, dataset, artifact_store=store)
    return outcome.model, dataset


def _serve(
    model: RegisteredModel,
    registry: ChampionRegistry,
    store: FileSystemArtifactStore,
    dataset: ForecastDataset,
) -> Any:
    return serve_registered_model(model.model_id, registry.get, store, dataset)


@pytest.mark.unit
def test_a_train_save_register_promote_new_process_load_predict(tmp_path):
    """The full serving path crosses a Python process boundary."""
    store = FileSystemArtifactStore(tmp_path / "artifacts")
    registry = ChampionRegistry()
    model, _dataset_for_train = _train_register_promote(store, registry, "champ-A")
    model_payload = model.to_dict()

    # The trainer's fitted engine is not returned or retained by the registry.
    # Destroy any local references before starting process B.
    del model
    del registry
    gc.collect()

    child = r"""
import json
import sys
from pathlib import Path

import pandas as pd

from trendx.forecasting.artifact import FileSystemArtifactStore
from trendx.forecasting.contract import Algorithm
from trendx.forecasting.dataset import ForecastDataset
from trendx.forecasting.registry import ChampionRegistry, ModelRole, ModelStatus, RegisteredModel
from trendx.forecasting.serving import serve_registered_model

payload = json.load(sys.stdin)
model_data = dict(payload["model"])
model_data["algorithm"] = Algorithm(model_data["algorithm"])
model_data["status"] = ModelStatus(model_data["status"])
model_data["role"] = ModelRole(model_data["role"])
registered = RegisteredModel(**model_data)
registry = ChampionRegistry()
registry.register(registered)
index = pd.date_range("2026-01-01", periods=36, freq="1h")
target = pd.Series([20.0] * len(index), index=index, name=payload["target"])
dataset = ForecastDataset(
    ds=pd.Series(index, index=index),
    target=target,
    X=pd.DataFrame(index=index),
    metadata={
        "tenant_id": payload["tenant_id"],
        "entity_id": payload["entity_id"],
        "target_metric": payload["target"],
        "frequency": payload["frequency"],
        "horizon": payload["horizon"],
        "feature_schema_fingerprint": payload["fingerprint"],
        "execution_id": "w95-process-b",
    },
)
store = FileSystemArtifactStore(payload["base_path"])
envelope = serve_registered_model(
    payload["model_id"], registry.get, store, dataset, horizon=payload["prediction_horizon"]
)
print(json.dumps(envelope.to_dict()))
"""
    payload = {
        "model": model_payload,
        "model_id": model_payload["model_id"],
        "base_path": str(tmp_path / "artifacts"),
        "target": "temperature",
        "tenant_id": "tenant-1",
        "entity_id": "e1",
        "frequency": "1h",
        "horizon": 24,
        "prediction_horizon": 6,
        "fingerprint": _fingerprint(()),
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
    assert envelope["target_metric"] == "temperature"
    assert len(envelope["result"]["values"]) == 6
    assert envelope["result"]["values"]


@pytest.mark.unit
def test_b_reload_is_not_training_instance(tmp_path):
    """The same W94 load contract returns a distinct model object."""
    store = FileSystemArtifactStore(tmp_path / "artifacts")
    trained = _fourier_model()
    uri = store.save(
        "identity-check",
        trained,
        provenance={
            "algorithm": "Fourier",
            "feature_schema_version": "v1",
            "feature_schema_fingerprint": _fingerprint(()),
            "frequency": "1h",
            "horizon": 24,
            "target_metric": "temperature",
            "entity_type": "DEVICE",
            "entity_id": "e1",
            "tenant_id": "tenant-1",
            "model_version": "1",
        },
    )
    loaded = store.load(uri, "Fourier")
    assert loaded is not trained
    assert loaded.__class__ is trained.__class__
    del trained
    gc.collect()
    assert loaded.predict(3).values.size == 3


@pytest.mark.unit
def test_c_serving_validates_metadata_identity(tmp_path):
    store = FileSystemArtifactStore(tmp_path / "artifacts")
    registry = ChampionRegistry()
    model, dataset = _train_register_promote(store, registry, "identity")
    metadata_path = Path(model.model_uri[7:]) / "metadata.json"
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    metadata["model_id"] = "wrong-model"
    metadata_path.write_text(json.dumps(metadata), encoding="utf-8")

    with pytest.raises(ServingError, match="metadata mismatch"):
        _serve(model, registry, store, dataset)


@pytest.mark.unit
@pytest.mark.parametrize(
    ("dataset_kwargs", "reason"),
    [
        (
            {"features": (FeatureDefinition(name="other", metric="other"),)},
            IncompatibilityReason.FEATURE_SCHEMA_MISMATCH,
        ),
        ({"frequency": "15m"}, IncompatibilityReason.FREQUENCY_MISMATCH),
        ({"horizon": 48}, IncompatibilityReason.HORIZON_MISMATCH),
    ],
)
def test_d_reload_rejects_w87_incompatibilities(tmp_path, dataset_kwargs, reason):
    store = FileSystemArtifactStore(tmp_path / "artifacts")
    registry = ChampionRegistry()
    model, dataset = _train_register_promote(store, registry, "incompatible")
    mismatched = _dataset(**dataset_kwargs)
    with pytest.raises(ServingError, match=reason.value):
        _serve(model, registry, store, mismatched)
    assert registry.get(model.model_id).role is ModelRole.CHAMPION


@pytest.mark.unit
def test_e_reload_rejects_fingerprint_mismatch(tmp_path):
    store = FileSystemArtifactStore(tmp_path / "artifacts")
    registry = ChampionRegistry()
    model, dataset = _train_register_promote(store, registry, "fingerprint")
    mismatched = replace(
        dataset,
        metadata={**dataset.metadata, "feature_schema_fingerprint": "wrong-fingerprint"},
    )
    with pytest.raises(ServingError, match=IncompatibilityReason.FEATURE_SCHEMA_MISMATCH.value):
        _serve(model, registry, store, mismatched)


@pytest.mark.unit
def test_f_reload_rejects_missing_and_corrupted_artifacts(tmp_path):
    store = FileSystemArtifactStore(tmp_path / "artifacts")
    registry = ChampionRegistry()
    model, dataset = _train_register_promote(store, registry, "artifact-failure")
    artifact_path = Path(model.model_uri[7:]) / "model.bin"
    artifact_path.unlink()
    with pytest.raises(ArtifactError):
        _serve(model, registry, store, dataset)

    model, dataset = _train_register_promote(store, registry, "artifact-corrupt")
    with (Path(model.model_uri[7:]) / "model.bin").open("ab") as handle:
        handle.write(b"corrupt")
    with pytest.raises(ArtifactIntegrityError):
        _serve(model, registry, store, dataset)


@pytest.mark.unit
def test_g_serving_does_not_change_champion(tmp_path):
    store = FileSystemArtifactStore(tmp_path / "artifacts")
    registry = ChampionRegistry()
    model_a, dataset_a = _train_register_promote(store, registry, "champ-A")
    assert [m.model_id for m in registry.champions()] == [model_a.model_id]
    envelope = _serve(model_a, registry, store, dataset_a)
    assert envelope.model_id == model_a.model_id
    assert [m.model_id for m in registry.champions()] == [model_a.model_id]

    model_b, dataset_b = _train_register_promote(store, registry, "champ-B", promote=False)
    assert [m.model_id for m in registry.champions()] == [model_a.model_id]
    assert registry.get(model_b.model_id).role is ModelRole.CHALLENGER
    registry.promote(model_b.model_id, dataset_b, artifact_store=store)
    assert [m.model_id for m in registry.champions()] == [model_b.model_id]
    _serve(model_b, registry, store, dataset_b)
    assert [m.model_id for m in registry.champions()] == [model_b.model_id]


@pytest.mark.unit
def test_h_multi_metric_serving_is_generic(tmp_path):
    store = FileSystemArtifactStore(tmp_path / "artifacts")
    registry = ChampionRegistry()
    for target in ("temperature", "humidity", "energy_consumption", "solar_power"):
        model, dataset = _train_register_promote(store, registry, f"model-{target}", target=target)
        envelope = _serve(model, registry, store, dataset)
        assert envelope.target_metric == target
        assert envelope.result.values.size == 24


@pytest.mark.unit
def test_i_multi_entity_serving_isolation(tmp_path):
    store = FileSystemArtifactStore(tmp_path / "artifacts")
    registry = ChampionRegistry()
    model_a, dataset_a = _train_register_promote(
        store, registry, "entity-A", entity="A", target="X"
    )
    model_b, dataset_b = _train_register_promote(
        store, registry, "entity-B", entity="B", target="X"
    )
    assert model_a.model_uri != model_b.model_uri
    assert registry.get(model_a.model_id).entity_id == "A"
    assert registry.get(model_b.model_id).entity_id == "B"
    envelope_a = _serve(model_a, registry, store, dataset_a)
    envelope_b = _serve(model_b, registry, store, dataset_b)
    assert (envelope_a.entity_id, envelope_a.target_metric) == ("A", "X")
    assert (envelope_b.entity_id, envelope_b.target_metric) == ("B", "X")
    assert envelope_a.model_id != envelope_b.model_id


@pytest.mark.unit
def test_j_external_feature_schema_survives_reload(tmp_path):
    features_s1 = (FeatureDefinition(name="temperature", metric="temperature", lag="1h"),)
    features_s2 = (*features_s1, FeatureDefinition(name="cloud_cover", metric="cloud_cover"))
    store = FileSystemArtifactStore(tmp_path / "artifacts")
    registry = ChampionRegistry()
    model, dataset = _train_register_promote(store, registry, "external-S1", features=features_s1)
    envelope = _serve(model, registry, store, dataset)
    assert envelope.feature_schema_version == "v1"
    assert envelope.result.values.size == 24
    with pytest.raises(ServingError, match=IncompatibilityReason.FEATURE_SCHEMA_MISMATCH.value):
        _serve(model, registry, store, _dataset(features=features_s2))


@pytest.mark.unit
def test_k_fourier_reload_prediction_is_stable(tmp_path):
    store = FileSystemArtifactStore(tmp_path / "artifacts")
    registry = ChampionRegistry()
    model, dataset = _train_register_promote(store, registry, "fourier")
    expected = store.load(model.model_uri, "Fourier").predict(24).values
    envelope = _serve(model, registry, store, dataset)
    assert_allclose(envelope.result.values, expected, rtol=1e-9, atol=1e-9)
    artifact_dir = Path(model.model_uri[7:])
    assert (artifact_dir / "model.bin").exists()
    assert (artifact_dir / "checksum.sha256").exists()
    assert (artifact_dir / "metadata.json").exists()
    assert (artifact_dir / "train_history.csv").exists()


@pytest.mark.unit
def test_l_serving_rejects_non_ready_model(tmp_path):
    store = FileSystemArtifactStore(tmp_path / "artifacts")
    registry = ChampionRegistry()
    model, dataset = _train_register_promote(store, registry, "not-ready")
    failed = replace(model, status=ModelStatus.FAILED)
    failed_registry = ChampionRegistry()
    failed_registry.register(failed)
    with pytest.raises(ArtifactError, match="not READY"):
        _serve(failed, failed_registry, store, dataset)
