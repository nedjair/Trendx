"""Generic multi-metric forecast pipeline tests (W88)."""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import pytest
from trendx.forecasting.base import ForecastResult
from trendx.forecasting.contract import (
    Algorithm,
    FeatureCategory,
    FeatureDefinition,
    ForecastRequest,
)
from trendx.forecasting.pipeline import ForecastPipeline
from trendx.forecasting.registry import (
    IncompatibilityReason,
    ModelRole,
    ModelStatus,
    RegisteredModel,
)


class _StubModel:
    def __init__(self, horizon=24):
        self._h = horizon
        self.calls = []

    def fit(self, data, *, context=None):
        self.calls.append("fit")
        return self

    def predict(self, horizon, *, context=None):
        n = int(horizon)
        return ForecastResult(
            values=np.full(n, 1.0),
            lower_bound=np.full(n, 0.5),
            upper_bound=np.full(n, 1.5),
            timestamps=None,
            model_name="stub",
        )

    def save(self, path):
        raise NotImplementedError

    @classmethod
    def load(cls, path):
        raise NotImplementedError


def _frame(base=20.0, periods=72):
    idx = pd.date_range("2026-01-01", periods=periods, freq="30min", tz="UTC")
    return pd.DataFrame({"ts": idx, "value": [base + (i % 24) * 0.5 for i in range(periods)]})


def _req(target, features=(), algo="Prophet"):
    return ForecastRequest(
        tenant_id="tenant-1",
        entity_type="DEVICE",
        entity_id="entity-A",
        target_metric=target,
        horizon=24,
        frequency="1h",
        features=features,
        algorithm=algo,
    )


def _model(
    mid="m1", target="temperature", fp=None, role=ModelRole.CHALLENGER, entity="entity-A", **kw
):
    from trendx.forecasting.contract import FeatureSchema

    fp = fp if fp is not None else FeatureSchema().fingerprint()
    params = {
        "model_id": mid,
        "tenant_id": "tenant-1",
        "entity_id": entity,
        "target_metric": target,
        "algorithm": Algorithm.PROPHET,
        "feature_schema_fingerprint": fp,
        "frequency": "1h",
        "horizon": 24,
        "status": ModelStatus.READY,
        "role": role,
        "model_uri": "runs:/r/model",
    }
    params.update(kw)
    return RegisteredModel(**params)


def _pipe(models, stubs):
    return ForecastPipeline(models=models, model_loader=lambda m: stubs[m.model_id])


def _temp_frames():
    return {("entity-A", "temperature"): _frame(20.0)}


def _fp_for(features):
    from trendx.forecasting.contract import FeatureSchema

    return FeatureSchema(features=features).fingerprint()


EMPTY_FP = _fp_for(())


def _outcome(target, features, frames, models, execution_id="e1"):
    req = _req(target, features)
    fp = _fp_for(features)
    models = [
        RegisteredModel(
            model_id=m.model_id,
            tenant_id=m.tenant_id,
            entity_type=m.entity_type,
            entity_id=m.entity_id,
            target_metric=m.target_metric,
            algorithm=m.algorithm,
            feature_schema_version=m.feature_schema_version,
            feature_schema_fingerprint=fp,
            frequency=m.frequency,
            horizon=m.horizon,
            training_window=m.training_window,
            model_version=m.model_version,
            status=m.status,
            role=m.role,
            model_uri=m.model_uri,
        )
        for m in models
    ]
    stubs = {m.model_id: _StubModel() for m in models}
    return _pipe(models, stubs).run(req, frames, execution_id=execution_id)


@pytest.mark.unit
def test_a_temperature_e2e():
    out = _outcome("temperature", (), _temp_frames(), [_model()])
    assert out.status == "ok"
    assert out.envelope.target_metric == "temperature"
    assert len(out.envelope.result.values) == 24


@pytest.mark.unit
def test_b_humidity_e2e():
    out = _outcome(
        "humidity", (), {("entity-A", "humidity"): _frame(55.0)}, [_model(target="humidity")]
    )
    assert out.status == "ok"


@pytest.mark.unit
def test_c_energy_e2e():
    feats = (
        FeatureDefinition(name="temperature", metric="temperature"),
        FeatureDefinition(name="humidity", metric="humidity"),
        FeatureDefinition(
            name="occupancy", metric="occupancy", category=FeatureCategory.KNOWN_FUTURE
        ),
    )
    frames = {
        ("entity-A", "energy_consumption"): _frame(100.0),
        ("entity-A", "temperature"): _frame(20.0),
        ("entity-A", "humidity"): _frame(55.0),
        ("entity-A", "occupancy"): _frame(3.0),
    }
    out = _outcome("energy_consumption", feats, frames, [_model(target="energy_consumption")])
    assert out.status == "ok"
    assert out.envelope.target_metric == "energy_consumption"


@pytest.mark.unit
def test_d_solar_e2e():
    feats = (
        FeatureDefinition(name="irradiance", metric="irradiance"),
        FeatureDefinition(name="cloud_cover", metric="cloud_cover"),
        FeatureDefinition(name="temperature", metric="temperature"),
    )
    frames = {
        ("entity-A", "solar_power"): _frame(300.0),
        ("entity-A", "irradiance"): _frame(500.0),
        ("entity-A", "cloud_cover"): _frame(0.3),
        ("entity-A", "temperature"): _frame(20.0),
    }
    out = _outcome("solar_power", feats, frames, [_model(target="solar_power")])
    assert out.status == "ok"


@pytest.mark.unit
def test_e_schema_exact_compatible():
    feats = (FeatureDefinition(name="temperature", metric="temperature", lag="1h"),)
    out = _outcome(
        "humidity",
        feats,
        {
            ("entity-A", "humidity"): _frame(55.0),
            ("entity-A", "temperature"): _frame(20.0),
        },
        [_model(target="humidity")],
    )
    assert out.status == "ok"
    assert out.compatibility.reason is IncompatibilityReason.COMPATIBLE


@pytest.mark.unit
def test_f_fingerprint_mismatch_refused():
    req = _req("temperature")
    out = _pipe([_model(fp="nope")], {"m1": _StubModel()}).run(req, _temp_frames())
    # model fp="nope" vs dataset fp of empty schema -> mismatch, no silent adapt
    assert out.status == "refused"
    assert out.reason == "feature_schema_mismatch"


@pytest.mark.unit
def test_g_target_mismatch_refused():
    req = _req("humidity")
    out = _pipe([_model(target="")], {"m1": _StubModel()}).run(
        req, {("entity-A", "humidity"): _frame(55.0)}
    )
    assert out.status == "refused" and out.reason == "target_mismatch"


@pytest.mark.unit
def test_h_entity_mismatch_refused():
    req = ForecastRequest(
        tenant_id="tenant-1",
        entity_type="DEVICE",
        entity_id="entity-B",
        target_metric="temperature",
        horizon=24,
        frequency="1h",
    )
    out = _pipe([_model(mid="m1", target="temperature")], {"m1": _StubModel()}).run(
        req, {("entity-B", "temperature"): _frame(20.0)}
    )
    # concrete entity mismatch filtered at selection: controlled refusal
    assert out.status == "refused" and out.reason == "no-compatible-model"


@pytest.mark.unit
def test_i_frequency_mismatch_refused():
    req = _req("temperature")
    req = ForecastRequest(
        tenant_id="tenant-1",
        entity_type="DEVICE",
        entity_id="entity-A",
        target_metric="temperature",
        horizon=24,
        frequency="15m",
    )
    out = _pipe([_model()], {"m1": _StubModel()}).run(req, _temp_frames())
    assert out.status == "refused" and out.reason == "frequency_mismatch"


@pytest.mark.unit
def test_j_horizon_mismatch_refused():
    req = _req("temperature")
    req = ForecastRequest(
        tenant_id="tenant-1",
        entity_type="DEVICE",
        entity_id="entity-A",
        target_metric="temperature",
        horizon=48,
        frequency="1h",
    )
    out = _pipe([_model()], {"m1": _StubModel()}).run(req, _temp_frames())
    assert out.status == "refused" and out.reason == "horizon_mismatch"


@pytest.mark.unit
def test_k_failed_model_never_used():
    req = _req("temperature")
    out = _pipe([_model(status=ModelStatus.FAILED)], {"m1": _StubModel()}).run(req, _temp_frames())
    assert out.status == "refused" and out.reason == "model_not_ready"


@pytest.mark.unit
def test_l_missing_uri_refused():
    req = _req("temperature")
    out = _pipe([_model(model_uri="")], {"m1": _StubModel()}).run(req, _temp_frames())
    assert out.status == "refused" and out.reason == "model_uri_missing"


@pytest.mark.unit
def test_m_target_not_in_x():
    feats = (FeatureDefinition(name="temperature", metric="temperature", lag="1h"),)
    out = _outcome(
        "humidity",
        feats,
        {
            ("entity-A", "humidity"): _frame(55.0),
            ("entity-A", "temperature"): _frame(20.0),
        },
        [_model(target="humidity")],
    )
    assert "humidity" not in list(out.dataset_meta["feature_columns"])


@pytest.mark.unit
def test_n_no_metric_branch_in_pipeline():
    path = (
        Path(__file__).resolve().parent.parent.parent
        / "src"
        / "trendx"
        / "forecasting"
        / "pipeline.py"
    )
    assert "temperature" not in path.read_text(encoding="utf-8").lower()


@pytest.mark.unit
def test_o_provenance_envelope():
    out = _outcome("temperature", (), _temp_frames(), [_model()], execution_id="exec-9")
    env = out.envelope.to_dict()
    assert env["execution_id"] == "exec-9"
    assert env["model_id"] == "m1"
    assert env["target_metric"] == "temperature"
    assert env["result"] is not None


@pytest.mark.unit
def test_p_same_architecture_all_metrics():
    import trendx.forecasting.pipeline as pipe_mod

    assert hasattr(pipe_mod, "ForecastPipeline")
    for target in ("temperature", "humidity", "energy_consumption", "solar_power"):
        req = _req(target)
        assert req.target_metric == target
        assert type(req).__name__ == "ForecastRequest"
