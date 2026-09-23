"""Model registry contract + compatibility tests (W87)."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest
from trendx.forecasting.contract import Algorithm, FeatureDefinition, FeatureSchema
from trendx.forecasting.dataset import DatasetBuilder, ForecastDataset
from trendx.forecasting.registry import (
    ChampionRegistry,
    IncompatibilityReason,
    ModelRole,
    ModelStatus,
    RegisteredModel,
    check_compatibility,
)


def _model(**overrides):
    params = {
        "model_id": "model-1",
        "tenant_id": "tenant-1",
        "entity_id": "entity-A",
        "target_metric": "temperature",
        "algorithm": Algorithm.PROPHET,
        "feature_schema_version": "v1",
        "feature_schema_fingerprint": "fp-v1",
        "frequency": "1h",
        "horizon": 24,
        "training_window": "90d",
        "model_version": "3",
        "status": ModelStatus.READY,
        "role": ModelRole.CHALLENGER,
        "model_uri": "runs:/run-1/model",
    }
    params.update(overrides)
    return RegisteredModel(**params)


def _dataset(target="temperature", entity="entity-A", fp="fp-v1", freq="1h", hz=24):
    idx = pd.date_range("2026-01-01", periods=30, freq="1h")
    return ForecastDataset(
        ds=pd.Series(idx),
        target=pd.Series([1.0] * 30, name=target),
        X=pd.DataFrame({"f": [1.0] * 30}),
        metadata={
            "tenant_id": "tenant-1",
            "entity_id": entity,
            "target_metric": target,
            "frequency": freq,
            "horizon": hz,
            "feature_schema_fingerprint": fp,
        },
    )


def _builder_dataset(target, features, store):
    from trendx.forecasting.contract import ForecastRequest
    from trendx.forecasting.resolution import FeatureResolver

    req = ForecastRequest(
        tenant_id="tenant-1",
        entity_type="DEVICE",
        entity_id="entity-A",
        target_metric=target,
        horizon=24,
        frequency="1h",
        features=features,
    )
    schema = FeatureSchema(features=features)
    resolved = FeatureResolver(available=set(store)).resolve(req, schema)
    assert resolved.ok
    return DatasetBuilder(frame_provider=lambda e, m: store[(e, m)].copy()).build(req, resolved)


def _frame(base=20.0):
    idx = pd.date_range("2026-01-01", periods=72, freq="30min", tz="UTC")
    return pd.DataFrame({"ts": idx, "value": [base + (i % 24) * 0.5 for i in range(72)]})


@pytest.mark.unit
def test_a_exact_match_compatible():
    c = check_compatibility(_model(), _dataset())
    assert c.ok and c.reason is IncompatibilityReason.COMPATIBLE


@pytest.mark.unit
def test_b_target_mismatch():
    c = check_compatibility(_model(), _dataset(target="humidity"))
    assert not c.ok and c.reason is IncompatibilityReason.TARGET_MISMATCH


@pytest.mark.unit
def test_c_entity_mismatch():
    c = check_compatibility(_model(), _dataset(entity="entity-B"))
    assert not c.ok and c.reason is IncompatibilityReason.ENTITY_MISMATCH


@pytest.mark.unit
def test_d_schema_mismatch():
    c = check_compatibility(_model(), _dataset(fp="fp-v2"))
    assert not c.ok and c.reason is IncompatibilityReason.FEATURE_SCHEMA_MISMATCH


@pytest.mark.unit
def test_e_fingerprint_authoritative_over_version():
    m = _model(feature_schema_version="v1", feature_schema_fingerprint="abc")
    c = check_compatibility(m, _dataset(fp="def"))
    assert not c.ok and c.reason is IncompatibilityReason.FEATURE_SCHEMA_MISMATCH


@pytest.mark.unit
def test_f_frequency_mismatch():
    c = check_compatibility(_model(), _dataset(freq="15m"))
    assert not c.ok and c.reason is IncompatibilityReason.FREQUENCY_MISMATCH


@pytest.mark.unit
def test_g_horizon_mismatch_explicit():
    c = check_compatibility(_model(), _dataset(hz=48))
    assert not c.ok and c.reason is IncompatibilityReason.HORIZON_MISMATCH


@pytest.mark.unit
def test_h_failed_model_incompatible():
    c = check_compatibility(_model(status=ModelStatus.FAILED), _dataset())
    assert not c.ok and c.reason is IncompatibilityReason.MODEL_NOT_READY


@pytest.mark.unit
def test_i_missing_uri_incompatible():
    c = check_compatibility(_model(model_uri=""), _dataset())
    assert not c.ok and c.reason is IncompatibilityReason.MODEL_URI_MISSING


@pytest.mark.unit
def test_j_auto_rejected_as_concrete():
    with pytest.raises(ValueError, match="AUTO"):
        _model(algorithm=Algorithm.AUTO)
    m = _model(algorithm="Prophet")
    assert m.algorithm is Algorithm.PROPHET


@pytest.mark.unit
def test_k_champion_uniqueness_on_promote():
    reg = ChampionRegistry()
    reg.register(_model(model_id="m1"))
    reg.register(_model(model_id="m2"))
    reg.promote_to_champion("m1")
    reg.promote_to_champion("m2")
    champs = reg.champions()
    assert [c.model_id for c in champs] == ["m2"]


@pytest.mark.unit
def test_l_challengers_coexist():
    reg = ChampionRegistry()
    reg.register(_model(model_id="m1"))
    reg.register(_model(model_id="m2"))
    assert len(reg.champions()) == 0


@pytest.mark.unit
def test_m_temperature_prophet_works():
    assert check_compatibility(_model(), _dataset(target="temperature")).ok


@pytest.mark.unit
def test_n_humidity_same_mechanism():
    assert check_compatibility(_model(target_metric="humidity"), _dataset(target="humidity")).ok


@pytest.mark.unit
def test_o_energy_same_mechanism():
    assert check_compatibility(
        _model(target_metric="energy_consumption"),
        _dataset(target="energy_consumption"),
    ).ok


@pytest.mark.unit
def test_p_solar_same_mechanism():
    assert check_compatibility(
        _model(target_metric="solar_power"), _dataset(target="solar_power")
    ).ok


@pytest.mark.unit
def test_q_no_temperature_branch_in_registry():
    path = (
        Path(__file__).resolve().parent.parent.parent
        / "src"
        / "trendx"
        / "forecasting"
        / "registry.py"
    )
    assert "temperature" not in path.read_text(encoding="utf-8").lower()


@pytest.mark.unit
def test_schema_mismatch_train_vs_inference_refused():
    schema_v1 = FeatureSchema(
        schema_version="v1",
        features=(
            FeatureDefinition(name="temperature", metric="temperature", lag="1h"),
            FeatureDefinition(name="humidity", metric="humidity"),
        ),
    )
    schema_v2 = FeatureSchema(
        schema_version="v2",
        features=(
            FeatureDefinition(name="temperature", metric="temperature", lag="1h"),
            FeatureDefinition(name="humidity", metric="humidity"),
            FeatureDefinition(name="occupancy", metric="occupancy"),
        ),
    )
    frames = {
        ("entity-A", "temperature"): _frame(20.0),
        ("entity-A", "humidity"): _frame(55.0),
        ("entity-A", "occupancy"): _frame(3.0),
    }
    ds_v2 = _builder_dataset("temperature", schema_v2.features, frames)
    model = _model(
        feature_schema_version="v1",
        feature_schema_fingerprint=schema_v1.fingerprint(),
    )
    c = check_compatibility(model, ds_v2)
    assert not c.ok and c.reason is IncompatibilityReason.FEATURE_SCHEMA_MISMATCH


@pytest.mark.unit
def test_legacy_rows_adapted_with_markers():
    row = SimpleNamespace(
        id="b565d0d3",
        tenant_id="t1",
        business_entity_id="e1",
        tb_telemetry_key="temperature",
        status="champion",
        algorithm=None,
        frequency=None,
        hyperparameters={},
        model_uri="runs:/r/model",
    )
    m = RegisteredModel.from_legacy(row)
    assert m.role is ModelRole.CHAMPION
    assert m.algorithm is Algorithm.UNKNOWN
    assert m.feature_schema_version == "LEGACY"
    c = check_compatibility(m, _dataset())
    assert not c.ok
    assert c.reason in (
        IncompatibilityReason.ALGORITHM_UNRESOLVED,
        IncompatibilityReason.FEATURE_SCHEMA_MISMATCH,
        IncompatibilityReason.FREQUENCY_MISMATCH,
        IncompatibilityReason.ENTITY_MISMATCH,
    )
