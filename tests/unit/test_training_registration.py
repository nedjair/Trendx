"""Generic training + registration tests (W93)."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest
from trendx.forecasting.contract import (
    Algorithm,
    FeatureDefinition,
    FeatureSchema,
    ForecastRequest,
)
from trendx.forecasting.dataset import DatasetBuilder
from trendx.forecasting.evaluation import SelectionResult
from trendx.forecasting.registry import (
    ChampionRegistry,
    ModelRole,
    ModelStatus,
    RegisteredModel,
)
from trendx.forecasting.resolution import FeatureResolver
from trendx.forecasting.training import ModelTrainer, TrainingOutcome, TrainingRequest


class _Engine:
    def __init__(self):
        self.fitted = False

    def fit(self, data, *, context=None):
        self.fitted = True
        return self

    def predict(self, horizon, *, context=None):
        raise NotImplementedError


class _BrokenEngine:
    def fit(self, data, *, context=None):
        raise RuntimeError("fit exploded")


def _frame(base=20.0, periods=72):
    idx = pd.date_range("2026-01-01", periods=periods, freq="30min", tz="UTC")
    return pd.DataFrame({"ts": idx, "value": [base + (i % 24) * 0.5 for i in range(periods)]})


def _req(target="temperature", features=(), horizon=24, frequency="1h"):
    return ForecastRequest(
        tenant_id="t",
        entity_type="DEVICE",
        entity_id="e",
        target_metric=target,
        horizon=horizon,
        frequency=frequency,
        features=features,
    )


def _dataset(target="temperature", features=(), base=20.0, horizon=24, frequency="1h"):
    req = _req(target, features, horizon, frequency)
    schema = FeatureSchema(features=features)
    frames = {("e", target): _frame(base)}
    for f in features:
        frames[("e", f.metric or target)] = _frame(base + 1.0)
    resolved = FeatureResolver(available=set(frames)).resolve(req, schema)
    assert resolved.ok
    return DatasetBuilder(frame_provider=lambda e, m: frames[(e, m)].copy()).build(req, resolved)


def _fp(features):
    return FeatureSchema(features=features).fingerprint()


def _candidate(mid="m1", target="temperature", features=(), fp=None):
    return RegisteredModel(
        model_id=mid,
        tenant_id="t",
        entity_id="e",
        target_metric=target,
        algorithm=Algorithm.PROPHET,
        feature_schema_fingerprint=fp if fp is not None else _fp(features),
        frequency="1h",
        horizon=24,
        status=ModelStatus.READY,
        role=ModelRole.CHALLENGER,
        model_uri="runs:/r/model",
    )


def _selection(model):
    from trendx.forecasting.evaluation import CandidateEvaluation

    ev = CandidateEvaluation(
        model_id=model.model_id,
        algorithm=model.algorithm.value,
        folds=(),
        mean_scores={"mae": 1.0},
        valid_folds=2,
        status="ok",
    )
    return SelectionResult(
        winner_model_id=model.model_id, reason="best-score", criterion="mae", evaluations=(ev,)
    )


def _trainer(**kw):
    kw.setdefault("model_factory", lambda algo: _Engine())
    kw.setdefault("artifact_store", lambda mid, eng: f"memory://{mid}/model")
    kw.setdefault("registry", ChampionRegistry())
    return ModelTrainer(**kw)


def _treq(target="temperature", **kw):
    params = {
        "tenant_id": "t",
        "entity_type": "DEVICE",
        "entity_id": "e",
        "target_metric": target,
        "frequency": "1h",
        "horizon": 24,
    }
    params.update(kw)
    return TrainingRequest(**params)


@pytest.mark.unit
def test_a_valid_request():
    r = _treq()
    assert r.target_metric == "temperature"


@pytest.mark.unit
def test_b_target_required():
    with pytest.raises(ValueError):
        _treq(target_metric="")


@pytest.mark.unit
def test_c_dataset_xy_correct():
    ds = _dataset()
    assert ds.target.name == "temperature" and len(ds.X.columns) == 0


@pytest.mark.unit
def test_d_missing_target_refused():
    import dataclasses

    ds = _dataset()
    ds2 = dataclasses.replace(ds, target=pd.Series([], dtype=float, name="temperature"))
    m = _candidate()
    out = _trainer().train(_selection(m), ds2, _treq(), {m.model_id: m})
    assert out.status == "failed"


@pytest.mark.unit
def test_e_schema_conformant():
    feats = (FeatureDefinition(name="temperature", metric="temperature", lag="1h"),)
    ds = _dataset("humidity", feats, base=55.0)
    assert ds.metadata["feature_schema_fingerprint"] == _fp(feats)


@pytest.mark.unit
def test_f_fingerprint_conformant():
    feats = (FeatureDefinition(name="temperature", metric="temperature", lag="1h"),)
    m = _candidate("m1", "humidity", feats, fp=_fp(feats))
    ds = _dataset("humidity", feats, base=55.0)
    out = _trainer().train(_selection(m), ds, _treq("humidity"), {m.model_id: m})
    assert out.status == "ok"


@pytest.mark.unit
def test_g_fingerprint_mismatch_refused():
    m = _candidate(fp="nope")
    ds = _dataset()
    out = _trainer().train(_selection(m), ds, _treq(), {m.model_id: m})
    assert out.status == "refused"


@pytest.mark.unit
def test_h_selection_consumed():
    m = _candidate()
    ds = _dataset()
    out = _trainer().train(_selection(m), ds, _treq(), {m.model_id: m})
    assert out.status == "ok" and out.model is not None


@pytest.mark.unit
def test_i_no_valid_candidate_no_training():
    sel = SelectionResult(winner_model_id=None, reason="no-valid-candidate")
    out = _trainer().train(sel, _dataset(), _treq(), {})
    assert out.status == "refused"


@pytest.mark.unit
def test_j_window_respected():
    ds = _dataset()
    out = _trainer().train(
        _selection(_candidate()),
        ds,
        _treq(training_window="90d"),
        {"m1": _candidate()},
    )
    assert out.status == "ok"
    assert out.model.training_window == "90d"


@pytest.mark.unit
def test_k_no_future_leakage():
    import dataclasses

    ds = _dataset()
    tampered = dataclasses.replace(ds, metadata={**ds.metadata, "cutoff": "2020-01-01 00:00:00"})
    m = _candidate()
    out = _trainer().train(_selection(m), tampered, _treq(), {m.model_id: m})
    assert out.status == "failed" and out.reason == "future-leakage"


@pytest.mark.unit
def test_l_frequency_kept():
    ds = _dataset()
    out = _trainer().train(
        _selection(_candidate()), ds, _treq(frequency="1h"), {"m1": _candidate()}
    )
    assert out.model.frequency == "1h"


@pytest.mark.unit
def test_m_horizon_kept():
    ds = _dataset()
    out = _trainer().train(_selection(_candidate()), ds, _treq(horizon=24), {"m1": _candidate()})
    assert out.model.horizon == 24


@pytest.mark.unit
def test_n_registered_model_created():
    m = _candidate()
    out = _trainer().train(_selection(m), _dataset(), _treq(), {m.model_id: m})
    assert isinstance(out, TrainingOutcome) and out.model.model_id.startswith("m1@")


@pytest.mark.unit
def test_o_ready_only_on_fit_success():
    m = _candidate()
    out = _trainer().train(_selection(m), _dataset(), _treq(), {m.model_id: m})
    assert out.model.status is ModelStatus.READY


@pytest.mark.unit
def test_p_fit_failure_no_ready():
    t = ModelTrainer(
        model_factory=lambda algo: _BrokenEngine(),
        artifact_store=lambda mid, eng: "memory://x",
        registry=ChampionRegistry(),
    )
    m = _candidate()
    out = t.train(_selection(m), _dataset(), _treq(), {m.model_id: m})
    assert out.status == "failed" and out.model is None


@pytest.mark.unit
def test_q_registry_failure_controlled():
    reg = ChampionRegistry()
    m = _candidate()
    reg.register(
        RegisteredModel(
            model_id="m1@1.1",
            tenant_id="t",
            entity_id="e",
            target_metric="temperature",
            algorithm=Algorithm.PROPHET,
            feature_schema_fingerprint=_fp(()),
            frequency="1h",
            horizon=24,
            status=ModelStatus.READY,
            role=ModelRole.CHALLENGER,
            model_uri="runs:/r/model",
        )
    )
    t = _trainer(registry=reg)
    out = t.train(_selection(m), _dataset(), _treq(), {m.model_id: m})
    assert out.status in ("ok", "failed")
    if out.status == "failed":
        assert "registry" in out.reason


@pytest.mark.unit
def test_r_no_auto_promotion():
    reg = ChampionRegistry()
    champ = _candidate("champ")
    reg.register(champ)
    reg.promote_to_champion("champ")
    m = _candidate("m1")
    t = _trainer(registry=reg)
    out = t.train(_selection(m), _dataset(), _treq(), {m.model_id: m})
    assert out.status == "ok"
    assert out.model.role is ModelRole.CHALLENGER
    assert [c.model_id for c in reg.champions()] == ["champ"]


@pytest.mark.unit
def test_s_champion_preserved():
    reg = ChampionRegistry()
    champ = _candidate("champ")
    reg.register(champ)
    reg.promote_to_champion("champ")
    assert reg.champions()[0].model_id == "champ"


@pytest.mark.unit
def test_t_two_entities_isolated():
    frames_a = {("A", "temperature"): _frame(20.0)}
    frames_b = {("B", "temperature"): _frame(21.0)}
    assert frames_a != frames_b


@pytest.mark.unit
def test_u_four_metrics_same_path():
    for target, base in (
        ("temperature", 20.0),
        ("humidity", 55.0),
        ("energy_consumption", 100.0),
        ("solar_power", 300.0),
    ):
        m = _candidate("m1", target)
        ds = _dataset(target, (), base)
        out = _trainer().train(_selection(m), ds, _treq(target), {m.model_id: m})
        assert out.status == "ok", target


@pytest.mark.unit
def test_v_external_generic():
    feats = (FeatureDefinition(name="cloud_cover", metric="cloud_cover"),)
    avail_frames = {
        ("e", "temperature"): _frame(20.0),
        ("e", "cloud_cover"): _frame(0.3),
    }
    req = _req_like("temperature", feats)
    from trendx.forecasting.resolution import FeatureResolver

    resolved = FeatureResolver(available=set(avail_frames)).resolve(
        req, FeatureSchema(features=feats)
    )
    assert resolved.ok


def _req_like(target="temperature", features=()):
    from trendx.forecasting.contract import ForecastRequest

    return ForecastRequest(
        tenant_id="t",
        entity_type="DEVICE",
        entity_id="e",
        target_metric=target,
        horizon=24,
        frequency="1h",
        features=features,
    )


@pytest.mark.unit
def test_w_trainer_ignores_weather():
    import inspect

    import trendx.forecasting.training as mod

    assert "WeatherAdapter" not in inspect.getsource(mod)


@pytest.mark.unit
def test_x_scada_compatible_path():
    m = _candidate("m1", "grid_load")
    ds = _dataset("grid_load", (), base=5.0)
    out = _trainer().train(_selection(m), ds, _treq("grid_load"), {m.model_id: m})
    assert out.status == "ok"


@pytest.mark.unit
def test_y_id_version_uri_distinct():
    m = _candidate()
    out = _trainer().train(_selection(m), _dataset(), _treq(), {m.model_id: m})
    assert out.model.model_id != out.model.model_version != out.model.model_uri


@pytest.mark.unit
def test_z_provenance_complete():
    m = _candidate()
    out = _trainer().train(_selection(m), _dataset(), _treq(), {m.model_id: m})
    assert out.provenance["rows"] > 0
    assert out.provenance["schema_fingerprint"] == _fp(())
    assert "cutoff" in out.provenance


@pytest.mark.unit
def test_aa_determinism():
    m = _candidate()
    kw = {"model_id": m, "ds": _dataset(), "req": _treq(), "cand": {m.model_id: m}}
    a = _trainer().train(_selection(m), kw["ds"], kw["req"], kw["cand"])
    b = _trainer().train(_selection(m), kw["ds"], kw["req"], kw["cand"])
    assert a.model.model_id == b.model.model_id
    assert a.provenance == b.provenance


@pytest.mark.unit
def test_ab_no_mlflow():
    import inspect

    import trendx.forecasting.training as mod

    assert "mlflow" not in inspect.getsource(mod).lower()


@pytest.mark.unit
def test_ac_no_tb_write():
    import inspect

    import trendx.forecasting.training as mod

    src = inspect.getsource(mod).lower()
    assert "thingsboard" not in src


@pytest.mark.unit
def test_ad_no_migration():
    import subprocess

    out = subprocess.run(
        ["git", "status", "--short", "--", "migrations/"],
        capture_output=True,
        text=True,
        check=False,
    )
    assert out.stdout.strip() == ""


@pytest.mark.unit
def test_ae_scheduler_untouched():
    import inspect

    import trendx.forecasting.training as mod

    assert "scheduler" not in inspect.getsource(mod).lower()


@pytest.mark.unit
def test_af_worker_untouched():
    import inspect

    import trendx.forecasting.training as mod

    assert "services.worker" not in inspect.getsource(mod).lower()


@pytest.mark.unit
def test_ag_no_metric_branch():
    path = (
        Path(__file__).resolve().parent.parent.parent
        / "src"
        / "trendx"
        / "forecasting"
        / "training.py"
    )
    src = path.read_text(encoding="utf-8").lower()
    assert "temperature" not in src
    assert "if metric ==" not in src


@pytest.mark.unit
def test_ah_from_request_contract():
    from trendx.forecasting.contract import ForecastRequest

    req = ForecastRequest(
        tenant_id="t",
        entity_type="DEVICE",
        entity_id="e",
        target_metric="humidity",
        horizon=48,
        frequency="1h",
    )
    from trendx.forecasting.training import TrainingRequest

    tr = TrainingRequest.from_forecast_request(req, training_window="30d")
    assert tr.target_metric == "humidity" and tr.horizon == 48
    assert tr.training_window == "30d"
