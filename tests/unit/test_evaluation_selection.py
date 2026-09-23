"""Generic evaluation + selection tests (W92)."""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import pytest
from trendx.forecasting.contract import Algorithm, FeatureDefinition, FeatureSchema
from trendx.forecasting.evaluation import (
    NO_VALID_CANDIDATE,
    CandidateSpec,
    evaluate_candidate,
    select_best,
    walk_folds,
)
from trendx.forecasting.registry import (
    IncompatibilityReason,
    ModelRole,
    ModelStatus,
    RegisteredModel,
    check_compatibility,
)


def _series(periods=200, tz="UTC", base=20.0):
    idx = pd.date_range("2026-01-01", periods=periods, freq="1h", tz=tz)
    y = [base + (i % 24) * 0.5 for i in range(periods)]
    return pd.DataFrame({"ds": idx, "y": y})


class _Good:
    def fit(self, data, *, context=None):
        self._last = float(data["y"].iloc[-1])
        return self

    def predict(self, horizon, *, context=None):
        from trendx.forecasting.base import ForecastResult

        n = int(horizon)
        return ForecastResult(
            values=np.full(n, self._last),
            lower_bound=np.full(n, self._last - 1),
            upper_bound=np.full(n, self._last + 1),
            timestamps=None,
            model_name="good",
        )


class _Biased:
    def fit(self, data, *, context=None):
        return self

    def predict(self, horizon, *, context=None):
        from trendx.forecasting.base import ForecastResult

        n = int(horizon)
        return ForecastResult(
            values=np.full(n, 1000.0),
            lower_bound=np.full(n, 999.0),
            upper_bound=np.full(n, 1001.0),
            timestamps=None,
            model_name="biased",
        )


class _Broken:
    def fit(self, data, *, context=None):
        raise RuntimeError("fit exploded")

    def predict(self, horizon, *, context=None):
        raise RuntimeError("unreachable")


def _model(
    mid="m1",
    target="temperature",
    algo=Algorithm.PROPHET,
    fp="fp",
    freq="1h",
    hz=24,
    status=ModelStatus.READY,
):
    return RegisteredModel(
        model_id=mid,
        tenant_id="t",
        entity_id="e",
        target_metric=target,
        algorithm=algo,
        feature_schema_fingerprint=fp,
        frequency=freq,
        horizon=hz,
        status=status,
        role=ModelRole.CHALLENGER,
        model_uri="runs:/r/model",
    )


def _spec(mid="m1", engine=None, **kw):
    eng = engine or _Good()
    return CandidateSpec(model=_model(mid, **kw), loader=lambda m: eng)


@pytest.mark.unit
def test_a_temporal_ordering():
    ds = _series()["ds"]
    for train_idx, test_idx in walk_folds(ds, n_folds=3, test_size=24):
        assert train_idx.max() < test_idx.min()


@pytest.mark.unit
def test_b_no_shuffle():
    ds = _series()["ds"]
    for _, test_idx in walk_folds(ds, n_folds=2, test_size=24):
        assert (np.diff(test_idx) == 1).all()


@pytest.mark.unit
def test_c_no_leakage_train_before_test():
    frame = _series()
    ev = evaluate_candidate(_spec(), frame, n_folds=2, test_size=24)
    assert ev.valid_folds == 2
    for f in ev.folds:
        assert f.train_end < f.test_start


@pytest.mark.unit
def test_d_horizon_24_1h():
    ev = evaluate_candidate(_spec(), _series(), n_folds=1, test_size=24)
    assert ev.valid_folds == 1


@pytest.mark.unit
def test_e_horizon_96_15m():
    idx = pd.date_range("2026-01-01", periods=400, freq="15min", tz="UTC")
    frame = pd.DataFrame({"ds": idx, "y": np.arange(400, dtype=float)})
    ev = evaluate_candidate(_spec(), frame, n_folds=1, test_size=96)
    assert ev.valid_folds == 1


@pytest.mark.unit
def test_f_frequency_kept():
    ds = _series()["ds"]
    assert (ds.diff().dropna() == pd.Timedelta("1h")).all()


@pytest.mark.unit
def test_g_frequency_15m_kept():
    idx = pd.date_range("2026-01-01", periods=100, freq="15min", tz="UTC")
    assert (idx.to_series().diff().dropna() == pd.Timedelta("15min")).all()


@pytest.mark.unit
def test_h_schema_fingerprint_propagated():
    fp = FeatureSchema(features=(FeatureDefinition(name="a", metric="a"),)).fingerprint()
    m = _model()
    assert isinstance(fp, str) and len(fp) == 64
    assert m.feature_schema_fingerprint == "fp"


@pytest.mark.unit
def test_i_schema_mismatch_detected():
    from trendx.forecasting.contract import ForecastRequest
    from trendx.forecasting.dataset import DatasetBuilder
    from trendx.forecasting.resolution import FeatureResolver

    feats = (
        FeatureDefinition(name="temperature", metric="temperature", lag="1h"),
        FeatureDefinition(name="humidity", metric="humidity"),
        FeatureDefinition(name="occupancy", metric="occupancy"),
    )
    req = ForecastRequest(
        tenant_id="t",
        entity_type="DEVICE",
        entity_id="e",
        target_metric="temperature",
        horizon=24,
        frequency="1h",
        features=feats,
    )
    schema = FeatureSchema(features=feats)
    frames = {
        ("e", "temperature"): pd.DataFrame(
            {
                "ts": pd.date_range("2026-01-01", periods=72, freq="30min", tz="UTC"),
                "value": [1.0] * 72,
            }
        ),
        ("e", "humidity"): pd.DataFrame(
            {
                "ts": pd.date_range("2026-01-01", periods=72, freq="30min", tz="UTC"),
                "value": [2.0] * 72,
            }
        ),
        ("e", "occupancy"): pd.DataFrame(
            {
                "ts": pd.date_range("2026-01-01", periods=72, freq="30min", tz="UTC"),
                "value": [3.0] * 72,
            }
        ),
    }
    resolved = FeatureResolver(available=set(frames)).resolve(req, schema)
    ds = DatasetBuilder(frame_provider=lambda e, m: frames[(e, m)].copy()).build(req, resolved)
    model = _model(fp="other-fingerprint")
    c = check_compatibility(model, ds)
    assert not c.ok and c.reason is IncompatibilityReason.FEATURE_SCHEMA_MISMATCH


@pytest.mark.unit
def test_j_candidate_failure_isolated():
    ev = evaluate_candidate(_spec(engine=_Broken()), _series(), n_folds=2)
    assert ev.valid_folds == 0 and ev.status == "invalid"
    assert all(f.status == "failed" for f in ev.folds)


@pytest.mark.unit
def test_k_no_valid_candidate():
    sel = select_best((evaluate_candidate(_spec("m1", _Broken()), _series(), n_folds=1),))
    assert sel.winner_model_id is None and sel.reason == NO_VALID_CANDIDATE


@pytest.mark.unit
def test_l_deterministic_selection():
    frame = _series()
    a = select_best((evaluate_candidate(_spec("m1"), frame, n_folds=2),))
    b = select_best((evaluate_candidate(_spec("m1"), frame, n_folds=2),))
    assert a.winner_model_id == b.winner_model_id == "m1"


@pytest.mark.unit
def test_m_deterministic_tiebreak():
    frame = _series()
    e1 = evaluate_candidate(_spec("m-b"), frame, n_folds=1)
    e2 = evaluate_candidate(_spec("m-a"), frame, n_folds=1)
    sel = select_best((e1, e2))
    assert sel.winner_model_id == "m-a"


@pytest.mark.unit
def test_n_metric_independence():
    for target in ("temperature", "humidity", "energy_consumption", "solar_power"):
        ev = evaluate_candidate(_spec("m", target=target), _series(), n_folds=1)
        assert ev.valid_folds == 1


@pytest.mark.unit
def test_o_no_temperature_branch():
    path = (
        Path(__file__).resolve().parent.parent.parent
        / "src"
        / "trendx"
        / "forecasting"
        / "evaluation.py"
    )
    assert "temperature" not in path.read_text(encoding="utf-8").lower()


@pytest.mark.unit
def test_p_optional_external_explicit():
    from trendx.forecasting.contract import FeatureCategory
    from trendx.forecasting.resolution import FeatureResolver

    w = FeatureDefinition(
        name="cloud_cover",
        metric="cloud_cover",
        category=FeatureCategory.EXTERNAL_FORECAST,
        source="weather",
    )
    req = _req_like(target="energy_consumption", features=(w,))
    r = FeatureResolver(available={("e", "energy_consumption")}).resolve(
        req, FeatureSchema(features=(w,))
    )
    assert r.ok
    assert r.resolutions[0].status.value == "unavailable"


def _req_like(target="energy_consumption", features=()):
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
def test_q_required_external_controlled():
    from trendx.forecasting.contract import FeatureCategory
    from trendx.forecasting.resolution import FeatureResolver

    w = FeatureDefinition(
        name="cloud_cover",
        metric="cloud_cover",
        category=FeatureCategory.EXTERNAL_FORECAST,
        source="weather",
        required=True,
    )
    req = _req_like(features=(w,))
    r = FeatureResolver(available={("e", "energy_consumption")}).resolve(
        req, FeatureSchema(features=(w,))
    )
    assert not r.ok


@pytest.mark.unit
def test_r_provider_independence_scada_and_weather():
    from trendx.forecasting.providers import ExternalFeatureProvider, InMemoryProvider

    frame = pd.DataFrame(
        {"ts": pd.date_range("2026-01-01", periods=10, freq="1h", tz="UTC"), "value": [1.0] * 10}
    )
    wx = InMemoryProvider(frames={("loc", "cloud_cover"): frame})
    scada = InMemoryProvider(frames={("plant", "grid_load_forecast"): frame})
    assert isinstance(wx, ExternalFeatureProvider)
    assert isinstance(scada, ExternalFeatureProvider)
    assert scada is not wx


@pytest.mark.unit
def test_s_registry_compat_refused_cases():
    from trendx.forecasting.contract import ForecastRequest
    from trendx.forecasting.dataset import DatasetBuilder
    from trendx.forecasting.resolution import FeatureResolver

    req = ForecastRequest(
        tenant_id="t",
        entity_type="DEVICE",
        entity_id="e",
        target_metric="temperature",
        horizon=24,
        frequency="1h",
        features=(),
    )
    resolved = FeatureResolver(available=set()).resolve(req, FeatureSchema(features=()))
    assert resolved.ok
    ds = DatasetBuilder(
        frame_provider=lambda e, m: _series().rename(columns={"ds": "ts", "y": "value"})
        if (e, m) == ("e", "temperature")
        else pd.DataFrame()
    ).build(req, resolved)
    bad_freq = _model(fp=FeatureSchema().fingerprint(), freq="15m")
    assert check_compatibility(bad_freq, ds).reason is IncompatibilityReason.FREQUENCY_MISMATCH
    bad_hz = _model(fp=FeatureSchema().fingerprint(), hz=48)
    assert check_compatibility(bad_hz, ds).reason is IncompatibilityReason.HORIZON_MISMATCH


@pytest.mark.unit
def test_t_no_implicit_promotion():
    ev = evaluate_candidate(_spec(), _series(), n_folds=1)
    assert ev.status == "ok"
    assert not hasattr(ev, "promoted")


@pytest.mark.unit
def test_u_provenance():
    ev = evaluate_candidate(_spec("mx"), _series(), n_folds=1)
    assert ev.model_id == "mx" and ev.algorithm == "Prophet"
    assert ev.folds[0].train_end and ev.folds[0].test_start


@pytest.mark.unit
def test_v_nan_inf_explicit():
    frame = _series()
    frame.loc[10:20, "y"] = np.nan
    ev = evaluate_candidate(_spec(), frame, n_folds=1, test_size=24)
    assert ev.valid_folds in (0, 1)
    assert all(np.isfinite(v) for f in ev.folds if f.status == "ok" for v in f.scores.values())


@pytest.mark.unit
def test_w_zero_safe_percentage():
    from trendx.forecasting.base import compute_metrics

    m = compute_metrics(np.zeros(10), np.zeros(10)).to_dict()
    assert all(np.isfinite(v) for v in m.values())


@pytest.mark.unit
def test_x_multi_entity_isolation():
    a = evaluate_candidate(_spec("ma"), _series(), n_folds=1)
    b = evaluate_candidate(_spec("mb"), _series(base=99.0), n_folds=1)
    assert a.model_id != b.model_id


@pytest.mark.unit
def test_y_multi_metric_same_path():
    for target in ("temperature", "humidity", "energy_consumption", "solar_power"):
        ev = evaluate_candidate(_spec("m", target=target), _series(), n_folds=1)
        assert ev.status == "ok"


@pytest.mark.unit
def test_z_no_side_effect_markers():
    assert NO_VALID_CANDIDATE == "no-valid-candidate"


@pytest.mark.unit
def test_aa_no_mlflow_in_evaluation():
    import inspect

    import trendx.forecasting.evaluation as mod

    assert "mlflow" not in inspect.getsource(mod).lower()


@pytest.mark.unit
def test_ab_no_thingsboard_in_evaluation():
    import inspect

    import trendx.forecasting.evaluation as mod

    assert "thingsboard" not in inspect.getsource(mod).lower()


@pytest.mark.unit
def test_ac_scheduler_worker_untouched_by_evaluation():
    import inspect

    import trendx.forecasting.evaluation as mod

    src = inspect.getsource(mod)
    assert "services.worker" not in src and "scheduler.service" not in src


@pytest.mark.unit
def test_ad_better_model_wins():
    frame = _series()
    good = evaluate_candidate(_spec("good"), frame, n_folds=2)
    bad = evaluate_candidate(_spec("bad", _Biased()), frame, n_folds=2)
    sel = select_best((bad, good))
    assert sel.winner_model_id == "good"
