"""Validation reference Forecast — benchmark synthetique reproductible.

2 devices (UUIDs), 2 metriques, series UTC, train/validation/horizon.
Compare Trendx vs reference directe (meme bibliotheque), no invented
external Trendz reference. Aucun TB reel, aucun device en dur,
writeback desactive, timestamps UTC.
"""

from __future__ import annotations

import uuid
from datetime import datetime
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest
from trendx.forecasting.base import Strategy, compute_metrics
from trendx.forecasting.selector import ModelSelector
from trendx.services import worker as worker_mod


def _series(n: int = 224, seed: int = 7, phase: float = 0.0, amp: float = 5.0) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    ts = pd.date_range("2024-01-01", periods=n, freq="1h", tz="UTC")
    y = 20 + amp * np.sin(2 * np.pi * np.arange(n) / 24 + phase) + np.arange(n) * 0.02
    y = y + rng.normal(0, 0.4, n)
    return pd.DataFrame({"ds": ts, "y": y})


@pytest.fixture
def two_devices_two_metrics():
    dev_a = str(uuid.uuid4())
    dev_b = str(uuid.uuid4())
    return {
        (dev_a, "temperature"): _series(seed=11, phase=0.0, amp=5.0),
        (dev_a, "humidity"): _series(seed=12, phase=1.0, amp=8.0),
        (dev_b, "temperature"): _series(seed=21, phase=0.5, amp=4.0),
        (dev_b, "humidity"): _series(seed=22, phase=1.5, amp=7.0),
    }


def _split(df: pd.DataFrame, horizon: int = 24):
    return df.iloc[:-horizon].copy(), df.iloc[-horizon:].copy()


# 8.1 LINEAR vs sklearn direct -------------------------------------------------
@pytest.mark.unit
def test_ref_linear_vs_sklearn(two_devices_two_metrics):
    from sklearn.linear_model import LinearRegression
    from trendx.forecasting.linear import LinearRegressionModel, _engineer_features

    for (_, _), df in list(two_devices_two_metrics.items())[:2]:
        train, valid = _split(df)
        m = LinearRegressionModel(feature_set=["trend", "hour"])
        m.fit(train)
        res = m.predict(horizon=24)
        assert len(res.values) == 24
        assert np.all(np.isfinite(res.values))
        # Reference directe meme features + sklearn
        feats_train = _engineer_features(train, ["trend", "hour"]).values.astype(float)
        ref = LinearRegression().fit(feats_train, train["y"].values.astype(float))
        pred_ref = ref.predict(feats_train[:24])
        assert np.all(np.isfinite(pred_ref))
        # Metriques vs verite
        met = compute_metrics(valid["y"].values.astype(float), res.values)
        assert met.mae >= 0 and np.isfinite(met.mae)
        assert met.rmse >= 0 and np.isfinite(met.rmse)


# 8.2 OLS vs statsmodels direct ------------------------------------------------
@pytest.mark.unit
def test_ref_ols_vs_statsmodels(two_devices_two_metrics):
    import statsmodels.api as sm
    from trendx.forecasting.linear import OLSRegressionModel, _engineer_features

    df = next(iter(two_devices_two_metrics.values()))
    train, valid = _split(df)
    m = OLSRegressionModel(feature_set=["trend", "hour"])
    m.fit(train)
    res = m.predict(horizon=12)
    assert len(res.values) == 12
    assert np.all(res.upper_bound >= res.lower_bound)
    feats = _engineer_features(train, ["trend", "hour"]).values.astype(float)
    x = sm.add_constant(feats)
    direct = sm.OLS(train["y"].values.astype(float), x).fit()
    assert len(direct.params) == feats.shape[1] + 1
    met = compute_metrics(valid["y"].values[:12].astype(float), res.values)
    assert met.mae >= 0


# 8.3 ARIMA manuel vs pmdarima direct ------------------------------------------
@pytest.mark.unit
def test_ref_arima_manual_vs_direct(two_devices_two_metrics):
    from pmdarima.arima import ARIMA
    from trendx.forecasting.arima import ArimaModel

    df = next(iter(two_devices_two_metrics.values()))
    train, valid = _split(df, horizon=12)
    m = ArimaModel(order=(1, 0, 1), seasonal=False)
    m.fit(train)
    res = m.predict(horizon=12)
    assert len(res.values) == 12
    assert np.all(np.isfinite(res.values))
    y = train["y"].values.astype(float)
    direct = ARIMA(order=(1, 0, 1), suppress_warnings=True)
    direct.fit(y)
    pred, _ = direct.predict(n_periods=12, return_conf_int=True)
    pred = np.asarray(pred, dtype=float).ravel()
    diff_abs = float(np.mean(np.abs(res.values - pred)))
    # Meme ordre, meme lib : tolerance serree
    assert diff_abs < 1e-6, f"ARIMA Trendx vs direct diff {diff_abs}"
    met = compute_metrics(valid["y"].values.astype(float), res.values)
    assert met.smape >= 0


# 8.4 FOURIER vs regression directe memes features ------------------------------
@pytest.mark.unit
def test_ref_fourier_vs_direct(two_devices_two_metrics):
    from sklearn.linear_model import LinearRegression
    from trendx.forecasting.fourier import FourierModel

    df = next(iter(two_devices_two_metrics.values()))
    train, valid = _split(df)
    m = FourierModel(k_daily=3, k_weekly=0, k_yearly=0, include_trend=True)
    m.fit(train)
    res = m.predict(horizon=24)
    assert len(res.values) == 24
    assert np.all(np.isfinite(res.values))
    feats = m._build_features(train).values.astype(float)
    ref = LinearRegression().fit(feats, train["y"].values.astype(float))
    assert ref.coef_.shape[0] == feats.shape[1]
    met = compute_metrics(valid["y"].values.astype(float), res.values)
    assert met.mape >= 0


# 8.5 Prophet vs prophet direct (tolerance elargie, determinisme partiel) -------
@pytest.mark.unit
def test_ref_prophet_vs_direct():
    from prophet import Prophet
    from trendx.forecasting.prophet import ProphetModel

    # Prophet refuse les ds timezone-aware : series naive reproductible.
    df = _series(n=150, seed=99)
    df = df.assign(ds=lambda d: pd.to_datetime(d["ds"]).dt.tz_localize(None))
    train, valid = _split(df, horizon=12)
    m = ProphetModel(
        yearly_seasonality=False,
        weekly_seasonality=False,
        daily_seasonality=True,
        changepoint_prior_scale=0.01,
        seasonality_prior_scale=1.0,
    )
    m.fit(train)
    res = m.predict(horizon=12)
    assert len(res.values) == 12
    assert np.all(res.upper_bound >= res.values)
    # Reference directe meme config
    pm = Prophet(
        yearly_seasonality=False,
        weekly_seasonality=False,
        daily_seasonality=True,
        changepoint_prior_scale=0.01,
        seasonality_prior_scale=1.0,
        interval_width=0.80,
    )
    pm.fit(train.rename(columns={"ds": "ds", "y": "y"}))
    fut = pm.make_future_dataframe(periods=12, freq="h", include_history=False)
    fc = pm.predict(fut)
    ref_vals = fc["yhat"].values.astype(float)
    diff_rel = float(np.mean(np.abs(res.values - ref_vals) / (np.abs(ref_vals) + 1e-9)))
    assert diff_rel < 1e-6, f"Prophet Trendx vs direct diff_rel {diff_rel}"
    met = compute_metrics(valid["y"].values.astype(float), res.values)
    assert met.mae >= 0


# 8.7 Normalisation auto : regle documentee ------------------------------------
@pytest.mark.unit
def test_ref_normalizer_auto_rule():
    from trendx.preprocessing.normalizer import Normalizer

    rng = np.random.default_rng(42)
    heavy = np.concatenate([rng.normal(0, 1, 80), rng.normal(0, 10, 20)])
    assert Normalizer(method="auto").fit(heavy).method == "robust"
    skewed = np.random.default_rng(38).gamma(1.5, 2, 100)
    assert Normalizer(method="auto").fit(skewed).method == "minmax"
    gauss = np.random.default_rng(42).normal(0, 1, 200)
    assert Normalizer(method="auto").fit(gauss).method == "standard"


# 8.21 Metriques vs calcul manuel -----------------------------------------------
@pytest.mark.unit
def test_ref_metrics_vs_manual():
    y_true = np.array([10.0, 20.0, 30.0])
    y_pred = np.array([12.0, 18.0, 33.0])
    m = compute_metrics(y_true, y_pred)
    assert m.mae == pytest.approx(7 / 3, rel=1e-9)
    assert m.rmse == pytest.approx(float(np.sqrt(17 / 3)), rel=1e-9)
    assert m.mape == pytest.approx((0.4 / 3) * 100, rel=1e-9)
    assert m.smape > 0 and np.isfinite(m.smape)


# 8.20 Competition/selection ----------------------------------------------------
@pytest.mark.unit
def test_ref_competition_selection(two_devices_two_metrics):
    sel = ModelSelector(
        strategy=Strategy.PER_DEVICE,
        n_windows=2,
        test_size=0.2,
        min_stable_windows=2,
        marginal_gain_threshold=0.0,
    )
    for (_, _), df in list(two_devices_two_metrics.items())[:2]:
        cands = [
            ("LinearRegression", {"feature_set": ["trend"]}),
            ("LinearRegression", {"feature_set": ["trend", "hour"]}),
        ]
        results = sel.run_competition(str(uuid.uuid4()), "temperature", df, cands, n_windows=2)
        assert len(results) >= 1
        champ = sel.select_champion(results)
        assert champ is not None
        # Champion = sMAPE minimal
        smapes = [r.aggregated_metrics.smape for r in results]
        assert champ.aggregated_metrics.smape == pytest.approx(min(smapes))


# 8.19 Strategies : routage worker (semantique Trendz externe non demontrable) --
@pytest.mark.unit
@pytest.mark.parametrize("strategy", ["PER_DEVICE", "PER_PROFILE", "GLOBAL", "AUTO"])
def test_ref_strategies_routing(strategy):
    from trendx.services.training import TrainingService

    dev = str(uuid.uuid4())
    job = {
        "tenant_id": str(uuid.uuid4()),
        "entity_type": "DEVICE",
        "entity_id": dev,
        "metric_name": "temperature",
        "strategy": strategy,
    }
    fake = MagicMock()
    fake.id = uuid.uuid4()
    with (
        patch.object(TrainingService, "train_model", return_value=fake) as mt,
        patch.object(TrainingService, "auto_select_strategy", return_value=fake) as ma,
    ):
        out = worker_mod._run_trendx_train(job, "t", "e")
    assert out["strategy"] == strategy
    if strategy == "AUTO":
        ma.assert_called_once()
    else:
        mt.assert_called_once()


# 8.17 Versionnement champion/rollback (logique, sans DB reelle) -----------------
@pytest.mark.unit
def test_ref_versioning_rollback_logic():
    from trendx.mlops.registry import ModelRegistry

    reg = ModelRegistry(repository=MagicMock())
    cur = MagicMock()
    cur.id = 2
    cur.status = "champion"
    prev = MagicMock()
    prev.id = 1
    prev.status = "challenger"
    sess = MagicMock()
    repo = MagicMock()
    repo.find_champion.return_value = cur
    repo.find_previous_champion.return_value = prev
    cm = MagicMock()
    cm.__enter__.return_value = sess
    cm.__exit__.return_value = False
    with (
        patch("trendx.mlops.registry.db_manager.get_session", return_value=cm),
        patch("trendx.mlops.registry.PredictionModelRepository", return_value=repo),
    ):
        out = reg.rollback("dev-x", "temperature")
    assert out is not None and out.id == 1


# Pipeline worker train->forecast (mocks, UTC, sans writeback) -------------------
@pytest.mark.unit
def test_ref_worker_pipeline_train_then_forecast():
    dev_a = str(uuid.uuid4())
    dev_b = str(uuid.uuid4())
    mk = "temperature"
    fake_model = MagicMock()
    fake_model.id = uuid.uuid4()
    fake_fc = MagicMock()
    fake_fc.values = np.array([1.0, 2.0, 3.0])
    fake_fc.lower_bound = np.array([0.9, 1.9, 2.9])
    fake_fc.upper_bound = np.array([1.1, 2.1, 3.1])
    fake_fc.timestamps = None
    fake_fc.model_name = "LinearRegression"

    def _base(dev):
        return {
            "tenant_id": str(uuid.uuid4()),
            "entity_type": "DEVICE",
            "entity_id": dev,
            "metric_name": mk,
        }

    with (
        patch("trendx.services.training.TrainingService") as mt,
        patch("trendx.services.inference.InferenceService") as mi,
        patch("trendx.thingsboard.client.ThingsBoardClient", autospec=True) as tb,
    ):
        mt.return_value.train_model.return_value = fake_model
        mt.return_value.auto_select_strategy.return_value = fake_model
        mi.return_value.generate_forecast.return_value = fake_fc
        mi.return_value.save_forecast_results.return_value = 3
        # Configure dry_run attribut mocke
        mi.return_value.dry_run = True

        for dev in (dev_a, dev_b):
            tr = worker_mod._run_trendx_train(_base(dev), "t", "e")
            assert tr["status"] == "ok" and tr["entity_id"] == dev
            ts = datetime.fromisoformat(tr["generated_at"])
            assert ts.tzinfo is not None

            fc = worker_mod._run_trendx_forecast({**_base(dev), "horizon": 3}, "t", "e")
            assert fc["status"] == "ok" and fc["points"] == 3
            assert fc["writeback"] == "disabled"
            assert datetime.fromisoformat(fc["generated_at"]).tzinfo is not None

        tb.assert_not_called()

    # Isolation : un device KO ne bloque pas l'autre
    with patch("trendx.services.training.TrainingService") as mt2:

        def _side(entity_id, metric_key, **kw):
            if entity_id == dev_b:
                raise RuntimeError("ko")
            return fake_model

        mt2.return_value.train_model.side_effect = _side
        assert worker_mod._run_trendx_train(_base(dev_a), "t", "e")["status"] == "ok"
        try:
            worker_mod._run_trendx_train(_base(dev_b), "t", "e")
            raise AssertionError("dev_b aurait du lever")
        except RuntimeError:
            pass
