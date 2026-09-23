from __future__ import annotations

import tempfile
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
from trendx.forecasting.prophet import ProphetModel


@pytest.fixture
def synthetic_data():
    rng = np.random.default_rng(42)
    n = 300
    ts = pd.date_range("2023-01-01", periods=n, freq="1h")
    trend = np.linspace(0, 5, n)
    seasonality = 10 * np.sin(2 * np.pi * np.arange(n) / 24)
    noise = rng.normal(0, 0.5, n)
    y = 50 + trend + seasonality + noise
    return pd.DataFrame({"ds": ts, "y": y})


@pytest.mark.unit
def test_fit_and_predict(synthetic_data):
    model = ProphetModel(
        yearly_seasonality=False,
        weekly_seasonality=False,
        daily_seasonality=True,
        changepoint_prior_scale=0.01,
        seasonality_prior_scale=1.0,
    )
    model.fit(synthetic_data)
    result = model.predict(horizon=12)
    assert len(result.values) == 12
    assert len(result.lower_bound) == 12
    assert len(result.upper_bound) == 12
    assert np.all(result.upper_bound >= result.values)
    assert np.all(result.lower_bound <= result.values)


@pytest.mark.unit
def test_predict_with_horizon(synthetic_data):
    model = ProphetModel(
        yearly_seasonality=False,
        weekly_seasonality=False,
        daily_seasonality=True,
        changepoint_prior_scale=0.01,
        seasonality_prior_scale=1.0,
    )
    model.fit(synthetic_data)
    result_24 = model.predict(horizon=24)
    result_48 = model.predict(horizon=48)
    assert len(result_24.values) == 24
    assert len(result_48.values) == 48


@pytest.mark.unit
def test_confidence_intervals(synthetic_data):
    model = ProphetModel(
        yearly_seasonality=False,
        weekly_seasonality=False,
        daily_seasonality=True,
        confidence_level=0.95,
        changepoint_prior_scale=0.01,
        seasonality_prior_scale=1.0,
    )
    model.fit(synthetic_data)
    result = model.predict(horizon=12)
    widths = result.upper_bound - result.lower_bound
    assert np.all(widths > 0)
    assert result.metrics.inference_duration >= 0


@pytest.mark.unit
@pytest.mark.xfail(
    reason="ProphetModel predict cap-floor constraints clipping — attend le future commit sur feat/detector-scoring-and-forecasting-fixes"
)
def test_cap_floor_constraints():
    rng = np.random.default_rng(42)
    n = 200
    ts = pd.date_range("2023-01-01", periods=n, freq="1h")
    y = 50 + 10 * np.sin(2 * np.pi * np.arange(n) / 24) + rng.normal(0, 0.5, n)
    df = pd.DataFrame({"ds": ts, "y": y})

    model = ProphetModel(
        growth="logistic",
        cap=60.0,
        floor=40.0,
        yearly_seasonality=False,
        weekly_seasonality=False,
        daily_seasonality=True,
        changepoint_prior_scale=0.01,
        seasonality_prior_scale=1.0,
    )
    model.fit(df)
    result = model.predict(horizon=12)
    assert np.all(result.values <= 60.0)
    assert np.all(result.values >= 40.0)


@pytest.mark.unit
def test_save_load(synthetic_data):
    model = ProphetModel(
        yearly_seasonality=False,
        weekly_seasonality=False,
        daily_seasonality=True,
        changepoint_prior_scale=0.01,
        seasonality_prior_scale=1.0,
    )
    model.fit(synthetic_data)

    with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as tmp:
        path = tmp.name

    try:
        model.save(path)
        loaded = ProphetModel.load(path)
        assert loaded._model is not None
        result_orig = model.predict(horizon=6)
        result_loaded = loaded.predict(horizon=6)
        np.testing.assert_allclose(result_orig.values, result_loaded.values, atol=1e-4)
    finally:
        Path(path).unlink(missing_ok=True)


@pytest.mark.unit
def test_predict_before_fit_raises():
    model = ProphetModel()
    with pytest.raises(RuntimeError, match="not fitted"):
        model.predict(horizon=12)


@pytest.mark.unit
def test_save_before_fit_raises():
    model = ProphetModel()
    with pytest.raises(RuntimeError, match="No fitted model"):
        model.save("/tmp/nonexistent.json")


@pytest.mark.unit
def test_prepare_dataframe_invalid():
    with pytest.raises((ValueError, TypeError)):
        ProphetModel._prepare_dataframe("invalid")


@pytest.mark.unit
def test_prepare_dataframe_utc_aware_to_naive():
    # W81-A : aware UTC -> naive, même mur UTC.
    df = pd.DataFrame(
        {
            "ds": pd.to_datetime(["2026-09-22 08:00:00+00:00", "2026-09-22 09:00:00+00:00"]),
            "y": [1.0, 2.0],
        }
    )
    out = ProphetModel._prepare_dataframe(df)
    assert out["ds"].dt.tz is None
    assert out["ds"].iloc[0] == pd.Timestamp("2026-09-22 08:00:00")


@pytest.mark.unit
def test_prepare_dataframe_non_utc_aware_preserves_instant():
    # W81-B : +02:00 -> mur UTC 08:00 (instant préservé, pas de troncature).
    df = pd.DataFrame(
        {
            "ds": pd.to_datetime(["2026-09-22 10:00:00+02:00", "2026-09-22 11:00:00+02:00"]),
            "y": [1.0, 2.0],
        }
    )
    out = ProphetModel._prepare_dataframe(df)
    assert out["ds"].dt.tz is None
    assert out["ds"].iloc[0] == pd.Timestamp("2026-09-22 08:00:00")
    assert out["ds"].iloc[0] != pd.Timestamp("2026-09-22 10:00:00")


@pytest.mark.unit
def test_prepare_dataframe_already_naive_untouched():
    # W81-C : naive reste naive, sans localisation système implicite.
    df = pd.DataFrame(
        {
            "ds": pd.to_datetime(["2026-09-22 08:00:00", "2026-09-22 09:00:00"]),
            "y": [1.0, 2.0],
        }
    )
    out = ProphetModel._prepare_dataframe(df)
    assert out["ds"].dt.tz is None
    assert out["ds"].iloc[0] == pd.Timestamp("2026-09-22 08:00:00")


@pytest.mark.unit
def test_fit_with_tz_aware_data_no_prophet_tz_error():
    # W81-D : fit réel avec ds aware ne doit plus lever l'erreur Prophet.
    rng = np.random.default_rng(7)
    n = 120
    ts = pd.date_range("2026-06-01", periods=n, freq="1h", tz="Europe/Paris")
    y = 20 + np.sin(2 * np.pi * np.arange(n) / 24) + rng.normal(0, 0.2, n)
    model = ProphetModel(
        yearly_seasonality=False,
        weekly_seasonality=False,
        daily_seasonality=True,
    )
    model.fit(pd.DataFrame({"ds": ts, "y": y}))
    forecast = model.predict(horizon=24)
    assert len(forecast.values) == 24
