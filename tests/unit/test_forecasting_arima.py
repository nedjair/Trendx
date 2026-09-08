from __future__ import annotations

import numpy as np
import pandas as pd
import pytest
from trendx.forecasting.arima import ArimaModel
from trendx.forecasting.base import ForecastResult


@pytest.fixture
def arima_data():
    rng = np.random.default_rng(42)
    n = 300
    ts = pd.date_range("2024-01-01", periods=n, freq="1h")
    trend = np.linspace(0, 5, n)
    seasonality = 5 * np.sin(2 * np.pi * np.arange(n) / 24)
    noise = rng.normal(0, 0.3, n)
    y = 50 + trend + seasonality + noise
    return pd.DataFrame({"ds": ts, "y": y})


@pytest.mark.unit
def test_auto_arima(arima_data):
    model = ArimaModel(
        seasonal=False,
        max_p=3,
        max_d=1,
        max_q=3,
        information_criterion="aic",
    )
    model.fit(arima_data)
    result = model.predict(horizon=12)
    assert isinstance(result, ForecastResult)
    assert len(result.values) == 12
    assert np.all(np.isfinite(result.values))


@pytest.mark.unit
def test_manual_order(arima_data):
    model = ArimaModel(
        order=(1, 0, 1),
        seasonal=False,
        confidence_level=0.80,
    )
    model.fit(arima_data)
    result = model.predict(horizon=24)
    assert len(result.values) == 24
    assert len(result.lower_bound) == 24
    assert len(result.upper_bound) == 24


@pytest.mark.unit
def test_prediction_shape(arima_data):
    model = ArimaModel(
        order=(1, 0, 0),
        seasonal=False,
        confidence_level=0.95,
    )
    model.fit(arima_data)
    for horizon in (1, 6, 24):
        result = model.predict(horizon=horizon)
        assert len(result.values) == horizon
        assert len(result.lower_bound) == horizon
        assert len(result.upper_bound) == horizon
    assert np.all(result.upper_bound >= result.lower_bound)


@pytest.mark.unit
def test_predict_before_fit_raises():
    model = ArimaModel(order=(1, 0, 1))
    with pytest.raises(RuntimeError, match="not fitted"):
        model.predict(horizon=12)


@pytest.mark.unit
def test_infer_seasonal_period():
    model = ArimaModel(m=24)
    n = 100
    ts = pd.date_range("2024-01-01", periods=n, freq="1h")
    df = pd.DataFrame({"ds": ts, "y": np.arange(n, dtype=float)})
    period = model._infer_seasonal_period(df)
    assert period == 24


@pytest.mark.unit
def test_infer_seasonal_period_default():
    model = ArimaModel()
    df = pd.DataFrame({"ds": pd.to_datetime(["2024-01-01"]), "y": [1.0]})
    period = model._infer_seasonal_period(df)
    assert period >= 1
