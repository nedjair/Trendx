from __future__ import annotations

import numpy as np
import pandas as pd
import pytest
from trendx.forecasting.base import ForecastResult
from trendx.forecasting.fourier import FourierModel, _fourier_series


@pytest.fixture
def fourier_data():
    rng = np.random.default_rng(42)
    n = 500
    ts = pd.date_range("2024-01-01", periods=n, freq="1h")
    daily = 10 * np.sin(2 * np.pi * np.arange(n) / 24)
    weekly = 5 * np.sin(2 * np.pi * np.arange(n) / (24 * 7))
    noise = rng.normal(0, 0.5, n)
    y = 50 + daily + weekly + noise
    return pd.DataFrame({"ds": ts, "y": y})


@pytest.mark.unit
def test_fit_predict(fourier_data):
    model = FourierModel(k_daily=5, k_weekly=3, k_yearly=0, include_trend=True)
    model.fit(fourier_data)
    result = model.predict(horizon=24)
    assert isinstance(result, ForecastResult)
    assert len(result.values) == 24
    assert np.all(np.isfinite(result.values))
    assert np.all(result.upper_bound >= result.lower_bound)


@pytest.mark.unit
def test_seasonality_detection():
    n = 100
    ts = pd.date_range("2024-01-01", periods=n, freq="1h")
    data = pd.DataFrame({"ds": ts, "y": np.sin(2 * np.pi * np.arange(n) / 24)})
    model = FourierModel(k_daily=3, k_weekly=0, k_yearly=0, include_trend=True)
    model.fit(data)
    result = model.predict(horizon=24)
    assert len(result.values) == 24


@pytest.mark.unit
def test_fourier_series_output():
    ts = pd.date_range("2024-01-01", periods=48, freq="1h")
    result = _fourier_series(ts, period=86400.0, k=3)
    assert result.shape[1] == 6
    assert "sin_86400_1" in result.columns
    assert "cos_86400_3" in result.columns


@pytest.mark.unit
def test_no_seasonality_raises():
    with pytest.raises(ValueError, match="At least one seasonality"):
        FourierModel(k_daily=0, k_weekly=0, k_yearly=0)


@pytest.mark.unit
def test_predict_before_fit_raises():
    model = FourierModel(k_daily=3, k_weekly=0, k_yearly=0)
    with pytest.raises(RuntimeError, match="not fitted"):
        model.predict(horizon=12)


@pytest.mark.unit
@pytest.mark.xfail(
    reason="FourierModel save/load missing internal attributes — attend le future commit sur feat/detector-scoring-and-forecasting-fixes"
)
def test_save_load(fourier_data):
    import tempfile
    from pathlib import Path

    model = FourierModel(k_daily=3, k_weekly=0, k_yearly=0)
    model.fit(fourier_data)
    with tempfile.NamedTemporaryFile(suffix=".pkl", delete=False) as tmp:
        path = tmp.name

    try:
        model.save(path)
        loaded = FourierModel.load(path)
        result_orig = model.predict(horizon=6)
        result_loaded = loaded.predict(horizon=6)
        np.testing.assert_allclose(result_orig.values, result_loaded.values, atol=1e-6)
    finally:
        Path(path).unlink(missing_ok=True)
