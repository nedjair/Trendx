from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from trendx.forecasting.base import ForecastResult
from trendx.forecasting.linear import LinearRegressionModel, OLSRegressionModel, _engineer_features


@pytest.fixture
def linear_data():
    rng = np.random.default_rng(42)
    n = 200
    ts = pd.date_range("2024-01-01", periods=n, freq="1h")
    trend = np.linspace(0, 10, n)
    noise = rng.normal(0, 0.5, n)
    y = 50 + trend + noise
    return pd.DataFrame({"ds": ts, "y": y})


@pytest.mark.unit
def test_fit_predict_linear(linear_data):
    model = LinearRegressionModel(feature_set=["trend", "hour"])
    model.fit(linear_data)
    result = model.predict(horizon=12)
    assert isinstance(result, ForecastResult)
    assert len(result.values) == 12
    assert np.all(np.isfinite(result.values))


@pytest.mark.unit
def test_confidence_intervals(linear_data):
    model = LinearRegressionModel(feature_set=["trend", "hour"], confidence_level=0.95)
    model.fit(linear_data)
    result = model.predict(horizon=12)
    assert np.all(result.upper_bound >= result.lower_bound)
    widths = result.upper_bound - result.lower_bound
    assert np.all(widths > 0)


@pytest.mark.unit
def test_feature_engineering():
    ts = pd.date_range("2024-01-01", periods=100, freq="1h")
    df = pd.DataFrame({"ds": ts})
    features = _engineer_features(df)
    assert "trend" in features.columns
    assert "hour" in features.columns
    assert "dayofweek" in features.columns
    assert "sin_hour" in features.columns
    assert "cos_hour" in features.columns
    assert len(features) == 100


@pytest.mark.unit
def test_ols_fit_predict(linear_data):
    model = OLSRegressionModel(feature_set=["trend", "hour"])
    model.fit(linear_data)
    result = model.predict(horizon=12)
    assert isinstance(result, ForecastResult)
    assert len(result.values) == 12
    assert np.all(np.isfinite(result.values))


@pytest.mark.unit
def test_ols_confidence_intervals(linear_data):
    model = OLSRegressionModel(feature_set=["trend", "hour"], confidence_level=0.90)
    model.fit(linear_data)
    result = model.predict(horizon=12)
    assert np.all(result.upper_bound >= result.lower_bound)


@pytest.mark.unit
def test_predict_before_fit_raises():
    model = LinearRegressionModel()
    with pytest.raises(RuntimeError, match="not fitted"):
        model.predict(horizon=12)


@pytest.mark.unit
def test_feature_subset():
    ts = pd.date_range("2024-01-01", periods=10, freq="1h")
    df = pd.DataFrame({"ds": ts})
    features = _engineer_features(df, feature_set=["trend", "hour"])
    assert list(features.columns) == ["trend", "hour"]


@pytest.mark.unit
def test_empty_feature_set():
    ts = pd.date_range("2024-01-01", periods=10, freq="1h")
    df = pd.DataFrame({"ds": ts})
    features = _engineer_features(df, feature_set=[])
    assert features.empty


@pytest.mark.unit
def test_save_load(linear_data):
    import tempfile
    from pathlib import Path

    model = LinearRegressionModel(feature_set=["trend"])
    model.fit(linear_data)
    with tempfile.NamedTemporaryFile(suffix=".pkl", delete=False) as tmp:
        path = tmp.name

    try:
        model.save(path)
        loaded = LinearRegressionModel.load(path)
        result_orig = model.predict(horizon=6)
        result_loaded = loaded.predict(horizon=6)
        np.testing.assert_allclose(result_orig.values, result_loaded.values, atol=1e-6)
    finally:
        Path(path).unlink(missing_ok=True)
