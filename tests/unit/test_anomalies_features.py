from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from trendx.anomalies.features import FeatureExtractor


@pytest.fixture
def time_series():
    rng = np.random.default_rng(42)
    n = 100
    idx = pd.date_range("2024-01-01", periods=n, freq="1h")
    values = 50 + 10 * np.sin(2 * np.pi * np.arange(n) / 24) + rng.normal(0, 1, n)
    return pd.Series(values, index=idx, name="temperature")


@pytest.fixture
def extractor():
    return FeatureExtractor(window_size=12, step_size=6)


@pytest.mark.unit
def test_create_windows(extractor, time_series):
    windows = list(extractor.create_windows(time_series))
    assert len(windows) > 0
    for w, start, end in windows:
        assert len(w) == 12
        assert end - start == 12


@pytest.mark.unit
def test_create_windows_too_short():
    extractor = FeatureExtractor(window_size=50)
    series = pd.Series(np.arange(10))
    windows = list(extractor.create_windows(series))
    assert len(windows) == 0


@pytest.mark.unit
def test_extract_features_all(extractor, time_series):
    features = extractor.extract_features(time_series)
    assert not features.empty
    expected = {"mean", "std", "min", "max", "slope", "amplitude", "spectral_energy", "diff_from_previous", "nan_ratio", "kurtosis", "skewness"}
    assert expected.issubset(set(features.columns))


@pytest.mark.unit
def test_extract_features_multivariate():
    extractor = FeatureExtractor(window_size=12, step_size=6)
    rng = np.random.default_rng(42)
    n = 50
    idx = pd.date_range("2024-01-01", periods=n, freq="1h")
    df = pd.DataFrame({
        "temp": 50 + 10 * np.sin(2 * np.pi * np.arange(n) / 24) + rng.normal(0, 1, n),
        "humidity": 60 + 5 * np.cos(2 * np.pi * np.arange(n) / 24) + rng.normal(0, 0.5, n),
    }, index=idx)
    features = extractor.extract_features_multivariate(df)
    assert not features.empty
    assert any("temp_mean" in c or "temp_" in c for c in features.columns)


@pytest.mark.unit
def test_normalize_features(extractor, time_series):
    features = extractor.extract_features(time_series)
    normalized = extractor.normalize_features(features, method="robust")
    assert not normalized.empty
    assert normalized.select_dtypes(include=[np.number]).shape[1] > 2


@pytest.mark.unit
def test_normalize_features_empty():
    extractor = FeatureExtractor(window_size=12)
    result = extractor.normalize_features(pd.DataFrame())
    assert result.empty


@pytest.mark.unit
def test_normalize_features_invalid_method(extractor, time_series):
    features = extractor.extract_features(time_series)
    with pytest.raises(ValueError, match="Unknown normalization method"):
        extractor.normalize_features(features, method="invalid")


@pytest.mark.unit
def test_window_size_validation():
    with pytest.raises(ValueError, match="window_size must be >= 2"):
        FeatureExtractor(window_size=1)


@pytest.mark.unit
def test_extract_all_nan_window():
    series = pd.Series([np.nan] * 20, name="test")
    extractor = FeatureExtractor(window_size=10, step_size=10)
    features = extractor.extract_features(series)
    assert not features.empty
    assert features["nan_ratio"].iloc[0] == 1.0
