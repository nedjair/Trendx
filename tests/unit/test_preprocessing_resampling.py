from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from trendx.preprocessing.resampling import Resampler


@pytest.fixture
def hourly_df():
    rng = pd.date_range("2024-01-01", periods=48, freq="1h")
    values = np.sin(np.arange(48) * 2 * np.pi / 24) + np.random.default_rng(42).normal(0, 0.1, 48)
    return pd.DataFrame({"ts": rng, "value": values})


@pytest.mark.unit
def test_resample_downsample(hourly_df):
    resampler = Resampler()
    result = resampler.downsample(hourly_df, frequency="6h", method="mean")
    assert len(result) < len(hourly_df)
    assert "ts" in result.columns
    assert "value" in result.columns


@pytest.mark.unit
def test_resample_upsample(hourly_df):
    resampler = Resampler()
    result = resampler.upsample(hourly_df, frequency="15min", method="linear")
    assert len(result) > len(hourly_df)
    assert result["value"].isna().sum() == 0


@pytest.mark.unit
def test_interpolate_missing_linear():
    rng = pd.date_range("2024-01-01", periods=10, freq="1h")
    values = [1.0, np.nan, 3.0, np.nan, np.nan, 6.0, 7.0, np.nan, 9.0, 10.0]
    df = pd.DataFrame({"ts": rng, "value": values})

    resampler = Resampler()
    result = resampler.interpolate_missing(df, method="linear")
    assert result["value"].isna().sum() < df["value"].isna().sum()
    assert result["value"].iloc[1] == pytest.approx(2.0, abs=0.01)


@pytest.mark.unit
def test_detect_frequency():
    rng = pd.date_range("2024-01-01", periods=10, freq="30min")
    df = pd.DataFrame({"ts": rng, "value": np.arange(10.0)})
    resampler = Resampler()
    freq = resampler.detect_frequency(df)
    assert freq == "30min"


@pytest.mark.unit
def test_aggregate_multiple():
    rng = pd.date_range("2024-01-01", periods=48, freq="1h")
    df = pd.DataFrame({"ts": rng, "temp": np.random.default_rng(42).normal(20, 5, 48), "humidity": np.random.default_rng(99).normal(50, 10, 48)})
    resampler = Resampler()
    result = resampler.aggregate(df, "6h", {"temp": "mean", "humidity": ["min", "max"]})
    assert not result.empty
    assert "humidity_min" in result.columns or any("humidity" in c for c in result.columns)


@pytest.mark.unit
def test_resample_empty():
    df = pd.DataFrame({"ts": pd.to_datetime([]), "value": []})
    resampler = Resampler()
    result = resampler.resample(df, "1h")
    assert result.empty


@pytest.mark.unit
def test_invalid_method():
    rng = pd.date_range("2024-01-01", periods=10, freq="1h")
    df = pd.DataFrame({"ts": rng, "value": np.arange(10.0)})
    resampler = Resampler()
    with pytest.raises(ValueError, match="Unsupported resample method"):
        resampler.resample(df, "1h", method="invalid")


@pytest.mark.unit
def test_detect_frequency_too_few():
    df = pd.DataFrame({"ts": pd.to_datetime(["2024-01-01"]), "value": [1.0]})
    resampler = Resampler()
    freq = resampler.detect_frequency(df)
    assert freq == "1h"


@pytest.mark.unit
def test_resample_no_value_col():
    rng = pd.date_range("2024-01-01", periods=10, freq="1h")
    df = pd.DataFrame({"ts": rng, "reading": np.arange(10.0)})
    resampler = Resampler()
    result = resampler.resample(df, "2h", method="mean")
    assert not result.empty
    assert "reading" in result.columns
