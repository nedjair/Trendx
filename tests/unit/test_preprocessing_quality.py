from __future__ import annotations

from datetime import datetime

import numpy as np
import pandas as pd
import pytest
from trendx.preprocessing.quality import DataQualityService


@pytest.fixture
def sample_df():
    rng = pd.date_range("2024-01-01", periods=100, freq="1h")
    rng = rng.drop(rng[10:12])
    values = np.sin(np.arange(len(rng)) * 2 * np.pi / 24) * 10 + 50
    values[5] = np.nan
    values[20] = 200.0
    df = pd.DataFrame({"ts": rng, "value": values})
    return df


@pytest.fixture
def service():
    return DataQualityService()


@pytest.mark.unit
def test_check_completeness(service, sample_df):
    result = service.check_completeness(sample_df, "1h")
    assert result["actual_points"] < result["expected_points"]
    assert result["completeness_ratio"] < 1.0
    assert result["missing_points"] > 0


@pytest.mark.unit
def test_check_nulls(service, sample_df):
    result = service.check_nulls(sample_df)
    assert result["null_count"] > 0
    assert result["null_ratio"] > 0.0


@pytest.mark.unit
def test_check_duplicates(service, sample_df):
    result = service.check_duplicates(sample_df)
    assert result["duplicate_count"] == 0

    dup = pd.concat([sample_df, sample_df.iloc[[0]]], ignore_index=True)
    result = service.check_duplicates(dup)
    assert result["duplicate_count"] > 0


@pytest.mark.unit
def test_check_outliers_iqr(service, sample_df):
    result = service.check_outliers(sample_df, method="iqr")
    assert result["outlier_count"] > 0
    assert result["method"] == "iqr"


@pytest.mark.unit
def test_check_outliers_zscore(service, sample_df):
    result = service.check_outliers(sample_df, method="zscore")
    assert result["method"] == "zscore"


@pytest.mark.unit
def test_check_range(service, sample_df):
    result = service.check_range(sample_df, min_val=0.0, max_val=100.0)
    assert result["below_min"] == 0
    assert result["above_max"] > 0 or result["in_range_ratio"] > 0


@pytest.mark.unit
def test_generate_report(service, sample_df):
    start = datetime(2024, 1, 1)
    end = datetime(2024, 1, 5)
    report = service.generate_report(
        entity_id="dev-001",
        metric_key="temperature",
        start_ts=start,
        end_ts=end,
        df=sample_df,
        expected_frequency="1h",
        min_val=0.0,
        max_val=100.0,
    )
    assert report["entity_id"] == "dev-001"
    assert report["metric_key"] == "temperature"
    assert "issues" in report
    assert report["completeness_ratio"] >= 0.0


@pytest.mark.unit
def test_fix_issues_interpolate(service, sample_df):
    config = {"remove_duplicates": True, "null_fill_method": "interpolate", "clip_outliers": True}
    fixed = service.fix_issues(sample_df, config)
    assert fixed["value"].isna().sum() < sample_df["value"].isna().sum()
    assert len(fixed) > 0


@pytest.mark.unit
def test_empty_dataframe(service):
    df = pd.DataFrame()
    assert service.check_completeness(df, "1h")["expected_points"] == 0
    assert service.check_nulls(df)["null_count"] == 0
    assert service.check_duplicates(df)["duplicate_count"] == 0
    assert service.check_outliers(df)["outlier_count"] == 0


@pytest.mark.unit
def test_full_check(service, sample_df):
    result = service.full_check(sample_df)
    assert "completeness" in result
    assert "nulls" in result
    assert "duplicates" in result
    assert "outliers" in result
    assert "frequency" in result
