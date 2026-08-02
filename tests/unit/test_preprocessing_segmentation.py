from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from trendx.preprocessing.segmentation import Segmenter


@pytest.fixture
def ts_df():
    rng = pd.date_range("2024-01-01", periods=200, freq="1h")
    values = np.sin(np.arange(200) * 2 * np.pi / 24) * 10 + 50
    return pd.DataFrame({"ts": rng, "value": values})


@pytest.fixture
def segmenter():
    return Segmenter()


@pytest.mark.unit
def test_split_train_test_temporal(segmenter, ts_df):
    train, test = segmenter.split_train_test(ts_df, test_ratio=0.2, method="temporal")
    assert len(train) == 160
    assert len(test) == 40
    assert train["ts"].max() <= test["ts"].min()


@pytest.mark.unit
def test_walk_forward_windows(segmenter, ts_df):
    windows = segmenter.walk_forward_windows(ts_df, n_windows=3, test_size=20)
    assert len(windows) == 3
    for train, test in windows:
        assert len(train) > 0
        assert len(test) == 20
        assert train["ts"].max() <= test["ts"].min()


@pytest.mark.unit
def test_sliding_windows(segmenter, ts_df):
    windows, targets = segmenter.sliding_windows(
        ts_df, window_size=24, step_size=12, value_col="value"
    )
    assert windows.shape[0] > 0
    assert windows.shape[1] == 24
    if targets is not None:
        assert len(targets) <= windows.shape[0]


@pytest.mark.unit
def test_split_by_calendar(segmenter, ts_df):
    groups = segmenter.split_by_calendar(ts_df, period="D")
    assert len(groups) > 0
    for key, group_df in groups.items():
        assert not group_df.empty
        assert "ts" in group_df.columns


@pytest.mark.unit
def test_split_train_test_random(segmenter, ts_df):
    train, test = segmenter.split_train_test(ts_df, test_ratio=0.3, method="random")
    assert len(train) == 140
    assert len(test) == 60


@pytest.mark.unit
def test_split_train_test_empty(segmenter):
    df = pd.DataFrame()
    train, test = segmenter.split_train_test(df)
    assert train.empty
    assert test.empty


@pytest.mark.unit
def test_walk_forward_insufficient_data(segmenter):
    df = pd.DataFrame({"ts": pd.date_range("2024-01-01", periods=5, freq="1h"), "value": range(5)})
    windows = segmenter.walk_forward_windows(df, n_windows=5, test_size=10)
    assert len(windows) == 0


@pytest.mark.unit
def test_split_by_time(segmenter, ts_df):
    segments = segmenter.split_by_time(ts_df, segment_size="24h", overlap="0s")
    assert len(segments) > 0
    total_rows = sum(len(s) for s in segments)
    assert total_rows == len(ts_df)


@pytest.mark.unit
def test_split_by_time_invalid_overlap(segmenter, ts_df):
    with pytest.raises(ValueError, match="Overlap"):
        segmenter.split_by_time(ts_df, segment_size="12h", overlap="24h")
