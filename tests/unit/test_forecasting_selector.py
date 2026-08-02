from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from trendx.forecasting.base import ForecastMetrics, Strategy
from trendx.forecasting.selector import ModelSelector, CompetitionResult, BacktestWindowResult


@pytest.fixture
def simple_data():
    n = 200
    ts = pd.date_range("2024-01-01", periods=n, freq="1h")
    y = 50 + np.sin(2 * np.pi * np.arange(n) / 24) * 5 + np.arange(n) * 0.02
    return pd.DataFrame({"ds": ts, "y": y})


@pytest.fixture
def selector():
    return ModelSelector(
        strategy=Strategy.PER_DEVICE,
        n_windows=3,
        test_size=0.2,
        min_stable_windows=2,
        marginal_gain_threshold=0.01,
    )


@pytest.mark.unit
def test_backtest_walk_forward(selector, simple_data):
    from trendx.forecasting.linear import LinearRegressionModel

    windows = selector.backtest(
        model_class=LinearRegressionModel,
        params={"feature_set": ["trend", "hour"]},
        data=simple_data,
        n_windows=3,
        test_size=0.2,
    )
    assert len(windows) > 0
    for w in windows:
        assert isinstance(w, BacktestWindowResult)
        assert w.metrics.mae >= 0.0


@pytest.mark.unit
def test_run_competition(selector, simple_data):
    candidates = [
        ("LinearRegression", {"feature_set": ["trend"]}),
        ("LinearRegression", {"feature_set": ["trend", "hour"]}),
    ]
    results = selector.run_competition(
        entity_id="dev-001",
        metric_key="temperature",
        data=simple_data,
        candidates=candidates,
        n_windows=2,
    )
    assert len(results) > 0
    for r in results:
        assert isinstance(r, CompetitionResult)
        assert r.aggregated_metrics.mae >= 0.0


@pytest.mark.unit
def test_select_champion(selector, simple_data):
    results = [
        CompetitionResult(
            algorithm="LR",
            params={"feature_set": ["trend"]},
            backtest_windows=[BacktestWindowResult(
                window_order=i, train_start=pd.Timestamp("2024-01-01"),
                train_end=pd.Timestamp("2024-01-02"),
                forecast_start=pd.Timestamp("2024-01-03"),
                forecast_end=pd.Timestamp("2024-01-04"),
                metrics=ForecastMetrics(mae=0.5 + i * 0.1, smape=5.0 + i),
            ) for i in range(3)],
            aggregated_metrics=ForecastMetrics(mae=0.6, smape=6.0),
            stability_score=0.9,
        ),
        CompetitionResult(
            algorithm="ARIMA",
            params={},
            backtest_windows=[BacktestWindowResult(
                window_order=i, train_start=pd.Timestamp("2024-01-01"),
                train_end=pd.Timestamp("2024-01-02"),
                forecast_start=pd.Timestamp("2024-01-03"),
                forecast_end=pd.Timestamp("2024-01-04"),
                metrics=ForecastMetrics(mae=1.0 + i * 0.2, smape=10.0 + i),
            ) for i in range(3)],
            aggregated_metrics=ForecastMetrics(mae=1.2, smape=12.0),
            stability_score=0.8,
        ),
    ]
    champion = selector.select_champion(results, metric="sMAPE")
    assert champion is not None
    assert champion.algorithm == "LR"


@pytest.mark.unit
def test_promote_stability_check(selector):
    single_window = [
        CompetitionResult(
            algorithm="LR",
            params={},
            backtest_windows=[BacktestWindowResult(
                window_order=0, train_start=pd.Timestamp("2024-01-01"),
                train_end=pd.Timestamp("2024-01-02"),
                forecast_start=pd.Timestamp("2024-01-03"),
                forecast_end=pd.Timestamp("2024-01-04"),
                metrics=ForecastMetrics(mae=0.5, smape=5.0),
            )],
            aggregated_metrics=ForecastMetrics(mae=0.5, smape=5.0),
        ),
    ]
    champion = selector.select_champion(single_window)
    assert champion is None


@pytest.mark.unit
def test_marginal_gain_threshold(selector):
    close_results = [
        CompetitionResult(
            algorithm="A",
            params={},
            backtest_windows=[BacktestWindowResult(
                window_order=i, train_start=pd.Timestamp("2024-01-01"),
                train_end=pd.Timestamp("2024-01-02"),
                forecast_start=pd.Timestamp("2024-01-03"),
                forecast_end=pd.Timestamp("2024-01-04"),
                metrics=ForecastMetrics(mae=1.0, smape=10.0),
            ) for i in range(3)],
            aggregated_metrics=ForecastMetrics(mae=1.0, smape=10.0),
            stability_score=0.9,
        ),
        CompetitionResult(
            algorithm="B",
            params={},
            backtest_windows=[BacktestWindowResult(
                window_order=i, train_start=pd.Timestamp("2024-01-01"),
                train_end=pd.Timestamp("2024-01-02"),
                forecast_start=pd.Timestamp("2024-01-03"),
                forecast_end=pd.Timestamp("2024-01-04"),
                metrics=ForecastMetrics(mae=1.02, smape=10.2),
            ) for i in range(3)],
            aggregated_metrics=ForecastMetrics(mae=1.02, smape=10.2),
            stability_score=0.9,
        ),
    ]
    selector.marginal_gain_threshold = 0.05
    champion = selector.select_champion(close_results)
    assert champion is None


@pytest.mark.unit
def test_empty_candidates(selector):
    champion = selector.select_champion([])
    assert champion is None


@pytest.mark.unit
def test_backtest_insufficient_data(selector):
    df = pd.DataFrame({"ds": pd.date_range("2024-01-01", periods=5, freq="1h"), "y": range(5)})
    from trendx.forecasting.base import ForecastModel

    windows = selector.backtest(ForecastModel.__class__, {}, df, n_windows=3, test_size=0.5)
    assert len(windows) == 0
