from __future__ import annotations

import numpy as np
import pytest

from trendx.forecasting.base import (
    ForecastMetrics,
    ForecastResult,
    Strategy,
    compute_metrics,
)


@pytest.mark.unit
def test_compute_metrics_all():
    y_true = np.array([10.0, 12.0, 14.0, 16.0, 18.0])
    y_pred = np.array([10.5, 11.8, 14.2, 15.5, 18.5])
    y_lower = np.array([9.0, 10.0, 12.0, 14.0, 16.0])
    y_upper = np.array([12.0, 13.0, 16.0, 17.0, 20.0])

    metrics = compute_metrics(y_true, y_pred, y_lower, y_upper)

    assert metrics.mae > 0.0
    assert metrics.rmse > 0.0
    assert metrics.smape > 0.0
    assert metrics.mape > 0.0
    assert metrics.coverage > 0.0
    assert metrics.interval_width > 0.0
    assert abs(metrics.bias) >= 0.0

    assert isinstance(metrics, ForecastMetrics)
    assert isinstance(metrics.to_dict(), dict)


@pytest.mark.unit
def test_compute_metrics_edge_cases():
    empty = compute_metrics(np.array([]), np.array([]))
    assert empty.mae == 0.0

    perfect = compute_metrics(np.array([1.0, 2.0]), np.array([1.0, 2.0]))
    assert perfect.mae == 0.0
    assert perfect.rmse == 0.0
    assert perfect.smape == 0.0

    single = compute_metrics(np.array([5.0]), np.array([7.0]))
    assert single.mae == 2.0


@pytest.mark.unit
def test_forecast_result_serialization():
    result = ForecastResult(
        values=np.array([1.0, 2.0, 3.0]),
        lower_bound=np.array([0.5, 1.5, 2.5]),
        upper_bound=np.array([1.5, 2.5, 3.5]),
        timestamps=np.array(["2024-01-01", "2024-01-02", "2024-01-03"], dtype="datetime64"),
        model_name="TestModel",
        metrics=ForecastMetrics(mae=0.5, rmse=0.6, smape=10.0),
    )
    d = result.to_dict()
    assert d["model_name"] == "TestModel"
    assert len(d["values"]) == 3
    assert d["metrics"]["mae"] == 0.5
    assert d["timestamps"] is not None


@pytest.mark.unit
def test_strategy_enum_values():
    assert Strategy.PER_DEVICE.value == 1
    assert Strategy.PER_PROFILE.value == 2
    assert Strategy.GLOBAL.value == 3
    assert Strategy.AUTO.value == 4


@pytest.mark.unit
def test_forecast_metrics_from_dict():
    data = {"mae": 0.5, "rmse": 0.7, "smape": 12.0, "unknown_field": 99.0}
    metrics = ForecastMetrics.from_dict(data)
    assert metrics.mae == 0.5
    assert metrics.rmse == 0.7
    assert not hasattr(metrics, "unknown_field")


@pytest.mark.unit
def test_compute_metrics_without_intervals():
    y_true = np.array([10.0, 12.0, 14.0])
    y_pred = np.array([10.5, 11.5, 14.5])
    metrics = compute_metrics(y_true, y_pred)
    assert metrics.coverage == 0.0
    assert metrics.interval_width == 0.0


@pytest.mark.unit
def test_compute_metrics_mape_near_zero():
    y_true = np.array([1e-15, 2e-15, 3e-15])
    y_pred = np.array([1e-15, 2e-15, 3e-15])
    metrics = compute_metrics(y_true, y_pred)
    assert np.isfinite(metrics.mape)
