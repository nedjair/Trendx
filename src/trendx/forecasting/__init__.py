from __future__ import annotations

from typing import Any

from trendx.forecasting.arima import ArimaModel
from trendx.forecasting.base import (
    ForecastMetrics,
    ForecastModel,
    ForecastResult,
    Strategy,
    compute_metrics,
)
from trendx.forecasting.fourier import FourierModel
from trendx.forecasting.linear import LinearRegressionModel, OLSRegressionModel
from trendx.forecasting.prophet import ProphetModel

MODEL_REGISTRY: dict[str, type[ForecastModel]] = {
    "Prophet": ProphetModel,
    "LinearRegression": LinearRegressionModel,
    "OLS": OLSRegressionModel,
    "ARIMA": ArimaModel,
    "Fourier": FourierModel,
}


def create_model(
    algorithm: str,
    params: dict[str, Any] | None = None,
) -> ForecastModel:
    """Factory function — instantiate a forecast model by name.

    Parameters
    ----------
    algorithm : str
        One of the keys in ``MODEL_REGISTRY``.
    params : dict or None
        Keyword arguments forwarded to the model constructor.

    Returns
    -------
    ForecastModel instance.

    Raises
    ------
    ValueError
        If ``algorithm`` is not registered.
    """
    cls = MODEL_REGISTRY.get(algorithm)
    if cls is None:
        known = ", ".join(MODEL_REGISTRY)
        msg = f"Unknown algorithm '{algorithm}'. Known: {known}"
        raise ValueError(msg)
    return cls(**(params or {}))


__all__ = [
    "ForecastModel",
    "ForecastResult",
    "ForecastMetrics",
    "Strategy",
    "compute_metrics",
    "ProphetModel",
    "LinearRegressionModel",
    "OLSRegressionModel",
    "ArimaModel",
    "FourierModel",
    "MODEL_REGISTRY",
    "create_model",
]
