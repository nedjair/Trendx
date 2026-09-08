from __future__ import annotations

import tempfile
import time
from pathlib import Path
from typing import Any

import numpy as np
import numpy.typing as npt
import pandas as pd
from loguru import logger
from trendx.config import settings
from trendx.forecasting.base import ForecastMetrics, ForecastModel, ForecastResult


def _fourier_series(
    ts: pd.DatetimeIndex,
    period: float,
    k: int,
) -> pd.DataFrame:
    """Generate Fourier series terms for a given period.

    Parameters
    ----------
    ts : pd.DatetimeIndex
        Timestamps.
    period : float
        Period in seconds (e.g. 86400 for daily).
    k : int
        Number of Fourier pairs (sin/cos) to generate.

    Returns
    -------
    pd.DataFrame with 2*k columns named ``sin_{period}_{j}`` and ``cos_{period}_{j}``.
    """
    t = np.asarray(ts, dtype="datetime64[ns]").astype(np.float64) / 1e9
    data: dict[str, npt.NDArray[np.float64]] = {}
    for j in range(1, k + 1):
        angle = 2.0 * np.pi * j * t / period
        data[f"sin_{int(period)}_{j}"] = np.sin(angle).astype(np.float64)
        data[f"cos_{int(period)}_{j}"] = np.cos(angle).astype(np.float64)
    return pd.DataFrame(data)


class FourierModel(ForecastModel):
    """Forecast model using Fourier series decomposition with linear regression.

    Decomposes seasonality into Fourier terms and fits a linear regression
    on trend + Fourier features.

    Parameters
    ----------
    k_daily : int
        Number of Fourier pairs for daily seasonality (default 3).
    k_weekly : int
        Number of Fourier pairs for weekly seasonality (default 3).
    k_yearly : int
        Number of Fourier pairs for yearly seasonality (default 5).
    include_trend : bool
        Whether to include a linear trend term (default True).
    confidence_level : float
        Confidence level for prediction intervals (default 0.80).
    """

    def __init__(
        self,
        k_daily: int = 3,
        k_weekly: int = 3,
        k_yearly: int = 5,
        include_trend: bool = True,  # noqa: FBT001,FBT002 -- API publique stable ; passage keyword-only casserait les appelants existants.
        confidence_level: float = 0.80,
    ) -> None:
        self.k_daily = k_daily
        self.k_weekly = k_weekly
        self.k_yearly = k_yearly
        self.include_trend = include_trend
        self.confidence_level = confidence_level
        self._model: Any = None
        self._feature_cols: list[str] = []
        self._train_df: pd.DataFrame | None = None

        if k_daily <= 0 and k_weekly <= 0 and k_yearly <= 0:
            msg = "At least one seasonality must have k > 0"
            raise ValueError(msg)

    def _build_features(self, df: pd.DataFrame) -> pd.DataFrame:
        ts = pd.DatetimeIndex(df["ds"])
        n = len(ts)
        features: dict[str, npt.NDArray[np.float64]] = {}

        if self.include_trend:
            features["trend"] = np.arange(n, dtype=np.float64)

        if self.k_daily > 0:
            daily_sec = 86400.0
            fourier_df = _fourier_series(ts, daily_sec, self.k_daily)
            for col in fourier_df.columns:
                features[col] = fourier_df[col].values

        if self.k_weekly > 0:
            weekly_sec = 604800.0
            fourier_df = _fourier_series(ts, weekly_sec, self.k_weekly)
            for col in fourier_df.columns:
                features[col] = fourier_df[col].values

        if self.k_yearly > 0:
            yearly_sec = 31557600.0
            fourier_df = _fourier_series(ts, yearly_sec, self.k_yearly)
            for col in fourier_df.columns:
                features[col] = fourier_df[col].values

        return pd.DataFrame(features)

    def fit(self, data: Any, *, context: dict[str, Any] | None = None) -> FourierModel:
        from sklearn.linear_model import LinearRegression

        logger.info("FourierModel.fit started")
        t_start = time.monotonic()

        df = self._prepare_dataframe(data)
        features = self._build_features(df)
        self._feature_cols = list(features.columns)

        x = features.values.astype(np.float64)
        y = df["y"].values.astype(np.float64)

        self._model = LinearRegression()
        self._model.fit(x, y)
        self._train_df = df

        elapsed = time.monotonic() - t_start
        logger.info("FourierModel.fit completed in {:.2f}s", elapsed)
        return self

    def predict(
        self, horizon: int = 24, *, context: dict[str, Any] | None = None
    ) -> ForecastResult:
        if self._model is None or self._train_df is None:
            msg = "Model not fitted yet. Call fit() first."
            raise RuntimeError(msg)

        logger.info("FourierModel.predict started, horizon={}", horizon)
        t_start = time.monotonic()

        last_ts = self._train_df["ds"].max()
        freq = pd.tseries.frequencies.to_offset(settings.forecast_frequency)
        future_ts = pd.date_range(start=last_ts + freq, periods=horizon, freq=freq)

        future_df = pd.DataFrame({"ds": future_ts})
        future_features = self._build_features(future_df)
        x_future = future_features.values.astype(np.float64)

        values = self._model.predict(x_future).astype(np.float64)

        residuals = self._train_df["y"].values - self._model.predict(
            self._build_features(self._train_df).values.astype(np.float64)
        )
        n = len(residuals)
        k = x_future.shape[1]
        std_resid = np.std(residuals, ddof=k) if n > k else np.std(residuals)

        from scipy import stats as scipy_stats

        alpha = 1.0 - self.confidence_level
        dof = max(1, n - k)
        t_val = scipy_stats.t.ppf(1 - alpha / 2, dof)
        delta = t_val * std_resid

        lower = values - delta
        upper = values + delta

        elapsed = time.monotonic() - t_start

        metrics = ForecastMetrics(inference_duration=elapsed)

        return ForecastResult(
            values=values,
            lower_bound=lower,
            upper_bound=upper,
            timestamps=future_ts.values.astype("datetime64[ns]"),
            model_name="Fourier",
            metrics=metrics,
        )

    def save(self, path: str) -> None:
        if self._model is None:
            msg = "No fitted model to save."
            raise RuntimeError(msg)

        import joblib

        path_obj = Path(path)
        path_obj.parent.mkdir(parents=True, exist_ok=True)

        params = {
            "k_daily": self.k_daily,
            "k_weekly": self.k_weekly,
            "k_yearly": self.k_yearly,
            "include_trend": self.include_trend,
            "confidence_level": self.confidence_level,
            "feature_cols": self._feature_cols,
        }
        with tempfile.NamedTemporaryFile(suffix=".pkl", delete=False) as tmp:
            joblib.dump({"model": self._model, "params": params}, tmp.name)
            tmp_path = tmp.name

        Path(tmp_path).rename(path_obj)
        logger.info("FourierModel saved to {}", path)

    @classmethod
    def load(cls, path: str) -> FourierModel:
        import joblib

        path_obj = Path(path)
        data = joblib.load(str(path_obj))
        params = data["params"]
        instance = cls(
            k_daily=params.get("k_daily", 3),
            k_weekly=params.get("k_weekly", 3),
            k_yearly=params.get("k_yearly", 5),
            include_trend=params.get("include_trend", True),
            confidence_level=params.get("confidence_level", 0.80),
        )
        instance._model = data["model"]
        instance._feature_cols = params.get("feature_cols", [])
        logger.info("FourierModel loaded from {}", path)
        return instance

    @staticmethod
    def _prepare_dataframe(data: Any) -> pd.DataFrame:
        if isinstance(data, pd.DataFrame):
            if "ds" in data.columns and "y" in data.columns:
                df = data[["ds", "y"]].copy()
            else:
                msg = "DataFrame must contain 'ds' and 'y' columns"
                raise ValueError(msg)
        elif isinstance(data, dict):
            df = pd.DataFrame(data)
            if "ds" not in df.columns or "y" not in df.columns:
                msg = "Dictionary must contain 'ds' and 'y' keys"
                raise ValueError(msg)
        else:
            msg = f"Unsupported data type: {type(data)}"
            raise TypeError(msg)

        df["ds"] = pd.to_datetime(df["ds"])
        df["y"] = pd.to_numeric(df["y"], errors="coerce")
        df = df.dropna(subset=["y"]).sort_values("ds").reset_index(drop=True)
        return df
