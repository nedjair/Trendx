from __future__ import annotations

import json
import tempfile
import time
from pathlib import Path
from typing import Any, Literal, Optional

import numpy as np
import numpy.typing as npt
import pandas as pd
from loguru import logger

from trendx.config import settings
from trendx.forecasting.base import ForecastMetrics, ForecastModel, ForecastResult


def _engineer_features(
    df: pd.DataFrame,
    feature_set: Optional[list[str]] = None,
) -> pd.DataFrame:
    """Build a feature matrix from time-based features.

    Parameters
    ----------
    df : pd.DataFrame
        Must contain a ``ds`` column (datetime).
    feature_set : list of str or None
        Subset of features to include. If None, all are used.

    Returns
    -------
    pd.DataFrame with feature columns.
    """
    all_features = [
        "trend",
        "hour",
        "day",
        "dayofweek",
        "month",
        "dayofyear",
        "weekend",
        "quarter",
        "sin_hour",
        "cos_hour",
        "sin_dayofweek",
        "cos_dayofweek",
        "sin_month",
        "cos_month",
    ]

    if feature_set is not None:
        selected = [f for f in all_features if f in feature_set]
    else:
        selected = all_features

    ts = pd.to_datetime(df["ds"])
    n = len(ts)
    features: dict[str, npt.NDArray[np.float64]] = {}

    if "trend" in selected:
        features["trend"] = np.arange(n, dtype=np.float64)

    if "hour" in selected:
        features["hour"] = ts.dt.hour.values.astype(np.float64)

    if "day" in selected:
        features["day"] = ts.dt.day.values.astype(np.float64)

    if "dayofweek" in selected:
        features["dayofweek"] = ts.dt.dayofweek.values.astype(np.float64)

    if "month" in selected:
        features["month"] = ts.dt.month.values.astype(np.float64)

    if "dayofyear" in selected:
        features["dayofyear"] = ts.dt.dayofyear.values.astype(np.float64)

    if "weekend" in selected:
        features["weekend"] = (ts.dt.dayofweek >= 5).values.astype(np.float64)

    if "quarter" in selected:
        features["quarter"] = ts.dt.quarter.values.astype(np.float64)

    if "sin_hour" in selected:
        features["sin_hour"] = np.sin(2 * np.pi * ts.dt.hour.values / 24).astype(np.float64)

    if "cos_hour" in selected:
        features["cos_hour"] = np.cos(2 * np.pi * ts.dt.hour.values / 24).astype(np.float64)

    if "sin_dayofweek" in selected:
        features["sin_dayofweek"] = np.sin(2 * np.pi * ts.dt.dayofweek.values / 7).astype(np.float64)

    if "cos_dayofweek" in selected:
        features["cos_dayofweek"] = np.cos(2 * np.pi * ts.dt.dayofweek.values / 7).astype(np.float64)

    if "sin_month" in selected:
        features["sin_month"] = np.sin(2 * np.pi * ts.dt.month.values / 12).astype(np.float64)

    if "cos_month" in selected:
        features["cos_month"] = np.cos(2 * np.pi * ts.dt.month.values / 12).astype(np.float64)

    return pd.DataFrame(features)


def _ols_prediction_intervals(
    X: npt.NDArray[np.float64],
    y: npt.NDArray[np.float64],
    X_pred: npt.NDArray[np.float64],
    alpha: float = 0.2,
) -> tuple[npt.NDArray[np.float64], npt.NDArray[np.float64]]:
    """Compute prediction intervals for OLS using statsmodels."""
    from scipy import stats as scipy_stats

    n, k = X.shape
    dof = n - k
    if dof <= 0:
        return np.full(X_pred.shape[0], np.nan), np.full(X_pred.shape[0], np.nan)

    XtX_inv = np.linalg.pinv(X.T @ X)
    y_pred = X_pred @ (np.linalg.pinv(X) @ y)

    residuals = y - X @ (np.linalg.pinv(X) @ y)
    sigma2 = np.sum(residuals**2) / dof

    t_val = scipy_stats.t.ppf(1 - alpha / 2, dof)
    se = np.sqrt(sigma2 * (1 + np.sum(X_pred * (X_pred @ XtX_inv), axis=1)))

    lower = y_pred - t_val * se
    upper = y_pred + t_val * se
    return lower.astype(np.float64), upper.astype(np.float64)


class LinearRegressionModel(ForecastModel):
    """Forecast model using scikit-learn LinearRegression with time-based features.

    Parameters
    ----------
    feature_set : list of str or None
        Time-based features to use. If None, all features are used.
    confidence_level : float
        Confidence level for prediction intervals (default 0.80).
    """

    def __init__(
        self,
        feature_set: Optional[list[str]] = None,
        confidence_level: float = 0.80,
    ) -> None:
        self.feature_set = feature_set
        self.confidence_level = confidence_level
        self._model: Any = None
        self._feature_cols: list[str] = []
        self._train_df: Optional[pd.DataFrame] = None

    def fit(
        self, data: Any, *, context: Optional[dict[str, Any]] = None
    ) -> "LinearRegressionModel":
        from sklearn.linear_model import LinearRegression

        logger.info("LinearRegressionModel.fit started")
        t_start = time.monotonic()

        df = self._prepare_dataframe(data)
        features = _engineer_features(df, self.feature_set)
        self._feature_cols = list(features.columns)

        X = features.values.astype(np.float64)
        y = df["y"].values.astype(np.float64)

        self._model = LinearRegression()
        self._model.fit(X, y)
        self._train_df = df

        elapsed = time.monotonic() - t_start
        logger.info("LinearRegressionModel.fit completed in {:.2f}s", elapsed)
        return self

    def predict(
        self, horizon: int = 24, *, context: Optional[dict[str, Any]] = None
    ) -> ForecastResult:
        if self._model is None or self._train_df is None:
            msg = "Model not fitted yet. Call fit() first."
            raise RuntimeError(msg)

        logger.info("LinearRegressionModel.predict started, horizon={}", horizon)
        t_start = time.monotonic()

        last_ts = self._train_df["ds"].max()
        freq = pd.tseries.frequencies.to_offset(settings.forecast_frequency)
        future_ts = pd.date_range(start=last_ts + freq, periods=horizon, freq=freq)

        future_df = pd.DataFrame({"ds": future_ts})
        future_features = _engineer_features(future_df, self.feature_set)
        X_future = future_features.values.astype(np.float64)

        values = self._model.predict(X_future).astype(np.float64)

        alpha = 1.0 - self.confidence_level
        n_features = X_future.shape[1]
        if n_features > 0:
            residuals = self._train_df["y"].values - self._model.predict(
                _engineer_features(self._train_df, self.feature_set).values.astype(np.float64)
            )
            std_resid = np.std(residuals, ddof=n_features) if len(residuals) > n_features else np.std(residuals)
            from scipy import stats as scipy_stats

            t_val = scipy_stats.t.ppf(1 - alpha / 2, max(1, len(residuals) - n_features))
            delta = t_val * std_resid
            lower = values - delta
            upper = values + delta
        else:
            lower = values
            upper = values

        elapsed = time.monotonic() - t_start

        metrics = ForecastMetrics(inference_duration=elapsed)

        return ForecastResult(
            values=values,
            lower_bound=lower,
            upper_bound=upper,
            timestamps=future_ts.values.astype("datetime64[ns]"),
            model_name="LinearRegression",
            metrics=metrics,
        )

    def save(self, path: str) -> None:
        if self._model is None:
            msg = "No fitted model to save."
            raise RuntimeError(msg)

        import joblib

        path_obj = Path(path)
        path_obj.parent.mkdir(parents=True, exist_ok=True)

        payload = {
            "feature_set": self.feature_set,
            "confidence_level": self.confidence_level,
            "feature_cols": self._feature_cols,
        }
        with tempfile.NamedTemporaryFile(suffix=".pkl", delete=False) as tmp:
            joblib.dump({"model": self._model, "params": payload}, tmp.name)
            tmp_path = tmp.name

        Path(tmp_path).rename(path_obj)
        logger.info("LinearRegressionModel saved to {}", path)

    @classmethod
    def load(cls, path: str) -> "LinearRegressionModel":
        import joblib

        path_obj = Path(path)
        data = joblib.load(str(path_obj))
        params = data["params"]
        instance = cls(
            feature_set=params.get("feature_set"),
            confidence_level=params.get("confidence_level", 0.80),
        )
        instance._model = data["model"]
        instance._feature_cols = params.get("feature_cols", [])
        logger.info("LinearRegressionModel loaded from {}", path)
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


class OLSRegressionModel(ForecastModel):
    """Forecast model using statsmodels OLS with time-based features.

    Parameters
    ----------
    feature_set : list of str or None
        Time-based features to use. If None, all features are used.
    confidence_level : float
        Confidence level for prediction intervals (default 0.80).
    """

    def __init__(
        self,
        feature_set: Optional[list[str]] = None,
        confidence_level: float = 0.80,
    ) -> None:
        self.feature_set = feature_set
        self.confidence_level = confidence_level
        self._model: Any = None
        self._feature_cols: list[str] = []
        self._train_df: Optional[pd.DataFrame] = None
        self._X_train: Optional[npt.NDArray[np.float64]] = None
        self._y_train: Optional[npt.NDArray[np.float64]] = None

    def fit(
        self, data: Any, *, context: Optional[dict[str, Any]] = None
    ) -> "OLSRegressionModel":
        import statsmodels.api as sm

        logger.info("OLSRegressionModel.fit started")
        t_start = time.monotonic()

        df = self._prepare_dataframe(data)
        features = _engineer_features(df, self.feature_set)
        self._feature_cols = list(features.columns)

        X = sm.add_constant(features.values.astype(np.float64))
        y = df["y"].values.astype(np.float64)

        self._model = sm.OLS(y, X).fit()
        self._train_df = df
        self._X_train = X
        self._y_train = y

        elapsed = time.monotonic() - t_start
        logger.info("OLSRegressionModel.fit completed in {:.2f}s", elapsed)
        return self

    def predict(
        self, horizon: int = 24, *, context: Optional[dict[str, Any]] = None
    ) -> ForecastResult:
        if self._model is None or self._train_df is None:
            msg = "Model not fitted yet. Call fit() first."
            raise RuntimeError(msg)

        logger.info("OLSRegressionModel.predict started, horizon={}", horizon)
        t_start = time.monotonic()

        last_ts = self._train_df["ds"].max()
        freq = pd.tseries.frequencies.to_offset(settings.forecast_frequency)
        future_ts = pd.date_range(start=last_ts + freq, periods=horizon, freq=freq)

        future_df = pd.DataFrame({"ds": future_ts})
        future_features = _engineer_features(future_df, self.feature_set)

        import statsmodels.api as sm

        X_future = sm.add_constant(future_features.values.astype(np.float64), has_constant="add")

        pred = self._model.get_prediction(X_future)
        values = pred.predicted_mean.astype(np.float64)

        alpha = 1.0 - self.confidence_level
        intervals = pred.conf_int(alpha=alpha)
        lower = intervals[:, 0].astype(np.float64)
        upper = intervals[:, 1].astype(np.float64)

        elapsed = time.monotonic() - t_start

        metrics = ForecastMetrics(inference_duration=elapsed)

        return ForecastResult(
            values=values,
            lower_bound=lower,
            upper_bound=upper,
            timestamps=future_ts.values.astype("datetime64[ns]"),
            model_name="OLS",
            metrics=metrics,
        )

    def save(self, path: str) -> None:
        if self._model is None:
            msg = "No fitted model to save."
            raise RuntimeError(msg)

        import joblib

        path_obj = Path(path)
        path_obj.parent.mkdir(parents=True, exist_ok=True)

        payload = {
            "feature_set": self.feature_set,
            "confidence_level": self.confidence_level,
            "feature_cols": self._feature_cols,
            "params": self._model.params.to_dict(),
            "cov_params": self._model.cov_params().to_dict() if hasattr(self._model, "cov_params") else None,
        }
        with tempfile.NamedTemporaryFile(suffix=".pkl", delete=False) as tmp:
            joblib.dump({"model": self._model, "params": payload}, tmp.name)
            tmp_path = tmp.name

        Path(tmp_path).rename(path_obj)
        logger.info("OLSRegressionModel saved to {}", path)

    @classmethod
    def load(cls, path: str) -> "OLSRegressionModel":
        import joblib

        path_obj = Path(path)
        data = joblib.load(str(path_obj))
        params = data["params"]
        instance = cls(
            feature_set=params.get("feature_set"),
            confidence_level=params.get("confidence_level", 0.80),
        )
        instance._model = data["model"]
        instance._feature_cols = params.get("feature_cols", [])
        logger.info("OLSRegressionModel loaded from {}", path)
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
