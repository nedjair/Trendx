from __future__ import annotations

import json
import pickle
import tempfile
import time
from pathlib import Path
from typing import Any, Optional

import numpy as np
import numpy.typing as npt
import pandas as pd
from loguru import logger

from trendx.config import settings
from trendx.forecasting.base import ForecastMetrics, ForecastModel, ForecastResult


class ArimaModel(ForecastModel):
    """ARIMA / SARIMA forecast model.

    Uses ``pmdarima.auto_arima`` for automatic order selection unless
    ``order`` and ``seasonal_order`` are provided explicitly.

    Parameters
    ----------
    order : tuple of int or None
        (p, d, q) for non-seasonal ARIMA. If None, auto-detected.
    seasonal_order : tuple of int or None
        (P, D, Q, s) for seasonal part. If None and seasonal=True, auto-detected.
    seasonal : bool
        Whether to consider seasonality (default True).
    max_p : int
        Maximum p for auto-arima (default 5).
    max_d : int
        Maximum d for auto-arima (default 2).
    max_q : int
        Maximum q for auto-arima (default 5).
    max_P : int
        Maximum P for auto-arima (default 2).
    max_D : int
        Maximum D for auto-arima (default 1).
    max_Q : int
        Maximum Q for auto-arima (default 2).
    m : int or None
        Seasonal period. If None, inferred from frequency.
    confidence_level : float
        Confidence level for intervals (default 0.80).
    information_criterion : str
        Model selection criterion for auto-arima ('aic', 'bic', 'hqic').
    """

    def __init__(
        self,
        order: Optional[tuple[int, int, int]] = None,
        seasonal_order: Optional[tuple[int, int, int, int]] = None,
        seasonal: bool = True,
        max_p: int = 5,
        max_d: int = 2,
        max_q: int = 5,
        max_P: int = 2,
        max_D: int = 1,
        max_Q: int = 2,
        m: Optional[int] = None,
        confidence_level: float = 0.80,
        information_criterion: str = "aic",
    ) -> None:
        self.order = order
        self.seasonal_order = seasonal_order
        self.seasonal = seasonal
        self.max_p = max_p
        self.max_d = max_d
        self.max_q = max_q
        self.max_P = max_P
        self.max_D = max_D
        self.max_Q = max_Q
        self.m = m
        self.confidence_level = confidence_level
        self.information_criterion = information_criterion
        self._model: Any = None
        self._last_y: Optional[npt.NDArray[np.float64]] = None
        self._train_df: Optional[pd.DataFrame] = None

    def fit(
        self, data: Any, *, context: Optional[dict[str, Any]] = None
    ) -> "ArimaModel":
        from pmdarima import auto_arima

        logger.info("ArimaModel.fit started")
        t_start = time.monotonic()

        df = self._prepare_dataframe(data)
        y = df["y"].values.astype(np.float64)

        seasonal_period = self._infer_seasonal_period(df)

        if self.order is not None:
            if self.seasonal_order is not None:
                from pmdarima.arima import ARIMA

                self._model = ARIMA(
                    order=self.order,
                    seasonal_order=self.seasonal_order,
                    suppress_warnings=True,
                )
                self._model.fit(y)
            else:
                from pmdarima.arima import ARIMA

                self._model = ARIMA(order=self.order, suppress_warnings=True)
                self._model.fit(y)
        else:
            self._model = auto_arima(
                y,
                seasonal=self.seasonal,
                m=seasonal_period,
                max_p=self.max_p,
                max_d=self.max_d,
                max_q=self.max_q,
                max_P=self.max_P,
                max_D=self.max_D,
                max_Q=self.max_Q,
                information_criterion=self.information_criterion,
                stepwise=True,
                trace=False,
                error_action="ignore",
                suppress_warnings=True,
                n_fits=50,
            )

        self._last_y = y
        self._train_df = df

        elapsed = time.monotonic() - t_start
        logger.info(
            "ArimaModel.fit completed in {:.2f}s, order={}",
            elapsed,
            self._model.order if self._model is not None else None,
        )
        return self

    def predict(
        self, horizon: int = 24, *, context: Optional[dict[str, Any]] = None
    ) -> ForecastResult:
        if self._model is None or self._train_df is None:
            msg = "Model not fitted yet. Call fit() first."
            raise RuntimeError(msg)

        logger.info("ArimaModel.predict started, horizon={}", horizon)
        t_start = time.monotonic()

        pred, conf_int = self._model.predict(
            n_periods=horizon,
            return_conf_int=True,
            alpha=1.0 - self.confidence_level,
        )

        values = np.asarray(pred, dtype=np.float64).ravel()
        if conf_int is not None:
            conf_int = np.asarray(conf_int, dtype=np.float64)
            lower = conf_int[:, 0]
            upper = conf_int[:, 1]
        else:
            lower = values.copy()
            upper = values.copy()

        last_ts = self._train_df["ds"].max()
        freq = pd.tseries.frequencies.to_offset(settings.forecast_frequency)
        future_ts = pd.date_range(start=last_ts + freq, periods=horizon, freq=freq)

        elapsed = time.monotonic() - t_start

        metrics = ForecastMetrics(inference_duration=elapsed)

        return ForecastResult(
            values=values,
            lower_bound=lower,
            upper_bound=upper,
            timestamps=future_ts.values.astype("datetime64[ns]"),
            model_name=f"ARIMA{self._model.order}" if hasattr(self._model, "order") else "ARIMA",
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
            "order": self.order,
            "seasonal_order": self.seasonal_order,
            "seasonal": self.seasonal,
            "max_p": self.max_p,
            "max_d": self.max_d,
            "max_q": self.max_q,
            "max_P": self.max_P,
            "max_D": self.max_D,
            "max_Q": self.max_Q,
            "m": self.m,
            "confidence_level": self.confidence_level,
            "information_criterion": self.information_criterion,
        }
        with tempfile.NamedTemporaryFile(suffix=".pkl", delete=False) as tmp:
            joblib.dump({"model": self._model, "params": params}, tmp.name)
            tmp_path = tmp.name

        Path(tmp_path).rename(path_obj)
        logger.info("ArimaModel saved to {}", path)

    @classmethod
    def load(cls, path: str) -> "ArimaModel":
        import joblib

        path_obj = Path(path)
        data = joblib.load(str(path_obj))
        params = data["params"]
        instance = cls(
            order=params.get("order"),
            seasonal_order=params.get("seasonal_order"),
            seasonal=params.get("seasonal", True),
            max_p=params.get("max_p", 5),
            max_d=params.get("max_d", 2),
            max_q=params.get("max_q", 5),
            max_P=params.get("max_P", 2),
            max_D=params.get("max_D", 1),
            max_Q=params.get("max_Q", 2),
            m=params.get("m"),
            confidence_level=params.get("confidence_level", 0.80),
            information_criterion=params.get("information_criterion", "aic"),
        )
        instance._model = data["model"]
        logger.info("ArimaModel loaded from {}", path)
        return instance

    def _infer_seasonal_period(self, df: pd.DataFrame) -> int:
        if self.m is not None:
            return self.m

        if len(df) < 2:
            return 1

        deltas = df["ds"].diff().dropna()
        if len(deltas) == 0:
            return 1

        median_delta = deltas.median()
        total_seconds = median_delta.total_seconds()

        if total_seconds <= 0:
            return 1

        if total_seconds < 3600:
            return 24
        elif total_seconds < 86400:
            return 24
        elif total_seconds < 604800:
            return 7
        else:
            return 12

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
