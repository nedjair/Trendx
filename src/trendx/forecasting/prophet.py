from __future__ import annotations

import time
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from loguru import logger
from trendx.config import settings
from trendx.forecasting.base import ForecastMetrics, ForecastModel, ForecastResult


class ProphetModel(ForecastModel):
    """Forecast model wrapping Facebook Prophet.

    Parameters
    ----------
    yearly_seasonality : bool or int
        If True, auto-detect; if int, number of Fourier terms for yearly seasonality.
    weekly_seasonality : bool or int
        If True, auto-detect; if int, number of Fourier terms for weekly seasonality.
    daily_seasonality : bool or int
        If True, auto-detect; if int, number of Fourier terms for daily seasonality.
    changepoint_prior_scale : float
        Flexibility of the trend (default 0.05).
    seasonality_prior_scale : float
        Strength of the seasonality (default 10.0).
    confidence_level : float
        Uncertainty interval width (default 0.80 → 80% intervals).
    cap : float or None
        Upper ceiling for logistic growth (from FORECAST_MAX_VALUE if None).
    floor : float or None
        Lower floor for logistic growth (from FORECAST_MIN_VALUE if None).
    custom_regressors : list of dict
        Each entry must have keys 'name' and 'prior_scale'.
    growth : str
        'linear' or 'logistic'.
    """

    def __init__(
        self,
        yearly_seasonality: bool | int = True,  # noqa: FBT002 -- API miroir de Prophet amont (bool|int|str acceptés) ; keyword-only casserait la compatibilité.
        weekly_seasonality: bool | int = True,  # noqa: FBT002 -- voir yearly_seasonality ci-dessus.
        daily_seasonality: bool | int = False,  # noqa: FBT002 -- voir yearly_seasonality ci-dessus.
        changepoint_prior_scale: float = 0.05,
        seasonality_prior_scale: float = 10.0,
        confidence_level: float = 0.80,
        cap: float | None = None,
        floor: float | None = None,
        custom_regressors: list[dict[str, Any]] | None = None,
        growth: str = "linear",
    ) -> None:
        self.yearly_seasonality = yearly_seasonality
        self.weekly_seasonality = weekly_seasonality
        self.daily_seasonality = daily_seasonality
        self.changepoint_prior_scale = changepoint_prior_scale
        self.seasonality_prior_scale = seasonality_prior_scale
        self.confidence_level = confidence_level
        self.cap = cap if cap is not None else settings.forecast_max_value
        self.floor = floor if floor is not None else settings.forecast_min_value
        self.custom_regressors = custom_regressors or []
        self.growth = growth
        self._model: Any = None
        # Shared training-history contract (linear/OLS/ARIMA/Fourier) required by
        # mlops.forecast_pyfunc.export_history: the training frame.
        self._train_df: pd.DataFrame | None = None

    def _build_model(self) -> Any:
        from prophet import Prophet

        if self.growth == "logistic":
            model = Prophet(
                growth="logistic",
                yearly_seasonality=self.yearly_seasonality,
                weekly_seasonality=self.weekly_seasonality,
                daily_seasonality=self.daily_seasonality,
                changepoint_prior_scale=self.changepoint_prior_scale,
                seasonality_prior_scale=self.seasonality_prior_scale,
                interval_width=self.confidence_level,
            )
        else:
            model = Prophet(
                yearly_seasonality=self.yearly_seasonality,
                weekly_seasonality=self.weekly_seasonality,
                daily_seasonality=self.daily_seasonality,
                changepoint_prior_scale=self.changepoint_prior_scale,
                seasonality_prior_scale=self.seasonality_prior_scale,
                interval_width=self.confidence_level,
            )

        for reg in self.custom_regressors:
            model.add_regressor(
                reg["name"],
                prior_scale=reg.get("prior_scale", 10.0),
                mode=reg.get("mode", "additive"),
            )

        return model

    def fit(self, data: Any, *, context: dict[str, Any] | None = None) -> ProphetModel:
        logger.info("ProphetModel.fit started")
        t_start = time.monotonic()

        df = self._prepare_dataframe(data)

        self._model = self._build_model()

        if self.growth == "logistic":
            df["cap"] = self.cap
            df["floor"] = self.floor

        self._model.fit(df)
        self._train_df = df

        elapsed = time.monotonic() - t_start
        logger.info("ProphetModel.fit completed in {:.2f}s", elapsed)
        return self

    def predict(
        self, horizon: int = 24, *, context: dict[str, Any] | None = None
    ) -> ForecastResult:
        if self._model is None:
            msg = "Model not fitted yet. Call fit() first."
            raise RuntimeError(msg)

        logger.info("ProphetModel.predict started, horizon={}", horizon)
        t_start = time.monotonic()

        future = self._model.make_future_dataframe(
            periods=horizon,
            freq=settings.forecast_frequency,
            include_history=False,
        )

        if self.growth == "logistic":
            future["cap"] = self.cap
            future["floor"] = self.floor

        for reg in self.custom_regressors:
            if "value" in reg:
                future[reg["name"]] = reg["value"]
            elif "default" in reg:
                future[reg["name"]] = reg["default"]

        forecast = self._model.predict(future)

        values = forecast["yhat"].values.astype(np.float64)
        lower = forecast.get("yhat_lower", values).values.astype(np.float64)
        upper = forecast.get("yhat_upper", values).values.astype(np.float64)

        timestamps = forecast["ds"].values.astype("datetime64[ns]")

        elapsed = time.monotonic() - t_start

        metrics = ForecastMetrics(inference_duration=elapsed)

        return ForecastResult(
            values=values,
            lower_bound=lower,
            upper_bound=upper,
            timestamps=timestamps,
            model_name="Prophet",
            metrics=metrics,
        )

    def save(self, path: str) -> None:
        if self._model is None:
            msg = "No fitted model to save."
            raise RuntimeError(msg)

        path_obj = Path(path)
        path_obj.parent.mkdir(parents=True, exist_ok=True)

        import json

        from prophet.serialize import model_to_json

        serialized = model_to_json(self._model)
        params = {
            "yearly_seasonality": self.yearly_seasonality,
            "weekly_seasonality": self.weekly_seasonality,
            "daily_seasonality": self.daily_seasonality,
            "changepoint_prior_scale": self.changepoint_prior_scale,
            "seasonality_prior_scale": self.seasonality_prior_scale,
            "confidence_level": self.confidence_level,
            "cap": self.cap,
            "floor": self.floor,
            "custom_regressors": self.custom_regressors,
            "growth": self.growth,
        }
        payload = {"params": params, "model_json": serialized}
        with open(path_obj, "w") as f:
            json.dump(payload, f)
        logger.info("ProphetModel saved to {}", path)

    @classmethod
    def load(cls, path: str) -> ProphetModel:
        import json

        from prophet.serialize import model_from_json

        path_obj = Path(path)
        with open(path_obj) as f:
            payload = json.load(f)

        params = payload.get("params", {})
        model_json = payload.get("model_json", "")

        instance = cls(
            yearly_seasonality=params.get("yearly_seasonality", True),
            weekly_seasonality=params.get("weekly_seasonality", True),
            daily_seasonality=params.get("daily_seasonality", False),
            changepoint_prior_scale=params.get("changepoint_prior_scale", 0.05),
            seasonality_prior_scale=params.get("seasonality_prior_scale", 10.0),
            confidence_level=params.get("confidence_level", 0.80),
            cap=params.get("cap"),
            floor=params.get("floor"),
            custom_regressors=params.get("custom_regressors", []),
            growth=params.get("growth", "linear"),
        )
        instance._model = model_from_json(model_json)
        logger.info("ProphetModel loaded from {}", path)
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

        # Prophet rejects tz-aware ds ("Column ds has timezone specified"):
        # normalize to tz-naive UTC while keeping the instant (same contract
        # as TrainingService._prepare_data). utc=True converts aware values
        # to UTC and treats naive values as already-UTC (no system-local
        # assumption); tz_localize(None) then drops tzinfo, preserving the
        # UTC wall time for Prophet.
        df["ds"] = pd.to_datetime(df["ds"], utc=True).dt.tz_localize(None)
        df["y"] = pd.to_numeric(df["y"], errors="coerce")
        df = df.dropna(subset=["y"]).sort_values("ds").reset_index(drop=True)

        return df
