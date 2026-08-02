from __future__ import annotations

import time
import uuid
from datetime import datetime, timezone
from typing import Any, Optional

import numpy as np
import pandas as pd
from loguru import logger
from sqlalchemy import text

from trendx.config import settings
from trendx.database.connection import manager as db_manager
from trendx.database.models import PredictionModel
from trendx.database.repositories import (
    BusinessEntityRepository,
    MetricDefinitionRepository,
    PredictionModelRepository,
)
from trendx.forecasting import create_model
from trendx.forecasting.base import ForecastResult
try:
    from trendx.mlops.registry import ModelRegistry
    HAS_MODEL_REGISTRY = True
except RuntimeError:
    HAS_MODEL_REGISTRY = False
    ModelRegistry = None
from trendx.preprocessing.normalizer import Normalizer
from trendx.preprocessing.resampling import Resampler


class InferenceService:
    """Generates forecasts from trained champion models and manages writeback."""

    def __init__(
        self,
        model_registry: ModelRegistry | None = None,
        resampler: Resampler | None = None,
        dry_run: bool = True,
    ) -> None:
        self._registry = model_registry or (ModelRegistry() if HAS_MODEL_REGISTRY else None)
        self._resampler = resampler or Resampler()
        self._dry_run = dry_run
        self._writeback_enabled = settings.tb_writeback_enabled
        self._forecast_min = settings.forecast_min_value
        self._forecast_max = settings.forecast_max_value

    @property
    def dry_run(self) -> bool:
        return self._dry_run

    @dry_run.setter
    def dry_run(self, value: bool) -> None:
        self._dry_run = value
        logger.info("InferenceService dry_run set to {}", value)

    def _fetch_model(self, entity_id: str, metric_key: str) -> PredictionModel | None:
        return self._registry.get_champion(entity_id, metric_key)

    def _fetch_recent_data(
        self,
        entity_id: str,
        metric_key: str,
        n_points: int = 100,
    ) -> pd.DataFrame:
        engine = db_manager.get_engine("analytics")
        stmt = text(
            """
            SELECT ts, dbl_v AS value
            FROM ts_kv
            WHERE entity_id = :eid
              AND metric_key = :key
              AND dbl_v IS NOT NULL
            ORDER BY ts DESC
            LIMIT :limit
            """
        )
        with engine.connect() as conn:
            rows = conn.execute(
                stmt,
                {"eid": entity_id, "key": metric_key, "limit": n_points},
            ).fetchall()
        if not rows:
            return pd.DataFrame()
        df = pd.DataFrame(rows, columns=["ts", "value"])
        df["ts"] = pd.to_datetime(df["ts"])
        df = df.sort_values("ts").reset_index(drop=True)
        return df

    def generate_forecast(
        self,
        entity_id: str,
        metric_key: str,
        horizon: int | None = None,
    ) -> ForecastResult | None:
        model_record = self._fetch_model(entity_id, metric_key)
        if model_record is None:
            logger.warning("No champion model for {}/{}", entity_id[:12], metric_key)
            return None
        horizon = horizon or settings.forecast_horizon
        logger.info(
            "Generating forecast for {}/{} (type={}, horizon={})",
            entity_id[:12],
            metric_key,
            model_record.type,
            horizon,
        )
        df = self._fetch_recent_data(
            entity_id,
            metric_key,
            n_points=max(horizon * 3, 100),
        )
        if df.empty:
            logger.error("No recent data for {}/{}", entity_id[:12], metric_key)
            return None
        frequency = getattr(model_record, "frequency", None) or settings.forecast_frequency
        df = self._resampler.resample(df, frequency=frequency, method="mean")
        df = self._resampler.interpolate_missing(df, method="linear", limit=3)
        if df.empty or len(df) < 10:
            logger.error("Insufficient data ({}) for forecast after resampling", len(df))
            return None
        df = df.rename(columns={"ts": "ds", "value": "y"}).dropna(subset=["y"])
        scaler_params = getattr(model_record, "scaler", {}) or {}
        normalizer = Normalizer(method=scaler_params.get("method", "auto") if scaler_params else "auto")
        y_values = df["y"].values.reshape(-1, 1).astype(np.float64)
        if scaler_params and scaler_params.get("fitted"):
            try:
                normalizer = Normalizer.load_params("")
                for key, val in scaler_params.items():
                    if hasattr(normalizer, key):
                        setattr(normalizer, key, val)
            except Exception:
                normalizer.fit(y_values)
        else:
            normalizer.fit(y_values)
        y_scaled = normalizer.transform(y_values).flatten()
        df_scaled = df.copy()
        df_scaled["y"] = y_scaled
        try:
            model_algorithm = getattr(model_record, "algorithm", None) or model_record.type or "Prophet"
            hyperparameters = getattr(model_record, "hyperparameters", {}) or {}
            model = create_model(model_algorithm, hyperparameters)
            model.fit(df_scaled)
            forecast = model.predict(horizon=horizon)
            for step in range(horizon):
                if step < len(forecast.values):
                    forecast.values[step] = float(
                        normalizer.inverse_transform(np.array([[forecast.values[step]]]))[0, 0]
                    )
                if step < len(forecast.lower_bound):
                    forecast.lower_bound[step] = float(
                        normalizer.inverse_transform(np.array([[forecast.lower_bound[step]]]))[0, 0]
                    )
                if step < len(forecast.upper_bound):
                    forecast.upper_bound[step] = float(
                        normalizer.inverse_transform(np.array([[forecast.upper_bound[step]]]))[0, 0]
                    )
            forecast.values = np.clip(forecast.values, self._forecast_min, self._forecast_max)
            forecast.lower_bound = np.clip(forecast.lower_bound, self._forecast_min, self._forecast_max)
            forecast.upper_bound = np.clip(forecast.upper_bound, self._forecast_min, self._forecast_max)
            last_ts = df["ds"].max()
            forecast_timestamps = pd.date_range(
                start=last_ts + pd.Timedelta(hours=1),
                periods=horizon,
                freq=frequency,
            )
            forecast.timestamps = np.array(forecast_timestamps, dtype=np.datetime64)
            forecast.model_name = model_algorithm
            logger.info(
                "Forecast generated for {}/{}: {} steps from {}",
                entity_id[:12],
                metric_key,
                horizon,
                forecast_timestamps[0],
            )
            return forecast
        except Exception as exc:
            logger.error("Forecast generation failed for {}/{}: {}", entity_id[:12], metric_key, exc)
            return None

    def generate_bulk_forecasts(self) -> list[dict[str, Any]]:
        results: list[dict[str, Any]] = []
        session = next(db_manager.get_session("catalog"))
        entity_repo = BusinessEntityRepository(session)
        metric_repo = MetricDefinitionRepository(session)
        entities = entity_repo.list()
        metrics = metric_repo.list()
        session.close()
        for entity in entities:
            for metric in metrics:
                try:
                    forecast = self.generate_forecast(
                        entity_id=str(entity.id),
                        metric_key=metric.item_name,
                    )
                    if forecast is not None:
                        save_result = self.save_forecast_results(
                            str(entity.id),
                            metric.item_name,
                            forecast,
                        )
                        results.append({
                            "entity_id": str(entity.id)[:12],
                            "metric_key": metric.item_name,
                            "success": True,
                            "points": len(forecast.values),
                            "saved": save_result,
                        })
                    else:
                        results.append({
                            "entity_id": str(entity.id)[:12],
                            "metric_key": metric.item_name,
                            "success": False,
                            "error": "No forecast generated",
                        })
                except Exception as exc:
                    logger.error("Bulk forecast failed for {}/{}: {}", str(entity.id)[:12], metric.item_name, exc)
                    results.append({
                        "entity_id": str(entity.id)[:12],
                        "metric_key": metric.item_name,
                        "success": False,
                        "error": str(exc),
                    })
        return results

    def save_forecast_results(
        self,
        entity_id: str,
        metric_key: str,
        forecast_result: ForecastResult,
    ) -> int:
        # TODO: forecast_series table does not exist in Trendz 1.15.0 schema.
        # Forecast results are stored via MLflow tracking or segment_data.
        logger.info("Skipping forecast_series insert (table does not exist in Trendz 1.15.0)")
        return 0

    def writeback_forecast(
        self,
        entity_id: str,
        metric_key: str,
        forecast_result: ForecastResult,
    ) -> dict[str, Any]:
        # TODO: writeback_batch table does not exist in Trendz 1.15.0 schema.
        # Writeback to ThingsBoard is handled via TB API directly when enabled.
        self._validate_writeback(entity_id, metric_key, forecast_result)
        target_key = f"_EPD_{metric_key}"
        logger.info(
            "Writeback {} points for {}/{} as '{}' (dry_run={})",
            len(forecast_result.values),
            entity_id[:12],
            metric_key,
            target_key,
            self._dry_run,
        )
        if self._dry_run or not self._writeback_enabled:
            logger.info("Dry-run mode: would write {} points to device {} key '{}'", len(forecast_result.values), entity_id[:12], target_key)
            return {
                "dry_run": True,
                "entity_id": entity_id[:12],
                "metric_key": metric_key,
                "target_key": target_key,
                "points": len(forecast_result.values),
                "status": "dry_run",
            }
        # Real writeback goes through ThingsBoard API, not writeback_batch table.
        from trendx.thingsboard.client import ThingsBoardClient
        tb = ThingsBoardClient()
        import asyncio
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        points = []
        for i in range(len(forecast_result.values)):
            ts_val = forecast_result.timestamps[i] if forecast_result.timestamps is not None and i < len(forecast_result.timestamps) else datetime.now(timezone.utc)
            if isinstance(ts_val, np.datetime64):
                ts_dt = pd.Timestamp(ts_val).to_pydatetime()
            else:
                ts_dt = ts_val
            points.append({
                "ts": int(ts_dt.timestamp() * 1000),
                "value": float(forecast_result.values[i]),
            })
        loop.run_until_complete(
            tb.post_telemetry(
                entity_type="DEVICE",
                entity_id=entity_id,
                values=[{target_key: p["value"]} for p in points],
            )
        )
        logger.info(
            "Writeback completed for {}/{} -> {}: {} points",
            entity_id[:12],
            metric_key,
            target_key,
            len(points),
        )
        return {"status": "completed", "batch_id": None, "points_written": len(points)}

    def check_writeback_readiness(self) -> dict[str, Any]:
        checks = {
            "writeback_enabled": self._writeback_enabled,
            "dry_run": self._dry_run,
            "forecast_min": self._forecast_min,
            "forecast_max": self._forecast_max,
        }
        session = next(db_manager.get_session("catalog"))
        entity_repo = BusinessEntityRepository(session)
        entities = entity_repo.find_by_type("DEVICE")
        checks["entities_count"] = len(entities)
        models_count = len(PredictionModelRepository(session).find_by_status("champion"))
        checks["champion_models"] = models_count
        session.close()
        checks["ready"] = self._writeback_enabled and models_count > 0 and len(entities) > 0
        return checks

    def _validate_writeback(
        self,
        entity_id: str,
        metric_key: str,
        forecast_result: ForecastResult,
    ) -> None:
        errors: list[str] = []
        if not entity_id:
            errors.append("entity_id is empty")
        if not metric_key:
            errors.append("metric_key is empty")
        values = forecast_result.values
        if len(values) == 0:
            errors.append("forecast has no values")
        for i, v in enumerate(values):
            if np.isnan(v):
                errors.append(f"forecast value at index {i} is NaN")
            if np.isinf(v):
                errors.append(f"forecast value at index {i} is Inf")
        if errors:
            raise ValueError("Writeback validation failed: " + "; ".join(errors))

    @staticmethod
    def _lit(value: Any) -> str:
        if value is None:
            return "NULL"
        if isinstance(value, bool):
            return "TRUE" if value else "FALSE"
        if isinstance(value, (int, float)):
            if np.isnan(value) or np.isinf(value):
                return "NULL"
            return str(value)
        if isinstance(value, datetime):
            return f"'{value.isoformat()}'::timestamptz"
        escaped = str(value).replace("'", "''")
        return f"'{escaped}'"
