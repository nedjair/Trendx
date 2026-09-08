from __future__ import annotations

import time
from datetime import UTC, datetime, timedelta
from typing import Any

import numpy as np
import pandas as pd
from loguru import logger
from sqlalchemy import text
from trendx.config import settings
from trendx.database.connection import manager as db_manager
from trendx.database.models import PredictionModel
from trendx.forecasting import Strategy, create_model
from trendx.forecasting.base import ForecastMetrics, compute_metrics
from trendx.forecasting.selector import CompetitionResult, ModelSelector
from trendx.mlops.registry import ModelRegistry
from trendx.mlops.tracking import MLflowTracker
from trendx.preprocessing.normalizer import Normalizer
from trendx.preprocessing.quality import DataQualityService
from trendx.preprocessing.resampling import Resampler


class TrainingService:
    """Orchestrates model training, competition, and champion selection."""

    def __init__(
        self,
        mlflow_tracker: MLflowTracker | None = None,
        model_registry: ModelRegistry | None = None,
        data_quality: DataQualityService | None = None,
        resampler: Resampler | None = None,
        model_selector: ModelSelector | None = None,
    ) -> None:
        self._tracker = mlflow_tracker or MLflowTracker()
        self._registry = model_registry or ModelRegistry()
        self._quality = data_quality or DataQualityService()
        self._resampler = resampler or Resampler()
        self._selector = model_selector or ModelSelector(
            strategy=Strategy.PER_DEVICE,
            n_windows=5,
            test_size=0.2,
            min_stable_windows=3,
            marginal_gain_threshold=0.02,
        )

    def _fetch_training_data(
        self,
        entity_id: str,
        metric_key: str,
        lookback_days: int | None = None,
    ) -> pd.DataFrame:
        lookback = lookback_days or settings.training_lookback_days
        end = datetime.now(UTC)
        start = end - timedelta(days=lookback)
        engine = db_manager.get_engine("analytics")
        stmt = text(
            """
            SELECT ts, dbl_v AS value
            FROM ts_kv
            WHERE entity_id = :eid
              AND metric_key = :key
              AND ts >= :start
              AND ts < :end
              AND dbl_v IS NOT NULL
            ORDER BY ts ASC
            """
        )
        with engine.connect() as conn:
            rows = conn.execute(
                stmt,
                {
                    "eid": entity_id,
                    "key": metric_key,
                    "start": start,
                    "end": end,
                },
            ).fetchall()
        if not rows:
            logger.warning(
                "No training data for {}/{} in the last {} days",
                entity_id[:12],
                metric_key,
                lookback,
            )
            return pd.DataFrame()
        df = pd.DataFrame(rows, columns=["ts", "value"])
        df["ts"] = pd.to_datetime(df["ts"])
        return df

    def _prepare_data(
        self,
        df: pd.DataFrame,
        frequency: str = "1h",
        min_val: float | None = None,
        max_val: float | None = None,
    ) -> pd.DataFrame:
        if df.empty:
            return df
        quality_check = self._quality.full_check(
            df,
            expected_frequency=frequency,
            min_val=min_val,
            max_val=max_val,
        )
        logger.debug("Quality check: {}", quality_check)
        df = self._resampler.resample(df, frequency=frequency, method="mean")
        df = self._resampler.interpolate_missing(df, method="linear", limit=3)
        df = df.rename(columns={"ts": "ds", "value": "y"})
        df = df.dropna(subset=["y"])
        return df

    def train_model(
        self,
        entity_id: str,
        metric_key: str,
        algorithm: str = "Prophet",
        params: dict[str, Any] | None = None,
        lookback_days: int | None = None,
        frequency: str = "1h",
        horizon: int | None = None,
        min_val: float | None = None,
        max_val: float | None = None,
    ) -> PredictionModel | None:
        horizon = horizon or settings.forecast_horizon
        logger.info(
            "Training model: {}/{} algorithm={} horizon={}",
            entity_id[:12],
            metric_key,
            algorithm,
            horizon,
        )
        df = self._fetch_training_data(entity_id, metric_key, lookback_days)
        if df.empty:
            logger.error("No data for {}/{}", entity_id[:12], metric_key)
            return None
        df = self._prepare_data(df, frequency, min_val, max_val)
        if df.empty or len(df) < 10:
            logger.error(
                "Insufficient data ({}) for {}/{} after preparation",
                len(df),
                entity_id[:12],
                metric_key,
            )
            return None
        normalizer = Normalizer(method="auto")
        y_values = df["y"].values.reshape(-1, 1).astype(np.float64)
        normalizer.fit(y_values)
        y_scaled = normalizer.transform(y_values).flatten()
        df_scaled = df.copy()
        df_scaled["y"] = y_scaled
        n = len(df_scaled)
        split = int(n * 0.8)
        train_df = df_scaled.iloc[:split]
        test_df = df_scaled.iloc[split:]
        experiment_name = f"trendx_{entity_id[:12]}_{metric_key}"
        tags = {
            "entity_id": entity_id,
            "metric_key": metric_key,
            "algorithm": algorithm,
            "strategy": "PER_DEVICE",
            "frequency": frequency,
            "horizon": str(horizon),
        }
        self._tracker.start_run(
            experiment_name, run_name=f"{algorithm}_{int(time.time())}", tags=tags
        )
        try:
            model = create_model(algorithm, params)
            start_fit = time.monotonic()
            model.fit(train_df)
            fit_duration = time.monotonic() - start_fit
            start_pred = time.monotonic()
            forecast = model.predict(horizon=horizon)
            pred_duration = time.monotonic() - start_pred
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
            y_true = test_df["y"].values.astype(np.float64)
            y_true_orig = normalizer.inverse_transform(y_true.reshape(-1, 1)).flatten()
            y_pred_min = min(len(y_true_orig), len(forecast.values))
            if y_pred_min > 0:
                metrics = compute_metrics(
                    y_true_orig[:y_pred_min],
                    np.array(forecast.values[:y_pred_min]),
                    np.array(forecast.lower_bound[:y_pred_min]),
                    np.array(forecast.upper_bound[:y_pred_min]),
                )
            else:
                metrics = ForecastMetrics()
            metrics.train_duration = fit_duration
            metrics.inference_duration = pred_duration
            data_version = MLflowTracker.compute_data_hash(df)
            code_version = MLflowTracker.compute_git_hash()
            self._tracker.log_params(
                {
                    "algorithm": algorithm,
                    "frequency": frequency,
                    "horizon": horizon,
                    "lookback_days": lookback_days or settings.training_lookback_days,
                    "data_version": data_version,
                    "code_version": code_version,
                    "train_samples": len(train_df),
                    "test_samples": len(test_df),
                    "scaler_method": normalizer.method or "auto",
                }
            )
            if params:
                self._tracker.log_params(params)
            self._tracker.log_metrics(metrics.to_dict())
            self._tracker.log_scaler(normalizer.get_params())
            self._tracker.log_tags(tags)
            model_uri = self._tracker.log_model(model, artifact_path="model", model_name=None)
            self._tracker.end_run("FINISHED")
            # Contrat actuel de ModelRegistry.register() (schéma/ORM Trendz 1.15.0) :
            # business_entity_id / tb_telemetry_key / model_type / model_uri.
            # Les metadonnees (metrics, hyperparameters, scaler, frequency, horizon,
            # lookback_days, data_version, train_period_*, code_version, mlflow_run_id)
            # sont deja enregistrees via MLflowTracker (log_params/metrics/scaler) plus haut.
            champion = self._registry.register(
                business_entity_id=entity_id,
                tb_telemetry_key=metric_key,
                model_type=algorithm,
                model_uri=model_uri,
            )
            return champion
        except Exception as exc:
            logger.error("Training failed for {}/{}: {}", entity_id[:12], metric_key, exc)
            self._tracker.end_run("FAILED")
            return None

    def run_competition(
        self,
        entity_id: str,
        metric_key: str,
        candidates: list[tuple[str, dict[str, Any] | None]] | None = None,
    ) -> CompetitionResult | None:
        if candidates is None:
            candidates = [
                ("Prophet", None),
                ("LinearRegression", None),
                ("ARIMA", {"seasonal": True, "m": 24}),
            ]
        df = self._fetch_training_data(entity_id, metric_key)
        if df.empty:
            return None
        df = self._prepare_data(df)
        if df.empty or len(df) < 20:
            return None
        experiment_name = f"competition_{entity_id[:12]}_{metric_key}"
        tags = {
            "entity_id": entity_id,
            "metric_key": metric_key,
            "type": "competition",
        }
        self._tracker.start_run(
            experiment_name, run_name=f"competition_{int(time.time())}", tags=tags
        )
        try:
            results = self._selector.run_competition(
                entity_id=entity_id,
                metric_key=metric_key,
                data=df,
                candidates=candidates,
            )
            champion = self._selector.select_champion(results)
            if champion is not None:
                self._selector.promote_champion(entity_id, metric_key, champion)
            candidate_data = [{"algorithm": alg, "params": p} for alg, p in candidates]
            self._registry.store_selection_run(
                entity_id=entity_id,
                metric_key=metric_key,
                candidates=candidate_data,
                champion_id=None,
                selection_metric="sMAPE",
                champion_score=champion.aggregated_metrics.smape if champion else None,
                margin_gain=None,
                status="completed" if champion else "failed",
                mlflow_parent_run_id=self._tracker.active_run.info.run_id
                if self._tracker.active_run
                else None,
            )
            self._tracker.end_run("FINISHED")
            return champion
        except Exception as exc:
            logger.error("Competition failed for {}/{}: {}", entity_id[:12], metric_key, exc)
            self._tracker.end_run("FAILED")
            return None

    def auto_select_strategy(
        self,
        entity_id: str,
        metric_key: str,
    ) -> CompetitionResult | None:
        candidates = [
            ("Prophet", None),
            ("LinearRegression", None),
            ("ARIMA", {"seasonal": True, "m": 24}),
            ("Fourier", {"n_harmonics": 10}),
        ]
        return self.run_competition(entity_id, metric_key, candidates)

    def retrain_champion(
        self,
        entity_id: str,
        metric_key: str,
    ) -> PredictionModel | None:
        # TODO: is_champion column does not exist in real prediction_model table.
        # Champion selection must be managed via application logic.
        champion = self._registry.get_champion(entity_id, metric_key)
        if champion is None:
            logger.warning("No champion to retrain for {}/{}", entity_id[:12], metric_key)
            return None
        # TODO: algorithm, hyperparameters, lookback_days, frequency, horizon
        # do not exist in real prediction_model columns.
        # These values must be retrieved from MLflow or external registry.
        return self.train_model(
            entity_id=entity_id,
            metric_key=metric_key,
            algorithm="Prophet",
            params={},
            lookback_days=settings.training_lookback_days,
            frequency="1h",
            horizon=settings.forecast_horizon,
        )
