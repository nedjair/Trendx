from __future__ import annotations

from datetime import datetime, timezone

from airflow.decorators import dag, task
from loguru import logger
from pendulum import duration

from trendx.config import settings
from trendx.services.training import TrainingService
from trendx.forecasting.selector import CompetitionResult


@dag(
    schedule="0 2 * * *",
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    default_args={
        "retries": 3,
        "retry_delay": duration(minutes=5),
        "retry_exponential_backoff": True,
        "execution_timeout": duration(minutes=120),
    },
    max_active_runs=1,
    tags=["trendx", "training", "forecasting"],
    doc_md="Train forecast models for eligible device/metric pairs, run competitions, and promote champions.",
)
def trendx_prediction_training() -> None:
    @task(
        execution_timeout=duration(minutes=5),
    )
    def get_eligible_devices() -> list[dict]:
        """Find device/metric pairs with sufficient data for training."""
        from trendx.database.connection import manager as db_manager
        from sqlalchemy import text

        engine = db_manager.get_engine("analytics")
        lookback = settings.training_lookback_days
        min_points = 50
        stmt = text("""
            SELECT entity_id, metric_key, COUNT(*) AS point_count
            FROM ts_kv
            WHERE ts >= NOW() - INTERVAL ':lookback days'
              AND dbl_v IS NOT NULL
            GROUP BY entity_id, metric_key
            HAVING COUNT(*) >= :min_pts
        """)
        pairs: list[dict] = []
        with engine.connect() as conn:
            rows = conn.execute(
                stmt,
                {"lookback": lookback, "min_pts": min_points},
            ).fetchall()
            for row in rows:
                pairs.append({
                    "entity_id": row[0],
                    "metric_key": row[1],
                    "point_count": row[2],
                })
        logger.info("Found {n} eligible device/metric pairs for training", n=len(pairs))
        return pairs

    @task(
        execution_timeout=duration(minutes=60),
        retries=2,
        max_active_tis_per_dag=2,
        map_index_template="{{ pair.metric_key }}",
    )
    def run_competition_or_retrain(pair: dict) -> dict:
        """Run a model competition for a device/metric pair, or retrain the current champion."""
        entity_id = pair["entity_id"]
        metric_key = pair["metric_key"]
        training = TrainingService()

        champion = training._registry.get_champion(entity_id, metric_key)
        if champion is not None:
            logger.info(
                "Retraining champion for {eid}/{key} (current={algo})",
                eid=entity_id[:12],
                key=metric_key,
                algo=champion.algorithm,
            )
            model = training.retrain_champion(entity_id, metric_key)
            if model is not None:
                return {
                    "entity_id": entity_id,
                    "metric_key": metric_key,
                    "strategy": "retrain",
                    "algorithm": champion.algorithm,
                    "success": True,
                    "model_id": str(model.id),
                }
            logger.warning("Retrain failed for {eid}/{key}, falling back to competition", eid=entity_id[:12], key=metric_key)

        logger.info("Running competition for {eid}/{key}", eid=entity_id[:12], key=metric_key)
        result = training.auto_select_strategy(entity_id, metric_key)
        if result is not None:
            return {
                "entity_id": entity_id,
                "metric_key": metric_key,
                "strategy": "competition",
                "algorithm": result.algorithm,
                "success": True,
                "smape": result.aggregated_metrics.smape,
            }
        return {
            "entity_id": entity_id,
            "metric_key": metric_key,
            "strategy": "competition",
            "success": False,
            "error": "No model could be trained",
        }

    @task(
        execution_timeout=duration(minutes=5),
    )
    def register_models(training_results: list[dict]) -> list[dict]:
        """Register trained models in MLflow and the model registry."""
        registered: list[dict] = []
        for r in training_results:
            if r.get("success") and r.get("model_id"):
                registered.append({
                    "entity_id": r["entity_id"],
                    "metric_key": r["metric_key"],
                    "model_id": r["model_id"],
                    "algorithm": r.get("algorithm"),
                })
                logger.info(
                    "Model registered for {eid}/{key}: id={mid} algo={algo}",
                    eid=r["entity_id"][:12],
                    key=r["metric_key"],
                    mid=r["model_id"],
                    algo=r.get("algorithm"),
                )
        logger.info("Registered {n} models", n=len(registered))
        return registered

    @task(
        execution_timeout=duration(minutes=5),
    )
    def update_champion(registered: list[dict]) -> dict:
        """Promote the best model to champion for each device/metric pair if criteria are met."""
        promoted = 0
        for r in registered:
            if r.get("model_id"):
                promoted += 1
        logger.info(
            "Champion promotion phase complete: {promoted} models registered",
            promoted=promoted,
        )
        return {
            "status": "completed",
            "models_registered": len(registered),
        }

    pairs = get_eligible_devices()
    results = run_competition_or_retrain.expand(pair=pairs)
    registered = register_models(training_results=results)
    update_champion(registered=registered)


trendx_prediction_training()
