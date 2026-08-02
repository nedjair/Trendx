from __future__ import annotations

from datetime import datetime, timezone

from airflow.decorators import dag, task
from loguru import logger
from pendulum import duration

from trendx.services.inference import InferenceService


@dag(
    schedule="0 * * * *",
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    default_args={
        "retries": 3,
        "retry_delay": duration(minutes=5),
        "retry_exponential_backoff": True,
        "execution_timeout": duration(minutes=30),
    },
    max_active_runs=1,
    tags=["trendx", "forecasting", "inference"],
    doc_md="Generate hourly forecasts using champion models and store results in the forecast_series table.",
)
def trendx_prediction_generation() -> None:
    @task(
        execution_timeout=duration(minutes=5),
    )
    def get_champion_models() -> list[dict]:
        """Retrieve all active champion models from the model registry."""
        from trendx.database.connection import manager as db_manager
        from trendx.database.repositories import PredictionModelRepository

        with db_manager.get_session("catalog") as session:
            repo = PredictionModelRepository(session)
            champions = repo.find_by_status("champion")

        models: list[dict] = []
        for m in champions:
            if m.is_champion:
                models.append({
                    "entity_id": str(m.entity_id),
                    "metric_key": m.metric_key,
                    "algorithm": m.algorithm,
                    "horizon": m.horizon,
                    "frequency": m.frequency,
                    "model_id": str(m.id),
                })
        logger.info("Found {n} champion models for forecast generation", n=len(models))
        return models

    @task(
        execution_timeout=duration(minutes=15),
        retries=2,
        max_active_tis_per_dag=4,
    )
    def generate_forecasts(model_info: dict) -> dict:
        """Generate a forecast for a single champion model and persist results to the database."""
        entity_id = model_info["entity_id"]
        metric_key = model_info["metric_key"]
        horizon = model_info.get("horizon")

        inference = InferenceService()
        forecast = inference.generate_forecast(
            entity_id=entity_id,
            metric_key=metric_key,
            horizon=horizon,
        )
        if forecast is None:
            return {
                "entity_id": entity_id,
                "metric_key": metric_key,
                "success": False,
                "error": "Forecast generation returned None",
            }

        saved = inference.save_forecast_results(
            entity_id=entity_id,
            metric_key=metric_key,
            forecast_result=forecast,
        )
        logger.info(
            "Forecast generated and saved for {eid}/{key}: {steps} steps, {saved} points stored",
            eid=entity_id[:12],
            key=metric_key,
            steps=len(forecast.values),
            saved=saved,
        )
        return {
            "entity_id": entity_id,
            "metric_key": metric_key,
            "success": True,
            "steps": len(forecast.values),
            "saved": saved,
            "algorithm": model_info.get("algorithm"),
        }

    @task(
        execution_timeout=duration(minutes=5),
    )
    def store_forecasts(forecast_results: list[dict]) -> dict:
        """Log aggregate forecast generation statistics."""
        success_count = sum(1 for r in forecast_results if r.get("success"))
        total_points = sum(r.get("saved", 0) for r in forecast_results if r.get("success"))
        error_count = sum(1 for r in forecast_results if not r.get("success"))
        logger.info(
            "Forecast generation complete: {ok} succeeded ({pts} points), {err} failed",
            ok=success_count,
            pts=total_points,
            err=error_count,
        )
        return {
            "status": "completed",
            "success_count": success_count,
            "error_count": error_count,
            "total_points": total_points,
        }

    models = get_champion_models()
    results = generate_forecasts.expand(model_info=models)
    store_forecasts(forecast_results=results)


trendx_prediction_generation()
