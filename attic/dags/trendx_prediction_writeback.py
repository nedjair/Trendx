from __future__ import annotations

from datetime import datetime, timezone

from airflow.decorators import dag, task
from loguru import logger
from pendulum import duration

from trendx.config import settings
from trendx.services.inference import InferenceService


@dag(
    schedule="30 * * * *",
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    default_args={
        "retries": 3,
        "retry_delay": duration(minutes=5),
        "retry_exponential_backoff": True,
        "execution_timeout": duration(minutes=30),
    },
    max_active_runs=1,
    tags=["trendx", "forecasting", "writeback"],
    doc_md="Write forecast values (_EPD_<key>) back to ThingsBoard devices. Writeback is DISABLED by default (dry_run=True).",
)
def trendx_prediction_writeback() -> None:
    @task(
        execution_timeout=duration(minutes=2),
    )
    def check_writeback_enabled() -> dict:
        """Verify that writeback is enabled in settings. Returns readiness info."""
        readiness = InferenceService().check_writeback_readiness()
        if not readiness.get("ready", False):
            logger.warning(
                "Writeback NOT enabled (writeback={wb}, dry_run={dry}, models={m})",
                wb=readiness.get("writeback_enabled"),
                dry=readiness.get("dry_run"),
                m=readiness.get("champion_models"),
            )
        else:
            logger.info("Writeback is ready")
        return readiness

    @task(
        execution_timeout=duration(minutes=10),
    )
    def validate_forecasts(readiness: dict) -> list[dict]:
        """Retrieve stored forecasts for champion models and validate them before writeback.

        This task uses the database rather than XCom to avoid transferring large DataFrames.
        """
        if not readiness.get("ready", False):
            return []

        from trendx.database.connection import manager as db_manager
        from trendx.database.repositories import PredictionModelRepository
        from sqlalchemy import text
        import numpy as np

        engine = db_manager.get_engine("analytics")
        valid_forecasts: list[dict] = []

        with db_manager.get_session("catalog") as session:
            repo = PredictionModelRepository(session)
            champions = repo.find_by_status("champion")

        for champion in champions:
            if not champion.is_champion:
                continue
            eid = str(champion.entity_id)
            key = champion.metric_key
            stmt = text("""
                SELECT ts, value, lower_bound, upper_bound, horizon_step
                FROM forecast_series
                WHERE entity_id = :eid
                  AND metric_key = :key
                  AND ts >= NOW() - INTERVAL '2 hours'
                ORDER BY ts ASC
            """)
            with engine.connect() as conn:
                rows = conn.execute(stmt, {"eid": eid, "key": key}).fetchall()
            if not rows:
                logger.debug("No recent forecast for {eid}/{key}, skipping", eid=eid[:12], key=key)
                continue

            has_nan = any(
                row[1] is None or (isinstance(row[1], float) and np.isnan(row[1]))
                for row in rows
            )
            if has_nan:
                logger.warning("Forecast for {eid}/{key} contains NaN, skipping", eid=eid[:12], key=key)
                continue

            valid_forecasts.append({
                "entity_id": eid,
                "metric_key": key,
                "points_count": len(rows),
                "horizon": champion.horizon,
            })

        logger.info(
            "Validated {n} forecasts ready for writeback",
            n=len(valid_forecasts),
        )
        return valid_forecasts

    @task(
        execution_timeout=duration(minutes=15),
        retries=2,
        max_active_tis_per_dag=4,
    )
    def writeback_to_tb(forecast_info: dict) -> dict:
        """Write forecast data for one device/metric pair as _EPD_<key> in ThingsBoard.

        Writeback is dry_run=True by default — set TB_WRITEBACK_ENABLED=true to activate.
        """
        entity_id = forecast_info["entity_id"]
        metric_key = forecast_info["metric_key"]

        from trendx.database.connection import manager as db_manager
        from trendx.forecasting.base import ForecastResult
        from sqlalchemy import text
        import numpy as np
        import pandas as pd

        engine = db_manager.get_engine("analytics")
        stmt = text("""
            SELECT ts, value, lower_bound, upper_bound, horizon_step
            FROM forecast_series
            WHERE entity_id = :eid
              AND metric_key = :key
              AND ts >= NOW() - INTERVAL '2 hours'
            ORDER BY ts ASC
        """)
        with engine.connect() as conn:
            rows = conn.execute(stmt, {"eid": entity_id, "key": metric_key}).fetchall()

        if not rows:
            return {"entity_id": entity_id, "metric_key": metric_key, "status": "skipped"}

        forecast_result = ForecastResult(
            values=np.array([r[1] for r in rows], dtype=np.float64),
            lower_bound=np.array([r[2] if r[2] is not None else r[1] for r in rows], dtype=np.float64),
            upper_bound=np.array([r[3] if r[3] is not None else r[1] for r in rows], dtype=np.float64),
            timestamps=np.array([pd.Timestamp(r[0]).to_pydatetime() for r in rows], dtype=np.datetime64),
        )

        inference = InferenceService(dry_run=not settings.tb_writeback_enabled)
        result = inference.writeback_forecast(
            entity_id=entity_id,
            metric_key=metric_key,
            forecast_result=forecast_result,
        )
        logger.info(
            "Writeback for {eid}/{key}: {status} (dry_run={dry})",
            eid=entity_id[:12],
            key=metric_key,
            status=result.get("status", "unknown"),
            dry=not settings.tb_writeback_enabled,
        )
        return {
            "entity_id": entity_id,
            "metric_key": metric_key,
            "status": result.get("status"),
            "points": forecast_info.get("points_count"),
            "dry_run": not settings.tb_writeback_enabled,
        }

    @task(
        execution_timeout=duration(minutes=5),
    )
    def verify_writeback(writeback_results: list[dict]) -> dict:
        """Verify a sample of writeback operations and log summary."""
        succeeded = sum(1 for r in writeback_results if r.get("status") == "completed")
        dry_run = sum(1 for r in writeback_results if r.get("status") == "dry_run")
        failed = sum(1 for r in writeback_results if r.get("status") in ("failed", "skipped"))
        total_points = sum(r.get("points", 0) for r in writeback_results if r.get("points"))

        logger.info(
            "Writeback verification: {ok} completed, {dry} dry-run, {fail} failed, {pts} total points",
            ok=succeeded,
            dry=dry_run,
            fail=failed,
            pts=total_points,
        )
        if failed > 0:
            failed_keys = [r["metric_key"] for r in writeback_results if r.get("status") in ("failed", "skipped")]
            logger.warning("Failed/skipped writebacks: {keys}", keys=failed_keys)

        return {
            "status": "completed",
            "succeeded": succeeded,
            "dry_run": dry_run,
            "failed": failed,
            "total_points": total_points,
            "writeback_enabled": settings.tb_writeback_enabled,
        }

    readiness = check_writeback_enabled()
    forecasts = validate_forecasts(readiness=readiness)
    writeback_results = writeback_to_tb.expand(forecast_info=forecasts)
    verify_writeback(writeback_results=writeback_results)


trendx_prediction_writeback()
