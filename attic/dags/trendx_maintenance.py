from __future__ import annotations

from datetime import datetime, timezone

from airflow.decorators import dag, task
from loguru import logger
from pendulum import duration

from trendx.config import settings
from trendx.database.connection import manager as db_manager
from sqlalchemy import text


@dag(
    schedule="0 4 * * *",
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    default_args={
        "retries": 3,
        "retry_delay": duration(minutes=5),
        "retry_exponential_backoff": True,
        "execution_timeout": duration(minutes=60),
    },
    max_active_runs=1,
    tags=["trendx", "maintenance"],
    doc_md="Daily maintenance: purge old data, refresh continuous aggregates, vacuum, and cleanup.",
)
def trendx_maintenance() -> None:
    @task(
        execution_timeout=duration(minutes=15),
    )
    def purge_old_data() -> dict:
        """Remove telemetry and forecast data outside the configured retention period."""
        retention_days = max(settings.training_lookback_days * 2, 180)
        cutoff = datetime.now(timezone.utc)

        engine = db_manager.get_engine("analytics")
        total_purged = 0

        tables = [
            ("ts_kv", f"ts < NOW() - INTERVAL '{retention_days} days'"),
            ("forecast_series", f"ts < NOW() - INTERVAL '{retention_days} days'"),
            ("anomaly_score", f"ts < NOW() - INTERVAL '{retention_days} days'"),
            ("data_quality", f"period < NOW() - INTERVAL '{retention_days} days'"),
        ]

        for table, condition in tables:
            try:
                stmt = text(f"DELETE FROM {table} WHERE {condition}")
                with engine.begin() as conn:
                    result = conn.execute(stmt)
                    count = result.rowcount
                    if count > 0:
                        logger.info("Purged {n} rows from {table}", n=count, table=table)
                    total_purged += count
            except Exception as exc:
                logger.error("Failed to purge {table}: {exc}", table=table, exc=exc)

        logger.info("Data purge complete: {n} rows removed (retention={d} days)", n=total_purged, d=retention_days)
        return {"status": "completed", "rows_purged": total_purged, "retention_days": retention_days}

    @task(
        execution_timeout=duration(minutes=15),
    )
    def refresh_continuous_aggregates() -> dict:
        """Refresh TimescaleDB continuous aggregates."""
        engine = db_manager.get_engine("analytics")

        caggs = [
            "ts_kv_hourly_agg",
            "ts_kv_daily_agg",
            "forecast_hourly_agg",
        ]

        refreshed = 0
        for cagg in caggs:
            try:
                stmt = text(f"CALL refresh_continuous_aggregate('{cagg}', NOW() - INTERVAL '2 days', NOW())")
                with engine.begin() as conn:
                    conn.execute(stmt)
                logger.info("Refreshed continuous aggregate: {cagg}", cagg=cagg)
                refreshed += 1
            except Exception as exc:
                logger.warning("Failed to refresh CAGG {cagg}: {exc}", cagg=cagg, exc=exc)

        return {"status": "completed", "caggs_refreshed": refreshed, "total_caggs": len(caggs)}

    @task(
        execution_timeout=duration(minutes=5),
    )
    def cleanup_failed_tasks() -> dict:
        """Mark stale RUNNING tasks as FAILED and clean up old task logs."""
        from trendx.database.repositories import TrendzTaskRepository

        with db_manager.get_session("catalog") as session:
            repo = TrendzTaskRepository(session)
            stuck = repo.find_stuck(timeout_minutes=120)
            cleaned = 0
            for task_obj in stuck:
                try:
                    repo.complete(task_obj.id, error_message="Stale task — auto-cleaned by maintenance")
                    logger.info("Cleaned up stuck task {id} (status={status})", id=task_obj.id, status=task_obj.status)
                    cleaned += 1
                except Exception as exc:
                    logger.error("Failed to clean task {id}: {exc}", id=task_obj.id, exc=exc)

        logger.info("Cleaned up {n} stuck tasks", n=cleaned)
        return {"status": "completed", "stuck_tasks_cleaned": cleaned}

    @task(
        execution_timeout=duration(minutes=15),
    )
    def vacuum_analyze() -> dict:
        """Run PostgreSQL VACUUM ANALYZE on key tables to maintain query performance."""
        engine = db_manager.get_engine("analytics")

        tables = [
            "ts_kv",
            "ts_kv_latest",
            "forecast_series",
            "anomaly_score",
            "data_quality",
        ]

        vacuumed = 0
        for table in tables:
            try:
                stmt = text(f"VACUUM ANALYZE {table}")
                with engine.begin() as conn:
                    conn.execute(stmt)
                logger.info("VACUUM ANALYZE completed on {table}", table=table)
                vacuumed += 1
            except Exception as exc:
                logger.warning("VACUUM ANALYZE failed on {table}: {exc}", table=table, exc=exc)

        engine_catalog = db_manager.get_engine("catalog")
        catalog_tables = ["business_entity", "metric_definition", "prediction_model"]
        for table in catalog_tables:
            try:
                stmt = text(f"VACUUM ANALYZE {table}")
                with engine_catalog.begin() as conn:
                    conn.execute(stmt)
                logger.info("VACUUM ANALYZE completed on catalog.{table}", table=table)
                vacuumed += 1
            except Exception as exc:
                logger.warning("VACUUM ANALYZE failed on catalog.{table}: {exc}", table=table, exc=exc)

        return {"status": "completed", "tables_vacuumed": vacuumed}

    @task(
        execution_timeout=duration(minutes=10),
    )
    def cleanup_old_mlflow_runs() -> dict:
        """Archive or delete old MLflow runs to prevent tracking bloat."""
        try:
            from trendx.mlops.tracking import MLflowTracker

            tracker = MLflowTracker()
            cutoff_days = 90

            experiments = tracker._client.search_experiments()
            archived = 0
            for exp in experiments:
                if exp.name and exp.name.startswith("trendx_"):
                    runs = tracker._client.search_runs(
                        experiment_ids=[exp.experiment_id],
                        filter_string=f"end_time < {int((datetime.now(timezone.utc).timestamp() - cutoff_days * 86400) * 1000)}",
                    )
                    for run in runs:
                        try:
                            tracker._client.delete_run(run.info.run_id)
                            archived += 1
                        except Exception:
                            pass
            logger.info("Archived {n} old MLflow runs (> {d} days)", n=archived, d=cutoff_days)
            return {"status": "completed", "mlflow_runs_archived": archived}
        except Exception as exc:
            logger.warning("MLflow cleanup skipped: {exc}", exc=exc)
            return {"status": "skipped", "reason": str(exc)}

    @task(
        execution_timeout=duration(minutes=5),
    )
    def send_maintenance_report(
        purge: dict,
        cagg: dict,
        tasks: dict,
        vacuum: dict,
        mlflow: dict,
    ) -> dict:
        """Aggregate and log a maintenance summary report."""
        report = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "data_purge": purge,
            "cagg_refresh": cagg,
            "task_cleanup": tasks,
            "vacuum_analyze": vacuum,
            "mlflow_cleanup": mlflow,
        }

        logger.info("=== Maintenance Report ===")
        logger.info("Data purge: {n} rows removed", n=purge.get("rows_purged", 0))
        logger.info("CAGGs refreshed: {n}", n=cagg.get("caggs_refreshed", 0))
        logger.info("Stuck tasks cleaned: {n}", n=tasks.get("stuck_tasks_cleaned", 0))
        logger.info("Tables vacuumed: {n}", n=vacuum.get("tables_vacuumed", 0))
        logger.info("MLflow runs archived: {n}", n=mlflow.get("mlflow_runs_archived", 0))
        logger.info("=== End Maintenance Report ===")

        return {"status": "completed", "report": report}

    purge_result = purge_old_data()
    cagg_result = refresh_continuous_aggregates()
    tasks_result = cleanup_failed_tasks()
    vacuum_result = vacuum_analyze()
    mlflow_result = cleanup_old_mlflow_runs()

    send_maintenance_report(
        purge=purge_result,
        cagg=cagg_result,
        tasks=tasks_result,
        vacuum=vacuum_result,
        mlflow=mlflow_result,
    )


trendx_maintenance()
