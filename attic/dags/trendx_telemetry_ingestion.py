from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

from airflow.decorators import dag, task
from loguru import logger
from pendulum import duration
from trendx.config import settings
from trendx.preprocessing.quality import DataQualityService
from trendx.services.ingestion import IngestionService


@dag(
    schedule="0 * * * *",
    start_date=datetime(2025, 1, 1, tzinfo=UTC),
    catchup=False,
    default_args={
        "retries": 3,
        "retry_delay": duration(minutes=5),
        "retry_exponential_backoff": True,
        "execution_timeout": duration(minutes=60),
    },
    max_active_runs=1,
    tags=["trendx", "ingestion"],
    doc_md="Ingest telemetry from ThingsBoard into TimescaleDB on an hourly schedule.",
)
def trendx_telemetry_ingestion() -> None:
    @task(
        execution_timeout=duration(minutes=5),
    )
    def discover_pending_devices() -> list[dict]:
        """Discover devices and metric pairs that need telemetry ingestion based on checkpoints."""
        ingestion = IngestionService()
        devices = ingestion._discover_devices()
        now = datetime.now(UTC)

        pending: list[dict] = []
        for device in devices:
            eid = device["entity_id"]
            for metric in device.get("metrics", []):
                key = metric["key"]
                cp = ingestion._get_checkpoint(eid, key)
                if cp is not None:
                    start_ts = cp
                else:
                    start_ts = now - timedelta(days=settings.training_lookback_days)
                if start_ts >= now:
                    continue
                pending.append(
                    {
                        "entity_id": eid,
                        "metric_key": key,
                        "start_ts": start_ts.isoformat(),
                        "end_ts": now.isoformat(),
                    }
                )

        logger.info(
            "Discovered {n} pending device/metric pairs for ingestion",
            n=len(pending),
        )
        return pending

    @task(
        execution_timeout=duration(minutes=30),
        retries=2,
        max_active_tis_per_dag=4,
    )
    def ingest_telemetry(pair: dict) -> dict:
        """Ingest telemetry for a single device/metric pair. Runs as a mapped task."""
        ingestion = IngestionService()

        async def _run():
            result = await ingestion.ingest_device_metric(
                entity_id=pair["entity_id"],
                metric_key=pair["metric_key"],
                start_ts=datetime.fromisoformat(pair["start_ts"]),
                end_ts=datetime.fromisoformat(pair["end_ts"]),
            )
            return result

        result = asyncio.run(_run())
        logger.info(
            "Ingested {stored} records for {eid}/{key}",
            stored=result.get("total_stored", 0),
            eid=pair["entity_id"][:12],
            key=pair["metric_key"],
        )
        return result

    @task(
        execution_timeout=duration(minutes=5),
    )
    def update_checkpoints(ingestion_results: list[dict]) -> list[dict]:
        """Verify and log checkpoint updates after ingestion."""
        updated: list[dict] = []
        for result in ingestion_results:
            if result.get("total_stored", 0) > 0:
                updated.append(
                    {
                        "entity_id": result["entity_id"],
                        "metric_key": result["metric_key"],
                        "watermark": result.get("end_ts"),
                        "records": result.get("total_stored"),
                    }
                )
        logger.info("Checkpoints updated for {n} pairs", n=len(updated))
        return updated

    @task(
        execution_timeout=duration(minutes=10),
    )
    def quality_check(checkpoints: list[dict]) -> dict:
        """Run a data quality check on ingested telemetry."""
        if not checkpoints:
            return {"status": "skipped", "reason": "no_data_ingested"}

        quality = DataQualityService()
        from sqlalchemy import text
        from trendx.database.connection import manager as db_manager

        engine = db_manager.get_engine("analytics")
        reports: list[dict] = []
        for cp in checkpoints[:20]:
            try:
                stmt = text("""
                    SELECT ts, dbl_v AS value
                    FROM ts_kv
                    WHERE entity_id = :eid
                      AND metric_key = :key
                      AND ts >= :start
                    ORDER BY ts ASC
                    LIMIT 500
                """)
                import pandas as pd

                with engine.connect() as conn:
                    rows = conn.execute(
                        stmt,
                        {
                            "eid": cp["entity_id"],
                            "key": cp["metric_key"],
                            "start": cp.get("watermark", datetime.now(UTC).isoformat()),
                        },
                    ).fetchall()
                if rows:
                    df = pd.DataFrame(rows, columns=["ts", "value"])
                    report = quality.generate_report(
                        entity_id=cp["entity_id"],
                        metric_key=cp["metric_key"],
                        start_ts=datetime.fromisoformat(
                            cp.get("watermark", datetime.now(UTC).isoformat())
                        ),
                        end_ts=datetime.now(UTC),
                        df=df,
                        expected_frequency=settings.forecast_frequency,
                    )
                    reports.append(report)
            except Exception as exc:
                logger.error(
                    "Quality check failed for {eid}/{key}: {exc}",
                    eid=cp["entity_id"][:12],
                    key=cp["metric_key"],
                    exc=exc,
                )

        return {
            "status": "completed",
            "reports_count": len(reports),
            "issues_found": sum(len(r.get("issues", [])) for r in reports),
        }

    pending = discover_pending_devices()
    ingested = ingest_telemetry.expand(pair=pending)
    checkpoints = update_checkpoints(ingestion_results=ingested)
    quality_check(checkpoints=checkpoints)


trendx_telemetry_ingestion()
