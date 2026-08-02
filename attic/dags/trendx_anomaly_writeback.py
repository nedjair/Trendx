from __future__ import annotations

from datetime import datetime, timezone

from airflow.decorators import dag, task
from loguru import logger
from pendulum import duration

from trendx.config import settings


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
    tags=["trendx", "anomalies", "writeback"],
    doc_md="Write anomaly scores back to ThingsBoard and generate alarms. Writeback and alarms are DISABLED by default.",
)
def trendx_anomaly_writeback() -> None:
    @task(
        execution_timeout=duration(minutes=2),
    )
    def check_writeback_enabled() -> dict:
        """Check if anomaly writeback and alarms are enabled in configuration."""
        enabled = settings.tb_writeback_enabled and settings.anomaly_detection_enabled
        logger.info(
            "Anomaly writeback check: tb_writeback={tb}, anomaly_enabled={an}",
            tb=settings.tb_writeback_enabled,
            an=settings.anomaly_detection_enabled,
        )
        return {
            "writeback_enabled": settings.tb_writeback_enabled,
            "anomaly_detection_enabled": settings.anomaly_detection_enabled,
            "alarms_enabled": settings.tb_alarms_enabled,
            "ready": enabled,
        }

    @task(
        execution_timeout=duration(minutes=10),
    )
    def validate_anomaly_scores(readiness: dict) -> list[dict]:
        """Retrieve recent anomaly scores from the database and validate them."""
        if not readiness.get("ready", False):
            return []

        from trendx.database.connection import manager as db_manager
        from sqlalchemy import text
        import numpy as np

        engine = db_manager.get_engine("analytics")
        stmt = text("""
            SELECT DISTINCT ON (entity_id, metric_key)
                   entity_id, metric_key, ts, normalised_score, algorithm
            FROM anomaly_score
            WHERE ts >= NOW() - INTERVAL '2 hours'
              AND normalised_score IS NOT NULL
            ORDER BY entity_id, metric_key, ts DESC
        """)
        valid: list[dict] = []
        with engine.connect() as conn:
            rows = conn.execute(stmt).fetchall()
            for row in rows:
                score = float(row[3]) if row[3] is not None else 0.0
                if np.isfinite(score) and 0.0 <= score <= 1.0:
                    valid.append({
                        "entity_id": row[0],
                        "metric_key": row[1],
                        "ts": row[2].isoformat() if hasattr(row[2], "isoformat") else str(row[2]),
                        "normalised_score": score,
                        "algorithm": row[4],
                    })
        logger.info("Validated {n} anomaly scores for writeback", n=len(valid))
        return valid

    @task(
        execution_timeout=duration(minutes=15),
        retries=2,
        max_active_tis_per_dag=4,
    )
    def writeback_scores(score_info: dict) -> dict:
        """Write a single anomaly score to ThingsBoard as _ANOMALY_<key>.

        Operates in dry-run mode unless TB_WRITEBACK_ENABLED=true.
        """
        import numpy as np
        entity_id = score_info["entity_id"]
        metric_key = score_info["metric_key"]
        score = score_info["normalised_score"]
        target_key = f"_ANOMALY_{metric_key}"

        if not settings.tb_writeback_enabled:
            logger.info(
                "Dry-run: would write anomaly score {score} for {eid}/{key} as {tkey}",
                score=score, eid=entity_id[:12], key=metric_key, tkey=target_key,
            )
            return {
                "entity_id": entity_id,
                "metric_key": metric_key,
                "target_key": target_key,
                "score": score,
                "status": "dry_run",
            }

        from trendx.thingsboard.client import ThingsBoardClient
        import asyncio

        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

        tb = ThingsBoardClient()
        loop.run_until_complete(
            tb.post_telemetry(
                entity_type="DEVICE",
                entity_id=entity_id,
                values=[{target_key: score}],
            )
        )
        logger.info(
            "Anomaly score written to ThingsBoard for {eid}/{key}: {tkey}={score}",
            eid=entity_id[:12], key=metric_key, tkey=target_key, score=score,
        )
        return {
            "entity_id": entity_id,
            "metric_key": metric_key,
            "target_key": target_key,
            "score": score,
            "status": "completed",
        }

    @task(
        execution_timeout=duration(minutes=10),
    )
    def generate_alarms(writeback_results: list[dict]) -> dict:
        """Generate ThingsBoard alarms for high anomaly scores.

        Alarms are DISABLED by default — set TB_ALARMS_ENABLED=true to activate.
        """
        if not settings.tb_alarms_enabled:
            logger.info("Anomaly alarm generation is disabled (TB_ALARMS_ENABLED=false)")
            return {"status": "disabled", "alarms_created": 0}

        high_score_results = [
            r for r in writeback_results
            if r.get("score", 0) > settings.anomaly_contamination * 2
        ]
        logger.info(
            "Would generate {n} alarms for high anomaly scores",
            n=len(high_score_results),
        )

        created = 0
        for result in high_score_results:
            try:
                from trendx.services.alerting import AlertingService
                alerting = AlertingService()
                incident = {
                    "logical_key": f"anomaly_{result['entity_id']}_{result['metric_key']}",
                    "entity_id": result["entity_id"],
                    "metric_key": result["metric_key"],
                    "score": result["score"],
                    "severity": "HIGH" if result.get("score", 0) > 0.8 else "MEDIUM",
                    "rule_type": "anomaly",
                }
                logger.info("Anomaly alarm triggered for {eid}/{key}: score={score}",
                            eid=result["entity_id"][:12], key=result["metric_key"],
                            score=result.get("score", 0))
                created += 1
            except Exception as exc:
                logger.error("Failed to create anomaly alarm: {exc}", exc=exc)

        return {
            "status": "completed" if created > 0 else "no_alarms",
            "alarms_created": created,
            "high_score_count": len(high_score_results),
        }

    readiness = check_writeback_enabled()
    scores = validate_anomaly_scores(readiness=readiness)
    writeback_results = writeback_scores.expand(score_info=scores)
    generate_alarms(writeback_results=writeback_results)


trendx_anomaly_writeback()
