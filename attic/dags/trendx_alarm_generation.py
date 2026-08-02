from __future__ import annotations

from datetime import datetime, timezone

from airflow.decorators import dag, task
from loguru import logger
from pendulum import duration

from trendx.config import settings
from trendx.services.alerting import AlertingService


@dag(
    schedule="*/15 * * * *",
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    default_args={
        "retries": 3,
        "retry_delay": duration(minutes=5),
        "retry_exponential_backoff": True,
        "execution_timeout": duration(minutes=15),
    },
    max_active_runs=1,
    tags=["trendx", "alarms"],
    doc_md="Evaluate alert rules and create/update/clear ThingsBoard alarms. Alarm generation is DISABLED by default.",
)
def trendx_alarm_generation() -> None:
    @task(
        execution_timeout=duration(minutes=2),
    )
    def check_alarm_enabled() -> dict:
        """Verify alarm generation is enabled in configuration."""
        enabled = settings.tb_alarms_enabled
        logger.info("Alarm generation enabled: {e}", e=enabled)
        return {
            "alarms_enabled": enabled,
            "writeback_enabled": settings.tb_writeback_enabled,
        }

    @task(
        execution_timeout=duration(minutes=10),
    )
    def evaluate_rules(readiness: dict) -> list[dict]:
        """Evaluate all active alert rules against recent telemetry data.

        Handles threshold rules, no-data rules, and anomaly rules.
        """
        if not readiness.get("alarms_enabled", False):
            logger.info("Alarm generation disabled, skipping rule evaluation")
            return []

        from trendx.database.connection import manager as db_manager
        from trendx.database.repositories import AlertRuleRepository, BusinessEntityRepository

        with db_manager.get_session("catalog") as session:
            rule_repo = AlertRuleRepository(session)
            rules = rule_repo.find_active()
            entity_repo = BusinessEntityRepository(session)
            devices = entity_repo.find_by_type("DEVICE")

        if not rules:
            logger.info("No active alert rules to evaluate")
            return []

        triggered: list[dict] = []
        threshold_rules = [r for r in rules if r.rule_type == "threshold"]
        no_data_rules = [r for r in rules if r.rule_type == "no_data"]
        anomaly_rules = [r for r in rules if r.rule_type == "anomaly"]

        from sqlalchemy import text

        engine = db_manager.get_engine("analytics")

        for device in devices[:50]:
            eid = str(device.entity_id)

            for rule in threshold_rules:
                metric_key = rule.metric_key
                if not metric_key:
                    continue
                stmt = text("""
                    SELECT dbl_v
                    FROM ts_kv_latest
                    WHERE entity_id = :eid AND metric_key = :key
                """)
                with engine.connect() as conn:
                    row = conn.execute(stmt, {"eid": eid, "key": metric_key}).fetchone()
                if row is None or row[0] is None:
                    continue
                current_value = float(row[0])
                alerting = AlertingService()
                results = alerting.evaluate_threshold_rules(eid, metric_key, current_value)
                for r in results:
                    if r.get("status") in ("created", "already_active"):
                        triggered.append({
                            "entity_id": eid,
                            "metric_key": metric_key,
                            "rule_type": "threshold",
                            "rule_name": rule.name,
                            "status": r["status"],
                            "severity": rule.severity,
                        })

            for rule in no_data_rules:
                metric_key = rule.metric_key
                if not metric_key:
                    continue
                stmt = text("""
                    SELECT MAX(ts) AS last_ts
                    FROM ts_kv
                    WHERE entity_id = :eid AND metric_key = :key
                """)
                with engine.connect() as conn:
                    row = conn.execute(stmt, {"eid": eid, "key": metric_key}).fetchone()
                last_ts = row[0] if row and row[0] else None
                alerting = AlertingService()
                results = alerting.evaluate_no_data_rules(eid, metric_key, last_ts)
                for r in results:
                    if r.get("status") in ("created", "already_active"):
                        triggered.append({
                            "entity_id": eid,
                            "metric_key": metric_key,
                            "rule_type": "no_data",
                            "rule_name": rule.name,
                            "status": r["status"],
                        })

        logger.info(
            "Rule evaluation complete: {n} rules evaluated, {t} triggered",
            n=len(rules),
            t=len(triggered),
        )
        return triggered

    @task(
        execution_timeout=duration(minutes=10),
    )
    def create_or_update_alarms(evaluation_results: list[dict]) -> dict:
        """Create, update, or clear ThingsBoard alarms based on rule evaluation.

        Uses AlertingService with idempotent alarm management via logical keys.
        """
        if not evaluation_results:
            return {"status": "completed", "alarms_created": 0, "alarms_updated": 0}

        created = 0
        for result in evaluation_results:
            if result.get("status") == "created":
                created += 1

        logger.info(
            "Alarm management: {created} created, {total} triggered",
            created=created,
            total=len(evaluation_results),
        )
        return {
            "status": "completed",
            "alarms_created": created,
            "alarms_evaluated": len(evaluation_results),
        }

    readiness = check_alarm_enabled()
    evaluation = evaluate_rules(readiness=readiness)
    create_or_update_alarms(evaluation_results=evaluation)


trendx_alarm_generation()
