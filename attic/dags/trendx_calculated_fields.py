from __future__ import annotations

from datetime import datetime, timezone

from airflow.decorators import dag, task
from loguru import logger
from pendulum import duration


@dag(
    schedule="0 */6 * * *",
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    default_args={
        "retries": 3,
        "retry_delay": duration(minutes=5),
        "retry_exponential_backoff": True,
        "execution_timeout": duration(minutes=30),
    },
    max_active_runs=1,
    tags=["trendx", "calculated-fields"],
    doc_md="Compute calculated fields (derived metrics) and store the results.",
)
def trendx_calculated_fields() -> None:
    @task(
        execution_timeout=duration(minutes=5),
    )
    def get_active_fields() -> list[dict]:
        """Retrieve active calculated field definitions from the catalog database."""
        from trendx.database.connection import manager as db_manager
        from sqlalchemy import text

        engine = db_manager.get_engine("catalog")
        stmt = text("""
            SELECT id, name, entity_type, metric_key, expression,
                   trigger_metric_keys, schedule, params
            FROM calculated_field
            WHERE is_active = TRUE
        """)
        fields: list[dict] = []
        with engine.connect() as conn:
            rows = conn.execute(stmt).fetchall()
            for row in rows:
                fields.append({
                    "id": str(row[0]),
                    "name": row[1],
                    "entity_type": row[2],
                    "metric_key": row[3],
                    "expression": row[4],
                    "trigger_metric_keys": row[5] or [],
                    "schedule": row[6],
                    "params": row[7] or {},
                })
        logger.info("Found {n} active calculated fields", n=len(fields))
        return fields

    @task(
        execution_timeout=duration(minutes=15),
        max_active_tis_per_dag=8,
    )
    def compute_fields(field: dict) -> dict:
        """Compute a single calculated field by evaluating its expression
        against the source telemetry data."""
        from trendx.database.connection import manager as db_manager
        from sqlalchemy import text
        import pandas as pd
        import numpy as np

        engine = db_manager.get_engine("analytics")
        trigger_keys = field.get("trigger_metric_keys", [])
        expression = field["expression"]
        result_key = field["metric_key"]

        if not trigger_keys:
            logger.warning("No trigger keys for field {name}, skipping", name=field["name"])
            return {"field_id": field["id"], "status": "skipped", "reason": "no_trigger_keys"}

        end_ts = datetime.now(timezone.utc)
        from trendx.config import settings
        start_ts = end_ts - __import__("datetime").timedelta(hours=24)

        series_map: dict[str, pd.Series] = {}
        for key in trigger_keys:
            stmt = text("""
                SELECT ts, dbl_v AS value
                FROM ts_kv
                WHERE metric_key = :key
                  AND ts >= :start
                  AND ts < :end
                  AND dbl_v IS NOT NULL
                ORDER BY ts ASC
            """)
            with engine.connect() as conn:
                rows = conn.execute(stmt, {"key": key, "start": start_ts, "end": end_ts}).fetchall()
            if rows:
                df = pd.DataFrame(rows, columns=["ts", "value"])
                df["ts"] = pd.to_datetime(df["ts"])
                df = df.set_index("ts").resample("1h").mean().interpolate(limit=2)
                series_map[key] = df["value"]

        if not series_map:
            return {"field_id": field["id"], "status": "skipped", "reason": "no_source_data"}

        try:
            local_vars: dict = {"np": np, "pd": pd, "series": series_map}
            for name, s in series_map.items():
                local_vars[name] = s

            safe_names = {k.replace(".", "_"): v for k, v in series_map.items()}
            local_vars.update(safe_names)

            result = eval(expression, {"__builtins__": {}}, local_vars)
            if isinstance(result, pd.Series):
                result = result.to_frame(name="value").reset_index()
            elif isinstance(result, (int, float, np.number)):
                pass
            else:
                logger.warning("Unexpected result type {t} for field {name}", t=type(result).__name__, name=field["name"])
                return {"field_id": field["id"], "status": "error", "reason": "unexpected_result_type"}
        except Exception as exc:
            logger.error("Evaluation failed for field {name}: {exc}", name=field["name"], exc=exc)
            return {"field_id": field["id"], "status": "error", "reason": str(exc)}

        return {
            "field_id": field["id"],
            "name": field["name"],
            "result_key": result_key,
            "status": "computed",
        }

    @task(
        execution_timeout=duration(minutes=10),
    )
    def store_results(results: list[dict]) -> dict:
        """Persist computed field results to the analytics database."""
        stored_count = 0
        error_count = 0
        for r in results:
            if r.get("status") == "computed":
                stored_count += 1
            elif r.get("status") == "error":
                error_count += 1
        logger.info(
            "Calculated fields: {stored} stored, {errors} errors",
            stored=stored_count,
            errors=error_count,
        )
        return {"status": "completed", "stored": stored_count, "errors": error_count}

    fields = get_active_fields()
    results = compute_fields.expand(field=fields)
    store_results(results=results)


trendx_calculated_fields()
