from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from loguru import logger
from pendulum import duration
from trendx.thingsboard.discovery import TopologyDiscoveryService


@dag(
    schedule="*/15 * * * *",
    start_date=datetime(2025, 1, 1, tzinfo=UTC),
    catchup=False,
    default_args={
        "retries": 3,
        "retry_delay": duration(minutes=5),
        "retry_exponential_backoff": True,
        "execution_timeout": duration(minutes=30),
    },
    max_active_runs=1,
    tags=["trendx", "topology"],
    doc_md="Synchronise ThingsBoard topology (devices, assets, relations) into the catalog database.",
)
def trendx_topology_sync() -> None:
    @task(
        retries=1,
        execution_timeout=duration(minutes=20),
    )
    def full_sync() -> dict:
        """Run a full topology sync: discover all devices, assets, profiles, relations, attributes,
        and telemetry keys. Runs every 6 hours or on demand."""
        ctx = get_current_context()
        dag_run = ctx.get("dag_run")
        force = (dag_run and dag_run.conf and dag_run.conf.get("force_full", False)) or False
        if not force:
            from airflow.models.variable import Variable

            try:
                last_full = Variable.get("trendx_topology_last_full_sync", default_var=None)
                if last_full:
                    last_dt = datetime.fromisoformat(last_full)
                    if datetime.now(UTC) - last_dt < timedelta(hours=6):
                        logger.info("Full sync skipped — last full sync less than 6h ago")
                        return {"status": "skipped", "reason": "within_cooldown"}
            except Exception:
                pass

        async def _run():
            service = TopologyDiscoveryService()
            catalog = await service.full_sync()
            await service.validate_topology()
            await service.update_catalog()
            await service.close()
            return {
                "status": "completed",
                "devices": len(catalog.devices),
                "assets": len(catalog.assets),
                "profiles": len(catalog.device_profiles),
                "relations": len(catalog.relations),
                "sync_count": catalog.sync_count,
            }

        result = asyncio.run(_run())

        from airflow.models.variable import Variable

        Variable.set("trendx_topology_last_full_sync", datetime.now(UTC).isoformat())
        logger.info("Full sync result: {r}", r=result)
        return result

    @task(
        execution_timeout=duration(minutes=15),
    )
    def incremental_sync() -> dict:
        """Incremental topology sync — discovers new/removed entities and updates the catalog."""

        async def _run():
            service = TopologyDiscoveryService()
            catalog = await service.incremental_sync()
            await service.validate_topology()
            await service.update_catalog()
            await service.close()
            return {
                "status": "completed",
                "devices": len(catalog.devices),
                "assets": len(catalog.assets),
                "new_entities": catalog.sync_count,
            }

        result = asyncio.run(_run())
        logger.info("Incremental sync result: {r}", r=result)
        return result

    @task(
        execution_timeout=duration(minutes=5),
    )
    def validate_topology(full_result: dict | None = None, inc_result: dict | None = None) -> dict:
        """Validate catalog consistency — check names, missing profiles, duplicate relations."""
        if full_result and full_result.get("status") == "skipped" and not inc_result:
            return {"status": "skipped", "issues": []}

        async def _run():
            service = TopologyDiscoveryService()
            issues = await service.validate_topology()
            await service.close()
            return {"status": "completed", "issues": issues, "issue_count": len(issues)}

        result = asyncio.run(_run())
        if result["issue_count"] > 0:
            logger.warning("Topology validation found {n} issues", n=result["issue_count"])
            for issue in result["issues"]:
                logger.warning("  - {issue}", issue=issue)
        else:
            logger.info("Topology validation passed with no issues")
        return result

    @task(
        execution_timeout=duration(minutes=5),
    )
    def update_catalog(validation: dict) -> str:
        """Update catalog metadata — sync count, last sync timestamp."""
        from sqlalchemy import text
        from trendx.database.connection import manager as db_manager

        engine = db_manager.get_engine("catalog")
        with engine.begin() as conn:
            conn.execute(
                text("""
                    INSERT INTO sync_metadata (sync_key, sync_value)
                    VALUES ('last_topology_sync', :ts)
                    ON CONFLICT (sync_key) DO UPDATE SET sync_value = EXCLUDED.sync_value
                """),
                {"ts": datetime.now(UTC).isoformat()},
            )
        logger.info("Catalog sync metadata updated")
        return f"catalog_updated_at_{datetime.now(UTC).isoformat()}"

    full = full_sync()
    inc = incremental_sync()
    validation = validate_topology(full_result=full, inc_result=inc)
    update_catalog(validation=validation)


trendx_topology_sync()
