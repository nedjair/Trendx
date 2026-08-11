from __future__ import annotations

import os
import sys
import threading
import time
from datetime import UTC, datetime, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

from loguru import logger
from trendx.config import settings

EXECUTOR_PORT = int(os.environ.get("SERVER_PORT", settings.trendx_python_executor_port))

logger.remove()
logger.add(
    sys.stderr,
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | <level>{level: <8}</level> | trendx-worker | <level>{message}</level>",
    colorize=True,
)


class _HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:  # noqa: N802
        try:
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(
                b'{"status":"ok","service":"trendx-worker","port":'
                + str(EXECUTOR_PORT).encode()
                + b"}"
            )
        except Exception:
            pass

    def log_message(self, format: str, *args: Any, **kwargs: Any) -> None:
        return


def _start_executor_http_server(port: int) -> None:
    addr = ("0.0.0.0", port)
    retries = 0
    while retries < 10:
        try:
            srv = ThreadingHTTPServer(addr, _HealthHandler)
            logger.info(f"trendx-worker executor HTTP listen on :{port}")
            srv.serve_forever()
            return
        except OSError as exc:
            retries += 1
            logger.warning(f"executor HTTP bind error port {port} (retry {retries}/10): {exc}")
            time.sleep(2)
    logger.error(f"Unable to bind executor HTTP server on port {port}")


_health_server_thread = threading.Thread(
    target=_start_executor_http_server,
    args=(EXECUTOR_PORT,),
    name="trendx-worker-health-server",
    daemon=True,
)
_health_server_thread.start()


def task_ping(payload: dict[str, Any] | None = None) -> dict[str, Any]:
    return {
        "status": "pong",
        "ts": time.time(),
        "service": "trendx-worker",
        "executor_engine": int(os.environ.get("EXECUTOR_SCRIPT_ENGINE", "6")),
        "throttling_capacity": int(os.environ.get("THROTTLING_QUEUE_CAPACITY", "10")),
    }


def task_noop() -> bool:
    return True


def task_forecast_dryrun(device_id: str, metric_name: str, horizon: int = 24) -> dict[str, Any]:
    logger.info(
        f"Prophet dryrun requested device={device_id} metric={metric_name} horizon={horizon}"
    )
    return {
        "ok": True,
        "placeholder": True,
        "note": "Phase 2 infrastructure only. Model training arrives in Phase 3.",
        "device_id": device_id,
        "metric_name": metric_name,
        "horizon": horizon,
    }


# ————————————————————————————————————————————————
# Maintenance planifiée : partitions mensuelles + agrégats (UPSERT incrémental)
# ————————————————————————————————————————————————
try:
    from apscheduler.schedulers.background import BackgroundScheduler
    from apscheduler.triggers.cron import CronTrigger
    from apscheduler.triggers.interval import IntervalTrigger

    HAS_APSCHEDULER = True
except ImportError:  # pragma: no cover - dépendance ajoutée au Dockerfile.worker
    HAS_APSCHEDULER = False
    logger.error("APScheduler non installé — maintenance partitions/agrégats DÉSACTIVÉE")

if HAS_APSCHEDULER:
    from sqlalchemy import text
    from trendx.database.connection import get_analytics_engine
    from trendx.services.scheduler_guards import (
        get_watermark,
        verify_aggregate_refresh,
        verify_aggregate_rows,
    )

    _agg_unchanged_cycles: dict[str, int] = {}
    _AGG_ALERT_THRESHOLD = 2

    def _call_db_function(sql: str, params: dict[str, Any]) -> Any:
        engine = get_analytics_engine()  # type: ignore[no-untyped-call]
        with engine.begin() as conn:
            return conn.execute(text(sql), params).scalar_one()

    def _job_partitions() -> None:
        try:
            result = _call_db_function(
                "SELECT trendx_analytics.ensure_partitions_forward(:months)", {"months": 3}
            )
            if result.get("missing", 0) > 0 and not result.get("created"):
                raise RuntimeError(
                    f"ensure_partitions_forward a échoué : missing={result.get('missing')} sans création"
                )
            logger.info(f"[scheduler] ensure_partitions_forward(3) -> {result}")
        except Exception as exc:
            logger.error(f"[scheduler] ensure_partitions_forward(3) échec: {exc}")

    def _job_aggregate(agg: str) -> None:
        try:
            before = get_watermark(agg)
            result = _call_db_function(
                "SELECT trendx_analytics.refresh_aggregate(:agg)", {"agg": agg}
            )
            verify_aggregate_refresh(agg, before)
            verify_aggregate_rows(
                agg,
                result.get("rows_upserted", 0),
                result.get("window_start", ""),
            )
            _agg_unchanged_cycles[agg] = 0
            logger.info(f"[scheduler] refresh_aggregate('{agg}') -> {result}")
        except Exception as exc:
            _agg_unchanged_cycles[agg] = _agg_unchanged_cycles.get(agg, 0) + 1
            logger.error(
                "[scheduler] refresh_aggregate('{}') échec: {} (cycles inaltérés={}/{})",
                agg,
                exc,
                _agg_unchanged_cycles[agg],
                _AGG_ALERT_THRESHOLD,
            )
            if _agg_unchanged_cycles[agg] >= _AGG_ALERT_THRESHOLD:
                logger.critical(
                    "[scheduler] refresh_aggregate('{}') : AUCUNE progression "
                    "pendant {} cycles consécutifs — maintenance requise",
                    agg,
                    _agg_unchanged_cycles[agg],
                )

    def _start_scheduler() -> BackgroundScheduler:
        sched = BackgroundScheduler(timezone=settings.trendx_timezone)
        sched.add_job(
            _job_partitions,
            CronTrigger(day="1", hour=0, minute=15),
            id="partitions_monthly",
            max_instances=1,
            coalesce=True,
            misfire_grace_time=3600,
        )
        sched.add_job(
            _job_aggregate,
            IntervalTrigger(minutes=15),
            args=["hourly"],
            id="aggregate_hourly",
            max_instances=1,
            coalesce=True,
            misfire_grace_time=600,
        )
        sched.add_job(
            _job_aggregate,
            IntervalTrigger(hours=1),
            args=["daily"],
            id="aggregate_daily",
            max_instances=1,
            coalesce=True,
            misfire_grace_time=3600,
        )
        sched.add_job(
            _job_aggregate,
            CronTrigger(hour=2, minute=30),
            args=["weekly"],
            id="aggregate_weekly",
            max_instances=1,
            coalesce=True,
            misfire_grace_time=3600,
        )
        # Vérification d'amorçage : contrôle immédiat de la couverture partitions
        sched.add_job(
            _job_partitions,
            "date",
            run_date=datetime.now(UTC) + timedelta(seconds=10),
            id="partitions_boot_check",
            max_instances=1,
            coalesce=True,
        )
        sched.start()
        logger.info(
            "trendx-worker APScheduler démarré (jobs: partitions_monthly, aggregate_hourly,"
            " aggregate_daily, aggregate_weekly, partitions_boot_check)"
        )
        return sched


_scheduler = None
if HAS_APSCHEDULER:
    _scheduler = _start_scheduler()


logger.info(
    "trendx-worker initialized (APScheduler process, no broker). port={port}, engine={engine}",
    port=EXECUTOR_PORT,
    engine=os.environ.get("EXECUTOR_SCRIPT_ENGINE", "6"),
)

try:
    from trendx.services.ingestion import probe_disk_mounts

    probe_disk_mounts("worker")
except Exception as exc:
    logger.error("[disk] impossible de valider les montages : {}", exc)
    # Les erreurs tmpfs sont fatales au démarrage
    if isinstance(exc, RuntimeError) and "tmpfs" in str(exc):
        raise

try:
    while True:
        time.sleep(3600)
except KeyboardInterrupt:
    if _scheduler is not None:
        _scheduler.shutdown(wait=False)
    logger.info("trendx-worker stopped by signal")
