from __future__ import annotations

import json
import os
import sys
import threading
import time
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, cast

from loguru import logger
from trendx.config import settings
from trendx.services.tasks import TaskService

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
        except Exception as exc:
            logger.debug("Health response write failed (ignored): {}", exc)

    def log_message(self, format: str, *args: Any, **kwargs: Any) -> None:
        return


def _start_executor_http_server(port: int) -> None:
    addr = ("0.0.0.0", port)  # nosec B104 -- health endpoint conteneur : doit écouter toutes interfaces pour le healthcheck Docker et le reverse-proxy ; aucun secret exposé (statut OK uniquement).
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
# Worker execution loop : claim -> dispatch -> run -> progress -> complete/fail
# Découplé du BackgroundScheduler de maintenance (voir bloc APScheduler ci-dessous).
# ————————————————————————————————————————————————
# Handlers P0 extraits vers trendx.scheduler.handlers (source de vérité unique,
# partageable avec le futur service scheduler dédié, sans import de ce module).
# Les noms préfixés sont ré-exportés ici : le contrat exécuteur (JOB_DISPATCH,
# attributs de module _run_*, __name__ assertés par les tests) reste inchangé.
from trendx.scheduler.handlers import (  # noqa: E402  -- imports métier en tête de bloc
    _run_ingestion,
    _run_topology_discovery,
    _run_topology_sync,
    _run_trendx_forecast,
    _run_trendx_train,
)

# Registre explicite job_type -> exécuteur. Aucun fallback silencieux : un
# job_type absent conduit à fail_task() (voir _process_one_task).
JOB_DISPATCH: dict[str, Callable[[dict[str, Any], str, str], Any]] = {
    "topology_discovery": _run_topology_discovery,
    "topology_sync": _run_topology_sync,
    "ingestion": _run_ingestion,
    "trendx_train": _run_trendx_train,
    "trendx_forecast": _run_trendx_forecast,
}


def is_single_planner_config(*, worker_ingestion_enabled: bool, scheduler_enabled: bool) -> bool:
    """Contrat de déduplication MR-5 (XOR planificateurs d'ingestion).

    Retourne True sauf si le worker ET le scheduler B1 dédié planifieraient
    l'ingestion simultanément — configuration interdite (double exécution).
    Les deux inactifs (fenêtre transitoire de la séquence d'activation)
    restent autorisés : le recouvrement checkpoints (1h) absorbe le gap au
    retour d'un planificateur. Fonction pure : sans effet de bord, testée en
    CI (le test XOR DOIT échouer si la double planification redevient
    possible).
    """
    return not (worker_ingestion_enabled and scheduler_enabled)


def _parse_json_job(raw: Any) -> dict[str, Any]:
    """Parse le json_job de la requête. Retourne {} si None/"".
    Lève json.JSONDecodeError si le contenu n'est pas du JSON valide.
    """
    if raw is None or raw == "":
        return {}
    if isinstance(raw, dict):
        return raw
    return cast("dict[str, Any]", json.loads(raw))


def _process_one_task(worker_id: str) -> bool:
    """Cycle atomique claim -> dispatch -> exécution -> complete/fail.

    Retourne True si une tâche a été claimée (et traitée), False si aucune
    tâche claimable n'était disponible.
    """
    svc = TaskService()
    request = svc.claim_task(worker_id=worker_id)
    if request is None:
        return False

    task_id = str(request.task_id)
    execution_id = str(request.execution_id)
    job_type = request.job_type

    # 1) json_job obligatoire et valide -> sinon FAILED
    try:
        json_job = _parse_json_job(request.json_job)
    except (json.JSONDecodeError, TypeError, ValueError) as exc:
        logger.error(
            "Worker {} : json_job invalide pour task {} execution {} : {}",
            worker_id,
            task_id,
            execution_id,
            exc,
        )
        svc.fail_task(task_id, error_message=f"Invalid json_job: {exc}")
        return True

    # 2) Transition CREATED -> RUNNING
    try:
        svc.update_progress(task_id, 0)
    except Exception as exc:
        logger.error(
            "Worker {} : update_progress échoué pour task {} execution {} : {}",
            worker_id,
            task_id,
            execution_id,
            exc,
        )
        svc.fail_task(task_id, error_message=f"update_progress failed: {exc}")
        return True

    # 3) Dispatch explicite (aucun fallback silencieux)
    handler = JOB_DISPATCH.get(job_type)
    if handler is None:
        logger.error(
            "Worker {} : job_type inconnu '{}' pour task {} execution {}",
            worker_id,
            job_type,
            task_id,
            execution_id,
        )
        svc.fail_task(task_id, error_message=f"Unknown job_type: {job_type}")
        return True

    # 4) Exécution + persistance, puis complete (succès) ou fail (exception)
    try:
        result = handler(json_job, task_id, execution_id)
        svc.complete_task(task_id, result=result)
        logger.info(
            "Worker {} : task {} (job_type={}) terminée FINISHED",
            worker_id,
            task_id,
            job_type,
        )
    except Exception as exc:
        logger.error(
            "Worker {} : échec handler job_type={} pour task {} execution {} : {}",
            worker_id,
            job_type,
            task_id,
            execution_id,
            exc,
        )
        svc.fail_task(task_id, error_message=f"{job_type} failed: {exc}")
    return True


_execution_stop = threading.Event()
_execution_thread: threading.Thread | None = None


def _worker_execution_loop(
    worker_id: str, poll_interval: float, stop_event: threading.Event
) -> None:
    logger.info("trendx-worker execution loop démarrée (worker_id={})", worker_id)
    while not stop_event.is_set():
        try:
            _process_one_task(worker_id)
        except Exception as exc:  # la boucle ne doit jamais mourir
            logger.error(
                "Worker {} : erreur non gérée dans la boucle d'exécution : {}",
                worker_id,
                exc,
            )
        stop_event.wait(poll_interval)


def start_execution_loop(
    poll_interval: float = 5.0, worker_id: str = "worker-execution"
) -> threading.Thread:
    """Démarre (idempotent) le thread d'exécution dédié (non-daemon) qui garde
    le process en vie indépendamment du scheduler APScheduler de maintenance.
    """
    global _execution_thread
    if _execution_thread is not None and _execution_thread.is_alive():
        return _execution_thread
    _execution_stop.clear()
    _execution_thread = threading.Thread(
        target=_worker_execution_loop,
        args=(worker_id, poll_interval, _execution_stop),
        name="trendx-worker-execution",
        daemon=False,
    )
    _execution_thread.start()
    logger.info("trendx-worker execution thread démarré (poll={}s)", poll_interval)
    return _execution_thread


def stop_execution_loop(timeout: float = 5.0) -> None:
    """Demande l'arrêt du thread d'exécution et attend sa fin."""
    _execution_stop.set()
    if _execution_thread is not None:
        _execution_thread.join(timeout=timeout)


# ————————————————————————————————————————————————
# Maintenance planifiée : partitions mensuelles + agrégats (UPSERT incrémental)
# ————————————————————————————————————————————————
try:
    from apscheduler.schedulers.background import BackgroundScheduler
    from apscheduler.triggers.cron import CronTrigger
    from apscheduler.triggers.interval import IntervalTrigger

    HAS_APSCHEDULER = True
except (
    ImportError
):  # pragma: no cover - dépendance runtime déclarée dans pyproject.toml (APScheduler)
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
        engine = get_analytics_engine()
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

    def _job_partitions_retention() -> None:
        """Rétention native des partitions : DROP des partitions mensuelles
        suffisamment anciennes pour les 5 parents (ts_kv, predictions,
        anomaly_scores, data_quality, ml_metrics).

        Délègue tout le DDL à la fonction SECURITY DEFINER
        trendx_analytics.drop_old_partitions() (migration 012) :
        AUCUN DROP n'est exécuté directement depuis Python, le chemin
        SECURITY DEFINER (propriétaire trendx_migration) n'est pas contourné.
        """
        try:
            result = _call_db_function("SELECT trendx_analytics.drop_old_partitions(NULL)", {})
            dropped = result.get("dropped_count", 0) if isinstance(result, dict) else 0
            logger.info(
                f"[scheduler] drop_old_partitions() -> partitions supprimées={dropped} "
                f"(détail: {result})"
            )
        except Exception as exc:
            logger.error(f"[scheduler] drop_old_partitions() échec: {exc}")

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

    def _job_ingestion() -> None:
        """Ingestion incrémentale planifiée (heure). Réutilise le handler
        existant JOB_DISPATCH['ingestion'] (= _run_ingestion) : aucune logique
        d'ingestion dupliquée. Le recouvrement de 1h et l'UPSERT garantissent
        l'idempotence à chaque exécution.
        """
        try:
            result = _run_ingestion({}, "ingestion_hourly", "scheduled")
            logger.info(
                "[scheduler] ingestion_hourly -> {tasks} tasks", tasks=result.get("tasks", 0)
            )
        except Exception as exc:
            logger.error(f"[scheduler] ingestion_hourly échec: {exc}")

    def _job_scheduler_runs_retention() -> None:
        """Purge des runs scheduler terminaux (ok/failed) au-delà de
        TRENDX_RETENTION_DAYS. Ne supprime jamais les lignes running
        (reprise crash + tâches enqueue en attente). DML simple via le
        repository, aucun DDL depuis Python.
        """
        try:
            from trendx.database.connection import manager as db_manager
            from trendx.database.repositories import SchedulerRunRepository

            cutoff = datetime.now(UTC) - timedelta(days=settings.trendx_retention_days)
            with db_manager.get_session("catalog") as session:
                deleted = SchedulerRunRepository(session).purge_before(cutoff)
                session.commit()
            logger.info(
                "[scheduler] scheduler_runs_retention -> runs supprimés={deleted} "
                "(cutoff={cutoff}, jours={days})",
                deleted=deleted,
                cutoff=cutoff.isoformat(),
                days=settings.trendx_retention_days,
            )
        except Exception as exc:
            logger.error(f"[scheduler] scheduler_runs_retention échec: {exc}")

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
        # Rétention native : exécutée APRÈS partitions_monthly (jour 1 00:15).
        # Le DROP des partitions anciennes ne doit jamais supprimer la fenêtre
        # forward +3 mois recréée par ensure_partitions_forward().
        sched.add_job(
            _job_partitions_retention,
            CronTrigger(day="2", hour=0, minute=30),
            id="partitions_retention_monthly",
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
        # Ingestion incrémentale horaire (Phase 3). Réutilise _job_ingestion ->
        # JOB_DISPATCH['ingestion']; aucun job existant n'est modifié.
        # MR-5 (déduplication, contrat XOR) : enregistrement conditionné par
        # TRENDX_WORKER_INGESTION_ENABLED. false quand le scheduler B1 dédié
        # assure la planification (profil scheduler actif) — jamais les deux
        # planificateurs simultanément. Rollback : true + restart worker.
        if settings.trendx_worker_ingestion_enabled:
            sched.add_job(
                _job_ingestion,
                IntervalTrigger(hours=1),
                id="ingestion_hourly",
                max_instances=1,
                coalesce=True,
                misfire_grace_time=600,
            )
        else:
            logger.info(
                "[scheduler] ingestion_hourly désactivé "
                "(TRENDX_WORKER_INGESTION_ENABLED=false) : planification "
                "assurée par le scheduler B1 dédié"
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
        # Purge mensuelle des runs scheduler terminaux (migration 014).
        # Cadence distincte des jobs partitions (jour 1-2) ; ne touche jamais
        # les lignes running.
        sched.add_job(
            _job_scheduler_runs_retention,
            CronTrigger(day="3", hour=1, minute=30),
            id="scheduler_runs_retention_monthly",
            max_instances=1,
            coalesce=True,
            misfire_grace_time=3600,
        )
        sched.start()
        # Log fidèle à l'enregistrement réel (ingestion_hourly conditionné
        # par TRENDX_WORKER_INGESTION_ENABLED — contrat XOR MR-5).
        ingestion_suffix = " ingestion_hourly," if settings.trendx_worker_ingestion_enabled else ""
        logger.info(
            "trendx-worker APScheduler démarré (jobs: partitions_monthly,"
            " partitions_retention_monthly, aggregate_hourly, aggregate_daily,"
            " aggregate_weekly,{} partitions_boot_check,"
            " scheduler_runs_retention_monthly)",
            ingestion_suffix,
        )
        return sched


_scheduler = None
if HAS_APSCHEDULER:
    _scheduler = _start_scheduler()


# Worker execution loop (découplé de la maintenance APScheduler). Désactivé par
# défaut ; activé en production via TRENDX_WORKER_EXECUTION=1 (Dockerfile.worker).
if os.environ.get("TRENDX_WORKER_EXECUTION", "0") == "1":
    start_execution_loop()


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

if __name__ == "__main__":
    start_execution_loop()
    try:
        while True:
            time.sleep(3600)
    except KeyboardInterrupt:
        stop_execution_loop()
        if _scheduler is not None:
            _scheduler.shutdown(wait=False)
        logger.info("trendx-worker stopped by signal")


# W49 (résolution) : enregistrement disjoint des handlers ML.
# Placé en fin de fichier, zone non touchée par l'extraction B1, afin que la
# fusion avec master reste propre : handlers définis dans
# trendx.scheduler.handlers (source de vérité, sans import de ce module),
# enregistrés ici sans modifier le bloc d'import ni le littéral JOB_DISPATCH.
from trendx.scheduler.handlers import _run_anomaly_scan as _w49_anomaly_scan  # noqa: E402
from trendx.scheduler.handlers import _run_ml_pipeline as _w49_ml_pipeline  # noqa: E402

JOB_DISPATCH["anomaly_scan"] = _w49_anomaly_scan
JOB_DISPATCH["ml_pipeline"] = _w49_ml_pipeline
