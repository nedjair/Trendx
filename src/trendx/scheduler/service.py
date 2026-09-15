"""Scheduler B1 dedicated service entrypoint: ``python -m trendx.scheduler.service``.

Composes the validated building blocks without rewriting them:

- MR-1: P0 catalogue (``P0_JOBS``) + shared handlers (``JOB_HANDLERS``) ;
- MR-2: run persistence (``DbRunStore``) + 1-arg/3-arg adaptation ;
- MR-3: leader election, heartbeat, fencing, configuration.

Explicit lifecycle::

    init -> config -> guards -> persistence -> election
      -> scheduling (leader only) -> service loop
      -> SIGTERM/SIGINT -> graceful stop

OFF by default: ``TRENDX_SCHEDULER_ENABLED=false`` means no scheduling at
all (the process serves health as ``disabled`` and idles until signal).
No worker import, no writeback, no alarms, no production activation here:
activation is a compose profile (separate authorization, MR-5).
"""

from __future__ import annotations

import socket
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

from loguru import logger
from trendx.scheduler.handlers import JOB_HANDLERS
from trendx.scheduler.jobs import JobSpec
from trendx.scheduler.leader import HeartbeatWriter, LeaderElection
from trendx.scheduler.persistence import DbRunStore, adapt_p0_handler, default_instance_id
from trendx.scheduler.registry import SchedulerRegistry
from trendx.scheduler.runtime import RuntimeScheduler

# Job types executed in-process (direct) vs delegated to the worker task
# queue (enqueue: created via TaskService, claimed by the worker loop).
ENQUEUE_JOB_TYPES = frozenset({"trendx_train", "trendx_forecast", "anomaly_scan"})

# P0 jobs without a registered handler stay unscheduled (explicit gap, no
# failing fire): today only anomaly_scan has no JOB_HANDLERS entry.
UNSCHEDULED_JOB_TYPES = frozenset({"anomaly_scan"})

# P0 forecast jobs gated behind TRENDX_SCHEDULER_FORECAST_ENABLED (default
# false). Forecast B1 never worked in production (no per-device fan-out :
# hourly fires produced bare payloads -> worker fail-fast + orphan running
# rows). Gating stops the noise without touching ingestion/discovery ; the
# functional forecast implementation (Option A) is a separate workstream.
FORECAST_JOB_IDS = frozenset({"forecast-train", "forecast-run"})


class SchedulerService:
    """B1 scheduler service: elect, schedule (leader only), serve, stop cleanly."""

    def __init__(
        self,
        settings_obj: Any | None = None,
        *,
        specs: tuple[JobSpec, ...] | None = None,
        handlers: dict[str, Any] | None = None,
        election: LeaderElection | None = None,
        metrics_port: int = 9109,
    ) -> None:
        from trendx.config import settings as _global_settings

        self._settings = settings_obj if settings_obj is not None else _global_settings
        self._metrics_port = metrics_port
        self._specs = specs
        self._handlers_override = handlers
        self._stop_event = threading.Event()
        self._scheduled = False
        self._started = False
        self._httpd: ThreadingHTTPServer | None = None
        self._http_thread: threading.Thread | None = None

        self._instance_id = (
            str(getattr(self._settings, "trendx_scheduler_instance_id", "") or "").strip()
            or default_instance_id()
        )
        self._registry = SchedulerRegistry(
            specs=self._scheduled_specs(),
            run_store=DbRunStore(),
            instance_id=self._instance_id,
        )
        for job_type, fn in self._p0_handlers().items():
            mode = "enqueue" if job_type in ENQUEUE_JOB_TYPES else "direct"
            self._registry.register_handler(job_type, adapt_p0_handler(fn, mode=mode))
        self._election = (
            election
            if election is not None
            else LeaderElection(
                lock_name=str(
                    getattr(self._settings, "trendx_scheduler_leader_lock", "")
                    or "trendx_scheduler_leader"
                ),
                instance_id=self._instance_id,
                heartbeat_seconds=int(
                    getattr(self._settings, "trendx_scheduler_heartbeat_seconds", 15)
                ),
                stale_seconds=int(getattr(self._settings, "trendx_scheduler_stale_seconds", 60)),
                acquire_timeout_seconds=int(
                    getattr(self._settings, "trendx_scheduler_acquire_timeout_seconds", 5)
                ),
                heartbeat_writer=self._registry_heartbeat_writer(),
                version="mr4",
                host=socket.gethostname(),
            )
        )
        self._runtime = RuntimeScheduler(
            self._registry, require_leadership=True, election=self._election
        )

    # ── wiring (composes validated blocks, invents nothing) ────────────

    def _scheduled_specs(self) -> tuple[JobSpec, ...] | None:
        if self._specs is not None:
            return self._specs
        from trendx.scheduler.jobs import P0_JOBS

        specs = [s for s in P0_JOBS if s.job_type not in UNSCHEDULED_JOB_TYPES]
        if not getattr(self._settings, "trendx_scheduler_forecast_enabled", False):
            specs = [s for s in specs if s.job_id not in FORECAST_JOB_IDS]
        return tuple(specs)

    def _p0_handlers(self) -> dict[str, Any]:
        if self._handlers_override is not None:
            return dict(self._handlers_override)
        return dict(JOB_HANDLERS)

    def _registry_heartbeat_writer(self) -> HeartbeatWriter:
        store = DbRunStore()

        def _write(*, instance_id: str, leader: bool, version: str, host: str) -> None:
            store.record_heartbeat(
                instance_id=instance_id, leader=leader, version=version, host=host
            )

        return _write

    # ── state ──────────────────────────────────────────────────────────

    @property
    def enabled(self) -> bool:
        return bool(getattr(self._settings, "trendx_scheduler_enabled", False))

    @property
    def instance_id(self) -> str:
        return self._instance_id

    @property
    def is_leader(self) -> bool:
        return self._election.is_leader()

    def health(self) -> dict[str, Any]:
        """Operational state for /healthz. Standby is never reported leader."""
        if not self.enabled:
            status = "disabled"
        elif self._election.is_stale():
            status = "degraded" if self._election.last_error else "standby"
            if self._election.is_leader():
                status = "degraded"
        elif self._election.is_leader():
            status = "leader"
        else:
            status = "standby"
        return {
            "status": status,
            "service": "trendx-scheduler",
            "scheduler_enabled": self.enabled,
            "leader": self._election.is_leader(),
            "instance_id": self._instance_id,
            "last_error": self._election.last_error,
        }

    def metrics_text(self) -> str:
        """Minimal Prometheus exposition (MR-4 contract, no new stack)."""
        lines = [
            "# HELP scheduler_up Service process alive (1).",
            "# TYPE scheduler_up gauge",
            "scheduler_up 1",
            "# HELP scheduler_enabled Scheduling allowed by configuration (0/1).",
            "# TYPE scheduler_enabled gauge",
            f"scheduler_enabled {1 if self.enabled else 0}",
            "# HELP scheduler_leader This instance effectively leads (0/1).",
            "# TYPE scheduler_leader gauge",
            f"scheduler_leader {1 if self._election.is_leader() else 0}",
        ]
        return "\n".join(lines) + "\n"

    # ── lifecycle ──────────────────────────────────────────────────────

    def start(self) -> str:
        """Start serving; schedule only when enabled AND leader. Idempotent."""
        self._refuse_if_unsafe()
        self._start_http_server()
        if not self.enabled:
            logger.info("trendx-scheduler disabled (TRENDX_SCHEDULER_ENABLED=false): idle")
            self._started = True
            return "disabled"
        if self.run_once() == "leader":
            logger.info("trendx-scheduler started as leader (instance={})", self._instance_id)
        else:
            logger.info("trendx-scheduler started as standby (instance={})", self._instance_id)
        self._started = True
        status: str = self.health()["status"]
        return status

    def run_once(self) -> str:
        """One service-loop iteration: elect, then schedule/freeze accordingly.

        Leader -> ensure runtime scheduled+started. Non-leader -> runtime
        stopped (frozen, heartbeating only). Returns the health status.
        """
        leader = bool(self._election.tick())
        if not self.enabled:
            return "disabled"
        if leader and not self._scheduled:
            self._runtime.schedule_all({})
            self._runtime.start()
            self._scheduled = True
        elif not leader and self._scheduled:
            self._runtime.stop()
            self._scheduled = False
        return "leader" if leader else "standby"

    def run_forever(self, interval_seconds: float | None = None) -> None:
        """Block until SIGTERM/SIGINT, ticking the election loop."""
        period = float(
            interval_seconds
            if interval_seconds is not None
            else getattr(self._settings, "trendx_scheduler_heartbeat_seconds", 15)
        )
        while not self._stop_event.wait(timeout=period):
            try:
                self.run_once()
            except Exception as exc:
                logger.error("Scheduler service tick failed (non-leader safe): {}", exc)

    def stop(self) -> None:
        """Graceful shutdown, idempotent: freeze scheduling, release
        leadership when possible, close resources, no stray traceback."""
        self._stop_event.set()
        try:
            self._runtime.stop()
        except Exception as exc:
            logger.error("Runtime stop failed during shutdown (ignored): {}", exc)
        self._scheduled = False
        try:
            self._election.release()
        except Exception as exc:
            logger.error("Leadership release failed during shutdown (ignored): {}", exc)
        httpd, self._httpd = self._httpd, None
        if httpd is not None:
            try:
                httpd.shutdown()
                httpd.server_close()
            except Exception as exc:
                logger.error("HTTP server shutdown failed (ignored): {}", exc)
        self._started = False

    # ── guards ─────────────────────────────────────────────────────────

    def _refuse_if_unsafe(self) -> None:
        if bool(getattr(self._settings, "tb_writeback_enabled", False)):
            raise RuntimeError("Refusing scheduler start: TB_WRITEBACK_ENABLED must be false")
        if bool(getattr(self._settings, "tb_alarms_enabled", False)):
            raise RuntimeError("Refusing scheduler start: TB_ALARMS_ENABLED must be false")

    # ── http ───────────────────────────────────────────────────────────

    def _start_http_server(self) -> None:
        if self._httpd is not None:
            return  # already serving: start() stays idempotent
        service = self

        class _Handler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:  # noqa: N802
                try:
                    if self.path == "/healthz":
                        import json as _json

                        body = _json.dumps(service.health()).encode()
                        self.send_response(200)
                        self.send_header("Content-Type", "application/json")
                    elif self.path == "/metrics":
                        body = service.metrics_text().encode()
                        self.send_response(200)
                        self.send_header("Content-Type", "text/plain; version=0.0.4")
                    else:
                        body = b"not found"
                        self.send_response(404)
                        self.send_header("Content-Type", "text/plain")
                    self.end_headers()
                    self.wfile.write(body)
                except Exception as exc:
                    logger.debug("Scheduler HTTP response failed (ignored): {}", exc)

            def log_message(self, format: str, *args: Any, **kwargs: Any) -> None:
                return

        srv = ThreadingHTTPServer(("127.0.0.1", self._metrics_port), _Handler)
        self._httpd = srv
        thread = threading.Thread(
            target=srv.serve_forever, name="trendx-scheduler-http", daemon=True
        )
        thread.start()
        self._http_thread = thread
        logger.info("trendx-scheduler HTTP listen on 127.0.0.1:{}", self._metrics_port)


def main(argv: list[str] | None = None) -> int:
    """Entrypoint: ``python -m trendx.scheduler.service``. Returns exit code."""
    del argv
    service = SchedulerService()

    def _on_signal(signum: int, frame: Any) -> None:
        logger.info("trendx-scheduler received signal {}, stopping", signum)
        service.stop()

    try:
        import signal as _signal

        _signal.signal(_signal.SIGTERM, _on_signal)
        _signal.signal(_signal.SIGINT, _on_signal)
    except Exception as exc:
        logger.error("Signal handler install failed (continuing): {}", exc)
    try:
        service.start()
        service.run_forever()
    except Exception as exc:
        logger.error("trendx-scheduler fatal: {}", exc)
        service.stop()
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
