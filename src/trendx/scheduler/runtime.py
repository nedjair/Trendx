"""APScheduler runtime wiring (dry-run only).

Real BackgroundScheduler + existing SchedulerRegistry. Guards fail
closed: start() refuses when TB writeback/alarms are enabled.
No production activation in this module: callers inject handlers
(mock/in-memory in tests) and control start/stop explicitly.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.date import DateTrigger
from apscheduler.triggers.interval import IntervalTrigger
from trendx.scheduler.registry import SchedulerRegistry

_INTERVAL_MINUTES: dict[str, float] = {"15m": 15.0, "1h": 60.0, "1d": 1440.0}


def _trigger_for_interval(interval: str, now: datetime) -> DateTrigger | IntervalTrigger:
    if interval == "once":
        return DateTrigger(run_date=now)
    minutes = _INTERVAL_MINUTES.get(interval)
    if minutes is None:
        raise ValueError(f"Unknown interval: {interval!r}")
    return IntervalTrigger(minutes=minutes, start_date=now)


class RuntimeScheduler:
    """Dry-run APScheduler bound to a SchedulerRegistry."""

    def __init__(self, registry: SchedulerRegistry | None = None) -> None:
        self._registry = registry or SchedulerRegistry()
        self._scheduler = BackgroundScheduler(timezone=UTC)
        self._started = False

    @property
    def registry(self) -> SchedulerRegistry:
        return self._registry

    def _guards(self) -> None:
        from trendx.config import settings as _settings

        if _settings.tb_writeback_enabled:
            raise RuntimeError("Refusing start: TB_WRITEBACK_ENABLED must be false for dry-run")
        if _settings.tb_alarms_enabled:
            raise RuntimeError("Refusing start: TB_ALARMS_ENABLED must be false for dry-run")

    def schedule_all(self, payload: dict[str, Any] | None = None) -> list[str]:
        now = datetime.now(UTC)
        ids: list[str] = []
        for job in self._registry.jobs:
            trigger = _trigger_for_interval(job.interval, now)

            def _fire(
                job_id: str = job.job_id, base: dict[str, Any] | None = payload
            ) -> dict[str, Any]:
                run_payload = dict(base or {})
                run_payload.setdefault("run_id", str(uuid4()))
                return self._registry.trigger(job_id, run_payload)

            self._scheduler.add_job(_fire, trigger=trigger, id=job.job_id, replace_existing=True)
            ids.append(job.job_id)
        return ids

    def start(self) -> None:
        self._guards()
        if not self._started:
            self._scheduler.start()
            self._started = True

    def run_job_now(self, job_id: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        if self._scheduler.get_job(job_id) is None:
            raise KeyError(f"Job not scheduled: {job_id}")
        return self._registry.trigger(job_id, dict(payload or {}))

    def stop(self) -> None:
        if self._started:
            self._scheduler.shutdown(wait=True)
            self._started = False

    def scheduled_ids(self) -> list[str]:
        return sorted(j.id for j in self._scheduler.get_jobs())

    def is_running(self) -> bool:
        return self._started and self._scheduler.running

    def __enter__(self) -> RuntimeScheduler:
        self.start()
        return self

    def __exit__(self, *exc: object) -> None:
        self.stop()
