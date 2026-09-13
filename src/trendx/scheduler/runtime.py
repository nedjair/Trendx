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
from trendx.scheduler.leader import LeaderElection
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
    """Dry-run APScheduler bound to a SchedulerRegistry.

    Leadership fencing is strictly opt-in (``require_leadership=False`` by
    default preserves the historical behavior): when enabled, an explicit
    ``LeaderElection`` is required and scheduling/triggering is refused
    unless this instance is effectively the leader. No silent fallback to
    execution without leadership. Freshness across ticks is the service
    loop's job (renew per tick) ; each fire additionally checks the local
    leadership flag so a stepped-down instance executes nothing.
    """

    def __init__(
        self,
        registry: SchedulerRegistry | None = None,
        *,
        require_leadership: bool = False,
        election: LeaderElection | None = None,
    ) -> None:
        if require_leadership and election is None:
            raise ValueError(
                "require_leadership=True needs an explicit election "
                "(LeaderElection) ; refusing silent unscheduled mode"
            )
        self._registry = registry or SchedulerRegistry()
        self._scheduler = BackgroundScheduler(timezone=UTC)
        self._started = False
        self._require_leadership = require_leadership
        self._election = election

    @property
    def registry(self) -> SchedulerRegistry:
        return self._registry

    def _guards(self) -> None:
        from trendx.config import settings as _settings

        if _settings.tb_writeback_enabled:
            raise RuntimeError("Refusing start: TB_WRITEBACK_ENABLED must be false for dry-run")
        if _settings.tb_alarms_enabled:
            raise RuntimeError("Refusing start: TB_ALARMS_ENABLED must be false for dry-run")

    def _leadership_gate(self) -> None:
        """Refuse scheduling/triggering unless effectively leader.

        No-op when leadership is not required. With require_leadership, the
        election is renewed (liveness + fresh heartbeat) ; any doubt means
        non-leader (fail-closed, explicit RuntimeError, never silent).
        """
        if not self._require_leadership:
            return
        election = self._election
        if election is None:  # defensive: __init__ already rejects this combination
            raise RuntimeError("Refusing schedule: leadership required but no election configured")
        try:
            alive = bool(election.is_leader()) and bool(election.renew())
        except Exception:
            alive = False
        if not alive:
            raise RuntimeError(
                "Refusing schedule: instance is not the scheduler leader "
                "(require_leadership=True)"
            )

    def schedule_all(self, payload: dict[str, Any] | None = None) -> list[str]:
        self._leadership_gate()
        now = datetime.now(UTC)
        ids: list[str] = []
        for job in self._registry.jobs:
            trigger = _trigger_for_interval(job.interval, now)

            def _fire(
                job_id: str = job.job_id, base: dict[str, Any] | None = payload
            ) -> dict[str, Any]:
                if self._require_leadership and not self._election_fires():
                    return {
                        "job_id": job_id,
                        "status": "refused-not-leader",
                        "reason": "stepped down after scheduling; no execution",
                    }
                run_payload = dict(base or {})
                run_payload.setdefault("run_id", str(uuid4()))
                return self._registry.trigger(job_id, run_payload)

            self._scheduler.add_job(_fire, trigger=trigger, id=job.job_id, replace_existing=True)
            ids.append(job.job_id)
        return ids

    def _election_fires(self) -> bool:
        """Per-fire fencing: local leadership flag only (no I/O).

        Freshness across ticks belongs to the service loop (renew per tick,
        MR-4) ; a stepped-down instance executes nothing even for jobs
        scheduled while it was leader.
        """
        election = self._election
        if election is None:
            return True
        try:
            return bool(election.is_leader())
        except Exception:
            return False

    def start(self) -> None:
        self._guards()
        if not self._started:
            self._scheduler.start()
            self._started = True

    def run_job_now(self, job_id: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        self._leadership_gate()
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
