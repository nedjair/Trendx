"""Mock scheduler registry: registration, dispatch, idempotency, isolation.

All timestamps UTC. Run IDs are synthetic UUIDs. No real waiting,
no threads, no production side effects.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from trendx.scheduler.jobs import P0_JOBS, JobSpec

Handler = Callable[[dict[str, Any]], dict[str, Any]]


@dataclass
class JobDefinition:
    job_id: str
    job_type: str
    interval: str
    description: str


@dataclass
class TriggerRecord:
    run_id: str
    job_id: str
    triggered_at: datetime
    status: str
    error: str | None = None


@dataclass
class SchedulerRegistry:
    """Wiring between P0 schedules and existing workers (injectable)."""

    _handlers: dict[str, Handler] = field(default_factory=dict)
    _records: list[TriggerRecord] = field(default_factory=list)
    _idempotency: dict[str, dict[str, Any]] = field(default_factory=dict)

    def __post_init__(self) -> None:
        seen: set[str] = set()
        for spec in P0_JOBS:
            if spec.job_id in seen:
                raise ValueError(f"Duplicate job id: {spec.job_id}")
            seen.add(spec.job_id)

    @property
    def jobs(self) -> list[JobDefinition]:
        return [JobDefinition(s.job_id, s.job_type, s.interval, s.description) for s in P0_JOBS]

    def schedules(self) -> dict[str, str]:
        return {s.job_id: s.interval for s in P0_JOBS}

    def register_handler(self, job_type: str, handler: Handler) -> None:
        self._handlers[job_type] = handler

    def _default_handler(self, job_type: str) -> Handler:
        from trendx.services import worker as worker_module

        dispatch = worker_module.JOB_DISPATCH
        if job_type == "anomaly_scan":
            from trendx.anomalies.detectors import IsolationForestDetector

            def _scan(payload: dict[str, Any]) -> dict[str, Any]:
                IsolationForestDetector(contamination=0.05)
                return {"job_type": job_type, "status": "ok"}

            return _scan
        if job_type not in dispatch:
            raise KeyError(f"No worker for job type: {job_type}")

        func = dispatch[job_type]

        def _call(payload: dict[str, Any]) -> dict[str, Any]:
            result = func(dict(payload), f"mock-task-{uuid4().hex[:8]}", f"exec-{uuid4().hex[:8]}")
            if isinstance(result, dict):
                return result
            return {"job_type": job_type, "status": "ok", "result": result}

        return _call

    def trigger(
        self,
        job_id: str,
        payload: dict[str, Any] | None = None,
        idempotency_key: str | None = None,
    ) -> dict[str, Any]:
        spec: JobSpec | None = next((s for s in P0_JOBS if s.job_id == job_id), None)
        if spec is None:
            raise KeyError(f"Unknown job id: {job_id}")
        if idempotency_key is not None:
            cache_key = f"{job_id}:{idempotency_key}"
            if cache_key in self._idempotency:
                cached = dict(self._idempotency[cache_key])
                cached["deduplicated"] = True
                return cached
        handler = self._handlers.get(spec.job_type) or self._default_handler(spec.job_type)
        run_id = str(uuid4())
        try:
            result = handler(dict(payload or {}))
            if not isinstance(result, dict):
                result = {"result": result}
            outcome = {"run_id": run_id, "job_id": job_id, "status": "ok", **result}
            self._records.append(TriggerRecord(run_id, job_id, datetime.now(UTC), "ok"))
        except Exception as exc:
            self._records.append(
                TriggerRecord(
                    run_id, job_id, datetime.now(UTC), "fail", f"{type(exc).__name__}: {exc}"
                )
            )
            raise
        else:
            if idempotency_key is not None:
                self._idempotency[f"{job_id}:{idempotency_key}"] = dict(outcome)
            return outcome

    def records(self) -> list[TriggerRecord]:
        return list(self._records)
