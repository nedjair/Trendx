"""Scheduler registry: registration, dispatch, idempotency, isolation.

All timestamps UTC. Run IDs are synthetic UUIDs. No real waiting,
no threads, no production side effects.

Isolation contract (dispatch-isolation fix): the job catalogue is either
injected explicitly via ``specs`` or resolved from ``P0_JOBS`` at
construction time -- never via a stale import-time binding. A job without
an explicitly registered handler fails closed (RuntimeError): this module
never imports ``trendx.services.worker`` and never falls back to a stub.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from trendx.scheduler.jobs import P0_JOBS, JobSpec
from trendx.scheduler.persistence import RunStore, default_instance_id, resolve_run_mode

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
    """Wiring between P0 schedules and injected handlers (explicit dispatch)."""

    _handlers: dict[str, Handler] = field(default_factory=dict)
    _records: list[TriggerRecord] = field(default_factory=list)
    _idempotency: dict[str, dict[str, Any]] = field(default_factory=dict)
    specs: tuple[JobSpec, ...] | None = None
    run_store: RunStore | None = None
    instance_id: str | None = None
    _specs: tuple[JobSpec, ...] = field(init=False, repr=False)
    _instance_id: str = field(init=False, repr=False)

    def __post_init__(self) -> None:
        # Resolved at construction time (module-global read): patching
        # trendx.scheduler.registry.P0_JOBS before constructing a registry
        # takes effect; an explicitly passed `specs` always wins.
        self._specs = self.specs if self.specs is not None else P0_JOBS
        seen: set[str] = set()
        for spec in self._specs:
            if spec.job_id in seen:
                raise ValueError(f"Duplicate job id: {spec.job_id}")
            seen.add(spec.job_id)
        # Identifiant d'instance stable pour la persistance des runs
        # (heartbeat/leader MR-3). Sans run_store, purement informatif.
        self._instance_id = self.instance_id or default_instance_id()

    @property
    def jobs(self) -> list[JobDefinition]:
        return [JobDefinition(s.job_id, s.job_type, s.interval, s.description) for s in self._specs]

    def schedules(self) -> dict[str, str]:
        return {s.job_id: s.interval for s in self._specs}

    def register_handler(self, job_type: str, handler: Handler) -> None:
        self._handlers[job_type] = handler

    def trigger(
        self,
        job_id: str,
        payload: dict[str, Any] | None = None,
        idempotency_key: str | None = None,
    ) -> dict[str, Any]:
        spec: JobSpec | None = next((s for s in self._specs if s.job_id == job_id), None)
        if spec is None:
            raise KeyError(f"Unknown job id: {job_id}")
        if idempotency_key is not None:
            cache_key = f"{job_id}:{idempotency_key}"
            if cache_key in self._idempotency:
                cached = dict(self._idempotency[cache_key])
                cached["deduplicated"] = True
                return cached
        handler = self._handlers.get(spec.job_type)
        if handler is None:
            # Fail closed: no implicit fallback, no services.worker import,
            # no silent stub. Register an explicit handler instead.
            raise RuntimeError(
                f"No handler registered for job_type {spec.job_type!r} "
                f"(job_id {job_id!r}): explicit dispatch required"
            )
        run_id = str(uuid4())
        # Mode d'exécution (défaut direct, fail-closed) : validé AVANT toute
        # persistance. Clé namespacée `scheduler_mode` : la clé métier `mode`
        # appartient aux payloads (ex. discovery "initial"/"incremental") et
        # doit traverser intacte. Sans run_store, le comportement historique
        # est inchangé.
        data = dict(payload or {})
        mode = resolve_run_mode(data.get("scheduler_mode", "direct"))
        payload_task_id = data.get("task_id")
        task_id = str(payload_task_id) if payload_task_id is not None else None
        if self.run_store is not None:
            self.run_store.begin_run(
                run_id=run_id,
                job_id=job_id,
                job_type=spec.job_type,
                instance_id=self._instance_id,
                mode=mode,
                task_id=task_id,
            )
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
            if self.run_store is not None:
                self.run_store.finish_run(
                    run_id=run_id, status="failed", error=f"{type(exc).__name__}: {exc}"
                )
            raise
        else:
            if self.run_store is not None:
                status = str(outcome.get("status") or "ok")
                task_out = outcome.get("task_id")
                self.run_store.finish_run(
                    run_id=run_id,
                    status=status,
                    task_id=str(task_out) if task_out is not None else None,
                )
            if idempotency_key is not None:
                self._idempotency[f"{job_id}:{idempotency_key}"] = dict(outcome)
            return outcome

    def records(self) -> list[TriggerRecord]:
        return list(self._records)
