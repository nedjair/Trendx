"""Scheduler-side fan-out and reconciliation for forecast jobs (remediation).

Context: P0 ``forecast-train`` / ``forecast-run`` triggers historically fired
with a bare ``{"run_id"}`` payload, and the enqueue adapter defaulted the task
type to ``trendx_train``. Workers fail-fasted (missing keys) while
``scheduler_run`` rows stayed ``running`` forever (no reconciler).

This module provides, scheduler-side only (never imported by the worker):

- :func:`iter_forecast_pairs` : tenant-scoped ``(tenant, entity, metric)``
  enumeration from the catalogue, deduplicated. Empty catalogue -> ``[]``
  with an explicit warning (never a silent fan-out, never fabricated pairs).
- :func:`forecast_reference_key` : deterministic traceability key
  ``{job_type}:{tenant}:{entity}:{metric}:{window}``. Traceability only :
  no DB uniqueness constraint backs it (stated, not assumed).
- :func:`fanout_enqueue_forecast_jobs` : per pair, one ``scheduler_run``
  (begin) + one ``TaskService`` task with the COMPLETE payload and the exact
  job type. Parent trigger row is finalized ``ok`` by the caller with the
  fanned-out count.
- :func:`reconcile_runs_job` : direct-mode P0 handler (5 min cadence) mapping
  terminal worker executions back onto ``scheduler_run`` via ``task_id``.
  Idempotent (terminal rows untouched, guarded by ``mark_terminal``).

Cross-tenant dispatch is impossible by construction: every payload carries its
own row's tenant, and rows with a missing tenant are skipped with a warning.
"""

from __future__ import annotations

import logging
import uuid
from datetime import UTC, datetime
from typing import Any

logger = logging.getLogger(__name__)

#: P0 job types planned through fan-out (never bare-fired).
FORECAST_PLAN_JOB_TYPES = frozenset({"trendx_train", "trendx_forecast"})

#: P0 job ids covered by fan-out.
FORECAST_PLAN_JOB_IDS = frozenset({"forecast-train", "forecast-run"})

#: Job id of the reconciler (direct mode, 5 min cadence).
RECONCILE_JOB_ID = "reconcile-runs"


def forecast_reference_key(
    *,
    tenant_id: str,
    entity_id: str,
    metric_name: str,
    job_type: str,
    window: str,
) -> str:
    """Deterministic traceability key (NOT a DB uniqueness guarantee)."""
    return f"{job_type}:{tenant_id}:{entity_id}:{metric_name}:{window}"


def window_bucket(*, job_id: str, at: datetime | None = None) -> str:
    """Stable window label for a P0 fire (hourly/daily bucket, UTC)."""
    moment = at or datetime.now(UTC)
    if job_id == "forecast-train":
        return moment.strftime("%Y-%m-%d")
    return moment.strftime("%Y-%m-%dT%H")


def iter_forecast_pairs(session: Any) -> list[dict[str, str]]:
    """Enumerate deduplicated tenant-scoped (entity, metric) pairs.

    Rows with a missing/blank tenant are skipped with a warning (no
    cross-tenant dispatch, no silent fabrication). ``entity_type`` is the
    constant ``"DEVICE"``, mirroring the ingestion contract.
    """
    from trendx.database.repositories import (
        BusinessEntityRepository,
        MetricDefinitionRepository,
    )

    entities = BusinessEntityRepository(session).list()
    pairs: dict[tuple[str, str, str], dict[str, str]] = {}
    skipped_no_tenant = 0
    for ent in entities:
        tenant = str(getattr(ent, "tenant_id", "") or "").strip()
        if not tenant:
            skipped_no_tenant += 1
            continue
        entity_id = str(ent.id)
        metrics = MetricDefinitionRepository(session).find_by_business_entity(ent.id)
        for metric in metrics:
            key = (tenant, entity_id, str(metric.item_name))
            pairs.setdefault(
                key,
                {
                    "tenant_id": tenant,
                    "entity_type": "DEVICE",
                    "entity_id": entity_id,
                    "metric_name": str(metric.item_name),
                },
            )
    if skipped_no_tenant:
        logger.warning(
            "forecast fan-out: skipped %d entities with missing tenant",
            skipped_no_tenant,
        )
    if not pairs:
        logger.warning("forecast fan-out: catalogue holds no eligible (entity, metric) pair")
    return list(pairs.values())


def build_forecast_payload(
    *,
    pair: dict[str, str],
    job_type: str,
    window: str,
    parent_run_id: str | None = None,
    overrides: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Complete, explicitly typed task payload for one (entity, metric) pair."""
    if job_type not in FORECAST_PLAN_JOB_TYPES:
        raise ValueError(f"unsupported fan-out job_type: {job_type!r}")
    for required in ("tenant_id", "entity_id", "metric_name"):
        if not pair.get(required):
            raise ValueError(f"forecast fan-out pair misses {required!r}")
    payload: dict[str, Any] = {
        "tenant_id": pair["tenant_id"],
        "entity_type": pair.get("entity_type", "DEVICE"),
        "entity_id": pair["entity_id"],
        "metric_name": pair["metric_name"],
        "job_type": job_type,
        "name": f"scheduler-{job_type}-{pair['entity_id'][:8]}-{pair['metric_name']}",
        "reference_key": forecast_reference_key(
            tenant_id=pair["tenant_id"],
            entity_id=pair["entity_id"],
            metric_name=pair["metric_name"],
            job_type=job_type,
            window=window,
        ),
        "window": window,
    }
    if parent_run_id is not None:
        payload["parent_run_id"] = parent_run_id
    if overrides:
        payload.update(overrides)
    return payload


def fanout_enqueue_forecast_jobs(
    *,
    run_store: Any,
    create_task: Any,
    session_factory: Any,
    instance_id: str,
    job_id: str,
    job_type: str,
    window: str | None = None,
    parent_run_id: str | None = None,
    overrides: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Fan out one P0 forecast fire into per-pair runs + tasks.

    For every eligible pair: ``begin_run`` (mode ``enqueue``) then task
    creation with the complete payload. Returns a summary; the caller
    finalizes the parent trigger row. Empty catalogue -> explicit
    ``{"status": "ok", "fanned_out": 0}`` (mirrors the ingestion empty path).
    """
    if job_type not in FORECAST_PLAN_JOB_TYPES:
        raise ValueError(f"unsupported fan-out job_type: {job_type!r}")
    fire_window = window or window_bucket(job_id=job_id)
    fanned_out = 0
    skipped = 0
    with session_factory() as session:
        pairs = iter_forecast_pairs(session)
    for pair in pairs:
        try:
            payload = build_forecast_payload(
                pair=pair,
                job_type=job_type,
                window=fire_window,
                parent_run_id=parent_run_id,
                overrides=overrides,
            )
        except ValueError as exc:
            logger.warning("forecast fan-out: skipping pair: %s", exc)
            skipped += 1
            continue
        task = create_task(
            name=payload["name"],
            job_type=job_type,
            json_job=payload,
            reference_key=payload["reference_key"],
        )
        run_store.begin_run(
            run_id=str(uuid.uuid4()),
            job_id=job_id,
            job_type=job_type,
            instance_id=instance_id,
            mode="enqueue",
            task_id=str(task.id),
        )
        fanned_out += 1
    return {"status": "ok", "fanned_out": fanned_out, "skipped": skipped, "window": fire_window}


def _latest_terminal_execution(session: Any, task_id: str) -> Any | None:
    """Newest terminal (FINISHED/FAILED) execution for a task, else None."""
    from sqlalchemy import select
    from trendx.database.models import TrendzTaskExecution

    rows = session.scalars(
        select(TrendzTaskExecution)
        .where(TrendzTaskExecution.task_id == uuid.UUID(str(task_id)))
        .order_by(TrendzTaskExecution.finish_ts.desc())
    ).all()
    for row in rows:
        if row.status in ("FINISHED", "FAILED"):
            return row
    return None


def reconcile_runs(
    *,
    run_store: Any,
    session_factory: Any,
    job_ids: Any = None,
    limit: int = 500,
) -> dict[str, Any]:
    """Map terminal worker executions back onto non-terminal scheduler runs.

    Idempotent: terminal rows are never touched (``mark_terminal`` refuses
    non-running rows, so a second pass is a no-op);
    a scheduler crash simply resumes at the next tick. A worker FAILURE is
    propagated as a scheduler ``failed`` run (never rewritten as success).
    Missing task -> documented ``failed``; no terminal execution yet -> left
    ``running``.
    """
    from sqlalchemy import select
    from trendx.database.models import SchedulerRun

    reconciled = 0
    left_running = 0
    with session_factory() as session:
        stmt = select(SchedulerRun).where(SchedulerRun.status == "running")
        if job_ids is not None:
            stmt = stmt.where(SchedulerRun.job_id.in_(list(job_ids)))
        rows = session.scalars(stmt.order_by(SchedulerRun.triggered_at).limit(limit)).all()
        for row in rows:
            if not row.task_id:
                continue
            execution = _latest_terminal_execution(session, str(row.task_id))
            if execution is None:
                left_running += 1
                continue
            if execution.status == "FINISHED":
                run_store.finish_run(run_id=str(row.run_id), status="ok")
            else:
                error = ""
                try:
                    import json as _json

                    payload = _json.loads(execution.json_result or "{}")
                    error = str(payload.get("error", ""))[:2000]
                except Exception:
                    error = ""
                run_store.finish_run(
                    run_id=str(row.run_id),
                    status="failed",
                    error=f"worker execution {execution.status}: {error}".strip(),
                )
            reconciled += 1
    return {"status": "ok", "reconciled": reconciled, "left_running": left_running}


def reconcile_runs_job(
    json_job: dict[str, Any] | None = None,
    task_id: str | None = None,
    execution_id: str | None = None,
) -> dict[str, Any]:
    """Direct-mode P0 handler for ``reconcile-runs`` (5 min cadence)."""
    from trendx.database.connection import manager as db_manager
    from trendx.scheduler.persistence import DbRunStore

    store = DbRunStore()
    return reconcile_runs(
        run_store=store,
        session_factory=lambda: db_manager.get_session("catalog"),
    )
