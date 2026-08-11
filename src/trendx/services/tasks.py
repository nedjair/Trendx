from __future__ import annotations

import json
import time
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any, cast

from loguru import logger
from sqlalchemy import Select, exists, select, text
from sqlalchemy.orm import Session
from trendx.config import settings
from trendx.database.connection import manager as db_manager
from trendx.database.models import (
    TrendzTask,
    TrendzTaskExecution,
    TrendzTaskExecutionProgressStep,
    TrendzTaskExecutionRequest,
    TrendzTaskExecutionStateRecord,
)
from trendx.database.repositories import TrendzTaskRepository

# Single canonical state for execution-request queue rows (per validation 0.7-B).
# No other state value (CLAIMED, RUNNING, ...) may be introduced.
PENDING_STATE = "PENDING"

# Canonical Trendz execution states for trendz_task_execution.status.
# Source of truth provided for the Task Lifecycle implementation: these are the
# ONLY values permitted. Never introduce COMPLETED, and never use PENDING here
# (PENDING stays the initial, established state of trendz_task_execution_request.state).
EXECUTION_STATUS_CREATED = "CREATED"
EXECUTION_STATUS_RUNNING = "RUNNING"
EXECUTION_STATUS_FINISHED = "FINISHED"
EXECUTION_STATUS_FAILED = "FAILED"
EXECUTION_STATUS_CANCELED = "CANCELED"
EXECUTION_STATUS_LOST = "LOST"
EXECUTION_STATUS_NONE = "NONE"
EXECUTION_STATUSES: frozenset[str] = frozenset(
    {
        EXECUTION_STATUS_CREATED,
        EXECUTION_STATUS_RUNNING,
        EXECUTION_STATUS_FINISHED,
        EXECUTION_STATUS_FAILED,
        EXECUTION_STATUS_CANCELED,
        EXECUTION_STATUS_LOST,
        EXECUTION_STATUS_NONE,
    }
)
# Terminal states: a second complete/fail is a no-op (idempotence).
TERMINAL_EXECUTION_STATUSES: frozenset[str] = frozenset(
    {
        EXECUTION_STATUS_FINISHED,
        EXECUTION_STATUS_FAILED,
        EXECUTION_STATUS_CANCELED,
        EXECUTION_STATUS_LOST,
    }
)

# Canonical Trendz states for trendz_task_execution_state_record.state.
# Only these two values are permitted.
STATE_RECORD_WORKING = "WORKING"
STATE_RECORD_CANCELLED = "CANCELLED"
STATE_RECORD_STATES: frozenset[str] = frozenset({STATE_RECORD_WORKING, STATE_RECORD_CANCELLED})


class TaskService:
    """Task management for Trendx background operations.

    Provides create/claim/update/complete/fail lifecycle with priority
    queues, retries, and progress tracking.

    NOTE: Trendz 1.15.0 real schema uses `job_type` instead of `task_type`,
    and does not have `priority`, `scheduled_at`, `started_at`, `finished_at`,
    `duration_ms`, `claimed_by`, `retry_count`, `max_retries`, `payload`,
    `result`, or `error_message` columns. Task status is managed via
    `trendz_task_execution` and `trendz_task_execution_state_record`.
    """

    def __init__(self) -> None:
        pass

    @contextmanager
    def _get_session_and_repo(self) -> Iterator[tuple[Session, TrendzTaskRepository]]:
        with db_manager.get_session("catalog") as session:
            repo = TrendzTaskRepository(session)
            yield session, repo

    def create_task(
        self,
        name: str,
        job_type: str,
        json_job: dict[str, Any],
        schedule_type: str = "NOT_SCHEDULED",
        reference_type: str = "MANUAL",
        reference_key: str | None = None,
    ) -> TrendzTask:
        tenant_id_str = settings.trendx_default_tenant_id
        customer_id_str = settings.trendx_default_customer_id
        user_id_str = settings.trendx_default_user_id
        if not tenant_id_str or not customer_id_str or not user_id_str:
            msg = (
                "TRENDX_DEFAULT_TENANT_ID / TRENDX_DEFAULT_CUSTOMER_ID / "
                "TRENDX_DEFAULT_USER_ID must be configured to create a task."
            )
            raise ValueError(msg)
        tenant_id = uuid.UUID(tenant_id_str)
        customer_id = uuid.UUID(customer_id_str)
        user_id = uuid.UUID(user_id_str)
        with self._get_session_and_repo() as (session, repo):
            task = repo.create(
                name=name,
                tenant_id=tenant_id,
                customer_id=customer_id,
                user_id=user_id,
                created_ts=int(time.time() * 1000),
                updated_ts=int(time.time() * 1000),
                enabled=True,
                reference_type=reference_type,
                reference_key=reference_key or uuid.uuid4().hex,
                job_type=job_type,
                json_job=json.dumps(json_job),
                schedule_type=schedule_type,
                schedule_period_ts=0,
                schedule_planned_ts=0,
                schedule_scheduling_unit="",
                schedule_scheduling_unit_count=0,
                schedule_scheduling_time_zone="UTC",
                ttl_enabled=False,
                ttl_duration=0,
                store_execution_enabled=False,
                store_execution_count=0,
                json_configs="{}",
            )
            # Populate the execution-request queue row (validation 0.7-B).
            # A task is claimed later via trendz_task_execution_request; the
            # only valid state at creation time is PENDING and it is not
            # scheduled.
            execution_id = uuid.uuid4()
            request = TrendzTaskExecutionRequest(
                task_id=task.id,
                execution_id=execution_id,
                tenant_id=tenant_id,
                customer_id=customer_id,
                user_id=user_id,
                scheduled=False,
                job_type=job_type,
                json_job=json.dumps(json_job),
                created_ts=int(time.time() * 1000),
                state=PENDING_STATE,
            )
            session.add(request)
            session.commit()
            logger.info(
                "Created task {} (job_type={}) and execution request {} state=PENDING",
                task.id,
                job_type,
                execution_id,
            )
            return task

    def _build_claim_stmt(
        self, job_type_filter: str | None = None
    ) -> Select[tuple[TrendzTaskExecutionRequest]]:
        """Build the atomic claim SELECT over trendz_task_execution_request.

        Available tasks are those not scheduled (scheduled=False) and still in
        the PENDING state, for which no execution has been created yet. The row
        is locked with FOR UPDATE SKIP LOCKED so that concurrent workers never
        claim the same request, and ordering by created_ts gives FIFO behaviour.

        The NOT EXISTS guard excludes requests that already have an execution
        (trendz_task_execution.id == request.execution_id): once claimed, a
        request can never be claimed twice, while its state stays PENDING (the
        only valid request state, per 0.7-B).
        """
        stmt = select(TrendzTaskExecutionRequest).where(
            TrendzTaskExecutionRequest.scheduled.is_(False),
            TrendzTaskExecutionRequest.state == PENDING_STATE,
            ~exists().where(TrendzTaskExecution.id == TrendzTaskExecutionRequest.execution_id),
        )
        if job_type_filter is not None:
            stmt = stmt.where(TrendzTaskExecutionRequest.job_type == job_type_filter)
        stmt = stmt.order_by(TrendzTaskExecutionRequest.created_ts.asc())
        stmt = stmt.with_for_update(skip_locked=True)
        return stmt

    def claim_task(
        self,
        worker_id: str,
        job_type_filter: str | None = None,
    ) -> TrendzTaskExecutionRequest | None:
        """Atomically claim one available execution request.

        Returns the claimed TrendzTaskExecutionRequest row, or None when no
        available task exists. The claim is protected against concurrent
        workers by FOR UPDATE SKIP LOCKED (see _build_claim_stmt).

        On claim, the execution lifecycle row is created so that
        trendz_task_execution.id == request.execution_id, with status CREATED,
        and a trendz_task_execution_state_record (state=WORKING). The request
        row keeps its PENDING state (the only valid request state); it is
        excluded from future claims by the NOT EXISTS guard in _build_claim_stmt.
        """
        stmt = self._build_claim_stmt(job_type_filter)
        with self._get_session_and_repo() as (session, _repo):
            logger.debug("Worker {} attempting to claim a task", worker_id)
            result = session.execute(stmt)
            row: TrendzTaskExecutionRequest | None = result.scalars().first()
            if row is None:
                logger.debug("Worker {} found no claimable task", worker_id)
                return None
            now = int(time.time() * 1000)
            execution = TrendzTaskExecution(
                id=row.execution_id,
                task_id=row.task_id,
                tenant_id=row.tenant_id,
                customer_id=row.customer_id,
                user_id=row.user_id,
                status=EXECUTION_STATUS_CREATED,
                created_ts=now,
                start_ts=now,
                finish_ts=0,
                duration=0,
                json_progress_content="{}",
                job_type=row.job_type,
                json_job=row.json_job,
                json_result="{}",
            )
            session.add(execution)
            state_record = TrendzTaskExecutionStateRecord(
                execution_id=row.execution_id,
                state=STATE_RECORD_WORKING,
                last_update_ts=now,
                removed_task=False,
            )
            session.add(state_record)
            session.commit()
            logger.info(
                "Worker {} claimed task {} execution {} (status=CREATED)",
                worker_id,
                row.task_id,
                row.execution_id,
            )
            return row

    def _coerce_uuid(self, value: Any) -> Any:
        """Coerce a task_id/execution_id to a UUID when possible.

        A valid UUID string (the production case) is returned as a uuid.UUID so
        the ORM column comparison is typed. Anything that is not a valid UUID
        (e.g. a test fixture key like "task-1") is returned unchanged so the
        caller can keep using it verbatim.
        """
        if isinstance(value, uuid.UUID):
            return value
        try:
            return uuid.UUID(str(value))
        except (ValueError, TypeError, AttributeError):
            return value

    def _find_execution(self, session: Session, task_id: Any) -> TrendzTaskExecution | None:
        """Resolve the latest trendz_task_execution for a task_id.

        task_id is not unique on trendz_task_execution (a task may have several
        executions across retries), so the most recently created execution is
        returned. Returns None when no execution exists yet.
        """
        stmt = (
            select(TrendzTaskExecution)
            .where(TrendzTaskExecution.task_id == self._coerce_uuid(task_id))
            .order_by(TrendzTaskExecution.created_ts.desc())
            .limit(1)
        )
        result = session.execute(stmt)
        return cast("TrendzTaskExecution | None", result.scalars().first())

    def _touch_state_record(
        self,
        session: Session,
        execution_id: uuid.UUID,
        now: int,
        state: str = STATE_RECORD_WORKING,
    ) -> TrendzTaskExecutionStateRecord:
        """Get-or-create the trendz_task_execution_state_record for an execution.

        state_record.state only ever takes WORKING or CANCELLED (canonical).
        While the execution is in progress this keeps the record at WORKING and
        bumps last_update_ts; it never invents a completion value.
        """
        rec = session.get(TrendzTaskExecutionStateRecord, execution_id)
        if rec is None:
            rec = TrendzTaskExecutionStateRecord(
                execution_id=execution_id,
                state=state,
                last_update_ts=now,
                removed_task=False,
            )
            session.add(rec)
        else:
            rec.last_update_ts = now
        return cast(TrendzTaskExecutionStateRecord, rec)

    def update_progress(
        self,
        task_id: str,
        progress: int,
        status: str | None = None,
    ) -> TrendzTask | None:
        """Record progress for the task's latest execution.

        Persists the percentage into trendz_task_execution.json_progress_content,
        appends a trendz_task_execution_progress_step, and keeps the
        state_record at WORKING. When no explicit status is given, a CREATED/NONE
        execution transitions to RUNNING (the worker has started progressing).
        Returns the parent TrendzTask, or None when no execution exists for the
        task_id.
        """
        if (
            isinstance(progress, bool)
            or not isinstance(progress, int)
            or not (0 <= progress <= 100)
        ):
            raise ValueError("Progress must be between 0 and 100")
        if status is not None and status not in EXECUTION_STATUSES:
            raise ValueError(f"Invalid execution status: {status}")
        now = int(time.time() * 1000)
        with self._get_session_and_repo() as (session, _repo):
            execution = self._find_execution(session, task_id)
            if execution is None:
                logger.debug("update_progress: no execution for task {}", task_id)
                return None
            execution.json_progress_content = json.dumps({"progress": progress, "status": status})
            if status is not None:
                execution.status = status
            elif execution.status in (EXECUTION_STATUS_CREATED, EXECUTION_STATUS_NONE):
                execution.status = EXECUTION_STATUS_RUNNING
            step = TrendzTaskExecutionProgressStep(
                execution_id=execution.id,
                name=f"progress:{progress}",
                start_ts=now,
                finish_ts=now,
            )
            session.add(step)
            self._touch_state_record(session, execution.id, now)
            session.commit()
            logger.info(
                "Task {} progress {}% (status={})",
                task_id,
                progress,
                execution.status,
            )
            return cast(TrendzTask | None, session.get(TrendzTask, task_id))

    def complete_task(
        self,
        task_id: str,
        result: Any = None,
    ) -> TrendzTask | None:
        """Mark the task's latest execution as FINISHED.

        Sets trendz_task_execution.status=FINISHED, records json_result, the
        finish_ts and the duration, and keeps the state_record at WORKING.
        Idempotent: a terminal execution is returned unchanged. Returns the
        parent TrendzTask, or None when no execution exists for the task_id.
        """
        now = int(time.time() * 1000)
        with self._get_session_and_repo() as (session, _repo):
            execution = self._find_execution(session, task_id)
            if execution is None:
                logger.debug("complete_task: no execution for task {}", task_id)
                return None
            if execution.status in TERMINAL_EXECUTION_STATUSES:
                return cast(TrendzTask | None, session.get(TrendzTask, task_id))
            execution.status = EXECUTION_STATUS_FINISHED
            execution.json_result = json.dumps(result) if result is not None else "{}"
            execution.finish_ts = now
            execution.duration = max(0, now - execution.start_ts) if execution.start_ts else 0
            self._touch_state_record(session, execution.id, now)
            session.commit()
            logger.info("Task {} completed (execution {})", task_id, execution.id)
            return cast(TrendzTask | None, session.get(TrendzTask, task_id))

    def fail_task(
        self,
        task_id: str,
        error_message: str,
    ) -> TrendzTask | None:
        """Mark the task's latest execution as FAILED.

        Sets trendz_task_execution.status=FAILED, records the error in
        json_result, the finish_ts and the duration, and keeps the state_record
        at WORKING. Idempotent: a terminal execution is returned unchanged.
        Returns the parent TrendzTask, or None when no execution exists for the
        task_id.
        """
        if not isinstance(error_message, str) or not error_message.strip():
            raise ValueError("error_message is required to fail a task")
        now = int(time.time() * 1000)
        with self._get_session_and_repo() as (session, _repo):
            execution = self._find_execution(session, task_id)
            if execution is None:
                logger.debug("fail_task: no execution for task {}", task_id)
                return None
            if execution.status in TERMINAL_EXECUTION_STATUSES:
                return cast(TrendzTask | None, session.get(TrendzTask, task_id))
            execution.status = EXECUTION_STATUS_FAILED
            execution.json_result = json.dumps({"error": error_message})
            execution.finish_ts = now
            execution.duration = max(0, now - execution.start_ts) if execution.start_ts else 0
            self._touch_state_record(session, execution.id, now)
            session.commit()
            logger.info(
                "Task {} failed (execution {}): {}",
                task_id,
                execution.id,
                error_message,
            )
            return cast(TrendzTask | None, session.get(TrendzTask, task_id))

    def cancel_task(self, task_id: str) -> TrendzTask | None:
        with self._get_session_and_repo() as (session, repo):
            task = repo.get(task_id)
            if task is None:
                return None
            task.enabled = False
            session.commit()
            logger.info("Task {} cancelled", task_id)
            return task

    def retry_task(self, task_id: str) -> TrendzTask | None:
        with self._get_session_and_repo() as (session, repo):
            task = repo.get(task_id)
            if task is not None:
                task.enabled = True
                session.commit()
                logger.info("Task {} retried", task_id)
            return task

    def list_tasks(
        self,
        status: str | None = None,
        job_type: str | None = None,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        with db_manager.get_session("catalog") as session:
            repo = TrendzTaskRepository(session)
            filters: list[Any] = []
            if job_type is not None:
                filters.append(TrendzTask.job_type == job_type)
            tasks = repo.list(*filters, order_by=TrendzTask.created_ts.desc(), limit=limit)
            result = [
                {
                    "id": str(t.id),
                    "name": t.name,
                    "job_type": t.job_type,
                    "enabled": t.enabled,
                    "reference_type": t.reference_type,
                    "reference_key": t.reference_key,
                    "schedule_type": t.schedule_type,
                    "schedule_planned_ts": t.schedule_planned_ts,
                    "tenant_id": str(t.tenant_id) if t.tenant_id else None,
                    "customer_id": str(t.customer_id) if t.customer_id else None,
                    "user_id": str(t.user_id) if t.user_id else None,
                    "created_ts": t.created_ts,
                    "updated_ts": t.updated_ts,
                }
                for t in tasks
            ]
            session.commit()
            return result

    def get_task(self, task_id: str) -> dict[str, Any] | None:
        with db_manager.get_session("catalog") as session:
            repo = TrendzTaskRepository(session)
            task = repo.get(task_id)
            if task is None:
                return None
            return {
                "id": str(task.id),
                "name": task.name,
                "job_type": task.job_type,
                "enabled": task.enabled,
                "reference_type": task.reference_type,
                "reference_key": task.reference_key,
                "schedule_type": task.schedule_type,
                "schedule_planned_ts": task.schedule_planned_ts,
                "tenant_id": str(task.tenant_id) if task.tenant_id else None,
                "customer_id": str(task.customer_id) if task.customer_id else None,
                "user_id": str(task.user_id) if task.user_id else None,
                "created_ts": task.created_ts,
                "updated_ts": task.updated_ts,
            }

    def get_task_logs(self, task_id: str) -> list[dict[str, Any]]:
        # TODO: task_log table does not exist in Trendz 1.15.0 schema.
        # Use trendz_task_execution + trendz_task_execution_progress_step instead.
        logger.warning("get_task_logs called but task_log table does not exist")
        return []

    def count_by_status(self) -> dict[str, int]:
        engine = db_manager.get_engine("catalog")
        stmt = text(
            """
            SELECT status, COUNT(*) as cnt
            FROM trendz_task_execution
            GROUP BY status
            """
        )
        with engine.connect() as conn:
            rows = conn.execute(stmt).fetchall()
        return {row[0]: row[1] for row in rows}
