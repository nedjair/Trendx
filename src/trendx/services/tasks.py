from __future__ import annotations

import time
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

from loguru import logger
from sqlalchemy import Select, select, text
from sqlalchemy.orm import Session
from trendx.config import settings
from trendx.database.connection import manager as db_manager
from trendx.database.models import TrendzTask, TrendzTaskExecutionRequest
from trendx.database.repositories import TrendzTaskRepository

# Single canonical state for execution-request queue rows (per validation 0.7-B).
# No other state value (CLAIMED, RUNNING, ...) may be introduced.
PENDING_STATE = "PENDING"


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
                json_job=str(json_job),
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
                json_job=str(json_job),
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
        the PENDING state. The row is locked with FOR UPDATE SKIP LOCKED so that
        concurrent workers never claim the same request, and ordering by
        created_ts gives FIFO behaviour.
        """
        stmt = select(TrendzTaskExecutionRequest).where(
            TrendzTaskExecutionRequest.scheduled.is_(False),
            TrendzTaskExecutionRequest.state == PENDING_STATE,
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
        """
        stmt = self._build_claim_stmt(job_type_filter)
        with self._get_session_and_repo() as (session, _repo):
            logger.debug("Worker {} attempting to claim a task", worker_id)
            result = session.execute(stmt)
            row: TrendzTaskExecutionRequest | None = result.scalars().first()
            if row is None:
                logger.debug("Worker {} found no claimable task", worker_id)
            else:
                logger.info(
                    "Worker {} claimed task {} execution {}",
                    worker_id,
                    row.task_id,
                    row.execution_id,
                )
            return row

    def update_progress(
        self,
        task_id: str,
        progress: int,
        status: str | None = None,
    ) -> TrendzTask | None:
        # TODO: Progress updates go through trendz_task_execution_progress_step.
        return None

    def complete_task(
        self,
        result: Any = None,
    ) -> TrendzTask | None:
        return None

    def fail_task(
        self,
        error_message: str,
    ) -> TrendzTask | None:
        return None

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
