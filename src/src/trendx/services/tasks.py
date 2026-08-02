from __future__ import annotations

import time
import uuid
from datetime import datetime, timezone
from typing import Any, Optional

from loguru import logger
from sqlalchemy import text

from trendx.database.connection import manager as db_manager
from trendx.database.models import TrendzTask
from trendx.database.repositories import TrendzTaskRepository


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

    def _get_session_and_repo(self):
        session = next(db_manager.get_session("catalog"))
        repo = TrendzTaskRepository(session)
        return session, repo

    def create_task(
        self,
        name: str,
        job_type: str,
        json_job: dict[str, Any],
        schedule_type: str = "NOT_SCHEDULED",
        reference_type: str = "MANUAL",
        reference_key: str | None = None,
    ) -> TrendzTask:
        session, repo = self._get_session_and_repo()
        task = repo.create(
            name=name,
            tenant_id=None,
            customer_id=None,
            user_id=None,
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
        session.commit()
        logger.info("Created task {} (job_type={})", task.id, job_type)
        return task

    def claim_task(
        self,
        worker_id: str,
        job_type_filter: str | None = None,
    ) -> TrendzTask | None:
        # TODO: Real task claiming uses trendz_task_execution_request + state records.
        # Simplified stub for backward compatibility.
        return None

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
        task_id: str,
        result: Any = None,
    ) -> TrendzTask | None:
        return None

    def fail_task(
        self,
        task_id: str,
        error_message: str,
    ) -> TrendzTask | None:
        return None

    def cancel_task(self, task_id: str) -> TrendzTask | None:
        session, repo = self._get_session_and_repo()
        task = repo.get(task_id)
        if task is None:
            session.close()
            return None
        task.enabled = False
        session.commit()
        logger.info("Task {} cancelled", task_id)
        session.close()
        return task

    def retry_task(self, task_id: str) -> TrendzTask | None:
        session, repo = self._get_session_and_repo()
        task = repo.get(task_id)
        if task is not None:
            task.enabled = True
            session.commit()
            logger.info("Task {} retried", task_id)
        session.close()
        return task

    def list_tasks(
        self,
        status: str | None = None,
        job_type: str | None = None,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        session = next(db_manager.get_session("catalog"))
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
        session.close()
        return result

    def get_task(self, task_id: str) -> dict[str, Any] | None:
        session, repo = self._get_session_and_repo()
        task = repo.get(task_id)
        session.close()
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
