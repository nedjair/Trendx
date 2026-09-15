"""Integration tests for forecast fan-out + reconciliation on disposable PostgreSQL.

Real tables (000/001/014 applied), real TaskService task creation, real
DbRunStore runs, synthetic catalogue rows. Worker executions are inserted
directly to document the terminal-state mapping (no worker process needed);
the claim path itself is covered by the B1 worker suites. Never production.
"""

from __future__ import annotations

import os
import subprocess
import uuid
from pathlib import Path
from typing import Any

import pytest

ROOT = Path(__file__).parents[2]
MIGS = (
    "000_service_accounts.sql",
    "001_trendz_native_schema.sql",
    "014_scheduler_runs.sql",
)

pytestmark = pytest.mark.integration

TENANT_A = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
TENANT_B = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"


def _apply(params: dict[str, object], filename: str) -> None:
    env = dict(os.environ)
    env["PGHOST"] = str(params["host"])
    env["PGPORT"] = str(params["port"])
    env["PGUSER"] = str(params["user"])
    env["PGDATABASE"] = str(params["dbname"])
    env["PGPASSWORD"] = str(params["password"])
    subprocess.run(
        ["psql", "-v", "ON_ERROR_STOP=1", "-X", "-q", "-f", str(ROOT / "migrations" / filename)],
        env=env,
        check=True,
        capture_output=True,
        text=True,
    )


@pytest.fixture
def fenv(provisioned_postgres, monkeypatch):
    from urllib.parse import quote

    from trendx.database.connection import DatabaseManager

    params = provisioned_postgres
    for f in MIGS:
        _apply(params, f)
    dsn = (
        f"postgresql://{params['user']}:{quote(str(params['password']))}"
        f"@{params['host']}:{params['port']}/{params['dbname']}"
    )
    mgr = DatabaseManager()
    mgr.register("catalog", dsn, search_path="trendx_catalog,public")
    monkeypatch.setattr("trendx.scheduler.persistence.db_manager", mgr, raising=True)
    monkeypatch.setattr("trendx.services.tasks.db_manager", mgr, raising=True)
    from trendx.config import settings as _settings

    monkeypatch.setattr(_settings, "trendx_default_tenant_id", TENANT_A)
    monkeypatch.setattr(_settings, "trendx_default_customer_id", TENANT_A)
    monkeypatch.setattr(_settings, "trendx_default_user_id", TENANT_A)
    return mgr, params


def _seed_catalogue(mgr) -> dict[str, Any]:
    from trendx.database.models import BusinessEntity, MetricDefinition

    now_ms = 1700000000000
    with mgr.get_session("catalog") as session:
        e1 = BusinessEntity(
            id=uuid.uuid4(),
            name="e1",
            tenant_id=uuid.UUID(TENANT_A),
            hidden=False,
            shared_with_customers=False,
        )
        e2 = BusinessEntity(
            id=uuid.uuid4(),
            name="e2",
            tenant_id=uuid.UUID(TENANT_B),
            hidden=False,
            shared_with_customers=False,
        )
        e3 = BusinessEntity(
            id=uuid.uuid4(),
            name="e3-no-tenant",
            tenant_id=None,
            hidden=False,
            shared_with_customers=False,
        )
        session.add_all([e1, e2, e3])
        session.flush()
        metrics = [
            MetricDefinition(
                business_entity_id=e1.id,
                item_id=uuid.uuid4(),
                item_name="temperature",
                tenant_id=uuid.UUID(TENANT_A),
                name="m1",
                user_input="u",
                description="d",
                how_to_calculate="direct",
                created_ts=now_ms,
                updated_ts=now_ms,
            ),
            MetricDefinition(
                business_entity_id=e1.id,
                item_id=uuid.uuid4(),
                item_name="humidity",
                tenant_id=uuid.UUID(TENANT_A),
                name="m2",
                user_input="u",
                description="d",
                how_to_calculate="direct",
                created_ts=now_ms,
                updated_ts=now_ms,
            ),
            MetricDefinition(
                business_entity_id=e2.id,
                item_id=uuid.uuid4(),
                item_name="temperature",
                tenant_id=uuid.UUID(TENANT_B),
                name="m3",
                user_input="u",
                description="d",
                how_to_calculate="direct",
                created_ts=now_ms,
                updated_ts=now_ms,
            ),
        ]
        session.add_all(metrics)
        session.commit()
        return {"e1": str(e1.id), "e2": str(e2.id)}


def _finish_execution(mgr, task_id: str, status: str, error: str = "") -> None:
    import json as _json
    import time

    from trendx.database.models import TrendzTaskExecution

    with mgr.get_session("catalog") as session:
        session.add(
            TrendzTaskExecution(
                task_id=uuid.UUID(task_id),
                tenant_id=uuid.UUID(TENANT_A),
                customer_id=uuid.UUID(TENANT_A),
                user_id=uuid.UUID(TENANT_A),
                status=status,
                created_ts=int(time.time() * 1000),
                start_ts=int(time.time() * 1000),
                finish_ts=int(time.time() * 1000),
                duration=1,
                json_progress_content="{}",
                job_type="trendx_forecast",
                json_job="{}",
                json_result=_json.dumps({"error": error} if error else {}),
            )
        )
        session.commit()


def test_plan_to_task_to_terminal_run(fenv) -> None:
    from trendx.scheduler.fanout import fanout_enqueue_forecast_jobs, reconcile_runs
    from trendx.scheduler.persistence import DbRunStore
    from trendx.services.tasks import TaskService

    mgr, _ = fenv
    seeded = _seed_catalogue(mgr)
    store = DbRunStore()
    out = fanout_enqueue_forecast_jobs(
        run_store=store,
        create_task=TaskService().create_task,
        session_factory=lambda: mgr.get_session("catalog"),
        instance_id="it-1",
        job_id="forecast-run",
        job_type="trendx_forecast",
        window="2026-01-01T00",
    )
    assert out["status"] == "ok" and out["fanned_out"] == 3
    # Per-pair runs exist, all running, typed tasks created.
    from trendx.database.models import SchedulerRun, TrendzTask

    with mgr.get_session("catalog") as session:
        runs = session.query(SchedulerRun).filter_by(job_id="forecast-run").all()
        assert len(runs) == 3 and all(r.status == "running" for r in runs)
        tasks = session.query(TrendzTask).all()
        assert len(tasks) == 3
        payloads = [__import__("json").loads(t.json_job) for t in tasks]
        assert {p["job_type"] for p in payloads} == {"trendx_forecast"}
        assert all(p["tenant_id"] and p["entity_id"] and p["metric_name"] for p in payloads)
        # Tenant separation: e1 pairs carry TENANT_A, e2 pair TENANT_B.
        by_entity = {p["entity_id"]: p["tenant_id"] for p in payloads}
        assert by_entity[seeded["e1"]] == TENANT_A
        assert by_entity[seeded["e2"]] == TENANT_B
    # Simulate worker outcomes: 2 FINISHED, 1 FAILED.
    with mgr.get_session("catalog") as session:
        task_ids = [str(t.id) for t in session.query(TrendzTask).all()]
    _finish_execution(mgr, task_ids[0], "FINISHED")
    _finish_execution(mgr, task_ids[1], "FINISHED")
    _finish_execution(mgr, task_ids[2], "FAILED", "boom")
    result = reconcile_runs(run_store=store, session_factory=lambda: mgr.get_session("catalog"))
    assert result == {"status": "ok", "reconciled": 3, "left_running": 0}
    with mgr.get_session("catalog") as session:
        states = sorted(r.status for r in session.query(SchedulerRun).all())
        assert states == ["failed", "ok", "ok"]
        failed = session.query(SchedulerRun).filter_by(status="failed").one()
        assert "boom" in (failed.error or "")
    # Restart-safe: second pass changes nothing.
    again = reconcile_runs(run_store=store, session_factory=lambda: mgr.get_session("catalog"))
    assert again["reconciled"] == 0


def test_empty_catalogue_fans_out_zero(fenv) -> None:
    from trendx.scheduler.fanout import fanout_enqueue_forecast_jobs
    from trendx.scheduler.persistence import DbRunStore
    from trendx.services.tasks import TaskService

    mgr, _ = fenv
    store = DbRunStore()
    out = fanout_enqueue_forecast_jobs(
        run_store=store,
        create_task=TaskService().create_task,
        session_factory=lambda: mgr.get_session("catalog"),
        instance_id="it-1",
        job_id="forecast-run",
        job_type="trendx_forecast",
    )
    assert out["status"] == "ok" and out["fanned_out"] == 0


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main([__file__, "-v"]))
