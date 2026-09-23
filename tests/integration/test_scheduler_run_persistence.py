"""Integration tests for scheduler run persistence on disposable PostgreSQL.

Covers, against a real database (migrations 000+001+014 applied):
- DbRunStore lifecycle: begin running -> terminal ok / failed + error ;
- "fail" normalization to "failed" through the store ;
- heartbeat upsert (leader flip) ;
- stale running search (old direct found, recent excluded, enqueue excluded
  by default and included on request) ;
- purge_before (old terminal deleted, running and recent kept) ;
- SchedulerRegistry.trigger() with a real DbRunStore (ok + failed paths,
  exception propagation, in-memory records preserved) ;
- enqueue path with real TaskService (task created, row running with task_id,
  outcome enqueued, handler NOT executed in-process).

Requires a disposable PostgreSQL (provisioned_postgres fixture). Never
production, never writeback, never alarms.
"""

from __future__ import annotations

import os
import subprocess
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import quote

import pytest

ROOT = Path(__file__).parents[2]
MIGS = (
    "000_service_accounts.sql",
    "001_trendz_native_schema.sql",
    "014_scheduler_runs.sql",
)

pytestmark = pytest.mark.integration


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
def sched_env(provisioned_postgres, monkeypatch):
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
    # TaskService lit les ids par défaut sur le singleton settings déjà
    # construit (setenv inopérant ici) : on patch les attributs du module.
    import trendx.services.tasks as _tasks_mod

    monkeypatch.setattr(_tasks_mod.settings, "trendx_default_tenant_id", str(uuid.uuid4()))
    monkeypatch.setattr(_tasks_mod.settings, "trendx_default_customer_id", str(uuid.uuid4()))
    monkeypatch.setattr(_tasks_mod.settings, "trendx_default_user_id", str(uuid.uuid4()))
    # TaskService utilise le manager global de son propre module.
    monkeypatch.setattr("trendx.services.tasks.db_manager", mgr, raising=True)
    return mgr


def _store(mgr):  # type: ignore[no-untyped-def]
    from trendx.scheduler.persistence import DbRunStore

    return DbRunStore(session_factory=lambda: mgr.get_session("catalog"))


def _row(mgr, run_id: str):  # type: ignore[no-untyped-def]
    from sqlalchemy import text

    with mgr.get_session("catalog") as session:
        return session.execute(
            text(
                "SELECT run_id, job_id, job_type, status, triggered_at,"
                " finished_at, error, instance_id, mode, task_id"
                " FROM trendx_catalog.scheduler_run WHERE run_id = :rid"
            ),
            {"rid": run_id},
        ).fetchone()


# ── DbRunStore lifecycle ────────────────────────────────────────────────────


def test_store_begin_then_ok(sched_env) -> None:
    store = _store(sched_env)
    run_id = str(uuid.uuid4())
    store.begin_run(
        run_id=run_id,
        job_id="ingestion-run",
        job_type="ingestion",
        instance_id="i-1",
        mode="direct",
    )
    row = _row(sched_env, run_id)
    assert row is not None and row[3] == "running" and row[5] is None
    store.finish_run(run_id=run_id, status="ok")
    row = _row(sched_env, run_id)
    assert row[3] == "ok" and row[5] is not None and row[6] is None


def test_store_begin_then_failed_with_error(sched_env) -> None:
    store = _store(sched_env)
    run_id = str(uuid.uuid4())
    store.begin_run(
        run_id=run_id,
        job_id="forecast-train",
        job_type="trendx_train",
        instance_id="i-1",
        mode="direct",
    )
    store.finish_run(run_id=run_id, status="failed", error="ValueError: boom")
    row = _row(sched_env, run_id)
    assert row[3] == "failed" and row[6] == "ValueError: boom" and row[5] is not None


def test_store_normalizes_fail_to_failed(sched_env) -> None:
    store = _store(sched_env)
    run_id = str(uuid.uuid4())
    store.begin_run(
        run_id=run_id,
        job_id="ingestion-run",
        job_type="ingestion",
        instance_id="i-1",
        mode="direct",
    )
    store.finish_run(run_id=run_id, status="fail", error="RuntimeError: x")
    assert _row(sched_env, run_id)[3] == "failed"


def test_store_rejects_unknown_terminal_status(sched_env) -> None:
    from trendx.database.repositories import SchedulerRunRepository

    store = _store(sched_env)
    run_id = str(uuid.uuid4())
    store.begin_run(
        run_id=run_id,
        job_id="ingestion-run",
        job_type="ingestion",
        instance_id="i-1",
        mode="direct",
    )
    # finish_run journalise sans masquer : la ligne reste running.
    store.finish_run(run_id=run_id, status="no_data")
    assert _row(sched_env, run_id)[3] == "running"
    # Au niveau repository, le statut inconnu est une erreur fail-closed.
    with sched_env.get_session("catalog") as session:
        with pytest.raises(ValueError):
            SchedulerRunRepository(session).mark_terminal(run_id, "no_data")


def test_store_rejects_double_terminal(sched_env) -> None:
    from trendx.database.repositories import SchedulerRunRepository

    store = _store(sched_env)
    run_id = str(uuid.uuid4())
    store.begin_run(
        run_id=run_id,
        job_id="ingestion-run",
        job_type="ingestion",
        instance_id="i-1",
        mode="direct",
    )
    store.finish_run(run_id=run_id, status="ok")
    with sched_env.get_session("catalog") as session:
        with pytest.raises(ValueError):
            SchedulerRunRepository(session).mark_terminal(run_id, "failed")


# ── heartbeat ───────────────────────────────────────────────────────────────


def test_heartbeat_upsert_and_leader_flip(sched_env) -> None:
    from sqlalchemy import text

    store = _store(sched_env)
    store.record_heartbeat(instance_id="i-1", leader=False, version="v1", host="h1")
    store.record_heartbeat(instance_id="i-1", leader=True, version="v2", host="h1")
    with sched_env.get_session("catalog") as session:
        rows = session.execute(
            text(
                "SELECT instance_id, leader, heartbeat_ts, version, host"
                " FROM trendx_catalog.scheduler_heartbeat"
            )
        ).fetchall()
    assert len(rows) == 1  # upsert, pas de doublon
    assert rows[0][0] == "i-1" and rows[0][1] is True and rows[0][3] == "v2"


# ── stale + purge ───────────────────────────────────────────────────────────


def test_stale_and_purge_semantics(sched_env) -> None:
    from trendx.database.repositories import SchedulerRunRepository

    now = datetime.now(UTC)
    old = now - timedelta(days=200)
    recent = now - timedelta(minutes=5)
    with sched_env.get_session("catalog") as session:
        repo = SchedulerRunRepository(session)
        r_old_ok = repo.create_run(
            run_id=str(uuid.uuid4()),
            job_id="j1",
            job_type="ingestion",
            instance_id="i-1",
            triggered_at=old,
        )
        repo.mark_terminal(r_old_ok.run_id, "ok")
        r_old_run = repo.create_run(
            run_id=str(uuid.uuid4()),
            job_id="j2",
            job_type="ingestion",
            instance_id="i-1",
            triggered_at=old,
        )
        r_old_enq = repo.create_run(
            run_id=str(uuid.uuid4()),
            job_id="j3",
            job_type="trendx_train",
            instance_id="i-1",
            mode="enqueue",
            triggered_at=old,
        )
        r_new_ok = repo.create_run(
            run_id=str(uuid.uuid4()),
            job_id="j4",
            job_type="ingestion",
            instance_id="i-1",
            triggered_at=recent,
        )
        repo.mark_terminal(r_new_ok.run_id, "ok")
        session.commit()

        stale = repo.list_stale_running(now - timedelta(hours=1))
        stale_ids = {str(r.run_id) for r in stale}
        assert str(r_old_run.run_id) in stale_ids  # vieux running direct
        assert str(r_old_enq.run_id) not in stale_ids  # enqueue exclu par défaut
        assert str(r_old_ok.run_id) not in stale_ids  # terminal exclu
        enclosed = repo.list_stale_running(now - timedelta(hours=1), modes=("direct", "enqueue"))
        assert str(r_old_enq.run_id) in {str(r.run_id) for r in enclosed}

        cutoff = now - timedelta(days=180)
        deleted = repo.purge_before(cutoff)
        assert deleted == 1  # seul le vieux ok ; running et récent conservés
        session.commit()

        remaining = {str(r.run_id) for r in repo.list()}
        assert str(r_old_ok.run_id) not in remaining
        assert str(r_old_run.run_id) in remaining
        assert str(r_old_enq.run_id) in remaining
        assert str(r_new_ok.run_id) in remaining


# ── trigger() with real store ───────────────────────────────────────────────


def test_trigger_persists_ok_end_to_end(sched_env) -> None:
    from trendx.scheduler.jobs import JobSpec
    from trendx.scheduler.registry import SchedulerRegistry

    specs = (JobSpec("ingestion-run", "ingestion", "1h", "ingestion"),)
    reg = SchedulerRegistry(specs=specs, run_store=_store(sched_env), instance_id="i-9")
    reg.register_handler("ingestion", lambda payload: {"status": "ok", "tasks": 1})
    out = reg.trigger("ingestion-run", {"tenant_id": "t"})

    assert out["status"] == "ok"
    row = _row(sched_env, out["run_id"])
    assert row is not None and row[3] == "ok" and row[7] == "i-9" and row[8] == "direct"


def test_trigger_persists_failed_and_reraises(sched_env) -> None:
    from trendx.scheduler.jobs import JobSpec
    from trendx.scheduler.registry import SchedulerRegistry

    specs = (JobSpec("ingestion-run", "ingestion", "1h", "ingestion"),)
    reg = SchedulerRegistry(specs=specs, run_store=_store(sched_env), instance_id="i-9")

    def boom(payload: dict[str, Any]) -> dict[str, Any]:
        raise RuntimeError("kaput")

    reg.register_handler("ingestion", boom)
    with pytest.raises(RuntimeError, match="kaput"):
        reg.trigger("ingestion-run", {"tenant_id": "t"})

    from sqlalchemy import text

    with sched_env.get_session("catalog") as session:
        rows = session.execute(
            text("SELECT status, error FROM trendx_catalog.scheduler_run")
        ).fetchall()
    assert len(rows) == 1 and rows[0][0] == "failed" and "kaput" in rows[0][1]


def test_trigger_enqueue_with_real_task_service(sched_env) -> None:
    from trendx.scheduler.jobs import JobSpec
    from trendx.scheduler.persistence import adapt_p0_handler
    from trendx.scheduler.registry import SchedulerRegistry

    called: list[Any] = []

    def fn(json_job: Any, task_id: Any, execution_id: Any) -> dict[str, Any]:
        called.append((json_job, task_id, execution_id))
        return {"status": "ok"}  # pragma: no cover - jamais exécuté en enqueue

    specs = (JobSpec("forecast-train", "trendx_train", "1d", "training"),)
    reg = SchedulerRegistry(specs=specs, run_store=_store(sched_env), instance_id="i-9")
    reg.register_handler("trendx_train", adapt_p0_handler(fn, mode="enqueue"))
    out = reg.trigger(
        "forecast-train",
        {"job_type": "trendx_train", "tenant_id": "t", "scheduler_mode": "enqueue"},
    )

    assert out["status"] == "enqueued"
    assert called == []  # pas d'exécution in-process
    row = _row(sched_env, out["run_id"])
    assert row is not None and row[3] == "running" and row[8] == "enqueue"
    assert row[9] is not None and str(row[9]) == out["task_id"]

    # La tâche existe réellement et reste claimable par le worker.
    from sqlalchemy import text

    with sched_env.get_session("catalog") as session:
        found = session.execute(
            text("SELECT id, job_type FROM trendx_catalog.trendz_task WHERE id = :tid"),
            {"tid": out["task_id"]},
        ).fetchone()
    assert found is not None and found[1] == "trendx_train"
