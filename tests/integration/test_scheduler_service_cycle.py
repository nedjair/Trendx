"""Integration tests for the MR-4 scheduler service on disposable PostgreSQL.

No mocks of MR-2/MR-3 components: real LeaderElection (advisory locks),
real DbRunStore, real RuntimeScheduler with fencing, real P0 adaptation
(`adapt_p0_handler`). Only the workload itself is synthetic (a trivial
3-arg handler) ; the full P0 business chain is already covered by the B1
suites. Never production, never writeback, never alarms.
"""

from __future__ import annotations

import os
import subprocess
import time
import urllib.request
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from urllib.parse import quote

import pytest

ROOT = Path(__file__).parents[2]
MIGS = (
    "000_service_accounts.sql",
    "014_scheduler_runs.sql",
)

pytestmark = pytest.mark.integration

ONCE_SPECS = None  # built per-test (unique job ids avoid cross-test leakage)


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
def svc_env(provisioned_postgres, monkeypatch):
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
    # DbRunStore default factory reads this module global at call time.
    monkeypatch.setattr("trendx.scheduler.persistence.db_manager", mgr, raising=True)
    return mgr, params


def _settings(**overrides: Any) -> SimpleNamespace:
    base = {
        "trendx_scheduler_enabled": True,
        "trendx_scheduler_instance_id": "",
        "trendx_scheduler_heartbeat_seconds": 15,
        "trendx_scheduler_stale_seconds": 60,
        "trendx_scheduler_acquire_timeout_seconds": 5,
        "trendx_scheduler_leader_lock": "it-cycle-lock",
        "tb_writeback_enabled": False,
        "tb_alarms_enabled": False,
    }
    base.update(overrides)
    return SimpleNamespace(**base)


def _election(mgr, params, instance_id: str, lock: str = "it-cycle-lock"):  # type: ignore[no-untyped-def]
    import psycopg2
    from trendx.scheduler.leader import LeaderElection
    from trendx.scheduler.persistence import DbRunStore

    def _connect():  # type: ignore[no-untyped-def]
        conn = psycopg2.connect(
            host=str(params["host"]),
            port=int(str(params["port"])),
            dbname=str(params["dbname"]),
            user=str(params["user"]),
            password=str(params["password"]),
            connect_timeout=5,
        )
        conn.autocommit = True
        return conn

    store = DbRunStore(session_factory=lambda: mgr.get_session("catalog"))
    return LeaderElection(
        lock_name=lock,
        instance_id=instance_id,
        connect=_connect,
        heartbeat_writer=lambda *, instance_id, leader, version, host: store.record_heartbeat(
            instance_id=instance_id, leader=leader, version=version, host=host
        ),
    )


def _trivial_fn(json_job: Any, task_id: Any, execution_id: Any) -> dict[str, Any]:
    return {"status": "ok", "echo": sorted(json_job)[:3]}


def _specs(tag: str):  # type: ignore[no-untyped-def]
    from trendx.scheduler.jobs import JobSpec

    return (JobSpec(f"it-once-{tag}", "ingestion", "once", "integration probe"),)


def _service(mgr, params, tag: str, **overrides: Any):  # type: ignore[no-untyped-def]
    from trendx.scheduler.service import SchedulerService

    settings_obj = _settings(**overrides)
    election = _election(
        mgr,
        params,
        f"it-{tag}",
        lock=overrides.get("lock", "it-cycle-lock"),
    )
    return SchedulerService(
        settings_obj=settings_obj,
        specs=_specs(tag),
        handlers={"ingestion": _trivial_fn},
        election=election,
        metrics_port=0,
    )


def _wait_for(predicate, timeout: float = 20.0):  # type: ignore[no-untyped-def]
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.2)
    return predicate()


def _http_get(port: int, path: str) -> tuple[int, str]:
    import urllib.error as _urlerror

    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{port}{path}", timeout=5) as resp:
            return resp.status, resp.read().decode()
    except _urlerror.HTTPError as exc:
        return exc.code, ""


# ── full cycle ──────────────────────────────────────────────────────────────


def test_full_cycle_start_schedule_run_heartbeat_metrics_shutdown(svc_env) -> None:
    from sqlalchemy import text

    mgr, params = svc_env
    svc = _service(mgr, params, "full")
    try:
        assert svc.start() == "leader"
        assert svc.is_leader is True

        # The "once" job fires through the fenced runtime into persistence.
        assert _wait_for(lambda: len(svc._registry.records()) >= 1), "job never fired"
        with mgr.get_session("catalog") as session:
            rows = session.execute(
                text(
                    "SELECT job_id, status, finished_at, mode FROM" " trendx_catalog.scheduler_run"
                )
            ).fetchall()
        assert len(rows) == 1
        assert rows[0][1] == "ok" and rows[0][2] is not None and rows[0][3] == "direct"

        with mgr.get_session("catalog") as session:
            hb = session.execute(
                text(
                    "SELECT leader FROM trendx_catalog.scheduler_heartbeat"
                    " WHERE instance_id = 'it-full'"
                )
            ).fetchall()
        assert hb and hb[0][0] is True

        port = svc._httpd.server_address[1]
        code, body = _http_get(port, "/healthz")
        assert code == 200 and '"status": "leader"' in body
        code, body = _http_get(port, "/metrics")
        assert code == 200 and "scheduler_leader 1" in body
    finally:
        svc.stop()
        svc.stop()  # idempotent, silent

    with mgr.get_session("catalog") as session:
        hb = session.execute(
            text(
                "SELECT leader FROM trendx_catalog.scheduler_heartbeat"
                " WHERE instance_id = 'it-full'"
            )
        ).fetchall()
    assert hb and hb[0][0] is False  # step-down published
    assert svc._runtime.is_running() is False


# ── leader + standby + takeover ─────────────────────────────────────────────


def test_leader_standby_takeover(svc_env) -> None:
    mgr, params = svc_env
    first = _service(mgr, params, "a", lock="it-takeover")
    second = _service(mgr, params, "b", lock="it-takeover")
    try:
        assert first.start() == "leader"
        assert second.start() == "standby"
        assert second._registry.records() == []
        assert second.health()["status"] == "standby"

        first.stop()  # leader steps down, lock freed
        assert _wait_for(lambda: second.run_once() == "leader", timeout=30.0)
        assert second.is_leader is True
    finally:
        first.stop()
        second.stop()


def test_leader_loss_freezes_scheduling(svc_env) -> None:
    mgr, params = svc_env
    svc = _service(mgr, params, "loss", lock="it-loss")
    try:
        assert svc.start() == "leader"
        assert svc._election._conn is not None
        svc._election._conn.close()  # kill session: crash simulation
        svc._election._conn = None
        assert svc.run_once() == "standby"
        assert svc._runtime.is_running() is False
        assert svc.health()["status"] in ("standby", "degraded")
    finally:
        svc.stop()


def test_db_failure_is_fail_closed_then_recoverable(svc_env) -> None:
    mgr, params = svc_env
    bad = _service(
        mgr,
        params,
        "bad",
        lock="it-recover",
        trendx_scheduler_instance_id="it-bad",
    )
    # Break only this instance's connection path (wrong port).
    import psycopg2 as _pg

    real_connect = _pg.connect

    def _dead(**kwargs: Any):  # type: ignore[no-untyped-def]
        return real_connect(
            host=str(params["host"]),
            port=1,
            dbname=str(params["dbname"]),
            user=str(params["user"]),
            password=str(params["password"]),
            connect_timeout=2,
        )

    bad._election._connect = _dead  # type: ignore[method-assign]
    try:
        # Uncertain leadership surfaces as degraded (never leader).
        assert bad.start() in ("standby", "degraded")
        assert bad.is_leader is False
        assert bad._registry.records() == []
        assert bad.health()["status"] in ("standby", "degraded")
        assert bad.health()["leader"] is False
    finally:
        bad.stop()

    # Recovery: a fresh instance against the live database leads.
    good = _service(mgr, params, "good", lock="it-recover")
    try:
        assert good.start() == "leader"
    finally:
        good.stop()


def test_two_services_never_schedule_twice(svc_env) -> None:
    mgr, params = svc_env
    lock = "it-nosplit"
    services = [_service(mgr, params, f"n{i}", lock=lock) for i in range(3)]
    try:
        states = [svc.start() for svc in services]
        assert states.count("leader") == 1
        leaders = [svc for svc in services if svc.is_leader]
        assert len(leaders) == 1
        scheduled = [svc for svc in services if svc._scheduled]
        assert scheduled == leaders
    finally:
        for svc in services:
            svc.stop()
