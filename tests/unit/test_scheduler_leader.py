"""Unit tests for MR-3 leader election / heartbeat (no database).

A FakeConn stands in for psycopg2 (lock results, liveness, failures are
scripted explicitly) and heartbeat writes are recorded. Real mutual
exclusion is NOT proven here on purpose: it is demonstrated against real
PostgreSQL, with repeated concurrent trials, in
tests/integration/test_scheduler_leader_election.py.
"""

from __future__ import annotations

import socket
from datetime import UTC, datetime, timedelta
from typing import Any
from unittest.mock import MagicMock

import pytest
from psycopg2 import OperationalError
from trendx.scheduler.jobs import JobSpec
from trendx.scheduler.leader import LeaderElection
from trendx.scheduler.registry import SchedulerRegistry
from trendx.scheduler.runtime import RuntimeScheduler


class FakeCursor:
    def __init__(self, conn: FakeConn) -> None:
        self._conn = conn
        self._row: tuple[Any, ...] | None = None

    def __enter__(self) -> FakeCursor:
        return self

    def __exit__(self, *exc: object) -> None:
        return None

    def execute(self, sql: str, params: Any = None) -> None:
        self._conn.statements.append(str(sql))
        if "pg_try_advisory_lock" in str(sql):
            if "lock" in self._conn.fail_on or not self._conn.alive:
                raise OperationalError("lock exploded")
            self._conn.lock_calls += 1
            self._row = (self._conn.lock_result,)
        elif "pg_advisory_unlock_all" in str(sql):
            if "unlock" in self._conn.fail_on:
                raise OperationalError("unlock exploded")
            self._conn.unlock_calls += 1
            self._row = (True,)
        elif str(sql).strip() == "SELECT 1":
            if not self._conn.alive or "select" in self._conn.fail_on:
                raise OperationalError("connection dead")
            self._row = (1,)
        else:  # pragma: no cover - unexpected statement
            raise AssertionError(f"unexpected SQL in fake: {sql!r}")

    def fetchone(self) -> tuple[Any, ...] | None:
        return self._row


class FakeConn:
    def __init__(
        self,
        *,
        lock_result: bool = True,
        alive: bool = True,
        fail_on: set[str] | None = None,
    ) -> None:
        self.lock_result = lock_result
        self.alive = alive
        self.fail_on = fail_on or set()
        self.autocommit = False
        self.closed = 0
        self.lock_calls = 0
        self.unlock_calls = 0
        self.statements: list[str] = []

    def cursor(self) -> FakeCursor:
        if self.closed:
            raise OperationalError("connection closed")
        return FakeCursor(self)

    def close(self) -> None:
        self.closed = 1


class Heartbeats:
    def __init__(self) -> None:
        self.calls: list[tuple[str, bool, str, str]] = []

    def __call__(self, *, instance_id: str, leader: bool, version: str, host: str) -> None:
        self.calls.append((instance_id, leader, version, host))


def _election(
    conn: FakeConn, hb: Heartbeats, instance_id: str = "i-test", **kwargs: Any
) -> LeaderElection:
    return LeaderElection(
        lock_name="test-lock",
        instance_id=instance_id,
        connect=lambda: conn,
        heartbeat_writer=hb,
        **kwargs,
    )


# ── acquire ───────────────────────────────────────────────────────────────


def test_acquire_success_publishes_leader_heartbeat() -> None:
    conn, hb = FakeConn(lock_result=True), Heartbeats()
    el = _election(conn, hb)
    assert el.acquire() is True
    assert el.is_leader() is True
    assert hb.calls == [("i-test", True, "", socket.gethostname())]
    assert conn.autocommit is True
    assert any("pg_try_advisory_lock" in s for s in conn.statements)


def test_acquire_refused_publishes_follower_heartbeat() -> None:
    conn, hb = FakeConn(lock_result=False), Heartbeats()
    el = _election(conn, hb)
    assert el.acquire() is False
    assert el.is_leader() is False
    assert hb.calls == [("i-test", False, "", socket.gethostname())]


def test_acquire_is_idempotent_without_relocking() -> None:
    conn, hb = FakeConn(lock_result=True), Heartbeats()
    el = _election(conn, hb)
    assert el.acquire() is True
    assert el.acquire() is True
    assert conn.lock_calls == 1  # no re-entrant lock inflation


# ── renew / release ───────────────────────────────────────────────────────


def test_renew_keeps_leadership_and_heartbeats() -> None:
    conn, hb = FakeConn(lock_result=True), Heartbeats()
    el = _election(conn, hb)
    assert el.acquire() is True
    assert el.renew() is True
    assert el.is_leader() is True
    assert [c[1] for c in hb.calls] == [True, True]
    assert conn.lock_calls == 1  # renew never re-locks


def test_renew_without_leadership_does_no_io() -> None:
    conn, hb = FakeConn(), Heartbeats()
    el = _election(conn, hb)
    assert el.renew() is False
    assert conn.statements == []
    assert hb.calls == []


def test_renew_on_dead_connection_steps_down() -> None:
    conn, hb = FakeConn(lock_result=True), Heartbeats()
    el = _election(conn, hb)
    assert el.acquire() is True
    conn.alive = False
    assert el.renew() is False
    assert el.is_leader() is False
    assert conn.closed == 1  # session dropped -> lock auto-released server-side


def test_release_unlocks_closes_and_publishes_stepdown() -> None:
    conn, hb = FakeConn(lock_result=True), Heartbeats()
    el = _election(conn, hb)
    assert el.acquire() is True
    el.release()
    assert el.is_leader() is False
    assert conn.unlock_calls == 1
    assert conn.closed == 1
    assert hb.calls[-1][1] is False
    el.release()  # idempotent, no error
    assert conn.unlock_calls == 1


# ── stale / instance ──────────────────────────────────────────────────────


def test_is_stale_never_fresh_and_old() -> None:
    conn, hb = FakeConn(), Heartbeats()
    el = _election(conn, hb)
    assert el.is_stale() is True  # never heartbeated
    el._last_heartbeat = datetime.now(UTC)
    assert el.is_stale() is False
    el._last_heartbeat = datetime.now(UTC) - timedelta(seconds=3600)
    assert el.is_stale() is True
    assert el.is_stale(ttl_seconds=7200) is False
    with pytest.raises(ValueError):
        el.is_stale(ttl_seconds=0)


def test_instance_id_explicit_and_auto() -> None:
    conn, hb = FakeConn(), Heartbeats()
    assert _election(conn, hb, instance_id="fixed-1").instance_id == "fixed-1"
    auto = LeaderElection(lock_name="l", connect=lambda: conn, heartbeat_writer=hb)
    assert auto.instance_id.startswith(socket.gethostname() + "-")
    other = LeaderElection(lock_name="l", connect=lambda: conn, heartbeat_writer=hb)
    assert auto.instance_id != other.instance_id  # fresh id per process object


# ── config validation ─────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "kwargs",
    [
        {"lock_name": ""},
        {"lock_name": "   "},
        {"lock_name": "l", "heartbeat_seconds": 0},
        {"lock_name": "l", "heartbeat_seconds": -3},
        {"lock_name": "l", "stale_seconds": 0},
        {"lock_name": "l", "heartbeat_seconds": 60, "stale_seconds": 60},
        {"lock_name": "l", "heartbeat_seconds": 60, "stale_seconds": 30},
        {"lock_name": "l", "acquire_timeout_seconds": 0},
    ],
)
def test_invalid_config_fails_closed(kwargs: Any) -> None:
    conn, hb = FakeConn(), Heartbeats()
    with pytest.raises(ValueError):
        LeaderElection(connect=lambda: conn, heartbeat_writer=hb, **kwargs)


def test_safe_defaults_do_not_enable_production() -> None:
    from trendx.config import settings

    assert settings.trendx_scheduler_enabled is False
    assert settings.trendx_scheduler_heartbeat_seconds == 15
    assert settings.trendx_scheduler_stale_seconds == 60
    assert settings.trendx_scheduler_acquire_timeout_seconds == 5
    assert settings.trendx_scheduler_leader_lock == "trendx_scheduler_leader"


# ── fail-closed ───────────────────────────────────────────────────────────


def test_connect_failure_is_non_leader_with_last_error() -> None:
    def _boom() -> Any:
        raise OperationalError("pg down")

    hb = Heartbeats()
    el = LeaderElection(lock_name="l", connect=_boom, heartbeat_writer=hb)
    assert el.acquire() is False
    assert el.is_leader() is False
    assert el.last_error is not None and "OperationalError" in el.last_error
    assert hb.calls == []


def test_heartbeat_failure_during_acquire_releases_everything() -> None:
    conn = FakeConn(lock_result=True)

    def _boom(*, instance_id: str, leader: bool, version: str, host: str) -> None:
        raise OperationalError("heartbeat down")

    el = _election(conn, _boom)  # type: ignore[arg-type]
    assert el.acquire() is False
    assert el.is_leader() is False
    assert conn.closed == 1  # session dropped -> lock freed
    assert el.last_error is not None


# ── tick ──────────────────────────────────────────────────────────────────


def test_tick_publishes_exactly_one_heartbeat() -> None:
    conn, hb = FakeConn(lock_result=True), Heartbeats()
    el = _election(conn, hb)
    assert el.tick() is True  # follower -> acquire
    assert len(hb.calls) == 1
    assert el.tick() is True  # leader -> renew
    assert len(hb.calls) == 2


def test_tick_follower_stays_follower() -> None:
    conn, hb = FakeConn(lock_result=False), Heartbeats()
    el = _election(conn, hb)
    assert el.tick() is False
    assert el.tick() is False
    assert [c[1] for c in hb.calls] == [False, False]


# ── runtime fencing ───────────────────────────────────────────────────────


def _runtime(*, leader: bool, **kwargs: Any) -> RuntimeScheduler:
    election = MagicMock()
    election.is_leader.return_value = leader
    election.renew.return_value = leader
    specs = (JobSpec("ingestion-run", "ingestion", "1h", "ingestion"),)
    reg = SchedulerRegistry(specs=specs)
    return RuntimeScheduler(reg, require_leadership=True, election=election, **kwargs)


def test_require_leadership_needs_explicit_election() -> None:
    with pytest.raises(ValueError):
        RuntimeScheduler(require_leadership=True, election=None)


def test_schedule_all_refused_without_leadership() -> None:
    rt = _runtime(leader=False)
    rt.registry.register_handler("ingestion", lambda payload: {"status": "ok"})
    with pytest.raises(RuntimeError, match="not the scheduler leader"):
        rt.schedule_all({})
    assert rt.scheduled_ids() == []


def test_run_job_now_refused_without_leadership() -> None:
    rt = _runtime(leader=False)
    with pytest.raises(RuntimeError, match="not the scheduler leader"):
        rt.run_job_now("ingestion-run", {})


def test_schedule_all_allowed_with_leadership() -> None:
    rt = _runtime(leader=True)
    rt.registry.register_handler("ingestion", lambda payload: {"status": "ok"})
    assert rt.schedule_all({}) == ["ingestion-run"]


def test_fire_after_stepdown_executes_nothing() -> None:
    election = MagicMock()
    election.is_leader.return_value = True
    election.renew.return_value = True
    specs = (JobSpec("ingestion-run", "ingestion", "1h", "ingestion"),)
    reg = SchedulerRegistry(specs=specs)
    calls: list[Any] = []
    reg.register_handler("ingestion", lambda payload: calls.append(payload) or {"ok": True})
    rt = RuntimeScheduler(reg, require_leadership=True, election=election)
    assert rt.schedule_all({}) == ["ingestion-run"]

    job = rt._scheduler.get_job("ingestion-run")
    assert job is not None
    election.is_leader.return_value = False  # step down after scheduling
    out = job.func()
    assert out["status"] == "refused-not-leader"
    assert calls == []

    election.is_leader.return_value = True
    out = job.func()
    assert calls != []


def test_default_runtime_behavior_unchanged() -> None:
    specs = (JobSpec("ingestion-run", "ingestion", "1h", "ingestion"),)
    reg = SchedulerRegistry(specs=specs)
    reg.register_handler("ingestion", lambda payload: {"status": "ok"})
    rt = RuntimeScheduler(reg)  # require_leadership=False by default
    assert rt.schedule_all({}) == ["ingestion-run"]
    assert rt.run_job_now("ingestion-run", {})["status"] == "ok"


# ── isolation ─────────────────────────────────────────────────────────────


def test_leader_module_has_no_worker_import() -> None:
    import ast as _ast
    from pathlib import Path as _Path

    path = _Path(__file__).parents[2] / "src" / "trendx" / "scheduler" / "leader.py"
    assert path.exists(), f"leader module not found at {path}"
    tree = _ast.parse(path.read_text(), filename=str(path))
    for node in _ast.walk(tree):
        if isinstance(node, _ast.Import):
            mods = [a.name for a in node.names]
        elif isinstance(node, _ast.ImportFrom):
            mods = [node.module or ""]
        else:
            continue
        assert not any(
            m == "trendx.services.worker" or m.startswith("trendx.services.worker.") for m in mods
        ), mods
