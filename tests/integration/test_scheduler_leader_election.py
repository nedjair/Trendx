"""Integration tests for MR-3 leader election on real PostgreSQL.

Proves, against a disposable database (migrations 000+014), what unit fakes
cannot: exactly one advisory-lock winner under real concurrency, crash-safe
lock release on session death, heartbeat rows, takeover, UTC timestamps,
follower fencing and absence of observable split-brain.

Never production, never writeback, never alarms.
"""

from __future__ import annotations

import os
import subprocess
import threading
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import quote

import pytest

ROOT = Path(__file__).parents[2]
MIGS = (
    "000_service_accounts.sql",
    "014_scheduler_runs.sql",
)

pytestmark = pytest.mark.integration

LOCK = "itest-scheduler-leader"


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
def lead_env(provisioned_postgres):
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
    return mgr, params


def _connect(params: dict[str, object]):  # type: ignore[no-untyped-def]
    import psycopg2

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


def _election(mgr, params, instance_id: str, **kwargs: Any):  # type: ignore[no-untyped-def]
    from trendx.scheduler.leader import LeaderElection
    from trendx.scheduler.persistence import DbRunStore

    store = DbRunStore(session_factory=lambda: mgr.get_session("catalog"))
    return LeaderElection(
        lock_name=LOCK,
        instance_id=instance_id,
        connect=lambda: _connect(params),
        heartbeat_writer=lambda *, instance_id, leader, version, host: store.record_heartbeat(
            instance_id=instance_id, leader=leader, version=version, host=host
        ),
        **kwargs,
    )


def _heartbeat_rows(mgr):  # type: ignore[no-untyped-def]
    from sqlalchemy import text

    with mgr.get_session("catalog") as session:
        return session.execute(
            text(
                "SELECT instance_id, leader, heartbeat_ts, version, host"
                " FROM trendx_catalog.scheduler_heartbeat ORDER BY instance_id"
            )
        ).fetchall()


# ── basic election ──────────────────────────────────────────────────────────


def test_single_instance_acquires(lead_env) -> None:
    mgr, params = lead_env
    el = _election(mgr, params, "solo-1")
    assert el.acquire() is True
    assert el.is_leader() is True
    rows = _heartbeat_rows(mgr)
    assert len(rows) == 1 and rows[0][0] == "solo-1" and rows[0][1] is True
    el.release()


def test_second_instance_cannot_acquire(lead_env) -> None:
    mgr, params = lead_env
    first = _election(mgr, params, "first-1")
    second = _election(mgr, params, "second-1")
    assert first.acquire() is True
    assert second.acquire() is False
    assert second.is_leader() is False
    rows = {r[0]: r[1] for r in _heartbeat_rows(mgr)}
    assert rows == {"first-1": True, "second-1": False}
    first.release()
    second.release()


def test_leader_heartbeat_row_is_fresh_and_tz_aware(lead_env) -> None:
    mgr, params = lead_env
    el = _election(mgr, params, "hb-1", version="v9", host="h9")
    assert el.acquire() is True
    assert el.renew() is True
    rows = _heartbeat_rows(mgr)
    assert len(rows) == 1
    _, leader, ts, version, host = rows[0]
    assert leader is True and version == "v9" and host == "h9"
    assert ts.tzinfo is not None
    assert abs((datetime.now(UTC) - ts).total_seconds()) < 30
    el.release()


def test_release_then_takeover(lead_env) -> None:
    mgr, params = lead_env
    first = _election(mgr, params, "take-a")
    second = _election(mgr, params, "take-b")
    assert first.acquire() is True
    assert second.acquire() is False
    first.release()
    assert second.acquire() is True
    assert second.is_leader() is True
    second.release()


# ── crash safety ────────────────────────────────────────────────────────────


def test_session_death_frees_lock_and_allows_takeover(lead_env) -> None:
    mgr, params = lead_env
    crashed = _election(mgr, params, "crash-1")
    taker = _election(mgr, params, "taker-1")
    assert crashed.acquire() is True
    # Simulate a crash: kill the session WITHOUT release (row stays leader=true).
    assert crashed._conn is not None
    crashed._conn.close()
    crashed._conn = None
    assert crashed.renew() is False  # stepped down, uncertain -> non-leader
    assert crashed.is_leader() is False
    # Server-side session death freed the lock: takeover succeeds.
    assert taker.acquire() is True
    taker.release()


def test_recovery_reconnects_and_reacquires(lead_env) -> None:
    mgr, params = lead_env
    el = _election(mgr, params, "rec-1")
    assert el.acquire() is True
    assert el._conn is not None
    el._conn.close()
    el._conn = None
    assert el.renew() is False
    assert el.acquire() is True  # fresh connection, lock free
    assert el.is_leader() is True
    el.release()


def test_stale_leader_row_never_confers_leadership(lead_env) -> None:
    mgr, params = lead_env
    stale_holder = _election(mgr, params, "stale-id")
    assert stale_holder.acquire() is True
    # Crash without step-down publish: row keeps leader=true (transient state).
    assert stale_holder._conn is not None
    stale_holder._conn.close()
    stale_holder._conn = None
    rows = {r[0]: r[1] for r in _heartbeat_rows(mgr)}
    assert rows.get("stale-id") is True

    live = _election(mgr, params, "live-id")
    assert live.acquire() is True  # lock truth, not table truth
    # A resurrected election reusing the stale id still needs the lock.
    ghost = _election(mgr, params, "stale-id")
    assert ghost.acquire() is False
    assert ghost.is_leader() is False
    live.release()
    ghost.release()


# ── concurrency: exactly one winner, no split-brain ─────────────────────────


def _run_competition_round(mgr, params, round_no: int, racers: int) -> None:  # type: ignore[no-untyped-def]
    """One contention round: racers acquire, main asserts tenure, then release.

    Module-level helper (not a loop closure) so lint B023 cannot misfire:
    all shared state arrives as explicit arguments.
    """
    from sqlalchemy import text

    start_barrier = threading.Barrier(racers)
    check_barrier = threading.Barrier(racers + 1)  # +1: main thread asserts tenure
    release_barrier = threading.Barrier(racers + 1)  # assert strictly before release
    winners: list[str] = []
    errors: list[str] = []

    def _race(tag: str) -> None:
        el = _election(mgr, params, f"r{round_no}-{tag}")
        try:
            start_barrier.wait(timeout=30)
            if el.acquire():
                winners.append(el.instance_id)
            check_barrier.wait(timeout=30)  # hold tenure while main asserts
            release_barrier.wait(timeout=30)  # wait until main asserted
        except Exception as exc:  # pragma: no cover - must not happen
            errors.append(f"{type(exc).__name__}: {exc}")
        finally:
            try:
                el.release()
            except Exception:  # pragma: no cover - best effort
                pass

    threads = [threading.Thread(target=_race, args=(f"t{i}",)) for i in range(racers)]
    for t in threads:
        t.start()
    check_barrier.wait(timeout=60)  # all racers acquired, tenure held
    assert errors == [], f"round {round_no}: {errors}"
    assert len(winners) == 1, f"round {round_no}: winners={winners} (split-brain?)"

    # Observable truth DURING tenure: exactly one fresh leader=true row.
    with mgr.get_session("catalog") as session:
        fresh = session.execute(
            text(
                "SELECT instance_id FROM trendx_catalog.scheduler_heartbeat"
                " WHERE leader = TRUE AND heartbeat_ts > now() - make_interval(secs => 120)"
            )
        ).fetchall()
        if [r[0] for r in fresh] != winners:
            everything = session.execute(
                text(
                    "SELECT instance_id, leader, heartbeat_ts, now() - heartbeat_ts AS age"
                    " FROM trendx_catalog.scheduler_heartbeat ORDER BY instance_id"
                )
            ).fetchall()
    assert [
        r[0] for r in fresh
    ] == winners, f"round {round_no}: fresh={fresh} winners={winners} all={everything}"
    release_barrier.wait(timeout=60)  # allow release only after assertion

    for t in threads:
        t.join(timeout=60)
    assert not any(t.is_alive() for t in threads), "racer thread hung"


def test_concurrent_acquisition_single_winner_no_split_brain(lead_env) -> None:
    mgr, _params = lead_env
    rounds, racers = 20, 4
    for round_no in range(rounds):
        _run_competition_round(mgr, _params, round_no, racers)

    assert rounds == 20  # 20 consecutive single-winner rounds


# ── fencing: follower executes nothing ──────────────────────────────────────


def test_follower_schedules_nothing_with_require_leadership(lead_env) -> None:
    from trendx.scheduler.jobs import JobSpec
    from trendx.scheduler.registry import SchedulerRegistry
    from trendx.scheduler.runtime import RuntimeScheduler

    mgr, params = lead_env
    specs = (JobSpec("ingestion-run", "ingestion", "1h", "ingestion"),)
    reg = SchedulerRegistry(specs=specs)
    calls: list[Any] = []
    reg.register_handler("ingestion", lambda p: calls.append(p) or {"status": "ok"})

    leader = _election(mgr, params, "sched-lead")
    follower = _election(mgr, params, "sched-follow")
    assert leader.acquire() is True
    assert follower.acquire() is False

    rt_follow = RuntimeScheduler(reg, require_leadership=True, election=follower)
    import pytest as _pytest

    with _pytest.raises(RuntimeError, match="not the scheduler leader"):
        rt_follow.schedule_all({})
    assert calls == []

    rt_lead = RuntimeScheduler(reg, require_leadership=True, election=leader)
    assert rt_lead.schedule_all({}) == ["ingestion-run"]
    assert rt_lead.run_job_now("ingestion-run", {})["status"] == "ok"
    assert len(calls) == 1
    leader.release()
    follower.release()


def test_single_effective_leader_over_window(lead_env) -> None:
    mgr, params = lead_env
    contenders = [_election(mgr, params, f"w-{i}") for i in range(3)]
    results = [el.acquire() for el in contenders]
    assert results.count(True) == 1
    winner = contenders[results.index(True)]
    assert winner.is_leader() is True
    assert all(not el.is_leader() for el in contenders if el is not winner)
    for el in contenders:
        el.release()
