"""Leader election + heartbeat over PostgreSQL (MR-3, library only).

Single-leader guarantee rests on ONE atomic primitive::

    SELECT pg_try_advisory_lock(hashtextextended(lock_name, 0))

executed on a DEDICATED connection held for the whole leadership tenure
(never a pooled checkout that could be returned to the pool). Crash or
session death releases the lock automatically server-side: a standby can
take over without any intervention.

The ``scheduler_heartbeat`` table is observability + self-fencing, NEVER the
election decision: several rows may transiently show ``leader=true`` during a
failover. On ANY disagreement, the instance is non-leader (fail-closed).

No production activation here: this module only elects and heartbeats.
Scheduling decisions belong to the runtime fencing layer (opt-in).

Timezone: heartbeats are written through the repository (timestamptz, UTC
tz-aware) ; staleness is evaluated against UTC now.
"""

from __future__ import annotations

import socket
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any, Protocol
from uuid import uuid4

from loguru import logger


def _positive_int(value: Any, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{name} must be a positive int, got {value!r}")
    result: int = value
    return result


def _non_empty_str(value: Any, name: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise ValueError(f"{name} must be a non-empty string")
    return text


class HeartbeatWriter(Protocol):
    """Contrat d'écriture heartbeat (argument keywords uniquement)."""

    def __call__(self, *, instance_id: str, leader: bool, version: str, host: str) -> None: ...


class LeaderElection:
    """PostgreSQL advisory-lock leader election with heartbeat.

    Parameters are validated fail-closed at construction. ``connect`` and
    ``heartbeat_writer`` are injectable seams (real PostgreSQL in production
    and integration tests, fakes in unit tests) ; defaults wire the catalog
    database and the heartbeat repository.
    """

    def __init__(
        self,
        *,
        lock_name: str,
        instance_id: str | None = None,
        heartbeat_seconds: int = 15,
        stale_seconds: int = 60,
        acquire_timeout_seconds: int = 5,
        connect: Callable[[], Any] | None = None,
        heartbeat_writer: HeartbeatWriter | None = None,
        version: str = "",
        host: str | None = None,
    ) -> None:
        self._lock_name = _non_empty_str(lock_name, "lock_name")
        self._heartbeat_seconds = _positive_int(heartbeat_seconds, "heartbeat_seconds")
        self._stale_seconds = _positive_int(stale_seconds, "stale_seconds")
        if self._stale_seconds <= self._heartbeat_seconds:
            raise ValueError(
                "stale_seconds must be strictly greater than heartbeat_seconds"
                f" (got stale={self._stale_seconds}, heartbeat={self._heartbeat_seconds})"
            )
        self._acquire_timeout = _positive_int(acquire_timeout_seconds, "acquire_timeout_seconds")
        resolved = str(instance_id or "").strip()
        self._instance_id = resolved or f"{socket.gethostname()}-{uuid4().hex[:8]}"
        self._connect = connect or (lambda: _default_connect(self._acquire_timeout))
        self._heartbeat_writer = heartbeat_writer or _default_heartbeat_writer
        self._version = version
        self._host = host if host is not None else socket.gethostname()
        self._conn: Any | None = None
        self._leader = False
        self._last_heartbeat: datetime | None = None
        self.last_error: str | None = None

    # ── introspection ──────────────────────────────────────────────────

    @property
    def instance_id(self) -> str:
        return self._instance_id

    @property
    def lock_name(self) -> str:
        return self._lock_name

    def is_leader(self) -> bool:
        """Local leadership flag. Cheap check ; freshness is renew()'s job."""
        return self._leader

    def is_stale(self, *, now: datetime | None = None, ttl_seconds: int | None = None) -> bool:
        """True when no heartbeat was ever published or it is older than TTL."""
        moment = now or datetime.now(UTC)
        if moment.tzinfo is None:
            moment = moment.replace(tzinfo=UTC)
        last = self._last_heartbeat
        if last is None:
            return True
        if last.tzinfo is None:
            last = last.replace(tzinfo=UTC)
        ttl = (
            self._stale_seconds
            if ttl_seconds is None
            else _positive_int(ttl_seconds, "ttl_seconds")
        )
        return (moment - last).total_seconds() > ttl

    # ── election ───────────────────────────────────────────────────────

    def acquire(self) -> bool:
        """Try to become leader (atomic single statement, exactly one winner).

        Returns True only when the advisory lock was granted AND the leader
        heartbeat was published. Any failure (lock refused, PostgreSQL error,
        heartbeat impossible) leaves the instance non-leader. Idempotent:
        calling acquire() while already leader revalidates without
        re-locking (pg_try_advisory_lock is re-entrant per session, so a
        second call would inflate the lock count).
        """
        if self._leader and self._conn_alive():
            return True
        try:
            conn = self._ensure_conn()
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT pg_try_advisory_lock(hashtextextended(%s, 0))",
                    (self._lock_name,),
                )
                row = cur.fetchone()
            granted = bool(row and row[0])
        except Exception as exc:
            self.last_error = f"{type(exc).__name__}: {exc}"
            self._drop_conn()
            self._leader = False
            return False
        if not granted:
            self._leader = False
            self.last_error = None
            self._write_heartbeat(leader=False)
            return False
        self._leader = True
        self.last_error = None
        try:
            self._write_heartbeat(leader=True)
        except Exception as exc:
            # Lock held but heartbeat impossible: uncertain state -> release
            # everything (fail-closed) rather than leading blind.
            self.last_error = f"{type(exc).__name__}: {exc}"
            logger.error("Leader heartbeat failed during acquire, stepping down: {}", exc)
            self._drop_conn()
            self._leader = False
            return False
        return True

    def renew(self) -> bool:
        """Leader liveness + heartbeat. False steps down (non-leader).

        renewal never calls pg_try_advisory_lock (re-entrant per session:
        it would inflate the lock count). Liveness = dedicated connection
        alive ; freshness = heartbeat published now.
        """
        if not self._leader or not self._conn_alive():
            self._leader = False
            return False
        conn = self._conn
        if conn is None:  # defensive: _conn_alive just passed, never silent
            self._leader = False
            return False
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                cur.fetchone()
            self._write_heartbeat(leader=True)
        except Exception as exc:
            self.last_error = f"{type(exc).__name__}: {exc}"
            logger.error("Leader renew failed, stepping down: {}", exc)
            self._drop_conn()
            self._leader = False
            return False
        return True

    def tick(self) -> bool:
        """One election-loop iteration with exactly one heartbeat published.

        Leader -> renew() ; follower -> acquire() attempt. Returns the
        resulting leadership state.
        """
        if self._leader:
            return self.renew()
        return self.acquire()

    def release(self) -> None:
        """Step down cleanly: publish leader=false (best effort), unlock,
        close the dedicated connection. Idempotent."""
        try:
            self._write_heartbeat(leader=False)
        except Exception as exc:
            logger.error("Heartbeat publish on release failed (best effort): {}", exc)
        try:
            if self._conn is not None and not self._conn_closed():
                with self._conn.cursor() as cur:
                    cur.execute("SELECT pg_advisory_unlock_all()")
        except Exception as exc:
            logger.error("Advisory unlock on release failed (session close frees it): {}", exc)
        finally:
            self._drop_conn()
            self._leader = False

    # ── internals ──────────────────────────────────────────────────────

    def _ensure_conn(self) -> Any:
        if self._conn is None or self._conn_closed():
            conn = self._connect()
            conn.autocommit = True
            self._conn = conn
        conn = self._conn
        if conn is None:  # pragma: no cover - defensive, connect() returned None
            raise RuntimeError("Leader dedicated connection could not be established")
        return conn

    def _conn_closed(self) -> bool:
        try:
            return bool(self._conn.closed) if self._conn is not None else True
        except Exception:
            return True

    def _conn_alive(self) -> bool:
        return self._conn is not None and not self._conn_closed()

    def _drop_conn(self) -> None:
        conn, self._conn = self._conn, None
        if conn is not None:
            try:
                conn.close()
            except Exception as exc:
                logger.error("Dedicated leader connection close failed: {}", exc)

    def _write_heartbeat(self, *, leader: bool) -> None:
        self._heartbeat_writer(
            instance_id=self._instance_id,
            leader=leader,
            version=self._version,
            host=self._host,
        )
        self._last_heartbeat = datetime.now(UTC)


def _default_connect(connect_timeout: int) -> Any:
    import psycopg2
    from trendx.config import settings

    conn = psycopg2.connect(settings.catalog_dsn_app(), connect_timeout=connect_timeout)
    conn.autocommit = True
    return conn


def _default_heartbeat_writer(*, instance_id: str, leader: bool, version: str, host: str) -> None:
    from trendx.scheduler.persistence import DbRunStore

    DbRunStore().record_heartbeat(
        instance_id=instance_id, leader=leader, version=version, host=host
    )
