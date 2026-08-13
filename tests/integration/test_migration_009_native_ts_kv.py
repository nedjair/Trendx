"""Integration test for migration 009 (native ts_kv / ts_kv_latest).

Applies (and exercises) migrations/009_native_ts_kv.sql against a REAL
PostgreSQL instance provided by the CI ``postgres:16-alpine`` service — i.e.
NATIVE PostgreSQL, NO TimescaleDB — and proves:

  * ``trendx_analytics.ts_kv`` and ``trendx_analytics.ts_kv_latest`` are
    actually created by migration 009 (the missing tables that block ingestion
    in the native profile, where migration 002 is a no-op);
  * their columns / primary keys match the contract used by
    ``IngestionService._store_telemetry`` (src/trendx/services/ingestion.py);
  * a REAL ``INSERT`` and ``UPSERT`` (``ON CONFLICT``) works on ``ts_kv``;
  * a REAL ``UPSERT`` works on ``ts_kv_latest``;
  * idempotence: re-inserting the same primary key keeps exactly one row;
  * on native PostgreSQL the table is a DECLARATIVELY PARTITIONED table
    (``relkind = 'p'``), i.e. NOT a TimescaleDB hypertable — per B5
    (TimescaleDB refused). Migration 010 turns the 009 plain heap into
    ``PARTITION BY RANGE(ts)``; the full partitioning/BRIN contract is asserted
    in test_migration_010_partition_ts_kv;
  * the migration file itself is idempotent (``IF NOT EXISTS``) and native-only
    (guarded by ``NOT EXISTS timescaledb``), with no ``create_hypertable`` call.

This test complements (and does not duplicate) test_migration_004_aggregates,
which only validates structure/grants/owner of the aggregates and explicitly
does NOT create ``ts_kv`` (002 is a no-op on native PG). Here we go one step
further and validate the real INSERT/UPSERT path that ingestion depends on.

Selection
---------
Selected by the ``integration`` marker and run inside the shared
``test-integration`` job, which now applies ``001``->``006`` *plus* ``009`` *plus* ``010``.
It is intentionally NOT marked ``migration_apply``: it asserts the post-state of
an already-applied migration rather than re-applying a non-idempotent migration
on a fresh database.
"""

from __future__ import annotations

import os
import re
import uuid
from pathlib import Path

import pytest

# Keep behaviour identical to the CI job (postgres:16-alpine service).
os.environ.setdefault("PG_ADMIN_HOST", "postgres")
os.environ.setdefault("PG_ADMIN_PORT", "5432")
os.environ.setdefault("PG_ADMIN_DB", "trendx")
os.environ.setdefault("PG_ADMIN_USER", "trendx_app")
os.environ.setdefault("PG_ADMIN_PASSWORD", "trendx_app_pass")
os.environ.setdefault("TRENDX_APP_USER", "trendx_app")
os.environ.setdefault("TRENDX_APP_PASSWORD", "trendx_app_pass")
os.environ.setdefault("TRENDX_DB_NAME", "trendx")
os.environ.setdefault("TRENDX_DB_HOST", "postgres")
os.environ.setdefault("TRENDX_DB_PORT", "5432")
os.environ.setdefault("TRENDX_DB_USER", "trendx_app")
os.environ.setdefault("TRENDX_DB_PASSWORD", "trendx_app_pass")

ROOT = Path(__file__).parents[2]
MIGRATIONS = ROOT / "migrations"

pytestmark = [pytest.mark.integration]

SCHEMA = "trendx_analytics"
TS_KV_COLUMNS = {
    "ts",
    "entity_id",
    "metric_key",
    "str_v",
    "long_v",
    "dbl_v",
    "bool_v",
    "json_v",
    "source",
    "ingestion_id",
    "created_at",
}
TS_KV_LATEST_COLUMNS = {
    "entity_id",
    "metric_key",
    "ts",
    "str_v",
    "long_v",
    "dbl_v",
    "bool_v",
    "json_v",
    "source",
    "updated_at",
}


def _pg_conn_params() -> dict[str, str]:
    return {
        "host": os.environ.get("PG_ADMIN_HOST", os.environ.get("TRENDX_DB_HOST", "postgres")),
        "port": int(os.environ.get("PG_ADMIN_PORT", os.environ.get("TRENDX_DB_PORT", "5432"))),
        "dbname": os.environ.get("PG_ADMIN_DB", os.environ.get("TRENDX_DB_NAME", "trendx")),
        "user": os.environ.get("PG_ADMIN_USER", os.environ.get("TRENDX_DB_USER", "trendx_app")),
        "password": os.environ.get(
            "PG_ADMIN_PASSWORD", os.environ.get("TRENDX_DB_PASSWORD", "trendx_app_pass")
        ),
    }


def _connect(params: dict[str, str]):
    import psycopg2

    return psycopg2.connect(
        host=params["host"],
        port=params["port"],
        dbname=params["dbname"],
        user=params["user"],
        password=params["password"],
    )


def _table_exists(params: dict[str, str], schema: str, table: str) -> bool:
    conn = _connect(params)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT 1 FROM information_schema.tables
                WHERE table_schema = %s AND table_name = %s
                """,
                (schema, table),
            )
            return cur.fetchone() is not None
    finally:
        conn.close()


def _columns(params: dict[str, str], schema: str, table: str) -> set[str]:
    conn = _connect(params)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT column_name FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
                """,
                (schema, table),
            )
            return {row[0] for row in cur.fetchall()}
    finally:
        conn.close()


def _relkind(params: dict[str, str], schema: str, table: str) -> str | None:
    conn = _connect(params)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT c.relkind
                FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = %s AND c.relname = %s
                """,
                (schema, table),
            )
            row = cur.fetchone()
            return row[0] if row else None
    finally:
        conn.close()


def _execute(params: dict[str, str], sql: str, args: tuple = ()) -> None:
    conn = _connect(params)
    try:
        with conn.cursor() as cur:
            cur.execute(sql, args)
        conn.commit()
    finally:
        conn.close()


def _scalar(params: dict[str, str], sql: str, args: tuple = ()):
    conn = _connect(params)
    try:
        with conn.cursor() as cur:
            cur.execute(sql, args)
            row = cur.fetchone()
            return row[0] if row else None
    finally:
        conn.close()


def _safe_partition_ts(params: dict[str, str]) -> str:
    """Return an ISO-8601 UTC timestamp guaranteed to fall inside the current
    month's ts_kv partition (the first partition always created by migration
    010's +3-months horizon on a fresh DB). Computed server-side so it does not
    depend on any client/server clock skew."""
    return _scalar(
        params,
        "SELECT to_char(date_trunc('month', now()) + interval '15 days', "
        '\'YYYY-MM-DD"T"HH24:MI:SS"+00:00"\')',
    )


def test_migration_009_tables_exist():
    params = _pg_conn_params()
    assert _table_exists(
        params, SCHEMA, "ts_kv"
    ), f"{SCHEMA}.ts_kv missing after applying migration 009 (blocks ingestion on native PG)"
    assert _table_exists(
        params, SCHEMA, "ts_kv_latest"
    ), f"{SCHEMA}.ts_kv_latest missing after applying migration 009"


def test_migration_009_ts_kv_structure():
    params = _pg_conn_params()
    got = _columns(params, SCHEMA, "ts_kv")
    missing = TS_KV_COLUMNS - got
    assert not missing, f"missing columns in {SCHEMA}.ts_kv: {missing}"


def test_migration_009_ts_kv_latest_structure():
    params = _pg_conn_params()
    got = _columns(params, SCHEMA, "ts_kv_latest")
    missing = TS_KV_LATEST_COLUMNS - got
    assert not missing, f"missing columns in {SCHEMA}.ts_kv_latest: {missing}"


def test_migration_009_ts_kv_insert_and_upsert():
    """Real INSERT + UPSERT on ts_kv, matching IngestionService._store_telemetry."""
    params = _pg_conn_params()
    entity_id = str(uuid.uuid4())
    ts = _safe_partition_ts(params)
    metric_key = "temperature"

    # First INSERT (mirrors ingestion.py:320).
    _execute(
        params,
        """
        INSERT INTO trendx_analytics.ts_kv (ts, entity_id, metric_key, dbl_v, source, ingestion_id)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (ts, entity_id, metric_key)
        DO UPDATE SET dbl_v = EXCLUDED.dbl_v,
                      source = EXCLUDED.source,
                      ingestion_id = EXCLUDED.ingestion_id
        """,
        (ts, entity_id, metric_key, 21.5, "thingsboard", "ing-1"),
    )
    count = _scalar(
        params,
        "SELECT count(*) FROM trendx_analytics.ts_kv WHERE entity_id = %s",
        (entity_id,),
    )
    assert count == 1, f"expected 1 row after first insert, got {count}"

    # UPSERT with a different value (idempotent re-key, value updated in place).
    _execute(
        params,
        """
        INSERT INTO trendx_analytics.ts_kv (ts, entity_id, metric_key, dbl_v, source, ingestion_id)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (ts, entity_id, metric_key)
        DO UPDATE SET dbl_v = EXCLUDED.dbl_v,
                      source = EXCLUDED.source,
                      ingestion_id = EXCLUDED.ingestion_id
        """,
        (ts, entity_id, metric_key, 99.0, "thingsboard", "ing-2"),
    )
    count = _scalar(
        params,
        "SELECT count(*) FROM trendx_analytics.ts_kv WHERE entity_id = %s",
        (entity_id,),
    )
    assert count == 1, f"upsert must not duplicate the primary key, got {count}"
    value = _scalar(
        params,
        "SELECT dbl_v FROM trendx_analytics.ts_kv WHERE entity_id = %s",
        (entity_id,),
    )
    assert value == 99.0, f"upsert must update dbl_v in place, got {value}"

    # Cleanup this test's rows (idempotent; keeps the shared DB tidy).
    _execute(
        params,
        "DELETE FROM trendx_analytics.ts_kv WHERE entity_id = %s",
        (entity_id,),
    )


def test_migration_009_ts_kv_latest_upsert():
    """Real UPSERT on ts_kv_latest, matching IngestionService._store_telemetry."""
    params = _pg_conn_params()
    entity_id = str(uuid.uuid4())
    ts = _safe_partition_ts(params)
    metric_key = "temperature"

    _execute(
        params,
        """
        INSERT INTO trendx_analytics.ts_kv_latest (entity_id, metric_key, ts, dbl_v, source)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (entity_id, metric_key)
        DO UPDATE SET ts = EXCLUDED.ts,
                      dbl_v = EXCLUDED.dbl_v,
                      source = EXCLUDED.source
        """,
        (entity_id, metric_key, ts, 21.5, "thingsboard"),
    )
    count = _scalar(
        params,
        "SELECT count(*) FROM trendx_analytics.ts_kv_latest WHERE entity_id = %s",
        (entity_id,),
    )
    assert count == 1, f"expected 1 latest row, got {count}"

    _execute(
        params,
        """
        INSERT INTO trendx_analytics.ts_kv_latest (entity_id, metric_key, ts, dbl_v, source)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (entity_id, metric_key)
        DO UPDATE SET ts = EXCLUDED.ts,
                      dbl_v = EXCLUDED.dbl_v,
                      source = EXCLUDED.source
        """,
        (entity_id, metric_key, ts, 42.0, "thingsboard"),
    )
    count = _scalar(
        params,
        "SELECT count(*) FROM trendx_analytics.ts_kv_latest WHERE entity_id = %s",
        (entity_id,),
    )
    assert count == 1, f"latest upsert must not duplicate, got {count}"
    value = _scalar(
        params,
        "SELECT dbl_v FROM trendx_analytics.ts_kv_latest WHERE entity_id = %s",
        (entity_id,),
    )
    assert value == 42.0, f"latest upsert must update dbl_v, got {value}"

    _execute(
        params,
        "DELETE FROM trendx_analytics.ts_kv_latest WHERE entity_id = %s",
        (entity_id,),
    )


def test_migration_009_native_heap():
    """On native PostgreSQL, ts_kv must be a declaratively partitioned table
    (relkind 'p'), not a TimescaleDB hypertable — per B5 (TimescaleDB refused).
    Migration 010 turns the 009 plain heap into PARTITION BY RANGE(ts);
    exhaustive partitioning/BRIN checks live in test_migration_010_partition_ts_kv."""
    params = _pg_conn_params()
    relkind = _relkind(params, SCHEMA, "ts_kv")
    assert (
        relkind == "p"
    ), f"{SCHEMA}.ts_kv must be a partitioned table on native PG (relkind='p'), got {relkind!r}"


def test_migration_009_idempotence_characteristic():
    """Migration 009 is native-only and idempotent."""
    migration_009 = next(MIGRATIONS.glob("009_*.sql"))
    assert migration_009.exists(), "migrations/009_*.sql missing"
    text = migration_009.read_text(encoding="utf-8")

    # Native-only guard: the migration skips itself when TimescaleDB is present
    # (002 already provides ts_kv in that profile), so it must be guarded by
    # IF EXISTS timescaledb (RETURN inside the DO block).
    assert re.search(
        r"IF EXISTS \(SELECT 1 FROM pg_extension WHERE extname = 'timescaledb'\)", text
    ), "migration 009 must be guarded by IF EXISTS timescaledb (skips when present)"

    # Idempotent object creation.
    assert re.search(
        r"CREATE SCHEMA IF NOT EXISTS trendx_analytics\b", text
    ), "migration 009 should CREATE SCHEMA IF NOT EXISTS trendx_analytics"
    assert re.search(
        r"CREATE TABLE IF NOT EXISTS trendx_analytics\.ts_kv\b", text
    ), "migration 009 should CREATE TABLE IF NOT EXISTS trendx_analytics.ts_kv"
    assert re.search(
        r"CREATE TABLE IF NOT EXISTS trendx_analytics\.ts_kv_latest\b", text
    ), "migration 009 should CREATE TABLE IF NOT EXISTS trendx_analytics.ts_kv_latest"

    # No TimescaleDB-specific constructs (keeps the native profile clean).
    assert (
        "create_hypertable" not in text
    ), "migration 009 must NOT use create_hypertable (native profile, B5)"
    assert (
        "timescaledb.transaction_per_chunk" not in text
    ), "migration 009 must NOT use TimescaleDB-specific index options"


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main([__file__, "-v"]))
