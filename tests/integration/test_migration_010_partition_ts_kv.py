"""Integration test for migration 010 (ts_kv declarative partitioning + BRIN).

Runs inside the shared ``test-integration`` job, which applies
``001``->``006`` *plus* ``009`` *plus* ``010`` against a REAL PostgreSQL
instance (``postgres:16-alpine``, NATIVE, NO TimescaleDB). It validates the B5
contract for ``trendx_analytics.ts_kv`` after migration 010:

  * ``ts_kv`` is a declaratively PARTITIONED table (``relkind = 'p'``),
    ``PARTITION BY RANGE(ts)``;
  * monthly partitions ``ts_kv_YYYY_MM`` exist for the current month and the
    next three months (horizon +3 months), plus any month required to cover
    existing data;
  * NO ``DEFAULT`` partition exists;
  * a real ``INSERT`` lands in the expected monthly partition;
  * ``ON CONFLICT (ts, entity_id, metric_key)`` upsert keeps exactly one row;
  * a BRIN index ``idx_ts_kv_brin_ts`` (access method ``brin``) exists;
  * re-applying the migration is idempotent (no partition duplication, no data
    loss, BRIN still present);
  * the migration file is native-only (no ``CREATE EXTENSION``,
    no ``create_hypertable``, no ``transaction_per_chunk``) and guarded by a
    ``timescaledb`` presence check;
  * ``refresh_aggregate('hourly')`` (from migration 004) still works end-to-end
    against the partitioned ``ts_kv``.

This test complements (and does not duplicate) test_migration_009_native_ts_kv,
which only asserts the post-009 plain-heap post-state and the INSERT/UPSERT path.
"""

from __future__ import annotations

import datetime
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


def _partitions(params: dict[str, str], schema: str, table: str) -> list[str]:
    conn = _connect(params)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT c.relname
                FROM pg_inherits i
                JOIN pg_class p ON p.oid = i.inhparent
                JOIN pg_class c ON c.oid = i.inhrelid
                JOIN pg_namespace pns ON pns.oid = p.relnamespace
                WHERE p.relname = %s AND pns.nspname = %s
                ORDER BY c.relname
                """,
                (table, schema),
            )
            return [r[0] for r in cur.fetchall()]
    finally:
        conn.close()


def _month_partition_name(d: datetime.datetime) -> str:
    return f"ts_kv_{d.year:04d}_{d.month:02d}"


def _add_months(d: datetime.datetime, n: int) -> datetime.datetime:
    m = d.month - 1 + n
    y = d.year + m // 12
    m = m % 12 + 1
    return d.replace(year=y, month=m, day=1, hour=0, minute=0, second=0, microsecond=0)


# A. Existence
def test_migration_010_ts_kv_exists():
    params = _pg_conn_params()
    assert (
        _relkind(params, SCHEMA, "ts_kv") is not None
    ), f"{SCHEMA}.ts_kv missing after applying migration 010"


# B. Partitioning
def test_migration_010_partitioned_range_ts():
    params = _pg_conn_params()
    relkind = _relkind(params, SCHEMA, "ts_kv")
    assert relkind == "p", f"{SCHEMA}.ts_kv must be partitioned (relkind='p'), got {relkind!r}"

    partkey = _scalar(
        params,
        "SELECT pg_get_partkeydef('trendx_analytics.ts_kv'::regclass)",
    )
    assert partkey is not None, "partition key definition missing"
    assert "RANGE" in partkey.upper(), f"partition strategy must be RANGE, got {partkey!r}"
    assert "ts" in partkey, f"partition key must include ts, got {partkey!r}"


# C. Partitions
def test_migration_010_partitions_coverage_and_no_default():
    params = _pg_conn_params()
    now = datetime.datetime.now(datetime.UTC)
    expected = [
        _month_partition_name(_add_months(now, i))
        for i in range(0, 4)  # current .. +3
    ]

    parts = _partitions(params, SCHEMA, "ts_kv")
    assert parts, f"no child partitions found for {SCHEMA}.ts_kv"
    for name in expected:
        assert name in parts, f"expected partition {name} missing; got {parts}"

    # No DEFAULT / catch-all partition allowed by B5.
    assert not any(
        "default" in p.lower() for p in parts
    ), f"DEFAULT partition forbidden by B5, found: {parts}"
    has_default_bound = _scalar(
        params,
        """
        SELECT bool_or(
            pg_get_expr(c.relpartbound, c.oid) ILIKE '%DEFAULT%'
            OR pg_get_expr(c.relpartbound, c.oid) ILIKE '%MAXVALUE%'
        )
        FROM pg_inherits i
        JOIN pg_class p ON p.oid = i.inhparent
        JOIN pg_class c ON c.oid = i.inhrelid
        JOIN pg_namespace pns ON pns.oid = p.relnamespace
        WHERE p.relname = 'ts_kv' AND pns.nspname = 'trendx_analytics'
        """,
    )
    assert not has_default_bound, "a partition uses DEFAULT/MAXVALUE bound (forbidden by B5)"


# D. INSERT lands in the expected monthly partition
def test_migration_010_insert_lands_in_month_partition():
    params = _pg_conn_params()
    entity_id = str(uuid.uuid4())
    metric_key = f"b5_insert_{uuid.uuid4().hex[:8]}"
    now = datetime.datetime.now(datetime.UTC)
    ts = now.isoformat()
    cur_part = _month_partition_name(now.replace(day=1, hour=0, minute=0, second=0, microsecond=0))

    _execute(
        params,
        "INSERT INTO trendx_analytics.ts_kv "
        "(ts, entity_id, metric_key, dbl_v, source, ingestion_id) "
        "VALUES (%s, %s, %s, 42.0, 'test', 'b5') "
        "ON CONFLICT (ts, entity_id, metric_key) DO NOTHING",
        (ts, entity_id, metric_key),
    )

    total = _scalar(
        params,
        "SELECT count(*) FROM trendx_analytics.ts_kv WHERE entity_id = %s AND metric_key = %s",
        (entity_id, metric_key),
    )
    assert total == 1, f"expected exactly 1 row in ts_kv, got {total}"

    in_part = _scalar(
        params,
        f"SELECT count(*) FROM trendx_analytics.{cur_part} "
        "WHERE entity_id = %s AND metric_key = %s",
        (entity_id, metric_key),
    )
    assert in_part == 1, f"row must land in partition {cur_part}, got {in_part}"


# E. UPSERT keeps exactly one row
def test_migration_010_upsert_on_conflict():
    params = _pg_conn_params()
    entity_id = str(uuid.uuid4())
    metric_key = f"b5_upsert_{uuid.uuid4().hex[:8]}"
    now = datetime.datetime.now(datetime.UTC)
    ts = now.isoformat()

    _execute(
        params,
        "INSERT INTO trendx_analytics.ts_kv "
        "(ts, entity_id, metric_key, dbl_v, source, ingestion_id) "
        "VALUES (%s, %s, %s, 1.0, 'test', 'b5') "
        "ON CONFLICT (ts, entity_id, metric_key) DO UPDATE SET dbl_v = EXCLUDED.dbl_v",
        (ts, entity_id, metric_key),
    )
    _execute(
        params,
        "INSERT INTO trendx_analytics.ts_kv "
        "(ts, entity_id, metric_key, dbl_v, source, ingestion_id) "
        "VALUES (%s, %s, %s, 2.0, 'test', 'b5') "
        "ON CONFLICT (ts, entity_id, metric_key) DO UPDATE SET dbl_v = EXCLUDED.dbl_v",
        (ts, entity_id, metric_key),
    )

    total = _scalar(
        params,
        "SELECT count(*) FROM trendx_analytics.ts_kv WHERE entity_id = %s AND metric_key = %s",
        (entity_id, metric_key),
    )
    assert total == 1, f"ON CONFLICT upsert must keep exactly 1 row, got {total}"

    dbl = _scalar(
        params,
        "SELECT dbl_v FROM trendx_analytics.ts_kv WHERE entity_id = %s AND metric_key = %s",
        (entity_id, metric_key),
    )
    assert dbl == 2.0, f"upsert must update dbl_v to 2.0, got {dbl}"


# F. BRIN index
def test_migration_010_brin_index():
    params = _pg_conn_params()
    amname = _scalar(
        params,
        """
        SELECT am.amname
        FROM pg_class i
        JOIN pg_am am ON am.oid = i.relam
        WHERE i.relname = 'idx_ts_kv_brin_ts'
        """,
    )
    assert amname == "brin", f"idx_ts_kv_brin_ts must be a BRIN index, got {amname!r}"


# G. Idempotence
def test_migration_010_idempotent_reapply():
    params = _pg_conn_params()
    migration = next(MIGRATIONS.glob("010_*.sql"))
    text = migration.read_text(encoding="utf-8")

    parts_before = _partitions(params, SCHEMA, "ts_kv")
    rows_before = _scalar(params, "SELECT count(*) FROM trendx_analytics.ts_kv") or 0
    brin_before = _scalar(
        params,
        "SELECT count(*) FROM pg_class WHERE relname = 'idx_ts_kv_brin_ts'",
    )

    # Re-apply the full migration file, exactly like the CI loop does.
    conn = _connect(params)
    try:
        with conn.cursor() as cur:
            cur.execute(text)
        conn.commit()
    finally:
        conn.close()

    parts_after = _partitions(params, SCHEMA, "ts_kv")
    rows_after = _scalar(params, "SELECT count(*) FROM trendx_analytics.ts_kv") or 0
    brin_after = _scalar(
        params,
        "SELECT count(*) FROM pg_class WHERE relname = 'idx_ts_kv_brin_ts'",
    )

    assert len(parts_after) == len(
        parts_before
    ), f"partition count changed after re-apply: {parts_before} -> {parts_after}"
    assert (
        rows_after == rows_before
    ), f"row count changed after re-apply: {rows_before} -> {rows_after}"
    assert (
        brin_after == brin_before and brin_after == 1
    ), f"BRIN index must remain (exactly 1), got before={brin_before} after={brin_after}"


# H. Absence of TimescaleDB dependency (static)
def test_migration_010_no_timescaledb_dependency():
    migration = next(MIGRATIONS.glob("010_*.sql"))
    text = migration.read_text(encoding="utf-8")
    low = text.lower()

    assert "create extension" not in low, "010 must not CREATE EXTENSION"
    assert "create_hypertable" not in low, "010 must not call create_hypertable"
    assert "transaction_per_chunk" not in low, "010 must not reference transaction_per_chunk"
    assert re.search(
        r"IF EXISTS\s*\([^)]*extname = 'timescaledb'[^)]*\)",
        text,
        re.IGNORECASE,
    ), "010 must be guarded by IF EXISTS timescaledb (skip when present)"


# I. Aggregate compatibility (refresh_aggregate on partitioned ts_kv)
def test_migration_010_refresh_aggregate_hourly():
    params = _pg_conn_params()
    entity_id = str(uuid.uuid4())
    metric_key = f"b5_agg_{uuid.uuid4().hex[:8]}"
    now = datetime.datetime.now(datetime.UTC)
    ts = now.isoformat()

    _execute(
        params,
        "INSERT INTO trendx_analytics.ts_kv "
        "(ts, entity_id, metric_key, dbl_v, source, ingestion_id) "
        "VALUES (%s, %s, %s, 7.0, 'test', 'b5') "
        "ON CONFLICT (ts, entity_id, metric_key) DO NOTHING",
        (ts, entity_id, metric_key),
    )

    result = _scalar(params, "SELECT trendx_analytics.refresh_aggregate('hourly')")
    assert result is not None, "refresh_aggregate('hourly') must execute without error"

    agg_rows = _scalar(
        params,
        "SELECT count(*) FROM trendx_analytics.ts_kv_hourly "
        "WHERE entity_id = %s AND metric_key = %s",
        (entity_id, metric_key),
    )
    assert agg_rows >= 1, "refresh_aggregate must populate ts_kv_hourly for the inserted row"

    watermark = _scalar(
        params,
        "SELECT watermark FROM trendx_analytics.aggregate_watermarks WHERE aggregate_name = 'hourly'",
    )
    assert watermark is not None, "aggregate_watermarks must be updated for 'hourly'"
