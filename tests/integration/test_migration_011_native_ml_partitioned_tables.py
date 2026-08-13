"""Integration test for migration 011 (native ML partitioned tables — B5 Lot 2).

Applies against a REAL PostgreSQL instance (CI ``postgres:16-alpine`` service,
i.e. NATIVE PostgreSQL, NO TimescaleDB) and proves that migration 011 creates
the four native partitioned ML tables so that ``ensure_partitions_forward(3)``
(defined in 004) becomes operational in the native profile.

This test is FUNCTIONAL, not textual-only:
  * it executes ``ensure_partitions_forward(3)`` and asserts it returns without
    raising (the exact failure mode before 011: it raised
    ``table partitionnée introuvable`` for the ML tables);
  * it verifies the resulting monthly partitions exist for ALL FIVE parents
    (ts_kv + the four ML tables);
  * it inserts a real row into each ML table and verifies PostgreSQL routed it
    to the correct monthly child partition.

It deliberately does NOT re-run migration 011 verbatim (it is applied once by
the ``test-integration`` before_script loop); it asserts the post-state and
exercises the runtime contract.

TimescaleDB scope
-----------------
Migration 011 is native-only (no TimescaleDB). The test runs on native PG and
additionally asserts the migration file does not reference TimescaleDB-specific
constructs (``create_hypertable`` / ``add_retention_policy``).
"""

from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path

import psycopg2
import pytest

# Keep behaviour identical to the CI job (postgres:16-alpine service).
os.environ.setdefault("PG_ADMIN_HOST", "postgres")
os.environ.setdefault("PG_ADMIN_PORT", "5432")
os.environ.setdefault("PG_ADMIN_DB", "trendx")
os.environ.setdefault("PG_ADMIN_USER", "trendx_app")
os.environ.setdefault("PG_ADMIN_PASSWORD", "trendx_app_pass")
os.environ.setdefault("TRENDX_DB_HOST", "postgres")
os.environ.setdefault("TRENDX_DB_PORT", "5432")
os.environ.setdefault("TRENDX_DB_USER", "trendx_app")
os.environ.setdefault("TRENDX_DB_PASSWORD", "trendx_app_pass")

ROOT = Path(__file__).parents[2]
MIGRATIONS = ROOT / "migrations"

SCHEMA = "trendx_analytics"
pytestmark = [pytest.mark.integration]

# (table, partition_key_column, is_data_quality)
ML_TABLES = [
    ("predictions", "ts", False),
    ("anomaly_scores", "ts", False),
    ("data_quality", "period", True),
    ("ml_metrics", "ts", False),
]


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
            # ensure_partitions_forward() performs DDL (CREATE TABLE PARTITION OF).
            # psycopg2 opens a transaction for the function call and would ROLL
            # BACK on close() if we don't commit, discarding the partitions it
            # created. Commit unconditionally: a no-op after a plain SELECT.
            conn.commit()
            return row[0] if row else None
    finally:
        conn.close()


def _table_exists(params: dict[str, str], schema: str, table: str) -> bool:
    return bool(
        _scalar(
            params,
            "SELECT 1 FROM information_schema.tables WHERE table_schema=%s AND table_name=%s",
            (schema, table),
        )
    )


def _relkind(params: dict[str, str], schema: str, table: str) -> str | None:
    return _scalar(
        params,
        "SELECT c.relkind FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
        "WHERE n.nspname=%s AND c.relname=%s",
        (schema, table),
    )


def _partition_key_column(params: dict[str, str], schema: str, table: str) -> str | None:
    # partattrs is an int2vector of the partition-key attribute numbers.
    return _scalar(
        params,
        "SELECT a.attname FROM pg_partitioned_table p "
        "JOIN pg_class c ON c.oid=p.partrelid "
        "JOIN pg_namespace n ON n.oid=c.relnamespace "
        "JOIN pg_attribute a ON a.attrelid=p.partrelid "
        "WHERE n.nspname=%s AND c.relname=%s AND a.attnum = ANY(p.partattrs)",
        (schema, table),
    )


def _partitions(params: dict[str, str], schema: str, table: str) -> list[str]:
    conn = _connect(params)
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT c.relname FROM pg_inherits i "
                "JOIN pg_class p ON p.oid=i.inhparent "
                "JOIN pg_class c ON c.oid=i.inhrelid "
                "JOIN pg_namespace pn ON pn.oid=p.relnamespace "
                "WHERE p.relname=%s AND pn.nspname=%s ORDER BY c.relname",
                (table, schema),
            )
            return [r[0] for r in cur.fetchall()]
    finally:
        conn.close()


def _month_partition_name(table: str, d: datetime) -> str:
    return f"{table}_{d.year:04d}_{d.month:02d}"


def _ensure_forward(params: dict[str, str]):
    """Run ensure_partitions_forward(3) and assert it does not raise."""
    result = _scalar(params, "SELECT trendx_analytics.ensure_partitions_forward(3)")
    assert result is not None, "ensure_partitions_forward(3) must execute without error"
    return result


# ---------------------------------------------------------------------------
# 1) Tables présentes
# ---------------------------------------------------------------------------
def test_migration_011_ml_tables_exist():
    params = _pg_conn_params()
    for table, _, _ in ML_TABLES:
        assert _table_exists(
            params, SCHEMA, table
        ), f"{SCHEMA}.{table} missing after applying migration 011"


# ---------------------------------------------------------------------------
# 2) Partitionnement RANGE(ts) [period pour data_quality]
# ---------------------------------------------------------------------------
def test_migration_011_partitioned_range():
    params = _pg_conn_params()
    for table, key_col, _ in ML_TABLES:
        relkind = _relkind(params, SCHEMA, table)
        assert (
            relkind == "p"
        ), f"{SCHEMA}.{table} must be partitioned (relkind='p'), got {relkind!r}"

        partkey = _scalar(params, "SELECT pg_get_partkeydef(%s::regclass)", (f"{SCHEMA}.{table}",))
        assert partkey is not None, f"partition key missing for {SCHEMA}.{table}"
        assert "RANGE" in partkey.upper(), f"partition strategy must be RANGE, got {partkey!r}"

        got_col = _partition_key_column(params, SCHEMA, table)
        assert (
            got_col == key_col
        ), f"{SCHEMA}.{table} partition key column must be {key_col!r}, got {got_col!r}"


# ---------------------------------------------------------------------------
# 3) Absence de TimescaleDB
# ---------------------------------------------------------------------------
def test_migration_011_no_timescaledb_dependency():
    migration = next(MIGRATIONS.glob("011_*.sql"))
    text = migration.read_text(encoding="utf-8").lower()
    assert "create_hypertable" not in text, "011 must not call create_hypertable"
    assert "add_retention_policy" not in text, "011 must not call add_retention_policy"
    assert "create extension" not in text, "011 must not CREATE EXTENSION"
    # ensure_partitions_forward() (004) must remain the unique forward mechanism.
    # 011 may *reference* it in comments, but must NOT redefine/reimplement it:
    # forbid a DDL (re)definition of the function, not a textual mention.
    import re

    assert not re.search(
        r"create\s+or\s+replace\s+function[^\n]*ensure_partitions_forward",
        text,
        re.IGNORECASE,
    ), "011 must not redefine ensure_partitions_forward() (004 owns forward maintenance)"


# ---------------------------------------------------------------------------
# 4) ensure_partitions_forward fonctionnel (point critique)
# ---------------------------------------------------------------------------
def test_migration_011_ensure_partitions_forward_succeeds():
    params = _pg_conn_params()
    result = _ensure_forward(params)
    # The function returns a jsonb describing created partitions.
    assert isinstance(result, dict) or result is not None


# ---------------------------------------------------------------------------
# 5) Couverture des cinq parents (ts_kv + 4 ML)
# ---------------------------------------------------------------------------
def test_migration_011_five_parents_forward_coverage():
    params = _pg_conn_params()
    _ensure_forward(params)

    all_parents = ["ts_kv"] + [t for t, _, _ in ML_TABLES]
    now_month = datetime.now().replace(day=1, hour=0, minute=0, second=0, microsecond=0)

    for parent in all_parents:
        parts = _partitions(params, SCHEMA, parent)
        expected = _month_partition_name(parent, now_month)
        assert (
            expected in parts
        ), f"{SCHEMA}.{parent} missing current-month partition {expected}; got {parts}"


# ---------------------------------------------------------------------------
# 6) Horizon +3 mois
# ---------------------------------------------------------------------------
def test_migration_011_horizon_plus_three_months():
    params = _pg_conn_params()
    _ensure_forward(params)

    all_parents = ["ts_kv"] + [t for t, _, _ in ML_TABLES]
    now_month = datetime.now().replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    m = now_month.month - 1 + 3
    y = now_month.year + m // 12
    m = m % 12 + 1
    plus3 = now_month.replace(year=y, month=m)

    # No DEFAULT partition allowed anywhere.
    for parent in all_parents:
        parts = _partitions(params, SCHEMA, parent)
        assert not any(
            "default" in p.lower() for p in parts
        ), f"{SCHEMA}.{parent} must not have a DEFAULT partition: {parts}"
        expected = _month_partition_name(parent, plus3)
        assert (
            expected in parts
        ), f"{SCHEMA}.{parent} missing +3-month partition {expected}; got {parts}"


# ---------------------------------------------------------------------------
# 7) Idempotence
# ---------------------------------------------------------------------------
def test_migration_011_ensure_partitions_forward_idempotent():
    params = _pg_conn_params()
    before = {
        t: set(_partitions(params, SCHEMA, t)) for t in ["ts_kv"] + [x for x, _, _ in ML_TABLES]
    }
    _ensure_forward(params)
    _ensure_forward(params)  # second call must not error / duplicate
    after = {
        t: set(_partitions(params, SCHEMA, t)) for t in ["ts_kv"] + [x for x, _, _ in ML_TABLES]
    }
    assert (
        before == after
    ), f"partition set changed after re-running ensure_partitions_forward: {before} -> {after}"


# ---------------------------------------------------------------------------
# 8) INSERT fonctionnel routé vers la bonne partition
# ---------------------------------------------------------------------------
def test_migration_011_insert_routing():
    params = _pg_conn_params()
    _ensure_forward(params)

    now_month = datetime.now().replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    entity_id = "11111111-2222-3333-4444-555555555555"

    # Deterministic setup: remove any rows from previous runs for this test
    # entity. ml_metrics has no PK (mirrors 002), so its INSERT is not naturally
    # idempotent; the other three tables use ON CONFLICT DO NOTHING. Deleting
    # from the partitioned parent routes to the correct child partition.
    for t in ("predictions", "anomaly_scores", "data_quality", "ml_metrics"):
        _execute(
            params,
            f"DELETE FROM trendx_analytics.{t} WHERE entity_id = %s",
            (entity_id,),
        )

    # predictions (partition key ts)
    _execute(
        params,
        "INSERT INTO trendx_analytics.predictions "
        "(ts, entity_id, metric_key, forecast_generated_at, model_id, model_used, value, horizon_step) "
        "VALUES (%s, %s, 'm_pred', %s, %s, 'prophet', 7.0, 1) "
        "ON CONFLICT (ts, entity_id, metric_key, forecast_generated_at, model_id) DO NOTHING",
        (now_month, entity_id, now_month, "22222222-2222-3333-4444-555555555555"),
    )
    # anomaly_scores (partition key ts)
    _execute(
        params,
        "INSERT INTO trendx_analytics.anomaly_scores "
        "(ts, entity_id, metric_key, detector_id, raw_score) "
        "VALUES (%s, %s, 'm_anom', %s, 0.42) "
        "ON CONFLICT (ts, entity_id, metric_key, detector_id) DO NOTHING",
        (now_month, entity_id, "33333333-2222-3333-4444-555555555555"),
    )
    # data_quality (partition key period)
    _execute(
        params,
        "INSERT INTO trendx_analytics.data_quality "
        "(period, entity_id, metric_key, period_length, expected_points, actual_points) "
        "VALUES (%s, %s, 'm_dq', '1h', 100, 98) "
        "ON CONFLICT (period, entity_id, metric_key, period_length) DO NOTHING",
        (now_month, entity_id),
    )
    # ml_metrics (partition key ts, no PK)
    _execute(
        params,
        "INSERT INTO trendx_analytics.ml_metrics "
        "(ts, entity_id, metric_key, run_type, metric_name, metric_value) "
        "VALUES (%s, %s, 'm_ml', 'TRAIN', 'mae', 1.5) "
        "ON CONFLICT DO NOTHING",
        (now_month, entity_id),
    )

    expected_pred = _month_partition_name("predictions", now_month)
    expected_anom = _month_partition_name("anomaly_scores", now_month)
    expected_dq = _month_partition_name("data_quality", now_month)
    expected_ml = _month_partition_name("ml_metrics", now_month)

    assert (
        _scalar(
            params,
            f"SELECT count(*) FROM trendx_analytics.{expected_pred} WHERE entity_id=%s",
            (entity_id,),
        )
        == 1
    ), f"row not routed into {expected_pred}"
    assert (
        _scalar(
            params,
            f"SELECT count(*) FROM trendx_analytics.{expected_anom} WHERE entity_id=%s",
            (entity_id,),
        )
        == 1
    ), f"row not routed into {expected_anom}"
    assert (
        _scalar(
            params,
            f"SELECT count(*) FROM trendx_analytics.{expected_dq} WHERE entity_id=%s",
            (entity_id,),
        )
        == 1
    ), f"row not routed into {expected_dq}"
    assert (
        _scalar(
            params,
            f"SELECT count(*) FROM trendx_analytics.{expected_ml} WHERE entity_id=%s",
            (entity_id,),
        )
        == 1
    ), f"row not routed into {expected_ml}"


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main([__file__, "-v"]))
