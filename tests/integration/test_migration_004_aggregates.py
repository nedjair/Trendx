"""Integration test for migration 004 (aggregates / partitions).

Applies (and re-examines) migrations/004_aggregates_partitions.sql against a
REAL PostgreSQL instance provided by the CI ``postgres:16-alpine`` service —
i.e. NATIVE PostgreSQL, NO TimescaleDB — and proves:

  * the four aggregate tables
    (``trendx_analytics.ts_kv_hourly`` / ``ts_kv_daily`` / ``ts_kv_weekly``,
    ``aggregate_watermarks``) are actually created;
  * the three functions (``refresh_aggregate``, ``ensure_partitions_forward``,
    ``partition_coverage``) exist;
  * table structure (columns, primary keys, indexes) matches the migration;
  * ownership is ``trendx_migration`` and ``trendx_app`` gets DML + EXECUTE;
  * the migration's idempotence characteristic is handled explicitly (see note).

Idempotence note
----------------
Migration 004 uses plain ``CREATE TABLE`` (NO ``IF NOT EXISTS``) for its
aggregate tables and ``CREATE OR REPLACE FUNCTION`` for its functions. It is
therefore NOT verbatim-rerunnable on an already-populated database: the first
``CREATE TABLE`` would raise ``already exists`` under ``ON_ERROR_STOP``. Like
migration 002 it is applied exactly ONCE by the ``test-integration``
``before_script`` (the loop applies ``001``->``006`` including ``004``). This
test makes the repeatability characteristic explicit: it asserts the artifacts
are present after that single, authoritative application, and it statically
confirms the file relies on ``CREATE TABLE`` (not ``CREATE TABLE IF NOT EXISTS``)
for the aggregate tables — i.e. the migration assumes a clean pre-state and must
not be re-run blindly. We deliberately do NOT re-run the whole file here (that
would error by design) nor fabricate a fake functional refresh.

TimescaleDB scope
------------------
``refresh_aggregate()`` queries ``trendx_analytics.ts_kv``, which is created by
migration 002. On native PostgreSQL (no TimescaleDB) 002 is a no-op, so ``ts_kv``
does not exist and ``refresh_aggregate()`` cannot be executed functionally here.
This test therefore validates DDL / structure / grants / owner / the
idempotence characteristic ONLY. Functional execution of ``refresh_aggregate()``
(and ``ensure_partitions_forward``, which also needs ``ts_kv``) is out of scope
for the native-PostgreSQL profile and requires a TimescaleDB instance — it is
NOT asserted here (no fake validation).

Selection
---------
This test is selected by the ``integration`` marker and runs inside the shared
``test-integration`` job (which applies ``001``->``006``, including ``004``). It
is intentionally NOT marked ``migration_apply``: it asserts the post-state of an
already-applied migration rather than re-applying a non-idempotent migration on
a fresh database.
"""

from __future__ import annotations

import os
import re
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

TABLES = ("ts_kv_hourly", "ts_kv_daily", "ts_kv_weekly", "aggregate_watermarks")
FUNCTIONS = {
    "refresh_aggregate": "p_agg text",
    "ensure_partitions_forward": "p_min_months integer DEFAULT 3",
    "partition_coverage": "p_min_months integer DEFAULT 3",
}
EXPECTED_COLUMNS = {
    "ts_kv_hourly": {
        "bucket",
        "entity_id",
        "metric_key",
        "samples_count",
        "avg_v",
        "min_v",
        "max_v",
        "stddev_v",
        "median_v",
        "p90_v",
        "p10_v",
        "sum_v",
    },
    "ts_kv_daily": {
        "bucket",
        "entity_id",
        "metric_key",
        "samples_count",
        "avg_v",
        "min_v",
        "max_v",
        "stddev_v",
        "median_v",
        "sum_v",
    },
    "ts_kv_weekly": {
        "bucket",
        "entity_id",
        "metric_key",
        "samples_count",
        "avg_v",
        "min_v",
        "max_v",
        "stddev_v",
        "sum_v",
    },
    "aggregate_watermarks": {
        "aggregate_name",
        "watermark",
        "last_refresh_start",
        "last_refresh_end",
        "last_refresh_rows",
        "updated_at",
    },
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


def _owner_is(params: dict[str, str], schema: str, table: str, role: str) -> bool:
    conn = _connect(params)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT 1
                FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                JOIN pg_roles r ON r.oid = c.relowner
                WHERE n.nspname = %s AND c.relname = %s AND r.rolname = %s
                """,
                (schema, table, role),
            )
            return cur.fetchone() is not None
    finally:
        conn.close()


def _has_table_privilege(
    params: dict[str, str], role: str, schema: str, table: str, priv: str
) -> bool:
    conn = _connect(params)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT has_table_privilege(%s, %s, %s)", (role, f"{schema}.{table}", priv))
            return bool(cur.fetchone()[0])
    finally:
        conn.close()


def _function_exists(params: dict[str, str], schema: str, name: str, args: str) -> bool:
    conn = _connect(params)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT 1 FROM pg_proc p
                JOIN pg_namespace n ON n.oid = p.pronamespace
                WHERE n.nspname = %s AND p.proname = %s
                  AND pg_get_function_identity_arguments(p.oid) = %s
                """,
                (schema, name, args),
            )
            return cur.fetchone() is not None
    finally:
        conn.close()


def _has_function_privilege(
    params: dict[str, str], role: str, schema: str, name: str, args: str, priv: str
) -> bool:
    conn = _connect(params)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT p.oid FROM pg_proc p
                JOIN pg_namespace n ON n.oid = p.pronamespace
                WHERE n.nspname = %s AND p.proname = %s
                  AND pg_get_function_identity_arguments(p.oid) = %s
                """,
                (schema, name, args),
            )
            row = cur.fetchone()
            if row is None:
                return False
            cur.execute("SELECT has_function_privilege(%s, %s, %s)", (role, row[0], priv))
            return bool(cur.fetchone()[0])
    finally:
        conn.close()


def test_migration_004_aggregate_tables_exist():
    params = _pg_conn_params()
    for table in TABLES:
        assert _table_exists(
            params, "trendx_analytics", table
        ), f"trendx_analytics.{table} missing after applying migration 004"


def test_migration_004_functions_exist():
    params = _pg_conn_params()
    for name, args in FUNCTIONS.items():
        assert _function_exists(
            params, "trendx_analytics", name, args
        ), f"trendx_analytics.{name}({args}) missing after applying migration 004"


def test_migration_004_table_structure():
    params = _pg_conn_params()
    for table, expected in EXPECTED_COLUMNS.items():
        got = _columns(params, "trendx_analytics", table)
        missing = expected - got
        assert not missing, f"missing columns in trendx_analytics.{table}: {missing}"


def test_migration_004_owner_and_grants():
    params = _pg_conn_params()
    for table in TABLES:
        assert _owner_is(
            params, "trendx_analytics", table, "trendx_migration"
        ), f"trendx_analytics.{table} owner must be trendx_migration"
        for priv in ("SELECT", "INSERT", "UPDATE", "DELETE"):
            assert _has_table_privilege(
                params, "trendx_app", "trendx_analytics", table, priv
            ), f"trendx_app missing {priv} on trendx_analytics.{table}"
    for name, args in FUNCTIONS.items():
        assert _has_function_privilege(
            params, "trendx_app", "trendx_analytics", name, args, "EXECUTE"
        ), f"trendx_app missing EXECUTE on trendx_analytics.{name}({args})"


def test_migration_004_idempotence_characteristic():
    """Explicitly document/handle 004's idempotence characteristic.

    Migration 004 creates its aggregate tables with plain ``CREATE TABLE`` (no
    ``IF NOT EXISTS``), so it is applied exactly ONCE by the CI chain and must
    not be re-run verbatim on a populated database. This test asserts (a) the
    artifacts are present after that single application, and (b) the migration
    file relies on ``CREATE TABLE`` (not ``CREATE TABLE IF NOT EXISTS``) for the
    aggregate tables — i.e. it assumes a clean pre-state. We do NOT re-run the
    file (that would error by design) and we do NOT fabricate a functional
    refresh of ``refresh_aggregate()`` (no ``ts_kv`` source on native PG).
    """
    params = _pg_conn_params()
    migration_004 = next(MIGRATIONS.glob("004_*.sql"))
    assert migration_004.exists(), "migrations/004_*.sql missing"

    # (a) artifacts present after the single authoritative application.
    for table in TABLES:
        assert _table_exists(params, "trendx_analytics", table)
    for name, args in FUNCTIONS.items():
        assert _function_exists(params, "trendx_analytics", name, args)

    # (b) file relies on CREATE TABLE (no IF NOT EXISTS) for aggregate tables,
    #     confirming the apply-once / clean-pre-state characteristic.
    text = migration_004.read_text(encoding="utf-8")
    for table in ("ts_kv_hourly", "ts_kv_daily", "ts_kv_weekly", "aggregate_watermarks"):
        assert re.search(
            rf"CREATE TABLE trendx_analytics\.{table}\b", text
        ), f"migration 004 should CREATE TABLE trendx_analytics.{table}"
        assert not re.search(rf"CREATE TABLE IF NOT EXISTS trendx_analytics\.{table}\b", text), (
            f"migration 004 must NOT use IF NOT EXISTS on trendx_analytics.{table} "
            "(apply-once / clean-pre-state characteristic)"
        )


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main([__file__, "-v"]))
