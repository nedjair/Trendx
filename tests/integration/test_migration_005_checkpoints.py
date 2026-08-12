"""Integration test for migration 005 (ingestion checkpoints).

Applies (and re-applies) migrations/005_ingestion_checkpoints.sql against a REAL
PostgreSQL instance (the CI ``postgres:16-alpine`` service, native PostgreSQL)
and proves:

  * the table ``trendx_catalog.ingestion_checkpoints`` exists;
  * its structure (columns, primary-key constraint, index) matches the migration;
  * the migration is idempotent (re-application succeeds);
  * ``trendx_app`` can perform the required DML (INSERT / SELECT / UPDATE / DELETE).

GRANT limitation (important)
-----------------------------
On the CI PostgreSQL service, ``trendx_app`` is the init/superuser and therefore
*owns* every object, so the DML assertions below PASS even if an explicit
``GRANT`` were missing. This test therefore CANNOT, by itself, prove that
``trendx_app`` would have DML in a production multi-role setup where migrations
are applied by ``trendx_migration`` and default privileges (set by migration
000) only cover SCHEMA ``public`` — not ``trendx_catalog``. The absence of an
explicit ``GRANT ... TO trendx_app`` in migration 005 is a known P1 gap tracked
separately and is intentionally NOT fixed in this MR (migration modification
requires its own authorization). This file only adds test coverage.

Selection
---------
Selected by the ``integration`` marker; runs inside the shared ``test-integration``
job (which applies ``001``->``006``, including ``005``). It is intentionally NOT
marked ``migration_apply``.
"""

from __future__ import annotations

import os
import uuid
from pathlib import Path

import pytest

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

TABLE = "ingestion_checkpoints"
EXPECTED_COLUMNS = {
    "pipeline",
    "entity_id",
    "metric_key",
    "watermark_ts",
    "records_processed",
    "last_batch_id",
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


def _has_pk(params: dict[str, str], schema: str, table: str) -> bool:
    conn = _connect(params)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT 1 FROM information_schema.table_constraints
                WHERE table_schema = %s AND table_name = %s
                  AND constraint_type = 'PRIMARY KEY'
                """,
                (schema, table),
            )
            return cur.fetchone() is not None
    finally:
        conn.close()


def _has_index(params: dict[str, str], schema: str, index: str) -> bool:
    conn = _connect(params)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT 1 FROM pg_indexes
                WHERE schemaname = %s AND indexname = %s
                """,
                (schema, index),
            )
            return cur.fetchone() is not None
    finally:
        conn.close()


def _psql_run(sql_file: Path, params: dict[str, str]) -> None:
    import subprocess

    env = dict(os.environ)
    env["PGHOST"] = params["host"]
    env["PGPORT"] = str(params["port"])
    env["PGDATABASE"] = params["dbname"]
    env["PGUSER"] = params["user"]
    env["PGPASSWORD"] = params["password"]
    subprocess.run(
        ["psql", "-v", "ON_ERROR_STOP=1", "-f", str(sql_file)],
        env=env,
        check=True,
    )


def test_migration_005_table_exists():
    params = _pg_conn_params()
    assert _table_exists(
        params, "trendx_catalog", TABLE
    ), "trendx_catalog.ingestion_checkpoints missing after applying migration 005"


def test_migration_005_structure():
    params = _pg_conn_params()
    got = _columns(params, "trendx_catalog", TABLE)
    missing = EXPECTED_COLUMNS - got
    assert not missing, f"missing columns in trendx_catalog.{TABLE}: {missing}"
    assert _has_pk(
        params, "trendx_catalog", TABLE
    ), f"trendx_catalog.{TABLE} must have a PRIMARY KEY"
    assert _has_index(
        params, "trendx_catalog", "ingestion_checkpoints_entity_idx"
    ), "trendx_catalog.ingestion_checkpoints must have ingestion_checkpoints_entity_idx"


def test_migration_005_idempotent():
    params = _pg_conn_params()
    migration_005 = next(MIGRATIONS.glob("005_*.sql"))
    assert migration_005.exists(), "migrations/005_*.sql missing"
    # 005 uses CREATE TABLE IF NOT EXISTS / CREATE INDEX IF NOT EXISTS, so a
    # re-application must succeed under ON_ERROR_STOP.
    _psql_run(migration_005, params)
    assert _table_exists(params, "trendx_catalog", TABLE)


def test_migration_005_trendx_app_dml():
    """trendx_app can perform the ingestion checkpoint DML.

    NOTE: on the CI service trendx_app is the superuser/owner, so this passes
    regardless of an explicit GRANT. It does NOT prove the production multi-role
    GRANT gap (see module docstring) — that is a separate, authorized fix.
    """
    params = _pg_conn_params()
    pipeline = f"ci_test_{uuid.uuid4().hex[:8]}"
    entity = f"entity_{uuid.uuid4().hex[:8]}"
    metric = "metric_a"

    conn = _connect(params)
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"INSERT INTO trendx_catalog.{TABLE} "
                "(pipeline, entity_id, metric_key, watermark_ts, records_processed) "
                "VALUES (%s, %s, %s, now(), 0)",
                (pipeline, entity, metric),
            )
            cur.execute(
                f"SELECT records_processed FROM trendx_catalog.{TABLE} "
                "WHERE pipeline = %s AND entity_id = %s AND metric_key = %s",
                (pipeline, entity, metric),
            )
            assert cur.fetchone() is not None
            cur.execute(
                f"UPDATE trendx_catalog.{TABLE} SET records_processed = 1 "
                "WHERE pipeline = %s AND entity_id = %s AND metric_key = %s",
                (pipeline, entity, metric),
            )
            cur.execute(
                f"DELETE FROM trendx_catalog.{TABLE} "
                "WHERE pipeline = %s AND entity_id = %s AND metric_key = %s",
                (pipeline, entity, metric),
            )
        conn.commit()
    finally:
        conn.close()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main([__file__, "-v"]))
