"""Integration test for migration 008 (issue #2).

Applies migrations 001 -> 008 against a real PostgreSQL instance and proves
that ``public.prediction_model.model_uri`` is created and that migration 008
is idempotent (re-applying it must succeed).

This test requires a live PostgreSQL. It is selected by the ``integration``
marker and only runs when ``--run-integration`` is passed or a DB is reachable,
consistent with tests/conftest.py. The dedicated CI job ``test-migration-008``
runs it with ``--run-integration`` and the ephemeral postgres service.

It MUST NOT modify any migration file, ORM model, or P1 scope.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).parents[2]
MIGRATIONS = ROOT / "migrations"

pytestmark = [pytest.mark.integration, pytest.mark.migration_apply]


def _env(key: str, default: str) -> str:
    return os.environ.get(key, default)


def _pg_conn_params() -> dict[str, str]:
    """Build psycopg2 connection params from CI environment variables."""
    return {
        "host": _env("PG_ADMIN_HOST", _env("TRENDX_DB_HOST", "postgres")),
        "port": int(_env("PG_ADMIN_PORT", _env("TRENDX_DB_PORT", "5432"))),
        "dbname": _env("PG_ADMIN_DB", _env("TRENDX_DB_NAME", "trendx")),
        "user": _env("PG_ADMIN_USER", _env("TRENDX_DB_USER", "trendx_app")),
        "password": _env("PG_ADMIN_PASSWORD", _env("TRENDX_DB_PASSWORD", "trendx_app_pass")),
    }


def _psql_run(sql_file: Path, params: dict[str, str]) -> None:
    """Apply a SQL migration file via psql with ON_ERROR_STOP.

    Raises subprocess.CalledProcessError (non-zero exit) on failure.
    """
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


def _ensure_schemas(params: dict[str, str]) -> None:
    import psycopg2

    conn = psycopg2.connect(
        host=params["host"],
        port=params["port"],
        dbname=params["dbname"],
        user=params["user"],
        password=params["password"],
    )
    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("CREATE SCHEMA IF NOT EXISTS trendx_catalog")
            cur.execute("CREATE SCHEMA IF NOT EXISTS trendx_analytics")
    finally:
        conn.close()


def _column_exists(params: dict[str, str], table: str, column: str) -> bool:
    import psycopg2

    conn = psycopg2.connect(
        host=params["host"],
        port=params["port"],
        dbname=params["dbname"],
        user=params["user"],
        password=params["password"],
    )
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT 1
                FROM information_schema.columns
                WHERE table_schema = 'trendx_catalog'
                  AND table_name = %s
                  AND column_name = %s
                """,
                (table, column),
            )
            return cur.fetchone() is not None
    finally:
        conn.close()


def _table_exists(params: dict[str, str], table: str) -> bool:
    import psycopg2

    conn = psycopg2.connect(
        host=params["host"],
        port=params["port"],
        dbname=params["dbname"],
        user=params["user"],
        password=params["password"],
    )
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema = 'trendx_catalog'
                  AND table_name = %s
                """,
                (table,),
            )
            return cur.fetchone() is not None
    finally:
        conn.close()


def test_migration_008_creates_model_uri_on_trendx_catalog_prediction_model():
    params = _pg_conn_params()

    migration_001 = next(MIGRATIONS.glob("001_*.sql"))
    migration_003 = MIGRATIONS / "003_relocate_catalog.sql"
    migration_008 = MIGRATIONS / "008_add_model_uri.sql"
    assert migration_008.exists(), "migrations/008_add_model_uri.sql missing"
    assert migration_003.exists(), "migrations/003_relocate_catalog.sql missing"

    # 1. Schemas required by the chain (mirrors CI before_script).
    _ensure_schemas(params)

    # 2. Apply 001 -> 003 -> 008 (real chain). ON_ERROR_STOP fails on any error.
    _psql_run(migration_001, params)
    _psql_run(migration_003, params)
    _psql_run(migration_008, params)

    # 3. trendx_catalog.prediction_model must exist (created by 001, relocated by 003).
    assert _table_exists(
        params, "prediction_model"
    ), "trendx_catalog.prediction_model missing after applying migration 001/003"

    # 4. trendx_catalog.prediction_model.model_uri must exist (added by 008).
    assert _column_exists(
        params, "prediction_model", "model_uri"
    ), "trendx_catalog.prediction_model.model_uri missing after applying migration 008"

    # 5. Idempotence: re-applying 008 must succeed (IF NOT EXISTS).
    _psql_run(migration_008, params)
    assert _column_exists(
        params, "prediction_model", "model_uri"
    ), "trendx_catalog.prediction_model.model_uri missing after re-applying migration 008"


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
