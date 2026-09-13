"""Integration test for migration 014 (scheduler run persistence).

Applies migrations 000 -> 013, proves scheduler tables are ABSENT, applies
014, proves scheduler_run / scheduler_heartbeat exist with columns,
constraints, index and grants, then re-applies 014 for idempotence.

This test requires a live PostgreSQL. It is selected by the ``integration``
and ``migration_apply`` markers and only runs when ``--run-integration`` is
passed or a DB is reachable, consistent with tests/conftest.py. It runs
locally and in a dedicated CI job (never in the main integration job, which
excludes ``migration_apply``).

It MUST NOT modify any migration file, ORM model, or unrelated scope.
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
            cur.execute(
                """
                DO $$
                BEGIN
                  IF NOT EXISTS (
                    SELECT 1 FROM pg_roles
                    WHERE rolname = 'trendx_migration'
                  ) THEN
                    CREATE ROLE trendx_migration NOLOGIN;
                  END IF;
                END
                $$;
                """
            )
            cur.execute(
                """
                DO $$
                BEGIN
                  IF NOT EXISTS (
                    SELECT 1 FROM pg_roles
                    WHERE rolname = 'trendx_ro'
                  ) THEN
                    CREATE ROLE trendx_ro NOLOGIN;
                  END IF;
                END
                $$;
                """
            )
            cur.execute("GRANT trendx_migration TO trendx_app")
    finally:
        conn.close()


def _query(params: dict[str, str], sql: str, args: tuple = ()) -> list:
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
            cur.execute(sql, args)
            return list(cur.fetchall())
    finally:
        conn.close()


def _table_exists(params: dict[str, str], table: str) -> bool:
    return bool(
        _query(
            params,
            """
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = 'trendx_catalog' AND table_name = %s
            """,
            (table,),
        )
    )


def _columns(params: dict[str, str], table: str) -> dict[str, str]:
    return {
        name: dtype
        for name, dtype in _query(
            params,
            """
            SELECT column_name, data_type FROM information_schema.columns
            WHERE table_schema = 'trendx_catalog' AND table_name = %s
            """,
            (table,),
        )
    }


def test_migration_014_creates_scheduler_tables_additively_and_idempotently():
    params = _pg_conn_params()

    migration_014 = MIGRATIONS / "014_scheduler_runs.sql"
    assert migration_014.exists(), "migrations/014_scheduler_runs.sql missing"

    # 1. Schemas required by the chain (mirrors CI before_script).
    _ensure_schemas(params)

    # 2. Apply 000 -> 013 (existing chain). ON_ERROR_STOP fails on any error.
    chain = sorted(MIGRATIONS.glob("[0-9][0-9][0-9]_*.sql"))
    chain_013 = [f for f in chain if f.name < "014_scheduler_runs.sql"]
    assert chain_013[-1].name == "013_forecast_alerting_and_model_enrichment.sql"
    for f in chain_013:
        _psql_run(f, params)

    # 3. Scheduler tables must be ABSENT before 014 (additive proof).
    assert not _table_exists(params, "scheduler_run")
    assert not _table_exists(params, "scheduler_heartbeat")

    # 4. Apply 014.
    _psql_run(migration_014, params)

    # 5. Tables + columns + types.
    run_cols = _columns(params, "scheduler_run")
    assert run_cols.get("run_id") == "uuid"
    assert run_cols.get("job_id") == "text"
    assert run_cols.get("job_type") == "text"
    assert run_cols.get("status") == "text"
    assert run_cols.get("triggered_at") == "timestamp with time zone"
    assert run_cols.get("finished_at") == "timestamp with time zone"
    assert run_cols.get("error") == "text"
    assert run_cols.get("instance_id") == "text"
    assert run_cols.get("mode") == "text"
    assert run_cols.get("task_id") == "uuid"
    hb_cols = _columns(params, "scheduler_heartbeat")
    assert hb_cols.get("instance_id") == "text"
    assert hb_cols.get("leader") == "boolean"
    assert hb_cols.get("heartbeat_ts") == "timestamp with time zone"

    # 6. CHECK constraints reject unknown status/mode.
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
            cur.execute(
                "INSERT INTO trendx_catalog.scheduler_run"
                " (job_id, job_type, status, triggered_at, instance_id)"
                " VALUES ('j','t','bogus', now(), 'i')"
            )
            raise AssertionError("CHECK(status) did not reject 'bogus'")
    except AssertionError:
        raise
    except Exception:
        pass
    finally:
        conn.close()

    # 7. Index present.
    assert _query(
        params,
        "SELECT 1 FROM pg_indexes WHERE schemaname='trendx_catalog'"
        " AND indexname='ix_scheduler_run_job_triggered'",
    ), "index ix_scheduler_run_job_triggered missing"

    # 8. Grants: trendx_app has full CRUD.
    privs = _query(
        params,
        "SELECT privilege_type FROM information_schema.role_table_grants"
        " WHERE table_schema='trendx_catalog' AND table_name='scheduler_run'"
        " AND grantee='trendx_app'",
    )
    assert {"SELECT", "INSERT", "UPDATE", "DELETE"} <= {p[0] for p in privs}

    # 9. Idempotence: re-applying 014 must succeed.
    _psql_run(migration_014, params)
    assert _table_exists(params, "scheduler_run")
    assert _table_exists(params, "scheduler_heartbeat")


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
