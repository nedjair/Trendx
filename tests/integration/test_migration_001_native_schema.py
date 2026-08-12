"""Integration test for migration 001 (Trendz native schema).

Applies ``migrations/001_trendz_native_schema.sql`` against a REAL PostgreSQL
instance via psql with ``ON_ERROR_STOP=1`` and proves:

  * the migration applies completely - psql exits 0 (no masked error, no
    ``|| true`` swallow);
  * every table declared by the migration is actually created, i.e. the
    application is FULL and not partial;
  * the migration file does NOT carry the stray ``\\restrict`` / ``\\unrestrict``
    restricted-mode artefact (regression guard for this chantier).

The PostgreSQL database is REAL (CI ``postgres:16-alpine`` service). The test is
fail-closed: any psql non-zero exit raises ``CalledProcessError`` and fails the
job; no swallowed exceptions, no artificial skip. It must NOT modify any other
migration, ORM model, or protected component, and must not touch the 006/007/008
chantiers.
"""

from __future__ import annotations

import os
import re
import subprocess
from pathlib import Path

import pytest

# Keep behaviour identical to the dedicated CI job (postgres:16-alpine service).
os.environ.setdefault("PG_ADMIN_HOST", "postgres")
os.environ.setdefault("PG_ADMIN_PORT", "5432")
os.environ.setdefault("PG_ADMIN_DB", "trendx")
os.environ.setdefault("PG_ADMIN_USER", "trendx_app")
os.environ.setdefault("PG_ADMIN_PASSWORD", "trendx_app_pass")
os.environ.setdefault("TRENDX_DB_NAME", "trendx")
os.environ.setdefault("TRENDX_DB_HOST", "postgres")
os.environ.setdefault("TRENDX_DB_PORT", "5432")
os.environ.setdefault("TRENDX_DB_USER", "trendx_app")
os.environ.setdefault("TRENDX_DB_PASSWORD", "trendx_app_pass")

ROOT = Path(__file__).parents[2]
MIGRATIONS = ROOT / "migrations"


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

    Raises subprocess.CalledProcessError (non-zero exit) on any failure.
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


def _public_base_table_count(params: dict[str, str]) -> int:
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
                SELECT count(*)
                FROM information_schema.tables
                WHERE table_schema = 'public'
                  AND table_type = 'BASE TABLE'
                """
            )
            return int(cur.fetchone()[0])
    finally:
        conn.close()


def _expected_table_count(migration_file: Path) -> int:
    text = migration_file.read_text(encoding="utf-8")
    return len(re.findall(r"CREATE TABLE\s+(?:IF NOT EXISTS\s+)?public\.", text))


@pytest.mark.integration
def test_migration_001_applies_completely_on_real_postgres():
    params = _pg_conn_params()
    migration_001 = next(MIGRATIONS.glob("001_*.sql"))
    assert migration_001.exists(), "migrations/001_*.sql missing"

    # 1. Apply 001 with ON_ERROR_STOP=1. A non-zero psql exit fails loudly.
    _psql_run(migration_001, params)

    # 2. Full (not partial) application: every declared table actually exists.
    expected = _expected_table_count(migration_001)
    assert expected > 0, "no CREATE TABLE found in migration 001"
    actual = _public_base_table_count(params)
    assert actual == expected, (
        f"migration 001 created {actual} public base tables, expected "
        f"{expected}. Application is partial or objects are missing."
    )

    # 3. Regression guard: the stray restricted-mode artefact must be gone so
    #    001 is pure SQL and never enters psql restricted mode.
    text = migration_001.read_text(encoding="utf-8")
    assert not re.search(
        r"^\\restrict\b", text, re.MULTILINE
    ), "migration 001 still carries a \\restrict restricted-mode artefact"
    assert not re.search(
        r"^\\unrestrict\b", text, re.MULTILINE
    ), "migration 001 still carries a \\unrestrict artefact"
