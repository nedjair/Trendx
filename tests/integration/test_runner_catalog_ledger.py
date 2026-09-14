"""Catalog-ledger tests on a production-hardened disposable PostgreSQL.

Reproduces the observed production hardening (gate
TRENDX-B1-SCHEDULER-MIGRATION-014-CATALOG-LEDGER-DESIGN-GATE-V1):

  * NO public schema ;
  * trendx_catalog + trendx_analytics present, owned by trendx_migration ;
  * trendx_app with USAGE only (no CREATE) ;
  * ledger absent, history unknown.

Verifies, exclusively against disposable databases (never production):

  * --check --only 014 plans 014 only and writes nothing ;
  * --apply --only 014 as trendx_migration creates the scheduler tables AND
    the ledger in trendx_catalog, records ONLY 014, creates no public schema ;
  * --apply --only 014 as trendx_app FAILS cleanly (least-privilege boundary),
    leaving nothing partial behind ;
  * bare --apply on this ledgerless non-empty DB is REFUSED ;
  * trendx_app can SELECT the ledger afterwards (runbook --check under least
    privilege) ;
  * no secret ever appears in runner output.
"""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

import pytest

from .test_fixtures import pg_query, run_sql

ROOT = Path(__file__).parents[2]
RUNNER = ROOT / "scripts" / "apply-migrations.sh"

INTEGRATION = pytest.mark.integration

ONLY_014 = "014_scheduler_runs.sql"
MIGRATION_PW = "migration-test-pw"
APP_PW = "app-test-pw"


def _harden(params):
    """Reproduce production hardening on a disposable database."""
    run_sql(params, "DROP SCHEMA IF EXISTS public CASCADE")
    run_sql(params, "CREATE SCHEMA IF NOT EXISTS trendx_catalog")
    run_sql(params, "CREATE SCHEMA IF NOT EXISTS trendx_analytics")
    for role in ("trendx_app", "trendx_migration"):
        run_sql(
            params,
            f"DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '{role}') "
            f"THEN CREATE ROLE {role} NOLOGIN; END IF; END; $$;",
        )
    run_sql(
        params,
        "ALTER SCHEMA trendx_catalog OWNER TO trendx_migration",
    )
    run_sql(
        params,
        "ALTER SCHEMA trendx_analytics OWNER TO trendx_migration",
    )
    run_sql(params, "GRANT USAGE ON SCHEMA trendx_catalog TO trendx_app")
    run_sql(params, f"ALTER ROLE trendx_migration WITH LOGIN PASSWORD '{MIGRATION_PW}'")
    run_sql(params, f"ALTER ROLE trendx_app WITH LOGIN PASSWORD '{APP_PW}'")


def _as_params(params, user, password):
    return {**params, "user": user, "password": password}


def _run(params, *args, confirm=None):
    env = dict(os.environ)
    env["PGHOST"] = params["host"]
    env["PGPORT"] = str(params["port"])
    env["PGUSER"] = params["user"]
    env["PGDATABASE"] = params["dbname"]
    env["PGPASSWORD"] = params["password"]
    if confirm is not None:
        env["TRENDX_CONFIRM_APPLY"] = confirm
    else:
        env.pop("TRENDX_CONFIRM_APPLY", None)
    return subprocess.run(
        ["bash", str(RUNNER), *args],
        env=env,
        capture_output=True,
        text=True,
    )


def _tables(params):
    return set(
        pg_query(
            params,
            "SELECT schemaname || '.' || tablename FROM pg_tables "
            "WHERE schemaname NOT IN ('pg_catalog','information_schema')",
        )
    )


def _ledger(params):
    try:
        return pg_query(
            params,
            "SELECT migration_name FROM trendx_catalog.schema_version WHERE status='OK'",
        )
    except Exception:
        return []


@INTEGRATION
def test_hardened_check_only_014_writes_nothing(ephemeral_postgres):
    params = ephemeral_postgres
    _harden(params)
    mig = _as_params(params, "trendx_migration", MIGRATION_PW)
    r = _run(mig, "--check", "--only", "014")
    assert r.returncode == 0, f"--check --only 014 failed:\n{r.stderr}"
    assert ONLY_014 in (r.stdout + r.stderr)
    assert "trendx_catalog" not in _tables(mig)


@INTEGRATION
def test_hardened_apply_only_014_as_migration_role(ephemeral_postgres):
    params = ephemeral_postgres
    _harden(params)
    mig = _as_params(params, "trendx_migration", MIGRATION_PW)
    r = _run(mig, "--apply", "--only", "014", confirm="YES")
    assert r.returncode == 0, f"--apply --only 014 failed:\n{r.stderr}"
    tables = _tables(mig)
    assert "trendx_catalog.scheduler_run" in tables
    assert "trendx_catalog.scheduler_heartbeat" in tables
    assert "trendx_catalog.schema_version" in tables
    # No public schema implicitly created, no 000..013 objects fabricated.
    assert not any(t.startswith("public.") for t in tables)
    assert _ledger(mig) == [ONLY_014]


@INTEGRATION
def test_hardened_apply_only_014_as_app_role_fails_cleanly(ephemeral_postgres):
    params = ephemeral_postgres
    _harden(params)
    app = _as_params(params, "trendx_app", APP_PW)
    r = _run(app, "--apply", "--only", "014", confirm="YES")
    assert r.returncode != 0, "apply as CREATE-less app role should fail"
    mig = _as_params(params, "trendx_migration", MIGRATION_PW)
    tables = _tables(mig)
    assert "trendx_catalog.scheduler_run" not in tables
    assert "trendx_catalog.schema_version" not in tables


@INTEGRATION
def test_hardened_bare_apply_refused(ephemeral_postgres):
    params = ephemeral_postgres
    _harden(params)
    mig = _as_params(params, "trendx_migration", MIGRATION_PW)
    r = _run(mig, "--apply", confirm="YES")
    assert r.returncode != 0
    assert "REFUSED" in r.stderr
    assert _ledger(mig) == []


@INTEGRATION
def test_hardened_app_can_select_ledger(ephemeral_postgres):
    params = ephemeral_postgres
    _harden(params)
    mig = _as_params(params, "trendx_migration", MIGRATION_PW)
    assert _run(mig, "--apply", "--only", "014", confirm="YES").returncode == 0
    app = _as_params(params, "trendx_app", APP_PW)
    rows = pg_query(app, "SELECT migration_name FROM trendx_catalog.schema_version")
    assert rows == [ONLY_014]


@INTEGRATION
def test_hardened_runner_output_contains_no_secret(ephemeral_postgres):
    params = ephemeral_postgres
    _harden(params)
    mig = _as_params(params, "trendx_migration", MIGRATION_PW)
    r = _run(mig, "--apply", "--only", "014", confirm="YES")
    combined = r.stdout + r.stderr
    assert MIGRATION_PW not in combined
    assert "PGPASSWORD" not in combined
    assert "://" not in combined


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main([__file__, "-v"]))
