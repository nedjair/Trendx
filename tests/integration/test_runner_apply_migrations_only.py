"""Tests for the 014-only selection of the canonical migration runner.

Covers the 014-only contract (gate
TRENDX-B1-SCHEDULER-MIGRATION-014-ONLY-PROCEDURE-DESIGN-GATE-V1):

  * Cas A: ledger absent + --check --only 014 => plan = 014 uniquement, no write.
  * Cas B: ledger absent + --apply --only 014 => executes 014 only, records
    ONLY 014 (000..013 rows are never fabricated).
  * Cas C: ledger absent + --apply (no --only) on a NON-EMPTY database
    => REFUSED (unknown history; no blind full-chain replay).
  * Cas D: --only 000/013/015/non-numeric/several selections => REFUSED,
    before any connection when possible (unit tests, no DB needed).
  * Cas E: --only 014 with the 014 file missing => REFUSED.
  * Cas F: --only 014 already applied => idempotent SKIP, documented.
  * Cas G: normal --check/--apply behaviour with a valid ledger is unchanged
    (covered by test_runner_apply_migrations.py; one explicit --check assert here).

All database tests run against disposable PostgreSQL only (never production).
"""

from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

import pytest

from .test_fixtures import pg_query, run_sql

ROOT = Path(__file__).parents[2]
RUNNER = ROOT / "scripts" / "apply-migrations.sh"

INTEGRATION = pytest.mark.integration

ONLY_014 = "014_scheduler_runs.sql"


def _env(params, confirm=None):
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
    return env


def _run(params, *args, confirm=None, runner=None):
    return subprocess.run(
        ["bash", str(runner or RUNNER), *args],
        env=_env(params, confirm),
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


def _dummy_params():
    # Unreachable connection: only usable for refusals decided before connect.
    return {"host": "127.0.0.1", "port": 1, "user": "x", "password": "x", "dbname": "trendx"}


# --- Cas D : refusals decided before any connection (unit, no DB) -------------


@pytest.mark.parametrize("bad", ["000", "013", "015", "00", "14", "abc", "014 "])
def test_only_unsupported_single_value_refused(bad):
    r = _run(_dummy_params(), "--check", "--only", bad)
    assert r.returncode != 0, f"--only {bad!r} was not refused"
    assert "REFUSED" in r.stderr


def test_only_several_selections_refused():
    r = _run(_dummy_params(), "--check", "--only", "014", "--only", "014")
    assert r.returncode != 0
    assert "REFUSED" in r.stderr


def test_only_missing_value_refused():
    r = _run(_dummy_params(), "--check", "--only")
    assert r.returncode != 0
    assert "REFUSED" in r.stderr


# --- Cas A : ledger absent + --check --only 014 --------------------------------


@INTEGRATION
def test_check_only_014_plans_014_only_and_writes_nothing(provisioned_postgres):
    params = provisioned_postgres  # roles exist, ledger absent, no user objects
    r = _run(params, "--check", "--only", "014")
    assert r.returncode == 0, f"--check --only 014 failed:\n{r.stderr}"
    out = r.stdout + r.stderr
    assert ONLY_014 in out
    assert "014-only selection" in out
    assert "[apply]  000_" not in out and "[apply]  013_" not in out
    # Read-only: no ledger table, no scheduler tables created by --check.
    assert _ledger(params) == []
    assert "trendx_catalog.scheduler_run" not in _tables(params)


# --- Cas B : ledger absent + --apply --only 014 --------------------------------


@INTEGRATION
def test_apply_only_014_executes_014_only(provisioned_postgres):
    params = provisioned_postgres
    # Simulate the production state: 000..013 objects exist (schema present),
    # ledger absent (history unknown). Minimal faithful setup: the schema.
    run_sql(params, "CREATE SCHEMA IF NOT EXISTS trendx_catalog")
    r = _run(params, "--apply", "--only", "014", confirm="YES")
    assert r.returncode == 0, f"--apply --only 014 failed:\n{r.stderr}"
    out = r.stdout + r.stderr
    assert "014-only selection" in out
    tables = _tables(params)
    assert "trendx_catalog.scheduler_run" in tables
    assert "trendx_catalog.scheduler_heartbeat" in tables
    # Ledger records ONLY 014 — 000..013 rows are never fabricated.
    assert _ledger(params) == [ONLY_014]
    # Nothing from 000..013 was executed by this procedure.
    assert "trendx_catalog.trendz_task" not in tables


# --- Cas C : ledger absent + bare --apply on non-empty DB => REFUSED -----------


@INTEGRATION
def test_apply_without_only_refused_on_ledgerless_nonempty_db(provisioned_postgres):
    params = provisioned_postgres
    run_sql(params, "CREATE SCHEMA IF NOT EXISTS trendx_catalog")
    run_sql(
        params,
        "CREATE TABLE IF NOT EXISTS trendx_catalog.preexisting_probe (id INT PRIMARY KEY)",
    )
    r = _run(params, "--apply", confirm="YES")
    assert r.returncode != 0, "bare --apply on ledgerless non-empty DB was not refused"
    assert "REFUSED" in r.stderr
    # Refusal happens before ensure_ledger: no ledger table may be created.
    assert _ledger(params) == []
    assert "trendx_catalog.schema_version" not in _tables(params)


# --- Cas E : --only 014 with the 014 file missing => REFUSED -------------------


@INTEGRATION
def test_only_014_missing_file_refused(provisioned_postgres, tmp_path):
    params = provisioned_postgres
    # Minimal repo layout without 014 (000..013 present so the chain check passes).
    (tmp_path / "scripts").mkdir()
    (tmp_path / "migrations").mkdir()
    shutil.copy(ROOT / "scripts" / "apply-migrations.sh", tmp_path / "scripts")
    for src in sorted((ROOT / "migrations").glob("[0-9][0-9][0-9]_*.sql")):
        if not src.name.startswith("014_"):
            shutil.copy(src, tmp_path / "migrations")
    runner = tmp_path / "scripts" / "apply-migrations.sh"
    r = _run(params, "--check", "--only", "014", runner=runner)
    assert r.returncode != 0
    assert "REFUSED" in r.stderr


# --- Cas F : already applied => idempotent SKIP ---------------------------------


@INTEGRATION
def test_apply_only_014_idempotent_when_already_ok(provisioned_postgres):
    params = provisioned_postgres
    run_sql(params, "CREATE SCHEMA IF NOT EXISTS trendx_catalog")
    r1 = _run(params, "--apply", "--only", "014", confirm="YES")
    assert r1.returncode == 0, f"first apply failed:\n{r1.stderr}"
    r2 = _run(params, "--apply", "--only", "014", confirm="YES")
    assert r2.returncode == 0, f"second apply failed:\n{r2.stderr}"
    out = r2.stdout + r2.stderr
    assert "[skip]" in out
    assert _ledger(params) == [ONLY_014]


# --- Cas G : normal behaviour with a valid ledger is unchanged ------------------


@INTEGRATION
def test_check_lists_skip_once_ledger_valid(provisioned_postgres):
    params = provisioned_postgres
    run_sql(params, "CREATE SCHEMA IF NOT EXISTS trendx_catalog")
    assert _run(params, "--apply", "--only", "014", confirm="YES").returncode == 0
    r = _run(params, "--check", "--only", "014")
    assert r.returncode == 0
    assert "[skip]" in (r.stdout + r.stderr)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main([__file__, "-v"]))
