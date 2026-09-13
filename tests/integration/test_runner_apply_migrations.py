"""Integration tests for the canonical migration runner (scripts/apply-migrations.sh).

Exercises the runner end-to-end against a REAL disposable PostgreSQL:

  * --check performs no write (no ledger created, no schema change) ;
  * --apply refuses without / with invalid TRENDX_CONFIRM_APPLY ;
  * --apply with TRENDX_CONFIRM_APPLY=YES applies the full 000->014 chain ;
  * strict numeric order 000..014 ;
  * stop-on-error: a failing migration aborts the run and is not recorded ;
  * idempotence / SKIP: re-applying records nothing new and prints [skip] ;
  * no secret (password / PGPASSWORD / DSN) ever appears in the runner output.

Run against a disposable PostgreSQL only (never production).
"""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

import pytest

from .test_fixtures import pg_query

ROOT = Path(__file__).parents[2]
RUNNER = ROOT / "scripts" / "apply-migrations.sh"

# Only the tests that apply the chain against a disposable PostgreSQL are
# integration tests. The confirmation-guard tests below are plain unit tests:
# they assert the runner refuses before any connection, so no DB is needed.
INTEGRATION = pytest.mark.integration

EXPECTED_CHAIN = [p.name for p in sorted(ROOT.glob("migrations/[0-9][0-9][0-9]_*.sql"))]


def _run_runner(params, mode, confirm=None):
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
        ["bash", str(RUNNER), mode],
        env=env,
        capture_output=True,
        text=True,
    )


def _ledger_names(params):
    """Return list of OK migration names, or [] if ledger missing/unreachable."""
    try:
        return pg_query(
            params,
            "SELECT migration_name FROM public.schema_version WHERE status='OK' "
            "ORDER BY applied_at",
        )
    except Exception:
        return []


@INTEGRATION
def test_runner_check_writes_nothing(provisioned_postgres):
    params = provisioned_postgres
    r = _run_runner(params, "--check")
    assert r.returncode == 0, f"--check failed:\n{r.stderr}"
    # No ledger table created, no schema created by --check.
    assert _ledger_names(params) == [], "ledger must not exist after --check (no write)"
    # trendx_catalog must not exist (000 not applied in check mode).
    cat = pg_query(
        params,
        "SELECT nspname FROM pg_namespace WHERE nspname='trendx_catalog'",
    )
    assert cat == [], "no schema may be created in --check mode"


def test_runner_apply_refuses_without_confirmation():
    # No TRENDX_CONFIRM_APPLY: refuse before any connection/write.
    params = {
        "host": "127.0.0.1",
        "port": 1,
        "user": "x",
        "password": "x",
        "dbname": "trendx",
    }
    r = _run_runner(params, "--apply", confirm=None)
    assert r.returncode != 0
    assert "BLOCKED" in r.stderr and "TRENDX_CONFIRM_APPLY" in r.stderr


def test_runner_apply_refuses_invalid_confirmation():
    params = {
        "host": "127.0.0.1",
        "port": 1,
        "user": "x",
        "password": "x",
        "dbname": "trendx",
    }
    for bad in ("no", "NO", "false", "YES ", "2", "maybe", ""):
        r = _run_runner(params, "--apply", confirm=bad)
        assert r.returncode != 0, f"accepted invalid confirmation: {bad!r}"
        assert "BLOCKED" in r.stderr


@INTEGRATION
def test_runner_apply_full_chain_000_to_014(provisioned_postgres):
    params = provisioned_postgres
    r = _run_runner(params, "--apply", confirm="YES")
    assert r.returncode == 0, f"apply failed:\n{r.stderr}"

    names = _ledger_names(params)
    assert set(names) == set(EXPECTED_CHAIN), f"chain mismatch:\n{names}"
    # Strict order: first and last.
    assert names[0] == "000_service_accounts.sql"
    assert names[-1] == "014_scheduler_runs.sql"

    # 000 created both schemas.
    assert pg_query(params, "SELECT 1 FROM pg_namespace WHERE nspname='trendx_catalog'")
    assert pg_query(params, "SELECT 1 FROM pg_namespace WHERE nspname='trendx_analytics'")


@INTEGRATION
def test_runner_stops_on_error_and_rolls_back(ephemeral_postgres):
    """Without the infra roles, a migration fails: the runner stops, no later
    migration is recorded, and the failing migration is not marked OK."""
    params = ephemeral_postgres  # roles NOT provisioned -> a migration will fail
    r = _run_runner(params, "--apply", confirm="YES")
    assert r.returncode != 0, "runner should have stopped on a failing migration"
    assert "FAILED" in r.stderr or "fail" in r.stderr.lower()

    names = _ledger_names(params)
    # 000 must be OK (it only grants to existing roles, conditionally).
    assert "000_service_accounts.sql" in names
    # The chain must NOT have reached the end.
    assert "012_native_partition_retention.sql" not in names
    # Fewer than the full chain were applied.
    assert len(names) < len(EXPECTED_CHAIN)


@INTEGRATION
def test_runner_skip_already_ok(provisioned_postgres):
    params = provisioned_postgres
    # First apply.
    r1 = _run_runner(params, "--apply", confirm="YES")
    assert r1.returncode == 0, f"first apply failed:\n{r1.stderr}"
    # Second apply: must skip everything, no re-apply.
    r2 = _run_runner(params, "--apply", confirm="VALID")
    assert r2.returncode == 0, f"second apply failed:\n{r2.stderr}"
    # Progress lines ([skip]/[apply]) are emitted on stderr by the runner.
    r2_out = r2.stdout + r2.stderr
    assert "[skip]" in r2_out, "second run must SKIP already-applied migrations"
    assert "[apply]" not in r2_out, "second run must not re-apply anything"
    # Ledger unchanged (still the full chain).
    assert set(_ledger_names(params)) == set(EXPECTED_CHAIN)


@INTEGRATION
def test_runner_output_contains_no_secret(provisioned_postgres):
    params = provisioned_postgres
    r = _run_runner(params, "--apply", confirm="true")
    combined = r.stdout + r.stderr
    assert params["password"] not in combined, "password leaked into runner output"
    assert "PGPASSWORD" not in combined, "PGPASSWORD leaked into runner output"
    assert "://" not in combined, "DSN leaked into runner output"
    # The runner must never surface the TRENDX_CONFIRM_APPLY value (the BLOCKED
    # branch prints the variable name, but a successful run must not).
    assert "BLOCKED" not in combined


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main([__file__, "-v"]))
