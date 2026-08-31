"""Pytest configuration: integration tests run when a DB is reachable.

Locally they are skipped if no DB is available; in CI they fail loudly so the
job can never report green while executing zero integration tests (fail-open bug).
"""

from __future__ import annotations

import os
import secrets

import pytest


def _db_available() -> bool:
    try:
        from trendx.database.connection import manager

        return manager.check_connection("catalog")
    except Exception:
        return False


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--run-integration",
        action="store_true",
        default=False,
        help="Run integration tests even if DB is unavailable.",
    )


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line(
        "markers",
        "integration: tests requiring external services (DB, API). "
        "Skips locally if the service is unavailable; runs in CI.",
    )
    config.addinivalue_line(
        "markers",
        "migration_apply: tests that (re-)apply a non-idempotent migration "
        "(e.g. 001) and therefore require a FRESH database. Excluded from the "
        "shared test-integration job, which runs against a pre-migrated DB.",
    )


def _in_ci() -> bool:
    # GitLab sets CI=true (and GITLAB_CI=true) for every pipeline job.
    # Distinguishing CI from a developer's laptop prevents a fail-open scenario:
    # in CI the DB *must* be reachable, so an unavailable DB is a hard failure,
    # not a quiet skip that leaves the job green while running zero tests.
    return bool(os.environ.get("CI")) or bool(os.environ.get("GITLAB_CI"))


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    markexpr = (getattr(config.option, "markexpr", "") or "").lower()
    # The fail-closed enforcement only applies when this pytest run actually selects
    # the integration suite (pytest -m integration). Other invocations (pytest -m unit,
    # or a default run) must never be aborted because an integration-marked test happens
    # to be collected: those tests are simply skipped. This relies on the existing,
    # standard -m marker selection rather than introducing a new env variable.
    running_integration_suite = "integration" in markexpr
    db_available = _db_available() if running_integration_suite else False
    for item in items:
        if not item.get_closest_marker("integration"):
            continue
        if not running_integration_suite:
            # Integration tests are not selected by this run (e.g. the unit job).
            # Skip them; never abort the whole session.
            item.add_marker(
                pytest.mark.skip(reason="integration test: not selected by -m in this run.")
            )
            continue
        # Tests that provision their own disposable PostgreSQL (ephemeral /
        # provisioned fixtures) do NOT depend on the external catalog DB, so the
        # catalog-reachability guard below must not block them. They self-skip via
        # the fixture if Docker / a test DB is unavailable.
        uses_disposable_db = bool(
            {"ephemeral_postgres", "provisioned_postgres"} & set(item.fixturenames)
        )
        if uses_disposable_db:
            continue
        # Integration suite selected: the DB must be reachable.
        if db_available:
            continue
        if _in_ci():
            raise pytest.UsageError(
                "integration tests: database unreachable in CI "
                f"(CI={os.environ.get('CI')!r}). The test-integration job's "
                "PostgreSQL service did not come up or migrations failed. "
                "This is a CI setup error, not a test failure."
            )
        # Locally (--run-integration or not) a DB-dependent integration test with
        # no reachable catalog DB is skipped — it cannot be exercised here. The
        # fail-open guarantee is preserved for CI above.
        item.add_marker(
            pytest.mark.skip(reason="integration test: external catalog DB unreachable locally.")
        )


# ————————————————————————————————————————
# Disposable PostgreSQL fixtures for migration tests
# ————————————————————————————————————————
@pytest.fixture
def ephemeral_postgres():
    """Yield connection params for a fresh, empty PostgreSQL database (trendx).

    Uses the CI ``postgres`` service when TRENDX_TEST_PGPORT is set, otherwise
    starts a disposable Docker container. Skips if neither is available.

    When a shared service database is provided via env vars, a dedicated
    database is created (and dropped on teardown) so the "fresh, empty" contract
    holds even though the underlying PostgreSQL instance is shared across jobs
    and fixtures. Without this isolation, an earlier ``provisioned_postgres``
    test would pre-populate the shared ledger and a later ``ephemeral_postgres``
    test would find every migration already recorded OK and skip them all --
    which makes tests that must observe a failing migration report green
    spuriously (see test_runner_stops_on_error_and_rolls_back).
    """
    from .integration.test_fixtures import run_sql, start_params, stop_params

    try:
        params = start_params()
    except RuntimeError as exc:
        pytest.skip(str(exc))

    # A disposable Docker container is already fresh per invocation; only the
    # shared service database needs an isolated database created for it.
    service_db = params.get("dbname")
    dedicated_db = None
    if "_container" not in params:
        dedicated_db = f"trendx_eph_{secrets.token_hex(6)}"
        run_sql({**params, "dbname": service_db}, f'CREATE DATABASE "{dedicated_db}"')
        params = {**params, "dbname": dedicated_db}
        # On a shared service instance the infra roles created by earlier
        # ``provisioned_postgres`` fixtures persist cluster-wide (roles are not
        # per-database). Drop them so this fresh database observes a genuine
        # migration failure, as the "fresh, empty" contract requires. By the
        # time this fixture runs, prior dedicated databases have been dropped on
        # teardown, so the roles own no objects here and DROP ROLE is safe.
        for role in ("trendx_app", "trendx_migration", "trendx_grafana", "trendx_ro"):
            try:
                run_sql({**params, "dbname": service_db}, f'DROP OWNED BY "{role}"')
            except Exception:
                pass
            try:
                run_sql({**params, "dbname": service_db}, f'DROP ROLE IF EXISTS "{role}"')
            except Exception:
                pass

    try:
        yield params
    finally:
        if dedicated_db is not None:
            # Connect to the service database (not the target) and force any
            # lingering sessions closed before dropping.
            try:
                run_sql(
                    {**params, "dbname": service_db},
                    f'DROP DATABASE IF EXISTS "{dedicated_db}" WITH (FORCE)',
                )
            except Exception:
                pass
        stop_params(params)


@pytest.fixture
def provisioned_postgres(ephemeral_postgres):
    """Disposable PostgreSQL with the real infra roles created (NOLOGIN, no secret).

    Mirrors what the infrastructure bootstrap provisions: trendx_app,
    trendx_migration, trendx_grafana, trendx_ro. The migration chain (notably 000
    grants, 007 OWNER/GRANT) requires them to exist.
    """
    from .integration.test_fixtures import run_sql

    params = ephemeral_postgres
    for role in ("trendx_app", "trendx_migration", "trendx_grafana", "trendx_ro"):
        run_sql(
            params,
            f"DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '{role}') "
            f"THEN CREATE ROLE {role} NOLOGIN; END IF; END; $$;",
        )
    yield params
