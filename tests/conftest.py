"""Pytest configuration: integration tests run when a DB is reachable.

Locally they are skipped if no DB is available; in CI they fail loudly so the
job can never report green while executing zero integration tests (fail-open bug).
"""

from __future__ import annotations

import os

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
    run_integration = config.getoption("--run-integration")
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
        if run_integration:
            raise pytest.UsageError(
                "integration tests: database unreachable locally despite "
                "--run-integration. Point TRENDX_DB_* at a reachable "
                "PostgreSQL, or unset --run-integration."
            )
        item.add_marker(
            pytest.mark.skip(
                reason="integration test: DB unavailable locally. "
                "Run with --run-integration against a reachable DB, or in CI."
            )
        )
