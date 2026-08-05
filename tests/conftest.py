"""Pytest configuration: skip integration tests when DB is unavailable."""

from __future__ import annotations

import pytest


def _db_available() -> bool:
    try:
        from trendx.database.connection import manager

        return manager.check_connection("catalog")
    except Exception:
        return False


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line(
        "markers",
        "integration: tests requiring external services (DB, API). "
        "Skips locally if the service is unavailable; runs in CI.",
    )


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    run_integration = config.getoption("--run-integration", default=False, action="store_true")
    for item in items:
        if item.get_closest_marker("integration"):
            if not run_integration and not _db_available():
                item.add_marker(
                    pytest.mark.skip(
                        reason="integration test: DB unavailable locally. "
                        "Run with --run-integration or in CI."
                    )
                )
