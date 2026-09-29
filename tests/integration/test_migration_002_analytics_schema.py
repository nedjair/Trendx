"""Integration test for migration 002 (analytics schema qualification).

Two layers of validation:

  * Static (always runs, no DB): the file qualifies analytics objects with
    ``trendx_analytics.``, targets the single base ``trendx`` in GRANT CONNECT,
    and carries NO legacy internal ledger (the runner records every migration
    in ``trendx_catalog.schema_version``, 75e1422). This proves the audit
    corrections (002 -> trendx_analytics) without needing a TimescaleDB image.
  * Dynamic (disposable native PostgreSQL): when TimescaleDB is ABSENT, 002 is a
    no-op — it must NOT leak any object into ``public`` and must be idempotent.

The TimescaleDB code path (hypertables/CAGGs) is only exercised when the
extension is really present; that path is NOT validated here (no TSDB image in
this environment) and is documented as a limitation in the final report.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from .test_fixtures import pg_query, psql_run_file

ROOT = Path(__file__).parents[2]
MIGRATION_002 = ROOT / "migrations" / "002_hypertables_analytics.sql"

# Only the dynamic (disposable-PostgreSQL) tests are integration tests. The
# static file-content audit below is a plain unit test (no DB required).
INTEGRATION = pytest.mark.integration


def test_002_qualifies_analytics_objects_statically():
    """Audit corrections are present in the file (no TSDB image required)."""
    text = MIGRATION_002.read_text(encoding="utf-8")

    # Analytics objects must be qualified with the trendx_analytics schema.
    assert "trendx_analytics.ts_kv" in text
    assert "trendx_analytics.predictions" in text
    assert "trendx_analytics.anomaly_scores" in text
    assert "trendx_analytics.data_quality" in text
    assert "trendx_analytics.ml_metrics" in text

    # GRANT CONNECT must target the single base `trendx`, not a separate DB.
    assert "GRANT CONNECT ON DATABASE trendx TO trendx_app" in text
    # Old bug: GRANT CONNECT on a non-existent analytics database.
    assert "GRANT CONNECT ON DATABASE trendx_analytics" not in text

    # USAGE/DEFAULT PRIVILEGES must target trendx_analytics, not public.
    assert "GRANT USAGE ON SCHEMA trendx_analytics TO trendx_app" in text
    assert "GRANT USAGE ON SCHEMA public TO trendx_app" not in text
    assert "ALTER DEFAULT PRIVILEGES IN SCHEMA trendx_analytics" in text
    assert "ALTER DEFAULT PRIVILEGES IN SCHEMA public" not in text

    # Ledger is runner-exclusive (trendx_catalog.schema_version, 75e1422):
    # the file must not carry a legacy internal ledger (bare or public),
    # which would fail on hardened databases where public is absent.
    assert "public.schema_version" not in text
    assert "CREATE TABLE IF NOT EXISTS schema_version" not in text


@INTEGRATION
def test_002_native_is_noop_and_leaks_nothing_in_public(provisioned_postgres):
    """On native PostgreSQL (no TimescaleDB) 002 must not create objects in public."""
    params = provisioned_postgres
    r = psql_run_file(params, MIGRATION_002)
    assert r.returncode == 0, f"002 failed on native PG:\n{r.stderr}"

    # No analytics object may appear in public (anti-leak guard). The native
    # path is a no-op, so these tables must not exist anywhere either.
    public_tables = set(
        pg_query(
            params,
            "SELECT tablename FROM pg_tables WHERE schemaname='public' "
            "AND tablename IN ('ts_kv','ts_kv_hourly','predictions','anomaly_scores')",
        )
    )
    assert public_tables == set(), f"002 leaked objects into public: {public_tables}"


@INTEGRATION
def test_002_native_idempotent(provisioned_postgres):
    params = provisioned_postgres
    assert psql_run_file(params, MIGRATION_002).returncode == 0
    r = psql_run_file(params, MIGRATION_002)
    assert r.returncode == 0, f"002 not idempotent on native PG:\n{r.stderr}"


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main([__file__, "-v"]))
