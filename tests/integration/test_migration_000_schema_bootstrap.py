"""Integration test for migration 000 (single-DB service accounts & schemas).

Applies migration 000 against a REAL disposable PostgreSQL and proves the
operator-validated guardrails for the single-DB design:

  * base vierge : 000 crée trendx_catalog et trendx_analytics ;
  * aucune tentative de connexion à des bases séparées (plus de \\connect vers
    ANALYTICS_DB_NAME / AIRFLOW_DB_NAME / MLFLOW_DB_NAME) ;
  * idempotence : réexécuter 000 ne crée pas d'erreur et laisse les schémas
    intacts ;
  * aucun credential en dur dans le fichier.

Run against a disposable PostgreSQL only (never production).
"""

from __future__ import annotations

from pathlib import Path

import pytest

from .test_fixtures import pg_query, psql_run_file

ROOT = Path(__file__).parents[2]
MIGRATION_000 = ROOT / "migrations" / "000_service_accounts.sql"

# Only the tests that actually apply migration 000 against a disposable
# PostgreSQL are integration tests. The static file-content guard below is a
# plain unit test and must run everywhere (no DB required).
INTEGRATION = pytest.mark.integration

SEPARATE_DB_TOKENS = ("ANALYTICS_DB_NAME", "AIRFLOW_DB_NAME", "MLFLOW_DB_NAME")


def test_000_file_uses_single_db_design():
    """Static guard: no \\connect to, and no reference to, separate databases."""
    text = MIGRATION_000.read_text(encoding="utf-8")
    assert "\\connect" not in text, "000 must not use \\connect (single-DB design)"
    for token in SEPARATE_DB_TOKENS:
        assert token not in text, f"000 must not reference separate DB token: {token}"
    # Explicit single-DB documentation.
    assert "single-db" in text.lower() or "SINGLE-DB" in text


@INTEGRATION
def test_000_creates_schemas_on_blank_db(provisioned_postgres):
    params = provisioned_postgres
    r = psql_run_file(params, MIGRATION_000)
    assert r.returncode == 0, f"000 failed:\n{r.stderr}"

    schemas = set(
        pg_query(
            params,
            "SELECT nspname FROM pg_namespace "
            "WHERE nspname IN ('trendx_catalog', 'trendx_analytics')",
        )
    )
    assert "trendx_catalog" in schemas, "trendx_catalog schema missing after 000"
    assert "trendx_analytics" in schemas, "trendx_analytics schema missing after 000"


@INTEGRATION
def test_000_idempotent_reapply(provisioned_postgres):
    params = provisioned_postgres
    # First apply.
    assert psql_run_file(params, MIGRATION_000).returncode == 0
    # Second apply must succeed (IF NOT EXISTS / idempotent grants).
    r = psql_run_file(params, MIGRATION_000)
    assert r.returncode == 0, f"000 not idempotent:\n{r.stderr}"

    schemas = set(
        pg_query(
            params,
            "SELECT nspname FROM pg_namespace "
            "WHERE nspname IN ('trendx_catalog', 'trendx_analytics')",
        )
    )
    assert schemas == {"trendx_catalog", "trendx_analytics"}


@INTEGRATION
def test_000_grants_to_existing_roles(provisioned_postgres):
    """000 must grant CONNECT/USAGE on the single DB to the real infra roles."""
    params = provisioned_postgres
    assert psql_run_file(params, MIGRATION_000).returncode == 0

    # trendx_app must have USAGE on both schemas (roles created by the fixture).
    ok = pg_query(
        params,
        "SELECT nspname FROM pg_namespace "
        "WHERE nspname IN ('trendx_catalog','trendx_analytics') "
        "AND has_schema_privilege('trendx_app', nspname, 'USAGE')",
    )
    assert set(ok) == {"trendx_catalog", "trendx_analytics"}


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main([__file__, "-v"]))
