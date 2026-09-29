from pathlib import Path

ROOT = Path(__file__).parents[1]


def test_bootstrap_is_opt_in_and_secret_safe():
    script = (ROOT / "scripts/bootstrap-service-accounts.sh").read_text()
    assert "TRENDX_CONFIRM_APPLY:-} == YES" in script
    assert "set -x" not in script
    assert ".secrets/" in (ROOT / ".gitignore").read_text()
    assert "DROP DATABASE" not in script
    assert "datname = :'dbname'" in script
    assert "random_password" in script
    assert "persist_secret" in script
    assert "ALTER ROLE" in script


def test_thingsboard_requires_explicit_device_and_customer_confirmation():
    script = (ROOT / "scripts/configure-thingsboard-service-user.sh").read_text()
    assert "TB_CONFIRMED_DEVICE_ID" in script
    assert "TB_CONFIRMED_CUSTOMER_ID" in script
    assert "TB_CONFIRMED_CUSTOMER_DEVICE_COUNT" in script
    assert "TB_CONFIRMED_CUSTOMER_ISOLATED" in script
    assert "TB_CONFIRMED_CUSTOMER_ISOLATED} == true" in script
    assert "TB_WRITEBACK_ENABLED:-false} == false" in script
    assert "TB_ALARMS_ENABLED:-false} == false" in script
    assert "CUSTOMER_USER" in script


def test_sql_contains_default_revocations_and_read_only_grafana():
    """Single-DB dynamic contract (Correction A, ef45094).

    Migration 000 targets ONE database (``current_database()``, no ``\\connect``
    switching), grants CONNECT/USAGE dynamically (``DO $$`` over the real
    roles), scopes DML defaults to ``trendx_app``, and leaves ``trendx_grafana``
    read-only. The pre-ef45094 static multi-database pattern (``REVOKE ALL ON
    DATABASE``, ``:GRAFANA_DB_USER`` psql vars, repeated static GRANTs) is
    intentionally absent.
    """
    sql = (ROOT / "migrations/000_service_accounts.sql").read_text()
    # Dynamic single-DB block over the real infrastructure roles.
    assert "DO $$" in sql
    assert "current_database()" in sql
    assert "GRANT CONNECT ON DATABASE" in sql
    assert "GRANT USAGE ON SCHEMA trendx_catalog" in sql
    assert "GRANT USAGE ON SCHEMA trendx_analytics" in sql
    assert "ALTER DEFAULT PRIVILEGES IN SCHEMA trendx_analytics" in sql
    assert "trendx_grafana" in sql
    # DML defaults go to the application role only, never to Grafana.
    assert "TO trendx_app" in sql
    assert "TO trendx_grafana" not in sql
    # Obsolete static multi-database assumptions must stay out.
    assert "REVOKE ALL ON DATABASE" not in sql
    assert "GRAFANA_DB_USER" not in sql
    assert "\\connect" not in sql


def test_grafana_prevalidates_and_uses_versioned_folder_permission_method():
    script = (ROOT / "scripts/configure-grafana-service-account.sh").read_text()
    assert "folder absent; refusing before any mutation" in script
    assert "datasource not found" in script
    assert "GRAFANA_FOLDER_PERMISSION_METHOD:-PUT" in script
    assert 'gapi -X "$folder_permission_method"' in script
    assert script.index("folder=$(gapi") < script.index("/api/serviceaccounts/search")
