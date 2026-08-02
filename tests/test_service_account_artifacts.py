from pathlib import Path


ROOT = Path(__file__).parents[1]


def test_bootstrap_is_opt_in_and_secret_safe():
    script = (ROOT / "scripts/bootstrap-service-accounts.sh").read_text()
    assert 'TRENDX_CONFIRM_APPLY:-} == YES' in script
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
    sql = (ROOT / "migrations/000_service_accounts.sql").read_text()
    assert "REVOKE ALL ON DATABASE" in sql
    assert 'GRANT SELECT ON ALL TABLES IN SCHEMA public TO :"GRAFANA_DB_USER"' in sql
    assert sql.count('GRANT CONNECT ON DATABASE') >= 3
    assert sql.count('GRANT USAGE, CREATE ON SCHEMA public') >= 3


def test_grafana_prevalidates_and_uses_versioned_folder_permission_method():
    script = (ROOT / "scripts/configure-grafana-service-account.sh").read_text()
    assert "folder absent; refusing before any mutation" in script
    assert "datasource not found" in script
    assert "GRAFANA_FOLDER_PERMISSION_METHOD:-PUT" in script
    assert 'gapi -X "$folder_permission_method"' in script
    assert script.index("folder=$(gapi") < script.index("/api/serviceaccounts/search")
