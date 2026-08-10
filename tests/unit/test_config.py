from __future__ import annotations

import os
from pathlib import Path
from unittest.mock import patch

import pytest
from pydantic import SecretStr
from pydantic_settings import SettingsConfigDict
from trendx.config import Settings, settings


class _TestSettings(Settings):
    model_config = SettingsConfigDict(
        env_file=None,
        env_file_encoding="utf-8",
        case_sensitive=True,
        extra="ignore",
    )


@pytest.mark.unit
def test_settings_loading():
    assert settings.trendx_env in ("development", "production", "testing")
    assert settings.tb_base_url is not None
    assert isinstance(settings.tb_password, SecretStr)


@pytest.mark.unit
def test_dsn_generation():
    s = _TestSettings(
        TRENDX_ENV="testing",
        TB_BASE_URL="http://test:8080",
        TB_USERNAME="test",
        TB_PASSWORD="test",
        PG_ADMIN_HOST="pg-host",
        PG_ADMIN_PORT=5432,
        PG_ADMIN_DB="postgres",
        PG_ADMIN_USER="admin",
        PG_ADMIN_PASSWORD="secret",
        TRENDX_CATALOG_DB_NAME="trendx",
        TRENDX_APP_USER="trendx_app",
        TRENDX_APP_PASSWORD="trendx_app_pass",
    )

    dsn = s.catalog_dsn_app()
    assert dsn.startswith("postgresql://")
    assert "trendx_app" in dsn
    assert "pg-host" in dsn
    assert "trendx" in dsn

    analytics_dsn = s.analytics_dsn_app()
    assert analytics_dsn == dsn  # une seule base trendx ; schémas séparés par search_path


@pytest.mark.unit
def test_tb_readonly_dsn():
    with patch.dict(os.environ, {}, clear=True):
        s = _TestSettings(
            TRENDX_ENV="testing",
            TB_BASE_URL="http://test:8080",
            TB_USERNAME="test",
            TB_PASSWORD="test",
            TB_DB_HOST="tb-host",
            TB_DB_PORT=32768,
            TB_DB_NAME="thingsboard",
        )
        dsn = s.tb_db_readonly_dsn()
        assert dsn is None

    with patch.dict(os.environ, {}, clear=True):
        s = _TestSettings(
            TRENDX_ENV="testing",
            TB_BASE_URL="http://test:8080",
            TB_USERNAME="test",
            TB_PASSWORD="test",
            TB_DB_HOST="tb-host",
            TB_DB_PORT=32768,
            TB_DB_NAME="thingsboard",
            TB_DB_READONLY_USER="reader",
            TB_DB_READONLY_PASSWORD="reader_pass",
        )
        dsn = s.tb_db_readonly_dsn()
        assert dsn is not None
        assert "reader" in dsn
        assert "tb-host" in dsn


@pytest.mark.unit
def test_settings_from_env():
    with patch.dict(
        os.environ,
        {
            "TRENDX_ENV": "testing",
            "TB_BASE_URL": "https://custom:8081",
            "TB_USERNAME": "custom@test.com",
            "TB_PASSWORD": "custom_pass",
            "FORECAST_HORIZON": "48",
            "ANOMALY_CONTAMINATION": "0.05",
        },
        clear=True,
    ):
        s = _TestSettings()
        assert s.trendx_env == "testing"
        assert s.tb_base_url == "https://custom:8081"
        assert s.tb_password.get_secret_value() == "custom_pass"
        assert s.forecast_horizon == 48
        assert s.anomaly_contamination == 0.05


@pytest.mark.unit
def test_settings_defaults():
    s = _TestSettings(
        TRENDX_ENV="testing",
        TB_BASE_URL="http://test:8080",
        TB_USERNAME="test",
        TB_PASSWORD="test",
    )
    assert s.forecast_horizon == 24
    assert s.forecast_frequency == "1h"
    assert s.trendx_log_level == "INFO"


@pytest.mark.unit
def test_optional_services_fields_present():
    """grafana_host_port et redis_url doivent exister : consommés par
    GET /, GET /api/v1/admin/config et CacheService (AttributeError sinon)."""
    with patch.dict(os.environ, {}, clear=True):
        s = _TestSettings(
            TRENDX_ENV="testing",
            TB_BASE_URL="http://test:8080",
            TB_USERNAME="test",
            TB_PASSWORD="test",
        )
        assert s.grafana_host_port == 3001  # 3000 occupé par Gitea sur l'hôte
        assert s.redis_url == "redis://localhost:6379/0"


@pytest.mark.unit
def test_optional_services_fields_from_env():
    with patch.dict(
        os.environ,
        {
            "TRENDX_ENV": "testing",
            "TB_BASE_URL": "http://test:8080",
            "TB_USERNAME": "test",
            "TB_PASSWORD": "test",
            "GRAFANA_HOST_PORT": "3002",
            "REDIS_URL": "redis://cache-host:6380/1",
        },
        clear=True,
    ):
        s = _TestSettings()
        assert s.grafana_host_port == 3002
        assert s.redis_url == "redis://cache-host:6380/1"


@pytest.mark.unit
def test_log_level_uppercased():
    s = _TestSettings(
        TRENDX_ENV="testing",
        TRENDX_LOG_LEVEL="debug",
        TB_BASE_URL="http://test:8080",
        TB_USERNAME="test",
        TB_PASSWORD="test",
    )
    assert s.trendx_log_level == "DEBUG"


@pytest.mark.unit
def test_secret_str_not_in_repr():
    s = _TestSettings(
        TRENDX_ENV="testing",
        TB_BASE_URL="http://test:8080",
        TB_USERNAME="test",
        TB_PASSWORD="super-secret-value",
    )
    rep = repr(s)
    assert "super-secret-value" not in rep


@pytest.mark.unit
def test_mask_dsn():
    s = _TestSettings(
        TRENDX_ENV="testing",
        TB_BASE_URL="http://test:8080",
        TB_USERNAME="test",
        TB_PASSWORD="test",
        PG_ADMIN_HOST="pg-host",
        PG_ADMIN_PORT=5432,
        TRENDX_APP_PASSWORD="leaked_password_123",
    )
    raw = s.catalog_dsn_app()
    masked = s.mask_dsn(raw)
    assert "leaked_password_123" not in masked
    assert "***" in masked
    assert "trendx_app" in masked
    assert "pg-host" in masked
    assert "trendx" in masked


@pytest.mark.unit
def test_tb_login_fallback_reads_vault(tmp_path: Path):
    vault = tmp_path / "service-accounts.env"
    vault.write_text(
        "TB_SERVICE_USER_EMAIL=vault@test.com\nTB_SERVICE_USER_PASSWORD=vault_pass_42\n"
    )
    s = _TestSettings(
        TRENDX_ENV="testing",
        TB_BASE_URL="http://test:8080",
        TB_USERNAME="",
        TB_PASSWORD="",
        TRENDX_CREDENTIALS_FILE=vault,
    )
    email, password = s.tb_login
    assert email == "vault@test.com"
    assert password == "vault_pass_42"


@pytest.mark.unit
def test_diagnostic_output_contains_no_password():
    s = _TestSettings(
        TRENDX_ENV="testing",
        TB_BASE_URL="http://test:8080",
        TB_USERNAME="test",
        TB_PASSWORD="super-secret-pw",
        PG_ADMIN_HOST="pg-host",
        PG_ADMIN_PORT=5432,
        TRENDX_APP_PASSWORD="app_secret_123",
        TB_DB_READONLY_USER="ro",
        TB_DB_READONLY_PASSWORD="ro_secret_456",
    )
    masked = "\n".join(
        [
            s.mask_dsn(s.catalog_dsn_app()),
            s.mask_dsn(s.analytics_dsn_app()),
            s.mask_dsn(s.tb_db_readonly_dsn() or ""),
        ]
    )
    assert "super-secret-pw" not in masked
    assert "app_secret_123" not in masked
    assert "ro_secret_456" not in masked
    assert "***" in masked
