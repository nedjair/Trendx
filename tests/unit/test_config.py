from __future__ import annotations

import os
from pathlib import Path
from unittest.mock import patch

import pytest
from pydantic import SecretStr

from trendx.config import Settings, settings


@pytest.mark.unit
def test_settings_loading():
    assert settings.trendx_env in ("development", "production", "testing")
    assert settings.tb_base_url is not None
    assert isinstance(settings.tb_password, SecretStr)


@pytest.mark.unit
def test_dsn_generation():
    s = Settings(
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
    assert "trendx_analytics" in analytics_dsn


@pytest.mark.unit
def test_tb_readonly_dsn():
    s = Settings(
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

    s = Settings(
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
    with patch.dict(os.environ, {
        "TRENDX_ENV": "testing",
        "TB_BASE_URL": "https://custom:8081",
        "TB_USERNAME": "custom@test.com",
        "TB_PASSWORD": "custom_pass",
        "FORECAST_HORIZON": "48",
        "ANOMALY_CONTAMINATION": "0.05",
    }, clear=True):
        s = Settings()
        assert s.trendx_env == "testing"
        assert s.tb_base_url == "https://custom:8081"
        assert s.tb_password.get_secret_value() == "custom_pass"
        assert s.forecast_horizon == 48
        assert s.anomaly_contamination == 0.05


@pytest.mark.unit
def test_settings_defaults():
    s = Settings(
        TRENDX_ENV="testing",
        TB_BASE_URL="http://test:8080",
        TB_USERNAME="test",
        TB_PASSWORD="test",
    )
    assert s.forecast_horizon == 24
    assert s.forecast_frequency == "1h"
    assert s.trendx_log_level == "INFO"


@pytest.mark.unit
def test_log_level_uppercased():
    s = Settings(
        TRENDX_ENV="testing",
        TRENDX_LOG_LEVEL="debug",
        TB_BASE_URL="http://test:8080",
        TB_USERNAME="test",
        TB_PASSWORD="test",
    )
    assert s.trendx_log_level == "DEBUG"


@pytest.mark.unit
def test_secret_str_not_in_repr():
    s = Settings(
        TRENDX_ENV="testing",
        TB_BASE_URL="http://test:8080",
        TB_USERNAME="test",
        TB_PASSWORD="super-secret-value",
    )
    rep = repr(s)
    assert "super-secret-value" not in rep
