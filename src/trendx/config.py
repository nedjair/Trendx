from __future__ import annotations

from pathlib import Path
from typing import Optional

from pydantic import Field, SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=True,
        extra="ignore",
    )

    trendx_env: str = Field(default="development", alias="TRENDX_ENV")
    trendx_log_level: str = Field(default="INFO", alias="TRENDX_LOG_LEVEL")
    trendx_timezone: str = Field(default="UTC", alias="TRENDX_TIMEZONE")
    trendx_host: str = Field(default="10.0.0.1", alias="TRENDX_HOST")
    trendx_hostname: str = Field(default="trendx", alias="TRENDX_HOSTNAME")
    trendx_confirm_apply: str = Field(default="NO", alias="TRENDX_CONFIRM_APPLY")
    trendx_profile: str = Field(default="minimal", alias="TRENDX_PROFILE")

    trendx_jwt_signing_key: SecretStr = Field(
        default=SecretStr("change-me-please-32bytes-minimum-key"),
        alias="TRENDX_JWT_SIGNING_KEY",
    )

    trendx_api_token: SecretStr = Field(
        default=SecretStr("CHANGE_ME"),
        alias="TRENDX_API_TOKEN",
    )

    trendx_disk_min_free_gb: int = Field(default=50, alias="TRENDX_DISK_MIN_FREE_GB")

    trendx_api_host: str = Field(default="0.0.0.0", alias="TRENDX_API_HOST")
    trendx_api_port: int = Field(default=8000, alias="TRENDX_API_PORT")
    trendx_python_executor_port: int = Field(default=8181, alias="TRENDX_PYTHON_EXECUTOR_PORT")
    trendx_workers: int = Field(default=2, alias="TRENDX_WORKERS")

    tb_base_url: str = Field(default="http://thingsboard_thingsboard-ce_1:8080", alias="TB_BASE_URL")
    tb_username: str = Field(default="tenant@example.com", alias="TB_USERNAME")
    tb_password: SecretStr = Field(default=SecretStr("CHANGE_ME"), alias="TB_PASSWORD")
    tb_request_timeout_seconds: int = Field(default=30, alias="TB_REQUEST_TIMEOUT_SECONDS")
    tb_retry_max_attempts: int = Field(default=5, alias="TB_RETRY_MAX_ATTEMPTS")
    tb_retry_backoff_seconds: int = Field(default=2, alias="TB_RETRY_BACKOFF_SECONDS")
    tb_jwt_leeway_seconds: int = Field(default=300, alias="TB_JWT_LEEWAY_SECONDS")
    tb_page_size: int = Field(default=100, alias="TB_PAGE_SIZE")
    tb_writeback_enabled: bool = Field(default=False, alias="TB_WRITEBACK_ENABLED")
    tb_alarms_enabled: bool = Field(default=False, alias="TB_ALARMS_ENABLED")
    tb_device_id: str = Field(default="ALG16025001", alias="TB_DEVICE_ID")
    tb_metric_name: str = Field(
        default="mppt_main_battery_voltage_v", alias="TB_METRIC_NAME"
    )
    tb_secondary_metric_name: str = Field(
        default="mppt_main_battery_current_a", alias="TB_SECONDARY_METRIC_NAME"
    )

    tb_db_host: str = Field(default="mobili_dahsboard-postgres-1", alias="TB_DB_HOST")
    tb_db_port: int = Field(default=32768, alias="TB_DB_PORT")
    tb_db_name: str = Field(default="thingsboard", alias="TB_DB_NAME")
    tb_db_readonly_user: Optional[str] = Field(default=None, alias="TB_DB_READONLY_USER")
    tb_db_readonly_password: Optional[SecretStr] = Field(
        default=None, alias="TB_DB_READONLY_PASSWORD"
    )

    pg_admin_host: str = Field(default="mobili_dahsboard-postgres-1", alias="PG_ADMIN_HOST")
    pg_admin_port: int = Field(default=5432, alias="PG_ADMIN_PORT")
    pg_admin_db: str = Field(default="postgres", alias="PG_ADMIN_DB")
    pg_admin_user: str = Field(default="postgres", alias="PG_ADMIN_USER")
    pg_admin_password: SecretStr = Field(
        default=SecretStr("postgres"), alias="PG_ADMIN_PASSWORD"
    )

    trendx_db_name: str = Field(default="trendx", alias="TRENDX_DB_NAME")
    trendx_db_host: str = Field(default="mobili_dahsboard-postgres-1", alias="TRENDX_DB_HOST")
    trendx_db_port: int = Field(default=5432, alias="TRENDX_DB_PORT")
    trendx_db_user: str = Field(default="trendx_app", alias="TRENDX_DB_USER")
    trendx_db_password: SecretStr = Field(default=SecretStr("CHANGE_ME"), alias="TRENDX_DB_PASSWORD")

    trendx_migration_user: str = Field(default="trendx_migration", alias="TRENDX_MIGRATION_USER")
    trendx_migration_password: SecretStr = Field(default=SecretStr("CHANGE_ME"), alias="TRENDX_MIGRATION_PASSWORD")
    trendx_app_user: str = Field(default="trendx_app", alias="TRENDX_APP_USER")
    trendx_app_password: SecretStr = Field(default=SecretStr("CHANGE_ME"), alias="TRENDX_APP_PASSWORD")

    trendx_profile: str = Field(default="minimal", alias="TRENDX_PROFILE")

    forecast_frequency: str = Field(default="1h", alias="FORECAST_FREQUENCY")
    forecast_horizon: int = Field(default=24, alias="FORECAST_HORIZON")
    training_lookback_days: int = Field(default=90, alias="TRAINING_LOOKBACK_DAYS")
    forecast_min_value: float = Field(default=0.0, alias="FORECAST_MIN_VALUE")
    forecast_max_value: float = Field(default=60.0, alias="FORECAST_MAX_VALUE")

    anomaly_detection_enabled: bool = Field(
        default=False, alias="ANOMALY_DETECTION_ENABLED"
    )
    anomaly_contamination: float = Field(default=0.01, alias="ANOMALY_CONTAMINATION")
    anomaly_window_size: int = Field(default=24, alias="ANOMALY_WINDOW_SIZE")

    credentials_file: Path = Field(
        default=Path(".secrets/service-accounts.env"), alias="TRENDX_CREDENTIALS_FILE"
    )

    @field_validator("trendx_log_level")
    @classmethod
    def _uppercase_log_level(cls, v: str) -> str:
        return v.upper()

    @property
    def tb_login(self) -> tuple[str, str]:
        """Identifiants ThingsBoard (email, password).

        .env (TB_USERNAME/TB_PASSWORD) d'abord s'ils sont réels ; sinon fallback
        sur le coffre .secrets/service-accounts.env (TB_SERVICE_USER_EMAIL /
        TB_SERVICE_USER_PASSWORD). Ne jamais journaliser les valeurs.
        """
        username = self.tb_username
        password = self.tb_password.get_secret_value()
        if username and password and password not in ("CHANGE_ME",) and len(password) >= 8:
            return username, password
        try:
            with self.credentials_file.open("r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line.startswith("TB_SERVICE_USER_EMAIL="):
                        username = line.split("=", 1)[1].strip() or username
                    elif line.startswith("TB_SERVICE_USER_PASSWORD="):
                        password = line.split("=", 1)[1].strip() or password
        except OSError:
            pass
        return username, password

    def pg_dsn(self, dbname: str, user: str, password: SecretStr) -> str:
        return (
            f"postgresql://{user}:{password.get_secret_value()}"
            f"@{self.pg_admin_host}:{self.pg_admin_port}/{dbname}"
        )

    @staticmethod
    def mask_dsn(dsn: str) -> str:
        import re

        return re.sub(
            r"(:[^:@/]+)(@)",
            r":***\2",
            dsn,
        )

    def catalog_dsn_app(self) -> str:
        return self.pg_dsn(self.trendx_db_name, self.trendx_app_user, self.trendx_app_password)

    def analytics_dsn_app(self) -> str:
        return self.pg_dsn(self.trendx_db_name, self.trendx_app_user, self.trendx_app_password)

    def tb_db_readonly_dsn(self) -> str | None:
        if self.tb_db_readonly_user and self.tb_db_readonly_password:
            return (
                f"postgresql://{self.tb_db_readonly_user}:{self.tb_db_readonly_password.get_secret_value()}"
                f"@{self.tb_db_host}:{self.tb_db_port}/{self.tb_db_name}"
            )
        return None


settings = Settings()
