from __future__ import annotations

from pathlib import Path

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

    # Verrou d'ingestion : false par défaut. L'endpoint /api/v1/ingestion/trigger
    # refuse (409) tant que TRENDX_INGEST_ENABLED n'est pas set a true.
    trendx_ingest_enabled: bool = Field(default=False, alias="TRENDX_INGEST_ENABLED")

    trendx_api_host: str = Field(default="0.0.0.0", alias="TRENDX_API_HOST")  # nosec B104 -- défaut conteneur : écoute toutes interfaces derrière le reverse-proxy, exposition gérée au niveau compose/pare-feu, surchargeable par TRENDX_API_HOST.
    trendx_api_port: int = Field(default=8000, alias="TRENDX_API_PORT")
    trendx_python_executor_port: int = Field(default=8181, alias="TRENDX_PYTHON_EXECUTOR_PORT")
    trendx_workers: int = Field(default=2, alias="TRENDX_WORKERS")

    # Port hôte de Grafana (service optionnel du profil minimal). 3001 et non 3000 :
    # le port 3000 de l'hôte est déjà occupé par Gitea (décision consignée dans
    # docs/audit-phase1.md et docs/rapport_phase2_infrastructure.md, .env : GRAFANA_HOST_PORT=3001).
    # Purement informatif côté API : sert à composer l'URL affichée par GET /.
    grafana_host_port: int = Field(default=3001, alias="GRAFANA_HOST_PORT")

    # Cache Redis (service optionnel : absent du profil minimal, cf. AGENTS.md).
    # Consommé par CacheService quand aucune URL n'est fournie en argument.
    redis_url: str = Field(default="redis://localhost:6379/0", alias="REDIS_URL")

    tb_base_url: str = Field(
        default="http://thingsboard_thingsboard-ce_1:8080", alias="TB_BASE_URL"
    )
    tb_username: str = Field(default="tenant@example.com", alias="TB_USERNAME")
    tb_password: SecretStr = Field(default=SecretStr("CHANGE_ME"), alias="TB_PASSWORD")
    tb_request_timeout_seconds: int = Field(default=30, alias="TB_REQUEST_TIMEOUT_SECONDS")
    tb_retry_max_attempts: int = Field(default=5, alias="TB_RETRY_MAX_ATTEMPTS")
    tb_retry_backoff_seconds: int = Field(default=2, alias="TB_RETRY_BACKOFF_SECONDS")
    tb_jwt_leeway_seconds: int = Field(default=300, alias="TB_JWT_LEEWAY_SECONDS")
    tb_page_size: int = Field(default=100, alias="TB_PAGE_SIZE")
    tb_writeback_enabled: bool = Field(default=False, alias="TB_WRITEBACK_ENABLED")
    tb_alarms_enabled: bool = Field(default=False, alias="TB_ALARMS_ENABLED")
    # Authentication ThingsBoard réelle configurée (compte service) : true uniquement
    # quand des identifiants vérifiés sont en place. Verrou d'ingestion : absent => false.
    tb_auth_configured: bool = Field(default=False, alias="TB_AUTH_CONFIGURED")
    tb_device_id: str = Field(default="ALG16025001", alias="TB_DEVICE_ID")
    tb_metric_name: str = Field(default="mppt_main_battery_voltage_v", alias="TB_METRIC_NAME")
    tb_secondary_metric_name: str = Field(
        default="mppt_main_battery_current_a", alias="TB_SECONDARY_METRIC_NAME"
    )

    tb_db_host: str = Field(default="mobili_dahsboard-postgres-1", alias="TB_DB_HOST")
    tb_db_port: int = Field(default=32768, alias="TB_DB_PORT")
    tb_db_name: str = Field(default="thingsboard", alias="TB_DB_NAME")
    tb_db_readonly_user: str | None = Field(default=None, alias="TB_DB_READONLY_USER")
    tb_db_readonly_password: SecretStr | None = Field(default=None, alias="TB_DB_READONLY_PASSWORD")
    # Canal 2 (SQL read-only) : bornes de session. Les deux niveaux coexistent
    # avec la protection côté rôle PostgreSQL (ALTER ROLE trendx_ro ...).
    tb_db_connect_timeout: int = Field(default=10, alias="TB_DB_CONNECT_TIMEOUT")
    tb_db_statement_timeout: int = Field(default=60000, alias="TB_DB_STATEMENT_TIMEOUT")
    tb_db_sslmode: str = Field(default="prefer", alias="TB_DB_SSLMODE")
    # Fenêtre de recouvrement (overlap) entre deux checkpoints d'ingestion.
    # 1h par défaut : reprise sans lacune, avec déduplication/idempotence DB.
    tb_ingest_recovery_window_hours: int = Field(default=1, alias="TB_INGEST_RECOVERY_WINDOW_HOURS")

    pg_admin_host: str = Field(default="mobili_dahsboard-postgres-1", alias="PG_ADMIN_HOST")
    pg_admin_port: int = Field(default=5432, alias="PG_ADMIN_PORT")
    pg_admin_db: str = Field(default="postgres", alias="PG_ADMIN_DB")
    pg_admin_user: str = Field(default="postgres", alias="PG_ADMIN_USER")
    pg_admin_password: SecretStr = Field(default=SecretStr("postgres"), alias="PG_ADMIN_PASSWORD")

    trendx_db_name: str = Field(default="trendx", alias="TRENDX_DB_NAME")
    trendx_db_host: str = Field(default="mobili_dahsboard-postgres-1", alias="TRENDX_DB_HOST")
    trendx_db_port: int = Field(default=5432, alias="TRENDX_DB_PORT")
    trendx_db_user: str = Field(default="trendx_app", alias="TRENDX_DB_USER")
    trendx_db_password: SecretStr = Field(
        default=SecretStr("CHANGE_ME"), alias="TRENDX_DB_PASSWORD"
    )

    trendx_migration_user: str = Field(default="trendx_migration", alias="TRENDX_MIGRATION_USER")
    trendx_migration_password: SecretStr = Field(
        default=SecretStr("CHANGE_ME"), alias="TRENDX_MIGRATION_PASSWORD"
    )
    trendx_app_user: str = Field(default="trendx_app", alias="TRENDX_APP_USER")
    trendx_app_password: SecretStr = Field(
        default=SecretStr("CHANGE_ME"), alias="TRENDX_APP_PASSWORD"
    )

    trendx_default_tenant_id: str = Field(default="", alias="TRENDX_DEFAULT_TENANT_ID")
    trendx_execution_history_path: str = Field(
        default="",
        alias="TRENDX_EXECUTION_HISTORY_PATH",
    )
    trendx_default_customer_id: str = Field(default="", alias="TRENDX_DEFAULT_CUSTOMER_ID")
    trendx_default_user_id: str = Field(default="", alias="TRENDX_DEFAULT_USER_ID")

    trendx_retention_days: int = Field(default=180, alias="TRENDX_RETENTION_DAYS")

    # Scheduler B1 (MR-3 : leader election / heartbeat). Tout est OFF par
    # défaut : TRENDX_SCHEDULER_ENABLED=false interdit toute planification
    # en production. La validation fail-closed vit dans LeaderElection
    # (entiers > 0, stale > heartbeat, lock/instance non vides).
    trendx_scheduler_enabled: bool = Field(default=False, alias="TRENDX_SCHEDULER_ENABLED")
    # Forecast B1 : jamais fonctionnel en production (pas de fan-out par device,
    # payloads incomplets -> échecs horaires + runs orphelins). false par défaut =
    # forecast-train/forecast-run non planifiés ; ingestion/discovery inchangés.
    # Option A (fan-out + réconciliation + catalogue provisionné) reste un
    # chantier ultérieur distinct.
    trendx_scheduler_forecast_enabled: bool = Field(
        default=False, alias="TRENDX_SCHEDULER_FORECAST_ENABLED"
    )
    trendx_scheduler_instance_id: str = Field(default="", alias="TRENDX_SCHEDULER_INSTANCE_ID")
    trendx_scheduler_heartbeat_seconds: int = Field(
        default=15, alias="TRENDX_SCHEDULER_HEARTBEAT_SECONDS"
    )
    trendx_scheduler_stale_seconds: int = Field(default=60, alias="TRENDX_SCHEDULER_STALE_SECONDS")
    trendx_scheduler_acquire_timeout_seconds: int = Field(
        default=5, alias="TRENDX_SCHEDULER_ACQUIRE_TIMEOUT_SECONDS"
    )
    trendx_scheduler_leader_lock: str = Field(
        default="trendx_scheduler_leader", alias="TRENDX_SCHEDULER_LEADER_LOCK"
    )

    # Planification ingestion côté worker (MR-5 : déduplication). true par
    # défaut = comportement historique (le worker planifie ingestion_hourly).
    # Passer à false quand le scheduler B1 dédié assure la planification
    # (profil scheduler actif) : garantit un seul planificateur actif pour
    # l'ingestion (contrat XOR : jamais worker + scheduler simultanément).
    # Rollback : true + restart worker.
    trendx_worker_ingestion_enabled: bool = Field(
        default=True, alias="TRENDX_WORKER_INGESTION_ENABLED"
    )

    forecast_frequency: str = Field(default="1h", alias="FORECAST_FREQUENCY")
    forecast_horizon: int = Field(default=24, alias="FORECAST_HORIZON")
    training_lookback_days: int = Field(default=90, alias="TRAINING_LOOKBACK_DAYS")
    # Exclusion explicite de batches d'ingestion du Train (isolement synthétique).
    # Liste séparée par des virgules d'ingestion_id à exclure de
    # TrainingService._fetch_training_data() via `AND ingestion_id NOT IN (...)`.
    # Vide par défaut = aucune exclusion (comportement normal). Voir
    # docs/train-synthetic-exclusion.md pour les 11 IDs synthétiques de
    # septembre 2026 et la procédure de remplacement de la liste.
    # `source=thingsboard` n'est jamais un critère (homogène valide/synthétique).
    training_excluded_ingestion_ids: str = Field(
        default="", alias="TRAINING_EXCLUDED_INGESTION_IDS"
    )
    forecast_min_value: float = Field(default=0.0, alias="FORECAST_MIN_VALUE")
    forecast_max_value: float = Field(default=60.0, alias="FORECAST_MAX_VALUE")

    anomaly_detection_enabled: bool = Field(default=False, alias="ANOMALY_DETECTION_ENABLED")
    anomaly_contamination: float = Field(default=0.01, alias="ANOMALY_CONTAMINATION")
    anomaly_window_size: int = Field(default=24, alias="ANOMALY_WINDOW_SIZE")

    mlflow_tracking_uri: str = Field(default="file:./mlruns", alias="MLFLOW_TRACKING_URI")

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

        .env (TB_USERNAME/TB_PASSWORD) en priorité s'ils sont réels ; sinon fallback
        sur le coffre .secrets/service-accounts.env (TB_SERVICE_USER_EMAIL /
        TB_SERVICE_USER_PASSWORD). Ne jamais journaliser les valeurs.
        """
        username = self.tb_username
        password = self.tb_password.get_secret_value()
        # Heuristique réduite aux placeholders : ne spécialise AUCUN mot de passe
        # réel (le constat du mot de passe d'usine ThingsBoard est consigné dans
        # docs/security.md sans valeur ; TB_AUTH_CONFIGURED fait foi).
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
            # Les paramètres de session (statement_timeout, connect_timeout,
            # sslmode, read-only) sont appliqués côté rôle PostgreSQL (ALTER ROLE
            # trendx_ro) ET ici au niveau libpq, pour garantir le bornage même si
            # le rôle est reconfiguré. connect_timeout n'est pas un paramètre de
            # session : il est transmis séparément à psycopg2.connect().
            opts = (
                f"-c statement_timeout={self.tb_db_statement_timeout}"
                f" -c default_transaction_read_only=on"
            )
            return (
                f"postgresql://{self.tb_db_readonly_user}:{self.tb_db_readonly_password.get_secret_value()}"
                f"@{self.tb_db_host}:{self.tb_db_port}/{self.tb_db_name}"
                f"?sslmode={self.tb_db_sslmode}&options={opts.replace(' ', '%20')}"
            )
        return None


settings = Settings()
