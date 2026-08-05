=== DIFF: src/trendx/config.py  vs  src/src/trendx/config.py  (at d34aa764) ===
diff --git a/src/trendx/config.py b/src/src/trendx/config.py
index 8695758..635f0f1 100644
--- a/src/trendx/config.py
+++ b/src/src/trendx/config.py
@@ -28,17 +28,6 @@ class Settings(BaseSettings):
         alias="TRENDX_JWT_SIGNING_KEY",
     )

-    trendx_api_token: SecretStr = Field(
-        default=SecretStr("CHANGE_ME"),
-        alias="TRENDX_API_TOKEN",
-    )
-
-    trendx_disk_min_free_gb: int = Field(default=50, alias="TRENDX_DISK_MIN_FREE_GB")
-
-    # Verrou d'ingestion : false par défaut. L'endpoint /api/v1/ingestion/trigger
-    # refuse (409) tant que TRENDX_INGEST_ENABLED n'est pas explicitement true.
-    trendx_ingest_enabled: bool = Field(default=False, alias="TRENDX_INGEST_ENABLED")
-
     trendx_api_host: str = Field(default="0.0.0.0", alias="TRENDX_API_HOST")
     trendx_api_port: int = Field(default=8000, alias="TRENDX_API_PORT")
     trendx_python_executor_port: int = Field(default=8181, alias="TRENDX_PYTHON_EXECUTOR_PORT")
@@ -54,9 +43,6 @@ class Settings(BaseSettings):
     tb_page_size: int = Field(default=100, alias="TB_PAGE_SIZE")
     tb_writeback_enabled: bool = Field(default=False, alias="TB_WRITEBACK_ENABLED")
     tb_alarms_enabled: bool = Field(default=False, alias="TB_ALARMS_ENABLED")
-    # Authentification ThingsBoard réelle configurée (compte service) : true uniquement
-    # quand des identifiants vérifiés sont en place. Verrou d'ingestion : absent => false.
-    tb_auth_configured: bool = Field(default=False, alias="TB_AUTH_CONFIGURED")
     tb_device_id: str = Field(default="ALG16025001", alias="TB_DEVICE_ID")
     tb_metric_name: str = Field(
         default="mppt_main_battery_voltage_v", alias="TB_METRIC_NAME"
@@ -115,55 +101,15 @@ class Settings(BaseSettings):
     def _uppercase_log_level(cls, v: str) -> str:
         return v.upper()

-    @property
-    def tb_login(self) -> tuple[str, str]:
-        """Identifiants ThingsBoard (email, password).
-
-        .env (TB_USERNAME/TB_PASSWORD) d'abord s'ils sont réels ; sinon fallback
-        sur le coffre .secrets/service-accounts.env (TB_SERVICE_USER_EMAIL /
-        TB_SERVICE_USER_PASSWORD). Ne jamais journaliser les valeurs.
-        """
-        username = self.tb_username
-        password = self.tb_password.get_secret_value()
-        # Heuristique réduite aux placeholders : ne spécialise AUCUN mot de passe
-        # réel (le constat du mot de passe d'usine ThingsBoard est consigné dans
-        # docs/security.md sans valeur ; TB_AUTH_CONFIGURED fait foi).
-        if username and password and password not in ("CHANGE_ME",) and len(password) >= 8:
-            return username, password
-        try:
-            with self.credentials_file.open("r", encoding="utf-8") as f:
-                for line in f:
-                    line = line.strip()
-                    if line.startswith("TB_SERVICE_USER_EMAIL="):
-                        username = line.split("=", 1)[1].strip() or username
-                    elif line.startswith("TB_SERVICE_USER_PASSWORD="):
-                        password = line.split("=", 1)[1].strip() or password
-        except OSError:
-            pass
-        return username, password
-
     def pg_dsn(self, dbname: str, user: str, password: SecretStr) -> str:
         return (
             f"postgresql://{user}:{password.get_secret_value()}"
             f"@{self.pg_admin_host}:{self.pg_admin_port}/{dbname}"
         )

-    @staticmethod
-    def mask_dsn(dsn: str) -> str:
-        import re
-
-        return re.sub(
-            r"(:[^:@/]+)(@)",
-            r":***\2",
-            dsn,
-        )
-
     def catalog_dsn_app(self) -> str:
         return self.pg_dsn(self.trendx_db_name, self.trendx_app_user, self.trendx_app_password)

-    def analytics_dsn_app(self) -> str:
-        return self.pg_dsn(self.trendx_db_name, self.trendx_app_user, self.trendx_app_password)
-
     def tb_db_readonly_dsn(self) -> str | None:
         if self.tb_db_readonly_user and self.tb_db_readonly_password:
             return (

=== DIFF: src/trendx/database/connection.py  vs  src/src/trendx/database/connection.py  (at d34aa764) ===
diff --git a/src/trendx/database/connection.py b/src/src/trendx/database/connection.py
index ee20ae4..0ee527f 100644
--- a/src/trendx/database/connection.py
+++ b/src/src/trendx/database/connection.py
@@ -13,8 +13,6 @@ from trendx.config import settings
 # Budget connexions trendx_app (AGENTS §2, rôle CONNECTION LIMIT 24) :
 #   2 moteurs (catalog + analytics) x (pool 2 + overflow 1) = 6 connexions max
 #   par processus ; API (2 workers) = 12, worker = 6, total = 18 <= 24.
-# Moteur tb_readonly (rôle trendx_ro, LIMIT 5) : pool 1 + overflow 0 par
-#   processus ; API (2 workers) = 2, worker = 1, total = 3 <= 5.
 DEFAULT_POOL_SIZE = 2
 DEFAULT_MAX_OVERFLOW = 1

@@ -97,20 +95,6 @@ class DatabaseManager:
     def get_analytics_engine(self):
         return self.get_engine("analytics")

-    def get_tb_readonly_engine(self):
-        return self.get_engine("tb_readonly")
-
-    def engine_search_paths(self) -> dict[str, str]:
-        paths: dict[str, str] = {}
-        for name, wrapper in self._engines.items():
-            try:
-                with wrapper.engine.connect() as conn:
-                    row = conn.execute(text("SHOW search_path")).fetchone()
-                    paths[name] = str(row[0]) if row and row[0] else ""
-            except Exception as exc:
-                paths[name] = f"ERROR: {exc}"
-        return paths
-
     @contextmanager
     def get_session(self, db_name: str = "catalog") -> Generator[Session, None, None]:
         maker = self.get_sessionmaker(db_name)
@@ -157,14 +141,8 @@ def _register_default_engines() -> None:
     manager.register(
         "analytics", settings.analytics_dsn_app(), search_path="trendx_analytics,public"
     )
-    ro_dsn = settings.tb_db_readonly_dsn()
-    if ro_dsn:
-        manager.register(
-            "tb_readonly", ro_dsn, pool_size=1, max_overflow=0, search_path="public"
-        )
-        logger.info("Registered read-only ThingsBoard engine (tb_readonly, pool=1/0)")

-    logger.info("Default engines registered (catalog, analytics[, tb_readonly])")
+    logger.info("Default engines registered (catalog, analytics)")


 _register_default_engines()
@@ -178,9 +156,5 @@ def get_analytics_engine():
     return manager.get_analytics_engine()


-def get_tb_readonly_engine():
-    return manager.get_tb_readonly_engine()
-
-
 def get_session(db_name: str = "catalog") -> Generator[Session, None, None]:
     return manager.get_session(db_name)

=== DIFF: src/trendx/database/repositories.py  vs  src/src/trendx/database/repositories.py  (at d34aa764) ===
diff --git a/src/trendx/database/repositories.py b/src/src/trendx/database/repositories.py
index 796acd0..5e2c901 100644
--- a/src/trendx/database/repositories.py
+++ b/src/src/trendx/database/repositories.py
@@ -3,7 +3,7 @@ from __future__ import annotations
 from typing import Any, Generic, Optional, Sequence, TypeVar

 from loguru import logger
-from sqlalchemy import func, select, text, update
+from sqlalchemy import func, select, update
 from sqlalchemy.orm import Session

 from trendx.database.models import (
@@ -304,17 +304,7 @@ class CheckpointRepository(BaseRepository):
     def get_watermark(
         self, pipeline: str, entity_id: Any, metric_key: Optional[str] = None
     ) -> Optional[Any]:
-        row = self._session.execute(
-            text(
-                "SELECT watermark_ts, records_processed, last_batch_id "
-                "FROM trendx_catalog.ingestion_checkpoints "
-                "WHERE pipeline = :pipeline AND entity_id = :entity_id AND metric_key = :metric_key"
-            ),
-            {"pipeline": pipeline, "entity_id": str(entity_id), "metric_key": metric_key},
-        ).fetchone()
-        if row is None:
-            return None
-        return {"watermark_ts": row[0], "records_processed": row[1], "last_batch_id": row[2]}
+        return None

     def upsert_watermark(
         self,
@@ -326,48 +316,10 @@ class CheckpointRepository(BaseRepository):
         source: Optional[str] = None,
         last_batch_id: Optional[str] = None,
     ) -> Any:
-        self._session.execute(
-            text(
-                "INSERT INTO trendx_catalog.ingestion_checkpoints "
-                "  (pipeline, entity_id, metric_key, watermark_ts, records_processed, last_batch_id, source, updated_at) "
-                "VALUES (:pipeline, :entity_id, :metric_key, :watermark_ts, :records_processed, :last_batch_id, :source, now()) "
-                "ON CONFLICT (pipeline, entity_id, metric_key) DO UPDATE SET "
-                "  watermark_ts = EXCLUDED.watermark_ts, "
-                "  records_processed = EXCLUDED.records_processed, "
-                "  last_batch_id = EXCLUDED.last_batch_id, "
-                "  source = EXCLUDED.source, "
-                "  updated_at = now()"
-            ),
-            {
-                "pipeline": pipeline,
-                "entity_id": str(entity_id),
-                "metric_key": metric_key,
-                "watermark_ts": watermark_ts,
-                "records_processed": records_processed,
-                "last_batch_id": last_batch_id,
-                "source": source,
-            },
-        )
-        return {"pipeline": pipeline, "entity_id": str(entity_id), "metric_key": metric_key}
+        return None

     def list_by_pipeline(self, pipeline: str) -> Sequence[Any]:
-        rows = self._session.execute(
-            text(
-                "SELECT entity_id, metric_key, watermark_ts, records_processed "
-                "FROM trendx_catalog.ingestion_checkpoints "
-                "WHERE pipeline = :pipeline"
-            ),
-            {"pipeline": pipeline},
-        ).fetchall()
-        return [
-            {
-                "entity_id": r[0],
-                "metric_key": r[1],
-                "watermark_ts": r[2],
-                "records_processed": r[3],
-            }
-            for r in rows
-        ]
+        return []


 class TrendzTaskRepository(BaseRepository[TrendzTask]):

=== DIFF: src/trendx/main.py  vs  src/src/trendx/main.py  (at d34aa764) ===
diff --git a/src/trendx/main.py b/src/src/trendx/main.py
index b6817ca..a83669e 100644
--- a/src/trendx/main.py
+++ b/src/src/trendx/main.py
@@ -1,7 +1,5 @@
 from __future__ import annotations

-import hmac
-import os
 import platform
 import sys
 import uuid
@@ -10,7 +8,7 @@ from enum import Enum
 from pathlib import Path
 from typing import Any, Optional

-from fastapi import FastAPI, HTTPException, Query, Request
+from fastapi import FastAPI, HTTPException, Query
 from fastapi.middleware.cors import CORSMiddleware
 from fastapi.responses import JSONResponse
 from loguru import logger
@@ -39,7 +37,16 @@ from trendx.services.tasks import TaskService
 class HealthResponse(BaseModel):
     status: str = Field(default="ok")
     service: str = Field(default="trendx-api")
-    detail: str | None = None
+    version: str = Field(default="0.1.0")
+    environment: str = Field()
+    timezone: str = Field()
+    device_id: str = Field()
+    primary_metric: str = Field()
+    writeback_enabled: bool = Field()
+    alarms_enabled: bool = Field()
+    python_version: str = Field()
+    platform: str = Field()
+    timestamp: str = Field()


 logger.remove()
@@ -70,58 +77,6 @@ app.add_middleware(
 )


-# ── Authentification ────────────────────────────────────────────────────
-# Tous les endpoints /api/v1/* exigent un jeton (Bearer ou X-API-Key),
-# vérifié en temps constant. Endpoints publics : infrastructure seulement.
-
-
-PUBLIC_PATHS = {"/", "/health", "/metrics", "/docs", "/redoc", "/openapi.json"}
-
-
-@app.middleware("http")
-async def require_auth_middleware(request: Request, call_next):
-    path = request.url.path
-    if path in PUBLIC_PATHS:
-        return await call_next(request)
-    if path.startswith("/api/v1"):
-        provided: Optional[str] = None
-        authorization = request.headers.get("authorization")
-        x_api_key = request.headers.get("x-api-key")
-        if x_api_key:
-            provided = x_api_key
-        elif authorization and authorization.lower().startswith("bearer "):
-            provided = authorization[7:].strip()
-        if not provided:
-            logger.warning("API auth refusée (jeton absent) : {}", path)
-            return JSONResponse(
-                content={"detail": "Missing authentication credentials"},
-                status_code=401,
-            )
-        expected = settings.trendx_api_token.get_secret_value()
-        if not hmac.compare_digest(provided.encode("utf-8"), expected.encode("utf-8")):
-            logger.warning("API auth refusée (jeton invalide) : {}", path)
-            return JSONResponse(
-                content={"detail": "Invalid authentication credentials"},
-                status_code=401,
-            )
-    return await call_next(request)
-
-
-@app.on_event("startup")
-async def _log_startup_disk_check() -> None:
-    # Sonde disque UNIQUE, partagée avec le worker (probe_disk_mounts) :
-    # _statvfs_available_gb + fstype + delta réservé, format de journal identique.
-    # tmpfs => fatal (fail-closed), cohérent avec le worker.
-    from trendx.services.ingestion import probe_disk_mounts
-    try:
-        probe_disk_mounts("api")
-    except RuntimeError as exc:
-        logger.error("[disk] {} : sonde disque fatale", exc)
-        raise
-    except Exception as exc:  # noqa: BLE001
-        logger.warning("API startup disk check failed: {}", exc)
-
-
 # ── Pydantic models ─────────────────────────────────────────────────────


@@ -467,21 +422,23 @@ def _to_metric_out(m: Any) -> dict[str, Any]:

 @app.get("/health", tags=["system"], response_model=HealthResponse)
 async def health() -> JSONResponse:
-    status = "ok"
-    detail = None
-    try:
-        from trendx.services.ingestion import _default_monitor_mounts, check_disk_min_free
-        for mp in _default_monitor_mounts():
-            if os.path.isdir(mp):
-                check_disk_min_free(mp)
-    except Exception as exc:  # noqa: BLE001
-        status = "degraded"
-        detail = str(exc)
-        logger.warning("Health dégradé : {}", exc)
-    payload = HealthResponse(status=status, service="trendx-api", detail=detail)
-    logger.debug("Health check : status={}", status)
-    code = 200 if status == "ok" else 503
-    return JSONResponse(content=payload.model_dump(exclude_none=True), status_code=code)
+    payload = HealthResponse(
+        status="ok",
+        service="trendx-api",
+        version=APP_VERSION,
+        environment=settings.trendx_env,
+        timezone=settings.trendx_timezone,
+        device_id=settings.tb_device_id,
+        primary_metric=settings.tb_metric_name,
+        writeback_enabled=settings.tb_writeback_enabled,
+        alarms_enabled=settings.tb_alarms_enabled,
+        python_version=platform.python_version(),
+        platform=f"{platform.system()} {platform.release()}".strip(),
+        timestamp=datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
+    )
+    logger.info("Health check OK : service={service}, env={env}, device={dev}",
+                service=payload.service, env=payload.environment, dev=payload.device_id)
+    return JSONResponse(content=payload.model_dump(), status_code=200)


 @app.get("/metrics", tags=["system"])
@@ -796,30 +753,7 @@ async def get_telemetry_stats(


 @app.post("/api/v1/ingestion/trigger", tags=["telemetry"], response_model=DiscoveryTriggerOut)
-async def trigger_ingestion(request: Request) -> JSONResponse:
-    # Verrou d'ingestion : refus systématique tant que TRENDX_INGEST_ENABLED n'est
-    # pas explicitement true, et tant que l'authentification ThingsBoard n'est pas
-    # déclarée configurée (TB_AUTH_CONFIGURED=true). Toute tentative est journalisée
-    # avec son déclencheur et son horodatage (loguru).
-    trigger = request.client.host if request.client else "inconnu"
-    if not settings.trendx_ingest_enabled:
-        logger.warning(
-            "Ingestion REFUSÉE (verrou TRENDX_INGEST_ENABLED != true), déclencheur={}",
-            trigger,
-        )
-        raise HTTPException(
-            status_code=409,
-            detail="Ingestion verrouillée : TRENDX_INGEST_ENABLED doit être true",
-        )
-    if not settings.tb_auth_configured:
-        logger.warning(
-            "Ingestion REFUSÉE (TB_AUTH_CONFIGURED != true), déclencheur={}",
-            trigger,
-        )
-        raise HTTPException(
-            status_code=409,
-            detail="Ingestion verrouillée : TB_AUTH_CONFIGURED doit être true",
-        )
+async def trigger_ingestion() -> JSONResponse:
     try:
         svc = TaskService()
         task = svc.create_task(
@@ -828,11 +762,7 @@ async def trigger_ingestion(request: Request) -> JSONResponse:
             json_job={"action": "incremental_ingest"},
             reference_type="MANUAL",
         )
-        logger.info(
-            "Ingestion déclenchée par {} : task={}",
-            trigger,
-            task.id,
-        )
+        logger.info("Ingestion triggered, task={}", task.id)
         return JSONResponse(
             content=DiscoveryTriggerOut(status="triggered", message="Ingestion scheduled", task_id=str(task.id)).model_dump(),
             status_code=202,

=== DIFF: src/trendx/mlops/__init__.py  vs  src/src/trendx/mlops/__init__.py  (at d34aa764) ===
diff --git a/src/trendx/mlops/__init__.py b/src/src/trendx/mlops/__init__.py
index 0e905ca..47768a9 100644
--- a/src/trendx/mlops/__init__.py
+++ b/src/src/trendx/mlops/__init__.py
@@ -1,11 +1,5 @@
-try:
-    from trendx.mlops.registry import ModelRegistry
-    from trendx.mlops.tracking import MLflowTracker
-    HAS_MLFLOW = True
-except RuntimeError:
-    HAS_MLFLOW = False
-    ModelRegistry = None
-    MLflowTracker = None
+from trendx.mlops.registry import ModelRegistry
+from trendx.mlops.tracking import MLflowTracker

 __all__ = [
     "MLflowTracker",

=== DIFF: src/trendx/mlops/tracking.py  vs  src/src/trendx/mlops/tracking.py  (at d34aa764) ===
diff --git a/src/trendx/mlops/tracking.py b/src/src/trendx/mlops/tracking.py
index 047480b..c03be99 100644
--- a/src/trendx/mlops/tracking.py
+++ b/src/src/trendx/mlops/tracking.py
@@ -8,17 +8,10 @@ import uuid
 from pathlib import Path
 from typing import Any, Optional

-try:
-    import mlflow
-    from mlflow.entities import Run
-    from mlflow.tracking import MlflowClient
-    HAS_MLFLOW = True
-except ImportError:
-    HAS_MLFLOW = False
-    mlflow = None
-    Run = None
-    MlflowClient = None
+import mlflow
 from loguru import logger
+from mlflow.entities import Run
+from mlflow.tracking import MlflowClient

 from trendx.config import settings


=== DIFF: src/trendx/services/cache.py  vs  src/src/trendx/services/cache.py  (at d34aa764) ===
diff --git a/src/trendx/services/cache.py b/src/src/trendx/services/cache.py
index d96494f..580ce9f 100644
--- a/src/trendx/services/cache.py
+++ b/src/src/trendx/services/cache.py
@@ -4,12 +4,7 @@ import json
 import time
 from typing import Any, Callable, Optional

-try:
-    import redis as sync_redis
-    HAS_REDIS = True
-except ImportError:
-    HAS_REDIS = False
-    sync_redis = None
+import redis as sync_redis
 from loguru import logger

 from trendx.config import settings
@@ -43,8 +38,6 @@ class CacheService:
         default_ttl: int = 900,
         ttl_by_namespace: dict[str, int] | None = None,
     ) -> None:
-        if not HAS_REDIS:
-            raise RuntimeError("redis package is required for CacheService")
         self._redis_url = redis_url or settings.redis_url
         self._default_ttl = default_ttl
         self._ttl_by_namespace = {**DEFAULT_TTL_BY_NAMESPACE, **(ttl_by_namespace or {})}

=== DIFF: src/trendx/services/inference.py  vs  src/src/trendx/services/inference.py  (at d34aa764) ===
diff --git a/src/trendx/services/inference.py b/src/src/trendx/services/inference.py
index 2455c36..be3e8c9 100644
--- a/src/trendx/services/inference.py
+++ b/src/src/trendx/services/inference.py
@@ -20,12 +20,7 @@ from trendx.database.repositories import (
 )
 from trendx.forecasting import create_model
 from trendx.forecasting.base import ForecastResult
-try:
-    from trendx.mlops.registry import ModelRegistry
-    HAS_MODEL_REGISTRY = True
-except RuntimeError:
-    HAS_MODEL_REGISTRY = False
-    ModelRegistry = None
+from trendx.mlops.registry import ModelRegistry
 from trendx.preprocessing.normalizer import Normalizer
 from trendx.preprocessing.resampling import Resampler

@@ -39,7 +34,7 @@ class InferenceService:
         resampler: Resampler | None = None,
         dry_run: bool = True,
     ) -> None:
-        self._registry = model_registry or (ModelRegistry() if HAS_MODEL_REGISTRY else None)
+        self._registry = model_registry or ModelRegistry()
         self._resampler = resampler or Resampler()
         self._dry_run = dry_run
         self._writeback_enabled = settings.tb_writeback_enabled

=== DIFF: src/trendx/services/ingestion.py  vs  src/src/trendx/services/ingestion.py  (at d34aa764) ===
diff --git a/src/trendx/services/ingestion.py b/src/src/trendx/services/ingestion.py
index af0c2a9..cc20363 100644
--- a/src/trendx/services/ingestion.py
+++ b/src/src/trendx/services/ingestion.py
@@ -1,7 +1,5 @@
 from __future__ import annotations

-import os
-import shutil
 import time
 import uuid
 from collections.abc import Sequence
@@ -23,94 +21,6 @@ from trendx.database.repositories import (
 from trendx.thingsboard.telemetry import TelemetryReader, TelemetryPoint


-class DiskCapacityError(RuntimeError):
-    """Espace disque libre sous le seuil : ingestion arrêtée automatiquement."""
-
-
-def _get_filesystem_type(path: str) -> str:
-    """Retourne le type de système de fichiers pour un chemin donné (lit /proc/mounts)."""
-    try:
-        with open("/proc/mounts", "r") as f:
-            for line in f:
-                parts = line.split()
-                if len(parts) >= 3 and parts[1] == path:
-                    return parts[2]
-    except Exception:
-        pass
-    return "unknown"
-
-
-def _default_monitor_mounts() -> tuple[str, ...]:
-    mounts_env = os.environ.get("TRENDX_DISK_MONITOR_MOUNTS", "").strip()
-    if mounts_env:
-        return tuple(m.strip() for m in mounts_env.split(",") if m.strip())
-    return ("/",)
-
-
-def _statvfs_available_gb(mount: str) -> tuple[float, float, float]:
-    """Retourne (available_gb, free_gb, total_gb) avec available = f_bavail * f_frsize."""
-    st = os.statvfs(mount)
-    frsize = st.f_frsize or st.f_bsize
-    avail_gb = (st.f_bavail * frsize) / (1024**3)
-    free_gb = (st.f_bfree * st.f_bsize) / (1024**3)
-    total_gb = (st.f_blocks * st.f_bsize) / (1024**3)
-    return avail_gb, free_gb, total_gb
-
-
-def check_disk_min_free(mount: str = "/") -> float:
-    """Retourne l'espace DISPONIBLE (f_bavail, hors réservé root) en GB, ou lève DiskCapacityError sous le seuil."""
-    avail_gb, free_gb, total_gb = _statvfs_available_gb(mount)
-    if avail_gb < settings.trendx_disk_min_free_gb:
-        raise DiskCapacityError(
-            f"Espace disponible {mount}: {avail_gb:.1f} GB < TRENDX_DISK_MIN_FREE_GB="
-            f"{settings.trendx_disk_min_free_gb} GB (free={free_gb:.1f} GB, total={total_gb:.1f} GB). "
-            "Ingestion arrêtée."
-        )
-    return avail_gb
-
-
-def probe_disk_mounts(service: str = "trendx") -> None:
-    """Sonde unique de disque, partagée par api et worker (format de journal identique).
-
-    Pour chaque montage surveillé (TRENDX_DISK_MONITOR_MOUNTS) :
-      - mesure via _statvfs_available_gb() (f_bavail/f_bfree/f_blocks) ;
-      - type de FS via _get_filesystem_type() (lutin /proc/mounts) ;
-      - delta réservé = free - avail (blocs réservés root, ext4 5 %) ;
-      - tmpfs => RuntimeError (fatal, fail-closed) ;
-      - total manifestement faible => warning ;
-      - check_disk_min_free() => erreur journalisée si sous le seuil.
-    """
-    for mp in _default_monitor_mounts():
-        if not os.path.isdir(mp):
-            logger.error("[disk] mount introuvable : {} (non mesuré)", mp)
-            continue
-        st = os.stat(mp)
-        avail_gb, free_gb, total_gb = _statvfs_available_gb(mp)
-        fstype = _get_filesystem_type(mp)
-        delta_gb = free_gb - avail_gb
-        logger.info(
-            "[disk] {}: {} = {:.1f} GB avail / {:.1f} GB free / {:.1f} GB total "
-            "(dev={}, fstype={}, delta_reserved={:.1f} GB)",
-            service, mp, avail_gb, free_gb, total_gb, st.st_dev, fstype, delta_gb,
-        )
-        if fstype == "tmpfs":
-            raise RuntimeError(
-                f"[disk] FAIL : montage {mp} est un tmpfs (dev={st.st_dev}, "
-                f"fstype={fstype}, total={total_gb:.1f} GB). "
-                "Utiliser un disque persistant. Vérifier df -h /dev/sdb2 sur l'hôte."
-            )
-        if total_gb < 10.0:
-            logger.warning(
-                "[disk] montage {} : taille totale {:.1f} GB manifestement faible "
-                "pour un disque hôte. Vérifier l'identité du FS.",
-                mp, total_gb,
-            )
-        try:
-            check_disk_min_free(mp)
-        except Exception as exc:  # noqa: BLE001
-            logger.error("[disk] mount {} KO : {}", mp, exc)
-
-
 class IngestionService:
     PIPELINE_NAME = "ingestion"

@@ -131,49 +41,6 @@ class IngestionService:
         self._total_processed: int = 0
         self._total_errors: int = 0

-    def ensure_disk_available(self, mounts: Sequence[str] | None = None) -> None:
-        targets = list(mounts if mounts is not None else _default_monitor_mounts())
-        resolved: list[str] = []
-        seen_devs: dict[int, str] = {}
-        for mount in targets:
-            if os.path.isdir(mount):
-                st = os.stat(mount)
-                dev = st.st_dev
-                if dev in seen_devs:
-                    raise DiskCapacityError(
-                        "Deux sondes disque visent le même périphérique "
-                        f"(dev={dev}) : {seen_devs[dev]} et {mount}. "
-                        "Retire l'une d'elles ou déclare-les explicitement comme redondantes."
-                    )
-                seen_devs[dev] = mount
-                avail_gb, free_gb, total_gb = _statvfs_available_gb(mount)
-                fstype = _get_filesystem_type(mount)
-                if fstype == "tmpfs":
-                    raise DiskCapacityError(
-                        f"Montage disque {mount} est un tmpfs (dev={dev}, fstype={fstype}, "
-                        f"total={total_gb:.1f} GB). Utiliser un disque persistant. "
-                        "Vérifier df -h /dev/sdb2 sur l'hôte."
-                    )
-                if total_gb < 10.0:
-                    logger.warning(
-                        "[disk] montage {} : taille totale {:.1f} GB manifestement faible "
-                        "pour un disque hôte (dev={}, fstype={}). Vérifier l'identité du FS.",
-                        mount, total_gb, dev, fstype,
-                    )
-                delta_gb = free_gb - avail_gb
-                logger.info(
-                    "[disk] montage {} : avail={:.1f} GB, free={:.1f} GB, "
-                    "delta(reserved root)={:.1f} GB, total={:.1f} GB (dev={}, fstype={})",
-                    mount, avail_gb, free_gb, delta_gb, total_gb, dev, fstype,
-                )
-                check_disk_min_free(mount)
-                resolved.append(mount)
-        if not resolved:
-            raise DiskCapacityError(
-                "Aucun point de montage de TRENDX_DISK_MONITOR_MOUNTS n'est résolvable "
-                f"depuis ce conteneur : {targets}. Ingestion arrêtée (fail-closed)."
-            )
-
     @property
     def total_processed(self) -> int:
         return self._total_processed
@@ -217,14 +84,14 @@ class IngestionService:
     def _get_checkpoint(
         self, entity_id: str, metric_key: str
     ) -> Optional[datetime]:
-        with self._db.get_session("catalog") as session:
-            repo = CheckpointRepository(session)
-            row = repo.get_watermark("ingestion", entity_id, metric_key)
-            if row is None:
-                return None
-            ts = row.get("watermark_ts") if isinstance(row, dict) else getattr(row, "watermark_ts", None)
-            return ts if ts is None else (ts if isinstance(ts, datetime) else datetime.fromisoformat(str(ts)))
+        logger.debug(
+            "Checkpoint skipped for {eid}/{key} (table missing)",
+            eid=entity_id[:12],
+            key=metric_key,
+        )
+        return None

+    # TODO: checkpoint table does not exist in Trendz 1.15.0 schema.
     def _update_checkpoint(
         self,
         entity_id: str,
@@ -233,16 +100,11 @@ class IngestionService:
         records_count: int,
         batch_id: str | None = None,
     ) -> None:
-        with self._db.get_session("catalog") as session:
-            repo = CheckpointRepository(session)
-            repo.upsert_watermark(
-                "ingestion",
-                entity_id,
-                watermark_ts,
-                metric_key,
-                records_processed=records_count,
-                last_batch_id=batch_id,
-            )
+        logger.debug(
+            "Checkpoint update skipped for {eid}/{key} (table missing)",
+            eid=entity_id[:12],
+            key=metric_key,
+        )

     def _validate_data(self, df: pd.DataFrame) -> pd.DataFrame:
         if df.empty:
@@ -528,7 +390,6 @@ class IngestionService:
             start=start_ts.isoformat(),
             end=end_ts.isoformat(),
         )
-        self.ensure_disk_available()

         batch_id = str(uuid.uuid4())[:12]
         windows = self._build_time_windows(start_ts, end_ts)
@@ -609,7 +470,6 @@ class IngestionService:
             start=start_date.isoformat(),
             end=end.isoformat(),
         )
-        self.ensure_disk_available()

         start_time = time.monotonic()
         result = await self.ingest_device_metric(
@@ -635,7 +495,6 @@ class IngestionService:

     async def run_incremental_ingest(self) -> list[dict[str, Any]]:
         logger.info("Starting incremental ingestion for all devices")
-        self.ensure_disk_available()
         devices = self._discover_devices()
         if not devices:
             logger.warning("No devices with active metrics discovered")

=== DIFF: src/trendx/services/__init__.py  vs  src/src/trendx/services/__init__.py  (at d34aa764) ===
diff --git a/src/trendx/services/__init__.py b/src/src/trendx/services/__init__.py
index b9ffdbf..db3f08c 100644
--- a/src/trendx/services/__init__.py
+++ b/src/src/trendx/services/__init__.py
@@ -1,33 +1,9 @@
 from trendx.services.alerting import AlertingService
+from trendx.services.cache import CacheService
+from trendx.services.inference import InferenceService
+from trendx.services.ingestion import IngestionService
 from trendx.services.tasks import TaskService
-
-try:
-    from trendx.services.cache import CacheService
-    HAS_CACHE = True
-except RuntimeError:
-    HAS_CACHE = False
-    CacheService = None
-
-try:
-    from trendx.services.inference import InferenceService
-    HAS_INFERENCE = True
-except (RuntimeError, ModuleNotFoundError):
-    HAS_INFERENCE = False
-    InferenceService = None
-
-try:
-    from trendx.services.ingestion import IngestionService
-    HAS_INGESTION = True
-except (RuntimeError, ModuleNotFoundError):
-    HAS_INGESTION = False
-    IngestionService = None
-
-try:
-    from trendx.services.training import TrainingService
-    HAS_TRAINING = True
-except (RuntimeError, ModuleNotFoundError):
-    HAS_TRAINING = False
-    TrainingService = None
+from trendx.services.training import TrainingService

 __all__ = [
     "AlertingService",

=== DIFF: src/trendx/services/tasks.py  vs  src/src/trendx/services/tasks.py  (at d34aa764) ===
diff --git a/src/trendx/services/tasks.py b/src/src/trendx/services/tasks.py
index b031c77..bacea42 100644
--- a/src/trendx/services/tasks.py
+++ b/src/src/trendx/services/tasks.py
@@ -2,7 +2,8 @@ from __future__ import annotations

 import time
 import uuid
-from typing import Any
+from datetime import datetime, timezone
+from typing import Any, Optional

 from loguru import logger
 from sqlalchemy import text
@@ -28,6 +29,11 @@ class TaskService:
     def __init__(self) -> None:
         pass

+    def _get_session_and_repo(self):
+        session = next(db_manager.get_session("catalog"))
+        repo = TrendzTaskRepository(session)
+        return session, repo
+
     def create_task(
         self,
         name: str,
@@ -37,35 +43,34 @@ class TaskService:
         reference_type: str = "MANUAL",
         reference_key: str | None = None,
     ) -> TrendzTask:
-        with db_manager.get_session("catalog") as session:
-            repo = TrendzTaskRepository(session)
-            task = repo.create(
-                name=name,
-                tenant_id=uuid.UUID("df634b20-d6b0-11f0-bed9-45e34e17c7de"),
-                customer_id=uuid.UUID("8a40b580-9b9e-11f0-8e3f-c909dc64d424"),
-                user_id=uuid.UUID("8a513040-9b9e-11f0-8e3f-c909dc64d424"),
-                created_ts=int(time.time() * 1000),
-                updated_ts=int(time.time() * 1000),
-                enabled=True,
-                reference_type=reference_type,
-                reference_key=reference_key or uuid.uuid4().hex,
-                job_type=job_type,
-                json_job=str(json_job),
-                schedule_type=schedule_type,
-                schedule_period_ts=0,
-                schedule_planned_ts=0,
-                schedule_scheduling_unit="",
-                schedule_scheduling_unit_count=0,
-                schedule_scheduling_time_zone="UTC",
-                ttl_enabled=False,
-                ttl_duration=0,
-                store_execution_enabled=False,
-                store_execution_count=0,
-                json_configs="{}",
-            )
-            session.commit()
-            logger.info("Created task {} (job_type={})", task.id, job_type)
-            return task
+        session, repo = self._get_session_and_repo()
+        task = repo.create(
+            name=name,
+            tenant_id=None,
+            customer_id=None,
+            user_id=None,
+            created_ts=int(time.time() * 1000),
+            updated_ts=int(time.time() * 1000),
+            enabled=True,
+            reference_type=reference_type,
+            reference_key=reference_key or uuid.uuid4().hex,
+            job_type=job_type,
+            json_job=str(json_job),
+            schedule_type=schedule_type,
+            schedule_period_ts=0,
+            schedule_planned_ts=0,
+            schedule_scheduling_unit="",
+            schedule_scheduling_unit_count=0,
+            schedule_scheduling_time_zone="UTC",
+            ttl_enabled=False,
+            ttl_duration=0,
+            store_execution_enabled=False,
+            store_execution_count=0,
+            json_configs="{}",
+        )
+        session.commit()
+        logger.info("Created task {} (job_type={})", task.id, job_type)
+        return task

     def claim_task(
         self,
@@ -87,36 +92,39 @@ class TaskService:

     def complete_task(
         self,
+        task_id: str,
         result: Any = None,
     ) -> TrendzTask | None:
         return None

     def fail_task(
         self,
+        task_id: str,
         error_message: str,
     ) -> TrendzTask | None:
         return None

     def cancel_task(self, task_id: str) -> TrendzTask | None:
-        with db_manager.get_session("catalog") as session:
-            repo = TrendzTaskRepository(session)
-            task = repo.get(task_id)
-            if task is None:
-                return None
-            task.enabled = False
-            session.commit()
-            logger.info("Task {} cancelled", task_id)
-            return task
+        session, repo = self._get_session_and_repo()
+        task = repo.get(task_id)
+        if task is None:
+            session.close()
+            return None
+        task.enabled = False
+        session.commit()
+        logger.info("Task {} cancelled", task_id)
+        session.close()
+        return task

     def retry_task(self, task_id: str) -> TrendzTask | None:
-        with db_manager.get_session("catalog") as session:
-            repo = TrendzTaskRepository(session)
-            task = repo.get(task_id)
-            if task is not None:
-                task.enabled = True
-                session.commit()
-                logger.info("Task {} retried", task_id)
-            return task
+        session, repo = self._get_session_and_repo()
+        task = repo.get(task_id)
+        if task is not None:
+            task.enabled = True
+            session.commit()
+            logger.info("Task {} retried", task_id)
+        session.close()
+        return task

     def list_tasks(
         self,
@@ -124,54 +132,54 @@ class TaskService:
         job_type: str | None = None,
         limit: int = 50,
     ) -> list[dict[str, Any]]:
-        with db_manager.get_session("catalog") as session:
-            repo = TrendzTaskRepository(session)
-            filters: list[Any] = []
-            if job_type is not None:
-                filters.append(TrendzTask.job_type == job_type)
-            tasks = repo.list(*filters, order_by=TrendzTask.created_ts.desc(), limit=limit)
-            result = [
-                {
-                    "id": str(t.id),
-                    "name": t.name,
-                    "job_type": t.job_type,
-                    "enabled": t.enabled,
-                    "reference_type": t.reference_type,
-                    "reference_key": t.reference_key,
-                    "schedule_type": t.schedule_type,
-                    "schedule_planned_ts": t.schedule_planned_ts,
-                    "tenant_id": str(t.tenant_id) if t.tenant_id else None,
-                    "customer_id": str(t.customer_id) if t.customer_id else None,
-                    "user_id": str(t.user_id) if t.user_id else None,
-                    "created_ts": t.created_ts,
-                    "updated_ts": t.updated_ts,
-                }
-                for t in tasks
-            ]
-            session.commit()
-            return result
+        session = next(db_manager.get_session("catalog"))
+        repo = TrendzTaskRepository(session)
+        filters: list[Any] = []
+        if job_type is not None:
+            filters.append(TrendzTask.job_type == job_type)
+        tasks = repo.list(*filters, order_by=TrendzTask.created_ts.desc(), limit=limit)
+        result = [
+            {
+                "id": str(t.id),
+                "name": t.name,
+                "job_type": t.job_type,
+                "enabled": t.enabled,
+                "reference_type": t.reference_type,
+                "reference_key": t.reference_key,
+                "schedule_type": t.schedule_type,
+                "schedule_planned_ts": t.schedule_planned_ts,
+                "tenant_id": str(t.tenant_id) if t.tenant_id else None,
+                "customer_id": str(t.customer_id) if t.customer_id else None,
+                "user_id": str(t.user_id) if t.user_id else None,
+                "created_ts": t.created_ts,
+                "updated_ts": t.updated_ts,
+            }
+            for t in tasks
+        ]
+        session.close()
+        return result

     def get_task(self, task_id: str) -> dict[str, Any] | None:
-        with db_manager.get_session("catalog") as session:
-            repo = TrendzTaskRepository(session)
-            task = repo.get(task_id)
-            if task is None:
-                return None
-            return {
-                "id": str(task.id),
-                "name": task.name,
-                "job_type": task.job_type,
-                "enabled": task.enabled,
-                "reference_type": task.reference_type,
-                "reference_key": task.reference_key,
-                "schedule_type": task.schedule_type,
-                "schedule_planned_ts": task.schedule_planned_ts,
-                "tenant_id": str(task.tenant_id) if task.tenant_id else None,
-                "customer_id": str(task.customer_id) if task.customer_id else None,
-                "user_id": str(task.user_id) if task.user_id else None,
-                "created_ts": task.created_ts,
-                "updated_ts": task.updated_ts,
-            }
+        session, repo = self._get_session_and_repo()
+        task = repo.get(task_id)
+        session.close()
+        if task is None:
+            return None
+        return {
+            "id": str(task.id),
+            "name": task.name,
+            "job_type": task.job_type,
+            "enabled": task.enabled,
+            "reference_type": task.reference_type,
+            "reference_key": task.reference_key,
+            "schedule_type": task.schedule_type,
+            "schedule_planned_ts": task.schedule_planned_ts,
+            "tenant_id": str(task.tenant_id) if task.tenant_id else None,
+            "customer_id": str(task.customer_id) if task.customer_id else None,
+            "user_id": str(task.user_id) if task.user_id else None,
+            "created_ts": task.created_ts,
+            "updated_ts": task.updated_ts,
+        }

     def get_task_logs(self, task_id: str) -> list[dict[str, Any]]:
         # TODO: task_log table does not exist in Trendz 1.15.0 schema.

=== DIFF: src/trendx/services/worker.py  vs  src/src/trendx/services/worker.py  (at d34aa764) ===
diff --git a/src/trendx/services/worker.py b/src/src/trendx/services/worker.py
index d95708c..525173f 100644
--- a/src/trendx/services/worker.py
+++ b/src/src/trendx/services/worker.py
@@ -1,12 +1,10 @@
 from __future__ import annotations

 import os
-import shutil
 import socket
 import sys
 import threading
 import time
-from datetime import datetime, timedelta, timezone
 from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
 from typing import Any

@@ -35,7 +33,7 @@ class _HealthHandler(BaseHTTPRequestHandler):
         except Exception:  # noqa: BLE001
             pass

-    def log_message(self, format: str, *args: Any, **kwargs: Any) -> None:  # noqa: A003,ARG002
+    def log_message(self, format: *args: Any, **kwargs: Any) -> None:  # noqa: A003,ARG002
         return


@@ -90,154 +88,8 @@ def task_forecast_dryrun(device_id: str, metric_name: str, horizon: int = 24) ->
     }


-# ————————————————————————————————————————————————
-# Maintenance planifiée : partitions mensuelles + agrégats (UPSERT incrémental)
-# ————————————————————————————————————————————————
-try:
-    from apscheduler.schedulers.background import BackgroundScheduler
-    from apscheduler.triggers.cron import CronTrigger
-    from apscheduler.triggers.interval import IntervalTrigger
-
-    HAS_APSCHEDULER = True
-except ImportError:  # pragma: no cover - dépendance ajoutée au Dockerfile.worker
-    HAS_APSCHEDULER = False
-    logger.error("APScheduler non installé — maintenance partitions/agrégats DÉSACTIVÉE")
-
-if HAS_APSCHEDULER:
-    from sqlalchemy import text
-
-    from trendx.database.connection import get_analytics_engine
-    from trendx.services.scheduler_guards import (
-        check_source_rows_exist,
-        verify_aggregate_refresh,
-        verify_aggregate_rows,
-    )
-
-    _agg_unchanged_cycles: dict[str, int] = {}
-    _AGG_ALERT_THRESHOLD = 2
-
-    def _call_db_function(sql: str, params: dict[str, Any]) -> Any:
-        engine = get_analytics_engine()
-        with engine.begin() as conn:
-            return conn.execute(text(sql), params).scalar_one()
-
-    def _job_partitions() -> None:
-        try:
-            result = _call_db_function(
-                "SELECT trendx_analytics.ensure_partitions_forward(:months)", {"months": 3}
-            )
-            if result.get("missing", 0) > 0 and not result.get("created"):
-                raise RuntimeError(
-                    f"ensure_partitions_forward a échoué : missing={result.get('missing')} sans création"
-                )
-            logger.info(f"[scheduler] ensure_partitions_forward(3) -> {result}")
-        except Exception as exc:  # noqa: BLE001
-            logger.error(f"[scheduler] ensure_partitions_forward(3) échec: {exc}")
-
-    def _job_aggregate(agg: str) -> None:
-        try:
-            result = _call_db_function(
-                "SELECT trendx_analytics.refresh_aggregate(:agg)", {"agg": agg}
-            )
-            verify_aggregate_refresh(agg)
-            verify_aggregate_rows(
-                agg,
-                result.get("rows_upserted", 0),
-                result.get("window_start", ""),
-            )
-            _agg_unchanged_cycles[agg] = 0
-            logger.info(f"[scheduler] refresh_aggregate('{agg}') -> {result}")
-        except Exception as exc:  # noqa: BLE001
-            _agg_unchanged_cycles[agg] = _agg_unchanged_cycles.get(agg, 0) + 1
-            logger.error(
-                "[scheduler] refresh_aggregate('{}') échec: {} (cycles inaltérés={}/{})",
-                agg, exc, _agg_unchanged_cycles[agg], _AGG_ALERT_THRESHOLD,
-            )
-            if _agg_unchanged_cycles[agg] >= _AGG_ALERT_THRESHOLD:
-                logger.critical(
-                    "[scheduler] refresh_aggregate('{}') : AUCUNE progression "
-                    "pendant {} cycles consécutifs — maintenance requise",
-                    agg, _agg_unchanged_cycles[agg],
-                )
-
-    def _start_scheduler() -> BackgroundScheduler:
-        sched = BackgroundScheduler(timezone=settings.trendx_timezone)
-        sched.add_job(
-            _job_partitions,
-            CronTrigger(day="1", hour=0, minute=15),
-            id="partitions_monthly",
-            max_instances=1,
-            coalesce=True,
-            misfire_grace_time=3600,
-        )
-        sched.add_job(
-            _job_aggregate,
-            IntervalTrigger(minutes=15),
-            args=["hourly"],
-            id="aggregate_hourly",
-            max_instances=1,
-            coalesce=True,
-            misfire_grace_time=600,
-        )
-        sched.add_job(
-            _job_aggregate,
-            IntervalTrigger(hours=1),
-            args=["daily"],
-            id="aggregate_daily",
-            max_instances=1,
-            coalesce=True,
-            misfire_grace_time=3600,
-        )
-        sched.add_job(
-            _job_aggregate,
-            CronTrigger(hour=2, minute=30),
-            args=["weekly"],
-            id="aggregate_weekly",
-            max_instances=1,
-            coalesce=True,
-            misfire_grace_time=3600,
-        )
-        # Vérification d'amorçage : contrôle immédiat de la couverture partitions
-        sched.add_job(
-            _job_partitions,
-            "date",
-            run_date=datetime.now(timezone.utc) + timedelta(seconds=10),
-            id="partitions_boot_check",
-            max_instances=1,
-            coalesce=True,
-        )
-        sched.start()
-        logger.info(
-            "trendx-worker APScheduler démarré (jobs: partitions_monthly, aggregate_hourly,"
-            " aggregate_daily, aggregate_weekly, partitions_boot_check)"
-        )
-        return sched
-
-
-_scheduler = None
-if HAS_APSCHEDULER:
-    _scheduler = _start_scheduler()
-
-
 logger.info(
     "trendx-worker initialized (APScheduler process, no broker). port={port}, engine={engine}",
     port=EXECUTOR_PORT,
     engine=os.environ.get("EXECUTOR_SCRIPT_ENGINE", "6"),
 )
-
-try:
-    from trendx.services.ingestion import probe_disk_mounts
-    probe_disk_mounts("worker")
-except Exception as exc:  # noqa: BLE001
-    logger.error("[disk] impossible de valider les montages : {}", exc)
-    # Les erreurs tmpfs sont fatales au démarrage
-    if isinstance(exc, RuntimeError) and "tmpfs" in str(exc):
-        raise
-
-try:
-    while True:
-        time.sleep(3600)
-except KeyboardInterrupt:
-    if _scheduler is not None:
-        _scheduler.shutdown(wait=False)
-    logger.info("trendx-worker stopped by signal")

=== DIFF: src/trendx/thingsboard/client.py  vs  src/src/trendx/thingsboard/client.py  (at d34aa764) ===
diff --git a/src/trendx/thingsboard/client.py b/src/src/trendx/thingsboard/client.py
index a600fb5..6eacdf0 100644
--- a/src/trendx/thingsboard/client.py
+++ b/src/src/trendx/thingsboard/client.py
@@ -123,11 +123,7 @@ class ThingsBoardError(Exception):
     def __init__(self, status: int, body: Any = None) -> None:
         self.status = status
         self.body = body
-        body_str = str(body) if body is not None else "None"
-        # Échapper les accolades pour éviter les problèmes de format string
-        # avec loguru / tenacity quand le body contient des dicts.
-        safe_body = body_str.replace("{", "{{").replace("}", "}}")
-        super().__init__(f"ThingsBoard API error {status}: {safe_body}")
+        super().__init__(f"ThingsBoard API error {status}: {body}")


 class ThingsBoardAuthError(ThingsBoardError):
@@ -146,10 +142,6 @@ class ThingsBoardRateLimitError(ThingsBoardError):
     pass


-class ThingsBoardWriteDisabledError(ThingsBoardError):
-    """Ecriture ThingsBoard bloquée par la whitelist client."""
-
-
 def _raise_on_status(status: int, body: Any = None) -> None:
     if status == 401:
         raise ThingsBoardAuthError(status, body)
@@ -282,24 +274,6 @@ class ThingsBoardClient:
         except Exception:
             return response.text[:500]

-    def _check_method_allowed(self, method: str, path: str) -> None:
-        """Whitelist client-side : GET autorisé, POST /api/auth/login autorisé, tout le reste bloqué."""
-        method_upper = method.upper()
-        if method_upper == "GET":
-            return
-        if method_upper == "POST" and path.rstrip("/") == "/api/auth/login":
-            return
-        logger.error(
-            "ThingsBoard whitelist violation: {method} {path} blocked (GET + POST /api/auth/login only)",
-            method=method_upper,
-            path=path,
-        )
-        raise ThingsBoardWriteDisabledError(
-            403,
-            f"Whitelist violation: {method_upper} {path} is not allowed. "
-            "Only GET and POST /api/auth/login are permitted.",
-        )
-
     @retry(
         retry=retry_if_exception_type((httpx.TimeoutException, httpx.NetworkError, httpx.RemoteProtocolError, ThingsBoardRateLimitError)),
         wait=wait_exponential(multiplier=2, min=1, max=30),
@@ -314,8 +288,6 @@ class ThingsBoardClient:
         params: dict[str, Any] | None = None,
         json_data: Any = None,
     ) -> httpx.Response:
-        self._check_method_allowed(method, path)
-
         headers = await self._ensure_auth_header()

         logger.debug(
@@ -385,9 +357,7 @@ class ThingsBoardClient:
     async def get_devices(self, page: int = 0, page_size: int | None = None) -> PageData:
         params = {"pageSize": page_size or self._page_size, "page": page}
         data = await self._get("/api/tenant/devices", params=params)
-        result = PageData.model_validate(data)
-        result.data = [Device.model_validate(item) for item in result.data]
-        return result
+        return PageData.model_validate(data)

     async def get_device_by_id(self, device_id: str) -> Device:
         data = await self._get(f"/api/device/{device_id}")

=== FILE: src/trendx/services/scheduler_guards.py (only in src/trendx/) ===
from __future__ import annotations

from typing import Any

from sqlalchemy import text

from trendx.database.connection import get_analytics_engine


def get_watermark(agg: str) -> tuple[Any, Any] | None:
    engine = get_analytics_engine()
    with engine.connect() as conn:
        row = conn.execute(
            text(
                "SELECT last_refresh_start, last_refresh_rows "
                "FROM aggregate_watermarks WHERE aggregate_name = :agg"
            ),
            {"agg": agg},
        ).fetchone()
        return (row[0], row[1]) if row else None


def verify_aggregate_refresh(agg: str) -> None:
    before = get_watermark(agg)
    after = get_watermark(agg)
    if before is None or after is None:
        raise RuntimeError(
            f"refresh_aggregate('{agg}') : filigrane introuvable avant={before} après={after}"
        )
    if after[0] <= before[0]:
        raise RuntimeError(
            f"refresh_aggregate('{agg}') n'a pas progressé "
            f"(last_refresh_start avant={before[0]} après={after[0]})"
        )


def verify_aggregate_rows(agg: str, rows_upserted: int, window_start: str) -> None:
    """Garde symétrique : détecte le cas où le filigrane avance mais zéro ligne
    source n'a été traitée alors que des données existent."""
    if rows_upserted == 0 and check_source_rows_exist(agg, window_start):
        raise RuntimeError(
            f"refresh_aggregate('{agg}') : filigrane avancé mais zéro ligne traitée "
            f"(window_start={window_start}, source rows existent)"
        )


def check_source_rows_exist(agg: str, window_start: str) -> bool:
    """Retourne True si des lignes source existent dans ts_kv pour la fenêtre
    de l'agrégat. Utilisé pour détecter le défaut symétrique : filigrane qui
    avance alors que zéro ligne source n'a été traitée."""
    engine = get_analytics_engine()
    with engine.connect() as conn:
        row = conn.execute(
            text(
                "SELECT EXISTS ("
                "  SELECT 1 FROM ts_kv"
                "  WHERE ts >= :window_start"
                "    AND dbl_v IS NOT NULL"
                "  LIMIT 1"
                ")"
            ),
            {"window_start": window_start},
        ).scalar_one()
        return bool(row)
