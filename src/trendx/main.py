from __future__ import annotations

import hmac
import os
import re
import sys
import time
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.exception_handlers import request_validation_exception_handler
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.openapi.utils import get_openapi
from fastapi.responses import JSONResponse
from loguru import logger
from pydantic import BaseModel, Field
from sqlalchemy import select, text
from trendx.config import settings
from trendx.database.connection import manager as db_manager
from trendx.database.models import (
    AlertRule,
    BusinessEntity,
    EntityRelation,
    MetricDefinition,
    Prediction,
    TrendzTask,
    TrendzTaskExecutionRequest,
)
from trendx.database.repositories import (
    AlertIncidentRepository,
    AlertRuleRepository,
    BusinessEntityRepository,
    EntityRelationRepository,
    MetricDefinitionRepository,
    PredictionModelRepository,
    TrendzTaskRepository,
)
from trendx.explorer.router import router as explorer_router
from trendx.forecasting.api import router as forecast_execution_router
from trendx.forecasting.audit import (
    AuditActorType,
    AuditError,
    AuditOperation,
    AuditOutcome,
    ExecutionAuditService,
    ExecutionAuditStore,
)
from trendx.forecasting.audit_health import AuditHealthService
from trendx.forecasting.audit_lifecycle import (
    AuditArchiveError,
    AuditLifecycleService,
    FileSystemAuditArchiveStore,
)
from trendx.forecasting.execution import DurableExecutionStore, ExecutionStoreError
from trendx.forecasting.history import ExecutionHistoryService
from trendx.forecasting.lifecycle import ExecutionHistoryLifecycle, FileSystemArchiveStore
from trendx.forecasting.recovery import RecoveryService
from trendx.services.tasks import PENDING_STATE, TaskService


class HealthResponse(BaseModel):
    status: str = Field(default="ok")
    service: str = Field(default="trendx-api")
    detail: str | None = None


logger.remove()
logger.add(
    sys.stderr,
    level=settings.trendx_log_level,
    format="<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> | request_id={extra[request_id]} - <level>{message}</level>",
    colorize=True,
)
logger.configure(extra={"request_id": "-"})

APP_VERSION = "0.1.0"

app = FastAPI(
    title="Trendx API",
    description="Plateforme Trendx — analyse, prévision, anomalies (parité Trendz 1.15.0)",
    version=APP_VERSION,
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["X-Request-ID"],
)

app.include_router(explorer_router)
app.include_router(forecast_execution_router)


def _openapi() -> dict[str, Any]:
    """Document existing authentication and W110 audit correlation."""

    if app.openapi_schema is not None:
        return app.openapi_schema
    schema = get_openapi(
        title=app.title,
        version=app.version,
        description=app.description,
        routes=app.routes,
    )
    components = schema.setdefault("components", {})
    security_schemes = components.setdefault("securitySchemes", {})
    security_schemes.setdefault(
        "BearerAuth",
        {"type": "http", "scheme": "bearer"},
    )
    security_schemes.setdefault(
        "ApiKeyAuth",
        {"type": "apiKey", "in": "header", "name": "X-API-Key"},
    )
    headers = components.setdefault("headers", {})
    headers.setdefault(
        "XRequestId",
        {
            "description": "Bounded non-secret correlation identifier",
            "schema": {"type": "string", "maxLength": 128},
        },
    )
    for path in (
        "/api/v1/forecast/executions/audit",
        "/api/v1/forecast/executions/audit/statistics",
        "/api/v1/forecast/executions/audit/integrity",
        "/api/v1/forecast/executions/audit/reconciliation",
        "/api/v1/forecast/executions/audit/export",
        "/api/v1/forecast/executions/audit/export/checksum",
        "/api/v1/forecast/executions/audit/lifecycle/preview",
        "/api/v1/forecast/executions/audit/lifecycle/archive",
        "/api/v1/forecast/executions/audit/lifecycle/purge",
        "/api/v1/forecast/executions/audit/lifecycle/restore",
        "/api/v1/forecast/executions/audit/health",
        "/api/v1/forecast/executions/audit/capacity",
        "/api/v1/forecast/executions/audit/readiness",
    ):
        operation = schema.get("paths", {}).get(path, {}).get("get")
        if not isinstance(operation, dict):
            continue
        parameters = operation.setdefault("parameters", [])
        if not any(
            isinstance(parameter, dict) and parameter.get("name") == "X-Request-ID"
            for parameter in parameters
        ):
            parameters.append(
                {
                    "name": "X-Request-ID",
                    "in": "header",
                    "required": False,
                    "description": "Optional bounded request correlation identifier",
                    "schema": {"type": "string", "maxLength": 128},
                }
            )
        success = operation.setdefault("responses", {}).setdefault("200", {})
        success.setdefault("headers", {})["X-Request-ID"] = {
            "$ref": "#/components/headers/XRequestId"
        }
    app.openapi_schema = schema
    return schema


app.openapi = _openapi  # type: ignore[method-assign]


# ── Authentication ────────────────────────────────────────────────────
# Tous les endpoints /api/v1/* exigent un jeton (Bearer ou X-API-Key),
# vérifié en temps constant. Endpoints publics : infrastructure seulement.


PUBLIC_PATHS = {"/", "/health", "/metrics", "/docs", "/redoc", "/openapi.json"}


@app.middleware("http")
async def require_auth_middleware(request: Request, call_next):
    path = request.url.path
    if path in PUBLIC_PATHS:
        return await call_next(request)
    if path.startswith("/api/v1"):
        provided: str | None = None
        authorization = request.headers.get("authorization")
        x_api_key = request.headers.get("x-api-key")
        if x_api_key:
            provided = x_api_key
        elif authorization and authorization.lower().startswith("bearer "):
            provided = authorization[7:].strip()
        if not provided:
            logger.warning("API auth refusée (jeton absent) : {}", path)
            return JSONResponse(
                content={"detail": "Missing authentication credentials"},
                status_code=401,
            )
        expected = settings.trendx_api_token.get_secret_value()
        if not hmac.compare_digest(provided.encode("utf-8"), expected.encode("utf-8")):
            logger.warning("API auth refusée (jeton invalid) : {}", path)
            return JSONResponse(
                content={"detail": "Invalid authentication credentials"},
                status_code=401,
            )
    return await call_next(request)


_REQUEST_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")


@app.middleware("http")
async def request_correlation_middleware(request: Request, call_next):
    """Attach one bounded, non-secret correlation id to every HTTP request."""

    supplied = request.headers.get("x-request-id", "").strip()
    supplied_folded = supplied.casefold()
    safe_supplied = _REQUEST_ID_PATTERN.fullmatch(supplied) and not any(
        marker in supplied_folded
        for marker in ("authorization", "bearer", "password", "secret", "token", "api_key")
    )
    request_id = supplied if safe_supplied else f"request-{uuid.uuid4()}"
    request.state.request_id = request_id
    with logger.contextualize(request_id=request_id):
        response = await call_next(request)
    response.headers["X-Request-ID"] = request_id
    return response


@app.exception_handler(RequestValidationError)
async def _sanitize_recovery_validation(
    request: Request,
    exc: RequestValidationError,
):
    """Keep restore validation errors free of request payloads and paths."""

    if request.url.path == "/api/v1/forecast/executions/restore":
        logger.info("execution_history operation=restore outcome=invalid_request status_code=422")
        tenant_id = settings.trendx_default_tenant_id.strip()
        if tenant_id:
            factory = getattr(request.app.state, "execution_audit_service_factory", None)
            try:
                service = factory() if callable(factory) else None
                if service is not None:
                    service.record(
                        operation=AuditOperation.RECOVERY_API,
                        outcome=AuditOutcome.REJECTED,
                        tenant_id=tenant_id,
                        actor_type=AuditActorType.API,
                        actor_id="api-key-context",
                        request_id=getattr(request.state, "request_id", None),
                        source="w109-http",
                        reason_code="invalid_request",
                    )
            except Exception as exc:
                logger.warning(
                    "execution_audit operation=recovery_api outcome=rejected "
                    "reason_code=audit_write_failed error_type={}",
                    type(exc).__name__,
                )
        return JSONResponse(status_code=422, content={"detail": "invalid_request"})
    if request.url.path in {
        "/api/v1/forecast/executions/audit",
        "/api/v1/forecast/executions/audit/statistics",
        "/api/v1/forecast/executions/audit/integrity",
        "/api/v1/forecast/executions/audit/reconciliation",
        "/api/v1/forecast/executions/audit/export",
        "/api/v1/forecast/executions/audit/export/checksum",
        "/api/v1/forecast/executions/audit/lifecycle/preview",
        "/api/v1/forecast/executions/audit/lifecycle/archive",
        "/api/v1/forecast/executions/audit/lifecycle/purge",
        "/api/v1/forecast/executions/audit/lifecycle/restore",
        "/api/v1/forecast/executions/audit/health",
        "/api/v1/forecast/executions/audit/capacity",
        "/api/v1/forecast/executions/audit/readiness",
    }:
        logger.info("execution_history operation=audit outcome=invalid_request status_code=422")
        return JSONResponse(status_code=422, content={"detail": "invalid_audit_query"})
    return await request_validation_exception_handler(request, exc)


def _execution_history_service_factory() -> ExecutionHistoryService:
    """Build a fresh read-only history service from the configured datastore."""

    configured_path = settings.trendx_execution_history_path.strip()
    if not configured_path:
        msg = "Forecast execution history is not configured"
        raise ExecutionStoreError(msg)
    history_path = Path(configured_path)
    if not history_path.is_file():
        msg = "Forecast execution history datastore is unavailable"
        raise ExecutionStoreError(msg)
    return ExecutionHistoryService(DurableExecutionStore(history_path))


def _execution_audit_service_factory() -> ExecutionAuditService | None:
    """Build the optional best-effort W110 audit service from server config."""

    configured_path = settings.trendx_execution_audit_path.strip()
    if not configured_path:
        return None
    audit_path = Path(configured_path)
    history_text = settings.trendx_execution_history_path.strip()
    archive_text = settings.trendx_execution_archive_path.strip()
    try:
        business_paths = [Path(value).resolve() for value in (history_text, archive_text) if value]
        resolved_audit = audit_path.resolve()
        overlaps_business_store = any(
            resolved_audit == business_path
            or resolved_audit in business_path.parents
            or business_path in resolved_audit.parents
            for business_path in business_paths
        )
    except (OSError, RuntimeError, ValueError):
        overlaps_business_store = True
    if overlaps_business_store:
        logger.warning("execution_audit backend_unavailable reason=store_overlap")
        return None
    if audit_path.is_symlink():
        logger.warning("execution_audit backend_unavailable reason=unsafe_store_path")
        return None
    try:
        return ExecutionAuditService(ExecutionAuditStore(audit_path))
    except AuditError:
        logger.warning("execution_audit backend_unavailable reason=store_initialization_failed")
        return None
    except OSError:
        logger.warning("execution_audit backend_unavailable reason=store_initialization_failed")
        return None
    except Exception:
        logger.warning("execution_audit backend_unavailable reason=store_initialization_failed")
        return None


def _execution_audit_archive_store_factory() -> FileSystemAuditArchiveStore | None:
    """Build the optional W112 audit archive, refused when it overlaps the audit store."""

    configured_path = settings.trendx_execution_audit_archive_path.strip()
    if not configured_path:
        return None
    archive_path = Path(configured_path)
    audit_text = settings.trendx_execution_audit_path.strip()
    try:
        audit_resolved = Path(audit_text).resolve() if audit_text else None
        resolved_archive = archive_path.resolve()
        overlaps_audit_store = audit_resolved is not None and (
            resolved_archive == audit_resolved
            or resolved_archive in audit_resolved.parents
            or audit_resolved in resolved_archive.parents
        )
    except (OSError, RuntimeError, ValueError):
        overlaps_audit_store = True
    if overlaps_audit_store:
        logger.warning("audit_lifecycle backend_unavailable reason=archive_overlap")
        return None
    if archive_path.is_symlink():
        logger.warning("audit_lifecycle backend_unavailable reason=unsafe_archive_path")
        return None
    try:
        return FileSystemAuditArchiveStore(archive_path)
    except AuditArchiveError:
        logger.warning("audit_lifecycle backend_unavailable reason=archive_unavailable")
        return None
    except OSError:
        logger.warning("audit_lifecycle backend_unavailable reason=archive_unavailable")
        return None
    except Exception:
        logger.warning("audit_lifecycle backend_unavailable reason=archive_unavailable")
        return None


def _execution_audit_lifecycle_service_factory() -> AuditLifecycleService | None:
    """Build the optional W112 lifecycle service from server configuration.

    W112 never runs by itself: it is only reachable through an authenticated,
    explicit lifecycle request.
    """

    audit_service = _execution_audit_service_factory()
    if audit_service is None:
        return None
    archive_store = _execution_audit_archive_store_factory()
    if archive_store is None:
        logger.warning("audit_lifecycle backend_unavailable reason=archive_not_configured")
        return None
    return AuditLifecycleService(audit_service, archive_store)


def _execution_audit_health_service_factory() -> AuditHealthService | None:
    """Build the optional W113 read-only diagnostic service.

    W113 only reads: it never mutates the audit store or the archive, and it
    never creates an audit event while reporting on the audit trail.
    """

    audit_service = _execution_audit_service_factory()
    if audit_service is None:
        return None
    archive_store = _execution_audit_archive_store_factory()
    lifecycle = (
        AuditLifecycleService(audit_service, archive_store) if archive_store is not None else None
    )
    audit_path = settings.trendx_execution_audit_path.strip()
    archive_path = settings.trendx_execution_audit_archive_path.strip()
    return AuditHealthService(
        audit_service,
        archive_store=archive_store,
        lifecycle=lifecycle,
        active_path=Path(audit_path) if audit_path else None,
        archive_path=Path(archive_path) if archive_path else None,
    )


def _execution_recovery_service_factory() -> RecoveryService:
    """Build W108 recovery around server-configured, existing local stores."""

    configured_history_path = settings.trendx_execution_history_path.strip()
    configured_archive_path = settings.trendx_execution_archive_path.strip()
    if not configured_history_path or not configured_archive_path:
        msg = "Execution history recovery is not configured"
        raise ExecutionStoreError(msg)
    history_path = Path(configured_history_path)
    archive_path = Path(configured_archive_path)
    if history_path.is_symlink() or not history_path.is_file():
        msg = "Forecast execution history datastore is unavailable"
        raise ExecutionStoreError(msg)
    if archive_path.is_symlink() or not archive_path.is_dir():
        msg = "Execution history archive store is unavailable"
        raise ExecutionStoreError(msg)
    audit_service = _execution_audit_service_factory()
    return RecoveryService(
        DurableExecutionStore(
            history_path,
            create_if_missing=False,
            audit_service=audit_service,
        ),
        FileSystemArchiveStore(archive_path),
        lambda: datetime.now(UTC),
        audit_service=audit_service,
    )


def _execution_lifecycle_service_factory() -> ExecutionHistoryLifecycle:
    """Build W107 lifecycle around configured, existing local stores."""

    configured_history_path = settings.trendx_execution_history_path.strip()
    configured_archive_path = settings.trendx_execution_archive_path.strip()
    if not configured_history_path or not configured_archive_path:
        msg = "Execution lifecycle is not configured"
        raise ExecutionStoreError(msg)
    history_path = Path(configured_history_path)
    archive_path = Path(configured_archive_path)
    if history_path.is_symlink() or not history_path.is_file():
        msg = "Forecast execution history datastore is unavailable"
        raise ExecutionStoreError(msg)
    if archive_path.is_symlink() or not archive_path.is_dir():
        msg = "Execution history archive store is unavailable"
        raise ExecutionStoreError(msg)
    audit_service = _execution_audit_service_factory()
    return ExecutionHistoryLifecycle(
        DurableExecutionStore(
            history_path,
            create_if_missing=False,
            audit_service=audit_service,
        ),
        FileSystemArchiveStore(archive_path),
        lambda: datetime.now(UTC),
        audit_service=audit_service,
    )


# The factories are lazy: importing the API never creates or mutates a datastore.
app.state.execution_history_service_factory = _execution_history_service_factory
app.state.execution_recovery_service_factory = _execution_recovery_service_factory
app.state.execution_lifecycle_service_factory = _execution_lifecycle_service_factory
app.state.execution_audit_service_factory = _execution_audit_service_factory
app.state.execution_audit_lifecycle_service_factory = _execution_audit_lifecycle_service_factory
app.state.execution_audit_health_service_factory = _execution_audit_health_service_factory


@app.on_event("startup")
async def _log_startup_disk_check() -> None:
    # Sonde disque UNIQUE, partagée avec le worker (probe_disk_mounts) :
    # _statvfs_available_gb + fstype + delta réservé, format de journal identique.
    # tmpfs => fatal (fail-closed), cohérent avec le worker.
    from trendx.services.ingestion import probe_disk_mounts

    try:
        probe_disk_mounts("api")
    except RuntimeError as exc:
        logger.error("[disk] {} : sonde disque fatale", exc)
        raise
    except Exception as exc:
        logger.warning("API startup disk check failed: {}", exc)


# ── Pydantic models ─────────────────────────────────────────────────────


class PaginatedResponse(BaseModel):
    data: list[Any]
    total: int
    page: int
    page_size: int
    has_next: bool


class DeviceOut(BaseModel):
    id: str
    name: str
    description: str | None = None
    hidden: bool = False
    shared_with_customers: bool = False
    tenant_id: str | None = None
    query: str | None = None


class MetricOut(BaseModel):
    id: str
    name: str
    item_name: str
    business_entity_id: str
    item_id: str
    tenant_id: str
    user_input: str
    description: str
    how_to_calculate: str
    is_advanced_mode: bool = False
    created_ts: int | None = None
    updated_ts: int | None = None


class ProfileOut(BaseModel):
    id: str
    name: str
    type: str
    description: str | None = None
    is_default: bool
    transport_type: str | None = None


class RelationOut(BaseModel):
    business_entity_id: str
    name: str
    related_entity_id: str
    direction: str
    enabled: bool = True
    query: str | None = None


class SyncStatusOut(BaseModel):
    last_sync_ts: str | None = None
    sync_count: int = 0
    entity_count: int = 0
    relation_count: int = 0
    status: str = "idle"


class DiscoveryTriggerOut(BaseModel):
    status: str
    message: str
    task_id: str | None = None


class TelemetryPointOut(BaseModel):
    ts: str
    value: float | None = None


class TelemetryStatsOut(BaseModel):
    min: float | None = None
    max: float | None = None
    avg: float | None = None
    std: float | None = None
    count: int = 0
    null_count: int = 0


class TrainRequest(BaseModel):
    entity_id: str
    metric_key: str
    algorithm: str = "Prophet"
    params: dict[str, Any] | None = None
    lookback_days: int | None = None
    frequency: str = "1h"
    horizon: int | None = None


class TrainResponse(BaseModel):
    model_id: str
    entity_id: str
    metric_key: str
    algorithm: str
    status: str
    message: str | None = None


class PredictRequest(BaseModel):
    entity_id: str
    metric_key: str
    horizon: int | None = None


class ForecastResultOut(BaseModel):
    timestamps: list[str]
    values: list[float]
    lower_bound: list[float]
    upper_bound: list[float]
    model_name: str
    horizon: int


class ForecastModelOut(BaseModel):
    id: str
    name: str
    type: str
    status: str
    tb_telemetry_key: str
    business_entity_id: str
    enabled: bool
    created_ts: int | None = None
    updated_ts: int | None = None


class CompetitionRequest(BaseModel):
    entity_id: str
    metric_key: str
    candidates: list[dict[str, Any]] | None = None


class ChampionOut(BaseModel):
    id: str
    name: str
    type: str
    status: str
    business_entity_id: str
    tb_telemetry_key: str
    enabled: bool
    created_ts: int | None = None


class AnomalyTrainRequest(BaseModel):
    entity_id: str
    metric_key: str
    algorithm: str = "IForest"
    contamination: float = 0.01
    window_size: int = 24
    params: dict[str, Any] | None = None


class AnomalyScanRequest(BaseModel):
    entity_id: str
    metric_key: str
    detector_id: str | None = None


class AnomalyScoreOut(BaseModel):
    t: int
    s: float
    anomaly_id: str | None = None


class AnomalyEpisodeOut(BaseModel):
    id: str
    item_id: str | None = None
    item_name: str | None = None
    start_ts: int | None = None
    end_ts: int | None = None
    cluster_id: int | None = None
    score: float | None = None
    score_index: int | None = None
    model_id: str | None = None
    alarm_id: str | None = None


class TaskCreateRequest(BaseModel):
    name: str
    job_type: str = Field(alias="task_type")
    json_job: dict[str, Any] = Field(default_factory=dict, alias="payload")
    schedule_type: str = "NOT_SCHEDULED"
    reference_type: str = "MANUAL"
    reference_key: str | None = None

    model_config = {"populate_by_name": True}


class TaskOut(BaseModel):
    id: str
    name: str
    job_type: str
    enabled: bool
    reference_type: str
    reference_key: str
    schedule_type: str
    schedule_planned_ts: int | None = None
    tenant_id: str | None = None
    customer_id: str | None = None
    user_id: str | None = None
    created_ts: int | None = None
    updated_ts: int | None = None


class AlertIncidentOut(BaseModel):
    id: int
    logical_key: str
    entity_id: str
    metric_key: str | None = None
    severity: str
    status: str
    opened_at: str
    acknowledged_at: str | None = None
    cleared_at: str | None = None
    closed_at: str | None = None
    last_value: float | None = None
    open_reason: str | None = None
    opened_count: int


class AlertRuleCreate(BaseModel):
    name: str
    entity_id: str | None = None
    metric_key: str | None = None
    rule_type: str
    condition_json: dict[str, Any] = Field(default_factory=dict)
    cooldown_seconds: int = 7200
    min_duration_seconds: int = 60
    severity: str = "MEDIUM"
    is_active: bool = True


class AlertRuleUpdate(BaseModel):
    name: str | None = None
    condition_json: dict[str, Any] | None = None
    cooldown_seconds: int | None = None
    min_duration_seconds: int | None = None
    severity: str | None = None
    is_active: bool | None = None


class AlertRuleOut(BaseModel):
    id: str
    name: str
    entity_id: str | None = None
    metric_key: str | None = None
    rule_type: str
    condition_json: dict[str, Any]
    cooldown_seconds: int
    min_duration_seconds: int
    severity: str
    is_active: bool
    created_at: str
    updated_at: str


class HealthDetailedOut(BaseModel):
    service: str = "trendx-api"
    version: str = APP_VERSION
    status: str = "ok"
    database: dict[str, Any]
    timestamp: str


class ConfigOut(BaseModel):
    trendx_env: str
    trendx_log_level: str
    trendx_timezone: str
    trendx_host: str
    trendx_api_port: int
    trendx_workers: int
    tb_base_url: str
    tb_device_id: str
    tb_metric_name: str
    tb_writeback_enabled: bool
    tb_alarms_enabled: bool
    forecast_frequency: str
    forecast_horizon: int
    training_lookback_days: int
    anomaly_detection_enabled: bool
    anomaly_contamination: float
    anomaly_window_size: int
    grafana_host_port: int


class StatsOut(BaseModel):
    devices: int = 0
    metrics: int = 0
    models: int = 0
    tasks: dict[str, int] = Field(default_factory=dict)
    alerts_active: int = 0
    anomalies_total: int = 0
    telemetry_points: int = 0


# ── Helper ──────────────────────────────────────────────────────────────


def _uuid(val: str) -> uuid.UUID:
    try:
        return uuid.UUID(val)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=f"Invalid UUID: {val}") from exc


def _to_entity_out(e: Any) -> dict[str, Any]:
    return {
        "id": str(e.id),
        "name": e.name,
        "description": e.description or "",
        "hidden": bool(e.hidden),
        "shared_with_customers": bool(e.shared_with_customers),
        "tenant_id": str(e.tenant_id) if e.tenant_id else None,
        "query": e.query or "",
    }


def _to_metric_out(m: Any) -> dict[str, Any]:
    return {
        "id": str(m.id),
        "name": m.name,
        "item_name": m.item_name,
        "business_entity_id": str(m.business_entity_id) if m.business_entity_id else None,
        "item_id": str(m.item_id) if m.item_id else None,
        "tenant_id": str(m.tenant_id) if m.tenant_id else None,
        "customer_id": str(m.customer_id) if m.customer_id else None,
        "user_input": m.user_input,
        "description": m.description,
        "how_to_calculate": m.how_to_calculate,
        "is_advanced_mode": bool(m.is_advanced_mode),
        "calculation_field_id": str(m.calculation_field_id) if m.calculation_field_id else None,
        "is_outdated": bool(m.is_outdated),
        "auto_deletable": bool(m.auto_deletable),
        "created_ts": m.created_ts,
        "updated_ts": m.updated_ts,
    }


# ── Existing endpoints ──────────────────────────────────────────────────


@app.get("/health", tags=["system"], response_model=HealthResponse)
async def health() -> JSONResponse:
    status = "ok"
    detail = None
    try:
        from trendx.services.ingestion import _default_monitor_mounts, check_disk_min_free

        for mp in _default_monitor_mounts():
            if os.path.isdir(mp):
                check_disk_min_free(mp)
    except Exception as exc:
        status = "degraded"
        detail = str(exc)
        logger.warning("Health dégradé : {}", exc)
    payload = HealthResponse(status=status, service="trendx-api", detail=detail)
    logger.debug("Health check : status={}", status)
    code = 200 if status == "ok" else 503
    return JSONResponse(content=payload.model_dump(exclude_none=True), status_code=code)


@app.get("/metrics", tags=["system"])
async def metrics() -> JSONResponse:
    py_mem = sys.getsizeof(object()) * 1024
    return JSONResponse(
        content={
            "service": "trendx-api",
            "version": APP_VERSION,
            "uptime_seconds": 0,
            "memory_bytes_approx": py_mem,
        },
        status_code=200,
    )


@app.get("/", tags=["system"])
async def root() -> JSONResponse:
    return JSONResponse(
        content={
            "name": "Trendx Analytics Platform",
            "version": APP_VERSION,
            "docs": "/docs",
            "health": "/health",
            "mvp": {
                "device": settings.tb_device_id,
                "metric": settings.tb_metric_name,
                "secondary_metric": settings.tb_secondary_metric_name,
            },
            "stack_services": {
                "ui": f"http://{settings.trendx_host}:8080",
                "api": f"http://{settings.trendx_host}:{settings.trendx_api_port}",
                "airflow": f"http://{settings.trendx_host}:8082",
                "mlflow": f"http://{settings.trendx_host}:5000",
                "grafana": f"http://{settings.trendx_host}:{settings.grafana_host_port}",
            },
            "constraints": {
                "tb_api_allowed": False,
                "tb_writeback_enabled": settings.tb_writeback_enabled,
                "tb_alarms_enabled": settings.tb_alarms_enabled,
                "tb_db_port": f"{settings.tb_db_host}:{settings.tb_db_port}",
                "comment": "Aucun appel REST API ThingsBoard 8081/8090 pendant Phase 2. Accès lecture TB DB 32768 désactivé tant que compte RO absent.",
            },
        },
        status_code=200,
    )


# ─────────────────────────────────────────────────────────────────────────
# Catalog Endpoints
# ─────────────────────────────────────────────────────────────────────────


@app.get("/api/v1/catalog/devices", tags=["catalog"], response_model=PaginatedResponse)
async def list_devices(
    page: int = Query(0, ge=0),
    page_size: int = Query(50, ge=1, le=200),
    search: str | None = Query(None),
    hidden: bool | None = Query(None),
) -> JSONResponse:
    try:
        with db_manager.get_session("catalog") as session:
            repo = BusinessEntityRepository(session)
            filters: list[Any] = []
            if hidden is not None:
                filters.append(BusinessEntity.hidden == hidden)
            if search is not None:
                filters.append(BusinessEntity.name.ilike(f"%{search}%"))
            total = repo.count(*filters)
            entities = repo.list(
                *filters, order_by=BusinessEntity.name, limit=page_size, offset=page * page_size
            )
            data = [_to_entity_out(e) for e in entities]
            return JSONResponse(
                content=PaginatedResponse(
                    data=data,
                    total=total,
                    page=page,
                    page_size=page_size,
                    has_next=(page + 1) * page_size < total,
                ).model_dump(),
                status_code=200,
            )
    except Exception as exc:
        logger.error("Failed to list devices: {}", exc)
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/api/v1/catalog/devices/{entity_id}", tags=["catalog"], response_model=DeviceOut)
async def get_device(entity_id: str) -> JSONResponse:
    try:
        with db_manager.get_session("catalog") as session:
            repo = BusinessEntityRepository(session)
            entity = repo.get(entity_id)
            if entity is None:
                raise HTTPException(status_code=404, detail="Device not found")
            return JSONResponse(content=_to_entity_out(entity), status_code=200)
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Failed to get device {}: {}", entity_id, exc)
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/api/v1/catalog/metrics", tags=["catalog"], response_model=PaginatedResponse)
async def list_metrics(
    page: int = Query(0, ge=0),
    page_size: int = Query(50, ge=1, le=200),
    business_entity_id: str | None = Query(None),
) -> JSONResponse:
    try:
        with db_manager.get_session("catalog") as session:
            repo = MetricDefinitionRepository(session)
            filters: list[Any] = []
            if business_entity_id is not None:
                filters.append(MetricDefinition.business_entity_id == _uuid(business_entity_id))
            total = repo.count(*filters)
            metrics = repo.list(
                *filters, order_by=MetricDefinition.name, limit=page_size, offset=page * page_size
            )
            data = [_to_metric_out(m) for m in metrics]
            return JSONResponse(
                content=PaginatedResponse(
                    data=data,
                    total=total,
                    page=page,
                    page_size=page_size,
                    has_next=(page + 1) * page_size < total,
                ).model_dump(),
                status_code=200,
            )
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Failed to list metrics: {}", exc)
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/api/v1/catalog/metrics/{metric_id}", tags=["catalog"], response_model=MetricOut)
async def get_metric(metric_id: str) -> JSONResponse:
    try:
        with db_manager.get_session("catalog") as session:
            repo = MetricDefinitionRepository(session)
            metric = repo.get(metric_id)
            if metric is None:
                raise HTTPException(status_code=404, detail="Metric not found")
            return JSONResponse(content=_to_metric_out(metric), status_code=200)
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Failed to get metric {}: {}", metric_id, exc)
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/api/v1/catalog/profiles", tags=["catalog"])
async def list_profiles() -> JSONResponse:
    return JSONResponse(content=[], status_code=200)


@app.get("/api/v1/catalog/relations", tags=["catalog"], response_model=PaginatedResponse)
async def list_relations(
    page: int = Query(0, ge=0),
    page_size: int = Query(50, ge=1, le=200),
) -> JSONResponse:
    try:
        with db_manager.get_session("catalog") as session:
            repo = EntityRelationRepository(session)
            total = repo.count()
            relations = repo.list(
                order_by=EntityRelation.name, limit=page_size, offset=page * page_size
            )
            data = [
                {
                    "business_entity_id": str(r.business_entity_id),
                    "name": r.name,
                    "related_entity_id": str(r.related_entity_id),
                    "direction": r.direction,
                    "enabled": r.enabled,
                    "query": r.query or "",
                }
                for r in relations
            ]
            return JSONResponse(
                content=PaginatedResponse(
                    data=data,
                    total=total,
                    page=page,
                    page_size=page_size,
                    has_next=(page + 1) * page_size < total,
                ).model_dump(),
                status_code=200,
            )
    except Exception as exc:
        logger.error("Failed to list relations: {}", exc)
        raise HTTPException(status_code=500, detail=str(exc)) from exc


# ─────────────────────────────────────────────────────────────────────────
# Topology Endpoints
# ─────────────────────────────────────────────────────────────────────────


@app.post("/api/v1/topology/discover", tags=["topology"], response_model=DiscoveryTriggerOut)
async def trigger_discovery() -> JSONResponse:
    try:
        svc = TaskService()
        task = svc.create_task(
            name="topology_full_discovery",
            job_type="topology_discovery",
            json_job={"action": "full_discovery"},
            reference_type="MANUAL",
        )
        logger.info("Topology discovery triggered, task={}", task.id)
        return JSONResponse(
            content=DiscoveryTriggerOut(
                status="triggered",
                message="Full topology discovery scheduled",
                task_id=str(task.id),
            ).model_dump(),
            status_code=202,
        )
    except Exception as exc:
        logger.error("Failed to trigger discovery: {}", exc)
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.post("/api/v1/topology/sync", tags=["topology"], response_model=DiscoveryTriggerOut)
async def trigger_sync() -> JSONResponse:
    try:
        svc = TaskService()
        task = svc.create_task(
            name="topology_incremental_sync",
            job_type="topology_sync",
            json_job={"action": "incremental_sync"},
            reference_type="MANUAL",
        )
        logger.info("Topology sync triggered, task={}", task.id)
        return JSONResponse(
            content=DiscoveryTriggerOut(
                status="triggered",
                message="Incremental topology sync scheduled",
                task_id=str(task.id),
            ).model_dump(),
            status_code=202,
        )
    except Exception as exc:
        logger.error("Failed to trigger sync: {}", exc)
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/api/v1/topology/status", tags=["topology"], response_model=SyncStatusOut)
async def get_topology_status() -> JSONResponse:
    try:
        with db_manager.get_session("catalog") as session:
            entity_count = BusinessEntityRepository(session).count()
            relation_count = EntityRelationRepository(session).count()
        return JSONResponse(
            content=SyncStatusOut(
                entity_count=entity_count,
                relation_count=relation_count,
                status="ready" if entity_count > 0 else "empty",
            ).model_dump(),
            status_code=200,
        )
    except Exception as exc:
        logger.error("Failed to get topology status: {}", exc)
        raise HTTPException(status_code=500, detail=str(exc)) from exc


# ─────────────────────────────────────────────────────────────────────────
# Telemetry Endpoints
# ─────────────────────────────────────────────────────────────────────────


@app.get("/api/v1/telemetry/{entity_id}/{metric_key}", tags=["telemetry"])
async def get_telemetry(
    entity_id: str,
    metric_key: str,
    start_ts: str | None = Query(None),
    end_ts: str | None = Query(None),
    limit: int = Query(1000, ge=1, le=100000),
) -> JSONResponse:
    try:
        with db_manager.get_session("analytics") as session:
            conditions = ["entity_id = :eid", "metric_key = :key", "dbl_v IS NOT NULL"]
            params: dict[str, Any] = {"eid": entity_id, "key": metric_key, "limit": limit}
            if start_ts is not None:
                conditions.append("ts >= :start")
                params["start"] = start_ts
            if end_ts is not None:
                conditions.append("ts < :end")
                params["end"] = end_ts
            where = " AND ".join(conditions)
            stmt = text(f"SELECT ts, dbl_v FROM ts_kv WHERE {where} ORDER BY ts LIMIT :limit")  # nosec B608 -- {where} assemblé depuis fragments SQL fixes internes ; toutes les valeurs utilisateur passent en bind params (:eid, :key, :start, :end, :limit).
            rows = session.execute(stmt, params).fetchall()
            data = [{"ts": str(row[0]), "value": float(row[1])} for row in rows]
            return JSONResponse(content=data, status_code=200)
    except Exception as exc:
        logger.error("Failed to get telemetry for {}/{}: {}", entity_id[:12], metric_key, exc)
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get(
    "/api/v1/telemetry/{entity_id}/{metric_key}/stats",
    tags=["telemetry"],
    response_model=TelemetryStatsOut,
)
async def get_telemetry_stats(
    entity_id: str,
    metric_key: str,
    start_ts: str | None = Query(None),
    end_ts: str | None = Query(None),
) -> JSONResponse:
    try:
        with db_manager.get_session("analytics") as session:
            conditions = ["entity_id = :eid", "metric_key = :key"]
            params: dict[str, Any] = {"eid": entity_id, "key": metric_key}
            if start_ts is not None:
                conditions.append("ts >= :start")
                params["start"] = start_ts
            if end_ts is not None:
                conditions.append("ts < :end")
                params["end"] = end_ts
            where = " AND ".join(conditions)
            stats_query = (
                "SELECT MIN(dbl_v), MAX(dbl_v), AVG(dbl_v), STDDEV(dbl_v),"
                " COUNT(*), SUM(CASE WHEN dbl_v IS NULL THEN 1 ELSE 0 END)"
                f" FROM ts_kv WHERE {where}"  # nosec B608 -- {where} = fragments SQL fixes internes uniquement ; toutes les valeurs utilisateur passent en bind params.
            )
            stmt = text(stats_query)
            row = session.execute(stmt, params).fetchone()
            if row is None:
                return JSONResponse(content=TelemetryStatsOut().model_dump(), status_code=200)
            return JSONResponse(
                content=TelemetryStatsOut(
                    min=float(row[0]) if row[0] is not None else None,
                    max=float(row[1]) if row[1] is not None else None,
                    avg=float(row[2]) if row[2] is not None else None,
                    std=float(row[3]) if row[3] is not None else None,
                    count=row[4] or 0,
                    null_count=row[5] or 0,
                ).model_dump(),
                status_code=200,
            )
    except Exception as exc:
        logger.error("Failed to get telemetry stats for {}/{}: {}", entity_id[:12], metric_key, exc)
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.post("/api/v1/ingestion/trigger", tags=["telemetry"], response_model=DiscoveryTriggerOut)
async def trigger_ingestion(request: Request) -> JSONResponse:
    # Verrou d'ingestion : refus systématique tant que TRENDX_INGEST_ENABLED n'est
    # pas explicitement true, et tant que l'authentication ThingsBoard n'est pas
    # déclarée configurée (TB_AUTH_CONFIGURED=true). Toute tentative est journalisée
    # avec son déclencheur et son horodatage (loguru).
    trigger = request.client.host if request.client else "inconnu"
    if not settings.trendx_ingest_enabled:
        logger.warning(
            "Ingestion REFUSÉE (verrou TRENDX_INGEST_ENABLED != true), déclencheur={}",
            trigger,
        )
        raise HTTPException(
            status_code=409,
            detail="Ingestion verrouillée : TRENDX_INGEST_ENABLED doit être true",
        )
    if not settings.tb_auth_configured:
        logger.warning(
            "Ingestion REFUSÉE (TB_AUTH_CONFIGURED != true), déclencheur={}",
            trigger,
        )
        raise HTTPException(
            status_code=409,
            detail="Ingestion verrouillée : TB_AUTH_CONFIGURED doit être true",
        )
    try:
        svc = TaskService()
        task = svc.create_task(
            name="ingestion_incremental",
            job_type="ingestion",
            json_job={"action": "incremental_ingest"},
            reference_type="MANUAL",
        )
        logger.info(
            "Ingestion déclenchée par {} : task={}",
            trigger,
            task.id,
        )
        return JSONResponse(
            content=DiscoveryTriggerOut(
                status="triggered", message="Ingestion scheduled", task_id=str(task.id)
            ).model_dump(),
            status_code=202,
        )
    except Exception as exc:
        logger.error("Failed to trigger ingestion: {}", exc)
        raise HTTPException(status_code=500, detail=str(exc)) from exc


# ─────────────────────────────────────────────────────────────────────────
# Forecast Endpoints
# ─────────────────────────────────────────────────────────────────────────


@app.post("/api/v1/forecast/train", tags=["forecast"], response_model=TrainResponse)
async def train_model(req: TrainRequest) -> JSONResponse:
    try:
        from trendx.services.training import TrainingService

        svc = TrainingService()
        model = svc.train_model(
            entity_id=req.entity_id,
            metric_key=req.metric_key,
            algorithm=req.algorithm,
            params=req.params,
            lookback_days=req.lookback_days,
            frequency=req.frequency,
            horizon=req.horizon,
        )
        if model is None:
            raise HTTPException(
                status_code=400, detail="Training failed — insufficient data or error"
            )
        return JSONResponse(
            content=TrainResponse(
                model_id=str(model.id),
                entity_id=req.entity_id,
                metric_key=req.metric_key,
                algorithm=req.algorithm,
                status="success",
            ).model_dump(),
            status_code=201,
        )
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Training failed: {}", exc)
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.post("/api/v1/forecast/predict", tags=["forecast"])
async def generate_forecast(req: PredictRequest) -> JSONResponse:
    try:
        from trendx.services.inference import InferenceService

        svc = InferenceService()
        forecast = svc.generate_forecast(
            entity_id=req.entity_id,
            metric_key=req.metric_key,
            horizon=req.horizon,
        )
        if forecast is None:
            raise HTTPException(
                status_code=400,
                detail="Forecast generation failed — no champion model or insufficient data",
            )
        result = ForecastResultOut(
            timestamps=[str(ts) for ts in (forecast.timestamps or [])],
            values=[float(v) for v in forecast.values],
            lower_bound=[float(v) for v in (forecast.lower_bound or [])],
            upper_bound=[float(v) for v in (forecast.upper_bound or [])],
            model_name=forecast.model_name or "unknown",
            horizon=len(forecast.values),
        )
        return JSONResponse(content=result.model_dump(), status_code=200)
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Forecast failed: {}", exc)
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/api/v1/forecast/results/{entity_id}/{metric_key}", tags=["forecast"])
async def get_forecast_results(
    entity_id: str,
    metric_key: str,
    limit: int = Query(100, ge=1, le=1000),
) -> JSONResponse:
    try:
        eid = _uuid(entity_id)
    except HTTPException:
        raise
    try:
        with db_manager.get_session("analytics") as session:
            latest = session.execute(
                select(Prediction.forecast_generated_at)
                .where(Prediction.entity_id == eid, Prediction.metric_key == metric_key)
                .order_by(Prediction.forecast_generated_at.desc())
                .limit(1)
            ).scalar_one_or_none()
            if latest is None:
                return JSONResponse(
                    content={
                        "entity_id": entity_id,
                        "metric_key": metric_key,
                        "forecast_generated_at": None,
                        "points": [],
                    },
                    status_code=200,
                )
            rows = (
                session.execute(
                    select(Prediction)
                    .where(
                        Prediction.entity_id == eid,
                        Prediction.metric_key == metric_key,
                        Prediction.forecast_generated_at == latest,
                    )
                    .order_by(Prediction.horizon_step.asc())
                    .limit(limit)
                )
                .scalars()
                .all()
            )
            points = [
                {
                    "ts": p.ts.isoformat() if p.ts else None,
                    "value": p.value,
                    "lower_bound": p.lower_bound,
                    "upper_bound": p.upper_bound,
                    "horizon_step": p.horizon_step,
                    "model_used": p.model_used,
                }
                for p in rows
            ]
        return JSONResponse(
            content={
                "entity_id": entity_id,
                "metric_key": metric_key,
                "forecast_generated_at": latest.isoformat() if latest else None,
                "points": points,
            },
            status_code=200,
        )
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Failed to fetch forecast results: {}", exc)
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/api/v1/forecast/models/{entity_id}/{metric_key}", tags=["forecast"])
async def list_forecast_models(
    entity_id: str,
    metric_key: str,
) -> JSONResponse:
    try:
        with db_manager.get_session("catalog") as session:
            repo = PredictionModelRepository(session)
            eid = _uuid(entity_id)
            models = repo.find_by_business_entity(eid)
            data = [
                ForecastModelOut(
                    id=str(m.id),
                    name=m.name,
                    type=m.type,
                    status=m.status,
                    tb_telemetry_key=m.tb_telemetry_key,
                    business_entity_id=str(m.business_entity_id) if m.business_entity_id else "",
                    enabled=m.enabled,
                    created_ts=m.created_ts,
                    updated_ts=m.updated_ts,
                ).model_dump()
                for m in models
            ]
            return JSONResponse(content=data, status_code=200)
    except HTTPException:
        raise
    except Exception as exc:
        logger.error(
            "Failed to list forecast models for {}/{}: {}", entity_id[:12], metric_key, exc
        )
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.post("/api/v1/forecast/competition", tags=["forecast"])
async def run_competition(req: CompetitionRequest) -> JSONResponse:
    try:
        from trendx.services.training import TrainingService

        svc = TrainingService()
        if req.candidates:
            candidates = [(c["algorithm"], c.get("params")) for c in req.candidates]
        else:
            candidates = None
        result = svc.run_competition(
            entity_id=req.entity_id,
            metric_key=req.metric_key,
            candidates=candidates,
        )
        if result is None:
            raise HTTPException(status_code=400, detail="Competition failed")
        return JSONResponse(
            content={
                "status": "completed",
                "champion_algorithm": result.champion_algorithm
                if hasattr(result, "champion_algorithm")
                else None,
                "champion_score": result.aggregated_metrics.smape
                if hasattr(result, "aggregated_metrics")
                else None,
            },
            status_code=200,
        )
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Competition failed: {}", exc)
        raise HTTPException(status_code=500, detail=str(exc)) from exc


# TODO: is_champion column does not exist in real prediction_model table.
@app.get(
    "/api/v1/forecast/champion/{entity_id}/{metric_key}",
    tags=["forecast"],
    response_model=ChampionOut,
)
async def get_champion(
    entity_id: str,
    metric_key: str,
) -> JSONResponse:
    try:
        from trendx.mlops.registry import ModelRegistry

        registry = ModelRegistry()
        champion = registry.get_champion(entity_id, metric_key)
        if champion is None:
            raise HTTPException(status_code=404, detail="No champion model found")
        return JSONResponse(
            content=ChampionOut(
                id=str(champion.id),
                name=champion.name,
                type=champion.type,
                status=champion.status,
                business_entity_id=str(champion.business_entity_id)
                if champion.business_entity_id
                else "",
                tb_telemetry_key=champion.tb_telemetry_key,
                enabled=champion.enabled,
                created_ts=champion.created_ts,
            ).model_dump(),
            status_code=200,
        )
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Failed to get champion for {}/{}: {}", entity_id[:12], metric_key, exc)
        raise HTTPException(status_code=500, detail=str(exc)) from exc


# ─────────────────────────────────────────────────────────────────────────
# Anomaly Endpoints
# ─────────────────────────────────────────────────────────────────────────


@app.post("/api/v1/anomaly/train", tags=["anomaly"], response_model=TrainResponse)
async def train_anomaly_detector(req: AnomalyTrainRequest) -> JSONResponse:
    try:
        from trendx.anomalies.detectors import AnomalyDetectorService

        svc = AnomalyDetectorService()
        detector = svc.train(
            entity_id=req.entity_id,
            metric_key=req.metric_key,
            algorithm=req.algorithm,
            contamination=req.contamination,
            window_size=req.window_size,
        )
        if detector is None:
            raise HTTPException(status_code=400, detail="Anomaly detector training failed")
        return JSONResponse(
            content=TrainResponse(
                model_id=str(detector.id),
                entity_id=req.entity_id,
                metric_key=req.metric_key,
                algorithm=req.algorithm,
                status="success",
            ).model_dump(),
            status_code=201,
        )
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Anomaly detector training failed: {}", exc)
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.post("/api/v1/anomaly/scan", tags=["anomaly"])
async def scan_anomalies(req: AnomalyScanRequest) -> JSONResponse:
    try:
        from trendx.anomalies.detectors import AnomalyDetectorService

        svc = AnomalyDetectorService()
        results = svc.scan(
            entity_id=req.entity_id,
            metric_key=req.metric_key,
            detector_id=req.detector_id,
        )
        return JSONResponse(
            content={"status": "completed", "anomalies_found": len(results) if results else 0},
            status_code=200,
        )
    except Exception as exc:
        logger.error("Anomaly scan failed: {}", exc)
        raise HTTPException(status_code=500, detail=str(exc)) from exc


# TODO: scored_point_anomaly has columns (t, s, anomaly_id) only.
# entity_id and metric_key are not present in the real table.
@app.get("/api/v1/anomaly/scores/{entity_id}/{metric_key}", tags=["anomaly"])
async def get_anomaly_scores(
    entity_id: str,
    metric_key: str,
    limit: int = Query(500, ge=1, le=10000),
) -> JSONResponse:
    try:
        with db_manager.get_session("catalog") as session:
            stmt = text("""
                SELECT t, s, anomaly_id
                FROM scored_point_anomaly
                ORDER BY t DESC
                LIMIT :lim
            """)
            rows = session.execute(stmt, {"lim": limit}).fetchall()
            data = [
                AnomalyScoreOut(
                    t=row[0],
                    s=float(row[1]) if row[1] is not None else 0.0,
                    anomaly_id=str(row[2]) if row[2] is not None else None,
                ).model_dump()
                for row in rows
            ]
            return JSONResponse(content=data, status_code=200)
    except Exception as exc:
        logger.error("Failed to get anomaly scores: {}", exc)
        raise HTTPException(status_code=500, detail=str(exc)) from exc


# TODO: Real anomaly table has item_id/item_name, not entity_id/metric_key.
# The old columns (anomaly_type, severity, duration_seconds, is_resolved) do not exist.
@app.get("/api/v1/anomaly/episodes/{entity_id}/{metric_key}", tags=["anomaly"])
async def get_anomaly_episodes(
    entity_id: str,
    metric_key: str,
    limit: int = Query(50, ge=1, le=500),
) -> JSONResponse:
    try:
        with db_manager.get_session("catalog") as session:
            stmt = text("""
                SELECT id, item_id, item_name, start_ts, end_ts, cluster_id, score, score_index, model_id, alarm_id
                FROM anomaly
                ORDER BY start_ts DESC
                LIMIT :lim
            """)
            rows = session.execute(stmt, {"lim": limit}).fetchall()
            data = [
                AnomalyEpisodeOut(
                    id=str(row[0]),
                    item_id=str(row[1]) if row[1] is not None else None,
                    item_name=row[2],
                    start_ts=row[3],
                    end_ts=row[4],
                    cluster_id=row[5],
                    score=float(row[6]) if row[6] is not None else 0.0,
                    score_index=row[7],
                    model_id=str(row[8]) if row[8] is not None else None,
                    alarm_id=str(row[9]) if row[9] is not None else None,
                ).model_dump()
                for row in rows
            ]
            return JSONResponse(content=data, status_code=200)
    except Exception as exc:
        logger.error("Failed to get anomaly episodes: {}", exc)
        raise HTTPException(status_code=500, detail=str(exc)) from exc


# ─────────────────────────────────────────────────────────────────────────
# Task Endpoints
# ─────────────────────────────────────────────────────────────────────────


@app.post("/api/v1/tasks", tags=["tasks"], response_model=TaskOut)
async def create_task(req: TaskCreateRequest) -> JSONResponse:
    try:
        svc = TaskService()
        task = svc.create_task(
            name=req.name,
            job_type=req.job_type,
            json_job=req.json_job,
            schedule_type=req.schedule_type,
            reference_type=req.reference_type,
            reference_key=req.reference_key,
        )
        return JSONResponse(
            content=TaskOut(
                id=str(task.id),
                name=task.name,
                job_type=task.job_type,
                enabled=task.enabled,
                reference_type=task.reference_type,
                reference_key=task.reference_key,
                schedule_type=task.schedule_type,
                schedule_planned_ts=task.schedule_planned_ts,
                tenant_id=str(task.tenant_id) if task.tenant_id else None,
                customer_id=str(task.customer_id) if task.customer_id else None,
                user_id=str(task.user_id) if task.user_id else None,
                created_ts=task.created_ts,
                updated_ts=task.updated_ts,
            ).model_dump(),
            status_code=201,
        )
    except Exception as exc:
        logger.error("Failed to create task: {}", exc)
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/api/v1/tasks", tags=["tasks"])
async def list_tasks(
    status: str | None = Query(None),
    job_type: str | None = Query(None),
    limit: int = Query(50, ge=1, le=200),
) -> JSONResponse:
    try:
        with db_manager.get_session("catalog") as session:
            repo = TrendzTaskRepository(session)
            filters: list[Any] = []
            if job_type is not None:
                filters.append(TrendzTask.job_type == job_type)
            tasks = repo.list(*filters, order_by=TrendzTask.created_ts.desc(), limit=limit)
            result = [
                {
                    "id": str(t.id),
                    "name": t.name,
                    "job_type": t.job_type,
                    "enabled": t.enabled,
                    "reference_type": t.reference_type,
                    "reference_key": t.reference_key,
                    "schedule_type": t.schedule_type,
                    "schedule_planned_ts": t.schedule_planned_ts,
                    "tenant_id": str(t.tenant_id) if t.tenant_id else None,
                    "customer_id": str(t.customer_id) if t.customer_id else None,
                    "user_id": str(t.user_id) if t.user_id else None,
                    "created_ts": t.created_ts,
                    "updated_ts": t.updated_ts,
                }
                for t in tasks
            ]
            return JSONResponse(content=result, status_code=200)
    except Exception as exc:
        logger.error("Failed to list tasks: {}", exc)
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/api/v1/tasks/{task_id}", tags=["tasks"], response_model=TaskOut)
async def get_task(task_id: str) -> JSONResponse:
    try:
        with db_manager.get_session("catalog") as session:
            repo = TrendzTaskRepository(session)
            task = repo.get(_uuid(task_id))
            if task is None:
                raise HTTPException(status_code=404, detail="Task not found")
            return JSONResponse(
                content=TaskOut(
                    id=str(task.id),
                    name=task.name,
                    job_type=task.job_type,
                    enabled=task.enabled,
                    reference_type=task.reference_type,
                    reference_key=task.reference_key,
                    schedule_type=task.schedule_type,
                    schedule_planned_ts=task.schedule_planned_ts,
                    tenant_id=str(task.tenant_id) if task.tenant_id else None,
                    customer_id=str(task.customer_id) if task.customer_id else None,
                    user_id=str(task.user_id) if task.user_id else None,
                    created_ts=task.created_ts,
                    updated_ts=task.updated_ts,
                ).model_dump(),
                status_code=200,
            )
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Failed to get task {}: {}", task_id, exc)
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.post("/api/v1/tasks/{task_id}/cancel", tags=["tasks"], response_model=TaskOut)
async def cancel_task(task_id: str) -> JSONResponse:
    try:
        with db_manager.get_session("catalog") as session:
            repo = TrendzTaskRepository(session)
            task = repo.get(_uuid(task_id))
            if task is None:
                raise HTTPException(status_code=404, detail="Task not found")
            task.enabled = False
            session.flush()
            return JSONResponse(
                content=TaskOut(
                    id=str(task.id),
                    name=task.name,
                    job_type=task.job_type,
                    enabled=task.enabled,
                    reference_type=task.reference_type,
                    reference_key=task.reference_key,
                    schedule_type=task.schedule_type,
                    schedule_planned_ts=task.schedule_planned_ts,
                    tenant_id=str(task.tenant_id) if task.tenant_id else None,
                    customer_id=str(task.customer_id) if task.customer_id else None,
                    user_id=str(task.user_id) if task.user_id else None,
                    created_ts=task.created_ts,
                    updated_ts=task.updated_ts,
                ).model_dump(),
                status_code=200,
            )
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Failed to cancel task {}: {}", task_id, exc)
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.post("/api/v1/tasks/{task_id}/retry", tags=["tasks"], response_model=TaskOut)
async def retry_task(task_id: str) -> JSONResponse:
    try:
        with db_manager.get_session("catalog") as session:
            repo = TrendzTaskRepository(session)
            task = repo.get(_uuid(task_id))
            if task is None:
                raise HTTPException(status_code=404, detail="Task not found")
            # Re-queue the task: create a fresh PENDING execution request with a
            # new execution_id so claim_task (NOT EXISTS execution guard) can pick
            # it up again. The previous request and its (FAILED) execution are left
            # untouched, preserving history.
            request = TrendzTaskExecutionRequest(
                task_id=task.id,
                execution_id=uuid.uuid4(),
                tenant_id=task.tenant_id,
                customer_id=task.customer_id,
                user_id=task.user_id,
                scheduled=False,
                job_type=task.job_type,
                json_job=task.json_job,
                created_ts=int(time.time() * 1000),
                state=PENDING_STATE,
            )
            session.add(request)
            task.enabled = True
            session.flush()
            return JSONResponse(
                content=TaskOut(
                    id=str(task.id),
                    name=task.name,
                    job_type=task.job_type,
                    enabled=task.enabled,
                    reference_type=task.reference_type,
                    reference_key=task.reference_key,
                    schedule_type=task.schedule_type,
                    schedule_planned_ts=task.schedule_planned_ts,
                    tenant_id=str(task.tenant_id) if task.tenant_id else None,
                    customer_id=str(task.customer_id) if task.customer_id else None,
                    user_id=str(task.user_id) if task.user_id else None,
                    created_ts=task.created_ts,
                    updated_ts=task.updated_ts,
                ).model_dump(),
                status_code=200,
            )
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Failed to retry task {}: {}", task_id, exc)
        raise HTTPException(status_code=500, detail=str(exc)) from exc


# ─────────────────────────────────────────────────────────────────────────
# Alert Endpoints (TrendX-owned, Étape 013)
# ─────────────────────────────────────────────────────────────────────────


def _alert_rule_out(r: AlertRule) -> dict[str, Any]:
    return {
        "id": str(r.id),
        "name": r.name,
        "entity_id": r.entity_id,
        "metric_key": r.metric_key,
        "rule_type": r.rule_type,
        "condition_json": r.condition_json if r.condition_json is not None else {},
        "cooldown_seconds": r.cooldown_seconds,
        "min_duration_seconds": r.min_duration_seconds,
        "severity": r.severity,
        "is_active": r.is_active,
        "created_at": r.created_at.isoformat() if r.created_at else "",
        "updated_at": r.updated_at.isoformat() if r.updated_at else "",
    }


@app.get("/api/v1/alerts", tags=["alerts"])
async def list_active_alerts(
    entity_id: str | None = Query(None),
    limit: int = Query(50, ge=1, le=200),
) -> JSONResponse:
    try:
        with db_manager.get_session("catalog") as session:
            incidents = AlertIncidentRepository(session).find_active(entity_id)
            data = [
                {
                    "id": inc.id,
                    "logical_key": inc.logical_key,
                    "entity_id": inc.entity_id,
                    "metric_key": inc.metric_key,
                    "severity": inc.severity,
                    "status": inc.status,
                    "opened_at": inc.opened_at.isoformat() if inc.opened_at else None,
                    "acknowledged_at": inc.acknowledged_at.isoformat()
                    if inc.acknowledged_at
                    else None,
                    "cleared_at": inc.cleared_at.isoformat() if inc.cleared_at else None,
                    "closed_at": inc.closed_at.isoformat() if inc.closed_at else None,
                    "last_value": inc.last_value,
                    "open_reason": inc.open_reason,
                    "opened_count": inc.opened_count,
                }
                for inc in incidents[:limit]
            ]
        return JSONResponse(content=data, status_code=200)
    except Exception as exc:
        logger.error("Failed to list active alerts: {}", exc)
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.post("/api/v1/alerts/rules", tags=["alerts"], response_model=AlertRuleOut)
async def create_alert_rule(req: AlertRuleCreate) -> JSONResponse:
    try:
        with db_manager.get_session("catalog") as session:
            repo = AlertRuleRepository(session)
            rule = repo.create(
                name=req.name,
                entity_id=req.entity_id,
                metric_key=req.metric_key,
                rule_type=req.rule_type,
                condition_json=req.condition_json,
                cooldown_seconds=req.cooldown_seconds,
                min_duration_seconds=req.min_duration_seconds,
                severity=req.severity,
                is_active=req.is_active,
            )
            session.commit()
            out = _alert_rule_out(rule)
        return JSONResponse(content=out, status_code=201)
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Failed to create alert rule: {}", exc)
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/api/v1/alerts/rules", tags=["alerts"])
async def list_alert_rules() -> JSONResponse:
    try:
        with db_manager.get_session("catalog") as session:
            rules = AlertRuleRepository(session).list()
            data = [_alert_rule_out(r) for r in rules]
        return JSONResponse(content=data, status_code=200)
    except Exception as exc:
        logger.error("Failed to list alert rules: {}", exc)
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.put("/api/v1/alerts/rules/{rule_id}", tags=["alerts"], response_model=AlertRuleOut)
async def update_alert_rule(rule_id: str, req: AlertRuleUpdate) -> JSONResponse:
    try:
        rid = _uuid(rule_id)
        with db_manager.get_session("catalog") as session:
            repo = AlertRuleRepository(session)
            rule = repo.get(rid)
            if rule is None:
                raise HTTPException(status_code=404, detail="Alert rule not found")
            if req.name is not None:
                rule.name = req.name
            if req.condition_json is not None:
                rule.condition_json = req.condition_json
            if req.cooldown_seconds is not None:
                rule.cooldown_seconds = req.cooldown_seconds
            if req.min_duration_seconds is not None:
                rule.min_duration_seconds = req.min_duration_seconds
            if req.severity is not None:
                rule.severity = req.severity
            if req.is_active is not None:
                rule.is_active = req.is_active
            rule.updated_at = datetime.now(UTC)
            session.commit()
            out = _alert_rule_out(rule)
        return JSONResponse(content=out, status_code=200)
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Failed to update alert rule: {}", exc)
        raise HTTPException(status_code=500, detail=str(exc)) from exc


# ─────────────────────────────────────────────────────────────────────────
# Admin Endpoints
# ─────────────────────────────────────────────────────────────────────────


@app.get("/api/v1/admin/health", tags=["admin"], response_model=HealthDetailedOut)
async def admin_health() -> JSONResponse:
    db_status = db_manager.check_all_connections()
    return JSONResponse(
        content=HealthDetailedOut(
            status="ok" if all(db_status.values()) else "degraded",
            database=db_status,
            timestamp=datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        ).model_dump(),
        status_code=200,
    )


@app.get("/api/v1/admin/config", tags=["admin"], response_model=ConfigOut)
async def admin_config() -> JSONResponse:
    return JSONResponse(
        content=ConfigOut(
            trendx_env=settings.trendx_env,
            trendx_log_level=settings.trendx_log_level,
            trendx_timezone=settings.trendx_timezone,
            trendx_host=settings.trendx_host,
            trendx_api_port=settings.trendx_api_port,
            trendx_workers=settings.trendx_workers,
            tb_base_url=settings.tb_base_url,
            tb_device_id=settings.tb_device_id,
            tb_metric_name=settings.tb_metric_name,
            tb_writeback_enabled=settings.tb_writeback_enabled,
            tb_alarms_enabled=settings.tb_alarms_enabled,
            forecast_frequency=settings.forecast_frequency,
            forecast_horizon=settings.forecast_horizon,
            training_lookback_days=settings.training_lookback_days,
            anomaly_detection_enabled=settings.anomaly_detection_enabled,
            anomaly_contamination=settings.anomaly_contamination,
            anomaly_window_size=settings.anomaly_window_size,
            grafana_host_port=settings.grafana_host_port,
        ).model_dump(),
        status_code=200,
    )


@app.get("/api/v1/admin/stats", tags=["admin"], response_model=StatsOut)
async def admin_stats() -> JSONResponse:
    try:
        stats = StatsOut()
        with db_manager.get_session("catalog") as session:
            stats.devices = BusinessEntityRepository(session).count()
            stats.metrics = MetricDefinitionRepository(session).count()
            stats.models = PredictionModelRepository(session).count()
        with db_manager.get_session("analytics") as session:
            row = session.execute(text("SELECT COUNT(*) FROM ts_kv")).fetchone()
            stats.telemetry_points = row[0] if row else 0
        return JSONResponse(content=stats.model_dump(), status_code=200)
    except Exception as exc:
        logger.error("Failed to get admin stats: {}", exc)
        raise HTTPException(status_code=500, detail=str(exc)) from exc
