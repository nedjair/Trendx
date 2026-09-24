"""W101/W104 — operational read-only API for forecast execution history.

The router delegates exclusively to :class:`ExecutionHistoryService`.  The
application wiring supplies the service through ``app.state``; this module
never reads or writes an ExecutionStore directly.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from enum import Enum
from typing import Annotated, Any, cast

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from loguru import logger
from pydantic import BaseModel, Field
from trendx.config import settings
from trendx.forecasting.analytics import (
    TREND_CONTRACT,
    ExecutionAnalytics,
    analyze_execution_history,
)
from trendx.forecasting.execution import ExecutionRecord, ExecutionStatus, ExecutionStoreError
from trendx.forecasting.history import (
    MAX_HISTORY_PAGE_SIZE,
    ExecutionHistoryService,
    ExecutionQuery,
    ExecutionStatistics,
    FailureDiagnostic,
)

_REDACTED_KEYS = frozenset(
    {
        "access_token",
        "api_key",
        "authorization",
        "credential",
        "credentials",
        "password",
        "refresh_token",
        "secret",
        "token",
    }
)


class TenantContextError(HTTPException):
    """Tenant context is unavailable or does not authorize the request."""


@dataclass(frozen=True)
class TenantContext:
    """Authorized tenant context supplied by the existing API configuration."""

    tenant_id: str


class ExecutionOut(BaseModel):
    model_config = {"protected_namespaces": ()}

    execution_id: str
    reference_key: str
    tenant_id: str
    entity_type: str
    entity_id: str
    target_metric: str
    frequency: str
    horizon: int
    model_id: str
    model_version: str
    algorithm: str
    feature_schema_version: str
    feature_schema_fingerprint: str
    artifact_uri: str
    created_at: str
    started_at: str
    completed_at: str
    status: ExecutionStatus
    error_code: str
    error_reason: str
    prediction_count: int | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)
    result: dict[str, Any] | None = None

    @classmethod
    def from_record(cls, record: ExecutionRecord) -> ExecutionOut:
        return cls(
            execution_id=record.execution_id,
            reference_key=record.reference_key,
            tenant_id=record.tenant_id,
            entity_type=record.entity_type,
            entity_id=record.entity_id,
            target_metric=record.target_metric,
            frequency=record.frequency,
            horizon=record.horizon,
            model_id=record.model_id,
            model_version=record.model_version,
            algorithm=record.algorithm,
            feature_schema_version=record.feature_schema_version,
            feature_schema_fingerprint=record.feature_schema_fingerprint,
            artifact_uri=record.artifact_uri,
            created_at=record.created_at,
            started_at=record.started_at,
            completed_at=record.completed_at,
            status=record.status,
            error_code=record.error_code,
            error_reason=record.error_reason,
            prediction_count=record.prediction_count,
            metadata=_redact_json(record.metadata),
            result=_redact_json(record.result) if record.result is not None else None,
        )


class ExecutionListOut(BaseModel):
    items: list[ExecutionOut]
    total: int
    limit: int
    offset: int
    has_more: bool


class ExecutionStatsOut(BaseModel):
    total: int
    success_count: int
    failed_count: int
    success_rate: float
    total_prediction_count: int
    duration_sample_count: int
    total_duration: float | None
    average_duration: float | None

    @classmethod
    def from_statistics(cls, statistics: ExecutionStatistics) -> ExecutionStatsOut:
        return cls(
            total=statistics.total_executions,
            success_count=statistics.success_count,
            failed_count=statistics.failure_count,
            success_rate=statistics.success_rate,
            total_prediction_count=statistics.prediction_count_total,
            duration_sample_count=statistics.duration_sample_count,
            total_duration=statistics.total_duration_seconds,
            average_duration=statistics.average_duration_seconds,
        )


class AnalyticsGroupOut(BaseModel):
    key: str
    total: int
    success_count: int
    failed_count: int
    success_rate: float
    prediction_count: int


class EntityAnalyticsOut(BaseModel):
    entity_type: str
    entity_id: str
    total: int
    success_count: int
    failed_count: int
    success_rate: float
    prediction_count: int


class ModelAnalyticsOut(BaseModel):
    model_config = {"protected_namespaces": ()}

    model_id: str
    model_version: str
    total: int
    success_count: int
    failed_count: int
    success_rate: float
    prediction_count: int


class FailureAnalyticsOut(BaseModel):
    error_code: str
    count: int
    proportion: float = Field(
        description="Fraction of failed executions in the filtered tenant scope"
    )


class TimeBucketOut(BaseModel):
    bucket: str
    total: int
    success_count: int
    failed_count: int
    prediction_count: int


class TrendContractOut(BaseModel):
    dimension: str
    bucket: str
    timezone: str
    inclusivity: str
    ordering: str


class ExecutionAnalyticsOut(BaseModel):
    tenant_id: str
    summary: ExecutionStatsOut
    by_metric: list[AnalyticsGroupOut]
    by_entity: list[EntityAnalyticsOut]
    by_algorithm: list[AnalyticsGroupOut]
    by_model: list[ModelAnalyticsOut]
    by_status: list[AnalyticsGroupOut]
    failures: list[FailureAnalyticsOut]
    created_at_trend: list[TimeBucketOut]
    trend_contract: TrendContractOut


class FailureDiagnosticOut(BaseModel):
    model_config = {"protected_namespaces": ()}

    execution_id: str
    reference_key: str
    tenant_id: str
    entity_type: str
    entity_id: str
    target_metric: str
    model_id: str
    model_version: str
    algorithm: str
    feature_schema_version: str
    feature_schema_fingerprint: str
    artifact_uri: str
    status: ExecutionStatus
    error_code: str
    error_reason: str
    created_at: str
    started_at: str
    completed_at: str

    @classmethod
    def from_diagnostic(cls, diagnostic: FailureDiagnostic) -> FailureDiagnosticOut:
        return cls(**diagnostic.to_dict())


class ExecutionDiagnosticOut(BaseModel):
    execution_id: str
    status: ExecutionStatus
    failure: FailureDiagnosticOut | None = None


class ExecutionErrorOut(BaseModel):
    detail: str


def _analytics_out(result: ExecutionAnalytics) -> ExecutionAnalyticsOut:
    return ExecutionAnalyticsOut(
        tenant_id=result.tenant_id,
        summary=ExecutionStatsOut.from_statistics(result.summary),
        by_metric=[
            AnalyticsGroupOut(
                key=item.key,
                total=item.total,
                success_count=item.success_count,
                failed_count=item.failed_count,
                success_rate=item.success_rate,
                prediction_count=item.prediction_count,
            )
            for item in result.by_metric
        ],
        by_entity=[
            EntityAnalyticsOut(
                entity_type=item.entity_type,
                entity_id=item.entity_id,
                total=item.total,
                success_count=item.success_count,
                failed_count=item.failed_count,
                success_rate=item.success_rate,
                prediction_count=item.prediction_count,
            )
            for item in result.by_entity
        ],
        by_algorithm=[
            AnalyticsGroupOut(
                key=item.key,
                total=item.total,
                success_count=item.success_count,
                failed_count=item.failed_count,
                success_rate=item.success_rate,
                prediction_count=item.prediction_count,
            )
            for item in result.by_algorithm
        ],
        by_model=[
            ModelAnalyticsOut(
                model_id=item.model_id,
                model_version=item.model_version,
                total=item.total,
                success_count=item.success_count,
                failed_count=item.failed_count,
                success_rate=item.success_rate,
                prediction_count=item.prediction_count,
            )
            for item in result.by_model
        ],
        by_status=[
            AnalyticsGroupOut(
                key=item.key,
                total=item.total,
                success_count=item.success_count,
                failed_count=item.failed_count,
                success_rate=item.success_rate,
                prediction_count=item.prediction_count,
            )
            for item in result.by_status
        ],
        failures=[
            FailureAnalyticsOut(
                error_code=item.error_code,
                count=item.count,
                proportion=item.proportion,
            )
            for item in result.failures
        ],
        created_at_trend=[
            TimeBucketOut(
                bucket=item.bucket,
                total=item.total,
                success_count=item.success_count,
                failed_count=item.failed_count,
                prediction_count=item.prediction_count,
            )
            for item in result.created_at_trend
        ],
        trend_contract=TrendContractOut(**TREND_CONTRACT),
    )


class _ApiErrorCode(str, Enum):
    EXECUTION_NOT_FOUND = "execution_not_found"
    EXECUTION_HISTORY_UNAVAILABLE = "execution_history_unavailable"
    TENANT_FORBIDDEN = "tenant_forbidden"
    TENANT_CONTEXT_UNAVAILABLE = "tenant_context_unavailable"
    INVALID_QUERY = "invalid_query"
    INVALID_PAGINATION = "invalid_pagination"
    DIAGNOSTIC_UNAVAILABLE = "diagnostic_unavailable"


def _observe(operation: str, outcome: str, status_code: int) -> None:
    """Emit a minimal, non-sensitive execution-history observability event."""

    logger.info(
        "execution_history operation={} outcome={} status_code={}",
        operation,
        outcome,
        status_code,
    )


_ERROR_RESPONSES: dict[int | str, dict[str, Any]] = {
    401: {"model": ExecutionErrorOut, "description": "Authentication required"},
    403: {"model": ExecutionErrorOut, "description": "Tenant context is not authorized"},
    404: {"model": ExecutionErrorOut, "description": "Execution was not found"},
    422: {"model": ExecutionErrorOut, "description": "Invalid query or pagination"},
    503: {
        "model": ExecutionErrorOut,
        "description": "Durable execution history is unavailable or corrupted",
    },
}


def _redact_json(value: Any) -> Any:
    """Redact conventional credential fields at the API boundary.

    W100 records are strict JSON.  This small boundary policy prevents the
    API from returning common credential keys while leaving domain payloads
    and forecast values intact; it does not classify arbitrary business data.
    """

    if isinstance(value, dict):
        return {
            key: "[REDACTED]" if str(key).casefold() in _REDACTED_KEYS else _redact_json(item)
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [_redact_json(item) for item in value]
    return value


def get_tenant_context() -> TenantContext:
    """Return the configured tenant, failing closed when no context exists."""

    tenant_id = settings.trendx_default_tenant_id.strip()
    if not tenant_id:
        _observe("tenant", "tenant_rejected", 403)
        raise TenantContextError(
            status_code=403,
            detail=_ApiErrorCode.TENANT_CONTEXT_UNAVAILABLE.value,
        )
    return TenantContext(tenant_id=tenant_id)


TenantContextDep = Annotated[TenantContext, Depends(get_tenant_context)]


def get_execution_history_service(request: Request) -> ExecutionHistoryService:
    """Resolve the read-only service supplied by application wiring."""

    factory = getattr(request.app.state, "execution_history_service_factory", None)
    if not callable(factory):
        _observe("service", "datastore_unavailable", 503)
        raise HTTPException(
            status_code=503,
            detail=_ApiErrorCode.EXECUTION_HISTORY_UNAVAILABLE.value,
        )
    try:
        service = cast(Callable[[], ExecutionHistoryService | None], factory)()
    except (ExecutionStoreError, OSError, UnicodeError) as exc:
        _observe("service", "datastore_unavailable", 503)
        raise HTTPException(
            status_code=503,
            detail=_ApiErrorCode.EXECUTION_HISTORY_UNAVAILABLE.value,
        ) from exc
    if service is None:
        _observe("service", "datastore_unavailable", 503)
        raise HTTPException(
            status_code=503,
            detail=_ApiErrorCode.EXECUTION_HISTORY_UNAVAILABLE.value,
        )
    return service


ExecutionHistoryServiceDep = Annotated[
    ExecutionHistoryService,
    Depends(get_execution_history_service),
]


def build_execution_query(
    tenant_context: TenantContextDep,
    tenant_id: str | None = Query(None),
    entity_type: str | None = Query(None),
    entity_id: str | None = Query(None),
    target_metric: str | None = Query(None),
    execution_id: str | None = Query(None),
    reference_key: str | None = Query(None),
    model_id: str | None = Query(None),
    model_version: str | None = Query(None),
    algorithm: str | None = Query(None),
    feature_schema_version: str | None = Query(None),
    feature_schema_fingerprint: str | None = Query(None),
    status: str | None = Query(None),
    created_at_from: str | None = Query(None),
    created_at_to: str | None = Query(None),
    completed_at_from: str | None = Query(None),
    completed_at_to: str | None = Query(None),
    limit: int = Query(100),
    offset: int = Query(0),
) -> ExecutionQuery:
    if tenant_id is not None and tenant_id != tenant_context.tenant_id:
        _observe("query", "tenant_rejected", 403)
        raise HTTPException(status_code=403, detail=_ApiErrorCode.TENANT_FORBIDDEN.value)
    if limit < 1 or limit > MAX_HISTORY_PAGE_SIZE or offset < 0:
        _observe("query", "invalid_request", 422)
        raise HTTPException(
            status_code=422,
            detail=_ApiErrorCode.INVALID_PAGINATION.value,
        )
    try:
        return ExecutionQuery(
            tenant_id=tenant_context.tenant_id,
            entity_type=entity_type,
            entity_id=entity_id,
            target_metric=target_metric,
            execution_id=execution_id,
            reference_key=reference_key,
            model_id=model_id,
            model_version=model_version,
            algorithm=algorithm,
            feature_schema_version=feature_schema_version,
            feature_schema_fingerprint=feature_schema_fingerprint,
            status=status,
            created_at_from=created_at_from,
            created_at_to=created_at_to,
            completed_at_from=completed_at_from,
            completed_at_to=completed_at_to,
            limit=limit,
            offset=offset,
        )
    except ValueError as exc:
        _observe("query", "invalid_request", 422)
        raise HTTPException(
            status_code=422,
            detail=_ApiErrorCode.INVALID_QUERY.value,
        ) from exc


ExecutionQueryDep = Annotated[ExecutionQuery, Depends(build_execution_query)]


def build_analytics_query(
    tenant_context: TenantContextDep,
    tenant_id: str | None = Query(None),
    entity_type: str | None = Query(None),
    entity_id: str | None = Query(None),
    target_metric: str | None = Query(None),
    execution_id: str | None = Query(None),
    reference_key: str | None = Query(None),
    model_id: str | None = Query(None),
    model_version: str | None = Query(None),
    algorithm: str | None = Query(None),
    feature_schema_version: str | None = Query(None),
    feature_schema_fingerprint: str | None = Query(None),
    status: str | None = Query(None),
    created_at_from: str | None = Query(None),
    created_at_to: str | None = Query(None),
    completed_at_from: str | None = Query(None),
    completed_at_to: str | None = Query(None),
) -> ExecutionQuery:
    if tenant_id is not None and tenant_id != tenant_context.tenant_id:
        _observe("analytics_query", "tenant_rejected", 403)
        raise HTTPException(status_code=403, detail=_ApiErrorCode.TENANT_FORBIDDEN.value)
    try:
        return ExecutionQuery(
            tenant_id=tenant_context.tenant_id,
            entity_type=entity_type,
            entity_id=entity_id,
            target_metric=target_metric,
            execution_id=execution_id,
            reference_key=reference_key,
            model_id=model_id,
            model_version=model_version,
            algorithm=algorithm,
            feature_schema_version=feature_schema_version,
            feature_schema_fingerprint=feature_schema_fingerprint,
            status=status,
            created_at_from=created_at_from,
            created_at_to=created_at_to,
            completed_at_from=completed_at_from,
            completed_at_to=completed_at_to,
        )
    except ValueError as exc:
        _observe("analytics_query", "invalid_request", 422)
        raise HTTPException(
            status_code=422,
            detail=_ApiErrorCode.INVALID_QUERY.value,
        ) from exc


ExecutionAnalyticsQueryDep = Annotated[ExecutionQuery, Depends(build_analytics_query)]


def _service_unavailable(exc: Exception) -> HTTPException:
    return HTTPException(
        status_code=503,
        detail=_ApiErrorCode.EXECUTION_HISTORY_UNAVAILABLE.value,
    )


def _authorized_record(
    record: ExecutionRecord | None,
    tenant_context: TenantContext,
    operation: str,
) -> ExecutionRecord:
    if record is None or record.tenant_id != tenant_context.tenant_id:
        _observe(operation, "not_found", 404)
        raise HTTPException(status_code=404, detail=_ApiErrorCode.EXECUTION_NOT_FOUND.value)
    return record


router = APIRouter(prefix="/api/v1/forecast/executions", tags=["forecast-executions"])


@router.get(
    "",
    response_model=ExecutionListOut,
    summary="List forecast executions",
    responses=_ERROR_RESPONSES,
)
def list_executions(
    query: ExecutionQueryDep,
    service: ExecutionHistoryServiceDep,
) -> ExecutionListOut:
    try:
        page = service.query(query)
    except ExecutionStoreError as exc:
        _observe("list", "datastore_unavailable", 503)
        raise _service_unavailable(exc) from exc
    result = ExecutionListOut(
        items=[ExecutionOut.from_record(record) for record in page.records],
        total=page.total,
        limit=page.limit or MAX_HISTORY_PAGE_SIZE,
        offset=page.offset,
        has_more=page.has_more,
    )
    _observe("list", "success", 200)
    return result


@router.get(
    "/statistics",
    response_model=ExecutionStatsOut,
    summary="Forecast execution statistics",
    responses=_ERROR_RESPONSES,
)
def execution_statistics(
    query: ExecutionQueryDep,
    service: ExecutionHistoryServiceDep,
) -> ExecutionStatsOut:
    try:
        result = ExecutionStatsOut.from_statistics(service.statistics(query))
    except ExecutionStoreError as exc:
        _observe("statistics", "datastore_unavailable", 503)
        raise _service_unavailable(exc) from exc
    _observe("statistics", "success", 200)
    return result


@router.get(
    "/analytics",
    response_model=ExecutionAnalyticsOut,
    summary="Analyze forecast execution history",
    description=(
        "Read-only aggregate projection over the authorized tenant's persisted execution history. "
        "All date filters use W99 UTC normalization and inclusive bounds; trend buckets are exact "
        "created_at timestamps in ascending UTC order. Pagination is intentionally not applied."
    ),
    responses=_ERROR_RESPONSES,
)
def execution_analytics(
    query: ExecutionAnalyticsQueryDep,
    service: ExecutionHistoryServiceDep,
) -> ExecutionAnalyticsOut:
    try:
        result = analyze_execution_history(service, query)
    except (ExecutionStoreError, OSError, UnicodeError) as exc:
        _observe("analytics", "datastore_unavailable", 503)
        raise _service_unavailable(exc) from exc
    except ValueError as exc:
        _observe("analytics", "invalid_request", 422)
        raise HTTPException(
            status_code=422,
            detail=_ApiErrorCode.INVALID_QUERY.value,
        ) from exc
    response = _analytics_out(result)
    _observe("analytics", "success", 200)
    return response


@router.get(
    "/reference/{reference_key}",
    response_model=ExecutionOut,
    summary="Get a forecast execution by reference key",
    responses=_ERROR_RESPONSES,
)
def get_execution_by_reference(
    reference_key: str,
    tenant_context: TenantContextDep,
    service: ExecutionHistoryServiceDep,
) -> ExecutionOut:
    try:
        record = service.get_by_reference_key(reference_key)
    except ExecutionStoreError as exc:
        _observe("get_by_reference", "datastore_unavailable", 503)
        raise _service_unavailable(exc) from exc
    result = ExecutionOut.from_record(
        _authorized_record(record, tenant_context, "get_by_reference")
    )
    _observe("get_by_reference", "success", 200)
    return result


@router.get(
    "/{execution_id}/diagnostic",
    response_model=ExecutionDiagnosticOut,
    summary="Get failure diagnostics for a forecast execution",
    responses=_ERROR_RESPONSES,
)
def get_execution_diagnostic(
    execution_id: str,
    tenant_context: TenantContextDep,
    service: ExecutionHistoryServiceDep,
) -> ExecutionDiagnosticOut:
    try:
        record = _authorized_record(service.get(execution_id), tenant_context, "diagnostic")
        if record.status is not ExecutionStatus.FAILED:
            result = ExecutionDiagnosticOut(
                execution_id=record.execution_id,
                status=record.status,
            )
            _observe("diagnostic", "success", 200)
            return result
        diagnostics = service.failure_diagnostics(
            ExecutionQuery(tenant_id=tenant_context.tenant_id, execution_id=execution_id)
        )
    except ExecutionStoreError as exc:
        _observe("diagnostic", "datastore_unavailable", 503)
        raise _service_unavailable(exc) from exc
    if not diagnostics:
        _observe("diagnostic", "diagnostic_unavailable", 503)
        raise HTTPException(
            status_code=503,
            detail=_ApiErrorCode.DIAGNOSTIC_UNAVAILABLE.value,
        )
    result = ExecutionDiagnosticOut(
        execution_id=record.execution_id,
        status=record.status,
        failure=FailureDiagnosticOut.from_diagnostic(diagnostics[0]),
    )
    _observe("diagnostic", "success", 200)
    return result


@router.get(
    "/{execution_id}",
    response_model=ExecutionOut,
    summary="Get a forecast execution by ID",
    responses=_ERROR_RESPONSES,
)
def get_execution(
    execution_id: str,
    tenant_context: TenantContextDep,
    service: ExecutionHistoryServiceDep,
) -> ExecutionOut:
    try:
        record = service.get(execution_id)
    except ExecutionStoreError as exc:
        _observe("get", "datastore_unavailable", 503)
        raise _service_unavailable(exc) from exc
    result = ExecutionOut.from_record(_authorized_record(record, tenant_context, "get"))
    _observe("get", "success", 200)
    return result


__all__ = [
    "AnalyticsGroupOut",
    "EntityAnalyticsOut",
    "ExecutionAnalyticsOut",
    "ExecutionDiagnosticOut",
    "ExecutionErrorOut",
    "ExecutionHistoryService",
    "ExecutionListOut",
    "ExecutionOut",
    "ExecutionStatsOut",
    "FailureAnalyticsOut",
    "FailureDiagnosticOut",
    "ModelAnalyticsOut",
    "TenantContext",
    "TenantContextError",
    "TimeBucketOut",
    "TrendContractOut",
    "build_analytics_query",
    "build_execution_query",
    "get_execution_history_service",
    "get_tenant_context",
    "router",
]
