"""W101/W104/W109/W110 — forecast execution history and audit APIs.

Read history routes delegate exclusively to :class:`ExecutionHistoryService`.
The W109 recovery route delegates exclusively to W108
:class:`RecoveryService`.  W110 adds a bounded, tenant-aware, read-only audit
projection over a separate audit store.  Application wiring supplies services
through ``app.state``; this module never reads or writes an ExecutionStore or
an audit event file directly.
"""

from __future__ import annotations

import uuid
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Annotated, Any, Literal, cast

from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response
from loguru import logger
from pydantic import BaseModel, Field, field_validator
from trendx.config import settings
from trendx.forecasting.analytics import (
    TREND_CONTRACT,
    ExecutionAnalytics,
    analyze_execution_history,
)
from trendx.forecasting.audit import (
    MAX_AUDIT_OFFSET,
    MAX_AUDIT_QUERY_LIMIT,
    AuditActorType,
    AuditOperation,
    AuditOutcome,
    ExecutionAuditEvent,
    ExecutionAuditPage,
    ExecutionAuditQuery,
    ExecutionAuditService,
    ExecutionAuditStatistics,
    audit_request_context,
)
from trendx.forecasting.execution import ExecutionRecord, ExecutionStatus, ExecutionStoreError
from trendx.forecasting.history import (
    MAX_HISTORY_PAGE_SIZE,
    ExecutionHistoryService,
    ExecutionQuery,
    ExecutionStatistics,
    FailureDiagnostic,
)
from trendx.forecasting.recovery import (
    ExecutionRestoreMetadata,
    ExecutionRestoreOutcome,
    ExecutionRestoreRequest,
    ExecutionRestoreResult,
    ExecutionRestoreStatus,
    RecoveryService,
    RestoreConflictPolicy,
)
from trendx.forecasting.reporting import (
    REPORT_CONTRACT_VERSION,
    ExecutionAnalyticsReport,
    ExecutionAnalyticsReportingService,
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
_RECOVERY_API_PLACEHOLDERS = frozenset({"", "CHANGE_ME", "change-me", "changeme"})


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


class ExecutionReportTenantContextOut(BaseModel):
    tenant_id: str


class ExecutionReportFiltersOut(BaseModel):
    model_config = {"protected_namespaces": ()}

    tenant_id: str
    entity_type: str | None = None
    entity_id: str | None = None
    target_metric: str | None = None
    execution_id: str | None = None
    reference_key: str | None = None
    model_id: str | None = None
    model_version: str | None = None
    algorithm: str | None = None
    feature_schema_version: str | None = None
    feature_schema_fingerprint: str | None = None
    status: str | None = None
    created_at_from: str | None = None
    created_at_to: str | None = None
    completed_at_from: str | None = None
    completed_at_to: str | None = None


class ExecutionReportPaginationOut(BaseModel):
    applied: bool
    reason: str


class ReportDimensionsOut(BaseModel):
    by_metric: list[AnalyticsGroupOut]
    by_entity: list[EntityAnalyticsOut]
    by_algorithm: list[AnalyticsGroupOut]
    by_model: list[ModelAnalyticsOut]
    by_status: list[AnalyticsGroupOut]


class ReportTemporalOut(BaseModel):
    created_at_trend: list[TimeBucketOut]
    trend_contract: TrendContractOut


class ExecutionAnalyticsReportOut(BaseModel):
    contract_version: str
    generated_at: str | None = Field(
        default=None,
        description=(
            "Reserved generation metadata; null in deterministic mode and never used for aggregates"
        ),
    )
    tenant_id: str
    tenant_context: ExecutionReportTenantContextOut
    applied_filters: ExecutionReportFiltersOut
    pagination: ExecutionReportPaginationOut
    summary: ExecutionStatsOut
    dimensions: ReportDimensionsOut
    temporal: ReportTemporalOut
    failures: list[FailureAnalyticsOut]

    @classmethod
    def from_report(cls, report: ExecutionAnalyticsReport) -> ExecutionAnalyticsReportOut:
        return cls(
            contract_version=report.contract_version,
            generated_at=report.generated_at,
            tenant_id=report.tenant_id,
            tenant_context=ExecutionReportTenantContextOut(
                tenant_id=report.tenant_context.tenant_id
            ),
            applied_filters=ExecutionReportFiltersOut(
                tenant_id=report.applied_filters.tenant_id,
                entity_type=report.applied_filters.entity_type,
                entity_id=report.applied_filters.entity_id,
                target_metric=report.applied_filters.target_metric,
                execution_id=report.applied_filters.execution_id,
                reference_key=report.applied_filters.reference_key,
                model_id=report.applied_filters.model_id,
                model_version=report.applied_filters.model_version,
                algorithm=report.applied_filters.algorithm,
                feature_schema_version=report.applied_filters.feature_schema_version,
                feature_schema_fingerprint=report.applied_filters.feature_schema_fingerprint,
                status=report.applied_filters.status,
                created_at_from=report.applied_filters.created_at_from,
                created_at_to=report.applied_filters.created_at_to,
                completed_at_from=report.applied_filters.completed_at_from,
                completed_at_to=report.applied_filters.completed_at_to,
            ),
            pagination=ExecutionReportPaginationOut(
                applied=report.pagination.applied,
                reason=report.pagination.reason,
            ),
            summary=ExecutionStatsOut.from_statistics(report.summary),
            dimensions=ReportDimensionsOut(
                by_metric=[
                    AnalyticsGroupOut(**item.to_dict()) for item in report.dimensions.by_metric
                ],
                by_entity=[
                    EntityAnalyticsOut(**item.to_dict()) for item in report.dimensions.by_entity
                ],
                by_algorithm=[
                    AnalyticsGroupOut(**item.to_dict()) for item in report.dimensions.by_algorithm
                ],
                by_model=[
                    ModelAnalyticsOut(**item.to_dict()) for item in report.dimensions.by_model
                ],
                by_status=[
                    AnalyticsGroupOut(**item.to_dict()) for item in report.dimensions.by_status
                ],
            ),
            temporal=ReportTemporalOut(
                created_at_trend=[
                    TimeBucketOut(**item.to_dict()) for item in report.temporal.created_at_trend
                ],
                trend_contract=TrendContractOut(**report.temporal.trend_contract.to_dict()),
            ),
            failures=[FailureAnalyticsOut(**item.to_dict()) for item in report.failures],
        )


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


class ExecutionAuditEventOut(BaseModel):
    """Sanitized durable event returned by the read-only audit API."""

    event_id: str
    event_version: str
    occurred_at: str
    operation: AuditOperation
    outcome: AuditOutcome
    tenant_id: str
    execution_id: str | None
    reference_key: str | None
    actor_type: AuditActorType
    actor_id: str
    request_id: str
    source: str
    reason_code: str
    idempotency_key: str
    checksum: str

    @classmethod
    def from_event(cls, event: ExecutionAuditEvent) -> ExecutionAuditEventOut:
        return cls(
            event_id=_safe_response_identifier(event.event_id),
            event_version=event.event_version,
            occurred_at=event.occurred_at.isoformat(),
            operation=event.operation,
            outcome=event.outcome,
            tenant_id=_safe_response_identifier(event.tenant_id),
            execution_id=(
                _safe_response_identifier(event.execution_id)
                if event.execution_id is not None
                else None
            ),
            reference_key=(
                _safe_response_identifier(event.reference_key)
                if event.reference_key is not None
                else None
            ),
            actor_type=event.actor_type,
            actor_id=_safe_response_identifier(event.actor_id),
            request_id=_safe_response_identifier(event.request_id),
            source=_safe_response_identifier(event.source),
            reason_code=_safe_response_identifier(event.reason_code),
            idempotency_key=_safe_response_identifier(event.idempotency_key or ""),
            checksum=event.checksum or "",
        )


class ExecutionAuditListOut(BaseModel):
    items: list[ExecutionAuditEventOut]
    total: int
    limit: int
    offset: int
    has_more: bool

    @classmethod
    def from_page(cls, page: ExecutionAuditPage) -> ExecutionAuditListOut:
        return cls(
            items=[ExecutionAuditEventOut.from_event(event) for event in page.events],
            total=page.total,
            limit=page.limit,
            offset=page.offset,
            has_more=page.has_more,
        )


class ExecutionAuditStatsOut(BaseModel):
    total_events: int
    successful_events: int
    failed_events: int
    rejected_events: int
    conflicts: int
    forbidden: int
    by_operation: dict[str, int]
    by_outcome: dict[str, int]
    by_tenant: dict[str, int]

    @classmethod
    def from_statistics(cls, statistics: ExecutionAuditStatistics) -> ExecutionAuditStatsOut:
        return cls(**statistics.to_dict())


def _safe_response_identifier(value: str) -> str:
    """Prevent client-controlled filesystem-looking identifiers from echoing."""

    normalized = value.replace("\\", "/")
    if (
        "\x00" in value
        or "/" in normalized
        or (len(normalized) >= 2 and normalized[1] == ":" and normalized[0].isalpha())
    ):
        return "[REDACTED]"
    return value


class ExecutionRestoreRequestIn(BaseModel):
    """Strict HTTP body mapped to the W108 recovery request."""

    model_config = {"extra": "forbid"}

    execution_id: str = Field(min_length=1, max_length=512)
    tenant_id: str = Field(min_length=1, max_length=256)
    expected_archive_checksum: str = Field(
        min_length=64,
        max_length=64,
        pattern=r"^[0-9a-f]{64}$",
    )
    conflict_policy: Literal["FAIL_IF_EXISTS"]

    @field_validator("execution_id", "tenant_id")
    @classmethod
    def reject_invalid_identifier(cls, value: str) -> str:
        if "\x00" in value:
            msg = "null bytes are not allowed"
            raise ValueError(msg)
        if not value.strip():
            msg = "identifier must not be blank"
            raise ValueError(msg)
        return value

    def to_domain(self) -> ExecutionRestoreRequest:
        return ExecutionRestoreRequest(
            execution_id=self.execution_id,
            tenant_id=self.tenant_id,
            expected_archive_checksum=self.expected_archive_checksum,
            conflict_policy=RestoreConflictPolicy.FAIL_IF_EXISTS,
        )


class ExecutionRestoreMetadataOut(BaseModel):
    archive_format_version: int
    archive_checksum: str
    restore_timestamp: str
    operation: str
    version: str

    @classmethod
    def from_metadata(
        cls,
        metadata: ExecutionRestoreMetadata,
    ) -> ExecutionRestoreMetadataOut:
        return cls(
            archive_format_version=metadata.archive_format_version,
            archive_checksum=metadata.archive_checksum,
            restore_timestamp=metadata.restore_timestamp,
            operation=metadata.operation,
            version=metadata.version,
        )


class ExecutionRestoreResultOut(BaseModel):
    execution_id: str
    tenant_id: str
    status: ExecutionRestoreStatus
    outcome: ExecutionRestoreOutcome
    archive_verified: bool
    record_restored: bool
    already_present: bool
    conflict: bool
    reason: str
    archive_format_version: int | None = None
    archive_checksum: str | None = None
    restore_metadata: ExecutionRestoreMetadataOut | None = None

    @classmethod
    def from_result(cls, result: ExecutionRestoreResult) -> ExecutionRestoreResultOut:
        return cls(
            execution_id=_safe_response_identifier(result.execution_id),
            tenant_id=_safe_response_identifier(result.tenant_id),
            status=result.status,
            outcome=result.outcome,
            archive_verified=result.archive_verified,
            record_restored=result.record_restored,
            already_present=result.already_present,
            conflict=result.conflict,
            reason=result.reason,
            archive_format_version=result.archive_format_version,
            archive_checksum=result.archive_checksum,
            restore_metadata=(
                ExecutionRestoreMetadataOut.from_metadata(result.metadata)
                if result.metadata is not None
                else None
            ),
        )


ExecutionRestoreErrorOut = ExecutionRestoreResultOut | ExecutionErrorOut


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
    RECOVERY_SERVICE_UNAVAILABLE = "recovery_service_unavailable"
    RECOVERY_AUTH_NOT_CONFIGURED = "recovery_authentication_not_configured"
    AUDIT_SERVICE_UNAVAILABLE = "audit_service_unavailable"
    AUDIT_AUTH_NOT_CONFIGURED = "audit_authentication_not_configured"
    INVALID_AUDIT_QUERY = "invalid_audit_query"


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

_RESTORE_ERROR_RESPONSES: dict[int | str, dict[str, Any]] = {
    401: {"model": ExecutionErrorOut, "description": "Authentication required"},
    403: {
        "model": ExecutionRestoreErrorOut,
        "description": "Tenant context is not authorized",
    },
    404: {
        "model": ExecutionRestoreResultOut,
        "description": "Archive was not found",
    },
    409: {
        "model": ExecutionRestoreResultOut,
        "description": "Active execution identity conflict",
    },
    422: {
        "model": ExecutionRestoreErrorOut,
        "description": "Invalid request or archive",
    },
    503: {
        "model": ExecutionRestoreErrorOut,
        "description": "Recovery backend is unavailable",
    },
}

_AUDIT_ERROR_RESPONSES: dict[int | str, dict[str, Any]] = {
    401: {"model": ExecutionErrorOut, "description": "Authentication required"},
    403: {"model": ExecutionErrorOut, "description": "Tenant context is not authorized"},
    422: {"model": ExecutionErrorOut, "description": "Invalid audit query"},
    503: {"model": ExecutionErrorOut, "description": "Audit store is unavailable or corrupted"},
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


def _recovery_api_is_configured() -> bool:
    token = settings.trendx_api_token.get_secret_value().strip()
    return bool(token) and token not in _RECOVERY_API_PLACEHOLDERS


def validate_restore_request(
    request: Request,
    payload: ExecutionRestoreRequestIn,
    tenant_context: TenantContextDep,
) -> None:
    """Validate HTTP-only controls before resolving the recovery backend."""

    audit_service = _optional_execution_audit_service(request)
    request_id = _request_id(request)
    if request.query_params:
        _observe("restore", "invalid_request", 422)
        _record_api_failure_audit(
            audit_service,
            tenant_id=tenant_context.tenant_id,
            execution_id=payload.execution_id,
            outcome=AuditOutcome.REJECTED,
            reason_code="invalid_request",
            request_id=request_id,
        )
        raise HTTPException(status_code=422, detail="invalid_request")
    if not _recovery_api_is_configured():
        _observe("restore", "authentication_not_configured", 503)
        _record_api_failure_audit(
            audit_service,
            tenant_id=tenant_context.tenant_id,
            execution_id=payload.execution_id,
            outcome=AuditOutcome.REJECTED,
            reason_code="authentication_not_configured",
            request_id=request_id,
        )
        raise HTTPException(
            status_code=503,
            detail=_ApiErrorCode.RECOVERY_AUTH_NOT_CONFIGURED.value,
        )
    if payload.tenant_id != tenant_context.tenant_id:
        _observe("restore", "tenant_forbidden", 403)
        _record_api_failure_audit(
            audit_service,
            tenant_id=tenant_context.tenant_id,
            execution_id=payload.execution_id,
            outcome=AuditOutcome.FORBIDDEN,
            reason_code="tenant_forbidden",
            request_id=request_id,
        )
        raise HTTPException(
            status_code=403,
            detail=_ApiErrorCode.TENANT_FORBIDDEN.value,
        )


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


def get_execution_recovery_service(request: Request) -> RecoveryService:
    """Resolve the W108 recovery service from application wiring."""

    if not _recovery_api_is_configured():
        _observe("restore", "authentication_not_configured", 503)
        raise HTTPException(
            status_code=503,
            detail=_ApiErrorCode.RECOVERY_AUTH_NOT_CONFIGURED.value,
        )
    factory = getattr(request.app.state, "execution_recovery_service_factory", None)
    if not callable(factory):
        _observe("restore", "backend_unavailable", 503)
        raise HTTPException(
            status_code=503,
            detail=_ApiErrorCode.RECOVERY_SERVICE_UNAVAILABLE.value,
        )
    try:
        service = cast(Callable[[], RecoveryService | None], factory)()
    except Exception as exc:  # adapter boundary: never expose a backend cause
        _observe("restore", "backend_unavailable", 503)
        raise HTTPException(
            status_code=503,
            detail=_ApiErrorCode.RECOVERY_SERVICE_UNAVAILABLE.value,
        ) from exc
    if service is None or not callable(getattr(service, "restore", None)):
        _observe("restore", "backend_unavailable", 503)
        raise HTTPException(
            status_code=503,
            detail=_ApiErrorCode.RECOVERY_SERVICE_UNAVAILABLE.value,
        )
    return service


RecoveryServiceDep = Annotated[
    RecoveryService,
    Depends(get_execution_recovery_service),
]


def _request_id(request: Request) -> str:
    value = getattr(request.state, "request_id", None)
    if isinstance(value, str) and value:
        return value
    generated = f"request-{uuid.uuid4()}"
    request.state.request_id = generated
    return generated


def _optional_execution_audit_service(request: Request) -> ExecutionAuditService | None:
    factory = getattr(request.app.state, "execution_audit_service_factory", None)
    if not callable(factory):
        return None
    try:
        service = cast(Callable[[], ExecutionAuditService | None], factory)()
    except Exception:
        return None
    if service is None or not callable(getattr(service, "record", None)):
        return None
    return service


def get_execution_audit_service(request: Request) -> ExecutionAuditService:
    """Resolve the W110 service for read-only audit queries."""

    if not _recovery_api_is_configured():
        _observe("audit", "authentication_not_configured", 503)
        raise HTTPException(
            status_code=503,
            detail=_ApiErrorCode.AUDIT_AUTH_NOT_CONFIGURED.value,
        )
    service = _optional_execution_audit_service(request)
    if service is None:
        _observe("audit", "backend_unavailable", 503)
        raise HTTPException(
            status_code=503,
            detail=_ApiErrorCode.AUDIT_SERVICE_UNAVAILABLE.value,
        )
    return service


ExecutionAuditServiceDep = Annotated[
    ExecutionAuditService,
    Depends(get_execution_audit_service),
]


def _record_optional_restore_audit(
    service: ExecutionAuditService | None,
    result: ExecutionRestoreResult,
    *,
    status_code: int,
    request_id: str,
) -> None:
    if service is None:
        return
    try:
        service.record_recovery_api_result(
            result,
            status_code=status_code,
            actor_type=AuditActorType.API,
            actor_id="api-key-context",
            request_id=request_id,
        )
    except Exception:
        # The recovery result is authoritative; audit is best effort.
        return


def _record_api_failure_audit(
    service: ExecutionAuditService | None,
    *,
    tenant_id: str,
    execution_id: str | None,
    outcome: AuditOutcome,
    reason_code: str,
    request_id: str,
) -> None:
    if service is None:
        return
    try:
        service.record(
            operation=AuditOperation.RECOVERY_API,
            outcome=outcome,
            tenant_id=tenant_id,
            execution_id=execution_id,
            actor_type=AuditActorType.API,
            actor_id="api-key-context",
            request_id=request_id,
            source="w109-http",
            reason_code=reason_code,
        )
    except Exception:
        return


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


def build_report_query(
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
        _observe("report", "tenant_rejected", 403)
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
        _observe("report", "invalid_request", 422)
        raise HTTPException(
            status_code=422,
            detail=_ApiErrorCode.INVALID_QUERY.value,
        ) from exc


ExecutionReportQueryDep = Annotated[ExecutionQuery, Depends(build_report_query)]


_AUDIT_QUERY_FIELDS = frozenset(
    {
        "tenant_id",
        "event_id",
        "operation",
        "outcome",
        "execution_id",
        "reference_key",
        "occurred_at_from",
        "occurred_at_to",
        "request_id",
        "actor_type",
        "source",
        "reason_code",
        "limit",
        "offset",
    }
)


def build_execution_audit_query(
    request: Request,
    tenant_context: TenantContextDep,
    tenant_id: str | None = Query(None),
    event_id: str | None = Query(None),
    operation: str | None = Query(None),
    outcome: str | None = Query(None),
    execution_id: str | None = Query(None),
    reference_key: str | None = Query(None),
    occurred_at_from: datetime | None = Query(None),  # noqa: B008
    occurred_at_to: datetime | None = Query(None),  # noqa: B008
    request_id: str | None = Query(None),
    actor_type: str | None = Query(None),
    source: str | None = Query(None),
    reason_code: str | None = Query(None),
    limit: int = Query(100, ge=1, le=MAX_AUDIT_QUERY_LIMIT),
    offset: int = Query(0, ge=0, le=MAX_AUDIT_OFFSET),
) -> ExecutionAuditQuery:
    unknown = set(request.query_params) - _AUDIT_QUERY_FIELDS
    if unknown:
        _observe("audit", "invalid_request", 422)
        raise HTTPException(status_code=422, detail=_ApiErrorCode.INVALID_AUDIT_QUERY.value)
    if tenant_id is not None and tenant_id != tenant_context.tenant_id:
        _observe("audit", "tenant_rejected", 403)
        raise HTTPException(status_code=403, detail=_ApiErrorCode.TENANT_FORBIDDEN.value)
    try:
        return ExecutionAuditQuery(
            tenant_id=tenant_context.tenant_id,
            event_id=event_id,
            operation=cast(Any, operation),
            outcome=cast(Any, outcome),
            execution_id=execution_id,
            reference_key=reference_key,
            occurred_at_from=occurred_at_from,
            occurred_at_to=occurred_at_to,
            request_id=request_id,
            actor_type=cast(Any, actor_type),
            source=source,
            reason_code=reason_code,
            limit=limit,
            offset=offset,
        )
    except (TypeError, ValueError) as exc:
        _observe("audit", "invalid_request", 422)
        raise HTTPException(
            status_code=422,
            detail=_ApiErrorCode.INVALID_AUDIT_QUERY.value,
        ) from exc


ExecutionAuditQueryDep = Annotated[ExecutionAuditQuery, Depends(build_execution_audit_query)]


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


def _restore_status_code(status: ExecutionRestoreStatus) -> int:
    return {
        ExecutionRestoreStatus.RESTORED: 200,
        ExecutionRestoreStatus.ALREADY_PRESENT: 200,
        ExecutionRestoreStatus.VERIFIED: 503,
        ExecutionRestoreStatus.CONFLICT: 409,
        ExecutionRestoreStatus.TENANT_FORBIDDEN: 403,
        ExecutionRestoreStatus.ARCHIVE_NOT_FOUND: 404,
        ExecutionRestoreStatus.INVALID_ARCHIVE: 422,
        ExecutionRestoreStatus.INTEGRITY_FAILURE: 422,
        ExecutionRestoreStatus.RESTORE_FAILED: 503,
    }[status]


def _restore_outcome(status: ExecutionRestoreStatus) -> str:
    return {
        ExecutionRestoreStatus.RESTORED: "success",
        ExecutionRestoreStatus.ALREADY_PRESENT: "already_present",
        ExecutionRestoreStatus.VERIFIED: "backend_unavailable",
        ExecutionRestoreStatus.CONFLICT: "conflict",
        ExecutionRestoreStatus.TENANT_FORBIDDEN: "tenant_forbidden",
        ExecutionRestoreStatus.ARCHIVE_NOT_FOUND: "archive_not_found",
        ExecutionRestoreStatus.INVALID_ARCHIVE: "invalid_archive",
        ExecutionRestoreStatus.INTEGRITY_FAILURE: "invalid_archive",
        ExecutionRestoreStatus.RESTORE_FAILED: "backend_unavailable",
    }[status]


def reject_reserved_restore_read(request: Request) -> None:
    """Keep the literal ``restore`` path unavailable to read methods."""

    if request.method in {"GET", "HEAD"} and request.url.path.endswith("/restore"):
        _observe("get", "method_not_allowed", 405)
        raise HTTPException(status_code=405, detail="method_not_allowed")


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
    "/report",
    response_model=ExecutionAnalyticsReportOut,
    summary="Report forecast execution history analytics",
    description=(
        "Stable W106 representation of the W104 analytics projection. "
        "The report is an aggregate snapshot, so W99 pagination is intentionally not applied. "
        "generated_at is null in deterministic mode and never affects analytics."
    ),
    responses=_ERROR_RESPONSES,
)
def execution_report(
    query: ExecutionReportQueryDep,
    service: ExecutionHistoryServiceDep,
) -> ExecutionAnalyticsReportOut:
    try:
        report = ExecutionAnalyticsReportingService(service).report(query)
    except (ExecutionStoreError, OSError, UnicodeError) as exc:
        _observe("report", "datastore_unavailable", 503)
        raise _service_unavailable(exc) from exc
    except ValueError as exc:
        _observe("report", "invalid_request", 422)
        raise HTTPException(
            status_code=422,
            detail=_ApiErrorCode.INVALID_QUERY.value,
        ) from exc
    response = ExecutionAnalyticsReportOut.from_report(report)
    _observe("report", "success", 200)
    return response


@router.post(
    "/restore",
    response_model=ExecutionRestoreResultOut,
    summary="Restore one verified archived execution",
    description=(
        "Explicit, authenticated W108 recovery operation. The archive is selected by "
        "execution_id through the server-configured ArchiveStore; clients cannot provide "
        "an archive path. Only FAIL_IF_EXISTS is accepted."
    ),
    responses=_RESTORE_ERROR_RESPONSES,
    dependencies=[Depends(validate_restore_request)],
    openapi_extra={
        "security": [
            {"BearerAuth": []},
            {"ApiKeyAuth": []},
        ]
    },
)
def restore_execution(
    request: Request,
    payload: ExecutionRestoreRequestIn,
    response: Response,
    tenant_context: TenantContextDep,
    service: RecoveryServiceDep,
) -> ExecutionRestoreResultOut:
    audit_service = _optional_execution_audit_service(request)
    request_id = _request_id(request)
    if payload.tenant_id != tenant_context.tenant_id:
        _observe("restore", "tenant_forbidden", 403)
        _record_api_failure_audit(
            audit_service,
            tenant_id=tenant_context.tenant_id,
            execution_id=payload.execution_id,
            outcome=AuditOutcome.FORBIDDEN,
            reason_code="tenant_forbidden",
            request_id=request_id,
        )
        raise HTTPException(
            status_code=403,
            detail=_ApiErrorCode.TENANT_FORBIDDEN.value,
        )
    try:
        domain_request = payload.to_domain()
    except (TypeError, ValueError) as exc:
        _observe("restore", "invalid_request", 422)
        _record_api_failure_audit(
            audit_service,
            tenant_id=tenant_context.tenant_id,
            execution_id=payload.execution_id,
            outcome=AuditOutcome.REJECTED,
            reason_code="invalid_request",
            request_id=request_id,
        )
        raise HTTPException(status_code=422, detail="invalid_request") from exc
    try:
        with audit_request_context(request_id):
            result = service.restore(domain_request)
    except Exception as exc:  # adapter boundary: never expose a backend cause
        _observe("restore", "backend_unavailable", 503)
        _record_api_failure_audit(
            audit_service,
            tenant_id=tenant_context.tenant_id,
            execution_id=payload.execution_id,
            outcome=AuditOutcome.FAILED,
            reason_code="backend_unavailable",
            request_id=request_id,
        )
        raise HTTPException(
            status_code=503,
            detail=_ApiErrorCode.RECOVERY_SERVICE_UNAVAILABLE.value,
        ) from exc
    if not isinstance(result, ExecutionRestoreResult):
        _observe("restore", "backend_unavailable", 503)
        _record_api_failure_audit(
            audit_service,
            tenant_id=tenant_context.tenant_id,
            execution_id=payload.execution_id,
            outcome=AuditOutcome.FAILED,
            reason_code="invalid_backend_result",
            request_id=request_id,
        )
        raise HTTPException(
            status_code=503,
            detail=_ApiErrorCode.RECOVERY_SERVICE_UNAVAILABLE.value,
        )
    try:
        status_code = _restore_status_code(result.status)
        response_body = ExecutionRestoreResultOut.from_result(result)
        outcome = _restore_outcome(result.status)
    except (AttributeError, KeyError, TypeError, ValueError) as exc:
        _observe("restore", "backend_unavailable", 503)
        _record_api_failure_audit(
            audit_service,
            tenant_id=tenant_context.tenant_id,
            execution_id=payload.execution_id,
            outcome=AuditOutcome.FAILED,
            reason_code="invalid_backend_result",
            request_id=request_id,
        )
        raise HTTPException(
            status_code=503,
            detail=_ApiErrorCode.RECOVERY_SERVICE_UNAVAILABLE.value,
        ) from exc
    response.status_code = status_code
    _record_optional_restore_audit(
        audit_service,
        result,
        status_code=status_code,
        request_id=request_id,
    )
    _observe("restore", outcome, status_code)
    return response_body


@router.get(
    "/audit/statistics",
    response_model=ExecutionAuditStatsOut,
    summary="Read execution audit statistics",
    description=(
        "Read-only, tenant-scoped operational counters. Audit statistics are independent "
        "from W104 execution-history analytics and never mutate the audit store."
    ),
    responses=_AUDIT_ERROR_RESPONSES,
    openapi_extra={
        "security": [
            {"BearerAuth": []},
            {"ApiKeyAuth": []},
        ]
    },
)
def execution_audit_statistics(
    query: ExecutionAuditQueryDep,
    service: ExecutionAuditServiceDep,
) -> ExecutionAuditStatsOut:
    try:
        result = ExecutionAuditStatsOut.from_statistics(service.statistics(query))
    except Exception as exc:
        _observe("audit_statistics", "backend_unavailable", 503)
        raise HTTPException(
            status_code=503,
            detail=_ApiErrorCode.AUDIT_SERVICE_UNAVAILABLE.value,
        ) from exc
    _observe("audit_statistics", "success", 200)
    return result


@router.get(
    "/audit",
    response_model=ExecutionAuditListOut,
    summary="Query execution operational audit events",
    description=(
        "Read-only and tenant-aware W110 audit query. Results are deterministically ordered, "
        "bounded to at most 1000 events per request, and never expose credentials, payloads, "
        "or filesystem paths."
    ),
    responses=_AUDIT_ERROR_RESPONSES,
    openapi_extra={
        "security": [
            {"BearerAuth": []},
            {"ApiKeyAuth": []},
        ]
    },
)
def query_execution_audit(
    query: ExecutionAuditQueryDep,
    service: ExecutionAuditServiceDep,
) -> ExecutionAuditListOut:
    try:
        result = ExecutionAuditListOut.from_page(service.query(query))
    except Exception as exc:
        _observe("audit", "backend_unavailable", 503)
        raise HTTPException(
            status_code=503,
            detail=_ApiErrorCode.AUDIT_SERVICE_UNAVAILABLE.value,
        ) from exc
    _observe("audit", "success", 200)
    return result


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
    dependencies=[Depends(reject_reserved_restore_read)],
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
    "ExecutionAnalyticsReportOut",
    "ExecutionAnalyticsReportingService",
    "ExecutionAuditEventOut",
    "ExecutionAuditListOut",
    "ExecutionAuditQuery",
    "ExecutionAuditServiceDep",
    "ExecutionAuditStatsOut",
    "ExecutionDiagnosticOut",
    "ExecutionReportFiltersOut",
    "ExecutionReportPaginationOut",
    "ExecutionReportTenantContextOut",
    "ExecutionRestoreErrorOut",
    "ExecutionRestoreMetadataOut",
    "ExecutionRestoreRequest",
    "ExecutionRestoreRequestIn",
    "ExecutionRestoreResultOut",
    "ReportDimensionsOut",
    "ReportTemporalOut",
    "REPORT_CONTRACT_VERSION",
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
    "build_execution_audit_query",
    "build_execution_query",
    "execution_audit_statistics",
    "query_execution_audit",
    "get_execution_audit_service",
    "get_execution_history_service",
    "get_execution_recovery_service",
    "get_tenant_context",
    "router",
]
