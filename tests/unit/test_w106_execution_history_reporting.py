"""W106 — stable reporting contract over W104 execution analytics."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient
from trendx.config import settings
from trendx.forecasting.analytics import ExecutionAnalytics, analyze_execution_history
from trendx.forecasting.api import (
    TenantContext,
    get_execution_history_service,
    get_tenant_context,
)
from trendx.forecasting.execution import (
    DurableExecutionStore,
    ExecutionProvenance,
    ExecutionRecord,
)
from trendx.forecasting.history import ExecutionHistoryService, ExecutionQuery
from trendx.forecasting.reporting import (
    REPORT_CONTRACT_VERSION,
    ExecutionAnalyticsReportingService,
    ExecutionReportingDataError,
    build_execution_analytics_report,
)
from trendx.main import app

BASE_TIME = datetime(2026, 1, 1, tzinfo=UTC)


@dataclass(frozen=True)
class Spec:
    execution_id: str
    tenant: str
    entity_type: str
    entity_id: str
    metric: str
    model: str
    version: str
    algorithm: str
    schema: str
    fingerprint: str
    created_at: str
    status: str = "SUCCESS"
    error_code: str = ""
    predictions: int = 1
    completed_at: str = "2099-01-01T00:00:10Z"


def _record(spec: Spec) -> ExecutionRecord:
    return ExecutionRecord(
        execution_id=spec.execution_id,
        reference_key=f"w106:{spec.execution_id}",
        tenant_id=spec.tenant,
        entity_type=spec.entity_type,
        entity_id=spec.entity_id,
        target_metric=spec.metric,
        frequency="1h",
        horizon=24,
        model_id=spec.model,
        model_version=spec.version,
        algorithm=spec.algorithm,
        feature_schema_version=spec.schema,
        feature_schema_fingerprint=spec.fingerprint,
        artifact_uri=f"file:///w106/{spec.execution_id}.bin",
        created_at=spec.created_at,
        metadata={"source": "w106"},
    )


def _provenance(record: ExecutionRecord, predictions: int) -> ExecutionProvenance:
    return ExecutionProvenance(
        model_id=record.model_id,
        model_version=record.model_version,
        algorithm=record.algorithm,
        feature_schema_version=record.feature_schema_version,
        feature_schema_fingerprint=record.feature_schema_fingerprint,
        artifact_uri=record.artifact_uri,
        prediction_count=predictions,
        metadata={"source": "w106"},
        result={"values": [1.0] * predictions},
    )


def _seed(path: Path, specs: tuple[Spec, ...]) -> DurableExecutionStore:
    store = DurableExecutionStore(path)
    for spec in specs:
        record = _record(spec)
        store.create(record)
        if spec.status == "PENDING":
            continue
        store.mark_started(record.execution_id)
        if spec.status == "RUNNING":
            continue
        if spec.status == "FAILED":
            store.mark_failed(
                record.execution_id,
                error_code=spec.error_code,
                error_reason="W106 controlled failure",
                provenance=_provenance(record, 0),
                completed_at=spec.completed_at,
            )
        elif spec.status == "SUCCESS":
            store.mark_success(
                record.execution_id,
                _provenance(record, spec.predictions),
                completed_at=spec.completed_at,
            )
        else:
            msg = f"Unsupported W106 fixture status: {spec.status}"
            raise ValueError(msg)
    return store


def _specs() -> tuple[Spec, ...]:
    return (
        Spec(
            "a-success",
            "tenant-A",
            "DEVICE",
            "device-a",
            "temperature",
            "model-a",
            "1",
            "Fourier",
            "schema-a",
            "fingerprint-a",
            "2026-01-01T00:10:00Z",
            predictions=3,
        ),
        Spec(
            "a-failure-schema",
            "tenant-A",
            "ASSET",
            "asset-a",
            "energy_consumption",
            "model-b",
            "1",
            "Prophet",
            "schema-b",
            "fingerprint-b",
            "2026-01-01T00:20:00+00:00",
            status="FAILED",
            error_code="feature_schema_mismatch",
        ),
        Spec(
            "a-failure-champion",
            "tenant-A",
            "ASSET",
            "asset-a",
            "energy_consumption",
            "model-b",
            "1",
            "Prophet",
            "schema-b",
            "fingerprint-b",
            "2026-01-01T00:30:00Z",
            status="FAILED",
            error_code="no-compatible-champion",
        ),
        Spec(
            "a-failure-artifact",
            "tenant-A",
            "DEVICE",
            "device-b",
            "pressure",
            "model-c",
            "2",
            "ARIMA",
            "schema-c",
            "fingerprint-c",
            "2026-01-01T00:40:00Z",
            status="FAILED",
            error_code="artifact-missing",
        ),
        Spec(
            "a-failure-payload",
            "tenant-A",
            "DEVICE",
            "device-b",
            "flow_rate",
            "model-c",
            "2",
            "ARIMA",
            "schema-c",
            "fingerprint-c",
            "2026-01-01T00:50:00Z",
            status="FAILED",
            error_code="payload_invalid",
        ),
        Spec(
            "a-failure-corrupted",
            "tenant-A",
            "DEVICE",
            "device-b",
            "solar_power",
            "model-c",
            "2",
            "ARIMA",
            "schema-c",
            "fingerprint-c",
            "2026-01-01T01:00:00Z",
            status="FAILED",
            error_code="artifact-corrupted",
        ),
        Spec(
            "a-pending",
            "tenant-A",
            "DEVICE",
            "device-c",
            "temperature",
            "model-a",
            "1",
            "Fourier",
            "schema-a",
            "fingerprint-a",
            "2026-01-01T01:10:00Z",
            status="PENDING",
        ),
        Spec(
            "a-running",
            "tenant-A",
            "DEVICE",
            "device-c",
            "humidity",
            "model-d",
            "3",
            "Prophet",
            "schema-d",
            "fingerprint-d",
            "2026-01-01T01:20:00Z",
            status="RUNNING",
        ),
        Spec(
            "b-success",
            "tenant-B",
            "DEVICE",
            "device-only-b",
            "temperature",
            "model-b",
            "9",
            "Fourier",
            "schema-b",
            "fingerprint-b",
            "2026-01-01T01:30:00Z",
            predictions=4,
        ),
    )


def _report(
    service: ExecutionHistoryService,
    query: ExecutionQuery,
) -> tuple[ExecutionAnalytics, Any]:
    analytics = analyze_execution_history(service, query)
    report = ExecutionAnalyticsReportingService(service).report(query)
    return analytics, report


def _w104_payload(analytics: ExecutionAnalytics) -> dict[str, Any]:
    return analytics.to_dict()


def _auth_headers() -> dict[str, str]:
    return {"Authorization": f"Bearer {settings.trendx_api_token.get_secret_value()}"}


@pytest.fixture
def api_client(
    tmp_path: Path,
) -> Iterator[tuple[TestClient, ExecutionHistoryService, DurableExecutionStore]]:
    store = _seed(tmp_path / "history.json", _specs())
    service = ExecutionHistoryService(store)
    app.dependency_overrides[get_execution_history_service] = lambda: service
    app.dependency_overrides[get_tenant_context] = lambda: TenantContext("tenant-A")
    try:
        with TestClient(app) as client:
            yield client, service, store
    finally:
        app.dependency_overrides.clear()
        store.close()


@pytest.mark.unit
def test_w106_report_contract(tmp_path: Path) -> None:
    store = _seed(tmp_path / "history.json", _specs())
    try:
        service = ExecutionHistoryService(store)
        report = ExecutionAnalyticsReportingService(service).report(
            ExecutionQuery(
                tenant_id="tenant-A",
                entity_type="DEVICE",
                created_at_from="2026-01-01T00:10:00Z",
            )
        )
        assert report.contract_version == REPORT_CONTRACT_VERSION == "1"
        assert report.generated_at is None
        assert report.tenant_context.tenant_id == "tenant-A"
        assert report.tenant_id == "tenant-A"
        assert report.applied_filters.tenant_id == "tenant-A"
        assert report.applied_filters.entity_type == "DEVICE"
        assert report.applied_filters.created_at_from == "2026-01-01T00:10:00+00:00"
        assert report.pagination.applied is False
        assert report.pagination.reason == "aggregate_report"
        assert report.summary.total_executions == 6
        assert report.dimensions.by_metric
        assert report.temporal.trend_contract.bucket == "exact_timestamp"
        assert report.failures
        timestamped = ExecutionAnalyticsReportingService(service).report(
            ExecutionQuery(
                tenant_id="tenant-A",
                entity_type="DEVICE",
                created_at_from="2026-01-01T00:10:00Z",
            ),
            generated_at="2026-01-01T00:00:00Z",
        )
        assert timestamped.generated_at == "2026-01-01T00:00:00Z"
        assert timestamped.summary == report.summary
        payload = report.to_dict()
        assert json.loads(json.dumps(payload, sort_keys=True)) == payload
        assert set(payload) == {
            "contract_version",
            "generated_at",
            "tenant_id",
            "tenant_context",
            "applied_filters",
            "pagination",
            "summary",
            "dimensions",
            "temporal",
            "failures",
        }
    finally:
        store.close()


@pytest.mark.unit
def test_w106_matches_w104(tmp_path: Path) -> None:
    store = _seed(tmp_path / "history.json", _specs())
    try:
        service = ExecutionHistoryService(store)
        analytics, report = _report(service, ExecutionQuery(tenant_id="tenant-A"))
        assert report.summary == analytics.summary
        assert report.dimensions.by_metric == analytics.by_metric
        assert report.dimensions.by_entity == analytics.by_entity
        assert report.dimensions.by_algorithm == analytics.by_algorithm
        assert report.dimensions.by_model == analytics.by_model
        assert report.dimensions.by_status == analytics.by_status
        assert report.failures == analytics.failures
        assert report.temporal.created_at_trend == analytics.created_at_trend
        assert report.to_dict()["summary"] == _w104_payload(analytics)["summary"]
    finally:
        store.close()


@pytest.mark.unit
def test_w106_filters(tmp_path: Path) -> None:
    store = _seed(tmp_path / "history.json", _specs())
    query_cases = (
        {"entity_type": "ASSET"},
        {"entity_id": "device-b"},
        {"target_metric": "energy_consumption"},
        {"execution_id": "a-success"},
        {"reference_key": "w106:a-success"},
        {"model_id": "model-b"},
        {"model_version": "1"},
        {"algorithm": "prophet"},
        {"feature_schema_version": "schema-b"},
        {"feature_schema_fingerprint": "fingerprint-b"},
        {"status": "FAILED"},
        {"created_at_from": "2026-01-01T00:10:00Z"},
        {"created_at_to": "2026-01-01T01:00:00Z"},
        {"completed_at_from": "2099-01-01T00:00:00Z"},
        {"completed_at_to": "2099-01-01T00:00:10Z"},
    )
    try:
        service = ExecutionHistoryService(store)
        for values in query_cases:
            query = ExecutionQuery(tenant_id="tenant-A", **values)
            analytics, report = _report(service, query)
            assert report.to_dict()["summary"] == _w104_payload(analytics)["summary"]
            assert report.applied_filters.status == values.get("status")
        combined = ExecutionQuery(
            tenant_id="tenant-A",
            entity_type="ASSET",
            target_metric="energy_consumption",
            status="FAILED",
        )
        _, report = _report(service, combined)
        assert report.summary.failure_count == 2
    finally:
        store.close()


@pytest.mark.unit
def test_w106_empty_dataset(tmp_path: Path) -> None:
    path = tmp_path / "empty.json"
    store = DurableExecutionStore(path)
    try:
        report = ExecutionAnalyticsReportingService(ExecutionHistoryService(store)).report(
            ExecutionQuery(tenant_id="tenant-A")
        )
        assert report.summary.total_executions == 0
        assert report.summary.success_count == 0
        assert report.summary.failure_count == 0
        assert report.summary.success_rate == 0.0
        assert report.summary.prediction_count_total == 0
        assert report.dimensions.by_metric == ()
        assert report.temporal.created_at_trend == ()
        assert report.failures == ()
        assert report.to_dict()["generated_at"] is None
    finally:
        store.close()


@pytest.mark.unit
def test_w106_failure_breakdown(tmp_path: Path) -> None:
    store = _seed(tmp_path / "history.json", _specs())
    try:
        report = ExecutionAnalyticsReportingService(ExecutionHistoryService(store)).report(
            ExecutionQuery(tenant_id="tenant-A")
        )
        assert {item.error_code: item.count for item in report.failures} == {
            "artifact-corrupted": 1,
            "artifact-missing": 1,
            "feature_schema_mismatch": 1,
            "no-compatible-champion": 1,
            "payload_invalid": 1,
        }
        assert sum(item.count for item in report.failures) == report.summary.failure_count
        assert all(0.0 <= item.proportion <= 1.0 for item in report.failures)
        serialized = json.dumps(report.to_dict(), sort_keys=True)
        assert "W106 controlled failure" not in serialized
        assert "file:///" not in serialized
    finally:
        store.close()


@pytest.mark.unit
def test_w106_tenant_isolation(tmp_path: Path) -> None:
    store = _seed(tmp_path / "history.json", _specs())
    try:
        service = ExecutionHistoryService(store)
        _, report_a = _report(service, ExecutionQuery(tenant_id="tenant-A"))
        _, report_b = _report(service, ExecutionQuery(tenant_id="tenant-B"))
        assert report_a.summary.total_executions == 8
        assert report_b.summary.total_executions == 1
        assert "device-only-b" not in {item.entity_id for item in report_a.dimensions.by_entity}
        assert "device-a" not in {item.entity_id for item in report_b.dimensions.by_entity}
        analytics_a = analyze_execution_history(service, ExecutionQuery(tenant_id="tenant-A"))
        with pytest.raises(ExecutionReportingDataError):
            build_execution_analytics_report(analytics_a, ExecutionQuery(tenant_id="tenant-B"))
    finally:
        store.close()


@pytest.mark.unit
def test_w106_invalid_request(
    api_client: tuple[TestClient, ExecutionHistoryService, DurableExecutionStore],
) -> None:
    client, _, _ = api_client
    invalid = client.get(
        "/api/v1/forecast/executions/report",
        params={"created_at_from": "not-a-date"},
        headers=_auth_headers(),
    )
    assert invalid.status_code == 422
    assert invalid.json() == {"detail": "invalid_query"}
    forced = client.get(
        "/api/v1/forecast/executions/report",
        params={"tenant_id": "tenant-B"},
        headers=_auth_headers(),
    )
    assert forced.status_code == 403
    assert forced.json() == {"detail": "tenant_forbidden"}


@pytest.mark.unit
def test_w106_read_only(tmp_path: Path) -> None:
    path = tmp_path / "history.json"
    store = _seed(path, _specs())
    before_hash = hashlib.sha256(path.read_bytes()).hexdigest()
    before_records = tuple(record.to_dict() for record in store.list())
    try:
        service = ExecutionHistoryService(store)
        first = ExecutionAnalyticsReportingService(service).report(
            ExecutionQuery(tenant_id="tenant-A")
        )
        second = ExecutionAnalyticsReportingService(service).report(
            ExecutionQuery(tenant_id="tenant-A")
        )
        assert first.to_dict() == second.to_dict()
        assert hashlib.sha256(path.read_bytes()).hexdigest() == before_hash
        assert tuple(record.to_dict() for record in store.list()) == before_records
    finally:
        store.close()
