from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import (
    BigInteger,
    Boolean,
    DateTime,
    Double,
    Float,
    ForeignKey,
    Integer,
    PrimaryKeyConstraint,
    Text,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


# =====================================================================
# LOT 1/4: Business entities, metrics, topology, clusters
# =====================================================================


class BusinessEntity(Base):
    __tablename__ = "business_entity"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    hidden: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    name: Mapped[str] = mapped_column(Text, nullable=False)
    query: Mapped[str | None] = mapped_column(Text, nullable=True)
    shared_with_customers: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    tenant_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)


class BusinessEntityField(Base):
    __tablename__ = "business_entity_field"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    hidden: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    name: Mapped[str] = mapped_column(Text, nullable=False)
    query: Mapped[str | None] = mapped_column(Text, nullable=True)
    type: Mapped[str | None] = mapped_column(Text, nullable=True)
    calc_function: Mapped[str | None] = mapped_column(Text, nullable=True)
    sql_id_key: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    sql_ts_key: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    business_entity_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("business_entity.id", ondelete="CASCADE"), nullable=True
    )


class BusinessEntityFieldMetadata(Base):
    __tablename__ = "business_entity_field_metadata"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    business_entity_field_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    tenant_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    customer_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    aggregation: Mapped[str | None] = mapped_column(Text, nullable=True)
    date_grouping: Mapped[str | None] = mapped_column(Text, nullable=True)
    range_config: Mapped[str | None] = mapped_column(Text, nullable=True)


class BusinessEntityMetadata(Base):
    __tablename__ = "business_entity_metadata"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    business_entity_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    tenant_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    customer_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    item_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)
    item_name: Mapped[str | None] = mapped_column(Text, nullable=True)


class Relation(Base):
    __tablename__ = "relation"

    business_entity_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("business_entity.id", ondelete="CASCADE"), primary_key=True
    )
    name: Mapped[str] = mapped_column(Text, primary_key=True)
    related_entity_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("business_entity.id", ondelete="CASCADE"), primary_key=True
    )
    direction: Mapped[str] = mapped_column(Text, primary_key=True)
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    query: Mapped[str | None] = mapped_column(Text, nullable=True)

    __table_args__ = (
        PrimaryKeyConstraint("business_entity_id", "name", "related_entity_id", "direction"),
    )


# TODO: EntityRelation renamed to Relation in Trendz 1.15.0.
# Backward-compatible alias — column names differ (from_id/related_entity_id, relation_type/name).
EntityRelation = Relation


class ClusterInfo(Base):
    __tablename__ = "cluster_info"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    cluster_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    segments_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    segments_percent: Mapped[int | None] = mapped_column(Integer, nullable=True)
    duration_ms: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    min_score: Mapped[float | None] = mapped_column(Float, nullable=True)
    max_score: Mapped[float | None] = mapped_column(Float, nullable=True)
    cluster_model_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("cluster_model.id", ondelete="SET NULL"), nullable=True
    )


class ClusterExample(Base):
    __tablename__ = "cluster_example"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    cluster_info_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("cluster_info.id", ondelete="SET NULL"), nullable=True
    )


# TODO: cluster_member does not exist in Trendz 1.15.0 schema.
# Use cluster_example instead.


class ClusterModel(Base):
    __tablename__ = "cluster_model"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    tenant_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)
    customer_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)
    create_ts: Mapped[int] = mapped_column(BigInteger, nullable=False)
    update_ts: Mapped[int] = mapped_column(BigInteger, nullable=False)
    name: Mapped[str | None] = mapped_column(Text, nullable=True)
    status: Mapped[str | None] = mapped_column(Text, nullable=True)
    type: Mapped[str | None] = mapped_column(Text, nullable=True)
    properties_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("ml_properties.id", ondelete="SET NULL"), nullable=True
    )
    dataset_config_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("dataset_config.id", ondelete="SET NULL"), nullable=True
    )
    tb_telemetry_key: Mapped[str | None] = mapped_column(Text, nullable=True)
    score_associated_field_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), nullable=True
    )
    score_index_associated_field_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), nullable=True
    )
    telemetry_save_period_unit: Mapped[str | None] = mapped_column(Text, nullable=True)
    alarm_type: Mapped[str | None] = mapped_column(Text, nullable=True)
    enabled_alarm_deletion: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    alarm_warning_threshold: Mapped[int | None] = mapped_column(Integer, nullable=True)
    alarm_minor_threshold: Mapped[int | None] = mapped_column(Integer, nullable=True)
    alarm_major_threshold: Mapped[int | None] = mapped_column(Integer, nullable=True)
    alarm_critical_threshold: Mapped[int | None] = mapped_column(Integer, nullable=True)


# =====================================================================
# LOT 2/4: Metrics, calculated fields, data sources, ML properties
# =====================================================================


class MetricDefinition(Base):
    __tablename__ = "metric_definition"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    business_entity_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    item_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    item_name: Mapped[str] = mapped_column(Text, nullable=False)
    tenant_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    customer_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)
    name: Mapped[str] = mapped_column(Text, nullable=False)
    user_input: Mapped[str] = mapped_column(Text, nullable=False)
    description: Mapped[str] = mapped_column(Text, nullable=False)
    how_to_calculate: Mapped[str] = mapped_column(Text, nullable=False)
    is_advanced_mode: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    use_cases: Mapped[str | None] = mapped_column(Text, nullable=True)
    fields: Mapped[str | None] = mapped_column(Text, nullable=True)
    code: Mapped[str | None] = mapped_column(Text, nullable=True)
    calculation_field_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), nullable=True
    )
    is_outdated: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    auto_deletable: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    created_ts: Mapped[int] = mapped_column(BigInteger, nullable=False)
    updated_ts: Mapped[int] = mapped_column(BigInteger, nullable=False)


class MetricDefinitionMetadata(Base):
    __tablename__ = "metric_definition_metadata"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    metric_definition_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    business_entity_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    item_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    tenant_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    customer_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)
    metric_name: Mapped[str] = mapped_column(Text, nullable=False)
    is_advanced_mode: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    is_saved: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    created_ts: Mapped[int] = mapped_column(BigInteger, nullable=False)


class MetricExploration(Base):
    __tablename__ = "metric_exploration"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    tenant_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    customer_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    item_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    item_name: Mapped[str] = mapped_column(Text, nullable=False)
    business_entity_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    business_entity_field_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    metric_definition_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    exploration_ts: Mapped[int] = mapped_column(BigInteger, nullable=False)


class CalculationField(Base):
    __tablename__ = "calculation_field"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    tenant_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    customer_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    name: Mapped[str] = mapped_column(Text, nullable=False)
    creation_time: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    update_time: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    business_entity_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    associated_entity_field_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), nullable=False
    )
    language: Mapped[str] = mapped_column(Text, nullable=False)
    calculation_field_type: Mapped[str] = mapped_column(Text, nullable=False)
    return_data_type: Mapped[str] = mapped_column(Text, nullable=False)
    grouping_interval: Mapped[str | None] = mapped_column(Text, nullable=True)
    field_aggregation: Mapped[str | None] = mapped_column(Text, nullable=True)
    fill_gap_enable: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    fill_gap_time_unit: Mapped[str | None] = mapped_column(Text, nullable=True)
    fill_gap_strategy: Mapped[str | None] = mapped_column(Text, nullable=True)
    tb_telemetry_key: Mapped[str] = mapped_column(Text, nullable=False)
    script: Mapped[str] = mapped_column(Text, nullable=False)
    time_range_strategy: Mapped[str | None] = mapped_column(Text, nullable=True)
    json_fixed_strategy_date_picker: Mapped[str | None] = mapped_column(Text, nullable=True)
    manual_dataset_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    split_time_range: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    split_time_range_time_unit: Mapped[str] = mapped_column(Text, nullable=False)

    __table_args__ = (
        UniqueConstraint("tenant_id", "name", name="calculation_field_tenant_id_name_key"),
    )


class CalculationFieldTaskData(Base):
    __tablename__ = "calculation_field_task_data"

    calculation_field_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True)
    enabled: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    tz_name: Mapped[str | None] = mapped_column(Text, nullable=True)
    json_item_name_set: Mapped[str | None] = mapped_column(Text, nullable=True)
    json_item_set: Mapped[str | None] = mapped_column(Text, nullable=True)
    json_reprocess_date_picker_config: Mapped[str | None] = mapped_column(Text, nullable=True)
    refresh_time_unit: Mapped[str | None] = mapped_column(Text, nullable=True)
    refresh_time_unit_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    refresh_time_unit_truncated: Mapped[bool | None] = mapped_column(Boolean, nullable=True)


# TODO: calculated_field_execution does not exist in Trendz 1.15.0 schema.


# TODO: Backward-compatible alias.
CalculatedField = CalculationField


class CachedTelemetry(Base):
    __tablename__ = "cached_telemetry"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    item_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)
    tenant_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)
    upload_time: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    calculated_field: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    state_field: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    start_ts: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    end_ts: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    field_type: Mapped[str | None] = mapped_column(Text, nullable=True)
    field_aggregation: Mapped[str | None] = mapped_column(Text, nullable=True)
    date_aggregation_type: Mapped[str | None] = mapped_column(Text, nullable=True)
    business_entity_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)
    business_entity_field_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), nullable=True
    )
    function: Mapped[str | None] = mapped_column(Text, nullable=True)
    latest_telemetry_point_ts: Mapped[int | None] = mapped_column(BigInteger, nullable=True)


class CachedTelemetryPoint(Base):
    __tablename__ = "cached_telemetry_point"

    ts: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    cached_telemetry_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True)
    numeric_value: Mapped[float | None] = mapped_column(Float, nullable=True)
    string_value: Mapped[str | None] = mapped_column(Text, nullable=True)
    boolean_value: Mapped[bool | None] = mapped_column(Boolean, nullable=True)

    __table_args__ = (PrimaryKeyConstraint("ts", "cached_telemetry_id"),)


class Datasource(Base):
    __tablename__ = "datasource"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    tenant_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    db_type: Mapped[str | None] = mapped_column(Text, nullable=True)
    url: Mapped[str | None] = mapped_column(Text, nullable=True)
    login: Mapped[str | None] = mapped_column(Text, nullable=True)
    pass_: Mapped[str | None] = mapped_column("pass", Text, nullable=True)


# TODO: Backward-compatible alias.
DataSource = Datasource


# TODO: metric_binding does not exist in Trendz 1.15.0 schema.


# =====================================================================
# LOT 3/4: Predictions, anomalies, tasks, alerts, writeback, ML
# =====================================================================


class PredictionModel(Base):
    __tablename__ = "prediction_model"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    tenant_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    customer_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    created_ts: Mapped[int] = mapped_column(BigInteger, nullable=False)
    updated_ts: Mapped[int] = mapped_column(BigInteger, nullable=False)
    name: Mapped[str] = mapped_column(Text, nullable=False)
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    partial_fit_enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    status: Mapped[str] = mapped_column(Text, nullable=False)
    type: Mapped[str] = mapped_column(Text, nullable=False)
    associated_entity_field_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), nullable=False
    )
    tb_telemetry_key: Mapped[str] = mapped_column(Text, nullable=False)
    model_parameters: Mapped[str] = mapped_column(Text, nullable=False)
    datasource_parameters: Mapped[str] = mapped_column(Text, nullable=False)
    method_parameters: Mapped[str] = mapped_column(Text, nullable=False)
    item_state_map: Mapped[str] = mapped_column(Text, nullable=False)
    trained_item_set: Mapped[str] = mapped_column(Text, nullable=False)
    business_entity_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    business_entity_field_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    model_uri: Mapped[str | None] = mapped_column(Text, nullable=True)
    avoid_disabling: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    # Enrichissement TrendX-owned (Étape 013, additif, colonnes NULL).
    # Lu via getattr() dans inference.py ; ne modifie aucune colonne Trendz.
    frequency: Mapped[str | None] = mapped_column(Text, nullable=True)
    algorithm: Mapped[str | None] = mapped_column(Text, nullable=True)
    scaler: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)
    hyperparameters: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)


# =====================================================================
# LOT 3b/4: TrendX-owned alerting + forecast persistence (Étape 013)
#   alert_rule / alert_incident : TrendX-owned (absents de Trendz 1.15.0)
#   Prediction : mappe la table EXISTANTE trendx_analytics.predictions (011)
# =====================================================================


class AlertRule(Base):
    __tablename__ = "alert_rule"
    __table_args__ = ({"schema": "trendx_catalog"},)

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name: Mapped[str] = mapped_column(Text, nullable=False)
    entity_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    metric_key: Mapped[str | None] = mapped_column(Text, nullable=True)
    rule_type: Mapped[str] = mapped_column(Text, nullable=False)
    condition_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)
    cooldown_seconds: Mapped[int] = mapped_column(Integer, nullable=False, default=7200)
    min_duration_seconds: Mapped[int] = mapped_column(Integer, nullable=False, default=60)
    open_threshold: Mapped[float | None] = mapped_column(Double, nullable=True)
    close_threshold: Mapped[float | None] = mapped_column(Double, nullable=True)
    severity: Mapped[str] = mapped_column(Text, nullable=False, default="MEDIUM")
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: datetime.now(UTC)
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: datetime.now(UTC)
    )


class AlertIncident(Base):
    __tablename__ = "alert_incident"
    __table_args__ = ({"schema": "trendx_catalog"},)

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    logical_key: Mapped[str] = mapped_column(Text, nullable=False, unique=True)
    rule_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("trendx_catalog.alert_rule.id", ondelete="SET NULL"),
        nullable=True,
    )
    entity_id: Mapped[str] = mapped_column(Text, nullable=False)
    metric_key: Mapped[str | None] = mapped_column(Text, nullable=True)
    severity: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(Text, nullable=False, default="ACTIVE")
    external_tb_alarm_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), nullable=True
    )
    opened_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: datetime.now(UTC)
    )
    acknowledged_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    cleared_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    closed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    acknowledged_by: Mapped[str | None] = mapped_column(Text, nullable=True)
    last_value: Mapped[float | None] = mapped_column(Double, nullable=True)
    open_reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    close_reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    additional_info: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)
    opened_count: Mapped[int] = mapped_column(Integer, nullable=False, default=1)


class Prediction(Base):
    __tablename__ = "predictions"
    __table_args__ = (
        PrimaryKeyConstraint("ts", "entity_id", "metric_key", "forecast_generated_at", "model_id"),
        {"schema": "trendx_analytics"},
    )

    ts: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    entity_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    metric_key: Mapped[str] = mapped_column(Text, nullable=False)
    forecast_generated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    model_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    model_used: Mapped[str] = mapped_column(Text, nullable=False)
    run_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    value: Mapped[float] = mapped_column(Double, nullable=False)
    lower_bound: Mapped[float | None] = mapped_column(Double, nullable=True)
    upper_bound: Mapped[float | None] = mapped_column(Double, nullable=True)
    horizon_step: Mapped[int] = mapped_column(Integer, nullable=False)
    frequency: Mapped[str] = mapped_column(Text, nullable=False, default="1h")
    mlflow_run_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    written_back: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    writeback_key: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: datetime.now(UTC)
    )


class PredictionModelStatusHistory(Base):
    __tablename__ = "prediction_model_status_history"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    prediction_model_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    business_entity_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    metric_key: Mapped[str] = mapped_column(Text, nullable=False)
    previous_status: Mapped[str | None] = mapped_column(Text, nullable=True)
    new_status: Mapped[str] = mapped_column(Text, nullable=False)
    changed_ts: Mapped[int] = mapped_column(BigInteger, nullable=False)


class PredictionModelLastItemPoint(Base):
    __tablename__ = "prediction_model_last_item_point"

    prediction_model_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True)
    item_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True)
    ts: Mapped[int] = mapped_column(BigInteger, nullable=False)

    __table_args__ = (PrimaryKeyConstraint("prediction_model_id", "item_id"),)


class PredictionModelTaskData(Base):
    __tablename__ = "prediction_model_task_data"

    prediction_model_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True)
    item_set_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    enabled: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    enabled_partial_fit: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    refresh_time_unit: Mapped[str | None] = mapped_column(Text, nullable=True)
    refresh_time_unit_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    refresh_time_unit_truncated: Mapped[bool | None] = mapped_column(Boolean, nullable=True)


# TODO: prediction_run does not exist in Trendz 1.15.0 schema.


# TODO: anomaly_detector does not exist in Trendz 1.15.0 schema.
# Real anomaly logic uses cluster_model + scored_point_anomaly.


class Anomaly(Base):
    __tablename__ = "anomaly"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    item_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)
    item_name: Mapped[str | None] = mapped_column(Text, nullable=True)
    start_ts: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    end_ts: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    cluster_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    score: Mapped[float | None] = mapped_column(Float, nullable=True)
    score_index: Mapped[int | None] = mapped_column(Integer, nullable=True)
    model_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)
    alarm_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)


class ScoredPointAnomaly(Base):
    __tablename__ = "scored_point_anomaly"

    t: Mapped[int | None] = mapped_column(BigInteger, primary_key=True)
    s: Mapped[float | None] = mapped_column(Float, primary_key=True)
    anomaly_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), primary_key=True)

    __table_args__ = (PrimaryKeyConstraint("t", "s", "anomaly_id"),)


# TODO: Backward-compatible alias — columns differ wildly from old anomaly_score model.
AnomalyScore = ScoredPointAnomaly


class ScoredPointCentroid(Base):
    __tablename__ = "scored_point_centroid"

    t: Mapped[int | None] = mapped_column(BigInteger, primary_key=True)
    s: Mapped[float | None] = mapped_column(Float, primary_key=True)
    cluster_info_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), primary_key=True)

    __table_args__ = (PrimaryKeyConstraint("t", "s", "cluster_info_id"),)


class ScoredPointCluster(Base):
    __tablename__ = "scored_point_cluster"

    t: Mapped[int | None] = mapped_column(BigInteger, primary_key=True)
    s: Mapped[float | None] = mapped_column(Float, primary_key=True)
    cluster_example_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), primary_key=True
    )

    __table_args__ = (PrimaryKeyConstraint("t", "s", "cluster_example_id"),)


class ScoredPointHistogram(Base):
    __tablename__ = "scored_point_histogram"

    t: Mapped[int | None] = mapped_column(BigInteger, primary_key=True)
    s: Mapped[float | None] = mapped_column(Float, primary_key=True)
    cluster_info_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), primary_key=True)

    __table_args__ = (PrimaryKeyConstraint("t", "s", "cluster_info_id"),)


class AnomalyModelTaskData(Base):
    __tablename__ = "anomaly_model_task_data"

    anomaly_model_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True)
    enabled_refresh: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    enabled_save_to_tb: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    enabled_alarm_creation: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    item_set_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    refresh_time_unit: Mapped[str | None] = mapped_column(Text, nullable=True)
    refresh_time_unit_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    refresh_time_unit_truncated: Mapped[bool | None] = mapped_column(Boolean, nullable=True)


# TODO: alert_rule does not exist in Trendz 1.15.0 schema.


# TODO: alert_incident does not exist in Trendz 1.15.0 schema.


# TODO: writeback_batch does not exist in Trendz 1.15.0 schema.


# TODO: writeback_point does not exist in Trendz 1.15.0 schema.


# =====================================================================
# LOT 4/4: Views, widgets, reports, tasks, system
# =====================================================================


class ViewConfig(Base):
    __tablename__ = "view_config"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    config_definition: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    date_picker_config: Mapped[str | None] = mapped_column(Text, nullable=True)
    name: Mapped[str | None] = mapped_column(Text, nullable=True)
    runtime_filters: Mapped[str | None] = mapped_column(Text, nullable=True)
    settings: Mapped[str | None] = mapped_column(Text, nullable=True)
    tz_name: Mapped[str | None] = mapped_column(Text, nullable=True)
    updated_at: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    view_type: Mapped[str | None] = mapped_column(Text, nullable=True)
    enable_report_cache: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    enable_persisted_cache: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    cache_time_unit: Mapped[str | None] = mapped_column(Text, nullable=True)
    auto_refresh_cache: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    task_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)
    refresh_frequency_time_unit: Mapped[str | None] = mapped_column(Text, nullable=True)
    refresh_frequency_time_unit_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    enable_calculated_telemetry_saving: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=False
    )
    calculated_telemetry_saving_task_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), nullable=True
    )
    calculated_telemetry_saving_execution_time_unit: Mapped[str | None] = mapped_column(
        Text, nullable=True
    )
    calculated_telemetry_saving_execution_time_unit_count: Mapped[int | None] = mapped_column(
        Integer, nullable=True
    )
    root_entity_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)
    row_click_entity_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)
    tenant_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    customer_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    collection_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    is_favorite: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    parent_path: Mapped[str] = mapped_column(Text, nullable=False)


class ViewField(Base):
    __tablename__ = "view_field"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    aggregation_type: Mapped[str | None] = mapped_column(Text, nullable=True)
    use_delta: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    color_config: Mapped[str | None] = mapped_column(Text, nullable=True)
    condition_field_ids: Mapped[str | None] = mapped_column(Text, nullable=True)
    date_grouping: Mapped[str | None] = mapped_column(Text, nullable=True)
    enable_runtime_filter: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    field_definition: Mapped[str | None] = mapped_column(Text, nullable=True)
    hidden: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    skip_render: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    include_historical_data: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    field_label: Mapped[str | None] = mapped_column(Text, nullable=True)
    local_time_range: Mapped[str | None] = mapped_column(Text, nullable=True)
    missed_relation_field: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    batch_calculation: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    native_calculation: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    calc_function: Mapped[str | None] = mapped_column(Text, nullable=True)
    local_calculation: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    from_template_entity_field: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    calculated_field: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    state_condition: Mapped[str | None] = mapped_column(Text, nullable=True)
    state_field: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    state_property: Mapped[str | None] = mapped_column(Text, nullable=True)
    parsed_condition: Mapped[str | None] = mapped_column(Text, nullable=True)
    parsed_function: Mapped[str | None] = mapped_column(Text, nullable=True)
    is_anomaly_field: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    anomaly_model_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)
    is_prediction_model_field: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    prediction_model_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)
    prediction_model_orig_entity_field_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), nullable=True
    )
    is_set_prediction_period: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    prediction_period_unit: Mapped[str] = mapped_column(Text, nullable=False)
    prediction_period_unit_count: Mapped[int] = mapped_column(Integer, nullable=False)
    selected_anomaly_field: Mapped[str | None] = mapped_column(Text, nullable=True)
    for_state_condition: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    is_alarm_field: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    selected_alarm_field: Mapped[str | None] = mapped_column(Text, nullable=True)
    script_language: Mapped[str | None] = mapped_column(Text, nullable=True)
    fill_gap_enable: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    fill_gap_time_unit: Mapped[str | None] = mapped_column(Text, nullable=True)
    fill_gap_strategy: Mapped[str | None] = mapped_column(Text, nullable=True)
    prediction_enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    prediction_method: Mapped[str | None] = mapped_column(Text, nullable=True)
    prediction_range_sec: Mapped[int] = mapped_column(Integer, nullable=False)
    custom_prediction_method: Mapped[str | None] = mapped_column(Text, nullable=True)
    multivariable_prediction_field_ids: Mapped[str | None] = mapped_column(Text, nullable=True)
    scale_value: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    separate_axis: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    separate_view_group: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    seria_type: Mapped[str | None] = mapped_column(Text, nullable=True)
    unit: Mapped[str | None] = mapped_column(Text, nullable=True)
    virtual_date_field: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    field_order: Mapped[int] = mapped_column(Integer, nullable=False)
    visually_hidden_field: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    prediction_pre_aggregation_disabled: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=False
    )
    state_max_duration: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    entity_field_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)
    business_entity_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)
    view_config_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("view_config.id", ondelete="CASCADE"), nullable=True
    )
    dataset_config_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)


# TODO: report_config does not exist in Trendz 1.15.0 schema.


# TODO: report_execution does not exist in Trendz 1.15.0 schema.


# TODO: backtest_window does not exist in Trendz 1.15.0 schema.


# TODO: model_selection_run does not exist in Trendz 1.15.0 schema.


# TODO: data_quality_report does not exist in Trendz 1.15.0 schema.


# TODO: data_quality_issue does not exist in Trendz 1.15.0 schema.


# TODO: feature_definition does not exist in Trendz 1.15.0 schema.


# TODO: forecast_leadtime does not exist in Trendz 1.15.0 schema.


# TODO: forecast_series does not exist in Trendz 1.15.0 schema.
# Use prediction_model + segment_data for forecast storage.


class TrendzTask(Base):
    __tablename__ = "trendz_task"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    tenant_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    customer_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    user_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    created_ts: Mapped[int] = mapped_column(BigInteger, nullable=False)
    updated_ts: Mapped[int] = mapped_column(BigInteger, nullable=False)
    name: Mapped[str] = mapped_column(Text, nullable=False)
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    reference_type: Mapped[str] = mapped_column(Text, nullable=False)
    reference_key: Mapped[str] = mapped_column(Text, nullable=False)
    job_type: Mapped[str] = mapped_column(Text, nullable=False)
    json_job: Mapped[str] = mapped_column(Text, nullable=False)
    schedule_type: Mapped[str] = mapped_column(Text, nullable=False)
    schedule_period_ts: Mapped[int] = mapped_column(BigInteger, nullable=False)
    schedule_planned_ts: Mapped[int] = mapped_column(BigInteger, nullable=False)
    schedule_scheduling_unit: Mapped[str] = mapped_column(Text, nullable=False)
    schedule_scheduling_unit_count: Mapped[int] = mapped_column(Integer, nullable=False)
    schedule_scheduling_time_zone: Mapped[str] = mapped_column(Text, nullable=False)
    ttl_enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    ttl_duration: Mapped[int] = mapped_column(BigInteger, nullable=False)
    store_execution_enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    store_execution_count: Mapped[int] = mapped_column(Integer, nullable=False)
    json_configs: Mapped[str] = mapped_column(Text, nullable=False)

    __table_args__ = (
        UniqueConstraint(
            "reference_type", "reference_key", name="trendz_task_reference_type_reference_key_key"
        ),
    )


class TrendzTaskExecution(Base):
    __tablename__ = "trendz_task_execution"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    task_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("trendz_task.id", ondelete="CASCADE"), nullable=False
    )
    tenant_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    customer_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    user_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    status: Mapped[str] = mapped_column(Text, nullable=False)
    created_ts: Mapped[int] = mapped_column(BigInteger, nullable=False)
    start_ts: Mapped[int] = mapped_column(BigInteger, nullable=False)
    finish_ts: Mapped[int] = mapped_column(BigInteger, nullable=False)
    duration: Mapped[int] = mapped_column(BigInteger, nullable=False)
    json_progress_content: Mapped[str] = mapped_column(Text, nullable=False)
    job_type: Mapped[str] = mapped_column(Text, nullable=False)
    json_job: Mapped[str] = mapped_column(Text, nullable=False)
    json_result: Mapped[str] = mapped_column(Text, nullable=False)


class TrendzTaskExecutionProgressStep(Base):
    __tablename__ = "trendz_task_execution_progress_step"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    execution_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("trendz_task_execution.id", ondelete="CASCADE"),
        nullable=False,
    )
    parent_step_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("trendz_task_execution_progress_step.id", ondelete="CASCADE"),
        nullable=True,
    )
    name: Mapped[str] = mapped_column(Text, nullable=False)
    start_ts: Mapped[int] = mapped_column(BigInteger, nullable=False)
    finish_ts: Mapped[int] = mapped_column(BigInteger, nullable=False)


class TrendzTaskExecutionRequest(Base):
    __tablename__ = "trendz_task_execution_request"

    task_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True)
    execution_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True)
    tenant_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    customer_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    user_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    scheduled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    job_type: Mapped[str] = mapped_column(Text, nullable=False)
    json_job: Mapped[str] = mapped_column(Text, nullable=False)
    created_ts: Mapped[int] = mapped_column(BigInteger, nullable=False)
    state: Mapped[str] = mapped_column(Text, nullable=False)

    __table_args__ = (PrimaryKeyConstraint("task_id", "execution_id"),)


class TrendzTaskExecutionStateRecord(Base):
    __tablename__ = "trendz_task_execution_state_record"

    execution_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True)
    state: Mapped[str] = mapped_column(Text, nullable=False)
    last_update_ts: Mapped[int] = mapped_column(BigInteger, nullable=False)
    removed_task: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)


class TrendzTaskSchedulingStateRecord(Base):
    __tablename__ = "trendz_task_scheduling_state_record"

    task_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True)
    state: Mapped[str] = mapped_column(Text, nullable=False)
    last_finish_ts: Mapped[int] = mapped_column(BigInteger, nullable=False)


class TrendzTaskSequence(Base):
    __tablename__ = "trendz_task_sequence"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    tenant_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    customer_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    user_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    created_ts: Mapped[int] = mapped_column(BigInteger, nullable=False)
    updated_ts: Mapped[int] = mapped_column(BigInteger, nullable=False)
    name: Mapped[str] = mapped_column(Text, nullable=False)
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    json_configs: Mapped[str] = mapped_column(Text, nullable=False)


class TrendzTaskSequenceItem(Base):
    __tablename__ = "trendz_task_sequence_item"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    sequence_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("trendz_task_sequence.id", ondelete="CASCADE"),
        nullable=False,
    )
    reference_type: Mapped[str] = mapped_column(Text, nullable=False)
    reference_key: Mapped[str] = mapped_column(Text, nullable=False)
    order_number: Mapped[int | None] = mapped_column(Integer, nullable=True)
    execution_delay_ms: Mapped[int | None] = mapped_column(BigInteger, nullable=True)


# TODO: checkpoint does not exist in Trendz 1.15.0 schema.
# Use trendx checkpoint tables or external watermark store.


# TODO: widget_config does not exist in Trendz 1.15.0 schema.


# TODO: audit_log does not exist in Trendz 1.15.0 schema.


# TODO: app_setting does not exist in Trendz 1.15.0 schema.
# Real equivalent: trendz_system_property (key/value).


class TrendzSystemProperty(Base):
    __tablename__ = "trendz_system_property"

    property_key: Mapped[str] = mapped_column(Text, primary_key=True)
    property_value: Mapped[str | None] = mapped_column(Text, nullable=True)


# TODO: notification does not exist in Trendz 1.15.0 schema.


# TODO: mlflow_alias does not exist in Trendz 1.15.0 schema.


# TODO: mlops_environment does not exist in Trendz 1.15.0 schema.


# TODO: topology_discovery does not exist in Trendz 1.15.0 schema.


# TODO: telemetry_snapshot does not exist in Trendz 1.15.0 schema.


class AgentAi(Base):
    __tablename__ = "agent_ai"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    agent_type: Mapped[str] = mapped_column(Text, nullable=False)
    system_message: Mapped[str] = mapped_column(Text, nullable=False)
    version: Mapped[int] = mapped_column(Integer, nullable=False)


class ApiKey(Base):
    __tablename__ = "api_key"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    tenant_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    token: Mapped[str] = mapped_column(Text, nullable=False)


class CustomPredictionModel(Base):
    __tablename__ = "custom_prediction_model"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    model_name: Mapped[str] = mapped_column(Text, nullable=False)
    content: Mapped[str] = mapped_column(Text, nullable=False)


class CustomPrompt(Base):
    __tablename__ = "custom_prompt"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name: Mapped[str] = mapped_column(Text, nullable=False)
    prompt: Mapped[str | None] = mapped_column(Text, nullable=True)
    is_system: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    tenant_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)
    customer_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)
    user_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)
    created_ts: Mapped[int] = mapped_column(BigInteger, nullable=False)
    last_modified_ts: Mapped[int] = mapped_column(BigInteger, nullable=False)
    is_deleted: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)


class CustomPromptMetadata(Base):
    __tablename__ = "custom_prompt_metadata"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    prompt: Mapped[str | None] = mapped_column(Text, nullable=True)
    data: Mapped[str | None] = mapped_column(Text, nullable=True)
    response: Mapped[str | None] = mapped_column(Text, nullable=True)
    tenant_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    customer_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)
    user_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)
    duration: Mapped[int] = mapped_column(BigInteger, nullable=False)
    created_ts: Mapped[int] = mapped_column(BigInteger, nullable=False)


class CustomViewSettings(Base):
    __tablename__ = "custom_view_settings"

    domain: Mapped[str] = mapped_column(Text, primary_key=True)
    palette_selection: Mapped[str | None] = mapped_column(Text, nullable=True)
    palette_trendz: Mapped[str | None] = mapped_column(Text, nullable=True)
    palette_tb: Mapped[str | None] = mapped_column(Text, nullable=True)
    url: Mapped[str | None] = mapped_column(Text, nullable=True)
    tab_name: Mapped[str | None] = mapped_column(Text, nullable=True)
    logo_base64: Mapped[str | None] = mapped_column(Text, nullable=True)
    dark_mode: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    help_mode_enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    thingsboard_redirect_url: Mapped[str | None] = mapped_column(Text, nullable=True)


class DatasetConfig(Base):
    __tablename__ = "dataset_config"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    max_points_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    start_ts: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    end_ts: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    business_entity_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)
    tz_name: Mapped[str | None] = mapped_column(Text, nullable=True)
    item_set: Mapped[str | None] = mapped_column(Text, nullable=True)


class DomainTenantPair(Base):
    __tablename__ = "domain_tenant_pair"

    domain: Mapped[str] = mapped_column(Text, primary_key=True)
    tenant_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True)

    __table_args__ = (PrimaryKeyConstraint("domain", "tenant_id"),)


class LatestTelemetry(Base):
    __tablename__ = "latest_telemetry"

    calculation_field_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True)
    item_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True)
    key: Mapped[str] = mapped_column(Text, primary_key=True)
    value: Mapped[int] = mapped_column(BigInteger, nullable=False)

    __table_args__ = (PrimaryKeyConstraint("calculation_field_id", "item_id", "key"),)


class LicenceData(Base):
    __tablename__ = "licence_data"

    tenant_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True)
    content: Mapped[str | None] = mapped_column(Text, nullable=True)


class LlmConfig(Base):
    __tablename__ = "llm_config"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    tenant_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    created_ts: Mapped[int] = mapped_column(BigInteger, nullable=False)
    updated_ts: Mapped[int] = mapped_column(BigInteger, nullable=False)
    name: Mapped[str] = mapped_column(Text, nullable=False)
    model_provider: Mapped[str] = mapped_column(Text, nullable=False)
    model_name: Mapped[str] = mapped_column(Text, nullable=False)
    json_properties: Mapped[str] = mapped_column(Text, nullable=False)


class LlmSettings(Base):
    __tablename__ = "llm_settings"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    tenant_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    use_default: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    default_llm_config_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), nullable=True
    )


class LlmSettingsChatTypeLink(Base):
    __tablename__ = "llm_settings_chat_type_link"

    llm_setting_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True)
    chat_type: Mapped[str] = mapped_column(Text, primary_key=True)
    llm_config_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)

    __table_args__ = (PrimaryKeyConstraint("llm_setting_id", "chat_type"),)


class MlProperties(Base):
    __tablename__ = "ml_properties"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    json_value: Mapped[str | None] = mapped_column(Text, nullable=True)


class ManualDataset(Base):
    __tablename__ = "manual_dataset"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    data: Mapped[str] = mapped_column(Text, nullable=False)


class SegmentData(Base):
    __tablename__ = "segment_data"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    model_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)
    item_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)
    item_name: Mapped[str | None] = mapped_column(Text, nullable=True)
    range_start_ts: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    range_end_ts: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    prehistorical_telemetry: Mapped[str | None] = mapped_column(Text, nullable=True)
    historical_telemetry: Mapped[str | None] = mapped_column(Text, nullable=True)
    additional_telemetries: Mapped[str | None] = mapped_column(Text, nullable=True)
    prediction_telemetry: Mapped[str | None] = mapped_column(Text, nullable=True)


class UserMetadata(Base):
    __tablename__ = "user_metadata"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    tenant_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    customer_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    default_business_entity_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), nullable=True
    )
    default_date_picker_config: Mapped[str | None] = mapped_column(Text, nullable=True)
    default_item_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)
    default_item_name: Mapped[str | None] = mapped_column(Text, nullable=True)


class UserRecord(Base):
    __tablename__ = "user_record"

    tenant_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True)
    customer_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True)
    user_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True)
    username: Mapped[str] = mapped_column(Text, nullable=False)
    visit_first_ts: Mapped[int] = mapped_column(BigInteger, nullable=False)
    visit_last_ts: Mapped[int] = mapped_column(BigInteger, nullable=False)
    valid: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    validation_last_ts: Mapped[int] = mapped_column(BigInteger, nullable=False)
    json_data: Mapped[str] = mapped_column(Text, nullable=False)

    __table_args__ = (PrimaryKeyConstraint("tenant_id", "customer_id", "user_id"),)


class ViewAssistanceChat(Base):
    __tablename__ = "view_assistance_chat"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    tenant_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    customer_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)
    user_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)
    type: Mapped[str] = mapped_column(Text, nullable=False)
    chat_summary: Mapped[str | None] = mapped_column(Text, nullable=True)
    available_topology: Mapped[str | None] = mapped_column(Text, nullable=True)
    reference_key: Mapped[str | None] = mapped_column(Text, nullable=True)
    metric_definition_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), nullable=True
    )
    created_ts: Mapped[int] = mapped_column(BigInteger, nullable=False)
    last_modified_ts: Mapped[int] = mapped_column(BigInteger, nullable=False)
    is_deleted: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)


class ViewAssistanceChatMessage(Base):
    __tablename__ = "view_assistance_chat_message"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    chat_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("view_assistance_chat.id", ondelete="CASCADE"),
        nullable=False,
    )
    user_question: Mapped[str] = mapped_column(Text, nullable=False)
    code: Mapped[str | None] = mapped_column(Text, nullable=True)
    ai_answer: Mapped[str | None] = mapped_column(Text, nullable=True)
    ai_memory: Mapped[str | None] = mapped_column(Text, nullable=True)
    json_job_response: Mapped[str | None] = mapped_column(Text, nullable=True)
    last_modified_by: Mapped[str] = mapped_column(Text, nullable=False)
    created_ts: Mapped[int] = mapped_column(BigInteger, nullable=False)
    last_modified_ts: Mapped[int] = mapped_column(BigInteger, nullable=False)
    version: Mapped[int] = mapped_column(BigInteger, nullable=False)
    is_deleted: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)


class ViewAssistanceTokenUsage(Base):
    __tablename__ = "view_assistance_token_usage"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    tenant_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    input_token_used: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    output_token_used: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    model_name: Mapped[str | None] = mapped_column(Text, nullable=True)
    start_of_month_ts: Mapped[int] = mapped_column(BigInteger, nullable=False)
    is_system_model: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)


class ViewCollection(Base):
    __tablename__ = "view_collection"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    tenant_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    customer_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    parent_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    collection_name: Mapped[str] = mapped_column(Text, nullable=False)
    creation_ts: Mapped[int] = mapped_column(BigInteger, nullable=False)
    update_ts: Mapped[int] = mapped_column(BigInteger, nullable=False)


# TODO: entity_group does not exist in Trendz 1.15.0 schema.


# TODO: entity_group_member does not exist in Trendz 1.15.0 schema.


# TODO: SchemaVersion does not exist in Trendz 1.15.0 schema.
# Use trendz_system_property for key/value config.


# Scheduler B1 (migration 014, TrendX-owned) — reporté verbatim depuis master
# pour la résolution W49 (persistance requise par la rétention
# scheduler_runs du worker). Aucune modification des modèles existants.


class SchedulerRun(Base):
    """Ligne d'exécution d'un job scheduler P0 (TrendX-owned, migration 014).

    Cycle : running -> ok | failed. Les lignes running orphelines (crash)
    sont reprises en failed (interrupted). task_id est NULLABLE et SANS FK :
    les jobs `direct` n'ont pas de tâche, et la purge des tâches ne doit
    jamais effacer l'historique des runs. finished_at est NULL tant que
    running (invariant : running => finished_at IS NULL).
    """

    __tablename__ = "scheduler_run"
    __table_args__ = ({"schema": "trendx_catalog"},)

    run_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    job_id: Mapped[str] = mapped_column(Text, nullable=False)
    job_type: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(Text, nullable=False)
    triggered_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: datetime.now(UTC)
    )
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)
    instance_id: Mapped[str] = mapped_column(Text, nullable=False)
    mode: Mapped[str] = mapped_column(Text, nullable=False, default="direct")
    task_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)


class SchedulerHeartbeat(Base):
    """Heartbeat d'instance scheduler (TrendX-owned, migration 014).

    Une ligne par instance_id. Utilisé par /healthz (leader|standby|stale),
    les métriques et la future élection leader (MR-3 : advisory lock, fencing).
    """

    __tablename__ = "scheduler_heartbeat"
    __table_args__ = ({"schema": "trendx_catalog"},)

    instance_id: Mapped[str] = mapped_column(Text, primary_key=True)
    leader: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    heartbeat_ts: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: datetime.now(UTC)
    )
    version: Mapped[str | None] = mapped_column(Text, nullable=True)
    host: Mapped[str | None] = mapped_column(Text, nullable=True)
