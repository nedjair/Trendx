from __future__ import annotations

import uuid
from collections.abc import Sequence
from datetime import UTC, datetime
from typing import Any, Generic, TypeVar, cast

from loguru import logger
from sqlalchemy import Integer, Table, delete, func, select, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.orm import Session
from trendx.database.models import (
    AlertIncident,
    AlertRule,
    BusinessEntity,
    EntityRelation,
    MetricDefinition,
    Prediction,
    PredictionModel,
    PredictionModelStatusHistory,
    SchedulerHeartbeat,
    SchedulerRun,
    TrendzTask,
    ViewConfig,
)

T = TypeVar("T")


class BaseRepository(Generic[T]):
    def __init__(self, session: Session, model: type[T]) -> None:
        self._session = session
        self._model = model

    def create(self, **kwargs: Any) -> T:
        instance = self._model(**kwargs)
        self._session.add(instance)
        self._session.flush()
        logger.debug("Created {}: {}", self._model.__name__, instance)
        return instance

    def create_from(self, instance: T) -> T:
        self._session.add(instance)
        self._session.flush()
        return instance

    def get(self, ident: Any) -> T | None:
        return cast(T | None, self._session.get(self._model, ident))

    def update(self, ident: Any, **kwargs: Any) -> T | None:
        instance = self.get(ident)
        if instance is None:
            logger.warning("{} with id={} not found", self._model.__name__, ident)
            return None
        for key, value in kwargs.items():
            setattr(instance, key, value)
        self._session.flush()
        logger.debug("Updated {} id={}", self._model.__name__, ident)
        return instance

    def update_many(self, stmt: Any) -> int:
        result = self._session.execute(stmt)
        self._session.flush()
        return cast(int, result.rowcount)

    def delete(self, ident: Any) -> bool:
        instance = self.get(ident)
        if instance is None:
            return False
        self._session.delete(instance)
        self._session.flush()
        logger.debug("Deleted {} id={}", self._model.__name__, ident)
        return True

    def delete_many(self, stmt: Any) -> int:
        result = self._session.execute(stmt)
        self._session.flush()
        return cast(int, result.rowcount)

    def list(
        self,
        *filters: Any,
        order_by: Any | None = None,
        limit: int | None = None,
        offset: int | None = None,
    ) -> Sequence[T]:
        stmt = select(self._model)
        if filters:
            stmt = stmt.where(*filters)
        if order_by is not None:
            stmt = stmt.order_by(order_by)
        if offset is not None:
            stmt = stmt.offset(offset)
        if limit is not None:
            stmt = stmt.limit(limit)
        values: Any = self._session.scalars(stmt).all()
        return cast(Sequence[T], values)

    def count(self, *filters: Any) -> int:
        stmt = select(func.count(cast(Any, self._model).id)).select_from(self._model)
        if filters:
            stmt = stmt.where(*filters)
        result = self._session.execute(stmt)
        value: Any = result.scalar_one()
        return cast(int, value)

    def exists(self, *filters: Any) -> bool:
        stmt = select(self._model).where(*filters).limit(1)
        return self._session.execute(stmt).first() is not None


# =====================================================================
# Specific repositories (aligned to Trendz 1.15.0 real schema)
# =====================================================================


class BusinessEntityRepository(BaseRepository[BusinessEntity]):
    def __init__(self, session: Session) -> None:
        super().__init__(session, BusinessEntity)

    def find_by_tenant(self, tenant_id: Any) -> Sequence[BusinessEntity]:
        return self.list(BusinessEntity.tenant_id == tenant_id)

    def find_by_name(self, name: str) -> Sequence[BusinessEntity]:
        return self.list(BusinessEntity.name.ilike(f"%{name}%"))

    def find_hidden(self, *, hidden: bool = True) -> Sequence[BusinessEntity]:
        return self.list(BusinessEntity.hidden == hidden)

    def search(self, term: str) -> Sequence[BusinessEntity]:
        stmt = select(BusinessEntity).where(
            BusinessEntity.name.ilike(f"%{term}%")
            | BusinessEntity.description.ilike(f"%{term}%")
            | BusinessEntity.query.ilike(f"%{term}%")
        )
        values: Any = self._session.scalars(stmt).all()
        return cast(Sequence[BusinessEntity], values)

    def upsert(
        self,
        name: str,
        tenant_id: Any = None,
        description: str = "",
        query: str = "",
        *,
        shared_with_customers: bool = False,
    ) -> BusinessEntity:
        existing = self.list(BusinessEntity.name == name, BusinessEntity.tenant_id == tenant_id)
        if existing:
            instance = existing[0]
            instance.description = description
            instance.query = query
            instance.shared_with_customers = shared_with_customers
            self._session.flush()
            return instance
        return self.create(
            name=name,
            tenant_id=tenant_id,
            description=description,
            query=query,
            shared_with_customers=shared_with_customers,
        )


class MetricDefinitionRepository(BaseRepository[MetricDefinition]):
    def __init__(self, session: Session) -> None:
        super().__init__(session, MetricDefinition)

    def find_by_business_entity(self, business_entity_id: Any) -> Sequence[MetricDefinition]:
        return self.list(MetricDefinition.business_entity_id == business_entity_id)

    def find_by_item_id(self, item_id: Any) -> Sequence[MetricDefinition]:
        return self.list(MetricDefinition.item_id == item_id)

    def find_by_tenant(self, tenant_id: Any) -> Sequence[MetricDefinition]:
        return self.list(MetricDefinition.tenant_id == tenant_id)

    def find_by_name(self, name: str) -> Sequence[MetricDefinition]:
        return self.list(MetricDefinition.name.ilike(f"%{name}%"))

    def find_outdated(self) -> Sequence[MetricDefinition]:
        return self.list(MetricDefinition.is_outdated.is_(True))

    def upsert(
        self,
        name: str,
        business_entity_id: Any,
        item_id: Any,
        item_name: str,
        tenant_id: Any,
        user_input: str = "",
        description: str = "",
        how_to_calculate: str = "",
    ) -> MetricDefinition:
        existing = self.list(
            MetricDefinition.business_entity_id == business_entity_id,
            MetricDefinition.item_id == item_id,
        )
        if existing:
            instance = existing[0]
            instance.name = name
            instance.item_name = item_name
            instance.user_input = user_input
            instance.description = description
            instance.how_to_calculate = how_to_calculate
            instance.updated_ts = func.extract("epoch", func.now()).cast(Integer)
            self._session.flush()
            return instance
        return self.create(
            name=name,
            business_entity_id=business_entity_id,
            item_id=item_id,
            item_name=item_name,
            tenant_id=tenant_id,
            user_input=user_input,
            description=description,
            how_to_calculate=how_to_calculate,
        )


class EntityRelationRepository(BaseRepository[EntityRelation]):
    def __init__(self, session: Session) -> None:
        super().__init__(session, EntityRelation)

    def find_by_business_entity(self, business_entity_id: Any) -> Sequence[EntityRelation]:
        return self.list(EntityRelation.business_entity_id == business_entity_id)

    def find_by_related_entity(self, related_entity_id: Any) -> Sequence[EntityRelation]:
        return self.list(EntityRelation.related_entity_id == related_entity_id)

    def find_by_name(self, name: str) -> Sequence[EntityRelation]:
        return self.list(EntityRelation.name == name)

    def find_by_direction(self, direction: str) -> Sequence[EntityRelation]:
        return self.list(EntityRelation.direction == direction)

    def find_pair(
        self,
        business_entity_id: Any,
        related_entity_id: Any,
        name: str = "",
        direction: str = "",
    ) -> EntityRelation | None:
        stmt = select(EntityRelation).where(
            EntityRelation.business_entity_id == business_entity_id,
            EntityRelation.related_entity_id == related_entity_id,
            EntityRelation.name == name,
            EntityRelation.direction == direction,
        )
        value: Any = self._session.scalars(stmt).first()
        return cast(EntityRelation | None, value)

    def find_enabled(self) -> Sequence[EntityRelation]:
        return self.list(EntityRelation.enabled.is_(True))


class PredictionModelRepository(BaseRepository[PredictionModel]):
    def __init__(self, session: Session) -> None:
        super().__init__(session, PredictionModel)

    def find_by_business_entity(self, business_entity_id: Any) -> Sequence[PredictionModel]:
        return self.list(PredictionModel.business_entity_id == business_entity_id)

    def find_by_field(self, business_entity_field_id: Any) -> Sequence[PredictionModel]:
        return self.list(PredictionModel.business_entity_field_id == business_entity_field_id)

    def find_by_type(self, type_: str) -> Sequence[PredictionModel]:
        return self.list(PredictionModel.type == type_)

    def find_by_status(self, status: str) -> Sequence[PredictionModel]:
        return self.list(PredictionModel.status == status)

    def find_enabled(self) -> Sequence[PredictionModel]:
        return self.list(PredictionModel.enabled.is_(True))

    def find_by_tenant_customer(
        self, tenant_id: Any, customer_id: Any
    ) -> Sequence[PredictionModel]:
        return self.list(
            PredictionModel.tenant_id == tenant_id,
            PredictionModel.customer_id == customer_id,
        )

    def find_champion(self, entity_id: Any, metric_key: str) -> PredictionModel | None:
        models = self.find_by_business_entity(entity_id)
        candidates = [
            model
            for model in models
            if model.tb_telemetry_key == metric_key and model.status == "champion"
        ]
        if not candidates:
            return None
        return max(candidates, key=lambda model: model.created_ts)

    def find_challengers(self, entity_id: Any, metric_key: str) -> Sequence[PredictionModel]:
        models = self.find_by_business_entity(entity_id)
        return [
            model
            for model in models
            if model.tb_telemetry_key == metric_key and model.status == "challenger"
        ]

    def record_status_transition(
        self,
        model_id: Any,
        entity_id: Any,
        metric_key: str,
        previous_status: str | None,
        new_status: str,
        changed_ts: int,
    ) -> PredictionModelStatusHistory:
        transition = PredictionModelStatusHistory(
            prediction_model_id=model_id,
            business_entity_id=entity_id,
            metric_key=metric_key,
            previous_status=previous_status,
            new_status=new_status,
            changed_ts=changed_ts,
        )
        self._session.add(transition)
        self._session.flush()
        return transition

    def find_previous_champion(
        self,
        entity_id: Any,
        metric_key: str,
        current_model_id: Any,
    ) -> PredictionModel | None:
        try:
            history = self._session.scalars(
                select(PredictionModelStatusHistory)
                .where(
                    PredictionModelStatusHistory.business_entity_id == entity_id,
                    PredictionModelStatusHistory.metric_key == metric_key,
                    PredictionModelStatusHistory.new_status == "champion",
                    PredictionModelStatusHistory.prediction_model_id != current_model_id,
                )
                .order_by(PredictionModelStatusHistory.changed_ts.desc())
            ).all()
        except ProgrammingError as exc:
            pgcode = getattr(exc.orig, "pgcode", None)
            if pgcode != "42P01":
                raise
            logger.warning(
                "Model status history unavailable; rollback is disabled until migration 006 is applied"
            )
            return None
        for transition in history:
            model = self.get(transition.prediction_model_id)
            if model is not None:
                return model
        return None

    def set_champion(
        self, model_id: Any, entity_id: Any, metric_key: str
    ) -> PredictionModel | None:
        models = self.find_by_business_entity(entity_id)
        promoted = next(
            (
                model
                for model in models
                if model.id == model_id and model.tb_telemetry_key == metric_key
            ),
            None,
        )
        if promoted is None:
            return None
        for model in models:
            if model.tb_telemetry_key == metric_key and model.id != model_id:
                if model.status == "champion":
                    model.status = "challenger"
        promoted.status = "champion"
        return promoted


# TODO: AnomalyDetector table does not exist in Trendz 1.15.0 schema.
# Use cluster_model for anomaly-related configuration.
class AnomalyDetectorRepository(BaseRepository[Any]):
    def __init__(self, session: Session) -> None:
        super().__init__(session, cast(type[Any], None))

    def find_by_entity_metric(self, entity_id: Any, metric_key: str) -> Sequence[Any]:
        return []

    def find_by_status(self, status: str) -> Sequence[Any]:
        return []

    def find_by_algorithm(self, algorithm: str) -> Sequence[Any]:
        return []


# TODO: Checkpoint table does not exist in Trendz 1.15.0 schema.
# Use trendx checkpoint tables or external watermark store.
class CheckpointRepository(BaseRepository[Any]):
    def __init__(self, session: Session) -> None:
        super().__init__(session, cast(type[Any], None))

    def get_watermark(
        self, pipeline: str, entity_id: Any, metric_key: str | None = None
    ) -> Any | None:
        row = self._session.execute(
            text(
                "SELECT watermark_ts, records_processed, last_batch_id "
                "FROM trendx_catalog.ingestion_checkpoints "
                "WHERE pipeline = :pipeline AND entity_id = :entity_id AND metric_key = :metric_key"
            ),
            {"pipeline": pipeline, "entity_id": str(entity_id), "metric_key": metric_key},
        ).fetchone()
        if row is None:
            return None
        return {"watermark_ts": row[0], "records_processed": row[1], "last_batch_id": row[2]}

    def upsert_watermark(
        self,
        pipeline: str,
        entity_id: Any,
        watermark_ts: Any,
        metric_key: str | None = None,
        records_processed: int = 0,
        source: str | None = None,
        last_batch_id: str | None = None,
    ) -> Any:
        self._session.execute(
            text(
                "INSERT INTO trendx_catalog.ingestion_checkpoints "
                "  (pipeline, entity_id, metric_key, watermark_ts, records_processed, last_batch_id, source, updated_at) "
                "VALUES (:pipeline, :entity_id, :metric_key, :watermark_ts, :records_processed, :last_batch_id, :source, now()) "
                "ON CONFLICT (pipeline, entity_id, metric_key) DO UPDATE SET "
                "  watermark_ts = EXCLUDED.watermark_ts, "
                "  records_processed = EXCLUDED.records_processed, "
                "  last_batch_id = EXCLUDED.last_batch_id, "
                "  source = EXCLUDED.source, "
                "  updated_at = now()"
            ),
            {
                "pipeline": pipeline,
                "entity_id": str(entity_id),
                "metric_key": metric_key,
                "watermark_ts": watermark_ts,
                "records_processed": records_processed,
                "last_batch_id": last_batch_id,
                "source": source,
            },
        )
        return {"pipeline": pipeline, "entity_id": str(entity_id), "metric_key": metric_key}

    def list_by_pipeline(self, pipeline: str) -> Sequence[Any]:
        rows = self._session.execute(
            text(
                "SELECT entity_id, metric_key, watermark_ts, records_processed "
                "FROM trendx_catalog.ingestion_checkpoints "
                "WHERE pipeline = :pipeline"
            ),
            {"pipeline": pipeline},
        ).fetchall()
        return [
            {
                "entity_id": r[0],
                "metric_key": r[1],
                "watermark_ts": r[2],
                "records_processed": r[3],
            }
            for r in rows
        ]


class TrendzTaskRepository(BaseRepository[TrendzTask]):
    def __init__(self, session: Session) -> None:
        super().__init__(session, TrendzTask)

    def find_enabled(self) -> Sequence[TrendzTask]:
        return self.list(TrendzTask.enabled.is_(True))

    def find_by_job_type(self, job_type: str) -> Sequence[TrendzTask]:
        return self.list(TrendzTask.job_type == job_type)

    def find_by_reference(self, reference_type: str, reference_key: str) -> TrendzTask | None:
        stmt = select(TrendzTask).where(
            TrendzTask.reference_type == reference_type,
            TrendzTask.reference_key == reference_key,
        )
        value: Any = self._session.scalars(stmt).first()
        return cast(TrendzTask | None, value)

    def find_by_schedule_type(self, schedule_type: str) -> Sequence[TrendzTask]:
        return self.list(TrendzTask.schedule_type == schedule_type)

    def find_scheduled_before(self, before_ts: int) -> Sequence[TrendzTask]:
        return self.list(TrendzTask.schedule_planned_ts < before_ts)

    # TODO: task_type, priority, scheduled_at, started_at, finished_at,
    # duration_ms, claimed_by, retry_count, max_retries, payload, result,
    # error_message do not exist in real trendz_task schema.
    # These methods are kept as no-ops / stubs for backward compatibility.

    def find_pending(self, task_type: str | None = None, limit: int = 10) -> Sequence[TrendzTask]:
        return []

    def claim(self, task_id: Any, claimed_by: str) -> TrendzTask | None:
        return None

    def complete(
        self, task_id: Any, result: Any = None, error_message: str | None = None
    ) -> TrendzTask | None:
        return None

    def mark_retry(self, task_id: Any) -> TrendzTask | None:
        return None

    def find_by_type(self, task_type: str) -> Sequence[TrendzTask]:
        return []

    def find_stuck(self, timeout_minutes: int = 30) -> Sequence[TrendzTask]:
        return []


# TODO: AlertRule table does not exist in Trendz 1.15.0 schema.
# TrendX-owned (Étape 013). entity_id est TEXT (device TB id), sans FK.
class AlertRuleRepository(BaseRepository[AlertRule]):
    def __init__(self, session: Session) -> None:
        super().__init__(session, AlertRule)

    def find_active(self) -> Sequence[AlertRule]:
        return self.list(AlertRule.is_active.is_(True))

    def find_by_entity(self, entity_id: Any) -> Sequence[AlertRule]:
        if entity_id is None:
            return []
        return self.list(AlertRule.entity_id == str(entity_id))

    def find_by_metric(self, metric_key: str) -> Sequence[AlertRule]:
        return self.list(AlertRule.metric_key == metric_key)

    def find_by_rule_type(self, rule_type: str) -> Sequence[AlertRule]:
        return self.list(AlertRule.rule_type == rule_type)


# TODO: AlertIncident table does not exist in Trendz 1.15.0 schema.
# TrendX-owned (Étape 013). Idempotent via logical_key unique.
class AlertIncidentRepository(BaseRepository[AlertIncident]):
    def __init__(self, session: Session) -> None:
        super().__init__(session, AlertIncident)

    def find_active(self, entity_id: Any | None = None) -> Sequence[AlertIncident]:
        stmt = select(AlertIncident).where(AlertIncident.status.in_(("ACTIVE", "ACK")))
        if entity_id is not None:
            stmt = stmt.where(AlertIncident.entity_id == str(entity_id))
        return self._session.scalars(stmt).all()

    def find_by_logical_key(self, logical_key: str) -> AlertIncident | None:
        stmt = select(AlertIncident).where(AlertIncident.logical_key == logical_key).limit(1)
        return self._session.scalars(stmt).first()

    @staticmethod
    def _to_uuid(value: Any) -> uuid.UUID | None:
        if value is None:
            return None
        if isinstance(value, uuid.UUID):
            return value
        try:
            return uuid.UUID(str(value))
        except (ValueError, AttributeError):
            return None

    def open_or_update(
        self,
        logical_key: str,
        rule_id: Any,
        entity_id: Any,
        severity: str,
        metric_key: str | None = None,
        open_reason: str | None = None,
        last_value: float | None = None,
    ) -> AlertIncident:
        existing = self.find_by_logical_key(logical_key)
        rid = self._to_uuid(rule_id)
        now = datetime.now(UTC)
        if existing is None:
            incident = AlertIncident(
                logical_key=logical_key,
                rule_id=rid,
                entity_id=str(entity_id),
                metric_key=metric_key,
                severity=severity,
                status="ACTIVE",
                open_reason=open_reason,
                last_value=last_value,
                opened_count=1,
                opened_at=now,
            )
            self._session.add(incident)
            self._session.flush()
            return incident
        # Re-open depuis CLEARED/CLOSED : on incrémente opened_count et on
        # réinitialise les champs de cycle de vie. Refresh d'un incident
        # ACTIVE/ACK : on conserve opened_at/ouverture, on met à jour le reste.
        if existing.status in ("CLEARED", "CLOSED"):
            existing.opened_count = (existing.opened_count or 0) + 1
            existing.opened_at = now
            existing.cleared_at = None
            existing.closed_at = None
            existing.acknowledged_at = None
            existing.acknowledged_by = None
        existing.status = "ACTIVE"
        existing.rule_id = rid
        existing.entity_id = str(entity_id)
        existing.metric_key = metric_key
        existing.severity = severity
        existing.open_reason = open_reason
        existing.last_value = last_value
        self._session.flush()
        return existing

    def acknowledge(self, logical_key: str, acknowledged_by: str) -> AlertIncident | None:
        existing = self.find_by_logical_key(logical_key)
        if existing is None:
            return None
        existing.status = "ACK"
        existing.acknowledged_at = datetime.now(UTC)
        existing.acknowledged_by = acknowledged_by
        self._session.flush()
        return existing

    def clear(self, logical_key: str, close_reason: str | None = None) -> AlertIncident | None:
        existing = self.find_by_logical_key(logical_key)
        if existing is None:
            return None
        existing.status = "CLEARED"
        existing.cleared_at = datetime.now(UTC)
        existing.close_reason = close_reason
        self._session.flush()
        return existing

    def close(self, logical_key: str, close_reason: str | None = None) -> AlertIncident | None:
        existing = self.find_by_logical_key(logical_key)
        if existing is None:
            return None
        existing.status = "CLOSED"
        existing.closed_at = datetime.now(UTC)
        existing.close_reason = close_reason
        self._session.flush()
        return existing


# TrendX-owned (Étape 013). Mappe trendx_analytics.predictions (migration 011).
# Upsert basé sur la PK composite (ts, entity_id, metric_key,
# forecast_generated_at, model_id). Aucune table supplémentaire.
class PredictionRepository(BaseRepository[Prediction]):
    def __init__(self, session: Session) -> None:
        super().__init__(session, Prediction)

    def bulk_upsert(self, rows: list[dict[str, Any]]) -> int:
        if not rows:
            return 0
        stmt = pg_insert(cast(Table, Prediction.__table__)).values(rows)
        update_cols = {
            "model_used": stmt.excluded.model_used,
            "value": stmt.excluded.value,
            "lower_bound": stmt.excluded.lower_bound,
            "upper_bound": stmt.excluded.upper_bound,
            "horizon_step": stmt.excluded.horizon_step,
            "frequency": stmt.excluded.frequency,
            "written_back": stmt.excluded.written_back,
            "writeback_key": stmt.excluded.writeback_key,
            "created_at": stmt.excluded.created_at,
        }
        stmt = stmt.on_conflict_do_update(
            index_elements=[
                "ts",
                "entity_id",
                "metric_key",
                "forecast_generated_at",
                "model_id",
            ],
            set_=update_cols,
        )
        self._session.execute(stmt)
        self._session.flush()
        return len(rows)


# TODO: WritebackBatch table does not exist in Trendz 1.15.0 schema.
class WritebackBatchRepository(BaseRepository[Any]):
    def __init__(self, session: Session) -> None:
        super().__init__(session, cast(type[Any], None))

    def find_recent(self, entity_id: Any, metric_key: str, limit: int = 10) -> Sequence[Any]:
        return []

    def find_by_status(self, status: str) -> Sequence[Any]:
        return []

    def mark_completed(self, batch_id: Any, points_written: int) -> Any | None:
        return None

    def mark_failed(self, batch_id: Any, error_message: str) -> Any | None:
        return None


class ViewConfigRepository(BaseRepository[ViewConfig]):
    def __init__(self, session: Session) -> None:
        super().__init__(session, ViewConfig)

    def find_by_tenant_customer(self, tenant_id: Any, customer_id: Any) -> Sequence[ViewConfig]:
        return self.list(
            ViewConfig.tenant_id == tenant_id,
            ViewConfig.customer_id == customer_id,
        )

    def find_by_collection(self, collection_id: Any) -> Sequence[ViewConfig]:
        return self.list(ViewConfig.collection_id == collection_id)

    def find_public(self) -> Sequence[ViewConfig]:
        return []

    def find_favorite(self) -> Sequence[ViewConfig]:
        return self.list(ViewConfig.is_favorite.is_(True))

    def find_by_view_type(self, view_type: str) -> Sequence[ViewConfig]:
        return self.list(ViewConfig.view_type == view_type)

    # TODO: owner_entity_id, is_favourite renamed to root_entity_id, is_favorite

    def find_by_owner(self, owner_entity_id: Any) -> Sequence[ViewConfig]:
        return self.list(ViewConfig.root_entity_id == owner_entity_id)

    def find_favourite(self) -> Sequence[ViewConfig]:
        return self.find_favorite()


TERMINAL_RUN_STATUSES = ("ok", "failed")


def normalize_terminal_status(status: str) -> str:
    """Normalise le statut terminal interne du registre vers le contrat DB.

    "fail" (TriggerRecord) -> "failed", "ok" inchangé. Tout autre statut lève
    ValueError : aucun nouvel état n'est inventé.
    """
    if status == "ok":
        return "ok"
    if status in ("fail", "failed"):
        return "failed"
    raise ValueError(f"Unknown terminal run status: {status!r} (expected 'ok' or 'failed')")


class SchedulerRunRepository(BaseRepository[SchedulerRun]):
    """Persistance des runs scheduler P0 (migration 014, base trendx uniquement).

    Les commits restent au niveau appelant (pattern existant) ; chaque méthode
    flush. Aucun couplage ThingsBoard, aucun effet scheduler implicite.
    """

    def __init__(self, session: Session) -> None:
        super().__init__(session, SchedulerRun)

    def create_run(
        self,
        *,
        run_id: str | uuid.UUID,
        job_id: str,
        job_type: str,
        instance_id: str,
        mode: str = "direct",
        task_id: str | uuid.UUID | None = None,
        triggered_at: datetime | None = None,
    ) -> SchedulerRun:
        """Crée une ligne running. finished_at reste NULL (invariant)."""
        row = self.create(
            run_id=uuid.UUID(str(run_id)),
            job_id=job_id,
            job_type=job_type,
            status="running",
            triggered_at=triggered_at or datetime.now(UTC),
            finished_at=None,
            error=None,
            instance_id=instance_id,
            mode=mode,
            task_id=uuid.UUID(str(task_id)) if task_id is not None else None,
        )
        return row

    def mark_terminal(
        self,
        run_id: str | uuid.UUID,
        status: str,
        error: str | None = None,
        finished_at: datetime | None = None,
    ) -> SchedulerRun:
        """running -> ok | failed ("fail" normalisé en "failed").

        Lève KeyError si la ligne est inconnue, ValueError si elle n'est plus
        running (pas de double transition terminale) ou si le statut est
        inconnu. finished_at par défaut : maintenant UTC.
        """
        row = self.get(uuid.UUID(str(run_id)))
        if row is None:
            raise KeyError(f"Unknown scheduler run: {run_id}")
        if row.status != "running":
            raise ValueError(
                f"Refusing terminal transition for run {run_id}: status is {row.status!r}"
            )
        row.status = normalize_terminal_status(status)
        row.error = error
        row.finished_at = finished_at or datetime.now(UTC)
        self._session.flush()
        return row

    def record_heartbeat(
        self,
        *,
        instance_id: str,
        leader: bool,
        version: str = "",
        host: str = "",
    ) -> SchedulerHeartbeat:
        """Upsert du heartbeat par instance_id (heartbeat_ts = maintenant UTC)."""
        stmt = (
            pg_insert(SchedulerHeartbeat)
            .values(
                instance_id=instance_id,
                leader=leader,
                heartbeat_ts=datetime.now(UTC),
                version=version,
                host=host,
            )
            .on_conflict_do_update(
                index_elements=["instance_id"],
                set_={
                    "leader": leader,
                    "heartbeat_ts": datetime.now(UTC),
                    "version": version,
                    "host": host,
                },
            )
        )
        self._session.execute(stmt)
        self._session.flush()
        row = self._session.get(SchedulerHeartbeat, instance_id)
        if row is None:  # pragma: no cover - upsert ci-dessus garantit la ligne
            raise RuntimeError(f"Heartbeat upsert failed for instance {instance_id!r}")
        return row

    def purge_before(self, cutoff: datetime) -> int:
        """Supprime les runs terminaux (ok/failed) antérieurs au cutoff.

        Ne touche jamais aux lignes running (reprise crash + tâches enqueue
        en attente). Retourne le nombre de lignes supprimées.
        """
        result = self._session.execute(
            delete(SchedulerRun).where(
                SchedulerRun.status.in_(TERMINAL_RUN_STATUSES),
                SchedulerRun.triggered_at < cutoff,
            )
        )
        self._session.flush()
        return int(result.rowcount or 0)

    def list_stale_running(
        self,
        older_than: datetime,
        modes: tuple[str, ...] = ("direct",),
    ) -> Sequence[SchedulerRun]:
        """Lignes running + anciennes, limitées aux modes donnés.

        Par défaut seuls les runs `direct` sont éligibles : une ligne
        `enqueue` running correspond à une tâche en attente d'exécution
        (worker claim), pas à un crash — elle ne doit pas être reprise ici.
        """
        return self.list(
            SchedulerRun.status == "running",
            SchedulerRun.triggered_at < older_than,
            SchedulerRun.mode.in_(modes),
            order_by=SchedulerRun.triggered_at,
        )
