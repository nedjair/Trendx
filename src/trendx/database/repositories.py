from __future__ import annotations

from typing import Any, Generic, Optional, Sequence, TypeVar

from loguru import logger
from sqlalchemy import func, select, update
from sqlalchemy.orm import Session

from trendx.database.models import (
    BusinessEntity,
    EntityRelation,
    MetricDefinition,
    PredictionModel,
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

    def get(self, ident: Any) -> Optional[T]:
        return self._session.get(self._model, ident)

    def update(self, ident: Any, **kwargs: Any) -> Optional[T]:
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
        return result.rowcount  # type: ignore[return-value]

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
        return result.rowcount  # type: ignore[return-value]

    def list(
        self,
        *filters: Any,
        order_by: Optional[Any] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
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
        return self._session.scalars(stmt).all()

    def count(self, *filters: Any) -> int:
        stmt = select(func.count(self._model.id)).select_from(self._model)
        if filters:
            stmt = stmt.where(*filters)
        result = self._session.execute(stmt)
        return result.scalar_one()  # type: ignore[return-value]

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

    def find_hidden(self, hidden: bool = True) -> Sequence[BusinessEntity]:
        return self.list(BusinessEntity.hidden == hidden)

    def search(self, term: str) -> Sequence[BusinessEntity]:
        stmt = select(BusinessEntity).where(
            BusinessEntity.name.ilike(f"%{term}%")
            | BusinessEntity.description.ilike(f"%{term}%")
            | BusinessEntity.query.ilike(f"%{term}%")
        )
        return self._session.scalars(stmt).all()

    def upsert(
        self,
        name: str,
        tenant_id: Any = None,
        description: str = "",
        query: str = "",
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
    ) -> Optional[EntityRelation]:
        stmt = select(EntityRelation).where(
            EntityRelation.business_entity_id == business_entity_id,
            EntityRelation.related_entity_id == related_entity_id,
            EntityRelation.name == name,
            EntityRelation.direction == direction,
        )
        return self._session.scalars(stmt).first()

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

    # TODO: is_champion column does not exist in real prediction_model table.
    # Champion selection must be managed via application logic or a separate registry.
    def find_champion(self, entity_id: Any, metric_key: str) -> Optional[PredictionModel]:
        return None

    def find_challengers(self, entity_id: Any, metric_key: str) -> Sequence[PredictionModel]:
        return []

    def set_champion(self, model_id: Any, entity_id: Any, metric_key: str) -> Optional[PredictionModel]:
        return None


# TODO: AnomalyDetector table does not exist in Trendz 1.15.0 schema.
# Use cluster_model for anomaly-related configuration.
class AnomalyDetectorRepository(BaseRepository):
    def __init__(self, session: Session) -> None:
        super().__init__(session, None)

    def find_by_entity_metric(self, entity_id: Any, metric_key: str) -> Sequence[Any]:
        return []

    def find_by_status(self, status: str) -> Sequence[Any]:
        return []

    def find_by_algorithm(self, algorithm: str) -> Sequence[Any]:
        return []


# TODO: Checkpoint table does not exist in Trendz 1.15.0 schema.
# Use trendx checkpoint tables or external watermark store.
class CheckpointRepository(BaseRepository):
    def __init__(self, session: Session) -> None:
        super().__init__(session, None)

    def get_watermark(
        self, pipeline: str, entity_id: Any, metric_key: Optional[str] = None
    ) -> Optional[Any]:
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
        metric_key: Optional[str] = None,
        records_processed: int = 0,
        source: Optional[str] = None,
        last_batch_id: Optional[str] = None,
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

    def find_by_reference(
        self, reference_type: str, reference_key: str
    ) -> Optional[TrendzTask]:
        stmt = select(TrendzTask).where(
            TrendzTask.reference_type == reference_type,
            TrendzTask.reference_key == reference_key,
        )
        return self._session.scalars(stmt).first()

    def find_by_schedule_type(self, schedule_type: str) -> Sequence[TrendzTask]:
        return self.list(TrendzTask.schedule_type == schedule_type)

    def find_scheduled_before(self, before_ts: int) -> Sequence[TrendzTask]:
        return self.list(TrendzTask.schedule_planned_ts < before_ts)

    # TODO: task_type, priority, scheduled_at, started_at, finished_at,
    # duration_ms, claimed_by, retry_count, max_retries, payload, result,
    # error_message do not exist in real trendz_task schema.
    # These methods are kept as no-ops / stubs for backward compatibility.

    def find_pending(self, task_type: Optional[str] = None, limit: int = 10) -> Sequence[TrendzTask]:
        return []

    def claim(self, task_id: Any, claimed_by: str) -> Optional[TrendzTask]:
        return None

    def complete(
        self, task_id: Any, result: Any = None, error_message: Optional[str] = None
    ) -> Optional[TrendzTask]:
        return None

    def mark_retry(self, task_id: Any) -> Optional[TrendzTask]:
        return None

    def find_by_type(self, task_type: str) -> Sequence[TrendzTask]:
        return []

    def find_stuck(self, timeout_minutes: int = 30) -> Sequence[TrendzTask]:
        return []


# TODO: AlertRule table does not exist in Trendz 1.15.0 schema.
class AlertRuleRepository(BaseRepository):
    def __init__(self, session: Session) -> None:
        super().__init__(session, None)

    def find_active(self) -> Sequence[Any]:
        return []

    def find_by_entity(self, entity_id: Any) -> Sequence[Any]:
        return []

    def find_by_metric(self, metric_key: str) -> Sequence[Any]:
        return []

    def find_by_rule_type(self, rule_type: str) -> Sequence[Any]:
        return []


# TODO: AlertIncident table does not exist in Trendz 1.15.0 schema.
class AlertIncidentRepository(BaseRepository):
    def __init__(self, session: Session) -> None:
        super().__init__(session, None)

    def find_active(self, entity_id: Optional[Any] = None) -> Sequence[Any]:
        return []

    def find_by_logical_key(self, logical_key: str) -> Optional[Any]:
        return None

    def open_or_update(
        self,
        logical_key: str,
        rule_id: Any,
        entity_id: Any,
        severity: str,
        metric_key: Optional[str] = None,
        open_reason: Optional[str] = None,
        last_value: Optional[float] = None,
    ) -> Any:
        return None

    def acknowledge(self, logical_key: str, acknowledged_by: str) -> Optional[Any]:
        return None

    def clear(self, logical_key: str, close_reason: Optional[str] = None) -> Optional[Any]:
        return None

    def close(self, logical_key: str, close_reason: Optional[str] = None) -> Optional[Any]:
        return None


# TODO: WritebackBatch table does not exist in Trendz 1.15.0 schema.
class WritebackBatchRepository(BaseRepository):
    def __init__(self, session: Session) -> None:
        super().__init__(session, None)

    def find_recent(
        self, entity_id: Any, metric_key: str, limit: int = 10
    ) -> Sequence[Any]:
        return []

    def find_by_status(self, status: str) -> Sequence[Any]:
        return []

    def mark_completed(
        self, batch_id: Any, points_written: int
    ) -> Optional[Any]:
        return None

    def mark_failed(self, batch_id: Any, error_message: str) -> Optional[Any]:
        return None


class ViewConfigRepository(BaseRepository[ViewConfig]):
    def __init__(self, session: Session) -> None:
        super().__init__(session, ViewConfig)

    def find_by_tenant_customer(
        self, tenant_id: Any, customer_id: Any
    ) -> Sequence[ViewConfig]:
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
