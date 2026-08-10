from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from loguru import logger
from trendx.database.connection import manager as db_manager
from trendx.database.models import PredictionModel
from trendx.database.repositories import PredictionModelRepository

CHAMPION_STATUS = "champion"
CHALLENGER_STATUS = "challenger"


class ModelRegistry:
    """Manages model versions and promotions.

    Works on top of the ``prediction_model`` table (Trendz 1.15.0 schema).
    NOTE: The real table does NOT have columns: entity_id, metric_key,
    algorithm, is_champion, champion_since, trained_at, hyperparameters,
    frequency, horizon, lookback_days, train_metrics, test_metrics.
    Champion selection is managed via application logic / external store.
    """

    def __init__(
        self,
        repository: PredictionModelRepository | None = None,
    ) -> None:
        self._repo = repository

    def _get_repo(self) -> PredictionModelRepository:
        if self._repo is not None:
            return self._repo
        with db_manager.get_session("catalog") as session:
            return PredictionModelRepository(session)

    def register(
        self,
        business_entity_id: str = "",
        business_entity_field_id: str = "",
        model_type: str = "",
        model_uri: str = "",
        tb_telemetry_key: str = "",
        model_parameters: str = "",
        datasource_parameters: str = "",
        method_parameters: str = "",
        item_state_map: str = "",
        trained_item_set: str = "",
        name: str | None = None,
        tenant_id: str | None = None,
        customer_id: str | None = None,
    ) -> PredictionModel:
        with db_manager.get_session("catalog") as session:
            repo = PredictionModelRepository(session)
            model = repo.create(
                name=name or f"{model_type}_{tb_telemetry_key}",
                tenant_id=tenant_id,
                customer_id=customer_id,
                created_ts=int(datetime.now(UTC).timestamp() * 1000),
                updated_ts=int(datetime.now(UTC).timestamp() * 1000),
                enabled=True,
                partial_fit_enabled=False,
                status="active",
                type=model_type,
                associated_entity_field_id=business_entity_field_id,
                tb_telemetry_key=tb_telemetry_key,
                model_parameters=model_parameters,
                datasource_parameters=datasource_parameters,
                method_parameters=method_parameters,
                item_state_map=item_state_map,
                trained_item_set=trained_item_set,
                business_entity_id=business_entity_id,
                business_entity_field_id=business_entity_field_id,
                avoid_disabling=False,
            )
            session.commit()
            logger.info(
                "Registered model {} for entity={} field={} type={}",
                model.id,
                business_entity_id,
                business_entity_field_id,
                model_type,
            )
            return model

    def get_champion(
        self,
        entity_id: str,
        metric_key: str,
    ) -> PredictionModel | None:
        with db_manager.get_session("catalog") as session:
            repo = PredictionModelRepository(session)
            candidates = [
                model
                for model in repo.find_by_business_entity(entity_id)
                if model.tb_telemetry_key == metric_key and model.status == CHAMPION_STATUS
            ]
            if not candidates:
                return None
            return max(candidates, key=lambda model: model.created_ts or 0)

    def get_challenger(
        self,
        entity_id: str,
        metric_key: str,
    ) -> PredictionModel | None:
        with db_manager.get_session("catalog") as session:
            repo = PredictionModelRepository(session)
            candidates = [
                model
                for model in repo.find_by_business_entity(entity_id)
                if model.tb_telemetry_key == metric_key and model.status == CHALLENGER_STATUS
            ]
            if not candidates:
                return None
            return max(candidates, key=lambda model: model.created_ts or 0)

    def promote_to_champion(
        self,
        entity_id: str,
        metric_key: str,
        model_version: PredictionModel,
    ) -> PredictionModel | None:
        with db_manager.get_session("catalog") as session:
            repo = PredictionModelRepository(session)
            models = repo.find_by_business_entity(entity_id)
            current = next(
                (
                    model
                    for model in models
                    if model.tb_telemetry_key == metric_key
                    and model.status == CHAMPION_STATUS
                    and model.id != model_version.id
                ),
                None,
            )
            previous_status = model_version.status
            promoted = repo.set_champion(model_version.id, entity_id, metric_key)
            if promoted is not None:
                changed_ts = int(datetime.now(UTC).timestamp() * 1000)
                if current is not None:
                    repo.record_status_transition(
                        current.id,
                        entity_id,
                        metric_key,
                        CHAMPION_STATUS,
                        CHALLENGER_STATUS,
                        changed_ts,
                    )
                repo.record_status_transition(
                    promoted.id,
                    entity_id,
                    metric_key,
                    previous_status,
                    CHAMPION_STATUS,
                    changed_ts,
                )
                session.commit()
                logger.info(
                    "Promoted model {} to champion for {}/{}",
                    promoted.id,
                    entity_id[:12],
                    metric_key,
                )
            return promoted

    # Validated against a real PostgreSQL (test-integration CI job) via migration 006.
    def rollback(
        self,
        entity_id: str,
        metric_key: str,
    ) -> PredictionModel | None:
        with db_manager.get_session("catalog") as session:
            repo = PredictionModelRepository(session)
            current = repo.find_champion(entity_id, metric_key)
            if current is None:
                logger.warning("No champion to roll back for {}/{}", entity_id[:12], metric_key)
                return None
            previous = repo.find_previous_champion(entity_id, metric_key, current.id)
            if previous is None:
                logger.warning(
                    "No previous champion or status history unavailable for {}/{}",
                    entity_id[:12],
                    metric_key,
                )
                return None
            if previous.status != CHALLENGER_STATUS:
                logger.error(
                    "Refusing rollback for {}/{}: previous champion {} has unexpected status {}",
                    entity_id[:12],
                    metric_key,
                    previous.id,
                    previous.status,
                )
                return None
            changed_ts = int(datetime.now(UTC).timestamp() * 1000)
            current.status = CHALLENGER_STATUS
            previous.status = CHAMPION_STATUS
            repo.record_status_transition(
                current.id,
                entity_id,
                metric_key,
                CHAMPION_STATUS,
                CHALLENGER_STATUS,
                changed_ts,
            )
            repo.record_status_transition(
                previous.id,
                entity_id,
                metric_key,
                CHALLENGER_STATUS,
                CHAMPION_STATUS,
                changed_ts,
            )
            session.commit()
            logger.info(
                "Rolled back champion from {} to {} for {}/{}",
                current.id,
                previous.id,
                entity_id[:12],
                metric_key,
            )
            return previous

    def compare_models(
        self,
        entity_id: str,
        metric_key: str,
    ) -> dict[str, Any]:
        champion = self.get_champion(entity_id, metric_key)
        return {
            "entity_id": entity_id,
            "metric_key": metric_key,
            "champion": {
                "id": str(champion.id) if champion else None,
                "type": champion.type if champion else None,
                "tb_telemetry_key": champion.tb_telemetry_key if champion else None,
                "status": champion.status if champion else None,
            }
            if champion
            else None,
            "challenger": None,
        }

    def list_versions(
        self,
        entity_id: str,
        metric_key: str,
    ) -> list[dict[str, Any]]:
        with db_manager.get_session("catalog") as session:
            repo = PredictionModelRepository(session)
            versions: list[dict[str, Any]] = []
            champion = self.get_champion(entity_id, metric_key)
            if champion is not None:
                versions.append(self._model_to_dict(champion, is_champion=True))
            models = repo.find_by_business_entity(entity_id)
            for model in models:
                if (
                    model.tb_telemetry_key == metric_key
                    and model.status == CHALLENGER_STATUS
                    and (champion is None or model.id != champion.id)
                ):
                    versions.append(self._model_to_dict(model))
            if not versions and champion is None:
                for model in models:
                    if model.tb_telemetry_key == metric_key:
                        versions.append(self._model_to_dict(model))
            return versions

    def get_model_metrics_history(
        self,
        entity_id: str,
        metric_key: str,
        limit: int = 10,
    ) -> list[dict[str, Any]]:
        with db_manager.get_session("catalog") as session:
            repo = PredictionModelRepository(session)
            models = repo.find_by_business_entity(entity_id)
            models_sorted = sorted(models, key=lambda m: m.created_ts or 0, reverse=True)
            return [self._model_to_dict(m) for m in models_sorted[:limit]]

    def store_selection_run(
        self,
        entity_id: str,
        metric_key: str,
        candidates: list[dict[str, Any]],
        champion_id: str | None,
        selection_metric: str,
        champion_score: float | None,
        margin_gain: float | None,
        status: str,
        mlflow_parent_run_id: str | None = None,
    ) -> int:
        # TODO: model_selection_run table does not exist in Trendz 1.15.0 schema.
        logger.warning("store_selection_run called but model_selection_run table does not exist")
        return 0

    @staticmethod
    def _model_to_dict(
        model: PredictionModel,
        *,
        is_champion: bool = False,
    ) -> dict[str, Any]:
        return {
            "id": str(model.id),
            "name": model.name,
            "type": model.type,
            "tb_telemetry_key": model.tb_telemetry_key,
            "business_entity_id": str(model.business_entity_id)
            if model.business_entity_id
            else None,
            "business_entity_field_id": str(model.business_entity_field_id)
            if model.business_entity_field_id
            else None,
            "status": model.status,
            "enabled": model.enabled,
            "partial_fit_enabled": model.partial_fit_enabled,
            "created_ts": model.created_ts,
            "updated_ts": model.updated_ts,
            "is_champion": is_champion,
        }
