"""Real (non-mocked) integration test for migration 006.

Validates that the promotion -> rollback cycle of ModelRegistry actually
persists transitions into trendx_catalog.prediction_model_status_history
once migration 006 (and its prerequisite migration 001) has been applied.
Requires a real PostgreSQL (provided by the test-integration CI job).
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime

import pytest
from sqlalchemy import delete, select, text
from sqlalchemy.orm import Session
from trendx.database.connection import get_catalog_engine
from trendx.database.models import PredictionModel
from trendx.mlops.registry import CHALLENGER_STATUS, CHAMPION_STATUS, ModelRegistry

# business_entity_id is a uuid column on prediction_model and on
# prediction_model_status_history. Use a real uuid, not an arbitrary string.
_ENTITY_ID = uuid.UUID("11111111-0000-0000-0000-000000000006")
_METRIC_KEY = "integration_006_metric"
_MODEL_TYPE = "Prophet"

# Required (NOT NULL, no default) columns on prediction_model.
_TENANT_ID = uuid.UUID("22222222-0000-0000-0000-000000000006")
_CUSTOMER_ID = uuid.UUID("33333333-0000-0000-0000-000000000006")
_ASSOCIATED_ENTITY_FIELD_ID = uuid.UUID("44444444-0000-0000-0000-000000000006")
_BUSINESS_ENTITY_FIELD_ID = uuid.UUID("55555555-0000-0000-0000-000000000006")

_MODEL_A_ID = uuid.UUID("aaaaaaaa-0000-0000-0000-0000000000a1")
_MODEL_B_ID = uuid.UUID("bbbbbbbb-0000-0000-0000-0000000000b1")


def _make_row(model_id: str, status: str) -> PredictionModel:
    ts = int(datetime.now(UTC).timestamp() * 1000)
    return PredictionModel(
        id=uuid.UUID(model_id),
        name=f"model-{model_id}",
        type=_MODEL_TYPE,
        tb_telemetry_key=_METRIC_KEY,
        tenant_id=_TENANT_ID,
        customer_id=_CUSTOMER_ID,
        associated_entity_field_id=_ASSOCIATED_ENTITY_FIELD_ID,
        business_entity_id=_ENTITY_ID,
        business_entity_field_id=_BUSINESS_ENTITY_FIELD_ID,
        model_parameters="{}",
        datasource_parameters="{}",
        method_parameters="{}",
        item_state_map="{}",
        trained_item_set="{}",
        status=status,
        enabled=True,
        partial_fit_enabled=False,
        created_ts=ts,
        updated_ts=ts,
    )


def _cleanup(conn) -> None:
    conn.execute(delete(PredictionModel).where(PredictionModel.business_entity_id == _ENTITY_ID))
    conn.commit()


@pytest.mark.integration
def test_promotion_rollback_writes_status_history() -> None:
    engine = get_catalog_engine()
    with engine.connect() as conn:
        _cleanup(conn)
    try:
        registry = ModelRegistry()

        with Session(engine) as session:
            session.add_all(
                [
                    _make_row(str(_MODEL_A_ID), CHAMPION_STATUS),
                    _make_row(str(_MODEL_B_ID), CHALLENGER_STATUS),
                ]
            )
            session.commit()

        # Promote A (baseline champion).
        with Session(engine) as live:
            registry.promote_to_champion(
                str(_ENTITY_ID),
                _METRIC_KEY,
                live.get(PredictionModel, _MODEL_A_ID),
            )
        # Promote B (A -> challenger, B -> champion; transitions recorded).
        with Session(engine) as live:
            registry.promote_to_champion(
                str(_ENTITY_ID),
                _METRIC_KEY,
                live.get(PredictionModel, _MODEL_B_ID),
            )

        with Session(engine) as session:
            states = {
                r.id: r.status
                for r in session.execute(
                    select(PredictionModel).where(PredictionModel.business_entity_id == _ENTITY_ID)
                ).scalars()
            }
        assert states[_MODEL_A_ID] == CHALLENGER_STATUS
        assert states[_MODEL_B_ID] == CHAMPION_STATUS

        # Real rollback: A should become champion again, B challenger.
        rolled_back = registry.rollback(str(_ENTITY_ID), _METRIC_KEY)
        assert rolled_back is not None
        assert rolled_back.id == _MODEL_A_ID

        with Session(engine) as session:
            final_states = {
                r.id: r.status
                for r in session.execute(
                    select(PredictionModel).where(PredictionModel.business_entity_id == _ENTITY_ID)
                ).scalars()
            }
            history_count = session.execute(
                text(
                    "SELECT count(*) FROM trendx_catalog.prediction_model_status_history "
                    "WHERE business_entity_id = :eid AND metric_key = :mk"
                ),
                {"eid": str(_ENTITY_ID), "mk": _METRIC_KEY},
            ).scalar()
        # Migration 006 validated: the history table exists and was written.
        assert history_count >= 2
        # Rollback restored the previous champion and demoted the current one.
        assert final_states[_MODEL_A_ID] == CHAMPION_STATUS
        assert final_states[_MODEL_B_ID] == CHALLENGER_STATUS
    finally:
        with engine.connect() as conn:
            _cleanup(conn)
