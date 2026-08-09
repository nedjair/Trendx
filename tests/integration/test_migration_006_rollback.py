"""Real (non-mocked) integration test for migration 006.

Validates that the promotion -> rollback cycle of ModelRegistry actually
persists transitions into trendx_catalog.prediction_model_status_history
once migration 006 has been applied. Requires a real PostgreSQL (provided by
the test-integration CI job) and is NOT executed in the local unit suite.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from sqlalchemy import delete, select, text
from sqlalchemy.orm import Session
from trendx.database.connection import get_catalog_engine
from trendx.database.models import PredictionModel
from trendx.mlops.registry import CHALLENGER_STATUS, CHAMPION_STATUS, ModelRegistry

_ENTITY_ID = "integration-006-e2e"
_METRIC_KEY = "integration_006_metric"
_MODEL_TYPE = "Prophet"


def _make_row(model_id: str, status: str) -> PredictionModel:
    ts = int(datetime.now(UTC).timestamp() * 1000)
    return PredictionModel(
        id=model_id,
        name=f"model-{model_id}",
        type=_MODEL_TYPE,
        tb_telemetry_key=_METRIC_KEY,
        business_entity_id=_ENTITY_ID,
        status=status,
        enabled=True,
        partial_fit_enabled=False,
        created_ts=ts,
        updated_ts=ts,
    )


def _cleanup(engine) -> None:
    with Session(engine) as session:
        session.execute(
            delete(PredictionModel).where(PredictionModel.business_entity_id == _ENTITY_ID)
        )
        session.commit()


@pytest.mark.integration
def test_promotion_rollback_writes_status_history() -> None:
    engine = get_catalog_engine()
    _cleanup(engine)
    try:
        registry = ModelRegistry()

        model_a = _make_row("aaaaaaaa-0000-0000-0000-0000000000a1", CHAMPION_STATUS)
        model_b = _make_row("bbbbbbbb-0000-0000-0000-0000000000b1", CHALLENGER_STATUS)
        with Session(engine) as session:
            session.add_all([model_a, model_b])
            session.commit()

        # Promote A (baseline champion).
        registry.promote_to_champion(_ENTITY_ID, _METRIC_KEY, model_a)
        # Promote B (A -> challenger, B -> champion; transitions recorded).
        registry.promote_to_champion(_ENTITY_ID, _METRIC_KEY, model_b)

        with Session(engine) as session:
            states = {
                r.id: r.status
                for r in session.execute(
                    select(PredictionModel).where(PredictionModel.business_entity_id == _ENTITY_ID)
                ).scalars()
            }
        assert states[model_a.id] == CHALLENGER_STATUS
        assert states[model_b.id] == CHAMPION_STATUS

        # Real rollback: A should become champion again, B challenger.
        rolled_back = registry.rollback(_ENTITY_ID, _METRIC_KEY)
        assert rolled_back is not None
        assert rolled_back.id == model_a.id

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
                {"eid": _ENTITY_ID, "mk": _METRIC_KEY},
            ).scalar()
        # Migration 006 validated: the history table exists and was written.
        assert history_count >= 2
        # Rollback restored the previous champion and demoted the current one.
        assert final_states[model_a.id] == CHAMPION_STATUS
        assert final_states[model_b.id] == CHALLENGER_STATUS
    finally:
        _cleanup(engine)
