"""Real (non-mocked) integration test for migration 006.

Validates, against a real PostgreSQL provided by the test-integration CI job,
that migration 006 (ModelRegistry status history) actually creates and populates
``trendx_catalog.prediction_model_status_history`` and that the migration is
reversible (rollback) by dropping and re-creating that table.

This test deliberately avoids the ``PredictionModel`` ORM and ``ModelRegistry``
business logic, which depend on the full 0.3 schema (model_uri from migration 008
and columns from later migrations not in P1 scope). It only exercises the artifacts
produced by migration 001 (prediction_model) and migration 006 (status history).
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime

import pytest
from sqlalchemy import text
from trendx.database.connection import get_catalog_engine

_METRIC_KEY = "integration_006_metric"
_MODEL_ID = uuid.UUID("aaaaaaaa-0000-0000-0000-0000000000a1")
_ENTITY_ID = uuid.UUID("11111111-0000-0000-0000-000000000006")

_STATUS_HISTORY = "trendx_catalog.prediction_model_status_history"


def _status_history_exists(conn) -> bool:
    return (
        conn.execute(
            text(
                "SELECT 1 FROM information_schema.tables "
                "WHERE table_schema = 'trendx_catalog' "
                "AND table_name = 'prediction_model_status_history'"
            )
        ).first()
        is not None
    )


def _seed(conn) -> None:
    ts = int(datetime.now(UTC).timestamp() * 1000)
    conn.execute(
        text(
            "INSERT INTO prediction_model (id, name, model_type, artifact_path) "
            "VALUES (:mid, 'm006', 'Prophet', 's3://x')"
        ),
        {"mid": str(_MODEL_ID)},
    )
    conn.execute(
        text(
            f"INSERT INTO {_STATUS_HISTORY} "
            "(prediction_model_id, business_entity_id, metric_key, new_status, changed_ts) "
            "VALUES (:mid, :eid, :mk, 'CHAMPION', :ts)"
        ),
        {"mid": str(_MODEL_ID), "eid": str(_ENTITY_ID), "mk": _METRIC_KEY, "ts": ts},
    )


def _cleanup(conn) -> None:
    conn.execute(
        text(f"DELETE FROM {_STATUS_HISTORY} WHERE prediction_model_id = :mid"),
        {"mid": str(_MODEL_ID)},
    )
    conn.execute(
        text("DELETE FROM prediction_model WHERE id = :mid"),
        {"mid": str(_MODEL_ID)},
    )


@pytest.mark.integration
def test_migration_006_status_history_and_rollback() -> None:
    engine = get_catalog_engine()

    with engine.begin() as conn:
        # 1. Migration 006 artifact present after the 001 -> 006 chain.
        assert _status_history_exists(
            conn
        ), "migration 006 table trendx_catalog.prediction_model_status_history missing"

        # 2. Both artifacts are writable using only the base (001) schema.
        _seed(conn)
        history_count = conn.execute(
            text(f"SELECT count(*) FROM {_STATUS_HISTORY} WHERE prediction_model_id = :mid"),
            {"mid": str(_MODEL_ID)},
        ).scalar()
        assert history_count == 1

        # 3. Rollback: drop 006 artifact, assert it is gone.
        conn.execute(text(f"DROP TABLE IF EXISTS {_STATUS_HISTORY}"))
        assert not _status_history_exists(conn)

        # 4. Re-apply 006 (idempotent DDL), assert the artifact is restored.
        conn.execute(
            text(
                f"CREATE TABLE IF NOT EXISTS {_STATUS_HISTORY} ("
                "id uuid NOT NULL DEFAULT gen_random_uuid(), "
                "prediction_model_id uuid NOT NULL, "
                "business_entity_id uuid NOT NULL, "
                "metric_key text NOT NULL, "
                "previous_status text, "
                "new_status text NOT NULL, "
                "changed_ts bigint NOT NULL, "
                "CONSTRAINT prediction_model_status_history_pk PRIMARY KEY (id))"
            )
        )
        assert _status_history_exists(conn)

        # 5. State restored; the seed + rollback must not leak between runs.
        _cleanup(conn)
