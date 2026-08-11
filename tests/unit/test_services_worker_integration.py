from __future__ import annotations

from typing import Any
from unittest.mock import patch

import pytest
from sqlalchemy import text
from trendx.database.connection import get_analytics_engine, get_catalog_engine
from trendx.database.repositories import CheckpointRepository
from trendx.services import scheduler_guards
from trendx.services.scheduler_guards import (
    verify_aggregate_refresh,
    verify_aggregate_rows,
)

_WATERMARK_ROW = {
    "aggregate_name": "hourly",
    "last_refresh_start": "2026-01-01T00:00:00+00:00",
    "last_refresh_rows": 0,
}


def _save_watermark() -> dict[str, Any] | None:
    engine = get_analytics_engine()
    with engine.connect() as conn:
        row = conn.execute(
            text(
                "SELECT aggregate_name, last_refresh_start, last_refresh_rows "
                "FROM aggregate_watermarks WHERE aggregate_name = :agg"
            ),
            {"agg": _WATERMARK_ROW["aggregate_name"]},
        ).fetchone()
        return (
            {"aggregate_name": row[0], "last_refresh_start": row[1], "last_refresh_rows": row[2]}
            if row
            else None
        )


def _restore_watermark(saved: dict[str, Any] | None) -> None:
    if saved is None:
        return
    engine = get_analytics_engine()
    with engine.begin() as conn:
        conn.execute(
            text(
                "UPDATE aggregate_watermarks SET last_refresh_start = :start, "
                "last_refresh_rows = :rows WHERE aggregate_name = :agg"
            ),
            {
                "start": saved["last_refresh_start"],
                "rows": saved["last_refresh_rows"],
                "agg": saved["aggregate_name"],
            },
        )


@pytest.mark.integration
def test_engine_begin_commits_aggregate_watermark() -> None:
    saved = _save_watermark()
    try:
        engine = get_analytics_engine()
        with engine.begin() as conn:
            conn.execute(
                text(
                    "UPDATE aggregate_watermarks SET last_refresh_rows = 42 "
                    "WHERE aggregate_name = :agg"
                ),
                {"agg": _WATERMARK_ROW["aggregate_name"]},
            )
        with engine.connect() as conn:
            row = conn.execute(
                text(
                    "SELECT last_refresh_rows FROM aggregate_watermarks "
                    "WHERE aggregate_name = :agg"
                ),
                {"agg": _WATERMARK_ROW["aggregate_name"]},
            ).scalar_one()
            assert row == 42, f"engine.begin() n'a pas commité (lu {row})"
    finally:
        _restore_watermark(saved)


@pytest.mark.integration
def test_engine_connect_rolls_back_aggregate_watermark() -> None:
    saved = _save_watermark()
    try:
        engine = get_analytics_engine()
        with engine.connect() as conn:
            conn.execute(
                text(
                    "UPDATE aggregate_watermarks SET last_refresh_rows = 99 "
                    "WHERE aggregate_name = :agg"
                ),
                {"agg": _WATERMARK_ROW["aggregate_name"]},
            )
        with engine.connect() as conn:
            row = conn.execute(
                text(
                    "SELECT last_refresh_rows FROM aggregate_watermarks "
                    "WHERE aggregate_name = :agg"
                ),
                {"agg": _WATERMARK_ROW["aggregate_name"]},
            ).scalar_one()
            assert (
                row == saved["last_refresh_rows"]
            ), f"engine.connect() a commité alors qu'il ne devrait pas (lu {row})"
    finally:
        _restore_watermark(saved)


def test_verify_aggregate_refresh_raises_on_stagnant_watermark() -> None:
    # Avant == Après : le refresh n'a pas fait avancer le filigrane.
    before = ("2020-01-01T00:00:00+00:00", 0)
    with patch.object(scheduler_guards, "get_watermark", side_effect=lambda agg: before):
        with pytest.raises(RuntimeError, match="n'a pas progressé"):
            verify_aggregate_refresh(_WATERMARK_ROW["aggregate_name"], before)


def test_verify_aggregate_refresh_passes_when_advanced() -> None:
    # Flux réel : l'appelant capture `before` (ancien), le refresh avance le
    # filigrane, verify relit `after` (plus récent) et ne doit pas lever.
    before = ("2020-01-01T00:00:00+00:00", 0)
    with patch.object(
        scheduler_guards, "get_watermark", return_value=("2026-08-03T09:00:00+00:00", 5)
    ):
        verify_aggregate_refresh(_WATERMARK_ROW["aggregate_name"], before)  # ne doit pas lever


def test_verify_aggregate_refresh_real_flow_before_refresh_advanced() -> None:
    # Représentation explicite du flux : before (capturé) -> refresh -> after avancé.
    before = ("2026-08-03T08:00:00+00:00", 0)
    after = ("2026-08-03T09:00:00+00:00", 5)
    with patch.object(scheduler_guards, "get_watermark", return_value=after):
        verify_aggregate_refresh(_WATERMARK_ROW["aggregate_name"], before)
        # after[0] > before[0] : le refresh a bien progressé
        assert after[0] > before[0]


def test_verify_aggregate_refresh_raises_when_watermark_missing() -> None:
    with patch.object(scheduler_guards, "get_watermark", return_value=None):
        with pytest.raises(RuntimeError, match="filigrane introuvable"):
            verify_aggregate_refresh(_WATERMARK_ROW["aggregate_name"], None)


def test_verify_aggregate_rows_raises_when_source_rows_exist() -> None:
    with patch.object(scheduler_guards, "check_source_rows_exist", return_value=True):
        with pytest.raises(RuntimeError, match="zéro ligne traitée"):
            verify_aggregate_rows(_WATERMARK_ROW["aggregate_name"], 0, "2026-08-03T08:00:00+00:00")


def test_verify_aggregate_rows_passes_when_no_source_rows() -> None:
    with patch.object(scheduler_guards, "check_source_rows_exist", return_value=False):
        verify_aggregate_rows(_WATERMARK_ROW["aggregate_name"], 0, "2026-08-03T08:00:00+00:00")


def test_verify_aggregate_rows_passes_when_rows_upserted() -> None:
    with patch.object(scheduler_guards, "check_source_rows_exist", return_value=True):
        verify_aggregate_rows(_WATERMARK_ROW["aggregate_name"], 5, "2026-08-03T08:00:00+00:00")


@pytest.mark.integration
def test_checkpoint_write_then_read_distinct_connection() -> None:
    engine = get_catalog_engine()
    test_pipeline = "integration_test"
    test_entity = "dev-checkpoint-001"
    test_metric = "temperature"
    test_ts = "2026-06-15T12:00:00+00:00"

    with engine.begin() as conn:
        conn.execute(
            text(
                "DELETE FROM trendx_catalog.ingestion_checkpoints "
                "WHERE pipeline = :p AND entity_id = :e AND metric_key = :m"
            ),
            {"p": test_pipeline, "e": test_entity, "m": test_metric},
        )

    with engine.begin() as conn:
        repo = CheckpointRepository(conn)
        repo.upsert_watermark(
            test_pipeline,
            test_entity,
            test_ts,
            test_metric,
            records_processed=1000,
            last_batch_id="batch-1",
        )

    with engine.connect() as conn:
        repo2 = CheckpointRepository(conn)
        row = repo2.get_watermark(test_pipeline, test_entity, test_metric)
        assert row is not None
        assert row["watermark_ts"] is not None
        assert str(row["watermark_ts"]).startswith("2026-06-15")
        assert row["records_processed"] == 1000
        assert row["last_batch_id"] == "batch-1"

    with engine.begin() as conn:
        conn.execute(
            text(
                "DELETE FROM trendx_catalog.ingestion_checkpoints "
                "WHERE pipeline = :p AND entity_id = :e AND metric_key = :m"
            ),
            {"p": test_pipeline, "e": test_entity, "m": test_metric},
        )


@pytest.mark.integration
def test_checkpoint_upsert_concurrent_same_key() -> None:
    engine = get_catalog_engine()
    test_pipeline = "integration_test"
    test_entity = "dev-checkpoint-002"
    test_metric = "humidity"

    with engine.begin() as conn:
        conn.execute(
            text(
                "DELETE FROM trendx_catalog.ingestion_checkpoints "
                "WHERE pipeline = :p AND entity_id = :e AND metric_key = :m"
            ),
            {"p": test_pipeline, "e": test_entity, "m": test_metric},
        )

    with engine.begin() as conn1:
        repo1 = CheckpointRepository(conn1)
        repo1.upsert_watermark(
            test_pipeline,
            test_entity,
            "2026-06-15T12:00:00+00:00",
            test_metric,
            records_processed=500,
            last_batch_id="batch-a",
        )

    with engine.begin() as conn2:
        repo2 = CheckpointRepository(conn2)
        repo2.upsert_watermark(
            test_pipeline,
            test_entity,
            "2026-06-15T13:00:00+00:00",
            test_metric,
            records_processed=1500,
            last_batch_id="batch-b",
        )

    with engine.connect() as conn3:
        repo3 = CheckpointRepository(conn3)
        row = repo3.get_watermark(test_pipeline, test_entity, test_metric)
        assert row is not None
        assert row["records_processed"] == 1500
        assert row["last_batch_id"] == "batch-b"

    with engine.begin() as conn:
        conn.execute(
            text(
                "DELETE FROM trendx_catalog.ingestion_checkpoints "
                "WHERE pipeline = :p AND entity_id = :e AND metric_key = :m"
            ),
            {"p": test_pipeline, "e": test_entity, "m": test_metric},
        )
