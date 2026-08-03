from __future__ import annotations

from unittest.mock import patch

import pytest

from sqlalchemy import text

from trendx.database.connection import get_analytics_engine
from trendx.services import scheduler_guards
from trendx.services.scheduler_guards import (
    check_source_rows_exist,
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
            assert row == saved["last_refresh_rows"], (
                f"engine.connect() a commité alors qu'il ne devrait pas (lu {row})"
            )
    finally:
        _restore_watermark(saved)


def test_verify_aggregate_refresh_raises_on_stagnant_watermark() -> None:
    with patch.object(
        scheduler_guards, "get_watermark", side_effect=lambda agg: ("2020-01-01T00:00:00+00:00", 0)
    ):
        with pytest.raises(RuntimeError, match="n'a pas progressé"):
            verify_aggregate_refresh(_WATERMARK_ROW["aggregate_name"])


def test_verify_aggregate_refresh_passes_when_advanced() -> None:
    with patch.object(
        scheduler_guards,
        "get_watermark",
        side_effect=[
            ("2020-01-01T00:00:00+00:00", 0),
            ("2026-08-03T09:00:00+00:00", 5),
        ],
    ):
        verify_aggregate_refresh(_WATERMARK_ROW["aggregate_name"])  # ne doit pas lever


def test_verify_aggregate_refresh_raises_when_watermark_missing() -> None:
    with patch.object(scheduler_guards, "get_watermark", return_value=None):
        with pytest.raises(RuntimeError, match="filigrane introuvable"):
            verify_aggregate_refresh(_WATERMARK_ROW["aggregate_name"])


def test_verify_aggregate_rows_raises_when_source_rows_exist() -> None:
    with patch.object(
        scheduler_guards, "check_source_rows_exist", return_value=True
    ):
        with pytest.raises(RuntimeError, match="zéro ligne traitée"):
            verify_aggregate_rows(_WATERMARK_ROW["aggregate_name"], 0, "2026-08-03T08:00:00+00:00")


def test_verify_aggregate_rows_passes_when_no_source_rows() -> None:
    with patch.object(
        scheduler_guards, "check_source_rows_exist", return_value=False
    ):
        verify_aggregate_rows(_WATERMARK_ROW["aggregate_name"], 0, "2026-08-03T08:00:00+00:00")


def test_verify_aggregate_rows_passes_when_rows_upserted() -> None:
    with patch.object(
        scheduler_guards, "check_source_rows_exist", return_value=True
    ):
        verify_aggregate_rows(_WATERMARK_ROW["aggregate_name"], 5, "2026-08-03T08:00:00+00:00")
