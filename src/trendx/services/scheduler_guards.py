from __future__ import annotations

from typing import Any

from sqlalchemy import text

from trendx.database.connection import get_analytics_engine


def get_watermark(agg: str) -> tuple[Any, Any] | None:
    engine = get_analytics_engine()
    with engine.connect() as conn:
        row = conn.execute(
            text(
                "SELECT last_refresh_start, last_refresh_rows "
                "FROM aggregate_watermarks WHERE aggregate_name = :agg"
            ),
            {"agg": agg},
        ).fetchone()
        return (row[0], row[1]) if row else None


def verify_aggregate_refresh(agg: str) -> None:
    before = get_watermark(agg)
    after = get_watermark(agg)
    if before is None or after is None:
        raise RuntimeError(
            f"refresh_aggregate('{agg}') : filigrane introuvable avant={before} après={after}"
        )
    if after[0] <= before[0]:
        raise RuntimeError(
            f"refresh_aggregate('{agg}') n'a pas progressé "
            f"(last_refresh_start avant={before[0]} après={after[0]})"
        )
