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


def verify_aggregate_rows(agg: str, rows_upserted: int, window_start: str) -> None:
    """Garde symétrique : détecte le cas où le filigrane avance mais zéro ligne
    source n'a été traitée alors que des données existent."""
    if rows_upserted == 0 and check_source_rows_exist(agg, window_start):
        raise RuntimeError(
            f"refresh_aggregate('{agg}') : filigrane avancé mais zéro ligne traitée "
            f"(window_start={window_start}, source rows existent)"
        )


def check_source_rows_exist(agg: str, window_start: str) -> bool:
    """Retourne True si des lignes source existent dans ts_kv pour la fenêtre
    de l'agrégat. Utilisé pour détecter le défaut symétrique : filigrane qui
    avance alors que zéro ligne source n'a été traitée."""
    engine = get_analytics_engine()
    with engine.connect() as conn:
        row = conn.execute(
            text(
                "SELECT EXISTS ("
                "  SELECT 1 FROM ts_kv"
                "  WHERE ts >= :window_start"
                "    AND dbl_v IS NOT NULL"
                "  LIMIT 1"
                ")"
            ),
            {"window_start": window_start},
        ).scalar_one()
        return bool(row)
