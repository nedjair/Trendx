"""Explorer backend: validation -> BusinessQuery -> engines -> stable rows.

Read-only. Production code never imports test oracles.
"""

from __future__ import annotations

from typing import Any

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine
from trendx.explorer import schemas
from trendx.query.engine import BusinessQuery
from trendx.query.sql import SqlQueryEngine, ensure_schema

_CONFIGURED: Engine | None = None
_BACKEND: str = "qsql"


def configure(engine: Engine | None, backend: str = "qsql") -> None:
    global _CONFIGURED, _BACKEND
    if backend not in ("qsql", "tskv"):
        raise ValueError(f"Unknown backend: {backend}")
    _CONFIGURED = engine
    _BACKEND = backend


def get_engine() -> Engine:
    if _CONFIGURED is None:
        raise RuntimeError("Explorer backend not configured")
    return _CONFIGURED


def catalog() -> schemas.CatalogOut:
    eng = get_engine()
    if _BACKEND == "tskv":
        from trendx.explorer.tskv import TskvBackend, ensure_tskv_schema

        ensure_tskv_schema(eng)
        return TskvBackend(eng).catalog()
    ensure_schema(eng)
    with eng.connect() as conn:
        entities = [
            {"id": r[0], "type": r[1], "profile": r[2], "customer": r[3], "attributes": r[4] or {}}
            for r in conn.execute(
                text("SELECT id, type, profile, customer, attributes FROM qsql_entity ORDER BY id")
            ).fetchall()
        ]
        metrics = [
            r[0]
            for r in conn.execute(
                text("SELECT DISTINCT metric FROM qsql_telemetry ORDER BY 1")
            ).fetchall()
        ]
        relations = [
            {"from": r[0], "to": r[1], "relation": r[2]}
            for r in conn.execute(
                text("SELECT from_id, to_id, relation FROM qsql_relation ORDER BY 1,2,3")
            ).fetchall()
        ]
        profiles = [
            r[0]
            for r in conn.execute(
                text("SELECT DISTINCT profile FROM qsql_entity ORDER BY 1")
            ).fetchall()
        ]
        customers = [
            r[0]
            for r in conn.execute(
                text("SELECT DISTINCT customer FROM qsql_entity ORDER BY 1")
            ).fetchall()
        ]
    return schemas.CatalogOut(
        entities=entities,
        metrics=metrics,
        relations=relations,
        dimensions={"profile": profiles, "customer": customers},
    )


def availability(entity_id: str, metric: str) -> schemas.AvailabilityOut:
    eng = get_engine()
    if _BACKEND == "tskv":
        from trendx.explorer.tskv import TskvBackend, ensure_tskv_schema

        ensure_tskv_schema(eng)
        return TskvBackend(eng).availability(entity_id, metric)
    ensure_schema(eng)
    with eng.connect() as conn:
        row = conn.execute(
            text(
                "SELECT MIN(ts), MAX(ts), COUNT(*) FROM qsql_telemetry WHERE entity_id=:e AND metric=:m"
            ),
            {"e": entity_id, "m": metric},
        ).fetchone()
    if row is None or row[2] == 0:
        return schemas.AvailabilityOut(entity_id=entity_id, metric=metric)
    return schemas.AvailabilityOut(
        entity_id=entity_id,
        metric=metric,
        start_ts=str(row[0]),
        end_ts=str(row[1]),
        n_points=int(row[2]),
    )


def stats(entity_id: str, metric: str) -> schemas.StatsOut:
    eng = get_engine()
    if _BACKEND == "tskv":
        from trendx.explorer.tskv import TskvBackend, ensure_tskv_schema

        ensure_tskv_schema(eng)
        return TskvBackend(eng).stats(entity_id, metric)
    ensure_schema(eng)
    with eng.connect() as conn:
        vals = [
            r[0]
            for r in conn.execute(
                text(
                    "SELECT value FROM qsql_telemetry WHERE entity_id=:e AND metric=:m ORDER BY ts"
                ),
                {"e": entity_id, "m": metric},
            ).fetchall()
        ]
    from trendx.query.stats import describe_series

    desc = describe_series(vals)
    return schemas.StatsOut(entity_id=entity_id, metric=metric, **desc)


def _to_business_query(req: schemas.QueryIn) -> BusinessQuery:
    attr_filters: dict[str, Any] = {}
    for key, cond in req.attr_filters.items():
        if isinstance(cond, list) and len(cond) == 2 and cond[0] in schemas.ATTR_OPS:
            attr_filters[key] = (cond[0], cond[1])
        elif isinstance(cond, list):
            attr_filters[key] = cond
        else:
            attr_filters[key] = cond
    telemetry_op = tuple(req.telemetry_op) if req.telemetry_op else None
    return BusinessQuery(
        entity_ids=list(req.entity_ids),
        metric=req.metric,
        start=pd.Timestamp(req.start) if req.start else None,
        end=pd.Timestamp(req.end) if req.end else None,
        attr_filters=attr_filters,
        telemetry_op=telemetry_op,
        group_by=tuple(req.group_by),
        aggregations=tuple(req.aggregations),
        bucket=req.bucket,
        metric_alias=req.metric_alias,
        agg_aliases=dict(req.agg_aliases),
        display_tz=req.display_tz,
    )


def query(req: schemas.QueryIn) -> schemas.QueryOut:
    eng = get_engine()
    if _BACKEND == "tskv":
        from trendx.explorer.tskv import TskvBackend, ensure_tskv_schema

        ensure_tskv_schema(eng)
        return TskvBackend(eng).query(_to_business_query(req))
    ensure_schema(eng)
    bq = _to_business_query(req)
    df = SqlQueryEngine(eng).run(bq)
    rows: list[dict[str, Any]] = []
    for _, record in df.iterrows():
        item: dict[str, Any] = {}
        for col in df.columns:
            val = record[col]
            if isinstance(val, pd.Timestamp):
                item[col] = val.isoformat()
            elif isinstance(val, float) and pd.isna(val):
                item[col] = None
            else:
                try:
                    item[col] = val.item() if hasattr(val, "item") else val
                except (ValueError, AttributeError):
                    item[col] = val
        rows.append(item)
    return schemas.QueryOut(columns=list(df.columns), rows=rows, n_rows=len(rows))


def drilldown(entity_id: str, depth: int = 3) -> schemas.DrilldownOut:
    if not isinstance(depth, int) or isinstance(depth, bool) or depth < 1 or depth > 6:
        raise ValueError(f"Invalid depth: {depth!r}. Expected int in 1..6")
    eng = get_engine()
    ensure_schema(eng)
    from trendx.query.sql import resolve_descendants_cte

    descendants = resolve_descendants_cte(eng, entity_id, max_depth=depth)
    return schemas.DrilldownOut(entity_id=entity_id, depth=depth, descendants=descendants)


def _selector_ids(conn: Any, side: schemas.CompareSide) -> list[str]:
    from sqlalchemy import text as _text

    ids = list(side.entity_ids)
    if side.profile is not None:
        rows = conn.execute(
            _text("SELECT id FROM qsql_entity WHERE profile = :p ORDER BY 1"), {"p": side.profile}
        ).fetchall()
        ids.extend(r[0] for r in rows)
    if side.customer is not None:
        rows = conn.execute(
            _text("SELECT id FROM qsql_entity WHERE customer = :c ORDER BY 1"), {"c": side.customer}
        ).fetchall()
        ids.extend(r[0] for r in rows)
    return sorted(set(ids))


def compare(req: schemas.CompareIn) -> schemas.CompareOut:
    from trendx.query.sql import SqlQueryEngine

    agg = str(req.aggregation).upper()
    if agg not in schemas.AGGREGATIONS and not (
        agg.startswith("P") and agg[1:].replace(".", "", 1).isdigit()
    ):
        raise ValueError(f"Unknown aggregation: {req.aggregation}")
    eng = get_engine()
    ensure_schema(eng)
    with eng.connect() as conn:
        left_ids = _selector_ids(conn, req.left)
        right_ids = _selector_ids(conn, req.right)

    def _side(ids: list[str]) -> list[dict[str, object]]:
        if not ids:
            return []
        bq = BusinessQuery(
            entity_ids=ids,
            metric=req.metric,
            start=pd.Timestamp(req.start) if req.start else None,
            end=pd.Timestamp(req.end) if req.end else None,
            group_by=("entity",),
            aggregations=(agg,),
            bucket=req.bucket,
        )
        df = SqlQueryEngine(eng).run(bq)
        return [
            {"entity": row["entity"], "value": float(row["value"])}
            for _, row in df.sort_values(by=["entity"]).iterrows()
        ]

    return schemas.CompareOut(
        metric=req.metric, aggregation=agg, left=_side(left_ids), right=_side(right_ids)
    )
