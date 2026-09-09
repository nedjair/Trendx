"""SQL backend (PostgreSQL jetable) for BusinessQuery semantics.

Contract BusinessQuery -> parameterized SQL. Values always bound;
identifiers come from fixed whitelists. Storage UTC; display TZ
applied in Python to bucket labels only.

Semantics: aggregates ignore NULL; COUNT = COUNT(value) (non-null);
empty partition yields no row; MEDIAN/Pn use percentile_cont
(linear interpolation, same convention as numpy method='linear').
"""

from __future__ import annotations

from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from trendx.query.engine import BusinessQuery

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS qsql_entity (
    id TEXT PRIMARY KEY,
    type TEXT NOT NULL,
    profile TEXT NOT NULL,
    customer TEXT NOT NULL,
    attributes JSONB NOT NULL DEFAULT '{}'
);
CREATE TABLE IF NOT EXISTS qsql_relation (
    from_id TEXT NOT NULL REFERENCES qsql_entity(id),
    to_id TEXT NOT NULL REFERENCES qsql_entity(id),
    relation TEXT NOT NULL DEFAULT 'Contains',
    PRIMARY KEY (from_id, to_id, relation)
);
CREATE TABLE IF NOT EXISTS qsql_telemetry (
    entity_id TEXT NOT NULL REFERENCES qsql_entity(id),
    metric TEXT NOT NULL,
    ts TIMESTAMPTZ NOT NULL,
    value DOUBLE PRECISION,
    PRIMARY KEY (entity_id, metric, ts)
);
CREATE INDEX IF NOT EXISTS qsql_telemetry_entity_metric_ts
    ON qsql_telemetry (entity_id, metric, ts);
"""

BUCKETS_SQL: dict[str, str] = {"1H": "hour", "1D": "day", "1W": "week", "1M": "month"}

_ATTR_OPS: frozenset[str] = frozenset({"eq", "ne", "gt", "gte", "lt", "lte"})

_TELEMETRY_OPS_SQL: dict[str, str] = {
    "gt": ">",
    "gte": ">=",
    "lt": "<",
    "lte": "<=",
    "eq": "=",
}

_AGGS_SQL: dict[str, str] = {
    "MIN": "MIN(t.value)",
    "MAX": "MAX(t.value)",
    "SUM": "SUM(t.value)",
    "AVG": "AVG(t.value)",
    "COUNT": "COUNT(t.value)",
    "MEDIAN": "percentile_cont(0.5) WITHIN GROUP (ORDER BY t.value)",
    "P90": "percentile_cont(0.9) WITHIN GROUP (ORDER BY t.value)",
    "P95": "percentile_cont(0.95) WITHIN GROUP (ORDER BY t.value)",
    "P99": "percentile_cont(0.99) WITHIN GROUP (ORDER BY t.value)",
    "UNIQ": "COUNT(DISTINCT t.value)",
}


def ensure_schema(engine: Engine) -> None:
    with engine.begin() as conn:
        for stmt in SCHEMA_SQL.split(";"):
            if stmt.strip():
                conn.execute(text(stmt))


def _resolve_entities(
    conn: Any, entity_ids: list[str], attr_filters: dict[str, object]
) -> list[str]:
    if not entity_ids:
        return []
    rows = conn.execute(
        text("SELECT id, attributes FROM qsql_entity WHERE id = ANY(:ids)"),
        {"ids": list(entity_ids)},
    ).fetchall()
    by_id = {r[0]: (r[1] or {}) for r in rows}
    out: list[str] = []
    for eid in sorted(entity_ids):
        attrs = by_id.get(eid)
        if attrs is None:
            continue
        keep = True
        for key, cond in attr_filters.items():
            actual = attrs.get(key)
            if isinstance(cond, tuple) and len(cond) == 2 and cond[0] in _ATTR_OPS:
                op, val = cond
                if op == "eq":
                    keep = actual == val
                elif op == "ne":
                    keep = actual != val
                elif op == "gt":
                    keep = actual is not None and actual > val
                elif op == "gte":
                    keep = actual is not None and actual >= val
                elif op == "lt":
                    keep = actual is not None and actual < val
                elif op == "lte":
                    keep = actual is not None and actual <= val
            elif isinstance(cond, set | list | frozenset | tuple):
                if actual not in cond:
                    keep = False
            else:
                if actual != cond:
                    keep = False
            if not keep:
                break
        if keep:
            out.append(eid)
    return out


_DIMENSIONS_SQL: dict[str, str] = {
    "profile": "e.profile",
    "customer": "e.customer",
}


def resolve_descendants_cte(engine: Engine, start_id: str, max_depth: int = 6) -> list[str]:
    """Deterministic descendant resolution via recursive CTE.

    Cycle-safe (visited path array), depth-bounded, sorted output.
    Values bound; depth validated as positive int.
    """
    if not isinstance(max_depth, int) or isinstance(max_depth, bool) or max_depth < 1:
        raise ValueError(f"Invalid max_depth: {max_depth!r}")
    sql = """
        WITH RECURSIVE tree(id, depth, path) AS (
            SELECT :start AS id, 0 AS depth, ARRAY[:start] AS path
            UNION
            SELECT r.to_id, tree.depth + 1, tree.path || r.to_id
            FROM qsql_relation r
            JOIN tree ON tree.id = r.from_id
            WHERE tree.depth < :max_depth AND NOT (r.to_id = ANY(tree.path))
        )
        SELECT DISTINCT id FROM tree WHERE depth > 0 ORDER BY 1
    """
    with engine.connect() as conn:
        rows = conn.execute(text(sql), {"start": start_id, "max_depth": max_depth}).fetchall()
    return [r[0] for r in rows]


class SqlQueryEngine:
    """Execute BusinessQuery against ephemeral PostgreSQL."""

    def __init__(self, engine: Engine) -> None:
        self._engine = engine

    def run(self, query: BusinessQuery) -> pd.DataFrame:
        aggs = [a.upper() for a in query.aggregations]
        seen_aliases: set[str] = set()
        for agg in aggs:
            alias = f"{agg.lower()}_v"
            if alias in seen_aliases:
                raise ValueError(f"Duplicate aggregation alias: {agg}")
            seen_aliases.add(alias)
        for agg in aggs:
            if agg not in _AGGS_SQL and not (
                agg.startswith("P") and agg[1:].replace(".", "", 1).isdigit()
            ):
                raise ValueError(f"Unknown aggregation: {agg}")
        if "time" in query.group_by and query.bucket not in BUCKETS_SQL:
            raise ValueError(
                f"Unknown bucket: {query.bucket!r}. Expected one of {sorted(BUCKETS_SQL)}"
            )
        if query.telemetry_op is not None and query.telemetry_op[0] not in _TELEMETRY_OPS_SQL:
            raise ValueError(f"Unknown telemetry op: {query.telemetry_op[0]}")
        with self._engine.connect() as conn:
            conn.execute(text("SET LOCAL time zone 'UTC'"))
            entity_ids = _resolve_entities(conn, sorted(query.entity_ids), query.attr_filters)
            if not entity_ids:
                return pd.DataFrame(
                    columns=["entity", "metric", "bucket", "metric_label", "agg", "value"]
                )
            where = ["t.entity_id = ANY(:eids)", "t.metric = :metric"]
            params: dict[str, Any] = {"eids": entity_ids, "metric": query.metric}
            if query.start is not None:
                where.append("t.ts >= :start")
                params["start"] = (
                    pd.Timestamp(query.start).tz_convert("UTC")
                    if pd.Timestamp(query.start).tzinfo
                    else pd.Timestamp(query.start).tz_localize("UTC")
                )
            if query.end is not None:
                where.append("t.ts < :end")
                params["end"] = (
                    pd.Timestamp(query.end).tz_convert("UTC")
                    if pd.Timestamp(query.end).tzinfo
                    else pd.Timestamp(query.end).tz_localize("UTC")
                )
            if query.telemetry_op is not None:
                op, threshold = query.telemetry_op
                where.append(f"t.value {_TELEMETRY_OPS_SQL[op]} :threshold")
                params["threshold"] = float(threshold)
            select: list[str] = []
            group: list[str] = []
            if "entity" in query.group_by:
                select.append("t.entity_id AS entity")
                group.append("t.entity_id")
            if "metric" in query.group_by:
                select.append("t.metric AS metric")
                group.append("t.metric")
            for dim in ("profile", "customer"):
                if dim in query.group_by:
                    select.append(f"{_DIMENSIONS_SQL[dim]} AS {dim}")
                    group.append(_DIMENSIONS_SQL[dim])
            bucketed = False
            if "time" in query.group_by:
                select.append(f"date_trunc('{BUCKETS_SQL[query.bucket]}', t.ts) AS bucket")
                group.append(f"date_trunc('{BUCKETS_SQL[query.bucket]}', t.ts)")
                bucketed = True
            for agg in aggs:
                expr = _AGGS_SQL.get(agg)
                if expr is None:
                    q = float(agg[1:]) / 100.0
                    expr = f"percentile_cont({q}) WITHIN GROUP (ORDER BY t.value)"
                label = (query.agg_aliases or {}).get(agg, agg)
                select.append(f"{expr} AS {agg.lower()}_v")
            from_clause = "qsql_telemetry t"
            dims = [d for d in ("profile", "customer") if d in query.group_by]
            if dims:
                from_clause += " JOIN qsql_entity e ON e.id = t.entity_id"
            sql = f"SELECT {', '.join(select)} FROM {from_clause} WHERE {' AND '.join(where)}"  # nosec B608 -- identifiers from fixed whitelists; values bound
            if group:
                sql += f" GROUP BY {', '.join(group)}"
            rows = conn.execute(text(sql), params).fetchall()
            metric_label = query.metric_alias or query.metric
            out_rows: list[dict[str, Any]] = []
            for row in rows:
                mapping = dict(row._mapping)
                base: dict[str, Any] = {}
                if "entity" in mapping:
                    base["entity"] = mapping["entity"]
                if "metric" in mapping:
                    base["metric"] = mapping["metric"]
                if "profile" in mapping:
                    base["profile"] = mapping["profile"]
                if "customer" in mapping:
                    base["customer"] = mapping["customer"]
                if "bucket" in mapping:
                    base["bucket"] = mapping["bucket"]
                for agg in aggs:
                    label = (query.agg_aliases or {}).get(agg, agg)
                    val = mapping[f"{agg.lower()}_v"]
                    if val is None:
                        continue  # empty/NULL-only partition yields no row
                    out_rows.append(
                        {**base, "metric_label": metric_label, "agg": label, "value": float(val)}
                    )
            if not out_rows:
                return pd.DataFrame(
                    columns=["entity", "metric", "bucket", "metric_label", "agg", "value"]
                )
            out = pd.DataFrame(out_rows)
            if bucketed and query.display_tz is not None:
                tz = ZoneInfo(query.display_tz)
                out["bucket"] = pd.to_datetime(out["bucket"], utc=True).dt.tz_convert(tz)
            cols = [
                c
                for c in (
                    [
                        "entity",
                        "metric",
                        "profile",
                        "customer",
                        "bucket",
                        "metric_label",
                        "agg",
                        "value",
                    ]
                )
                if c in out.columns
            ]
            return out[cols].sort_values(by=cols).reset_index(drop=True)


def create_engine_ephemeral(dsn: str) -> Engine:
    return create_engine(dsn, pool_pre_ping=True)
