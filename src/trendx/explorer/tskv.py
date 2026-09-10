"""Adapter prod-shape (catalog 001 + ts_kv 009) for Explorer.

Mapping qsql_* <-> prod (documented adaptations):
- qsql_entity(id TEXT, type, profile, customer, attributes JSONB)
  -> business_entity(id UUID, name) + tenant_id as customer dimension;
     attributes = {"name": ..., "profile": <field type>} (no generic
     attribute store in 001; relation/field metadata carry the rest).
- qsql_relation(from,to) -> relation(business_entity_id, name,
  related_entity_id, direction) filtered enabled=true.
- qsql_telemetry(entity, metric, ts, value)
  -> ts_kv(ts, entity_id UUID, metric_key, dbl_v).
- metrics catalog = business_entity_field names UNION distinct
  metric_key present in ts_kv.

Read-only. Values bound, identifiers whitelisted (reuse sql module).
"""

from __future__ import annotations

from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine
from trendx.explorer import schemas
from trendx.query.engine import BusinessQuery
from trendx.query.sql import BUCKETS_SQL

SCHEMA_TSKV_SQL = """
CREATE SCHEMA IF NOT EXISTS trendx_catalog;
CREATE SCHEMA IF NOT EXISTS trendx_analytics;
CREATE TABLE IF NOT EXISTS trendx_catalog.business_entity (
    id UUID PRIMARY KEY,
    name TEXT NOT NULL,
    tenant_id UUID
);
CREATE TABLE IF NOT EXISTS trendx_catalog.business_entity_field (
    id UUID PRIMARY KEY,
    name TEXT NOT NULL,
    business_entity_id UUID REFERENCES trendx_catalog.business_entity(id),
    type TEXT
);
CREATE TABLE IF NOT EXISTS trendx_catalog.relation (
    business_entity_id UUID REFERENCES trendx_catalog.business_entity(id),
    name TEXT NOT NULL,
    related_entity_id UUID REFERENCES trendx_catalog.business_entity(id),
    direction TEXT NOT NULL DEFAULT 'OUT',
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    PRIMARY KEY (business_entity_id, name, related_entity_id, direction)
);
CREATE TABLE IF NOT EXISTS trendx_analytics.ts_kv (
    ts TIMESTAMPTZ NOT NULL,
    entity_id UUID NOT NULL,
    metric_key TEXT NOT NULL,
    dbl_v DOUBLE PRECISION,
    PRIMARY KEY (ts, entity_id, metric_key)
);
CREATE INDEX IF NOT EXISTS tskv_entity_metric_ts
    ON trendx_analytics.ts_kv (entity_id, metric_key, ts);
"""

_AGGS_TSKV: dict[str, str] = {
    "MIN": "MIN(t.dbl_v)",
    "MAX": "MAX(t.dbl_v)",
    "SUM": "SUM(t.dbl_v)",
    "AVG": "AVG(t.dbl_v)",
    "COUNT": "COUNT(t.dbl_v)",
    "MEDIAN": "percentile_cont(0.5) WITHIN GROUP (ORDER BY t.dbl_v)",
    "P90": "percentile_cont(0.9) WITHIN GROUP (ORDER BY t.dbl_v)",
    "P95": "percentile_cont(0.95) WITHIN GROUP (ORDER BY t.dbl_v)",
    "P99": "percentile_cont(0.99) WITHIN GROUP (ORDER BY t.dbl_v)",
    "UNIQ": "COUNT(DISTINCT t.dbl_v)",
}

_TELEMETRY_OPS: dict[str, str] = {"gt": ">", "gte": ">=", "lt": "<", "lte": "<=", "eq": "="}


def ensure_tskv_schema(engine: Engine) -> None:
    with engine.begin() as conn:
        for stmt in SCHEMA_TSKV_SQL.split(";"):
            if stmt.strip():
                conn.execute(text(stmt))


def _attrs(row_name: str, row_type: str | None) -> dict[str, Any]:
    attrs: dict[str, Any] = {"name": row_name}
    if row_type:
        attrs["profile"] = row_type
    return attrs


class TskvBackend:
    """Explorer queries against prod-shape catalog + ts_kv."""

    def __init__(self, engine: Engine) -> None:
        self._engine = engine

    def catalog(self) -> schemas.CatalogOut:
        with self._engine.connect() as conn:
            entities = [
                {
                    "id": str(r[0]),
                    "type": "BusinessEntity",
                    "profile": r[2] or "",
                    "customer": str(r[1]) if r[1] else "",
                    "attributes": _attrs(r[3], r[2]),
                }
                for r in conn.execute(
                    text(
                        "SELECT be.id, be.tenant_id, f.type, be.name FROM trendx_catalog.business_entity be "
                        "LEFT JOIN trendx_catalog.business_entity_field f ON f.business_entity_id = be.id "
                        "ORDER BY be.id, f.name"
                    )
                ).fetchall()
            ]
            seen: dict[str, dict[str, Any]] = {}
            for e in entities:
                seen.setdefault(str(e["id"]), e)
            fields = [
                r[0]
                for r in conn.execute(
                    text(
                        "SELECT DISTINCT name FROM trendx_catalog.business_entity_field ORDER BY 1"
                    )
                ).fetchall()
            ]
            keys = [
                r[0]
                for r in conn.execute(
                    text("SELECT DISTINCT metric_key FROM trendx_analytics.ts_kv ORDER BY 1")
                ).fetchall()
            ]
            metrics = sorted(set(fields) | set(keys))
            relations = [
                {"from": str(r[0]), "to": str(r[1]), "relation": r[2]}
                for r in conn.execute(
                    text(
                        "SELECT business_entity_id, related_entity_id, name FROM trendx_catalog.relation "
                        "WHERE enabled = TRUE ORDER BY 1,2,3"
                    )
                ).fetchall()
            ]
            profiles = sorted({e["profile"] for e in seen.values() if e["profile"]})
            customers = sorted({e["customer"] for e in seen.values() if e["customer"]})
        return schemas.CatalogOut(
            entities=sorted(seen.values(), key=lambda e: e["id"]),
            metrics=metrics,
            relations=relations,
            dimensions={"profile": profiles, "customer": customers},
        )

    def _resolve(self, conn: Any, entity_ids: list[str], attr_filters: dict[str, Any]) -> list[str]:
        rows = conn.execute(
            text(
                "SELECT be.id, be.name, f.type FROM trendx_catalog.business_entity be "
                "LEFT JOIN trendx_catalog.business_entity_field f ON f.business_entity_id = be.id"
            )
        ).fetchall()
        attrs_by_id: dict[str, dict[str, Any]] = {}
        for eid, name, ftype in rows:
            key = str(eid)
            merged = attrs_by_id.setdefault(key, {"name": name})
            if ftype:
                merged["profile"] = ftype
        out: list[str] = []
        for eid in sorted(entity_ids):
            attrs: dict[str, Any] | None = attrs_by_id.get(eid)
            if attrs is None:
                continue
            keep = True
            for fkey, cond in attr_filters.items():
                actual = attrs.get(fkey)
                if (
                    isinstance(cond, tuple)
                    and len(cond) == 2
                    and cond[0] in ("eq", "ne", "gt", "gte", "lt", "lte")
                ):
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

    def availability(self, entity_id: str, metric: str) -> schemas.AvailabilityOut:
        with self._engine.connect() as conn:
            row = conn.execute(
                text(
                    "SELECT MIN(ts), MAX(ts), COUNT(*) FROM trendx_analytics.ts_kv "
                    "WHERE entity_id = :e AND metric_key = :m"
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

    def stats(self, entity_id: str, metric: str) -> schemas.StatsOut:
        from trendx.query.stats import describe_series

        with self._engine.connect() as conn:
            vals = [
                r[0]
                for r in conn.execute(
                    text(
                        "SELECT dbl_v FROM trendx_analytics.ts_kv WHERE entity_id = :e AND metric_key = :m ORDER BY ts"
                    ),
                    {"e": entity_id, "m": metric},
                ).fetchall()
            ]
        return schemas.StatsOut(entity_id=entity_id, metric=metric, **describe_series(vals))

    def query(self, query: BusinessQuery) -> schemas.QueryOut:
        aggs = [a.upper() for a in query.aggregations]
        for agg in aggs:
            if agg not in _AGGS_TSKV and not (
                agg.startswith("P") and agg[1:].replace(".", "", 1).isdigit()
            ):
                raise ValueError(f"Unknown aggregation: {agg}")
        if "time" in query.group_by and query.bucket not in BUCKETS_SQL:
            raise ValueError(
                f"Unknown bucket: {query.bucket!r}. Expected one of {sorted(BUCKETS_SQL)}"
            )
        if query.telemetry_op is not None and query.telemetry_op[0] not in _TELEMETRY_OPS:
            raise ValueError(f"Unknown telemetry op: {query.telemetry_op[0]}")
        with self._engine.connect() as conn:
            conn.execute(text("SET LOCAL time zone 'UTC'"))
            entity_ids = self._resolve(conn, sorted(query.entity_ids), query.attr_filters)
            if not entity_ids:
                return schemas.QueryOut(columns=[], rows=[], n_rows=0)
            import uuid as _uuid

            where = ["t.entity_id = ANY(:eids)", "t.metric_key = :metric"]
            params: dict[str, Any] = {
                "eids": [_uuid.UUID(e) for e in entity_ids],
                "metric": query.metric,
            }
            if query.start is not None:
                where.append("t.ts >= :start")
                ts = pd.Timestamp(query.start)
                params["start"] = ts.tz_convert("UTC") if ts.tzinfo else ts.tz_localize("UTC")
            if query.end is not None:
                where.append("t.ts < :end")
                te = pd.Timestamp(query.end)
                params["end"] = te.tz_convert("UTC") if te.tzinfo else te.tz_localize("UTC")
            if query.telemetry_op is not None:
                op, threshold = query.telemetry_op
                where.append(f"t.dbl_v {_TELEMETRY_OPS[op]} :threshold")
                params["threshold"] = float(threshold)
            select: list[str] = []
            group: list[str] = []
            if "entity" in query.group_by:
                select.append("t.entity_id AS entity")
                group.append("t.entity_id")
            if "metric" in query.group_by:
                select.append("t.metric_key AS metric")
                group.append("t.metric_key")
            bucketed = False
            if "time" in query.group_by:
                select.append(f"date_trunc('{BUCKETS_SQL[query.bucket]}', t.ts) AS bucket")
                group.append(f"date_trunc('{BUCKETS_SQL[query.bucket]}', t.ts)")
                bucketed = True
            for agg in aggs:
                expr = _AGGS_TSKV.get(agg)
                if expr is None:
                    q = float(agg[1:]) / 100.0
                    expr = f"percentile_cont({q}) WITHIN GROUP (ORDER BY t.dbl_v)"
                label = (query.agg_aliases or {}).get(agg, agg)
                select.append(f"{expr} AS {agg.lower()}_v")
            seen_aliases: set[str] = set()
            for agg in aggs:
                alias = f"{agg.lower()}_v"
                if alias in seen_aliases:
                    raise ValueError(f"Duplicate aggregation alias: {agg}")
                seen_aliases.add(alias)
            sql = f"SELECT {', '.join(select)} FROM trendx_analytics.ts_kv t WHERE {' AND '.join(where)}"  # nosec B608 -- whitelists; values bound
            if group:
                sql += f" GROUP BY {', '.join(group)}"
            rows = conn.execute(text(sql), params).fetchall()
            metric_label = query.metric_alias or query.metric
            out_rows: list[dict[str, Any]] = []
            for row in rows:
                mapping = dict(row._mapping)
                base: dict[str, Any] = {}
                if "entity" in mapping:
                    base["entity"] = str(mapping["entity"])
                if "metric" in mapping:
                    base["metric"] = mapping["metric"]
                if "bucket" in mapping:
                    base["bucket"] = mapping["bucket"]
                for agg in aggs:
                    label = (query.agg_aliases or {}).get(agg, agg)
                    val = mapping[f"{agg.lower()}_v"]
                    if val is None:
                        continue
                    out_rows.append(
                        {**base, "metric_label": metric_label, "agg": label, "value": float(val)}
                    )
            if not out_rows:
                return schemas.QueryOut(columns=[], rows=[], n_rows=0)
            out = pd.DataFrame(out_rows)
            if bucketed and query.display_tz is not None:
                out["bucket"] = pd.to_datetime(out["bucket"], utc=True).dt.tz_convert(
                    ZoneInfo(query.display_tz)
                )
            cols = [
                c
                for c in (["entity", "metric", "bucket", "metric_label", "agg", "value"])
                if c in out.columns
            ]
            out = out[cols].sort_values(by=cols).reset_index(drop=True)
            records: list[dict[str, Any]] = []
            for _, record in out.iterrows():
                item: dict[str, Any] = {}
                for col in out.columns:
                    val = record[col]
                    if isinstance(val, pd.Timestamp):
                        item[col] = val.isoformat()
                    else:
                        item[col] = val.item() if hasattr(val, "item") else val
                records.append(item)
            return schemas.QueryOut(columns=list(out.columns), rows=records, n_rows=len(records))
