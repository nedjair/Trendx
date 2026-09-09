"""BusinessQueryEngine: relation path + filters + groupBy + aggregations + alias.

In-memory over explicit fixtures (entity attributes + telemetry series).
All timestamps stored UTC; display_tz applied only to output bucket labels.
Deterministic: sorted entity_ids, sorted group keys, stable column order.
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from zoneinfo import ZoneInfo

import numpy as np
import numpy.typing as npt
import pandas as pd

AGGREGATIONS = ("MIN", "MAX", "SUM", "AVG", "COUNT", "MEDIAN", "P90")

ATTR_OPS = frozenset({"eq", "ne", "gt", "gte", "lt", "lte"})

TIME_BUCKETS = {"1H": "h", "1D": "D"}


def _percentile(values: npt.NDArray[np.float64], p: float) -> float:
    return float(np.percentile(values.astype(float), p, method="linear"))


def aggregate_values(values: npt.NDArray[np.float64], agg: str) -> float | int:
    a = agg.upper()
    v = np.asarray(values, dtype=float)
    v = v[~np.isnan(v)]
    if a == "COUNT":
        return int(len(v))
    if len(v) == 0:
        return float("nan")
    if a == "MIN":
        return float(np.min(v))
    if a == "MAX":
        return float(np.max(v))
    if a == "SUM":
        return float(np.sum(v))
    if a == "AVG":
        return float(np.mean(v))
    if a == "MEDIAN":
        return float(np.median(v))
    if a == "P90":
        return _percentile(v, 90.0)
    if a.startswith("P"):
        return _percentile(v, float(a[1:]))
    raise ValueError(f"Unknown aggregation: {agg}")


class RelationGraph:
    """Deterministic directed relation graph between entity ids."""

    def __init__(self) -> None:
        self._nodes: dict[str, dict[str, object]] = {}
        self._edges: dict[str, list[tuple[str, str]]] = {}

    def add_entity(
        self, entity_id: str, entity_type: str, attributes: dict[str, object] | None = None
    ) -> None:
        self._nodes[entity_id] = {"type": entity_type, "attributes": dict(attributes or {})}

    def add_relation(self, from_id: str, to_id: str, relation: str = "Contains") -> None:
        self._edges.setdefault(from_id, []).append((to_id, relation))
        self._edges[from_id].sort()

    def resolve_path(self, start_id: str, target_type: str, max_depth: int = 6) -> list[str]:
        """BFS deterministic path start -> ... -> first node of target_type."""
        if start_id not in self._nodes:
            raise KeyError(f"Unknown entity: {start_id}")
        if self._nodes[start_id]["type"] == target_type:
            return [start_id]
        prev: dict[str, str | None] = {start_id: None}
        queue: deque[str] = deque([start_id])
        while queue:
            cur = queue.popleft()
            for nxt, _rel in sorted(self._edges.get(cur, [])):
                if nxt not in prev:
                    prev[nxt] = cur
                    if self._nodes[nxt]["type"] == target_type:
                        path = [nxt]
                        while prev[path[-1]] is not None:
                            path.append(prev[path[-1]])  # type: ignore[arg-type]
                        return list(reversed(path))
                    queue.append(nxt)
        raise KeyError(f"No path from {start_id} to type {target_type}")

    def descendants(self, start_id: str, target_type: str) -> list[str]:
        out: list[str] = []
        seen = {start_id}
        queue: deque[str] = deque([start_id])
        while queue:
            cur = queue.popleft()
            for nxt, _rel in sorted(self._edges.get(cur, [])):
                if nxt not in seen:
                    seen.add(nxt)
                    if self._nodes[nxt]["type"] == target_type:
                        out.append(nxt)
                    queue.append(nxt)
        return sorted(out)


@dataclass
class BusinessQuery:
    entity_ids: list[str] = field(default_factory=list)
    metric: str = ""
    start: pd.Timestamp | None = None
    end: pd.Timestamp | None = None
    attr_filters: dict[str, object] = field(
        default_factory=dict
    )  # key -> value | (op, value) | set/list
    telemetry_op: tuple[str, float] | None = None  # (gt/lt/gte/lte/eq, threshold) applied per point
    group_by: tuple[str, ...] = ("entity",)
    aggregations: tuple[str, ...] = ("AVG",)
    metric_alias: str | None = None
    agg_aliases: dict[str, str] | None = None
    display_tz: str | None = None  # e.g. Europe/Paris; storage always UTC
    bucket: str = "1D"  # time bucket in UTC: 1H or 1D


class BusinessQueryEngine:
    """Query telemetry fixtures with filters/groupBy/aggregations."""

    def __init__(
        self, graph: RelationGraph, telemetry: dict[tuple[str, str], pd.DataFrame]
    ) -> None:
        """telemetry keyed by (entity_id, metric) -> DataFrame[ts (UTC tz-aware), value]."""
        self._graph = graph
        self._telemetry = telemetry

    def _apply_attr_filters(self, entity_ids: list[str], filters: dict[str, object]) -> list[str]:
        out: list[str] = []
        for eid in sorted(entity_ids):
            node: dict[str, object] = self._graph._nodes[eid]
            attrs = node["attributes"]
            if not isinstance(attrs, dict):
                raise TypeError(f"Invalid attributes for entity {eid}")
            keep = True
            for key, cond in filters.items():
                actual = attrs.get(key)
                if isinstance(cond, tuple) and len(cond) == 2 and cond[0] in ATTR_OPS:
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

    def run(self, query: BusinessQuery) -> pd.DataFrame:
        # time bounds normalized to UTC
        def _as_utc(v: object) -> pd.Timestamp | None:
            if v is None:
                return None
            ts = pd.Timestamp(v)
            if ts.tzinfo is None:
                return ts.tz_localize("UTC")
            return ts.tz_convert("UTC")

        start, end = _as_utc(query.start), _as_utc(query.end)
        entity_ids = self._apply_attr_filters(sorted(query.entity_ids), query.attr_filters)
        rows: list[dict[str, object]] = []
        for eid in entity_ids:
            df = self._telemetry.get((eid, query.metric))
            if df is None or df.empty:
                continue
            sub = df
            if start is not None:
                sub = sub[sub["ts"] >= start]
            if end is not None:
                sub = sub[sub["ts"] < end]
            if query.telemetry_op is not None:
                op, threshold = query.telemetry_op
                if op == "gt":
                    sub = sub[sub["value"] > threshold]
                elif op == "gte":
                    sub = sub[sub["value"] >= threshold]
                elif op == "lt":
                    sub = sub[sub["value"] < threshold]
                elif op == "lte":
                    sub = sub[sub["value"] <= threshold]
                elif op == "eq":
                    sub = sub[sub["value"] == threshold]
                else:
                    raise ValueError(f"Unknown telemetry op: {op}")
            for _, r in sub.iterrows():
                rows.append(
                    {
                        "entity": eid,
                        "metric": query.metric,
                        "ts": r["ts"],
                        "value": float(r["value"]),
                    }
                )
        if not rows:
            return pd.DataFrame(columns=["entity", "metric", "bucket", "agg", "value"])
        data = pd.DataFrame(rows)
        # grouping
        group_cols: list[str] = []
        if "entity" in query.group_by:
            group_cols.append("entity")
        if "metric" in query.group_by:
            group_cols.append("metric")
        bucketed = False
        if "time" in query.group_by:
            # bucket applied in UTC; display_tz only affects label
            try:
                freq = TIME_BUCKETS[query.bucket]
            except KeyError:
                raise ValueError(
                    f"Unknown bucket: {query.bucket!r}. Expected one of {sorted(TIME_BUCKETS)}"
                ) from None
            data["bucket"] = data["ts"].dt.floor(freq)
            group_cols.append("bucket")
            bucketed = True
        out_rows: list[dict[str, object]] = []
        metric_label = query.metric_alias or query.metric
        if group_cols:
            grouped = data.groupby(group_cols, sort=True, dropna=False)
            for keys, grp in grouped:
                if not isinstance(keys, tuple):
                    keys = (keys,)
                base = dict(zip(group_cols, keys, strict=True))
                for agg in query.aggregations:
                    label = (query.agg_aliases or {}).get(agg, agg)
                    out_rows.append(
                        {
                            **base,
                            "metric_label": metric_label,
                            "agg": label,
                            "value": aggregate_values(grp["value"].to_numpy(), agg),
                        }
                    )
        else:
            for agg in query.aggregations:
                label = (query.agg_aliases or {}).get(agg, agg)
                out_rows.append(
                    {
                        "metric_label": metric_label,
                        "agg": label,
                        "value": aggregate_values(data["value"].to_numpy(), agg),
                    }
                )
        out = pd.DataFrame(out_rows)
        # display TZ conversion for bucket labels only
        if bucketed and query.display_tz is not None:
            tz = ZoneInfo(query.display_tz)
            out["bucket"] = pd.to_datetime(out["bucket"], utc=True).dt.tz_convert(tz)
        # stable column order
        cols = [
            c
            for c in (["entity", "metric", "bucket", "metric_label", "agg", "value"])
            if c in out.columns
        ]
        return out[cols].sort_values(by=cols).reset_index(drop=True)
