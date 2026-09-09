"""Explorer request/response contracts (Pydantic v2, whitelists only)."""

from __future__ import annotations

from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from pydantic import BaseModel, Field, field_validator
from trendx.query.engine import AGGREGATIONS as ENGINE_AGGS
from trendx.query.engine import ATTR_OPS as ENGINE_ATTR_OPS
from trendx.query.sql import BUCKETS_SQL

ATTR_OPS = sorted(ENGINE_ATTR_OPS | {"eq", "ne", "gt", "gte", "lt", "lte"})
TELEMETRY_OPS = ("gt", "gte", "lt", "lte", "eq")
AGGREGATIONS = sorted(set(ENGINE_AGGS) | {"P95", "P99", "UNIQ"})
BUCKETS = sorted(BUCKETS_SQL)


class CatalogOut(BaseModel):
    entities: list[dict[str, Any]]
    metrics: list[str]
    relations: list[dict[str, str]]
    dimensions: dict[str, list[str]]


class AvailabilityOut(BaseModel):
    entity_id: str
    metric: str
    start_ts: str | None = None
    end_ts: str | None = None
    n_points: int = 0


class StatsOut(BaseModel):
    entity_id: str
    metric: str
    n: int = 0
    min: float | None = None
    max: float | None = None
    mean: float | None = None
    std: float | None = None
    sum: float | None = None
    null_count: int = 0


class QueryIn(BaseModel):
    entity_ids: list[str] = Field(min_length=1)
    metric: str = Field(min_length=1)
    start: str | None = None
    end: str | None = None
    attr_filters: dict[str, Any] = Field(default_factory=dict)
    telemetry_op: list[Any] | None = None
    group_by: list[str] = Field(default_factory=lambda: ["entity"])
    aggregations: list[str] = Field(default_factory=lambda: ["AVG"])
    bucket: str = "1D"
    metric_alias: str | None = None
    agg_aliases: dict[str, str] = Field(default_factory=dict)
    display_tz: str | None = None

    @field_validator("telemetry_op")
    @classmethod
    def _check_telemetry_op(cls, v: list[Any] | None) -> list[Any] | None:
        if v is None:
            return v
        if len(v) != 2 or v[0] not in TELEMETRY_OPS:
            raise ValueError(f"telemetry_op must be [op, threshold] with op in {TELEMETRY_OPS}")
        return v

    @field_validator("aggregations")
    @classmethod
    def _check_aggs(cls, v: list[str]) -> list[str]:
        for agg in v:
            up = str(agg).upper()
            if up not in AGGREGATIONS and not (
                up.startswith("P") and up[1:].replace(".", "", 1).isdigit()
            ):
                raise ValueError(f"Unknown aggregation: {agg}")
        return v

    @field_validator("group_by")
    @classmethod
    def _check_group_by(cls, v: list[str]) -> list[str]:
        for dim in v:
            if dim not in ("entity", "metric", "time", "profile", "customer"):
                raise ValueError(f"Unknown group_by dimension: {dim}")
        return v

    @field_validator("bucket")
    @classmethod
    def _check_bucket(cls, v: str) -> str:
        if v not in BUCKETS:
            raise ValueError(f"Unknown bucket: {v!r}. Expected one of {BUCKETS}")
        return v

    @field_validator("display_tz")
    @classmethod
    def _check_tz(cls, v: str | None) -> str | None:
        if v is None:
            return v
        try:
            ZoneInfo(v)
        except ZoneInfoNotFoundError:
            raise ValueError(f"Unknown timezone: {v}") from None
        return v


class QueryOut(BaseModel):
    columns: list[str]
    rows: list[dict[str, Any]]
    n_rows: int = 0


class DrilldownOut(BaseModel):
    entity_id: str
    depth: int
    descendants: list[str]


class CompareSide(BaseModel):
    entity_ids: list[str] = Field(default_factory=list)
    profile: str | None = None
    customer: str | None = None


class CompareIn(BaseModel):
    left: CompareSide
    right: CompareSide
    metric: str = Field(min_length=1)
    start: str | None = None
    end: str | None = None
    aggregation: str = "AVG"
    bucket: str = "1D"


class CompareOut(BaseModel):
    metric: str
    aggregation: str
    left: list[dict[str, Any]]
    right: list[dict[str, Any]]
