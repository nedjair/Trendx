"""BE Query Engine P0 (jetable): relation path, filtres, groupBy, aggs, alias, UTC/TZ, stats, isolation, idempotence."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest
from trendx.query.engine import BusinessQuery, BusinessQueryEngine, RelationGraph
from trendx.query.stats import describe_series

BUILDING = "b1"
APT_A = "aA"
APT_B = "aB"
METER_A = "mA"
METER_B = "mB"


def _graph() -> RelationGraph:
    g = RelationGraph()
    g.add_entity(BUILDING, "Building", {"city": "Paris"})
    g.add_entity(APT_A, "Apartment", {"floor": 1, "zone": "north"})
    g.add_entity(APT_B, "Apartment", {"floor": 2, "zone": "south"})
    g.add_entity(METER_A, "Meter", {"floor": 1, "zone": "north", "model": "X1"})
    g.add_entity(METER_B, "Meter", {"floor": 2, "zone": "south", "model": "X1"})
    g.add_relation(BUILDING, APT_A)
    g.add_relation(BUILDING, APT_B)
    g.add_relation(APT_A, METER_A)
    g.add_relation(APT_B, METER_B)
    return g


def _telemetry() -> dict:
    ts = pd.date_range("2024-01-01", periods=48, freq="1h", tz="UTC")
    out = {}
    out[(METER_A, "power")] = pd.DataFrame({"ts": ts, "value": 100.0 + np.arange(48)})
    out[(METER_B, "power")] = pd.DataFrame({"ts": ts, "value": 200.0 + np.arange(48) * 2})
    out[(METER_A, "temp")] = pd.DataFrame({"ts": ts, "value": 20.0 + np.sin(np.arange(48))})
    out[(METER_B, "temp")] = pd.DataFrame({"ts": ts, "value": 22.0 + np.sin(np.arange(48))})
    return out


@pytest.fixture
def engine():
    return BusinessQueryEngine(_graph(), _telemetry())


def _expected_agg(values: np.ndarray, agg: str):
    """Oracle pandas, sans appel a aggregate_values."""
    v = np.asarray(values, dtype=float)
    v = v[~np.isnan(v)]
    if agg == "MIN":
        return float(np.min(v))
    if agg == "MAX":
        return float(np.max(v))
    if agg == "SUM":
        return float(np.sum(v))
    if agg == "AVG":
        return float(np.mean(v))
    if agg == "COUNT":
        return int(len(v))
    if agg == "MEDIAN":
        return float(np.median(v))
    if agg == "P90":
        return float(np.percentile(v, 90, method="linear"))
    raise AssertionError(agg)


def test_01_relation_path_3_levels():
    g = _graph()
    assert g.resolve_path(BUILDING, "Meter") == [BUILDING, APT_A, METER_A]
    assert g.resolve_path(APT_B, "Meter") == [APT_B, METER_B]
    assert g.descendants(BUILDING, "Meter") == [METER_A, METER_B]


def test_02_attr_filters(engine):
    q = BusinessQuery(
        entity_ids=[METER_A, METER_B],
        metric="power",
        attr_filters={"zone": "north"},
        aggregations=("COUNT",),
    )
    r = engine.run(q)
    assert r["entity"].unique().tolist() == [METER_A]
    q2 = BusinessQuery(
        entity_ids=[METER_A, METER_B],
        metric="power",
        attr_filters={"floor": ("gt", 1)},
        aggregations=("COUNT",),
    )
    assert engine.run(q2)["entity"].unique().tolist() == [METER_B]
    q3 = BusinessQuery(
        entity_ids=[METER_A, METER_B],
        metric="power",
        attr_filters={"zone": {"north", "south"}},
        aggregations=("COUNT",),
    )
    assert sorted(engine.run(q3)["entity"].unique()) == [METER_A, METER_B]


def test_02b_attr_filters_r1(engine):
    tup = BusinessQuery(
        entity_ids=[METER_A, METER_B],
        metric="power",
        attr_filters={"zone": ("north", "south")},
        aggregations=("COUNT",),
    )
    assert sorted(engine.run(tup)["entity"].unique()) == [METER_A, METER_B]
    eq = BusinessQuery(
        entity_ids=[METER_A, METER_B],
        metric="power",
        attr_filters={"zone": ("eq", "north")},
        aggregations=("COUNT",),
    )
    assert engine.run(eq)["entity"].unique().tolist() == [METER_A]
    gte = BusinessQuery(
        entity_ids=[METER_A, METER_B],
        metric="power",
        attr_filters={"floor": ("gte", 2)},
        aggregations=("COUNT",),
    )
    assert engine.run(gte)["entity"].unique().tolist() == [METER_B]
    lte = BusinessQuery(
        entity_ids=[METER_A, METER_B],
        metric="power",
        attr_filters={"floor": ("lte", 1)},
        aggregations=("COUNT",),
    )
    assert engine.run(lte)["entity"].unique().tolist() == [METER_A]
    combo = BusinessQuery(
        entity_ids=[METER_A, METER_B],
        metric="power",
        attr_filters={"zone": ("north", "south"), "floor": ("gte", 2)},
        aggregations=("COUNT",),
    )
    assert engine.run(combo)["entity"].unique().tolist() == [METER_B]


def test_03_telemetry_filters(engine):
    q = BusinessQuery(
        entity_ids=[METER_A], metric="power", telemetry_op=("gt", 120.0), aggregations=("COUNT",)
    )
    assert int(engine.run(q)["value"].iloc[0]) == 27


def test_04_groupby_entity(engine):
    q = BusinessQuery(
        entity_ids=[METER_A, METER_B], metric="power", group_by=("entity",), aggregations=("SUM",)
    )
    r = engine.run(q)
    assert len(r) == 2
    got = float(r.loc[r.entity == METER_A, "value"].iloc[0])
    assert got == _expected_agg(np.arange(48) + 100.0, "SUM")


def test_05_groupby_metric_time(engine):
    q = BusinessQuery(
        entity_ids=[METER_A], metric="power", group_by=("metric", "time"), aggregations=("COUNT",)
    )
    r = engine.run(q)
    assert len(r) == 2 and set(r["value"]) == {24}


def test_05b_time_bucket_r2(engine):
    hourly = BusinessQuery(
        entity_ids=[METER_A],
        metric="power",
        group_by=("time",),
        bucket="1H",
        aggregations=("COUNT",),
    )
    r = engine.run(hourly)
    assert len(r) == 48 and set(r["value"]) == {1}
    daily = BusinessQuery(
        entity_ids=[METER_A, METER_B],
        metric="power",
        group_by=("entity", "time"),
        bucket="1D",
        aggregations=("COUNT",),
    )
    r2 = engine.run(daily)
    assert len(r2) == 4 and set(r2["value"]) == {24}
    empty = BusinessQuery(
        entity_ids=[METER_A],
        metric="power",
        start=pd.Timestamp("2030-01-01T00:00Z"),
        end=pd.Timestamp("2030-01-02T00:00Z"),
        group_by=("time",),
        bucket="1H",
        aggregations=("COUNT",),
    )
    assert engine.run(empty).empty
    bad = BusinessQuery(
        entity_ids=[METER_A],
        metric="power",
        group_by=("time",),
        bucket="1W",
        aggregations=("COUNT",),
    )
    with pytest.raises(ValueError, match="Unknown bucket"):
        engine.run(bad)


@pytest.mark.parametrize("agg", ["MIN", "MAX", "SUM", "AVG", "COUNT", "MEDIAN", "P90"])
def test_06_aggregations_vs_oracle(engine, agg):
    q = BusinessQuery(
        entity_ids=[METER_A], metric="power", group_by=("entity",), aggregations=(agg,)
    )
    got = float(engine.run(q)["value"].iloc[0])
    exp = _expected_agg(100.0 + np.arange(48), agg)
    assert got == pytest.approx(exp, rel=1e-9)


def test_07_alias(engine):
    q = BusinessQuery(
        entity_ids=[METER_A],
        metric="power",
        group_by=("entity",),
        aggregations=("AVG",),
        metric_alias="puissance",
        agg_aliases={"AVG": "moyenne"},
    )
    r = engine.run(q)
    assert r["metric_label"].iloc[0] == "puissance" and r["agg"].iloc[0] == "moyenne"


def test_08_utc_tz(engine):
    q = BusinessQuery(
        entity_ids=[METER_A],
        metric="power",
        start=pd.Timestamp("2024-01-01T00:00Z"),
        end=pd.Timestamp("2024-01-02T00:00Z"),
        group_by=("time",),
        aggregations=("COUNT",),
        display_tz="Europe/Paris",
    )
    r = engine.run(q)
    assert str(r["bucket"].dt.tz) == "Europe/Paris"
    q2 = BusinessQuery(
        entity_ids=[METER_A], metric="power", group_by=("entity",), aggregations=("COUNT",)
    )
    assert int(engine.run(q2)["value"].iloc[0]) == 48


def test_08b_dst_paris(engine):
    spring = pd.date_range("2024-03-30", periods=72, freq="1h", tz="UTC")
    tel = {(METER_A, "power"): pd.DataFrame({"ts": spring, "value": np.arange(72, dtype=float)})}
    eng = BusinessQueryEngine(_graph(), tel)
    base = BusinessQuery(
        entity_ids=[METER_A],
        metric="power",
        group_by=("time",),
        bucket="1D",
        aggregations=("COUNT",),
    )
    utc_buckets = eng.run(base)["bucket"]
    paris = eng.run(
        BusinessQuery(
            entity_ids=[METER_A],
            metric="power",
            group_by=("time",),
            bucket="1D",
            aggregations=("COUNT",),
            display_tz="Europe/Paris",
        )
    )
    assert utc_buckets.dt.tz is not None and str(utc_buckets.dt.tz) == "UTC"
    assert str(paris["bucket"].dt.tz) == "Europe/Paris"
    assert int(paris["value"].sum()) == 72
    assert paris["bucket"].dt.tz_convert("UTC").equals(utc_buckets.reset_index(drop=True))
    autumn = pd.date_range("2024-10-26", periods=72, freq="1h", tz="UTC")
    tel2 = {(METER_A, "power"): pd.DataFrame({"ts": autumn, "value": np.arange(72, dtype=float)})}
    eng2 = BusinessQueryEngine(_graph(), tel2)
    r2 = eng2.run(
        BusinessQuery(
            entity_ids=[METER_A],
            metric="power",
            group_by=("time",),
            bucket="1D",
            aggregations=("COUNT",),
            display_tz="Europe/Paris",
        )
    )
    assert int(r2["value"].sum()) == 72


def test_09_stats_rules():
    assert describe_series([])["n"] == 0 and describe_series([])["min"] is None
    single = describe_series([5.0])
    assert single == {
        "n": 1,
        "min": 5.0,
        "max": 5.0,
        "mean": 5.0,
        "std": None,
        "sum": 5.0,
        "null_count": 0,
    }
    d = describe_series([1.0, None, float("nan"), 3.0])
    assert d["n"] == 2 and d["null_count"] == 2 and d["sum"] == 4.0
    assert describe_series([7.0, 7.0, 7.0])["std"] == 0.0


def test_10_stats_vs_pandas_oracle():
    vals = [1.0, 2.0, 3.0, 4.0, None, float("nan")]
    got = describe_series(vals)
    s = pd.Series(vals, dtype="float64").dropna()
    assert got["mean"] == pytest.approx(float(s.mean()))
    assert got["std"] == pytest.approx(float(s.std(ddof=1)))
    assert got["sum"] == pytest.approx(float(s.sum()))


def test_11_isolation_idempotence(engine):
    qa = BusinessQuery(
        entity_ids=[METER_A], metric="power", group_by=("entity",), aggregations=("SUM",)
    )
    qb = BusinessQuery(
        entity_ids=[METER_B], metric="power", group_by=("entity",), aggregations=("SUM",)
    )
    va = float(engine.run(qa)["value"].iloc[0])
    vb = float(engine.run(qb)["value"].iloc[0])
    assert va != vb
    assert engine.run(qa).equals(engine.run(qa))
    assert METER_A not in engine.run(qb)["entity"].tolist()
