"""Hardening SQL (PG16 jetable 127.0.0.1:55437): buckets calendaires, aggs, dims, CTE, filtres."""

from __future__ import annotations

import os

import numpy as np
import pandas as pd
import pytest
from sqlalchemy import text
from trendx.query.engine import BusinessQuery
from trendx.query.sql import (
    SqlQueryEngine,
    create_engine_ephemeral,
    ensure_schema,
    resolve_descendants_cte,
)

PW = os.environ.get("TRENDX_QSH_PW", "trendx_sh_pw")
needs_pg = pytest.mark.skipif(os.environ.get("TRENDX_QSH_DSN") != "1", reason="PG jetable requis")

M_A, M_B = "mA", "mB"


def _oracle(values, agg):
    v = np.asarray(values, dtype=float)
    v = v[~np.isnan(v)]
    if agg == "COUNT":
        return int(len(v))
    if agg == "UNIQ":
        return int(len(np.unique(v)))
    if len(v) == 0:
        return None
    if agg == "MIN":
        return float(np.min(v))
    if agg == "MAX":
        return float(np.max(v))
    if agg == "SUM":
        return float(np.sum(v))
    if agg == "AVG":
        return float(np.mean(v))
    if agg == "MEDIAN":
        return float(np.median(v))
    return float(np.percentile(v, float(agg[1:]), method="linear"))


@pytest.fixture(scope="module")
def engine():
    if os.environ.get("TRENDX_QSH_DSN") != "1":
        pytest.skip("TRENDX_QSH_DSN absent")
    eng = create_engine_ephemeral(f"postgresql://trendx_sh:{PW}@127.0.0.1:55437/trendx_sh")
    ensure_schema(eng)
    cal = pd.date_range("2023-12-28", periods=24 * 40, freq="1h", tz="UTC")
    assert cal[0] == pd.Timestamp("2023-12-28T00:00Z") and cal[-1] < pd.Timestamp(
        "2024-02-06T00:00Z"
    )
    with eng.begin() as c:
        c.execute(text("DELETE FROM qsql_telemetry"))
        c.execute(text("DELETE FROM qsql_relation"))
        c.execute(text("DELETE FROM qsql_entity"))
        for eid, typ, prof, cust, attrs in [
            ("b1", "Building", "campus", "cust1", {"city": "Paris"}),
            ("aA", "Apartment", "flat", "cust1", {"floor": 1}),
            ("aB", "Apartment", "flat", "cust2", {"floor": 2}),
            (M_A, "Meter", "meter", "cust1", {"zone": "north"}),
            (M_B, "Meter", "meter", "cust2", {"zone": "south"}),
        ]:
            import json as _json

            c.execute(
                text("INSERT INTO qsql_entity VALUES (:i,:t,:p,:c,:a)"),
                {"i": eid, "t": typ, "p": prof, "c": cust, "a": _json.dumps(attrs)},
            )
        for f, t_ in [("b1", "aA"), ("b1", "aB"), ("aA", M_A), ("aB", M_B)]:
            c.execute(
                text("INSERT INTO qsql_relation VALUES (:f,:t,'Contains')"), {"f": f, "t": t_}
            )
        rows = []
        for i, tstamp in enumerate(cal):
            rows.append({"e": M_A, "k": "energy", "t": tstamp, "v": 10.0 + (i % 100) * 0.5})
            rows.append({"e": M_B, "k": "energy", "t": tstamp, "v": -5.0 + (i % 50) * 0.25})
        for i, tstamp in enumerate(cal[:48]):
            rows.append({"e": M_A, "k": "neg", "t": tstamp, "v": -100.0 + i * 0.5})
            rows.append({"e": M_A, "k": "dup", "t": tstamp, "v": float(i % 5)})
        rows.append({"e": M_A, "k": "energy", "t": pd.Timestamp("2024-02-06T00:00Z"), "v": None})
        c.execute(text("INSERT INTO qsql_telemetry VALUES (:e,:k,:t,:v)"), rows)
    yield SqlQueryEngine(eng), eng
    with eng.begin() as c:
        c.execute(text("DELETE FROM qsql_telemetry"))
        c.execute(text("DELETE FROM qsql_relation"))
        c.execute(text("DELETE FROM qsql_entity"))
    eng.dispose()


def _run(engine, **kw):
    base = {
        "entity_ids": [M_A],
        "metric": "energy",
        "group_by": ["entity"],
        "aggregations": ["AVG"],
    }
    base.update(kw)
    return engine.run(BusinessQuery(**base))


@needs_pg
def test_1w_calendar_boundaries(engine):
    eng, _ = engine
    r = _run(eng, group_by=["time"], bucket="1W", aggregations=["COUNT"])
    assert r["bucket"].dt.weekday.unique().tolist() == [0]
    assert int(r["value"].sum()) == 960
    jan1 = r.loc[r["bucket"] == pd.Timestamp("2024-01-01T00:00Z")]
    assert int(jan1["value"].iloc[0]) == 168


@needs_pg
def test_1m_month_year_boundaries(engine):
    eng, _ = engine
    r = _run(eng, group_by=["time"], bucket="1M", aggregations=["COUNT"])
    labels = r["bucket"].dt.strftime("%Y-%m").tolist()
    assert labels == ["2023-12", "2024-01", "2024-02"]
    assert int(r["value"].sum()) == 960


@needs_pg
def test_exact_boundary_belongs_to_next(engine):
    eng, _ = engine
    r = _run(
        eng,
        start=pd.Timestamp("2024-01-01T00:00Z"),
        end=pd.Timestamp("2024-01-01T01:00Z"),
        group_by=["time"],
        bucket="1H",
        aggregations=["COUNT"],
    )
    assert len(r) == 1 and int(r["value"].iloc[0]) == 1


@needs_pg
@pytest.mark.parametrize(
    "agg", ["MIN", "MAX", "SUM", "AVG", "COUNT", "MEDIAN", "P90", "P95", "P99", "UNIQ"]
)
def test_aggs_neg_decimals_oracle(engine, agg):
    eng, _ = engine
    vals = -5.0 + (np.arange(960) % 50) * 0.25
    r = _run(eng, entity_ids=[M_B], aggregations=[agg])
    assert float(r["value"].iloc[0]) == pytest.approx(float(_oracle(vals, agg)), rel=1e-6)


@needs_pg
def test_uniq_duplicates_null_empty(engine):
    eng, _ = engine
    r = _run(eng, metric="dup", aggregations=["UNIQ"])
    assert int(r["value"].iloc[0]) == 5
    c = _run(eng, aggregations=["COUNT"])
    assert int(c["value"].iloc[0]) == 960
    e = _run(eng, entity_ids=["nope"], aggregations=["AVG"])
    assert e.empty


@needs_pg
def test_groupby_profile_customer(engine):
    eng, _ = engine
    r = _run(eng, entity_ids=[M_A, M_B], group_by=["profile"], aggregations=["COUNT"])
    assert r.set_index("profile")["value"].to_dict() == {"meter": 1920}
    r2 = _run(eng, entity_ids=[M_A, M_B], group_by=["customer"], aggregations=["COUNT"])
    assert r2.set_index("customer")["value"].to_dict() == {"cust1": 960, "cust2": 960}
    r3 = _run(
        eng,
        entity_ids=[M_A, M_B],
        group_by=["customer", "time"],
        bucket="1D",
        aggregations=["COUNT"],
    )
    assert set(r3["customer"]) == {"cust1", "cust2"}


@needs_pg
def test_cte_descendants_vs_python_oracle(engine):
    eng, raw = engine
    assert resolve_descendants_cte(raw, "b1") == ["aA", "aB", M_A, M_B]
    assert resolve_descendants_cte(raw, "aA") == [M_A]
    assert resolve_descendants_cte(raw, M_A) == []
    assert resolve_descendants_cte(raw, "b1", max_depth=1) == ["aA", "aB"]
    with pytest.raises(ValueError, match="max_depth"):
        resolve_descendants_cte(raw, "b1", max_depth=0)
    # oracle python indépendant (BFS)
    edges = {"b1": ["aA", "aB"], "aA": [M_A], "aB": [M_B]}
    seen, queue, out = {"b1"}, ["b1"], []
    while queue:
        cur = queue.pop(0)
        for nxt in sorted(edges.get(cur, [])):
            if nxt not in seen:
                seen.add(nxt)
                out.append(nxt)
                queue.append(nxt)
    assert resolve_descendants_cte(raw, "b1") == sorted(out)


@needs_pg
def test_filters_combos_multidevice(engine):
    eng, _ = engine
    r = _run(
        eng,
        entity_ids=[M_A, M_B],
        attr_filters={"zone": ("ne", "north")},
        telemetry_op=("lte", 0.0),
        group_by=("entity",),
        aggregations=("COUNT",),
    )
    assert r["entity"].tolist() == [M_B]
    r2 = _run(
        eng,
        entity_ids=[M_A, M_B],
        attr_filters={"zone": ("north", "south")},
        start=pd.Timestamp("2024-01-01T00:00Z"),
        end=pd.Timestamp("2024-01-02T00:00Z"),
        group_by=("entity",),
        aggregations=("COUNT",),
    )
    assert sorted(r2["entity"].tolist()) == [M_A, M_B]
    assert set(r2["value"].tolist()) == {24}


@needs_pg
def test_aliases_stable_and_case(engine):
    eng, _ = engine
    q = dict(
        entity_ids=[M_A],
        group_by=["entity"],
        aggregations=["AVG", "P90"],
        metric_alias="energie",
        agg_aliases={"AVG": "moyenne"},
    )
    first, second = _run(eng, **q), _run(eng, **q)
    assert first.equals(second)
    assert set(first["agg"]) == {"moyenne", "P90"}
    with pytest.raises(ValueError, match="Duplicate"):
        _run(
            eng,
            entity_ids=[M_A],
            group_by=[
                "entity",
            ],
            aggregations=["P90", "p90"],
        )


@needs_pg
def test_timezone_dst_buckets(engine):
    eng, _ = engine
    r = _run(
        eng,
        start=pd.Timestamp("2023-12-31T00:00Z"),
        end=pd.Timestamp("2024-01-02T00:00Z"),
        group_by=["time"],
        bucket="1D",
        aggregations=["COUNT"],
        display_tz="Europe/Paris",
    )
    assert str(r["bucket"].dt.tz) == "Europe/Paris"
    assert int(r["value"].sum()) == 48
    u = _run(
        eng,
        start=pd.Timestamp("2023-12-31T00:00Z"),
        end=pd.Timestamp("2024-01-02T00:00Z"),
        group_by=["time"],
        bucket="1D",
        aggregations=["COUNT"],
    )
    assert str(u["bucket"].dt.tz) == "UTC"
