"""SQL backend jetable (PG16 127.0.0.1:55434): contrat SQL <-> oracle pandas."""

from __future__ import annotations

import json
import os

import numpy as np
import pandas as pd
import pytest
from sqlalchemy import text
from trendx.query.engine import BusinessQuery
from trendx.query.sql import SqlQueryEngine, create_engine_ephemeral, ensure_schema

DSN = os.environ.get("TRENDX_QSQL_DSN", "")
PW = os.environ.get("TRENDX_QSQL_PW", "trendx_qsql_pw")

METER_A, METER_B = "mA", "mB"

needs_pg = pytest.mark.skipif(not DSN, reason="PostgreSQL jetable requis (TRENDX_QSQL_DSN)")


def _oracle_agg(values: np.ndarray, agg: str):
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
    if agg.startswith("P"):
        return float(np.percentile(v, float(agg[1:]), method="linear"))
    raise AssertionError(agg)


@pytest.fixture(scope="module")
def engine():
    if not DSN:
        pytest.skip("TRENDX_QSQL_DSN absent")
    eng = create_engine_ephemeral(f"postgresql://trendx_qsql:{PW}@127.0.0.1:55434/trendx_qsql")
    ensure_schema(eng)
    ts = pd.date_range("2024-01-01", periods=96, freq="1h", tz="UTC")
    entities = [
        ("b1", "Building", "campus", "cust1", {"city": "Paris"}),
        ("aA", "Apartment", "flat", "cust1", {"floor": 1, "zone": "north"}),
        ("aB", "Apartment", "flat", "cust1", {"floor": 2, "zone": "south"}),
        (METER_A, "Meter", "meter", "cust1", {"floor": 1, "zone": "north", "model": "X1"}),
        (METER_B, "Meter", "meter", "cust1", {"floor": 2, "zone": "south", "model": "X1"}),
    ]
    relations = [("b1", "aA"), ("b1", "aB"), ("aA", METER_A), ("aB", METER_B)]
    with eng.begin() as c:
        c.execute(text("DELETE FROM qsql_telemetry"))
        c.execute(text("DELETE FROM qsql_relation"))
        c.execute(text("DELETE FROM qsql_entity"))
        for eid, typ, prof, cust, attrs in entities:
            c.execute(
                text("INSERT INTO qsql_entity VALUES (:i,:t,:p,:c,:a)"),
                {"i": eid, "t": typ, "p": prof, "c": cust, "a": json.dumps(attrs)},
            )
        for f, t_ in relations:
            c.execute(
                text("INSERT INTO qsql_relation VALUES (:f,:t,'Contains')"), {"f": f, "t": t_}
            )
        rows = []
        for i in range(96):
            rows.append({"e": METER_A, "k": "power", "t": ts[i], "v": 100.0 + i})
            rows.append({"e": METER_B, "k": "power", "t": ts[i], "v": 200.0 + i * 2})
            rows.append({"e": METER_A, "k": "temp", "t": ts[i], "v": 20.0 + (i % 24) * 0.1})
            rows.append({"e": METER_B, "k": "temp", "t": ts[i], "v": 22.0 + (i % 24) * 0.1})
        rows.append({"e": METER_A, "k": "power", "t": pd.Timestamp("2024-01-05T00:00Z"), "v": None})
        c.execute(text("INSERT INTO qsql_telemetry VALUES (:e,:k,:t,:v)"), rows)
    yield SqlQueryEngine(eng)
    with eng.begin() as c:
        c.execute(text("DELETE FROM qsql_telemetry"))
        c.execute(text("DELETE FROM qsql_relation"))
        c.execute(text("DELETE FROM qsql_entity"))
    eng.dispose()


@needs_pg
def test_01_catalog(engine):
    with engine._engine.connect() as c:
        n = c.execute(text("SELECT count(*) FROM qsql_entity")).scalar()
        rel = c.execute(text("SELECT count(*) FROM qsql_relation")).scalar()
    assert n == 5 and rel == 4


@needs_pg
def test_02_relation_3_levels(engine):
    with engine._engine.connect() as c:
        path = c.execute(
            text("SELECT to_id FROM qsql_relation WHERE from_id='b1' ORDER BY to_id")
        ).fetchall()
    assert [r[0] for r in path] == ["aA", "aB"]


@needs_pg
def test_03_isolation(engine):
    a = engine.run(BusinessQuery([METER_A], "power", group_by=("entity",), aggregations=("COUNT",)))
    b = engine.run(BusinessQuery([METER_B], "power", group_by=("entity",), aggregations=("COUNT",)))
    assert int(a["value"].iloc[0]) == 96 and int(b["value"].iloc[0]) == 96


@needs_pg
@pytest.mark.parametrize(
    "cond,expected",
    [
        ({"zone": "north"}, [METER_A]),
        ({"zone": ("eq", "south")}, [METER_B]),
        ({"floor": ("gt", 1)}, [METER_B]),
        ({"floor": ("gte", 2)}, [METER_B]),
        ({"floor": ("lt", 2)}, [METER_A]),
        ({"floor": ("lte", 1)}, [METER_A]),
        ({"zone": ("north", "south")}, [METER_A, METER_B]),
    ],
)
def test_04_attr_filters(engine, cond, expected):
    r = engine.run(
        BusinessQuery(
            [METER_A, METER_B],
            "power",
            attr_filters=cond,
            group_by=("entity",),
            aggregations=("COUNT",),
        )
    )
    assert sorted(r["entity"].tolist()) == expected


@needs_pg
def test_06_telemetry_filters(engine):
    r = engine.run(
        BusinessQuery(
            [METER_A],
            "power",
            telemetry_op=("gt", 150.0),
            group_by=("entity",),
            aggregations=("COUNT",),
        )
    )
    assert int(r["value"].iloc[0]) == 45


@needs_pg
def test_07_utc_bounds(engine):
    r = engine.run(
        BusinessQuery(
            [METER_A],
            "power",
            start=pd.Timestamp("2024-01-01T00:00Z"),
            end=pd.Timestamp("2024-01-02T00:00Z"),
            group_by=("entity",),
            aggregations=("COUNT",),
        )
    )
    assert int(r["value"].iloc[0]) == 24


@needs_pg
def test_08_groupby_entity(engine):
    r = engine.run(
        BusinessQuery([METER_A, METER_B], "power", group_by=("entity",), aggregations=("SUM",))
    )
    assert len(r) == 2
    assert float(r.loc[r.entity == METER_A, "value"].iloc[0]) == pytest.approx(
        100 * 96 + sum(range(96))
    )


@needs_pg
def test_09_groupby_metric(engine):
    got = {}
    for metric in ("power", "temp"):
        r = engine.run(
            BusinessQuery([METER_A], metric, group_by=("metric",), aggregations=("COUNT",))
        )
        got[metric] = int(r["value"].iloc[0])
    assert got == {"power": 96, "temp": 96}


@needs_pg
def test_10_groupby_1h(engine):
    r = engine.run(
        BusinessQuery([METER_A], "power", group_by=("time",), bucket="1H", aggregations=("COUNT",))
    )
    assert len(r) == 97 and set(r["value"]) == {0, 1}


@needs_pg
def test_11_groupby_1d(engine):
    r = engine.run(
        BusinessQuery([METER_A], "power", group_by=("time",), bucket="1D", aggregations=("COUNT",))
    )
    assert len(r) == 5 and sorted(r["value"].tolist()) == [0, 24, 24, 24, 24]


@needs_pg
def test_12_profile_customer(engine):
    with engine._engine.connect() as c:
        profs = c.execute(text("SELECT DISTINCT profile FROM qsql_entity ORDER BY 1")).fetchall()
        custs = c.execute(text("SELECT DISTINCT customer FROM qsql_entity ORDER BY 1")).fetchall()
    assert [r[0] for r in profs] == ["campus", "flat", "meter"]
    assert [r[0] for r in custs] == ["cust1"]


@needs_pg
@pytest.mark.parametrize(
    "agg", ["MIN", "MAX", "SUM", "AVG", "COUNT", "MEDIAN", "P90", "P95", "P99", "UNIQ"]
)
def test_13_17_aggs_vs_oracle(engine, agg):
    r = engine.run(BusinessQuery([METER_B], "power", group_by=("entity",), aggregations=(agg,)))
    got = r["value"].iloc[0]
    exp = _oracle_agg(200.0 + np.arange(96) * 2, agg)
    assert float(got) == pytest.approx(float(exp), rel=1e-6)


@needs_pg
def test_18_aliases(engine):
    r = engine.run(
        BusinessQuery(
            [METER_A],
            "power",
            group_by=("entity",),
            aggregations=("AVG",),
            metric_alias="puissance",
            agg_aliases={"AVG": "moyenne"},
        )
    )
    assert r["metric_label"].iloc[0] == "puissance" and r["agg"].iloc[0] == "moyenne"


@needs_pg
def test_19_null_empty(engine):
    r = engine.run(BusinessQuery(["nope"], "power", group_by=("entity",), aggregations=("AVG",)))
    assert r.empty
    c = engine.run(BusinessQuery([METER_A], "power", group_by=("entity",), aggregations=("COUNT",)))
    assert int(c["value"].iloc[0]) == 96


@needs_pg
def test_20_dst(engine):
    r = engine.run(
        BusinessQuery(
            [METER_A],
            "power",
            group_by=("time",),
            bucket="1D",
            aggregations=("COUNT",),
            display_tz="Europe/Paris",
        )
    )
    assert str(r["bucket"].dt.tz) == "Europe/Paris" and int(r["value"].sum()) == 96


@needs_pg
def test_21_determinism(engine):
    q = BusinessQuery([METER_A, METER_B], "temp", group_by=("entity",), aggregations=("AVG", "P90"))
    assert engine.run(q).equals(engine.run(q))


@needs_pg
def test_22_full_oracle_matrix(engine):
    for metric, base, step in (("power", 100.0, 1.0), ("temp", 20.0, 0.1)):
        vals = base + (np.arange(96) % 24) * step if metric == "temp" else base + np.arange(96)
        for agg in ("MIN", "MAX", "SUM", "AVG", "MEDIAN", "P90"):
            r = engine.run(
                BusinessQuery([METER_A], metric, group_by=("entity",), aggregations=(agg,))
            )
            assert float(r["value"].iloc[0]) == pytest.approx(
                float(_oracle_agg(vals, agg)), rel=1e-6
            )


@needs_pg
def test_20b_dst_transitions(engine):
    spring = pd.date_range("2024-03-30", periods=72, freq="1h", tz="UTC")
    autumn = pd.date_range("2024-10-26", periods=72, freq="1h", tz="UTC")
    with engine._engine.begin() as c:
        rows = [{"e": METER_A, "k": "dst", "t": t, "v": float(i)} for i, t in enumerate(spring)]
        rows += [{"e": METER_A, "k": "dst", "t": t, "v": float(i)} for i, t in enumerate(autumn)]
        c.execute(text("INSERT INTO qsql_telemetry VALUES (:e,:k,:t,:v)"), rows)
    try:
        for start, label in (("2024-03-30", "spring"), ("2024-10-26", "autumn")):
            q = BusinessQuery(
                [METER_A],
                "dst",
                start=pd.Timestamp(f"{start}T00:00Z"),
                end=pd.Timestamp(f"{start}T00:00Z") + pd.Timedelta(hours=72),
                group_by=("time",),
                bucket="1D",
                aggregations=("COUNT",),
                display_tz="Europe/Paris",
            )
            r = engine.run(q)
            assert int(r["value"].sum()) == 72, label
            assert str(r["bucket"].dt.tz) == "Europe/Paris", label
            utc = engine.run(
                BusinessQuery(
                    [METER_A],
                    "dst",
                    start=pd.Timestamp(f"{start}T00:00Z"),
                    end=pd.Timestamp(f"{start}T00:00Z") + pd.Timedelta(hours=72),
                    group_by=("time",),
                    bucket="1D",
                    aggregations=("COUNT",),
                )
            )
            assert str(utc["bucket"].dt.tz) == "UTC", label
            assert (
                r["bucket"].dt.tz_convert("UTC").equals(utc["bucket"].reset_index(drop=True))
            ), label
    finally:
        with engine._engine.begin() as c:
            c.execute(text("DELETE FROM qsql_telemetry WHERE metric='dst'"))


def test_23_duplicate_alias_rejected():
    from trendx.query.sql import SqlQueryEngine

    eng = SqlQueryEngine.__new__(SqlQueryEngine)
    with pytest.raises(ValueError, match="Duplicate aggregation alias"):
        eng.run(
            BusinessQuery([METER_A], "power", group_by=("entity",), aggregations=("P90", "p90"))
        )
