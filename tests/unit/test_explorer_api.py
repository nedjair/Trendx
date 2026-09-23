"""Explorer API (TestClient reel + PG16 jetable 127.0.0.1:55435)."""

from __future__ import annotations

import json
import os

import numpy as np
import pandas as pd
import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, text
from trendx.config import settings
from trendx.explorer import service as explorer_service
from trendx.main import app
from trendx.query.sql import ensure_schema

PW = os.environ.get("TRENDX_EXP_PW", "trendx_exp_pw")
DSN = "postgresql://trendx_exp:PLACEHOLDER@127.0.0.1:55435/trendx_exp"
METER_A, METER_B = "mA", "mB"

needs_pg = pytest.mark.skipif(
    os.environ.get("TRENDX_EXP_DSN") != "1", reason="PG jetable requis (TRENDX_EXP_DSN=1)"
)


def _auth():
    return {"Authorization": f"Bearer {settings.trendx_api_token.get_secret_value()}"}


def _oracle(values, agg):
    v = np.asarray(values, dtype=float)
    v = v[~np.isnan(v)]
    if agg == "COUNT":
        return int(len(v))
    if agg == "UNIQ":
        return int(len(np.unique(v)))
    if len(v) == 0:
        return None
    return {
        "MIN": float(np.min(v)),
        "MAX": float(np.max(v)),
        "SUM": float(np.sum(v)),
        "AVG": float(np.mean(v)),
        "MEDIAN": float(np.median(v)),
    }.get(agg, float(np.percentile(v, float(agg[1:]), method="linear")))


@pytest.fixture(scope="module")
def client():
    eng = create_engine(DSN.replace("PLACEHOLDER", PW), pool_pre_ping=True)
    ensure_schema(eng)
    ts = pd.date_range("2024-01-01", periods=96, freq="1h", tz="UTC")
    spring = pd.date_range("2024-03-30", periods=48, freq="1h", tz="UTC")
    autumn = pd.date_range("2024-10-26", periods=48, freq="1h", tz="UTC")
    with eng.begin() as c:
        c.execute(text("DELETE FROM qsql_telemetry"))
        c.execute(text("DELETE FROM qsql_relation"))
        c.execute(text("DELETE FROM qsql_entity"))
        for eid, typ, prof, attrs in [
            ("b1", "Building", "campus", {"city": "Paris"}),
            ("aA", "Apartment", "flat", {"floor": 1, "zone": "north"}),
            ("aB", "Apartment", "flat", {"floor": 2, "zone": "south"}),
            (METER_A, "Meter", "meter", {"floor": 1, "zone": "north"}),
            (METER_B, "Meter", "meter", {"floor": 2, "zone": "south"}),
        ]:
            c.execute(
                text("INSERT INTO qsql_entity VALUES (:i,:t,:p,'cust1',:a)"),
                {"i": eid, "t": typ, "p": prof, "a": json.dumps(attrs)},
            )
        for f, t_ in [("b1", "aA"), ("b1", "aB"), ("aA", METER_A), ("aB", METER_B)]:
            c.execute(
                text("INSERT INTO qsql_relation VALUES (:f,:t,'Contains')"), {"f": f, "t": t_}
            )
        rows = []
        for i in range(96):
            rows.append({"e": METER_A, "k": "power", "t": ts[i], "v": 100.0 + i})
            rows.append({"e": METER_B, "k": "power", "t": ts[i], "v": 200.0 + i * 2})
        for i in range(48):
            rows.append({"e": METER_A, "k": "dst", "t": spring[i], "v": float(i)})
            rows.append({"e": METER_A, "k": "dst", "t": autumn[i], "v": float(i)})
        rows.append({"e": METER_A, "k": "const", "t": ts[0], "v": 7.0})
        rows.append({"e": METER_A, "k": "const", "t": ts[1], "v": 7.0})
        c.execute(text("INSERT INTO qsql_telemetry VALUES (:e,:k,:t,:v)"), rows)
    explorer_service.configure(eng)
    with TestClient(app) as cli:
        yield cli
    explorer_service.configure(None)
    with eng.begin() as c:
        c.execute(text("DELETE FROM qsql_telemetry"))
        c.execute(text("DELETE FROM qsql_relation"))
        c.execute(text("DELETE FROM qsql_entity"))
    eng.dispose()


def _q(client, **kw):
    base = {
        "entity_ids": [METER_A],
        "metric": "power",
        "group_by": ["entity"],
        "aggregations": ["AVG"],
    }
    base.update(kw)
    return client.post("/api/v1/explorer/query", json=base, headers=_auth())


@needs_pg
def test_01_catalog(client):
    r = client.get("/api/v1/explorer/catalog", headers=_auth())
    assert r.status_code == 200
    body = r.json()
    assert {e["id"] for e in body["entities"]} == {"b1", "aA", "aB", METER_A, METER_B}
    assert set(body["metrics"]) >= {"power", "dst", "const"}
    assert len(body["relations"]) == 4
    assert body["dimensions"]["profile"] == ["campus", "flat", "meter"]


@needs_pg
def test_02_availability(client):
    r = client.get(
        "/api/v1/explorer/availability",
        params={"entity_id": METER_A, "metric": "power"},
        headers=_auth(),
    )
    assert r.status_code == 200 and r.json()["n_points"] == 96
    r2 = client.get(
        "/api/v1/explorer/availability",
        params={"entity_id": "nope", "metric": "power"},
        headers=_auth(),
    )
    assert r2.json()["n_points"] == 0 and r2.json()["start_ts"] is None


@needs_pg
def test_03_stats_nominal(client):
    r = client.get(
        "/api/v1/explorer/stats", params={"entity_id": METER_A, "metric": "power"}, headers=_auth()
    )
    b = r.json()
    assert b["n"] == 96 and b["min"] == 100.0 and b["max"] == 195.0
    assert b["mean"] == pytest.approx(float(np.mean(100.0 + np.arange(96))))
    assert b["sum"] == pytest.approx(float(np.sum(100.0 + np.arange(96))))
    assert b["null_count"] == 0


@needs_pg
def test_04_stats_empty(client):
    r = client.get(
        "/api/v1/explorer/stats", params={"entity_id": "nope", "metric": "power"}, headers=_auth()
    )
    assert r.json()["n"] == 0 and r.json()["min"] is None


@needs_pg
def test_05_stats_null_nan(client):
    from trendx.query.stats import describe_series

    d = describe_series([1.0, None, float("nan"), 3.0])
    assert d["n"] == 2 and d["null_count"] == 2


@needs_pg
def test_06_stats_constant(client):
    r = client.get(
        "/api/v1/explorer/stats", params={"entity_id": METER_A, "metric": "const"}, headers=_auth()
    )
    assert r.json()["std"] == 0.0 and r.json()["n"] == 2


@needs_pg
def test_07_query_simple(client):
    r = _q(client)
    assert r.status_code == 200 and r.json()["n_rows"] == 1


@needs_pg
def test_08_query_multi_device(client):
    r = _q(client, entity_ids=[METER_A, METER_B], aggregations=["SUM"])
    body = r.json()
    assert body["n_rows"] == 2
    got = {row["entity"]: row["value"] for row in body["rows"]}
    assert got[METER_A] == pytest.approx(float(np.sum(100.0 + np.arange(96))))
    assert got[METER_B] == pytest.approx(float(np.sum(200.0 + np.arange(96) * 2)))


@needs_pg
def test_09_isolation(client):
    a = _q(client, entity_ids=[METER_A], aggregations=["SUM"]).json()
    b = _q(client, entity_ids=[METER_B], aggregations=["SUM"]).json()
    assert a["rows"][0]["value"] != b["rows"][0]["value"]


@needs_pg
def test_10_attribute_filter(client):
    r = _q(client, entity_ids=[METER_A, METER_B], attr_filters={"zone": "north"})
    assert [row["entity"] for row in r.json()["rows"]] == [METER_A]


@needs_pg
def test_11_telemetry_filter(client):
    r = _q(client, telemetry_op=["gt", 150.0], aggregations=["COUNT"])
    assert int(r.json()["rows"][0]["value"]) == 45


@needs_pg
def test_12_groupby_entity(client):
    assert len(_q(client, entity_ids=[METER_A, METER_B]).json()["rows"]) == 2


@needs_pg
def test_13_groupby_metric(client):
    r = _q(client, group_by=["metric"])
    assert r.json()["rows"][0]["metric"] == "power"


@needs_pg
def test_14_bucket_1h(client):
    r = _q(client, group_by=["time"], bucket="1H", aggregations=["COUNT"])
    assert r.json()["n_rows"] == 96


@needs_pg
def test_15_bucket_1d(client):
    r = _q(client, group_by=["time"], bucket="1D", aggregations=["COUNT"])
    assert sorted(row["value"] for row in r.json()["rows"]) == [24, 24, 24, 24]


@needs_pg
def test_16_utc_bounds(client):
    r = _q(client, start="2024-01-01T00:00:00Z", end="2024-01-02T00:00:00Z", aggregations=["COUNT"])
    assert int(r.json()["rows"][0]["value"]) == 24


@needs_pg
def test_17_timezone_paris(client):
    r = _q(
        client, group_by=["time"], bucket="1D", aggregations=["COUNT"], display_tz="Europe/Paris"
    )
    assert "+01:00" in r.json()["rows"][0]["bucket"] or "+02:00" in r.json()["rows"][0]["bucket"]


@needs_pg
def test_18_dst_spring(client):
    r = _q(
        client,
        metric="dst",
        start="2024-03-30T00:00:00Z",
        end="2024-04-02T00:00:00Z",
        group_by=["time"],
        bucket="1D",
        aggregations=["COUNT"],
        display_tz="Europe/Paris",
    )
    assert sum(row["value"] for row in r.json()["rows"]) == 48


@needs_pg
def test_19_dst_fall(client):
    r = _q(
        client,
        metric="dst",
        start="2024-10-26T00:00:00Z",
        end="2024-10-29T00:00:00Z",
        group_by=["time"],
        bucket="1D",
        aggregations=["COUNT"],
        display_tz="Europe/Paris",
    )
    assert sum(row["value"] for row in r.json()["rows"]) == 48


@needs_pg
@pytest.mark.parametrize(
    "agg,exp",
    [
        ("MIN", 100.0),
        ("MAX", 195.0),
        ("SUM", float(np.sum(100.0 + np.arange(96)))),
        ("AVG", float(np.mean(100.0 + np.arange(96)))),
        ("COUNT", 96),
        ("MEDIAN", float(np.median(100.0 + np.arange(96)))),
        ("P90", float(np.percentile(100.0 + np.arange(96), 90, method="linear"))),
        ("P95", float(np.percentile(100.0 + np.arange(96), 95, method="linear"))),
        ("P99", float(np.percentile(100.0 + np.arange(96), 99, method="linear"))),
        ("UNIQ", 96),
    ],
)
def test_20_29_aggs_oracle(client, agg, exp):
    r = _q(client, aggregations=[agg])
    assert float(r.json()["rows"][0]["value"]) == pytest.approx(exp, rel=1e-6)


@needs_pg
def test_30_percentile_pn(client):
    r = _q(client, aggregations=["P50"])
    assert float(r.json()["rows"][0]["value"]) == pytest.approx(
        float(np.percentile(100.0 + np.arange(96), 50, method="linear")), rel=1e-6
    )


@needs_pg
def test_31_duplicate_alias(client):
    r = _q(client, aggregations=["P90", "p90"])
    assert r.status_code in (422, 500)


@needs_pg
def test_32_invalid_operator(client):
    r = _q(client, telemetry_op=["bogus", 1.0])
    assert r.status_code == 422


@needs_pg
def test_33_invalid_bucket(client):
    r = _q(client, group_by=["time"], bucket="1Y")
    assert r.status_code == 422


@needs_pg
def test_34_invalid_aggregation(client):
    r = _q(client, aggregations=["BOGUS"])
    assert r.status_code == 422


@needs_pg
def test_35_invalid_timezone(client):
    r = _q(client, group_by=["time"], display_tz="Mars/Olympus")
    assert r.status_code == 422


@needs_pg
def test_36_empty_result(client):
    r = _q(client, entity_ids=["nope"])
    body = r.json()
    assert r.status_code == 200 and body["n_rows"] == 0 and body["rows"] == []
    assert "columns" in body


def test_37_protected_without_token():
    with TestClient(app) as cli:
        assert cli.get("/api/v1/explorer/catalog").status_code == 401


def test_38_backend_unconfigured_returns_503():
    explorer_service.configure(None)
    try:
        with TestClient(app) as cli:
            token = settings.trendx_api_token.get_secret_value()
            r = cli.get("/api/v1/explorer/catalog", headers={"Authorization": f"Bearer {token}"})
            assert r.status_code == 503
    finally:
        explorer_service.configure(None)
