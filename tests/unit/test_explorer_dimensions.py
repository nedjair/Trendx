"""Dimensions/drill-down/compare (qsql jetable PG16 127.0.0.1:55438)."""

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

PW = os.environ.get("TRENDX_DIMS_PW", "trendx_dims_pw")
needs_pg = pytest.mark.skipif(os.environ.get("TRENDX_DIMS_DSN") != "1", reason="PG jetable requis")


def _auth():
    return {"Authorization": f"Bearer {settings.trendx_api_token.get_secret_value()}"}


@pytest.fixture(scope="module")
def client():
    eng = create_engine(
        f"postgresql://trendx_dims:{PW}@127.0.0.1:55438/trendx_dims", pool_pre_ping=True
    )
    ensure_schema(eng)
    ts = pd.date_range("2024-01-01", periods=72, freq="1h", tz="UTC")
    with eng.begin() as c:
        c.execute(text("DELETE FROM qsql_telemetry"))
        c.execute(text("DELETE FROM qsql_relation"))
        c.execute(text("DELETE FROM qsql_entity"))
        for eid, typ, prof, cust, attrs in [
            ("b1", "Building", "campus", "cust1", {}),
            ("aA", "Apartment", "flat", "cust1", {}),
            ("aB", "Apartment", "flat", "cust2", {}),
            ("mA", "Meter", "meter", "cust1", {"zone": "north"}),
            ("mB", "Meter", "meter", "cust2", {"zone": "south"}),
            ("mC", "Meter", "meter", "cust1", {"zone": "north"}),
        ]:
            c.execute(
                text("INSERT INTO qsql_entity VALUES (:i,:t,:p,:c,:a)"),
                {"i": eid, "t": typ, "p": prof, "c": cust, "a": json.dumps(attrs)},
            )
        for f, t_ in [("b1", "aA"), ("b1", "aB"), ("aA", "mA"), ("aA", "mC"), ("aB", "mB")]:
            c.execute(
                text("INSERT INTO qsql_relation VALUES (:f,:t,'Contains')"), {"f": f, "t": t_}
            )
        rows = []
        for i in range(72):
            rows.append({"e": "mA", "k": "power", "t": ts[i], "v": 100.0 + i})
            rows.append({"e": "mB", "k": "power", "t": ts[i], "v": 200.0 + i})
            rows.append({"e": "mC", "k": "power", "t": ts[i], "v": 300.0})
        rows.append({"e": "mA", "k": "power", "t": pd.Timestamp("2024-01-04T00:00Z"), "v": None})
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
        "entity_ids": ["mA"],
        "metric": "power",
        "group_by": ["entity"],
        "aggregations": ["AVG"],
    }
    base.update(kw)
    return client.post("/api/v1/explorer/query", json=base, headers=_auth())


@needs_pg
def test_a_profile_groupby(client):
    r = _q(client, entity_ids=["mA", "mB", "mC"], group_by=["profile"], aggregations=["COUNT"])
    assert r.json()["rows"][0]["profile"] == "meter"
    assert int(r.json()["rows"][0]["value"]) == 216


@needs_pg
def test_a_profile_filter(client):
    r = _q(client, entity_ids=["mA", "mB"], attr_filters={"zone": "north"})
    assert [row["entity"] for row in r.json()["rows"]] == ["mA"]


@needs_pg
def test_b_customer_groupby_isolation(client):
    r = _q(client, entity_ids=["mA", "mB", "mC"], group_by=["customer"], aggregations=["SUM"])
    got = {row["customer"]: row["value"] for row in r.json()["rows"]}
    assert set(got) == {"cust1", "cust2"}
    assert got["cust1"] == pytest.approx(float(np.sum(100.0 + np.arange(72)) + 72 * 300.0))
    assert got["cust2"] == pytest.approx(float(np.sum(200.0 + np.arange(72))))


@needs_pg
def test_c_drilldown_parent_children(client):
    r = client.get(
        "/api/v1/explorer/drilldown", params={"entity_id": "aA", "depth": 1}, headers=_auth()
    )
    assert r.status_code == 200 and sorted(r.json()["descendants"]) == ["mA", "mC"]


@needs_pg
def test_c_drilldown_depth_and_determinism(client):
    r1 = client.get(
        "/api/v1/explorer/drilldown", params={"entity_id": "b1", "depth": 3}, headers=_auth()
    ).json()
    r2 = client.get(
        "/api/v1/explorer/drilldown", params={"entity_id": "b1", "depth": 3}, headers=_auth()
    ).json()
    assert r1 == r2
    assert sorted(r1["descendants"]) == ["aA", "aB", "mA", "mB", "mC"]
    d1 = client.get(
        "/api/v1/explorer/drilldown", params={"entity_id": "b1", "depth": 1}, headers=_auth()
    ).json()
    assert sorted(d1["descendants"]) == ["aA", "aB"]


@needs_pg
def test_c_drilldown_sister_no_leak(client):
    r = client.get(
        "/api/v1/explorer/drilldown", params={"entity_id": "aB", "depth": 3}, headers=_auth()
    ).json()
    assert r["descendants"] == ["mB"]
    bad = client.get(
        "/api/v1/explorer/drilldown", params={"entity_id": "aA", "depth": 0}, headers=_auth()
    )
    assert bad.status_code == 422


@needs_pg
def test_d_compare_entities(client):
    r = client.post(
        "/api/v1/explorer/compare",
        json={
            "left": {"entity_ids": ["mA"]},
            "right": {"entity_ids": ["mB"]},
            "metric": "power",
            "aggregation": "AVG",
        },
        headers=_auth(),
    )
    body = r.json()
    assert r.status_code == 200 and body["aggregation"] == "AVG"
    assert body["left"][0]["entity"] == "mA" and body["right"][0]["entity"] == "mB"
    assert body["left"][0]["value"] != body["right"][0]["value"]


@needs_pg
def test_d_compare_customers(client):
    r = client.post(
        "/api/v1/explorer/compare",
        json={
            "left": {"customer": "cust1"},
            "right": {"customer": "cust2"},
            "metric": "power",
            "start": "2024-01-01T00:00:00Z",
            "end": "2024-01-02T00:00:00Z",
            "aggregation": "SUM",
        },
        headers=_auth(),
    )
    body = r.json()
    assert {row["entity"] for row in body["left"]} == {"mA", "mC"}
    assert {row["entity"] for row in body["right"]} == {"mB"}


@needs_pg
def test_d_compare_profiles_window(client):
    r = client.post(
        "/api/v1/explorer/compare",
        json={
            "left": {"profile": "meter"},
            "right": {"entity_ids": ["mB"]},
            "metric": "power",
            "start": "2024-01-01T00:00:00Z",
            "end": "2024-01-02T00:00:00Z",
            "aggregation": "COUNT",
        },
        headers=_auth(),
    )
    body = r.json()
    assert sum(row["value"] for row in body["left"]) == 72
    assert sum(row["value"] for row in body["right"]) == 24


@needs_pg
def test_e_combos(client):
    r = _q(
        client,
        entity_ids=["mA", "mB", "mC"],
        group_by=["customer", "time"],
        bucket="1D",
        aggregations=["COUNT"],
    )
    assert set(row["customer"] for row in r.json()["rows"]) == {"cust1", "cust2"}
    r2 = _q(
        client, entity_ids=["mA", "mB"], group_by=["profile", "metric"], aggregations=["MEDIAN"]
    )
    assert r2.json()["n_rows"] == 1
    exp = float(np.median(np.concatenate([100.0 + np.arange(72), 200.0 + np.arange(72)])))
    got = [row for row in r2.json()["rows"] if row["profile"] == "meter"]
    assert got[0]["value"] == pytest.approx(exp)
