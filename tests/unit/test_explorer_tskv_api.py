"""Explorer sur schema prod-shape (catalog 001 + ts_kv 009), PG16 jetable 127.0.0.1:55436."""

from __future__ import annotations

import os
import uuid

import numpy as np
import pandas as pd
import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, text
from trendx.config import settings
from trendx.explorer import service as explorer_service
from trendx.explorer.tskv import ensure_tskv_schema
from trendx.main import app

PW = os.environ.get("TRENDX_TSKV_PW", "trendx_tskv_pw")
TENANT = str(uuid.uuid4())
BE_B, BE_A, BE_C, DEV_A, DEV_B = (str(uuid.uuid4()) for _ in range(5))

needs_pg = pytest.mark.skipif(
    os.environ.get("TRENDX_TSKV_DSN") != "1", reason="PG jetable requis (TRENDX_TSKV_DSN=1)"
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
def client():
    eng = create_engine(
        f"postgresql://trendx_tskv:{PW}@127.0.0.1:55436/trendx_tskv", pool_pre_ping=True
    )
    ensure_tskv_schema(eng)
    ts = pd.date_range("2024-01-01", periods=96, freq="1h", tz="UTC")
    spring = pd.date_range("2024-03-30", periods=48, freq="1h", tz="UTC")
    autumn = pd.date_range("2024-10-26", periods=48, freq="1h", tz="UTC")
    with eng.begin() as c:
        c.execute(text("DELETE FROM trendx_analytics.ts_kv"))
        c.execute(text("DELETE FROM trendx_catalog.relation"))
        c.execute(text("DELETE FROM trendx_catalog.business_entity_field"))
        c.execute(text("DELETE FROM trendx_catalog.business_entity"))
        for eid, name in [
            (BE_B, "Building B"),
            (BE_A, "Apartment A"),
            (BE_C, "Apartment B"),
            (DEV_A, "Meter A"),
            (DEV_B, "Meter B"),
        ]:
            c.execute(
                text("INSERT INTO trendx_catalog.business_entity VALUES (:i,:n,:t)"),
                {"i": eid, "n": name, "t": TENANT},
            )
        for fid, name, be, typ in [
            (str(uuid.uuid4()), "power", DEV_A, "meter"),
            (str(uuid.uuid4()), "temp", DEV_A, "meter"),
            (str(uuid.uuid4()), "power", DEV_B, "meter"),
            (str(uuid.uuid4()), "temp", DEV_B, "meter"),
        ]:
            c.execute(
                text("INSERT INTO trendx_catalog.business_entity_field VALUES (:i,:n,:b,:t)"),
                {"i": fid, "n": name, "b": be, "t": typ},
            )
        for f, t_, rel in [
            (BE_B, BE_A, "Contains"),
            (BE_B, BE_C, "Contains"),
            (BE_A, DEV_A, "Contains"),
            (BE_C, DEV_B, "Contains"),
        ]:
            c.execute(
                text("INSERT INTO trendx_catalog.relation VALUES (:f,:r,:t,'OUT',TRUE)"),
                {"f": f, "r": rel, "t": t_},
            )
        rows = []
        for i in range(96):
            rows.append({"e": DEV_A, "k": "power", "t": ts[i], "v": 100.0 + i})
            rows.append({"e": DEV_B, "k": "power", "t": ts[i], "v": 200.0 + i * 2})
        for i in range(48):
            rows.append({"e": DEV_A, "k": "dst", "t": spring[i], "v": float(i)})
            rows.append({"e": DEV_A, "k": "dst", "t": autumn[i], "v": float(i)})
        rows.append({"e": DEV_A, "k": "power", "t": pd.Timestamp("2024-01-05T00:00Z"), "v": None})
        rows.append({"e": DEV_A, "k": "const", "t": ts[0], "v": 7.0})
        rows.append({"e": DEV_A, "k": "const", "t": ts[1], "v": 7.0})
        c.execute(text("INSERT INTO trendx_analytics.ts_kv VALUES (:t,:e,:k,:v)"), rows)
    explorer_service.configure(eng, backend="tskv")
    with TestClient(app) as cli:
        yield cli
    explorer_service.configure(None)
    with eng.begin() as c:
        c.execute(text("DELETE FROM trendx_analytics.ts_kv"))
        c.execute(text("DELETE FROM trendx_catalog.relation"))
        c.execute(text("DELETE FROM trendx_catalog.business_entity_field"))
        c.execute(text("DELETE FROM trendx_catalog.business_entity"))
    eng.dispose()


def _q(client, **kw):
    base = {
        "entity_ids": [DEV_A],
        "metric": "power",
        "group_by": ["entity"],
        "aggregations": ["AVG"],
    }
    base.update(kw)
    return client.post("/api/v1/explorer/query", json=base, headers=_auth())


@needs_pg
def test_a_catalog_tskv(client):
    body = client.get("/api/v1/explorer/catalog", headers=_auth()).json()
    assert {e["id"] for e in body["entities"]} == {BE_B, BE_A, BE_C, DEV_A, DEV_B}
    assert set(body["metrics"]) >= {"power", "temp", "dst", "const"}
    assert len(body["relations"]) == 4
    assert body["dimensions"]["customer"] == [TENANT]


@needs_pg
def test_b_availability_tskv(client):
    r = client.get(
        "/api/v1/explorer/availability",
        params={"entity_id": DEV_A, "metric": "power"},
        headers=_auth(),
    ).json()
    assert r["n_points"] == 97
    r2 = client.get(
        "/api/v1/explorer/availability",
        params={"entity_id": str(uuid.uuid4()), "metric": "power"},
        headers=_auth(),
    ).json()
    assert r2["n_points"] == 0


@needs_pg
def test_c_stats_tskv(client):
    b = client.get(
        "/api/v1/explorer/stats", params={"entity_id": DEV_A, "metric": "power"}, headers=_auth()
    ).json()
    assert b["n"] == 96 and b["min"] == 100.0 and b["max"] == 195.0
    assert b["null_count"] == 1


@needs_pg
def test_d_query_simple(client):
    assert _q(client).status_code == 200


@needs_pg
def test_e_attr_filter_name(client):
    r = _q(client, entity_ids=[DEV_A, DEV_B], attr_filters={"name": "Meter A"})
    assert [row["entity"] for row in r.json()["rows"]] == [DEV_A]


@needs_pg
def test_f_telemetry_filter(client):
    r = _q(client, telemetry_op=["gt", 150.0], aggregations=["COUNT"])
    assert int(r.json()["rows"][0]["value"]) == 45


@needs_pg
def test_g_groupby_entity(client):
    assert len(_q(client, entity_ids=[DEV_A, DEV_B]).json()["rows"]) == 2


@needs_pg
def test_h_groupby_time(client):
    assert _q(client, group_by=["time"], bucket="1H", aggregations=["COUNT"]).json()["n_rows"] == 97
    r = _q(client, group_by=["time"], bucket="1D", aggregations=["COUNT"]).json()
    assert sorted(row["value"] for row in r["rows"]) == [0, 24, 24, 24, 24]


@needs_pg
@pytest.mark.parametrize(
    "agg", ["MIN", "MAX", "SUM", "AVG", "COUNT", "MEDIAN", "P90", "P95", "P99", "UNIQ"]
)
def test_i_aggs_oracle(client, agg):
    r = _q(client, entity_ids=[DEV_B], aggregations=[agg])
    assert float(r.json()["rows"][0]["value"]) == pytest.approx(
        float(_oracle(200.0 + np.arange(96) * 2, agg)), rel=1e-6
    )


@needs_pg
def test_j_aliases(client):
    r = _q(client, metric_alias="puissance", agg_aliases={"AVG": "moyenne"})
    assert r.json()["rows"][0]["metric_label"] == "puissance"


@needs_pg
def test_k_timezone_dst(client):
    for start in ("2024-03-30T00:00:00Z", "2024-10-26T00:00:00Z"):
        end = (pd.Timestamp(start) + pd.Timedelta(hours=48)).isoformat()
        r = _q(
            client,
            metric="dst",
            start=start,
            end=end,
            group_by=["time"],
            bucket="1D",
            aggregations=["COUNT"],
            display_tz="Europe/Paris",
        )
        assert sum(row["value"] for row in r.json()["rows"]) == 48


@needs_pg
def test_l_null_empty_const_isolation(client):
    assert _q(client, entity_ids=[str(uuid.uuid4())]).json()["n_rows"] == 0
    r = client.get(
        "/api/v1/explorer/stats", params={"entity_id": DEV_A, "metric": "const"}, headers=_auth()
    ).json()
    assert r["std"] == 0.0
    a = _q(client, entity_ids=[DEV_A], aggregations=["SUM"]).json()
    b = _q(client, entity_ids=[DEV_B], aggregations=["SUM"]).json()
    assert a["rows"][0]["value"] != b["rows"][0]["value"]


@needs_pg
def test_m_auth_401(client):
    assert client.get("/api/v1/explorer/catalog").status_code == 401


@needs_pg
def test_n_invalid_422(client):
    assert _q(client, aggregations=["BOGUS"]).status_code == 422
    assert _q(client, group_by=["time"], bucket="1Y").status_code == 422
    assert _q(client, telemetry_op=["bogus", 1.0]).status_code == 422
    assert _q(client, display_tz="Mars/Olympus", group_by=["time"]).status_code == 422


@needs_pg
def test_o_qsql_compat_intact(client):
    import sqlalchemy as sa

    eng = sa.create_engine("sqlite://")
    from trendx.query.sql import ensure_schema

    ensure_schema(eng)
    explorer_service.configure(eng)
    try:
        r = client.get("/api/v1/explorer/catalog", headers=_auth())
        assert r.status_code == 200 and r.json()["entities"] == []
    finally:
        explorer_service.configure(None)
