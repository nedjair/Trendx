"""Non-régression fix B1 : _store_telemetry alimentait ts_kv_latest (5 colonnes)
avec la clause VALUES 6 colonnes de ts_kv -> erreur SQL -> rollback total.

Prouvé ici sur PostgreSQL jetable + vraies migrations 001/005/009 :
- persistance nominale ts_kv + ts_kv_latest + checkpoints (Test A) ;
- overlap/idempotence déterministe (Test B) ;
- isolation inter-devices (Test C) ;
- échec reader -> dead_letter, SANS faux checkpoint (contrat §7).

Marqueur integration + fixture ephemeral_postgres : skip propre sans Docker.
"""

from __future__ import annotations

import asyncio
import os
import subprocess
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path
from urllib.parse import quote

import pytest

pytestmark = [pytest.mark.integration]

ROOT = Path(__file__).parents[2]
ENT_A = str(uuid.uuid4())
ENT_B = str(uuid.uuid4())
TENANT = str(uuid.uuid4())


def _apply_migration(params: dict[str, object], filename: str) -> None:
    env = dict(os.environ)
    env["PGHOST"] = str(params["host"])
    env["PGPORT"] = str(params["port"])
    env["PGUSER"] = str(params["user"])
    env["PGDATABASE"] = str(params["dbname"])
    env["PGPASSWORD"] = str(params["password"])
    subprocess.run(
        ["psql", "-v", "ON_ERROR_STOP=1", "-X", "-q", "-f", str(ROOT / "migrations" / filename)],
        env=env,
        check=True,
        capture_output=True,
        text=True,
    )


@pytest.fixture
def b1_managers(ephemeral_postgres, monkeypatch):
    """Migrations réelles + DatabaseManager frais sur PG jetable."""
    from trendx.database.connection import DatabaseManager

    params = ephemeral_postgres
    for f in (
        "001_trendz_native_schema.sql",
        "005_ingestion_checkpoints.sql",
        "009_native_ts_kv.sql",
    ):
        _apply_migration(params, f)
    dsn = (
        f"postgresql://{params['user']}:{quote(str(params['password']))}"
        f"@{params['host']}:{params['port']}/{params['dbname']}"
    )
    mgr = DatabaseManager()
    mgr.register("catalog", dsn, search_path="trendx_catalog,public")
    mgr.register("analytics", dsn, search_path="trendx_analytics,public")
    return mgr


class FakeReader:
    """Transport synthétique déterministe (2 devices, UTC horaire)."""

    def __init__(self, *, with_nan: bool = False) -> None:
        self._with_nan = with_nan

    async def read_historical(self, entity_type, entity_id, keys, start_ts, end_ts):
        from trendx.thingsboard.telemetry import TelemetryPoint

        out: dict[str, list] = {}
        step = 3600_000
        base = (start_ts // step) * step
        for key in keys:
            pts = []
            h = 0
            while base + h * step < end_ts and h < 12:
                if self._with_nan and entity_id == ENT_A and key == "temperature" and h == 3:
                    val = float("nan")
                elif key == "humidity":
                    val = 55.5
                else:
                    val = round(20.0 + 0.05 * h + (-1.0 if entity_id == ENT_B else 0.0), 3)
                pts.append(TelemetryPoint(ts=base + h * step, value=val))
                h += 1
            out[key] = pts
        return out

    async def close(self):
        return None


def _seed_catalog(mgr) -> None:
    import time

    from trendx.database.models import BusinessEntity, MetricDefinition

    now_ms = int(time.time() * 1000)
    with mgr.get_session("catalog") as session:
        session.add(BusinessEntity(id=uuid.UUID(ENT_A), name="b1-a"))
        session.add(BusinessEntity(id=uuid.UUID(ENT_B), name="b1-b"))
        for ent, metric in ((ENT_A, "temperature"), (ENT_A, "humidity"), (ENT_B, "temperature")):
            session.add(
                MetricDefinition(
                    business_entity_id=uuid.UUID(ent),
                    item_id=uuid.uuid4(),
                    item_name=metric,
                    tenant_id=uuid.UUID(TENANT),
                    name=metric,
                    user_input="auto",
                    description="B1",
                    how_to_calculate="raw",
                    created_ts=now_ms,
                    updated_ts=now_ms,
                )
            )
        session.commit()


def _counts(mgr):
    from sqlalchemy import text

    with mgr.get_session("analytics") as session:
        n = session.execute(text("SELECT count(*) FROM ts_kv")).scalar_one()
        n_latest = session.execute(text("SELECT count(*) FROM ts_kv_latest")).scalar_one()
    with mgr.get_session("catalog") as session:
        n_ckpt = session.execute(
            text("SELECT count(*) FROM trendx_catalog.ingestion_checkpoints")
        ).scalar_one()
    return n, n_latest, n_ckpt


def _window():
    end = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
    return end - timedelta(hours=6), end


def test_a_nominal_persistence_and_latest(b1_managers):
    """Test A : ts_kv > 0, latest alimenté, checkpoints, NaN ignoré par contrat."""
    from trendx.services.ingestion import IngestionService

    _seed_catalog(b1_managers)
    svc = IngestionService(database_manager=b1_managers, telemetry_reader=FakeReader(with_nan=True))
    start, end = _window()
    for ent, metric in ((ENT_A, "temperature"), (ENT_A, "humidity"), (ENT_B, "temperature")):
        res = asyncio.run(svc.ingest_device_metric(ent, metric, start, end))
        assert res["total_stored"] > 0, (ent, metric, res)
    n, n_latest, n_ckpt = _counts(b1_managers)
    # 6 points/serie (fenêtre 6h) moins 1 NaN ignoré (contrat: pas de ligne NULL).
    assert n == 6 + 6 + 6 - 1, n
    assert n_latest == 3, n_latest  # une ligne latest par (entité, métrique)
    assert n_ckpt == 3, n_ckpt
    from sqlalchemy import text

    with b1_managers.get_session("analytics") as session:
        latest_ts = session.execute(
            text("SELECT max(ts) FROM ts_kv_latest WHERE entity_id=:e"),
            {"e": ENT_A},
        ).scalar_one()
        assert latest_ts is not None and latest_ts.tzinfo is not None
    with b1_managers.get_session("catalog") as session:
        wms = session.execute(
            text("SELECT watermark_ts FROM trendx_catalog.ingestion_checkpoints")
        ).fetchall()
        assert all(w[0] is not None for w in wms)


def test_b_overlap_idempotent_and_isolated(b1_managers):
    """Test B/C : rejouée overlap sans doublon ; compteurs A/B indépendants."""
    from sqlalchemy import text
    from trendx.services.ingestion import IngestionService

    _seed_catalog(b1_managers)
    svc = IngestionService(database_manager=b1_managers, telemetry_reader=FakeReader())
    start, end = _window()
    for ent, metric in ((ENT_A, "temperature"), (ENT_B, "temperature")):
        asyncio.run(svc.ingest_device_metric(ent, metric, start, end))
    n1, _, _ = _counts(b1_managers)
    assert n1 == 12, n1
    for ent, metric in ((ENT_A, "temperature"), (ENT_B, "temperature")):
        asyncio.run(svc.ingest_device_metric(ent, metric, start, end))
    n2, n_latest2, _ = _counts(b1_managers)
    assert n2 == n1 == 12, (n1, n2)  # upsert ON CONFLICT : 0 duplication
    assert n_latest2 == 2, n_latest2
    with b1_managers.get_session("analytics") as session:
        vals_b = sorted(
            v[0]
            for v in session.execute(
                text("SELECT DISTINCT dbl_v FROM ts_kv WHERE entity_id=:e"), {"e": ENT_B}
            ).fetchall()
        )
        vals_a = sorted(
            v[0]
            for v in session.execute(
                text("SELECT DISTINCT dbl_v FROM ts_kv WHERE entity_id=:e"), {"e": ENT_A}
            ).fetchall()
        )
        # Séries distinctes par device (offset -1.0 sur B), 0 contamination.
        assert vals_b == [round(19.0 + 0.05 * h, 3) for h in range(6)], vals_b
        assert vals_a == [round(20.0 + 0.05 * h, 3) for h in range(6)], vals_a
        n_a = session.execute(
            text("SELECT count(*) FROM ts_kv WHERE entity_id=:e"), {"e": ENT_A}
        ).scalar_one()
        assert n_a == 6, n_a


def test_failure_writes_dead_letter_without_false_checkpoint(b1_managers):
    """Contrat §7 : échec -> dead_letter + PAS de checkpoint mensonger."""
    from sqlalchemy import text
    from trendx.services.ingestion import IngestionService

    _seed_catalog(b1_managers)

    class BoomReader(FakeReader):
        async def read_historical(self, entity_type, entity_id, keys, start_ts, end_ts):
            if entity_id == ENT_A:
                raise RuntimeError("canal TB coupé (contrôlé)")
            return await super().read_historical(entity_type, entity_id, keys, start_ts, end_ts)

    svc = IngestionService(database_manager=b1_managers, telemetry_reader=BoomReader())
    start, end = _window()
    asyncio.run(svc.ingest_device_metric(ENT_A, "temperature", start, end))
    asyncio.run(svc.ingest_device_metric(ENT_B, "temperature", start, end))
    with b1_managers.get_session("analytics") as session:
        dl = session.execute(
            text("SELECT count(*) FROM ts_kv WHERE source='dead_letter' AND entity_id=:e"),
            {"e": ENT_A},
        ).scalar_one()
        assert dl >= 1, dl
    with b1_managers.get_session("catalog") as session:
        ckpt_a = session.execute(
            text(
                "SELECT count(*) FROM trendx_catalog.ingestion_checkpoints "
                "WHERE entity_id=:e AND metric_key='temperature'"
            ),
            {"e": ENT_A},
        ).scalar_one()
        ckpt_b = session.execute(
            text(
                "SELECT count(*) FROM trendx_catalog.ingestion_checkpoints "
                "WHERE entity_id=:e AND metric_key='temperature'"
            ),
            {"e": ENT_B},
        ).scalar_one()
        assert ckpt_a == 0, "faux checkpoint : telemetry absente mais watermark avancé"
        assert ckpt_b == 1, ckpt_b
