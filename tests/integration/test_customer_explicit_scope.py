"""Contrat ModelRegistry sur PostgreSQL 16 jetable (défaut B1 corrigé).

Couvre §3 A-G sans mock du registre :
- A register nominal 2 modèles, UUID valides, URI MLflow conservées ;
- B register -> champion -> get_champion ;
- C promotion v1 -> v2 (+ promotion explicite) avec historique réel ;
- D rollback v2 -> v1 avec historique ;
- E isolation 2 devices x 2 metrics ;
- F ré-enregistrement : un seul champion par contexte, historique cohérent ;
- §4 train -> MLflow -> register -> champion -> forecast (2 devices,
  file-store MLflow jetable, writeback dry-run) ;
- §5 scheduler réel forecast_train/forecast_run, sans services.worker.

Migrations réelles appliquées : 001/005/006/008/009/011/013 (002 Timescale et
007/010 hors périmètre du contrat registre). Aucun xfail : tout est prouvé.
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
TENANT = str(uuid.uuid4())
CUSTOMER_A = str(uuid.uuid4())
CUSTOMER_B = str(uuid.uuid4())
DEV_A1 = str(uuid.uuid4())
DEV_A2 = str(uuid.uuid4())
DEV_B1 = str(uuid.uuid4())
DEV_B2 = str(uuid.uuid4())
MIGS = (
    "001_trendz_native_schema.sql",
    "005_ingestion_checkpoints.sql",
    "006_prediction_model_status_history.sql",
    "008_add_model_uri.sql",
    "009_native_ts_kv.sql",
    "011_native_ml_partitioned_tables.sql",
    "013_forecast_alerting_and_model_enrichment.sql",
)
_PATCH_TARGETS = [
    "trendx.services.training.db_manager",
    "trendx.services.inference.db_manager",
    "trendx.mlops.registry.db_manager",
    "trendx.preprocessing.quality.db_manager",
]


def _apply(params: dict[str, object], filename: str) -> None:
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
def reg_env(provisioned_postgres, tmp_path, monkeypatch):
    from trendx.database.connection import DatabaseManager

    params = provisioned_postgres
    for f in MIGS:
        _apply(params, f)
    dsn = (
        f"postgresql://{params['user']}:{quote(str(params['password']))}"
        f"@{params['host']}:{params['port']}/{params['dbname']}"
    )
    mgr = DatabaseManager()
    mgr.register("catalog", dsn, search_path="trendx_catalog,public")
    mgr.register("analytics", dsn, search_path="trendx_analytics,public")
    for target in _PATCH_TARGETS:
        mod_name, attr = target.rsplit(".", 1)
        monkeypatch.setattr(f"{mod_name}.{attr}", mgr, raising=True)
    from sqlalchemy import text

    with mgr.get_session("analytics") as session:
        session.execute(
            text(
                "CREATE TABLE IF NOT EXISTS pred_reg PARTITION OF "
                "trendx_analytics.predictions FOR VALUES FROM ('2026-01-01') TO ('2027-01-01')"
            )
        )
        session.commit()
    # Entités réelles (résolution tenant contextuelle, jamais inventée).
    from trendx.database.models import BusinessEntity

    with mgr.get_session("catalog") as session:
        for _dev, _name in ((DEV_A1, "ca-1"), (DEV_A2, "ca-2"), (DEV_B1, "cb-1"), (DEV_B2, "cb-2")):
            session.add(BusinessEntity(id=uuid.UUID(_dev), name=_name, tenant_id=uuid.UUID(TENANT)))
        session.commit()
    from trendx.mlops.registry import ModelRegistry

    return mgr, ModelRegistry(), f"file://{tmp_path}/mlruns"


def test_a_register_nominal_two_models(reg_env) -> None:
    """A : 2 modèles, 0 erreur PG, UUID valides, URI conservées."""
    from sqlalchemy import text

    mgr, reg, _ = reg_env
    m1 = reg.register(
        business_entity_id=DEV_A1,
        tb_telemetry_key="temperature",
        model_type="LinearRegression",
        model_uri="runs:/run-a/model",
        customer_id=CUSTOMER_A,
        promote=False,
    )
    m2 = reg.register(
        business_entity_id=DEV_B1,
        tb_telemetry_key="temperature",
        model_type="LinearRegression",
        model_uri="runs:/run-b/model",
        customer_id=CUSTOMER_B,
        promote=False,
    )
    assert m1.id != m2.id
    assert m1.model_uri == "runs:/run-a/model"
    assert m2.model_uri == "runs:/run-b/model"
    with mgr.get_session("catalog") as session:
        rows = session.execute(
            text(
                "SELECT business_entity_id, business_entity_field_id,"
                " associated_entity_field_id, tenant_id, status"
                " FROM trendx_catalog.prediction_model"
            )
        ).fetchall()
        assert len(rows) == 2
        for ent, field, assoc, tenant, _status in rows:
            uuid.UUID(str(ent))
            uuid.UUID(str(field))
            uuid.UUID(str(assoc))
            uuid.UUID(str(tenant))


def test_b_register_resolves_champion(reg_env) -> None:
    """B : register (promotion par défaut) -> get_champion."""
    _, reg, _ = reg_env
    made = reg.register(
        business_entity_id=DEV_A1,
        tb_telemetry_key="temperature",
        model_type="LinearRegression",
        model_uri="runs:/run-c/model",
        customer_id=CUSTOMER_A,
    )
    assert made.status == "champion"
    got = reg.get_champion(DEV_A1, "temperature")
    assert got is not None and got.id == made.id


def test_c_promotion_with_history(reg_env) -> None:
    """C : v1 champion -> v2 challenger -> promote(v2), historique réel."""
    from sqlalchemy import text

    mgr, reg, _ = reg_env
    v1 = reg.register(
        business_entity_id=DEV_A1,
        tb_telemetry_key="humidity",
        model_type="LinearRegression",
        model_uri="runs:/v1/model",
        customer_id=CUSTOMER_A,
    )
    assert reg.get_champion(DEV_A1, "humidity").id == v1.id
    v2 = reg.register(
        business_entity_id=DEV_A1,
        tb_telemetry_key="humidity",
        model_type="LinearRegression",
        model_uri="runs:/v2/model",
        customer_id=CUSTOMER_A,
        promote=False,
    )
    assert v2.status == "challenger"
    promoted = reg.promote_to_champion(DEV_A1, "humidity", v2)
    assert promoted is not None and promoted.id == v2.id
    assert reg.get_champion(DEV_A1, "humidity").id == v2.id
    with mgr.get_session("catalog") as session:
        hist = session.execute(
            text(
                "SELECT previous_status, new_status"
                " FROM trendx_catalog.prediction_model_status_history"
                " WHERE business_entity_id=:e AND metric_key='humidity'"
                " ORDER BY changed_ts"
            ),
            {"e": DEV_A1},
        ).fetchall()
        transitions = [(r[0], r[1]) for r in hist]
        assert ("challenger", "champion") in transitions, transitions
        assert ("champion", "challenger") in transitions, transitions


def test_d_rollback_restores_v1(reg_env) -> None:
    """D : v2 champion -> rollback -> v1 champion + historique."""
    from sqlalchemy import text

    mgr, reg, _ = reg_env
    v1 = reg.register(
        business_entity_id=DEV_B1,
        tb_telemetry_key="humidity",
        model_type="LinearRegression",
        model_uri="runs:/w1/model",
        customer_id=CUSTOMER_B,
    )
    v2 = reg.register(
        business_entity_id=DEV_B1,
        tb_telemetry_key="humidity",
        model_type="LinearRegression",
        model_uri="runs:/w2/model",
        customer_id=CUSTOMER_B,
    )
    assert reg.get_champion(DEV_B1, "humidity").id == v2.id
    back = reg.rollback(DEV_B1, "humidity")
    assert back is not None and back.id == v1.id
    assert reg.get_champion(DEV_B1, "humidity").id == v1.id
    with mgr.get_session("catalog") as session:
        n = session.execute(
            text(
                "SELECT count(*) FROM trendx_catalog.prediction_model_status_history"
                " WHERE business_entity_id=:e AND metric_key='humidity'"
            ),
            {"e": DEV_B1},
        ).scalar_one()
        assert n >= 4, n


def test_e_isolation_two_devices_two_metrics(reg_env) -> None:
    """E : contexts étanches tenant/device/metric."""
    _, reg, _ = reg_env
    for dev, metric in ((DEV_A1, "temperature"), (DEV_A1, "humidity"), (DEV_B1, "temperature")):
        reg.register(
            business_entity_id=dev,
            tb_telemetry_key=metric,
            model_type="LinearRegression",
            model_uri=f"runs:/{dev[:8]}-{metric}/model",
            customer_id=CUSTOMER_A if dev == DEV_A1 else CUSTOMER_B,
        )
    assert reg.get_champion(DEV_A1, "temperature").model_uri.startswith("runs:/")
    assert reg.get_champion(DEV_A1, "humidity") is not None
    assert reg.get_champion(DEV_B1, "temperature") is not None
    assert reg.get_champion(DEV_B1, "humidity") is None
    assert reg.get_champion(str(uuid.uuid4()), "temperature") is None
    uris = {
        reg.get_champion(DEV_A1, "temperature").model_uri,
        reg.get_champion(DEV_A1, "humidity").model_uri,
        reg.get_champion(DEV_B1, "temperature").model_uri,
    }
    assert len(uris) == 3


def test_f_reregister_keeps_single_champion(reg_env) -> None:
    """F : ré-enregistrement -> un seul champion, historique cohérent."""
    from sqlalchemy import text

    mgr, reg, _ = reg_env
    first = reg.register(
        business_entity_id=DEV_A1,
        tb_telemetry_key="temperature",
        model_type="LinearRegression",
        model_uri="runs:/dup1/model",
        customer_id=CUSTOMER_A,
    )
    second = reg.register(
        business_entity_id=DEV_A1,
        tb_telemetry_key="temperature",
        model_type="LinearRegression",
        model_uri="runs:/dup2/model",
        customer_id=CUSTOMER_A,
    )
    assert second.id != first.id
    assert reg.get_champion(DEV_A1, "temperature").id == second.id
    with mgr.get_session("catalog") as session:
        n_champ = session.execute(
            text(
                "SELECT count(*) FROM trendx_catalog.prediction_model"
                " WHERE business_entity_id=:e AND tb_telemetry_key='temperature'"
                " AND status='champion'"
            ),
            {"e": DEV_A1},
        ).scalar_one()
        assert n_champ == 1, n_champ


def _seed_ts(mgr, ent: str, metric: str, base: float) -> None:
    from sqlalchemy import text as _t

    end = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
    with mgr.get_session("analytics") as session:
        for h in range(48):
            session.execute(
                _t(
                    "INSERT INTO ts_kv (ts, entity_id, metric_key, dbl_v, source, ingestion_id)"
                    " VALUES (:ts, :e, :k, :v, 'thingsboard', 'reg-seed')"
                    " ON CONFLICT DO NOTHING"
                ),
                {"ts": end - timedelta(hours=48 - h), "e": ent, "k": metric, "v": base + 0.05 * h},
            )
        session.commit()


class FakeReader:
    """Transport synthétique (le fix ingestion porté ici est exercé pour de vrai)."""

    async def read_historical(self, entity_type, entity_id, keys, start_ts, end_ts):
        from trendx.thingsboard.telemetry import TelemetryPoint

        out: dict[str, list] = {}
        step = 3600_000
        base = (start_ts // step) * step
        for key in keys:
            pts, h = [], 0
            while base + h * step < end_ts and h < 12:
                pts.append(TelemetryPoint(ts=base + h * step, value=round(20.0 + 0.05 * h, 3)))
                h += 1
            out[key] = pts
        return out

    async def close(self):
        return None


def test_scheduler_forecast_run_anomaly_no_worker(reg_env) -> None:
    """§5 (partiel, train exclu — voir BLOCKED) : ingestion + forecast-run +
    anomaly réels via RuntimeScheduler, champion seedé par register() fixé."""
    import sys
    import time
    import uuid as uuid_mod

    import numpy as np
    from trendx.anomalies.detectors import IsolationForestDetector
    from trendx.database.models import MetricDefinition
    from trendx.scheduler.registry import SchedulerRegistry
    from trendx.scheduler.runtime import RuntimeScheduler
    from trendx.services.inference import InferenceService
    from trendx.services.ingestion import IngestionService
    from trendx.services.training import TrainingService

    mgr, reg, uri = reg_env
    _seed_ts(mgr, DEV_A1, "temperature", 20.0)
    _seed_ts(mgr, DEV_B1, "temperature", 19.0)
    now_ms = int(time.time() * 1000)
    with mgr.get_session("catalog") as session:
        for _dev in (DEV_A1, DEV_B1):
            session.add(
                MetricDefinition(
                    business_entity_id=uuid.UUID(_dev),
                    item_id=uuid.uuid4(),
                    item_name="temperature",
                    tenant_id=uuid.UUID(TENANT),
                    name="temperature",
                    user_input="auto",
                    description="reg",
                    how_to_calculate="raw",
                    created_ts=now_ms,
                    updated_ts=now_ms,
                )
            )
        session.commit()
    assert "trendx.services.worker" not in sys.modules

    def h_ingest(p):
        r = asyncio.run(
            IngestionService(
                database_manager=mgr, telemetry_reader=FakeReader()
            ).ingest_device_metric(
                DEV_A1,
                "temperature",
                datetime.now(UTC) - timedelta(hours=12),
                datetime.now(UTC),
            )
        )
        assert r["total_stored"] > 0, r
        return {"stored": r["total_stored"]}

    reg.register(
        business_entity_id=DEV_A1,
        tb_telemetry_key="temperature",
        model_type="LinearRegression",
        model_uri="runs:/sched/model",
        customer_id=CUSTOMER_A,
    )

    def h_train(p):
        from trendx.mlops.tracking import MLflowTracker

        dev = p.get("device_id", DEV_A1)
        cust = CUSTOMER_A if dev == DEV_A1 else CUSTOMER_B
        tracker = MLflowTracker(tracking_uri=uri)
        m = TrainingService(mlflow_tracker=tracker).train_model(
            dev,
            "temperature",
            algorithm="LinearRegression",
            lookback_days=2,
            customer_id=cust,
        )
        if m is None:
            raise RuntimeError("train None")
        return {"model_id": str(m.id), "customer": cust}

    def h_forecast(p):
        f = InferenceService().generate_forecast(DEV_A1, "temperature", horizon=3)
        assert f is not None
        return {"points": InferenceService().save_forecast_results(DEV_A1, "temperature", f)}

    def h_anomaly(p):
        rng = np.random.default_rng(7)
        x = 20.0 + rng.normal(0, 0.1, 60)
        d = IsolationForestDetector(contamination=0.05)
        d.fit(x.reshape(-1, 1))
        return {"scored": True, "stub": False, "n": len(d.score(x.reshape(-1, 1)))}

    def h_noop(p):
        return {"noop": True}

    registry = SchedulerRegistry()
    for jt, fn in (
        ("topology_discovery", h_noop),
        ("topology_sync", h_noop),
        ("ingestion", h_ingest),
        ("trendx_train", h_train),
        ("trendx_forecast", h_forecast),
        ("anomaly_scan", h_anomaly),
    ):
        registry.register_handler(jt, fn)
    rt = RuntimeScheduler(registry)
    assert len(rt.schedule_all()) == 6
    before = set(sys.modules)
    rt.start()
    assert rt.is_running()
    for job, payload in (
        ("ingestion-run", {"device_id": DEV_A1}),
        ("forecast-train", {"device_id": DEV_A1}),
        ("forecast-train", {"device_id": DEV_B1}),
        ("forecast-run", {"device_id": DEV_A1}),
        ("anomaly-scan", {"device_id": DEV_A1}),
    ):
        out = rt.run_job_now(job, payload)
        uuid_mod.UUID(out["run_id"])
        assert out["status"] == "ok", (job, out)
    recs = [
        r
        for r in registry.records()
        if r.job_id in ("ingestion-run", "forecast-train", "forecast-run", "anomaly-scan")
    ]
    assert len(recs) == 5 and all(r.status == "ok" for r in recs)
    assert all(r.triggered_at.tzinfo is not None for r in recs)
    rt.stop()
    assert not rt.is_running()
    assert "trendx.services.worker" not in set(sys.modules) - before


def test_g_cross_customer_no_leak(reg_env) -> None:
    """§7 : A->B = None et B->A = None ; sans scope, comportement conservé."""
    _, reg, _ = reg_env
    for dev, cust in (
        (DEV_A1, CUSTOMER_A),
        (DEV_A2, CUSTOMER_A),
        (DEV_B1, CUSTOMER_B),
        (DEV_B2, CUSTOMER_B),
    ):
        for metric in ("temperature", "humidity"):
            reg.register(
                business_entity_id=dev,
                tb_telemetry_key=metric,
                model_type="LinearRegression",
                model_uri=f"runs:/{dev[:8]}-{metric}/model",
                customer_id=cust,
            )
    for dev, cust, other in ((DEV_A1, CUSTOMER_A, CUSTOMER_B), (DEV_B1, CUSTOMER_B, CUSTOMER_A)):
        for metric in ("temperature", "humidity"):
            own = reg.get_champion(dev, metric, customer_id=cust)
            assert own is not None and own.customer_id == uuid.UUID(cust), (dev, metric)
            assert reg.get_champion(dev, metric, customer_id=other) is None, (dev, metric)
            assert reg.get_champion(dev, metric) is not None  # non scopé : conservé


def test_train_chain_explicit_and_env_customer(reg_env, monkeypatch) -> None:
    """§5 : train explicite + via env, champion scopé, forecast, dry-run."""
    import numpy as np
    import pandas as pd
    from sqlalchemy import text
    from trendx.config import settings
    from trendx.mlops.tracking import MLflowTracker
    from trendx.services.inference import InferenceService
    from trendx.services.training import TrainingService

    mgr, reg, uri = reg_env
    _seed_ts(mgr, DEV_A1, "temperature", 20.0)
    _seed_ts(mgr, DEV_A2, "humidity", 50.0)
    monkeypatch.setattr(settings, "trendx_default_customer_id", CUSTOMER_A)
    tracker = MLflowTracker(tracking_uri=uri)
    svc = TrainingService(mlflow_tracker=tracker)
    m1 = svc.train_model(
        DEV_A1,
        "temperature",
        algorithm="LinearRegression",
        lookback_days=2,
        customer_id=CUSTOMER_A,
    )
    m2 = svc.train_model(DEV_A2, "humidity", algorithm="LinearRegression", lookback_days=2)
    assert m1 is not None and m2 is not None
    assert m1.model_uri.startswith("runs:/") and m2.model_uri.startswith("runs:/")
    assert reg.get_champion(DEV_A1, "temperature", customer_id=CUSTOMER_A).id == m1.id
    assert reg.get_champion(DEV_A2, "humidity", customer_id=CUSTOMER_A).id == m2.id
    assert reg.get_champion(DEV_A1, "temperature", customer_id=CUSTOMER_B) is None
    infer = InferenceService()
    fc = infer.generate_forecast(DEV_A1, "temperature", horizon=6)
    assert fc is not None and len(fc.values) == 6
    assert np.all(np.isfinite(np.asarray(fc.values, dtype=float)))
    with mgr.get_session("analytics") as session:
        actual_max = session.execute(
            text("SELECT max(ts) FROM ts_kv WHERE entity_id=:e"), {"e": DEV_A1}
        ).scalar_one()
    assert all(pd.Timestamp(t).tz_localize("UTC") > actual_max for t in list(fc.timestamps))
    assert infer.save_forecast_results(DEV_A1, "temperature", fc) == 6
    assert infer.writeback_forecast(DEV_A1, "temperature", fc).get("dry_run") is True
