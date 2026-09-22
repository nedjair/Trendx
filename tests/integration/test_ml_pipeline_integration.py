"""W49 — Pipeline integration on disposable PostgreSQL (never production).

Applies migrations 000→013 on an ephemeral database, reseeds synthetic
telemetry for ≥2 entities, then runs the real MLPipelineOrchestrator end to
end (Quality → Preprocessing → Train → Forecast → Anomaly) with real services
and a temp-dir MLflow store. Verifies persistence, traceability, idempotence,
isolation and absence of writeback. Skips when no disposable PG is available.
"""

from __future__ import annotations

import os
import uuid
from pathlib import Path

import pytest
from sqlalchemy import text

from .test_fixtures import pg_query, psql_run_file, run_sql

ROOT = Path(__file__).parents[2]
MIGRATIONS = sorted(ROOT.glob("migrations/[0-9][0-9][0-9]_*.sql"))

pytestmark = [pytest.mark.integration]

ENTITY_A = str(uuid.uuid4())
ENTITY_B = str(uuid.uuid4())
METRIC = "temperature"
N_POINTS = 150


def _dsn(params: dict[str, str]) -> str:
    return (
        f"postgresql://{params['user']}:{params['password']}"
        f"@{params['host']}:{params['port']}/{params['dbname']}"
    )


@pytest.fixture()
def pipeline_db(provisioned_postgres, tmp_path, monkeypatch):
    """Ephemeral PG with the full migration chain + engines re-pointed at it."""
    import mlflow
    from trendx.database.connection import EngineWrapper, manager

    params = provisioned_postgres
    for sql_file in MIGRATIONS:
        result = psql_run_file(params, sql_file)
        assert result.returncode == 0, f"{sql_file.name} failed: {result.stderr[-2000:]}"
    # Mirror production ops (worker boot check / make doctor): forward partitions.
    run_sql(
        params,
        "SELECT trendx_analytics.ensure_partitions_forward(3)",
    )
    mlflow_store = f"file://{tmp_path}/mlflow"
    monkeypatch.setenv("MLFLOW_TRACKING_URI", mlflow_store)
    mlflow.set_tracking_uri(mlflow_store)

    saved_engines = dict(manager._engines)
    saved_makers = dict(manager._sessionmakers)
    catalog = EngineWrapper("catalog", _dsn(params), search_path="trendx_catalog,public")
    analytics = EngineWrapper("analytics", _dsn(params), search_path="trendx_analytics,public")
    from sqlalchemy.orm import sessionmaker

    manager._engines["catalog"] = catalog
    manager._engines["analytics"] = analytics
    manager._sessionmakers["catalog"] = sessionmaker(bind=catalog.engine, expire_on_commit=False)
    manager._sessionmakers["analytics"] = sessionmaker(
        bind=analytics.engine, expire_on_commit=False
    )
    try:
        yield params
    finally:
        catalog.dispose()
        analytics.dispose()
        manager._engines.clear()
        manager._engines.update(saved_engines)
        manager._sessionmakers.clear()
        manager._sessionmakers.update(saved_makers)


def _seed_telemetry() -> None:
    from trendx.database.connection import manager

    engine = manager.get_engine("analytics")
    with engine.begin() as conn:
        for entity in (ENTITY_A, ENTITY_B):
            conn.execute(
                text(
                    """
                    INSERT INTO trendx_analytics.ts_kv (ts, entity_id, metric_key, dbl_v, source)
                    SELECT now() - (s || ' hours')::interval,
                           :eid, :key,
                           20 + 5 * sin(s / 24.0 * 2 * pi()),
                           'synthetic-e2e'
                    FROM generate_series(1, :n) AS s
                    """
                ),
                {"eid": entity, "key": METRIC, "n": N_POINTS},
            )


def _count(params: dict[str, str], sql: str) -> int:
    rows = pg_query(params, sql)
    return int(rows[0]) if rows else 0


@pytest.mark.integration
def test_pipeline_e2e_two_entities(pipeline_db, tmp_path) -> None:
    from trendx.anomalies.detectors import AnomalyDetectorService
    from trendx.mlops.registry import ModelRegistry
    from trendx.mlops.tracking import MLflowTracker
    from trendx.services.pipeline import MLPipelineOrchestrator, PipelineContext
    from trendx.services.training import TrainingService

    params = pipeline_db
    _seed_telemetry()

    tracker = MLflowTracker(tracking_uri=f"file://{tmp_path}/mlflow")
    training = TrainingService(mlflow_tracker=tracker)
    orch = MLPipelineOrchestrator(
        training=training,
        anomaly=AnomalyDetectorService(tracker=None),
    )
    contexts = [
        PipelineContext(
            tenant_id=str(uuid.uuid4()),
            entity_type="DEVICE",
            entity_id=ENTITY_A,
            metric_name=METRIC,
            lookback_days=30,
            algorithm="LinearRegression",
        ),
        PipelineContext(
            tenant_id=str(uuid.uuid4()),
            entity_type="DEVICE",
            entity_id=ENTITY_B,
            metric_name=METRIC,
            lookback_days=30,
            algorithm="LinearRegression",
        ),
    ]
    results = orch.run_many(contexts)
    assert [r.status for r in results] == ["ok", "ok"]

    for result, entity in zip(results, (ENTITY_A, ENTITY_B), strict=True):
        assert result.execution_id
        assert result.model_id
        champion = ModelRegistry().get_champion(entity, METRIC)
        assert champion is not None and str(champion.id) == result.model_id
        assert result.forecast_saved > 0

    dq_a = _count(
        params,
        f"SELECT count(*) FROM trendx_analytics.data_quality WHERE entity_id = '{ENTITY_A}'",
    )
    dq_b = _count(
        params,
        f"SELECT count(*) FROM trendx_analytics.data_quality WHERE entity_id = '{ENTITY_B}'",
    )
    assert dq_a >= 1 and dq_b >= 1

    pred_a = _count(
        params,
        f"SELECT count(*) FROM trendx_analytics.predictions WHERE entity_id = '{ENTITY_A}'",
    )
    pred_b = _count(
        params,
        f"SELECT count(*) FROM trendx_analytics.predictions WHERE entity_id = '{ENTITY_B}'",
    )
    assert pred_a > 0 and pred_b > 0

    wb = _count(
        params,
        f"SELECT count(*) FROM trendx_analytics.predictions "
        f"WHERE entity_id IN ('{ENTITY_A}', '{ENTITY_B}') AND written_back IS TRUE",
    )
    assert wb == 0

    anom_a = _count(
        params, f"SELECT count(*) FROM trendx_catalog.anomaly WHERE item_id = '{ENTITY_A}'"
    )
    assert anom_a == results[0].anomaly_episodes

    # Idempotence: rerun A changes nothing durable.
    again = orch.run_single(contexts[0])
    assert again.status == "ok" and again.deduped is True
    assert (
        _count(
            params,
            f"SELECT count(*) FROM trendx_analytics.predictions WHERE entity_id = '{ENTITY_A}'",
        )
        == pred_a
    )
    assert (
        _count(
            params,
            f"SELECT count(*) FROM trendx_analytics.data_quality WHERE entity_id = '{ENTITY_A}'",
        )
        == dq_a
    )
    assert (
        _count(
            params,
            f"SELECT count(*) FROM trendx_analytics.predictions WHERE entity_id = '{ENTITY_B}'",
        )
        == pred_b
    )
    assert os.environ.get("TB_WRITEBACK_ENABLED", "false") == "false"
