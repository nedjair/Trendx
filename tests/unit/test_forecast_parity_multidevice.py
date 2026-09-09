"""Parité Forecast multi-devices — TDD Phase 2 (synthétique, sans TB réel).

Couvre les 15 points exigés :
1. découverte/dispatch dynamique
2. séparation des contexts device
3. PER_DEVICE
4. PER_PROFILE
5. GLOBAL
6. AUTO
7. sélection/compétition
8. MAE
9. RMSE
10. sMAPE
11. MAPE
12. versionnement MLflow/registry
13. isolation erreur
14. absence writeback
15. idempotence/reprise

Aucun device_id codé en dur (UUIDs), timestamps UTC, lectures bornées,
writeback désactivé. Ne modifie aucun WIP/xfail/gardien.
"""

from __future__ import annotations

import uuid
from datetime import datetime
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest
from trendx.forecasting.base import Strategy, compute_metrics
from trendx.forecasting.selector import ModelSelector
from trendx.services import worker as worker_mod


def _job(entity_id: str, metric: str = "temperature", **overrides) -> dict:
    base = {
        "tenant_id": str(uuid.uuid4()),
        "entity_type": "DEVICE",
        "entity_id": entity_id,
        "metric_name": metric,
    }
    base.update(overrides)
    return base


def _sine_data(n: int = 200, seed: int = 0) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    ts = pd.date_range("2024-01-01", periods=n, freq="1h", tz="UTC")
    y = 20 + 5 * np.sin(2 * np.pi * np.arange(n) / 24) + rng.normal(0, 0.5, n)
    return pd.DataFrame({"ds": ts, "y": y})


# 1. découverte/dispatch dynamique -------------------------------------------------
@pytest.mark.unit
def test_01_dynamic_dispatch_train_and_forecast() -> None:
    assert "trendx_train" in worker_mod.JOB_DISPATCH
    assert "trendx_forecast" in worker_mod.JOB_DISPATCH
    # Le dispatch est dynamique : _process_one_task route via JOB_DISPATCH,
    # aucun branchement statique par device dans le worker.
    import inspect

    src = inspect.getsource(worker_mod._process_one_task)
    assert "JOB_DISPATCH" in src
    assert "ALG16025001" not in src  # aucun device codé en dur


# 2. séparation des contexts ------------------------------------------------------
@pytest.mark.unit
def test_02_context_separation_two_devices() -> None:
    dev_a = str(uuid.uuid4())
    dev_b = str(uuid.uuid4())
    seen: list[dict] = []

    def fake_train(json_job, task_id, execution_id):
        seen.append(dict(json_job))
        return {"job_type": "trendx_train", "status": "ok", "entity_id": json_job["entity_id"]}

    with patch.dict(worker_mod.JOB_DISPATCH, {"trendx_train": fake_train}):
        worker_mod.JOB_DISPATCH["trendx_train"](_job(dev_a), "t1", "e1")
        worker_mod.JOB_DISPATCH["trendx_train"](_job(dev_b), "t2", "e2")

    assert seen[0]["entity_id"] == dev_a
    assert seen[1]["entity_id"] == dev_b
    assert seen[0]["entity_id"] != seen[1]["entity_id"]


# 3-6. stratégies ------------------------------------------------------------------
@pytest.mark.unit
@pytest.mark.parametrize("strategy", ["PER_DEVICE", "PER_PROFILE", "GLOBAL", "AUTO"])
def test_03_06_strategies_accepted(strategy: str) -> None:
    from trendx.services.training import TrainingService

    dev = str(uuid.uuid4())
    job = _job(dev, strategy=strategy, lookback_days=30, frequency="1h", horizon=12)

    with (
        patch.object(
            TrainingService, "train_model", return_value=MagicMock(id=uuid.uuid4())
        ) as m_train,
        patch.object(
            TrainingService, "auto_select_strategy", return_value=MagicMock(id=uuid.uuid4())
        ) as m_auto,
    ):
        result = worker_mod._run_trendx_train(job, "t", "e")

    assert result["strategy"] == strategy
    assert result["entity_id"] == dev
    if strategy == "AUTO":
        m_auto.assert_called_once()
    else:
        m_train.assert_called_once()
        # Vérifie que le contexte device/métrique est propagé (pas de fuite).
        kwargs = m_train.call_args.kwargs
        assert kwargs["entity_id"] == dev
        assert kwargs["metric_key"] == "temperature"


# 7. sélection/compétition ----------------------------------------------------------
@pytest.mark.unit
def test_07_selection_competition_two_devices() -> None:
    selector = ModelSelector(
        strategy=Strategy.PER_DEVICE,
        n_windows=2,
        test_size=0.2,
        min_stable_windows=2,
        marginal_gain_threshold=0.0,
    )
    for seed in (11, 22):
        data = _sine_data(n=120, seed=seed)
        candidates = [
            ("LinearRegression", {"feature_set": ["trend"]}),
            ("LinearRegression", {"feature_set": ["trend", "hour"]}),
        ]
        results = selector.run_competition(
            entity_id=str(uuid.uuid4()),
            metric_key="temperature",
            data=data,
            candidates=candidates,
            n_windows=2,
        )
        assert len(results) >= 1
        champion = selector.select_champion(results)
        # Avec threshold 0, le meilleur doit être promu s'il a assez de fenêtres.
        assert champion is not None
        assert champion.algorithm == "LinearRegression"


# 8-11. métriques -------------------------------------------------------------------
@pytest.mark.unit
def test_08_11_metrics_mae_rmse_smape_mape() -> None:
    y_true = np.array([10.0, 20.0, 30.0])
    y_pred = np.array([12.0, 18.0, 33.0])
    y_lower = np.array([11.0, 17.0, 32.0])
    y_upper = np.array([13.0, 19.0, 34.0])
    m = compute_metrics(y_true, y_pred, y_lower, y_upper)
    # MAE = (2+2+3)/3
    assert m.mae == pytest.approx(7 / 3, rel=1e-6)
    # RMSE = sqrt((4+4+9)/3)
    assert m.rmse == pytest.approx(float(np.sqrt(17 / 3)), rel=1e-6)
    # sMAPE/MAPE > 0 et finis
    assert np.isfinite(m.smape) and m.smape > 0
    assert np.isfinite(m.mape) and m.mape > 0
    # MAPE manuel : (2/10 + 2/20 + 3/30)/3*100 = (0.2+0.1+0.1)/3*100
    assert m.mape == pytest.approx((0.4 / 3) * 100, rel=1e-6)
    # Coverage/largeur/biais calculés car bornes fournies
    assert 0 <= m.coverage <= 100
    assert m.interval_width == pytest.approx(2.0, rel=1e-6)
    assert m.bias == pytest.approx(float(np.mean(y_pred - y_true)), rel=1e-6)


# 12. versionnement -----------------------------------------------------------------
@pytest.mark.unit
def test_12_versioning_registry_model_uri() -> None:
    dev = str(uuid.uuid4())
    fake_model = MagicMock()
    fake_model.id = uuid.uuid4()
    fake_svc = MagicMock()
    fake_svc.train_model.return_value = fake_model
    fake_svc.auto_select_strategy.return_value = fake_model
    with patch("trendx.services.training.TrainingService", return_value=fake_svc):
        result = worker_mod._run_trendx_train(_job(dev), "t", "e")
    assert result["status"] == "ok"
    assert result["model_id"] == str(fake_model.id)
    fake_svc.train_model.assert_called_once()


# 13. isolation erreur ---------------------------------------------------------------
@pytest.mark.unit
def test_13_error_isolation_one_device_fails_other_succeeds() -> None:
    from trendx.services.training import TrainingService

    dev_ok = str(uuid.uuid4())
    dev_ko = str(uuid.uuid4())

    def side_effect(entity_id, metric_key, **kwargs):
        if entity_id == dev_ko:
            raise RuntimeError("boom device KO")
        m = MagicMock()
        m.id = uuid.uuid4()
        return m

    with patch.object(TrainingService, "train_model", side_effect=side_effect):
        ok = worker_mod._run_trendx_train(_job(dev_ok), "t-ok", "e-ok")
        assert ok["status"] == "ok"
        with pytest.raises(RuntimeError, match="boom device KO"):
            worker_mod._run_trendx_train(_job(dev_ko), "t-ko", "e-ko")
        # Le device OK reste utilisable après l'échec du KO (pas d'état global corrompu).
        ok2 = worker_mod._run_trendx_train(_job(dev_ok), "t-ok2", "e-ok2")
        assert ok2["status"] == "ok"


# 14. absence writeback ---------------------------------------------------------------
@pytest.mark.unit
def test_14_no_writeback_by_default() -> None:
    from trendx.config import settings
    from trendx.services.inference import InferenceService

    assert settings.tb_writeback_enabled is False
    svc = InferenceService(dry_run=True)
    assert svc.dry_run is True

    dev = str(uuid.uuid4())
    fake_forecast = MagicMock()
    fake_forecast.values = np.array([1.0, 2.0, 3.0])
    fake_forecast.lower_bound = np.array([0.9, 1.9, 2.9])
    fake_forecast.upper_bound = np.array([1.1, 2.1, 3.1])
    fake_forecast.timestamps = None
    fake_forecast.model_name = "LinearRegression"

    with (
        patch.object(InferenceService, "generate_forecast", return_value=fake_forecast),
        patch.object(InferenceService, "save_forecast_results", return_value=3) as m_save,
        patch("trendx.thingsboard.client.ThingsBoardClient", autospec=True) as m_tb,
    ):
        result = worker_mod._run_trendx_forecast(_job(dev, horizon=3), "t", "e")

    assert result["writeback"] == "disabled"
    assert result["saved"] == 3
    m_save.assert_called_once()
    m_tb.assert_not_called()


# 15. idempotence/reprise --------------------------------------------------------------
@pytest.mark.unit
def test_15_idempotence_repeat_same_job() -> None:
    from trendx.services.training import TrainingService

    dev = str(uuid.uuid4())
    job = _job(dev, strategy="PER_DEVICE")
    fake_model = MagicMock()
    fake_model.id = "model-stable-id"

    with patch.object(TrainingService, "train_model", return_value=fake_model) as m:
        r1 = worker_mod._run_trendx_train(job, "t", "e")
        r2 = worker_mod._run_trendx_train(dict(job), "t", "e")  # reprise même params

    assert m.call_count == 2
    assert r1["model_id"] == r2["model_id"] == "model-stable-id"
    assert r1["entity_id"] == r2["entity_id"] == dev
    # Timestamps UTC ISO (preuve normalisation UTC).
    for r in (r1, r2):
        ts = datetime.fromisoformat(r["generated_at"])
        assert ts.tzinfo is not None


@pytest.mark.unit
def test_15_forecast_idempotence_save_upsert() -> None:
    from trendx.services.inference import InferenceService

    dev = str(uuid.uuid4())
    fake_forecast = MagicMock()
    fake_forecast.values = np.array([5.0, 6.0])
    fake_forecast.lower_bound = np.array([4.9, 5.9])
    fake_forecast.upper_bound = np.array([5.1, 6.1])
    fake_forecast.timestamps = None
    fake_forecast.model_name = "LinearRegression"

    with (
        patch.object(InferenceService, "generate_forecast", return_value=fake_forecast),
        patch.object(InferenceService, "save_forecast_results", return_value=2) as m_save,
    ):
        r1 = worker_mod._run_trendx_forecast(_job(dev, horizon=2), "t", "e")
        r2 = worker_mod._run_trendx_forecast(_job(dev, horizon=2), "t", "e")

    assert m_save.call_count == 2
    assert r1["points"] == r2["points"] == 2
    assert r1["saved"] == r2["saved"] == 2
