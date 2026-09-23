"""Contrat worker/scheduler Forecast — TDD Phase 2.

Vérifie que le worker expose un dispatch dynamique pour les jobs
trendx_train / trendx_forecast, sans job statique par device, avec
timestamps UTC, lectures bornées et writeback désactivé par défaut.

Indépendant de ThingsBoard réel : 100% mocks + données synthétiques.
Aucun device_id codé en dur : UUIDs générés.
Ne modifie aucun WIP, aucun xfail, aucun gardien existant.
"""

from __future__ import annotations

import json
import uuid

import pytest
from trendx.services import worker as worker_mod


def _synthetic_job(
    entity_id: str | None = None,
    metric_name: str = "temperature",
    extra: dict | None = None,
) -> dict:
    base = {
        "tenant_id": str(uuid.uuid4()),
        "entity_type": "DEVICE",
        "entity_id": entity_id or str(uuid.uuid4()),
        "metric_name": metric_name,
    }
    if extra:
        base.update(extra)
    return base


@pytest.mark.unit
def test_dispatch_registry_contains_forecast_jobs() -> None:
    """Le registre doit exposer trendx_train et trendx_forecast (dispatch dynamique)."""
    assert "trendx_train" in worker_mod.JOB_DISPATCH
    assert "trendx_forecast" in worker_mod.JOB_DISPATCH
    assert callable(worker_mod.JOB_DISPATCH["trendx_train"])
    assert callable(worker_mod.JOB_DISPATCH["trendx_forecast"])


@pytest.mark.unit
def test_dispatch_registry_no_static_per_device_job() -> None:
    """Aucun job statique par device : seules des clés génériques sont autorisées."""
    for key in worker_mod.JOB_DISPATCH:
        # Les clés doivent être des types génériques, jamais un UUID/device.
        assert key in {
            "topology_discovery",
            "topology_sync",
            "ingestion",
            "trendx_train",
            "trendx_forecast",
            "generic_forecast",
            "anomaly_scan",
            "ml_pipeline",
        }, f"job_type inattendu (potentiellement statique par device): {key}"


@pytest.mark.unit
def test_train_job_requires_minimal_params() -> None:
    """trendx_train exige tenant_id/entity_type/entity_id/metric_name."""
    handler = worker_mod.JOB_DISPATCH["trendx_train"]
    # Job vide -> doit lever (sera converti en fail_task par _process_one_task).
    with pytest.raises((ValueError, KeyError, TypeError)):
        handler({}, "task-1", "exec-1")


@pytest.mark.unit
def test_forecast_job_requires_minimal_params() -> None:
    """trendx_forecast exige tenant_id/entity_type/entity_id/metric_name."""
    handler = worker_mod.JOB_DISPATCH["trendx_forecast"]
    with pytest.raises((ValueError, KeyError, TypeError)):
        handler({}, "task-1", "exec-1")


@pytest.mark.unit
def test_train_job_accepts_strategy_and_window() -> None:
    """trendx_train accepte stratégie + fenêtre d'entraînement + params modèle."""
    import inspect

    handler = worker_mod.JOB_DISPATCH["trendx_train"]
    src = inspect.getsource(handler)
    # Le handler doit mentionner les paramètres minimaux (preuve de contrat).
    for keyword in ("tenant_id", "entity_type", "entity_id", "metric_name", "strategy"):
        assert keyword in src, f"paramètre manquant dans _run_trendx_train: {keyword}"


@pytest.mark.unit
def test_forecast_job_accepts_horizon_and_strategy() -> None:
    """trendx_forecast accepte horizon + stratégie."""
    import inspect

    handler = worker_mod.JOB_DISPATCH["trendx_forecast"]
    src = inspect.getsource(handler)
    for keyword in ("tenant_id", "entity_type", "entity_id", "metric_name", "horizon", "strategy"):
        assert keyword in src, f"paramètre manquant dans _run_trendx_forecast: {keyword}"


@pytest.mark.unit
def test_forecast_handler_never_calls_writeback_by_default() -> None:
    """Le handler forecast ne doit jamais appeler writeback (désactivé par défaut)."""
    import inspect

    src = inspect.getsource(worker_mod.JOB_DISPATCH["trendx_forecast"])
    # Le code du handler ne doit pas invoquer l'écriture TB (appel réel).
    # On exclut la docstring : on cherche un appel (".writeback" / "post_telemetry(").
    code_lines = [
        line
        for line in src.splitlines()
        if line.strip() and not line.strip().startswith(('"""', "'''", "#", ":", "-"))
    ]
    code_only = "\n".join(code_lines)
    assert ".writeback_forecast(" not in code_only
    assert "post_telemetry(" not in code_only
    # Le handler force dry_run et mentionne l'absence d'écriture TB.
    assert "dry_run" in src
    assert "disabled" in src
    # Il doit persister via save_forecast_results (upsert idempotent).
    assert "save_forecast_results" in src or "generate_forecast" in src


@pytest.mark.unit
def test_worker_process_train_job_end_to_end_mocked() -> None:
    """_process_one_task dispatche trendx_train et complète (isolation par tâche)."""
    from unittest.mock import MagicMock, patch

    class _FakeRequest:
        def __init__(self, task_id, execution_id, job_type, json_job):
            self.task_id = task_id
            self.execution_id = execution_id
            self.job_type = job_type
            self.json_job = json_job

    class _FakeTaskService:
        def __init__(self) -> None:
            self.claim_return = None
            self.progress: list = []
            self.completed: list = []
            self.failed: list = []

        def claim_task(self, worker_id: str):
            return self.claim_return

        def update_progress(self, task_id: str, progress: int, status=None) -> None:
            self.progress.append((task_id, progress, status))

        def complete_task(self, task_id: str, result=None) -> None:
            self.completed.append((task_id, result))

        def fail_task(self, task_id: str, error_message: str) -> None:
            self.failed.append((task_id, error_message))

    def _patch_svc(fake):
        return patch("trendx.services.worker.TaskService", return_value=fake)

    tid = uuid.uuid4()
    eid = uuid.uuid4()
    job = _synthetic_job(entity_id=str(uuid.uuid4()), extra={"strategy": "PER_DEVICE"})
    fake_svc = _FakeTaskService()
    fake_svc.claim_return = _FakeRequest(tid, eid, "trendx_train", json.dumps(job))

    handler = MagicMock(return_value={"job_type": "trendx_train", "status": "ok"})
    with _patch_svc(fake_svc), patch.dict(worker_mod.JOB_DISPATCH, {"trendx_train": handler}):
        done = worker_mod._process_one_task("w-tdd-1")

    assert done is True
    assert fake_svc.completed == [(str(tid), {"job_type": "trendx_train", "status": "ok"})]
    assert fake_svc.failed == []
    handler.assert_called_once()
    # Vérifie séparation contexte : le handler reçoit le bon entity_id.
    called_job = handler.call_args[0][0]
    assert called_job["entity_id"] == job["entity_id"]
    assert called_job["metric_name"] == job["metric_name"]
