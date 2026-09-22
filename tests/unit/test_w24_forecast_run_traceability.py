"""W24 — Forecast run traceability harness (CODE-ONLY / TEST-ONLY).

Verrouille le chemin normal futur sans rejouer W23L ni ALG16025001 :

  registry.trigger(...) + DbRunStore (interface RunStore)
  -> begin_run une seule fois (running)
  -> handler Forecast (Prophet, PER_DEVICE, 1h, horizon 24)
  -> persistence predictions (upsert idempotent, mockee ici)
  -> finish_run success / failed + erreur tracee
  -> idempotence : pas de double run pour une meme execution
  -> isolation (entity_id, metric_key) : aucun melange inter-entities

100% fakes + UUIDs synthetiques. Aucune DB prod, aucun ThingsBoard,
aucun writeback, aucun mapping, aucune prediction historique, aucun MLflow.
"""

from __future__ import annotations

import uuid
from typing import Any

import pytest
from trendx.scheduler.jobs import JobSpec
from trendx.scheduler.persistence import adapt_p0_handler
from trendx.scheduler.registry import SchedulerRegistry

pytestmark = pytest.mark.unit

FORECAST_SPECS = (JobSpec("forecast-run", "trendx_forecast", "1h", "Periodic forecasting"),)

W24_HORIZON = 24


class FakeRunStore:
    """RunStore en memoire, semantique DbRunStore (sans DB).

    - begin_run : 1 ligne running, finished_at None (invariant).
    - finish_run : running -> ok | failed (fail normalise en failed),
      "enqueued" laisse running (chemin enqueue). Double transition
      terminale refusee (ValueError), comme SchedulerRunRepository.
    """

    def __init__(self) -> None:
        self.begun: list[dict[str, Any]] = []
        self.finished: list[dict[str, Any]] = []
        self.rows: dict[str, dict[str, Any]] = {}

    def begin_run(self, **kwargs: Any) -> None:
        self.begun.append(dict(kwargs))
        self.rows[kwargs["run_id"]] = {"status": "running", **kwargs}

    def finish_run(
        self,
        *,
        run_id: str,
        status: str,
        error: str | None = None,
        task_id: str | None = None,
    ) -> None:
        self.finished.append(
            {"run_id": run_id, "status": status, "error": error, "task_id": task_id}
        )
        row = self.rows[run_id]
        if status == "enqueued":
            if task_id is not None and row.get("task_id") is None:
                row["task_id"] = task_id
            return
        if row["status"] != "running":
            raise ValueError(f"Refusing terminal transition: {row['status']!r}")
        if status == "fail":
            status = "failed"
        if status not in ("ok", "failed"):
            raise ValueError(f"Unknown terminal run status: {status!r}")
        row["status"] = status
        row["error"] = error


def _payload(
    entity_id: str | None = None,
    metric_name: str = "temperature",
    tenant_id: str | None = None,
) -> dict[str, Any]:
    return {
        "tenant_id": tenant_id or str(uuid.uuid4()),
        "entity_type": "DEVICE",
        "entity_id": entity_id or str(uuid.uuid4()),
        "metric_name": metric_name,
        "horizon": W24_HORIZON,
        "strategy": "PER_DEVICE",
    }


def _registry(store: FakeRunStore) -> SchedulerRegistry:
    return SchedulerRegistry(specs=FORECAST_SPECS, run_store=store, instance_id="w24-harness")


def _prophet_handler_factory(
    seen: list[dict[str, Any]],
    *,
    horizon: int = W24_HORIZON,
    saved: int | None = None,
) -> Any:
    """Imite _run_trendx_forecast : Prophet/PER_DEVICE/1h/24, sans TB."""

    def _fn(json_job: Any, task_id: Any, execution_id: Any) -> dict[str, Any]:
        seen.append(dict(json_job))
        # Isolation : le handler ne lit que son propre couple.
        assert json_job["entity_id"] and json_job["metric_name"]
        n = saved if saved is not None else horizon
        return {
            "job_type": "trendx_forecast",
            "status": "ok",
            "entity_id": json_job["entity_id"],
            "metric_name": json_job["metric_name"],
            "strategy": "PER_DEVICE",
            "horizon": horizon,
            "points": horizon,
            "saved": n,
            "writeback": "disabled",
        }

    return _fn


def test_a_forecast_success_creates_exactly_one_run() -> None:
    """TEST A : 1 run, 1 execution, N predictions, status success."""
    store = FakeRunStore()
    reg = _registry(store)
    seen: list[dict[str, Any]] = []
    reg.register_handler(
        "trendx_forecast",
        adapt_p0_handler(_prophet_handler_factory(seen), mode="direct"),
    )
    payload = _payload()

    out = reg.trigger("forecast-run", payload)

    assert out["status"] == "ok"
    assert out["points"] == W24_HORIZON
    assert out["saved"] == W24_HORIZON
    assert len(seen) == 1
    assert len(store.begun) == 1
    assert len(store.finished) == 1
    begun = store.begun[0]
    assert begun["job_id"] == "forecast-run"
    assert begun["job_type"] == "trendx_forecast"
    assert begun["mode"] == "direct"
    row = store.rows[out["run_id"]]
    assert row["status"] == "ok"
    assert row["error"] is None
    assert store.finished[0]["status"] == "ok"


def test_b_forecast_failure_traces_failed_no_ghost_no_double() -> None:
    """TEST B : erreur controlee -> 1 run failed, erreur tracee, pas de doublon."""
    store = FakeRunStore()
    reg = _registry(store)

    def _boom(json_job: Any, task_id: Any, execution_id: Any) -> dict[str, Any]:
        raise RuntimeError("w24-controlled-prophet-failure")

    reg.register_handler("trendx_forecast", adapt_p0_handler(_boom, mode="direct"))

    with pytest.raises(RuntimeError, match="w24-controlled-prophet-failure"):
        reg.trigger("forecast-run", _payload())

    assert len(store.begun) == 1
    assert len(store.finished) == 1
    assert store.finished[0]["status"] == "failed"
    assert "RuntimeError: w24-controlled-prophet-failure" in (store.finished[0]["error"] or "")
    rows = list(store.rows.values())
    assert len(rows) == 1
    assert rows[0]["status"] == "failed"


def test_c_isolation_entity_metric_no_cross_talk() -> None:
    """TEST C : A/metric A ne lit/ecrit que son propre couple."""
    store = FakeRunStore()
    reg = _registry(store)
    seen: list[dict[str, Any]] = []
    reg.register_handler(
        "trendx_forecast",
        adapt_p0_handler(_prophet_handler_factory(seen), mode="direct"),
    )
    tenant_a, tenant_b = str(uuid.uuid4()), str(uuid.uuid4())
    ent_a, ent_b = str(uuid.uuid4()), str(uuid.uuid4())
    assert ent_a != ent_b

    out_a = reg.trigger("forecast-run", _payload(ent_a, "temperature", tenant_a))
    out_b = reg.trigger("forecast-run", _payload(ent_b, "humidity", tenant_b))

    assert out_a["run_id"] != out_b["run_id"]
    assert len(seen) == 2
    assert seen[0]["entity_id"] == ent_a
    assert seen[0]["metric_name"] == "temperature"
    assert seen[1]["entity_id"] == ent_b
    assert seen[1]["metric_name"] == "humidity"
    # Aucun melange : chaque appel n'a vu que son couple.
    assert all(s["entity_id"] == ent_a for s in seen[:1])
    assert all(s["entity_id"] == ent_b for s in seen[1:])
    assert len(store.begun) == 2
    assert len(store.finished) == 2


def test_d_double_trigger_creates_no_second_run() -> None:
    """TEST D : meme execution (idempotency_key) -> un seul forecast_run."""
    store = FakeRunStore()
    reg = _registry(store)
    seen: list[dict[str, Any]] = []
    reg.register_handler(
        "trendx_forecast",
        adapt_p0_handler(_prophet_handler_factory(seen), mode="direct"),
    )
    payload = _payload()

    first = reg.trigger("forecast-run", payload, idempotency_key="w24-once")
    second = reg.trigger("forecast-run", payload, idempotency_key="w24-once")

    assert second.get("deduplicated") is True
    assert first["run_id"] == second["run_id"]
    assert len(seen) == 1
    assert len(store.begun) == 1
    assert len(store.finished) == 1


def test_e_forecast_contract_invariants_preserved() -> None:
    """TEST E (contrat) : handler reel inchange sur les invariants W24.

    Preuve par le code reel (lecture seule, aucune execution) :
    Prophet par défaut, PER_DEVICE, horizon/frequency via settings,
    persistance via save_forecast_results, dry_run force, aucun writeback.
    """
    import inspect

    from trendx.scheduler.handlers import _run_trendx_forecast

    src = inspect.getsource(_run_trendx_forecast)
    assert "tenant_id" in src
    assert "entity_id" in src
    assert "metric_name" in src
    assert "PER_DEVICE" in src
    assert "horizon" in src
    assert "generate_forecast" in src
    assert "save_forecast_results" in src
    assert "dry_run" in src
    code_lines = [
        line
        for line in src.splitlines()
        if line.strip() and not line.strip().startswith(('"""', "'''", "#", ":", "-"))
    ]
    code_only = "\n".join(code_lines)
    assert ".writeback_forecast(" not in code_only
    assert "post_telemetry(" not in code_only

    from trendx.config import settings as _settings

    assert _settings.forecast_frequency == "1h"
    assert _settings.forecast_horizon == 24
    assert _settings.training_lookback_days == 90
