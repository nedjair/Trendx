"""Unit tests for the MR-4 scheduler service (no database, no threads relied on).

Covers: OFF/ON configuration, guards, no worker import, initialization
wiring, leadership/standby/fencing, shutdown idempotence, HTTP health and
metrics, per-fire fencing after step-down. The election is faked here;
real mutual exclusion is proven in test_scheduler_leader_election.py and
the full service cycle in test_scheduler_service_cycle.py.
"""

from __future__ import annotations

import ast
import sys
import urllib.request
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

import pytest
from trendx.scheduler.jobs import JobSpec
from trendx.scheduler.service import ENQUEUE_JOB_TYPES, SchedulerService


def _settings(**overrides: Any) -> SimpleNamespace:
    base = {
        "trendx_scheduler_enabled": False,
        "trendx_scheduler_instance_id": "unit-1",
        "trendx_scheduler_heartbeat_seconds": 15,
        "trendx_scheduler_stale_seconds": 60,
        "trendx_scheduler_acquire_timeout_seconds": 5,
        "trendx_scheduler_leader_lock": "unit-lock",
        "tb_writeback_enabled": False,
        "tb_alarms_enabled": False,
    }
    base.update(overrides)
    return SimpleNamespace(**base)


def _service(**kwargs: Any) -> SchedulerService:
    settings_obj = kwargs.pop("settings_obj", _settings())
    return SchedulerService(settings_obj=settings_obj, **kwargs)


def _fake_election(*, leader: bool) -> MagicMock:
    election = MagicMock()
    election.is_leader.return_value = leader
    election.renew.return_value = leader
    election.tick.return_value = leader
    election.last_error = None
    election.is_stale.return_value = False
    return election


SPECS = (JobSpec("ingestion-run", "ingestion", "1h", "ingestion"),)


def _service_with_specs(*, leader: bool, **kwargs: Any) -> SchedulerService:
    election = _fake_election(leader=leader)
    svc = _service(specs=SPECS, election=election, **kwargs)
    svc._registry.register_handler("ingestion", lambda payload: {"status": "ok"})
    return svc


# ── configuration OFF/ON ──────────────────────────────────────────────────


def test_disabled_service_schedules_nothing() -> None:
    svc = _service_with_specs(leader=True)
    assert svc.enabled is False
    assert svc.start() == "disabled"
    assert svc._registry.records() == []
    assert svc.health()["status"] == "disabled"
    svc.stop()


def test_enabled_leader_schedules() -> None:
    svc = _service_with_specs(leader=True, settings_obj=_settings(trendx_scheduler_enabled=True))
    assert svc.run_once() == "leader"
    assert svc._scheduled is True
    assert svc._runtime.scheduled_ids() == ["ingestion-run"]
    svc.stop()


def test_enabled_standby_schedules_nothing() -> None:
    svc = _service_with_specs(leader=False, settings_obj=_settings(trendx_scheduler_enabled=True))
    assert svc.run_once() == "standby"
    assert svc._scheduled is False
    assert svc._runtime.scheduled_ids() == []
    svc.stop()


# ── guards ────────────────────────────────────────────────────────────────


@pytest.mark.parametrize("flag", ["tb_writeback_enabled", "tb_alarms_enabled"])
def test_guards_refuse_unsafe_flags(flag: str) -> None:
    svc = _service(settings_obj=_settings(**{flag: True}))
    with pytest.raises(RuntimeError):
        svc.start()
    svc.stop()


# ── no worker import ──────────────────────────────────────────────────────


def test_service_module_has_no_worker_import() -> None:
    root = Path(__file__).parents[2] / "src" / "trendx" / "scheduler" / "service.py"
    assert root.exists()
    tree = ast.parse(root.read_text(), filename=str(root))
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            mods = [a.name for a in node.names]
        elif isinstance(node, ast.ImportFrom):
            mods = [node.module or ""]
        else:
            continue
        assert not any(
            m == "trendx.services.worker" or m.startswith("trendx.services.worker.") for m in mods
        ), mods


def test_service_import_does_not_pull_worker() -> None:
    import trendx.scheduler.service as _svc_mod

    worker_mod = sys.modules.get("trendx.services.worker")
    if worker_mod is None:
        return  # worker never imported in this session: nothing to reference
    held = [
        name
        for name, value in vars(_svc_mod).items()
        if value is worker_mod
        or (isinstance(value, type) and value.__module__ == "trendx.services.worker")
        or (callable(value) and getattr(value, "__module__", "") == "trendx.services.worker")
    ]
    assert held == [], f"service module references worker objects: {held}"


# ── initialization wiring ─────────────────────────────────────────────────


def test_initialization_registers_p0_handlers_with_modes() -> None:
    svc = _service()
    handlers = svc._registry._handlers
    assert set(handlers) == {
        "topology_discovery",
        "topology_sync",
        "ingestion",
        "trendx_train",
        "trendx_forecast",
    }
    assert svc._instance_id == "unit-1"
    assert svc._registry._instance_id == "unit-1"
    svc.stop()


def test_anomaly_scan_without_handler_stays_unscheduled() -> None:
    svc = _service()
    assert "anomaly_scan" not in svc._registry._handlers
    assert "anomaly-scan" not in [s.job_id for s in svc._registry._specs]
    svc.stop()


def test_enqueue_job_types_documented() -> None:
    assert ENQUEUE_JOB_TYPES == {"trendx_train", "trendx_forecast", "anomaly_scan"}


# ── leadership / fencing / standby ────────────────────────────────────────


def test_stepdown_freezes_scheduling() -> None:
    svc = _service_with_specs(leader=True, settings_obj=_settings(trendx_scheduler_enabled=True))
    assert svc.run_once() == "leader"
    assert svc._scheduled is True
    svc._election.is_leader.return_value = False
    svc._election.renew.return_value = False
    svc._election.tick.return_value = False
    assert svc.run_once() == "standby"
    assert svc._scheduled is False
    assert svc._runtime.is_running() is False
    svc.stop()


def test_fire_after_stepdown_executes_nothing_service_level() -> None:
    svc = _service_with_specs(leader=True, settings_obj=_settings(trendx_scheduler_enabled=True))
    assert svc.run_once() == "leader"
    job = svc._runtime._scheduler.get_job("ingestion-run")
    assert job is not None
    svc._election.is_leader.return_value = False
    out = job.func()
    assert out["status"] == "refused-not-leader"
    assert svc._registry.records() == []
    svc.stop()


# ── shutdown ──────────────────────────────────────────────────────────────


def test_stop_is_idempotent_and_releases() -> None:
    svc = _service_with_specs(leader=True, settings_obj=_settings(trendx_scheduler_enabled=True))
    svc.run_once()
    svc.stop()
    svc.stop()
    svc.stop()
    assert svc._scheduled is False
    assert svc._runtime.is_running() is False


# ── HTTP health / metrics ─────────────────────────────────────────────────


def _get(port: int, path: str) -> tuple[int, str, str]:
    import urllib.error as _urlerror

    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{port}{path}", timeout=5) as resp:
            return resp.status, resp.headers.get("Content-Type", ""), resp.read().decode()
    except _urlerror.HTTPError as exc:
        return exc.code, exc.headers.get("Content-Type", ""), exc.read().decode()


def test_http_health_and_metrics() -> None:
    svc = _service(metrics_port=0)
    svc._start_http_server()
    assert svc._httpd is not None
    port = svc._httpd.server_address[1]
    try:
        status, ctype, body = _get(port, "/healthz")
        assert status == 200 and "application/json" in ctype
        assert '"status": "disabled"' in body
        assert '"instance_id": "unit-1"' in body
        status, ctype, body = _get(port, "/metrics")
        assert status == 200 and "text/plain" in ctype
        assert "scheduler_up 1" in body
        assert "scheduler_enabled 0" in body
        assert "scheduler_leader 0" in body
        status, _, _ = _get(port, "/nope")
        assert status == 404
    finally:
        svc.stop()
    # Second stop after HTTP close stays silent.
    svc.stop()


def test_http_health_leader_and_standby() -> None:
    svc = _service_with_specs(leader=True, settings_obj=_settings(trendx_scheduler_enabled=True))
    assert svc.health()["status"] == "leader"
    svc._election.is_leader.return_value = False
    assert svc.health()["status"] == "standby"
    svc._election.last_error = "boom"
    assert svc.health()["status"] == "standby"
    svc.stop()


def test_start_is_idempotent() -> None:
    svc = _service()
    assert svc.start() == "disabled"
    assert svc.start() == "disabled"
    svc.stop()
