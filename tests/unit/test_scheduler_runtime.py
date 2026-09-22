"""Runtime APScheduler dry-run (jetable): start/stop, dispatch reel, guards."""

from __future__ import annotations

import time
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from apscheduler.schedulers.background import BackgroundScheduler
from trendx.scheduler.registry import SchedulerRegistry
from trendx.scheduler.runtime import RuntimeScheduler

DEV_A = str(uuid4())
DEV_B = str(uuid4())

EXPECTED_IDS = {
    "discovery-sync-initial",
    "discovery-sync-incremental",
    "ingestion-run",
    "forecast-train",
    "forecast-run",
    "anomaly-scan",
    "reconcile-runs",
}


def _registry(**handlers):
    reg = SchedulerRegistry()
    for job_type, func in handlers.items():
        reg.register_handler(job_type, func)
    return reg


def _ok(payload):
    return {"status": "ok"}


def test_start_stop_lifecycle():
    rt = RuntimeScheduler(_registry(ingestion=_ok))
    assert not rt.is_running()
    rt.schedule_all()
    assert rt.scheduled_ids() == sorted(EXPECTED_IDS)
    rt.start()
    assert rt.is_running()
    rt.stop()
    assert not rt.is_running()


def test_context_manager():
    with RuntimeScheduler(_registry(ingestion=_ok)) as rt:
        rt.schedule_all()
        assert rt.is_running()
    assert not rt.is_running()


def test_controlled_dispatch_all_jobs():
    seen: dict[str, dict] = {}

    def _handler(payload):
        seen[payload["job"]] = dict(payload)
        return {"status": "ok"}

    reg = _registry(
        topology_discovery=_handler,
        topology_sync=_handler,
        ingestion=_handler,
        trendx_train=_handler,
        trendx_forecast=_handler,
        anomaly_scan=_handler,
        reconcile_runs=_handler,
    )
    rt = RuntimeScheduler(reg)
    rt.schedule_all()
    for job_id in sorted(EXPECTED_IDS):
        out = rt.run_job_now(job_id, {"job": job_id, "device_id": DEV_A})
        assert out["status"] == "ok"
    assert set(seen) == EXPECTED_IDS
    assert all(v["device_id"] == DEV_A for v in seen.values())


def test_apscheduler_really_fires():
    fired: list[dict] = []
    sched = BackgroundScheduler(timezone=UTC)
    sched.add_job(
        lambda: fired.append({"at": datetime.now(UTC)}),
        trigger="date",
        run_date=datetime.now(UTC) + timedelta(milliseconds=100),
        id="probe",
    )
    sched.start()
    try:
        deadline = time.time() + 10
        while not fired and time.time() < deadline:
            time.sleep(0.05)
    finally:
        sched.shutdown(wait=True)
    assert len(fired) == 1


def test_runtime_fires_scheduled_job():
    fired: list[str] = []

    def _handler(payload):
        fired.append(payload.get("device_id"))
        return {"status": "ok"}

    rt = RuntimeScheduler(_registry(ingestion=_handler))
    rt.schedule_all()
    rt.start()
    try:
        rt.run_job_now("ingestion-run", {"device_id": DEV_A})
    finally:
        rt.stop()
    assert fired == [DEV_A]


def test_idempotence_and_isolation():
    calls: list[dict] = []

    def _flaky(payload):
        calls.append(dict(payload))
        if payload.get("device_id") == DEV_A:
            raise ValueError("controlled A failure")
        return {"status": "ok"}

    rt = RuntimeScheduler(_registry(ingestion=_flaky))
    rt.schedule_all()
    out1 = rt.run_job_now(
        "ingestion-run",
        {"device_id": DEV_B},
    )
    with pytest.raises(ValueError, match="controlled A failure"):
        rt.run_job_now("ingestion-run", {"device_id": DEV_A})
    out2 = rt.run_job_now("ingestion-run", {"device_id": DEV_B})
    assert out1["status"] == "ok" and out2["status"] == "ok"
    assert [r.status for r in rt.registry.records()] == ["ok", "fail", "ok"]


def test_guards_refuse_when_enabled(monkeypatch):
    from trendx.config import settings as _settings

    rt = RuntimeScheduler(_registry(ingestion=_ok))
    monkeypatch.setattr(_settings, "tb_writeback_enabled", True)
    with pytest.raises(RuntimeError, match="TB_WRITEBACK"):
        rt.start()
    monkeypatch.setattr(_settings, "tb_writeback_enabled", False)
    monkeypatch.setattr(_settings, "tb_alarms_enabled", True)
    with pytest.raises(RuntimeError, match="TB_ALARMS"):
        rt.start()


def test_unknown_job_and_interval():
    rt = RuntimeScheduler(_registry(ingestion=_ok))
    rt.schedule_all()
    with pytest.raises(KeyError, match="not scheduled"):
        rt.run_job_now("nope", {})
    from trendx.scheduler.runtime import _trigger_for_interval

    with pytest.raises(ValueError, match="Unknown interval"):
        _trigger_for_interval("9y", datetime.now(UTC))


def test_records_utc():
    rt = RuntimeScheduler(_registry(ingestion=_ok))
    rt.schedule_all()
    rt.run_job_now("ingestion-run", {"device_id": DEV_A})
    rec = rt.registry.records()[0]
    assert rec.triggered_at.tzinfo == UTC and rec.run_id and rec.job_id == "ingestion-run"
