"""Scheduler wiring mock P0: registration, dispatch, schedules, idempotency, isolation."""

from __future__ import annotations

from datetime import UTC
from uuid import uuid4

import pytest
from trendx.scheduler.registry import SchedulerRegistry

DEV_A = str(uuid4())
DEV_B = str(uuid4())

EXPECTED_IDS = {
    "discovery-sync-initial",
    "discovery-sync-incremental",
    "ingestion-run",
    "forecast-train",
    "forecast-run",
    "anomaly-scan",
}


def _registry(**handlers):
    reg = SchedulerRegistry()
    for job_type, func in handlers.items():
        reg.register_handler(job_type, func)
    return reg


def _ok(payload):
    return {"status": "ok", "echo": dict(payload)}


# A. registration
def test_a_registration():
    reg = SchedulerRegistry()
    ids = [j.job_id for j in reg.jobs]
    assert set(ids) == EXPECTED_IDS
    assert len(ids) == len(set(ids))
    assert all(j.job_id and j.job_type and j.interval for j in reg.jobs)


# B. dispatch
def test_b_dispatch_routing_and_args():
    seen = {}

    def _handler(payload):
        seen.update(payload)
        return {"status": "ok"}

    reg = _registry(ingestion=_handler)
    out = reg.trigger("ingestion-run", {"device_id": DEV_A, "metric": "power"})
    assert out["status"] == "ok"
    assert seen == {"device_id": DEV_A, "metric": "power"}
    with pytest.raises(KeyError, match="Unknown job id"):
        reg.trigger("no-such-job", {})


def test_b_worker_error_propagates_with_record():
    def _boom(payload):
        raise RuntimeError("sensor_timeout_controlled")

    reg = _registry(ingestion=_boom)
    with pytest.raises(RuntimeError, match="sensor_timeout_controlled"):
        reg.trigger("ingestion-run", {"device_id": DEV_A})
    recs = reg.records()
    assert len(recs) == 1 and recs[0].status == "fail" and "RuntimeError" in (recs[0].error or "")


# C. periodicity (declared, no real waiting)
def test_c_schedules_declared():
    reg = SchedulerRegistry()
    schedules = reg.schedules()
    assert schedules["discovery-sync-incremental"] == "15m"
    assert schedules["ingestion-run"] == "1h"
    assert schedules["forecast-train"] == "1d"
    assert schedules["discovery-sync-initial"] == "once"
    assert len(schedules) == 6


# D. idempotence
def test_d_idempotence():
    calls = []

    def _handler(payload):
        calls.append(dict(payload))
        return {"status": "ok", "n": len(calls)}

    reg = _registry(ingestion=_handler)
    first = reg.trigger("ingestion-run", {"device_id": DEV_A}, idempotency_key="ckpt-123")
    second = reg.trigger("ingestion-run", {"device_id": DEV_A}, idempotency_key="ckpt-123")
    assert len(calls) == 1
    assert second.get("deduplicated") is True
    assert second["run_id"] == first["run_id"]


# E. isolation 2 devices
def test_e_isolation():
    def _flaky(payload):
        if payload.get("device_id") == DEV_A:
            raise ValueError("device A controlled failure")
        return {"status": "ok"}

    reg = _registry(ingestion=_flaky)
    with pytest.raises(ValueError, match="controlled failure"):
        reg.trigger("ingestion-run", {"device_id": DEV_A})
    out = reg.trigger("ingestion-run", {"device_id": DEV_B})
    assert out["status"] == "ok"
    assert [r.status for r in reg.records()] == ["fail", "ok"]


# F. forecast train -> forecast, no writeback
def test_f_forecast_chain_no_writeback():
    order = []

    def _train(payload):
        order.append("train")
        assert payload.get("writeback") is not True
        return {"status": "ok", "model": "linear"}

    def _forecast(payload):
        order.append("forecast")
        assert payload.get("writeback") is not True
        return {"status": "ok", "horizon": 24}

    reg = _registry(trendx_train=_train, trendx_forecast=_forecast)
    assert reg.trigger("forecast-train", {"device_id": DEV_A})["status"] == "ok"
    assert reg.trigger("forecast-run", {"device_id": DEV_A})["status"] == "ok"
    assert order == ["train", "forecast"]


# G. anomalies scan 2 devices, alarms/writeback 0
def test_g_anomaly_scan():
    scanned = []

    def _scan(payload):
        scanned.append(payload.get("device_id"))
        assert payload.get("alarms_enabled") is not True
        assert payload.get("writeback") is not True
        return {"status": "ok", "episodes": 0}

    reg = _registry(anomaly_scan=_scan)
    assert reg.trigger("anomaly-scan", {"device_id": DEV_A})["status"] == "ok"
    assert reg.trigger("anomaly-scan", {"device_id": DEV_B})["status"] == "ok"
    assert sorted(scanned) == sorted([DEV_A, DEV_B])


# H. discovery sync then ingestion, initial/incremental
def test_h_discovery_then_ingestion():
    calls = []

    def _discovery(payload):
        calls.append(("discovery", payload.get("mode")))
        return {"status": "ok", "mode": payload.get("mode")}

    def _ingest(payload):
        calls.append(("ingestion", payload.get("mode")))
        return {"status": "ok"}

    reg = _registry(topology_discovery=_discovery, topology_sync=_discovery, ingestion=_ingest)
    assert reg.trigger("discovery-sync-initial", {"mode": "initial"})["status"] == "ok"
    assert reg.trigger("discovery-sync-incremental", {"mode": "incremental"})["status"] == "ok"
    assert (
        reg.trigger("ingestion-run", {"mode": "incremental", "device_id": DEV_A})["status"] == "ok"
    )
    assert calls[0] == ("discovery", "initial")
    assert calls[1] == ("discovery", "incremental")


def test_records_utc_and_run_ids():
    reg = _registry(ingestion=_ok)
    reg.trigger("ingestion-run", {"device_id": DEV_A})
    rec = reg.records()[0]
    assert rec.triggered_at.tzinfo is not None and rec.triggered_at.tzinfo == UTC
    assert rec.run_id and rec.job_id == "ingestion-run"
