"""Régression dispatch-isolation : catalogue injectable + fail-closed sans worker.

- catalogue injecté vs défaut P0, rejets de doublons ;
- handler absent => RuntimeError explicite, jamais d'import worker ;
- anti-import services.worker en interpréteur isolé (subprocess) ;
- coexistence : runtime seul vs worker seul, processus séparés.
"""

from __future__ import annotations

import os
import subprocess
import sys

import pytest
from trendx.scheduler import registry as registry_module
from trendx.scheduler.jobs import P0_JOBS, JobSpec
from trendx.scheduler.registry import SchedulerRegistry

CUSTOM_SPECS = (
    JobSpec("custom-once", "custom_type", "once", "custom one-shot"),
    JobSpec("custom-hourly", "custom_type", "1h", "custom hourly"),
)


def _ok(payload: dict[str, object]) -> dict[str, object]:
    return {"status": "ok"}


def test_specs_injected_catalog_used() -> None:
    reg = SchedulerRegistry(specs=CUSTOM_SPECS)
    assert [j.job_id for j in reg.jobs] == ["custom-once", "custom-hourly"]
    assert reg.schedules() == {"custom-once": "once", "custom-hourly": "1h"}
    reg.register_handler("custom_type", _ok)
    out = reg.trigger("custom-once", {"a": 1})
    assert out["status"] == "ok" and out["job_id"] == "custom-once"


def test_specs_duplicates_rejected() -> None:
    dup = (
        JobSpec("dup", "t", "once", "x"),
        JobSpec("dup", "t", "1h", "y"),
    )
    with pytest.raises(ValueError, match="Duplicate job id"):
        SchedulerRegistry(specs=dup)


def test_default_catalog_is_p0() -> None:
    reg = SchedulerRegistry()
    assert {j.job_id for j in reg.jobs} == {s.job_id for s in P0_JOBS}
    assert len(reg.jobs) == 7


def test_patched_module_catalog_takes_effect(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(registry_module, "P0_JOBS", CUSTOM_SPECS)
    reg = SchedulerRegistry()
    assert [j.job_id for j in reg.jobs] == ["custom-once", "custom-hourly"]


def test_missing_handler_fails_closed_without_worker_import() -> None:
    before = set(sys.modules)
    reg = SchedulerRegistry()
    with pytest.raises(RuntimeError, match="explicit dispatch required"):
        reg.trigger("ingestion-run", {"device_id": "x"})
    assert "trendx.services.worker" not in set(sys.modules) - before


def _repo_src() -> str:
    import trendx

    pkg_dir = os.path.dirname(str(trendx.__file__))
    return os.path.dirname(pkg_dir)


def _run_isolated(script: str, timeout: int = 90) -> subprocess.CompletedProcess[str]:
    env = dict(os.environ)
    env["PYTHONPATH"] = _repo_src() + os.pathsep + env.get("PYTHONPATH", "")
    return subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        timeout=timeout,
        env=env,
        check=False,
    )


ISOLATED_PROBE = """
import sys, time
from trendx.scheduler.jobs import JobSpec
from trendx.scheduler.registry import SchedulerRegistry
from trendx.scheduler.runtime import RuntimeScheduler
reg = SchedulerRegistry(specs=(
    JobSpec("probe-once", "probe", "once", "p"),
    JobSpec("probe-hourly", "probe", "1h", "h"),
))
reg.register_handler("probe", lambda p: {"status": "ok"})
rt = RuntimeScheduler(reg)
rt.schedule_all()
assert rt.scheduled_ids() == ["probe-hourly", "probe-once"], rt.scheduled_ids()
rt.start()
assert rt.is_running()
deadline = time.time() + 15
while not reg.records() and time.time() < deadline:
    time.sleep(0.1)
assert reg.records(), "no APScheduler auto fire"
rt.run_job_now("probe-hourly", {"a": 1})
reg2 = SchedulerRegistry(specs=(JobSpec("j", "nope", "once", "x"),))
try:
    reg2.trigger("j", {})
except RuntimeError:
    pass
else:
    raise SystemExit("missing handler did not fail closed")
assert "trendx.services.worker" not in sys.modules, "worker imported!"
rt.stop()
assert not rt.is_running()
print("ISOLATED-OK")
"""


def test_runtime_isolated_no_worker_import_subprocess() -> None:
    proc = _run_isolated(ISOLATED_PROBE)
    assert "ISOLATED-OK" in proc.stdout, proc.stderr[-2000:]
    assert proc.returncode == 0


WORKER_PROBE = """
import trendx.services.worker as w
sched = w._scheduler
assert sched is not None and sched.running, "maintenance scheduler absent"
ids = sorted(j.id for j in sched.get_jobs())
assert "ingestion_hourly" in ids, ids
assert "trendx.services.worker" in __import__("sys").modules
sched.shutdown(wait=False)
print("WORKER-OK " + ",".join(ids))
"""


def test_worker_process_has_maintenance_without_runtime() -> None:
    proc = _run_isolated(WORKER_PROBE)
    assert "WORKER-OK" in proc.stdout, proc.stderr[-2000:]
    assert proc.returncode == 0
