"""Unit tests for scheduler run persistence primitives (no DB).

Covers, without any database:
- terminal status normalization (fail -> failed, fail-closed otherwise) ;
- run mode resolution (direct default, fail-closed otherwise) ;
- adapter payload 1-arg -> (json_job, task_id, execution_id) for both modes ;
- adapt_p0_handler wrappers (direct in-process, enqueue via TaskService
  without in-process execution) ;
- SchedulerRegistry.trigger() lifecycle with a recording FakeRunStore
  (running -> ok/failed + error, exception propagation, no immediate retry,
  idempotency dedup without double persistence, persist-disabled default) ;
- isolation contract: no `services.worker` import anywhere under
  src/trendx/scheduler/ (AST-level check).
"""

from __future__ import annotations

import ast
import uuid
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from trendx.database.repositories import normalize_terminal_status
from trendx.scheduler.jobs import JobSpec
from trendx.scheduler.persistence import (
    adapt_p0_call,
    adapt_p0_handler,
    default_instance_id,
    resolve_run_mode,
)
from trendx.scheduler.registry import SchedulerRegistry

SPECS = (
    JobSpec("ingestion-run", "ingestion", "1h", "Incremental ingestion"),
    JobSpec("forecast-train", "trendx_train", "1d", "Periodic training"),
)


def _registry(**kwargs: Any) -> SchedulerRegistry:
    return SchedulerRegistry(specs=SPECS, **kwargs)


class FakeRunStore:
    """In-memory RunStore recording lifecycle calls (no DB)."""

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
        row["status"] = status
        row["error"] = error


# ── normalization ─────────────────────────────────────────────────────────


def test_normalize_terminal_status_ok_and_failed() -> None:
    assert normalize_terminal_status("ok") == "ok"
    assert normalize_terminal_status("failed") == "failed"


def test_normalize_terminal_status_fail_maps_to_failed() -> None:
    assert normalize_terminal_status("fail") == "failed"


@pytest.mark.parametrize("bad", ["running", "enqueued", "no_data", "", "OK", "FAILED"])
def test_normalize_terminal_status_rejects_unknown(bad: str) -> None:
    with pytest.raises(ValueError):
        normalize_terminal_status(bad)


# ── mode resolution ───────────────────────────────────────────────────────


@pytest.mark.parametrize("raw", [None, ""])
def test_resolve_run_mode_defaults_to_direct(raw: Any) -> None:
    assert resolve_run_mode(raw) == "direct"


@pytest.mark.parametrize("raw", ["direct", "enqueue"])
def test_resolve_run_mode_passthrough(raw: str) -> None:
    assert resolve_run_mode(raw) == raw


@pytest.mark.parametrize("bad", ["DIRECT", "auto", "once", 42])
def test_resolve_run_mode_rejects_unknown(bad: Any) -> None:
    with pytest.raises(ValueError):
        resolve_run_mode(bad)


# ── adapter payload 1-arg -> 3-args ─────────────────────────────────────────


def test_adapt_direct_forces_task_id_none() -> None:
    json_job, task_id, execution_id = adapt_p0_call(
        {"tenant_id": "t", "task_id": "should-be-ignored"},
        execution_id="exec-1",
        mode="direct",
    )
    assert json_job["tenant_id"] == "t"
    assert task_id is None
    assert execution_id == "exec-1"


def test_adapt_enqueue_requires_and_returns_task_id() -> None:
    json_job, task_id, execution_id = adapt_p0_call(
        {"tenant_id": "t"}, execution_id=None, task_id="task-9", mode="enqueue"
    )
    assert task_id == "task-9"
    assert execution_id is None
    assert json_job["tenant_id"] == "t"


def test_adapt_enqueue_without_task_id_fails_closed() -> None:
    with pytest.raises(ValueError):
        adapt_p0_call({"tenant_id": "t"}, execution_id=None, mode="enqueue")


def test_adapt_rejects_unknown_mode() -> None:
    with pytest.raises(ValueError):
        adapt_p0_call({}, execution_id="e", mode="bogus")


# ── adapt_p0_handler wrappers ───────────────────────────────────────────────


def test_wrapper_direct_calls_fn_in_process() -> None:
    seen: list[tuple[Any, ...]] = []

    def fn(json_job: Any, task_id: Any, execution_id: Any) -> dict[str, Any]:
        seen.append((json_job, task_id, execution_id))
        return {"status": "ok"}

    wrapped = adapt_p0_handler(fn, mode="direct")
    out = wrapped({"tenant_id": "t", "execution_id": "exec-7"})
    assert out == {"status": "ok"}
    assert len(seen) == 1
    json_job, task_id, execution_id = seen[0]
    assert json_job["tenant_id"] == "t"
    assert task_id is None
    assert execution_id == "exec-7"


def test_wrapper_direct_generates_execution_id_when_absent() -> None:
    seen: list[tuple[Any, ...]] = []

    def fn(json_job: Any, task_id: Any, execution_id: Any) -> dict[str, Any]:
        seen.append((json_job, task_id, execution_id))
        return {"status": "ok"}

    wrapped = adapt_p0_handler(fn, mode="direct")
    wrapped({"tenant_id": "t"})
    assert seen[0][2]  # generated execution_id, non-empty


def test_wrapper_enqueue_creates_task_without_calling_fn() -> None:
    called: list[Any] = []
    fake_task = MagicMock()
    fake_task.id = uuid.uuid4()

    def fn(json_job: Any, task_id: Any, execution_id: Any) -> dict[str, Any]:
        called.append((json_job, task_id, execution_id))
        return {"status": "ok"}  # pragma: no cover - must never run

    svc = MagicMock()
    svc.create_task.return_value = fake_task
    with patch("trendx.services.tasks.TaskService", return_value=svc):
        wrapped = adapt_p0_handler(fn, mode="enqueue")
        out = wrapped({"job_type": "trendx_train", "tenant_id": "t"})

    assert called == []  # no in-process execution
    svc.create_task.assert_called_once()
    kwargs = svc.create_task.call_args
    assert kwargs.kwargs["job_type"] == "trendx_train"
    assert kwargs.kwargs["json_job"]["tenant_id"] == "t"
    assert out["status"] == "enqueued"
    assert out["task_id"] == str(fake_task.id)


# ── trigger() lifecycle with FakeRunStore ───────────────────────────────────


def test_trigger_persists_running_then_ok() -> None:
    store = FakeRunStore()
    reg = _registry(run_store=store, instance_id="i-1")
    reg.register_handler("ingestion", lambda payload: {"status": "ok", "tasks": 2})

    out = reg.trigger("ingestion-run", {"tenant_id": "t"})

    assert out["status"] == "ok"
    assert len(store.begun) == 1
    begun = store.begun[0]
    assert begun["job_id"] == "ingestion-run"
    assert begun["job_type"] == "ingestion"
    assert begun["instance_id"] == "i-1"
    assert begun["mode"] == "direct"
    assert begun["task_id"] is None
    assert len(store.finished) == 1
    assert store.finished[0]["status"] == "ok"
    assert store.finished[0]["error"] is None
    assert store.rows[out["run_id"]]["status"] == "ok"


def test_trigger_persists_failed_with_error_and_reraises() -> None:
    store = FakeRunStore()
    reg = _registry(run_store=store, instance_id="i-1")

    def boom(payload: dict[str, Any]) -> dict[str, Any]:
        raise RuntimeError("kaput")

    reg.register_handler("ingestion", boom)
    with pytest.raises(RuntimeError, match="kaput"):
        reg.trigger("ingestion-run", {"tenant_id": "t"})

    assert len(store.begun) == 1  # exactly one attempt: no immediate retry
    assert len(store.finished) == 1
    assert store.finished[0]["status"] == "failed"
    assert "RuntimeError: kaput" in (store.finished[0]["error"] or "")
    row = next(iter(store.rows.values()))
    assert row["status"] == "failed"


def test_trigger_without_store_keeps_historical_behavior() -> None:
    reg = _registry()  # run_store=None by default
    reg.register_handler("ingestion", lambda payload: {"status": "ok"})
    out = reg.trigger("ingestion-run", {"tenant_id": "t"})
    assert out["status"] == "ok"
    assert out["run_id"]


def test_trigger_idempotency_dedup_does_not_persist_twice() -> None:
    store = FakeRunStore()
    reg = _registry(run_store=store, instance_id="i-1")
    calls: list[Any] = []
    reg.register_handler("ingestion", lambda payload: calls.append(payload) or {"n": 1})

    first = reg.trigger("ingestion-run", {"a": 1}, idempotency_key="k")
    second = reg.trigger("ingestion-run", {"a": 1}, idempotency_key="k")

    assert second.get("deduplicated") is True
    assert first["run_id"] == second["run_id"]
    assert len(calls) == 1
    assert len(store.begun) == 1
    assert len(store.finished) == 1


def test_trigger_rejects_unknown_mode_before_persisting() -> None:
    store = FakeRunStore()
    reg = _registry(run_store=store, instance_id="i-1")
    reg.register_handler("ingestion", lambda payload: {"status": "ok"})
    with pytest.raises(ValueError):
        reg.trigger("ingestion-run", {"scheduler_mode": "bogus"})
    assert store.begun == []
    assert store.finished == []


def test_trigger_enqueue_outcome_leaves_row_running_with_task() -> None:
    store = FakeRunStore()
    reg = _registry(run_store=store, instance_id="i-1")
    reg.register_handler(
        "trendx_train", lambda payload: {"status": "enqueued", "task_id": "task-3"}
    )

    out = reg.trigger("forecast-train", {"scheduler_mode": "enqueue"})

    assert out["status"] == "enqueued"
    assert len(store.begun) == 1
    assert store.begun[0]["mode"] == "enqueue"
    assert len(store.finished) == 1
    assert store.finished[0]["status"] == "enqueued"
    assert store.rows[out["run_id"]]["status"] == "running"
    assert store.rows[out["run_id"]]["task_id"] == "task-3"


# ── misc ────────────────────────────────────────────────────────────────────


def test_default_instance_id_is_stable_per_registry_and_nonempty() -> None:
    assert default_instance_id()
    reg = _registry()
    assert reg._instance_id
    assert _registry()._instance_id != ""  # generated per registry


def test_scheduler_package_has_no_worker_import() -> None:
    root = Path(__file__).parents[3] / "src" / "trendx" / "scheduler"
    offenders: list[str] = []
    for path in sorted(root.glob("*.py")):
        tree = ast.parse(path.read_text(), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                mods = [a.name for a in node.names]
            elif isinstance(node, ast.ImportFrom):
                mods = [node.module or ""]
            else:
                continue
            if any(
                m == "trendx.services.worker" or m.startswith("trendx.services.worker.")
                for m in mods
            ):
                offenders.append(f"{path.name}: {mods}")
    assert offenders == [], f"scheduler imports services.worker: {offenders}"
