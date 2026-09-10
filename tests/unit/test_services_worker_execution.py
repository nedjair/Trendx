from __future__ import annotations

import asyncio
import json
import uuid
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from trendx.database.models import (
    TrendzTaskExecution,
    TrendzTaskExecutionProgressStep,
    TrendzTaskExecutionRequest,
    TrendzTaskExecutionStateRecord,
)
from trendx.services import worker as worker_mod
from trendx.services.tasks import (
    EXECUTION_STATUS_FINISHED,
    EXECUTION_STATUS_RUNNING,
    PENDING_STATE,
    STATE_RECORD_WORKING,
    TaskService,
)

# ─────────────────────────────────────────────────────────────────────────
# Fake TaskService (tests de flux de contrôle du worker, sans DB)
# ─────────────────────────────────────────────────────────────────────────


class _FakeRequest:
    def __init__(self, task_id: uuid.UUID, execution_id: uuid.UUID, job_type: str, json_job: Any):
        self.task_id = task_id
        self.execution_id = execution_id
        self.job_type = job_type
        self.json_job = json_job


class _FakeTaskService:
    def __init__(self) -> None:
        self.claim_return: _FakeRequest | None = None
        self.progress: list[tuple[str, int, Any]] = []
        self.completed: list[tuple[str, Any]] = []
        self.failed: list[tuple[str, str]] = []

    def claim_task(self, worker_id: str) -> _FakeRequest | None:
        return self.claim_return

    def update_progress(self, task_id: str, progress: int, status: str | None = None) -> None:
        self.progress.append((task_id, progress, status))

    def complete_task(self, task_id: str, result: Any = None) -> None:
        self.completed.append((task_id, result))

    def fail_task(self, task_id: str, error_message: str) -> None:
        self.failed.append((task_id, error_message))


@pytest.fixture
def fake_svc() -> _FakeTaskService:
    return _FakeTaskService()


def _patch_svc(fake: _FakeTaskService):
    return patch("trendx.services.worker.TaskService", return_value=fake)


# ─────────────────────────────────────────────────────────────────────────
# JOB_DISPATCH : registre explicite, sans fallback
# ─────────────────────────────────────────────────────────────────────────


@pytest.mark.unit
def test_job_dispatch_registry_has_three_types() -> None:
    assert set(worker_mod.JOB_DISPATCH.keys()) >= {
        "topology_discovery",
        "topology_sync",
        "ingestion",
    }
    for fn in worker_mod.JOB_DISPATCH.values():
        assert callable(fn)


# ─────────────────────────────────────────────────────────────────────────
# Flux de contrôle : claim -> dispatch -> complete / fail
# ─────────────────────────────────────────────────────────────────────────


@pytest.mark.unit
def test_process_one_task_success_calls_complete(fake_svc: _FakeTaskService) -> None:
    tid = uuid.uuid4()
    eid = uuid.uuid4()
    fake_svc.claim_return = _FakeRequest(
        tid, eid, "topology_discovery", json.dumps({"action": "full_discovery"})
    )
    handler = MagicMock(return_value={"ok": True})
    with _patch_svc(fake_svc), patch.dict(worker_mod.JOB_DISPATCH, {"topology_discovery": handler}):
        done = worker_mod._process_one_task("w1")

    assert done is True
    # progression 0 déclenche la transition CREATED -> RUNNING
    assert fake_svc.progress == [(str(tid), 0, None)]
    # succès -> complete_task (uniquement après exécution réussie)
    assert fake_svc.completed == [(str(tid), {"ok": True})]
    assert fake_svc.failed == []
    handler.assert_called_once_with({"action": "full_discovery"}, str(tid), str(eid))


@pytest.mark.unit
def test_process_one_task_no_task_returns_false(fake_svc: _FakeTaskService) -> None:
    fake_svc.claim_return = None
    with _patch_svc(fake_svc):
        assert worker_mod._process_one_task("w1") is False
    assert fake_svc.progress == []
    assert fake_svc.completed == []
    assert fake_svc.failed == []


@pytest.mark.unit
def test_process_one_task_invalid_json_fails(fake_svc: _FakeTaskService) -> None:
    tid = uuid.uuid4()
    eid = uuid.uuid4()
    fake_svc.claim_return = _FakeRequest(tid, eid, "topology_discovery", "not valid json")
    with _patch_svc(fake_svc):
        done = worker_mod._process_one_task("w1")

    assert done is True
    assert fake_svc.failed and "Invalid json_job" in fake_svc.failed[0][1]
    assert fake_svc.completed == []


@pytest.mark.unit
def test_process_one_task_unknown_job_type_fails(fake_svc: _FakeTaskService) -> None:
    tid = uuid.uuid4()
    eid = uuid.uuid4()
    fake_svc.claim_return = _FakeRequest(tid, eid, "does_not_exist", json.dumps({}))
    with _patch_svc(fake_svc):
        done = worker_mod._process_one_task("w1")

    assert done is True
    assert fake_svc.failed and "Unknown job_type" in fake_svc.failed[0][1]
    assert fake_svc.completed == []


@pytest.mark.unit
def test_process_one_task_handler_exception_fails(fake_svc: _FakeTaskService) -> None:
    tid = uuid.uuid4()
    eid = uuid.uuid4()
    fake_svc.claim_return = _FakeRequest(tid, eid, "topology_discovery", json.dumps({}))
    handler = MagicMock(side_effect=RuntimeError("boom"))
    with _patch_svc(fake_svc), patch.dict(worker_mod.JOB_DISPATCH, {"topology_discovery": handler}):
        done = worker_mod._process_one_task("w1")

    assert done is True
    assert fake_svc.failed and "topology_discovery failed: boom" in fake_svc.failed[0][1]
    assert fake_svc.completed == []


@pytest.mark.unit
def test_process_one_task_update_progress_exception_fails(fake_svc: _FakeTaskService) -> None:
    tid = uuid.uuid4()
    eid = uuid.uuid4()
    fake_svc.claim_return = _FakeRequest(tid, eid, "topology_discovery", json.dumps({}))
    fake_svc.update_progress = MagicMock(side_effect=RuntimeError("db down"))
    with _patch_svc(fake_svc):
        done = worker_mod._process_one_task("w1")

    assert done is True
    assert fake_svc.failed and "update_progress failed" in fake_svc.failed[0][1]
    assert fake_svc.completed == []


# ─────────────────────────────────────────────────────────────────────────
# Pont async : asyncio.run par exécution
# ─────────────────────────────────────────────────────────────────────────


async def _fake_coro() -> None:
    return None


@pytest.mark.unit
def test_async_bridge_uses_asyncio_run_per_handler() -> None:
    captured: list[Any] = []
    svc_instance = MagicMock()
    svc_instance.full_sync = MagicMock(return_value=_fake_coro())
    svc_instance.update_catalog = MagicMock(return_value=_fake_coro())

    with (
        # Les handlers P0 vivent désormais dans trendx.scheduler.handlers
        # (extraction, comportement inchangé) : le seam asyncio/service suit.
        patch("trendx.scheduler.handlers.TopologyDiscoveryService", return_value=svc_instance),
        patch(
            "trendx.scheduler.handlers.asyncio.run",
            side_effect=lambda c: (captured.append(c) or {"ok": True}),
        ),
    ):
        result = worker_mod._run_topology_discovery({}, "t", "e")

    # full_sync() puis update_catalog() : deux appels asyncio.run
    assert len(captured) == 2
    for c in captured:
        assert asyncio.iscoroutine(c)
    svc_instance.full_sync.assert_called_once()
    svc_instance.update_catalog.assert_called_once()
    assert result["status"] == "ok"


# ─────────────────────────────────────────────────────────────────────────
# E2E réel contre le cycle de vie (sans DB réelle, session factice)
# PENDING -> CREATED + WORKING -> RUNNING -> FINISHED
# ─────────────────────────────────────────────────────────────────────────


class _FakeSession:
    def __init__(self, seed_request: TrendzTaskExecutionRequest) -> None:
        self._added: list[Any] = []
        self._seed_request = seed_request

    def execute(self, stmt: Any, *args: Any, **kwargs: Any) -> MagicMock:
        result = MagicMock()
        executions = [o for o in self._added if isinstance(o, TrendzTaskExecution)]
        if executions:
            result.scalars.return_value.first.return_value = executions[-1]
        else:
            result.scalars.return_value.first.return_value = self._seed_request
        return result

    def get(self, cls: Any, pk: Any = None) -> Any:
        for o in self._added:
            if isinstance(o, cls):
                return o
        return None

    def add(self, obj: Any) -> None:
        self._added.append(obj)

    def commit(self) -> None:
        pass

    def rollback(self) -> None:
        pass

    def close(self) -> None:
        pass


@pytest.mark.unit
def test_worker_e2e_real_lifecycle_transitions_to_finished() -> None:
    tid = uuid.uuid4()
    eid = uuid.uuid4()
    request = TrendzTaskExecutionRequest(
        task_id=tid,
        execution_id=eid,
        tenant_id=uuid.uuid4(),
        customer_id=uuid.uuid4(),
        user_id=uuid.uuid4(),
        scheduled=False,
        job_type="topology_discovery",
        json_job=json.dumps({"action": "full_discovery"}),
        created_ts=1,
        state=PENDING_STATE,
    )
    session = _FakeSession(request)
    repo = MagicMock()
    cm = MagicMock()
    cm.__enter__ = lambda s: (session, repo)  # type: ignore[assignment]
    cm.__exit__ = lambda s, *a: False  # type: ignore[assignment]

    svc = TaskService()
    handler = MagicMock(return_value={"ok": True})

    with (
        patch.object(svc, "_get_session_and_repo", return_value=cm),
        patch("trendx.services.worker.TaskService", return_value=svc),
        patch.dict(worker_mod.JOB_DISPATCH, {"topology_discovery": handler}),
    ):
        done = worker_mod._process_one_task("w1")

    assert done is True
    execution = next(o for o in session._added if isinstance(o, TrendzTaskExecution))
    # PENDING request -> execution CREATED -> (RUNNING via progress) -> FINISHED
    assert execution.status == EXECUTION_STATUS_FINISHED
    rec = next(o for o in session._added if isinstance(o, TrendzTaskExecutionStateRecord))
    assert rec.state == STATE_RECORD_WORKING
    steps = [o for o in session._added if isinstance(o, TrendzTaskExecutionProgressStep)]
    assert len(steps) == 1
    assert steps[0].name == "progress:0"


@pytest.mark.unit
def test_worker_e2e_real_lifecycle_running_intermediate() -> None:
    # Vérifie explicitement la transition CREATED -> RUNNING avant FINISHED.
    tid = uuid.uuid4()
    eid = uuid.uuid4()
    request = TrendzTaskExecutionRequest(
        task_id=tid,
        execution_id=eid,
        tenant_id=uuid.uuid4(),
        customer_id=uuid.uuid4(),
        user_id=uuid.uuid4(),
        scheduled=False,
        job_type="topology_sync",
        json_job=json.dumps({"action": "incremental_sync"}),
        created_ts=1,
        state=PENDING_STATE,
    )
    session = _FakeSession(request)
    repo = MagicMock()
    cm = MagicMock()
    cm.__enter__ = lambda s: (session, repo)  # type: ignore[assignment]
    cm.__exit__ = lambda s, *a: False  # type: ignore[assignment]

    svc = TaskService()
    handler = MagicMock(return_value={"ok": True})

    with (
        patch.object(svc, "_get_session_and_repo", return_value=cm),
        patch("trendx.services.worker.TaskService", return_value=svc),
        patch.dict(worker_mod.JOB_DISPATCH, {"topology_sync": handler}),
    ):
        execution = None

        def _capture_progress(task_id: str, progress: int, status: str | None = None) -> None:
            nonlocal execution
            execution = next(o for o in session._added if isinstance(o, TrendzTaskExecution))
            assert execution.status == EXECUTION_STATUS_RUNNING

        svc.update_progress = _capture_progress  # type: ignore[assignment]
        done = worker_mod._process_one_task("w1")

    assert done is True
    assert execution is not None


# ─────────────────────────────────────────────────────────────────────────
# Thread d'exécution : démarrage / arrêt (keep-alive explicite, non-daemon)
# ─────────────────────────────────────────────────────────────────────────


@pytest.mark.unit
def test_execution_loop_start_stop() -> None:
    with patch("trendx.services.worker._process_one_task", return_value=False):
        t = worker_mod.start_execution_loop(poll_interval=0.01, worker_id="test")
        assert t.is_alive()
        assert t is worker_mod._execution_thread
        assert t.daemon is False
        # idempotent : un second appel renvoie le même thread
        t2 = worker_mod.start_execution_loop(poll_interval=0.01)
        assert t2 is t
        worker_mod.stop_execution_loop(timeout=2)
        assert not t.is_alive()
