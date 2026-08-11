from __future__ import annotations

import uuid
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy.dialects import postgresql
from trendx.config import settings
from trendx.database.models import (
    TrendzTask,
    TrendzTaskExecutionProgressStep,
    TrendzTaskExecutionRequest,
    TrendzTaskExecutionStateRecord,
)
from trendx.services.tasks import PENDING_STATE, TaskService

# ── Issue: trendx_task_execution_request + trendz_task_execution_state_record
#    Le cycle de vie réel (claim/update/complete/fail) doit passer par ces
#    tables Trendz 1.15.0, non par trendz_task. Ces méthodes sont des stubs.
#    TRENDX_SCHEDULER_STORAGE trendx_catalog.trendz_task dépend de ce cycle
#    de vie pour la Phase 3. Sans implémentation → la planification n'a pas
#    de suivi d'état de job.
_STUB_REASON = (
    "claim/complete/fail non implémentés — issue #42: "
    "requires trendz_task_execution_request + "
    "trendz_task_execution_state_record integration"
)


@pytest.fixture
def service():
    return TaskService()


@pytest.fixture
def mock_session_repo():
    session = MagicMock()
    session.commit = MagicMock(side_effect=None)
    repo = MagicMock()
    cm = MagicMock()
    cm.__enter__ = MagicMock(return_value=(session, repo))
    cm.__exit__ = MagicMock(return_value=False)
    return session, repo, cm


@pytest.fixture(autouse=True)
def _mock_tenant_config(monkeypatch):
    monkeypatch.setattr(settings, "trendx_default_tenant_id", str(uuid.uuid4()))
    monkeypatch.setattr(settings, "trendx_default_customer_id", str(uuid.uuid4()))
    monkeypatch.setattr(settings, "trendx_default_user_id", str(uuid.uuid4()))


# ── create_task — REAL implementation (no stub) ──────────────────────────


@pytest.mark.unit
def test_create_task(service, mock_session_repo):
    session, repo, cm = mock_session_repo
    repo.create.return_value = MagicMock(id="task-1", name="test-task")

    with patch.object(service, "_get_session_and_repo", return_value=cm):
        task = service.create_task(
            name="test-task",
            job_type="forecast",
            json_job={"device_id": "dev-001"},
        )

    assert task.id == "task-1"
    repo.create.assert_called_once()


@pytest.mark.unit
def test_create_task_requires_tenant_config(monkeypatch):
    monkeypatch.setattr(settings, "trendx_default_tenant_id", "")
    monkeypatch.setattr(settings, "trendx_default_customer_id", "")
    monkeypatch.setattr(settings, "trendx_default_user_id", "")

    with pytest.raises(ValueError, match="TRENDX_DEFAULT_TENANT_ID"):
        service = TaskService()
        service.create_task(name="t", job_type="forecast", json_job={})


# ── cancel_task — REAL implementation (no stub) ─────────────────────────


@pytest.mark.unit
def test_cancel_task(service, mock_session_repo):
    session, repo, cm = mock_session_repo
    running = MagicMock(id="task-1", enabled=True)
    repo.get.return_value = running

    with patch.object(service, "_get_session_and_repo", return_value=cm):
        result = service.cancel_task("task-1")

    assert result is not None
    assert result.enabled is False
    session.commit.assert_called_once()


@pytest.mark.unit
def test_cancel_task_not_found(service, mock_session_repo):
    _, repo, cm = mock_session_repo
    repo.get.return_value = None

    with patch.object(service, "_get_session_and_repo", return_value=cm):
        result = service.cancel_task("task-nonexistent")

    assert result is None
    repo.get.assert_called_once_with("task-nonexistent")


# ── retry_task — REAL implementation (no stub) ──────────────────────────


@pytest.mark.unit
def test_retry_task(service, mock_session_repo):
    session, repo, cm = mock_session_repo
    retried = MagicMock(
        id="task-1",
        enabled=False,
        tenant_id=uuid.uuid4(),
        customer_id=uuid.uuid4(),
        user_id=uuid.uuid4(),
        job_type="forecast",
        json_job='{"device_id": "dev-001"}',
    )
    repo.get.return_value = retried

    added: list[Any] = []
    session.add.side_effect = added.append

    with patch.object(service, "_get_session_and_repo", return_value=cm):
        result = service.retry_task("task-1")

    assert result is not None
    assert result.enabled is True
    session.commit.assert_called_once()
    # A brand-new PENDING execution request must be queued (the actual fix).
    requests = [o for o in added if isinstance(o, TrendzTaskExecutionRequest)]
    assert len(requests) == 1
    req = requests[0]
    assert req.task_id == "task-1"
    assert req.state == PENDING_STATE
    assert req.scheduled is False
    assert req.job_type == "forecast"
    assert req.json_job == '{"device_id": "dev-001"}'
    assert isinstance(req.execution_id, uuid.UUID)


@pytest.mark.unit
def test_retry_task_not_found(service, mock_session_repo):
    _, repo, cm = mock_session_repo
    repo.get.return_value = None

    with patch.object(service, "_get_session_and_repo", return_value=cm):
        result = service.retry_task("task-nonexistent")

    assert result is None
    repo.get.assert_called_once_with("task-nonexistent")


# ── retry_task behavioral: real end-to-end re-claim via in-memory catalog ──
# Self-contained SQLite engine (no conftest change, no real DB touched). It
# proves the retried request is actually consumable by claim_task, while the
# original FAILED execution is preserved and the consumed request is excluded.


def _in_memory_catalog_session():
    """Build an isolated in-memory SQLite session with only the catalog
    lifecycle tables needed for the claim/retry flow."""
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    from trendx.database.models import (
        Base,
        TrendzTask,
        TrendzTaskExecution,
        TrendzTaskExecutionRequest,
        TrendzTaskExecutionStateRecord,
    )

    engine = create_engine(
        "sqlite://", poolclass=__import__("sqlalchemy.pool", fromlist=["StaticPool"]).StaticPool
    )
    Base.metadata.create_all(
        engine,
        tables=[
            TrendzTask.__table__,
            TrendzTaskExecution.__table__,
            TrendzTaskExecutionRequest.__table__,
            TrendzTaskExecutionStateRecord.__table__,
        ],
    )
    return sessionmaker(bind=engine)()


@pytest.mark.unit
def test_retry_makes_task_reclaimable_and_preserves_failed_execution():
    from trendx.database.models import (
        TrendzTask,
        TrendzTaskExecution,
        TrendzTaskExecutionRequest,
    )
    from trendx.database.repositories import TrendzTaskRepository

    session = _in_memory_catalog_session()
    task_id = uuid.uuid4()
    tenant = uuid.uuid4()
    customer = uuid.uuid4()
    user = uuid.uuid4()
    first_exec_id = uuid.uuid4()
    task = TrendzTask(
        id=task_id,
        tenant_id=tenant,
        customer_id=customer,
        user_id=user,
        created_ts=1,
        updated_ts=1,
        name="retryable",
        enabled=False,
        reference_type="MANUAL",
        reference_key="rk",
        job_type="topology_discovery",
        json_job='{"k": "v"}',
        schedule_type="NOT_SCHEDULED",
        schedule_period_ts=0,
        schedule_planned_ts=0,
        schedule_scheduling_unit="",
        schedule_scheduling_unit_count=0,
        schedule_scheduling_time_zone="UTC",
        ttl_enabled=False,
        ttl_duration=0,
        store_execution_enabled=False,
        store_execution_count=0,
        json_configs="{}",
    )
    first_request = TrendzTaskExecutionRequest(
        task_id=task_id,
        execution_id=first_exec_id,
        tenant_id=tenant,
        customer_id=customer,
        user_id=user,
        scheduled=False,
        job_type="topology_discovery",
        json_job='{"k": "v"}',
        created_ts=1,
        state=PENDING_STATE,
    )
    failed_execution = TrendzTaskExecution(
        id=first_exec_id,
        task_id=task_id,
        tenant_id=tenant,
        customer_id=customer,
        user_id=user,
        status="FAILED",
        created_ts=1,
        start_ts=1,
        finish_ts=2,
        duration=1,
        json_progress_content="{}",
        job_type="topology_discovery",
        json_job='{"k": "v"}',
        json_result='{"error": "boom"}',
    )
    session.add_all([task, first_request, failed_execution])
    session.commit()

    svc = TaskService()
    with patch.object(svc, "_get_session_and_repo") as get_session:
        get_session.return_value.__enter__.return_value = (session, TrendzTaskRepository(session))

        # Sanity: before retry, the original request is excluded by NOT EXISTS
        # (its execution_id already has a FAILED execution) → nothing claimable.
        claimed_before = svc.claim_task(worker_id="worker-1")
        assert claimed_before is None

        # Retry: must queue a NEW PENDING request with a fresh execution_id.
        svc.retry_task(str(task_id))

    requests = (
        session.query(TrendzTaskExecutionRequest)
        .filter_by(task_id=task_id)
        .order_by(TrendzTaskExecutionRequest.created_ts.asc())
        .all()
    )
    assert len(requests) == 2
    new_request = requests[-1]
    assert new_request.execution_id != first_exec_id
    assert new_request.state == PENDING_STATE
    assert new_request.scheduled is False
    assert new_request.job_type == "topology_discovery"
    assert new_request.json_job == '{"k": "v"}'

    # The original FAILED execution is preserved, untouched.
    still_failed = session.query(TrendzTaskExecution).filter_by(id=first_exec_id).one()
    assert still_failed.status == "FAILED"

    # The new request IS claimable: claim_task selects it and creates a new
    # execution for the new execution_id (NOT EXISTS now satisfied).
    with patch.object(svc, "_get_session_and_repo") as get_session:
        get_session.return_value.__enter__.return_value = (session, TrendzTaskRepository(session))
        claimed = svc.claim_task(worker_id="worker-2")
    assert claimed is not None
    assert claimed.execution_id == new_request.execution_id
    assert claimed.task_id == task_id

    # After claiming, the new request is also excluded (its execution now exists).
    with patch.object(svc, "_get_session_and_repo") as get_session:
        get_session.return_value.__enter__.return_value = (session, TrendzTaskRepository(session))
        claimed_again = svc.claim_task(worker_id="worker-3")
    assert claimed_again is None


@pytest.mark.unit
def test_retry_leaves_original_request_untouched():
    """The previous execution request must NOT be deleted or mutated."""

    from trendx.database.models import (
        TrendzTask,
        TrendzTaskExecutionRequest,
    )
    from trendx.database.repositories import TrendzTaskRepository

    session = _in_memory_catalog_session()
    task_id = uuid.uuid4()
    tenant = uuid.uuid4()
    customer = uuid.uuid4()
    user = uuid.uuid4()
    first_exec_id = uuid.uuid4()
    task = TrendzTask(
        id=task_id,
        tenant_id=tenant,
        customer_id=customer,
        user_id=user,
        created_ts=1,
        updated_ts=1,
        name="retryable",
        enabled=False,
        reference_type="MANUAL",
        reference_key="rk",
        job_type="topology_discovery",
        json_job='{"k": "v"}',
        schedule_type="NOT_SCHEDULED",
        schedule_period_ts=0,
        schedule_planned_ts=0,
        schedule_scheduling_unit="",
        schedule_scheduling_unit_count=0,
        schedule_scheduling_time_zone="UTC",
        ttl_enabled=False,
        ttl_duration=0,
        store_execution_enabled=False,
        store_execution_count=0,
        json_configs="{}",
    )
    original_request = TrendzTaskExecutionRequest(
        task_id=task_id,
        execution_id=first_exec_id,
        tenant_id=tenant,
        customer_id=customer,
        user_id=user,
        scheduled=False,
        job_type="topology_discovery",
        json_job='{"k": "v"}',
        created_ts=1,
        state=PENDING_STATE,
    )
    session.add_all([task, original_request])
    session.commit()

    svc = TaskService()
    with patch.object(svc, "_get_session_and_repo") as get_session:
        get_session.return_value.__enter__.return_value = (session, TrendzTaskRepository(session))
        svc.retry_task(str(task_id))

    remaining = session.query(TrendzTaskExecutionRequest).filter_by(task_id=task_id).all()
    assert len(remaining) == 2
    original = next(r for r in remaining if r.execution_id == first_exec_id)
    assert original.state == PENDING_STATE  # unchanged (NOT deleted/mutated)


@pytest.mark.unit
def test_retry_then_claim_full_lifecycle_failed_to_running():
    """End-to-end chain: FAILED -> retry -> PENDING request -> claim -> CREATED
    execution (WORKING state record), with the original FAILED execution kept."""
    from trendx.database.models import (
        TrendzTask,
        TrendzTaskExecution,
        TrendzTaskExecutionRequest,
        TrendzTaskExecutionStateRecord,
    )
    from trendx.database.repositories import TrendzTaskRepository
    from trendx.services.tasks import EXECUTION_STATUS_CREATED, STATE_RECORD_WORKING

    session = _in_memory_catalog_session()
    task_id = uuid.uuid4()
    tenant = uuid.uuid4()
    customer = uuid.uuid4()
    user = uuid.uuid4()
    first_exec_id = uuid.uuid4()
    task = TrendzTask(
        id=task_id,
        tenant_id=tenant,
        customer_id=customer,
        user_id=user,
        created_ts=1,
        updated_ts=1,
        name="retryable",
        enabled=False,
        reference_type="MANUAL",
        reference_key="rk",
        job_type="topology_discovery",
        json_job='{"k": "v"}',
        schedule_type="NOT_SCHEDULED",
        schedule_period_ts=0,
        schedule_planned_ts=0,
        schedule_scheduling_unit="",
        schedule_scheduling_unit_count=0,
        schedule_scheduling_time_zone="UTC",
        ttl_enabled=False,
        ttl_duration=0,
        store_execution_enabled=False,
        store_execution_count=0,
        json_configs="{}",
    )
    first_request = TrendzTaskExecutionRequest(
        task_id=task_id,
        execution_id=first_exec_id,
        tenant_id=tenant,
        customer_id=customer,
        user_id=user,
        scheduled=False,
        job_type="topology_discovery",
        json_job='{"k": "v"}',
        created_ts=1,
        state=PENDING_STATE,
    )
    failed_execution = TrendzTaskExecution(
        id=first_exec_id,
        task_id=task_id,
        tenant_id=tenant,
        customer_id=customer,
        user_id=user,
        status="FAILED",
        created_ts=1,
        start_ts=1,
        finish_ts=2,
        duration=1,
        json_progress_content="{}",
        job_type="topology_discovery",
        json_job='{"k": "v"}',
        json_result='{"error": "boom"}',
    )
    session.add_all([task, first_request, failed_execution])
    session.commit()

    svc = TaskService()
    with patch.object(svc, "_get_session_and_repo") as get_session:
        get_session.return_value.__enter__.return_value = (session, TrendzTaskRepository(session))

        # retry re-queues a fresh PENDING request with a new execution_id
        svc.retry_task(str(task_id))

        # claim picks up the new request and creates a CREATED execution
        claimed = svc.claim_task(worker_id="worker-1")

    assert claimed is not None
    new_exec = session.get(TrendzTaskExecution, claimed.execution_id)
    assert new_exec is not None
    assert new_exec.status == EXECUTION_STATUS_CREATED
    state_record = (
        session.query(TrendzTaskExecutionStateRecord)
        .filter_by(execution_id=claimed.execution_id)
        .one()
    )
    assert state_record.state == STATE_RECORD_WORKING

    # original FAILED execution is preserved in history
    still_failed = session.get(TrendzTaskExecution, first_exec_id)
    assert still_failed is not None
    assert still_failed.status == "FAILED"

    # after claiming, the new request is excluded (its execution now exists)
    with patch.object(svc, "_get_session_and_repo") as get_session:
        get_session.return_value.__enter__.return_value = (session, TrendzTaskRepository(session))
        assert svc.claim_task(worker_id="worker-2") is None


# ── create_task → populates trendz_task_execution_request (PENDING, scheduled=False) ──


@pytest.mark.unit
def test_create_task_populates_execution_request(service, mock_session_repo):
    session, repo, cm = mock_session_repo
    created_task = MagicMock()
    created_task.id = uuid.uuid4()
    repo.create.return_value = created_task

    added: list[Any] = []
    session.add.side_effect = added.append

    with patch.object(service, "_get_session_and_repo", return_value=cm):
        task = service.create_task(
            name="t",
            job_type="forecast",
            json_job={"device_id": "dev-001"},
        )

    assert task is created_task
    requests = [o for o in added if isinstance(o, TrendzTaskExecutionRequest)]
    assert len(requests) == 1
    req = requests[0]
    assert req.state == PENDING_STATE
    assert req.state == "PENDING"
    assert req.scheduled is False
    assert req.task_id == created_task.id
    assert req.job_type == "forecast"
    assert req.tenant_id is not None
    assert req.customer_id is not None
    assert req.user_id is not None
    assert req.execution_id is not None


# ── claim_task — REAL implementation (trendz_task_execution_request) ──


def _mock_claim_result(session, row):
    result = MagicMock()
    result.scalars.return_value.first.return_value = row
    session.execute.return_value = result


@pytest.mark.unit
def test_claim_task(service, mock_session_repo):
    session, _repo, cm = mock_session_repo
    request_row = TrendzTaskExecutionRequest(
        task_id=uuid.uuid4(),
        execution_id=uuid.uuid4(),
        tenant_id=uuid.uuid4(),
        customer_id=uuid.uuid4(),
        user_id=uuid.uuid4(),
        scheduled=False,
        job_type="forecast",
        json_job="{}",
        created_ts=123,
        state=PENDING_STATE,
    )
    _mock_claim_result(session, request_row)

    with patch.object(service, "_get_session_and_repo", return_value=cm):
        claimed = service.claim_task(worker_id="worker-1")

    assert claimed is request_row
    session.execute.assert_called_once()


@pytest.mark.unit
def test_claim_task_no_pending(service, mock_session_repo):
    session, _repo, cm = mock_session_repo
    _mock_claim_result(session, None)

    with patch.object(service, "_get_session_and_repo", return_value=cm):
        claimed = service.claim_task(worker_id="worker-1")

    assert claimed is None
    session.execute.assert_called_once()


@pytest.mark.unit
def test_claim_task_respects_job_type_filter(service, mock_session_repo):
    session, _repo, cm = mock_session_repo
    _mock_claim_result(session, None)

    with patch.object(service, "_get_session_and_repo", return_value=cm):
        claimed = service.claim_task(worker_id="worker-1", job_type_filter="forecast")

    assert claimed is None
    stmt = session.execute.call_args.args[0]
    sql = str(stmt.compile(dialect=postgresql.dialect()))
    assert "job_type =" in sql


# ── claim SELECT statement construction (SKIP LOCKED / criteria) ──


@pytest.mark.unit
def test_claim_stmt_uses_skip_locked(service):
    stmt = service._build_claim_stmt()
    sql = str(stmt.compile(dialect=postgresql.dialect()))
    assert "FOR UPDATE" in sql
    assert "SKIP LOCKED" in sql
    # available-task criteria
    assert "scheduled" in sql
    assert "state" in sql
    # PENDING bound value
    params = dict(stmt.compile(dialect=postgresql.dialect()).params)
    assert any(v == PENDING_STATE for v in params.values())


@pytest.mark.unit
def test_claim_stmt_job_type_filter_present_only_when_requested(service):
    stmt_no = service._build_claim_stmt()
    stmt_yes = service._build_claim_stmt(job_type_filter="forecast")
    sql_no = str(stmt_no.compile(dialect=postgresql.dialect()))
    sql_yes = str(stmt_yes.compile(dialect=postgresql.dialect()))
    assert "job_type =" not in sql_no
    assert "job_type =" in sql_yes
    params = dict(stmt_yes.compile(dialect=postgresql.dialect()).params)
    assert any(v == "forecast" for v in params.values())


# ── update_progress / complete_task / fail_task — REAL schema (trendz_task_execution*) ──
# No migration: trendz_task_execution.id == request.execution_id; execution.status uses the
# canonical Trendz values {CREATED,RUNNING,FINISHED,FAILED,CANCELED,LOST,NONE}; state_record.state
# uses {WORKING,CANCELLED}. PENDING stays request-only. These tests assert on the real ORM
# objects (execution / state_record / progress_step), not on non-existent trendz_task columns.


def _fake_execution(status="CREATED", start_ts=1000):
    ex = MagicMock()
    ex.id = uuid.uuid4()
    ex.status = status
    ex.start_ts = start_ts
    ex.finish_ts = 0
    ex.duration = 0
    ex.json_progress_content = "{}"
    ex.json_result = "{}"
    return ex


def _configure_session(session, execution, task_id="task-1"):
    """Wire a mock session so the service resolves `execution` and a task."""
    res = MagicMock()
    res.scalars.return_value.first.return_value = execution
    session.execute.return_value = res

    def _get(model, ident):
        if model is TrendzTask:
            return MagicMock(id=task_id)
        if model is TrendzTaskExecutionStateRecord:
            # force the creation path so we can assert a state_record is added
            return None
        return MagicMock()

    session.get.side_effect = _get
    return session


@pytest.mark.unit
def test_update_progress(service, mock_session_repo):
    session, _repo, cm = mock_session_repo
    execution = _fake_execution(status="CREATED")
    _configure_session(session, execution)
    added = []
    session.add.side_effect = added.append

    with patch.object(service, "_get_session_and_repo", return_value=cm):
        result = service.update_progress("task-1", 50)

    assert result is not None
    assert result.id == "task-1"
    # progress is persisted on the execution row (real JSON column)
    assert '"progress": 50' in execution.json_progress_content
    # CREATED -> RUNNING on first progress update
    assert execution.status == "RUNNING"
    steps = [o for o in added if isinstance(o, TrendzTaskExecutionProgressStep)]
    assert len(steps) == 1
    assert steps[0].execution_id == execution.id
    state_records = [o for o in added if isinstance(o, TrendzTaskExecutionStateRecord)]
    assert len(state_records) == 1
    assert state_records[0].state == "WORKING"


@pytest.mark.unit
def test_update_progress_with_explicit_status(service, mock_session_repo):
    session, _repo, cm = mock_session_repo
    execution = _fake_execution(status="RUNNING")
    _configure_session(session, execution)
    added = []
    session.add.side_effect = added.append

    with patch.object(service, "_get_session_and_repo", return_value=cm):
        result = service.update_progress("task-1", 75, status="RUNNING")

    assert result is not None
    assert execution.status == "RUNNING"
    assert '"progress": 75' in execution.json_progress_content


@pytest.mark.unit
def test_update_progress_invalid(service):
    with pytest.raises(ValueError, match="Progress must be between"):
        service.update_progress("task-1", -1)
    with pytest.raises(ValueError, match="Progress must be between"):
        service.update_progress("task-1", 150)


@pytest.mark.unit
def test_update_progress_invalid_status(service):
    with pytest.raises(ValueError, match="Invalid execution status"):
        service.update_progress("task-1", 10, status="COMPLETED")


@pytest.mark.unit
def test_update_progress_not_found(service, mock_session_repo):
    session, _repo, cm = mock_session_repo
    res = MagicMock()
    res.scalars.return_value.first.return_value = None
    session.execute.return_value = res

    with patch.object(service, "_get_session_and_repo", return_value=cm):
        result = service.update_progress("task-nonexistent", 50)

    assert result is None


@pytest.mark.unit
def test_complete_task(service, mock_session_repo):
    session, _repo, cm = mock_session_repo
    execution = _fake_execution(status="RUNNING")
    _configure_session(session, execution)
    added = []
    session.add.side_effect = added.append

    with patch.object(service, "_get_session_and_repo", return_value=cm):
        result = service.complete_task("task-1", result={"ok": True})

    assert result is not None
    assert result.id == "task-1"
    assert execution.status == "FINISHED"
    assert '"ok": true' in execution.json_result
    assert execution.finish_ts != 0
    assert execution.duration >= 0
    state_records = [o for o in added if isinstance(o, TrendzTaskExecutionStateRecord)]
    assert len(state_records) == 1
    assert state_records[0].state == "WORKING"


@pytest.mark.unit
def test_complete_task_not_found(service, mock_session_repo):
    session, _repo, cm = mock_session_repo
    res = MagicMock()
    res.scalars.return_value.first.return_value = None
    session.execute.return_value = res

    with patch.object(service, "_get_session_and_repo", return_value=cm):
        result = service.complete_task("task-nonexistent")

    assert result is None


@pytest.mark.unit
def test_complete_task_idempotent_when_terminal(service, mock_session_repo):
    session, _repo, cm = mock_session_repo
    execution = _fake_execution(status="FINISHED")
    _configure_session(session, execution)

    with patch.object(service, "_get_session_and_repo", return_value=cm):
        result = service.complete_task("task-1", result={"again": True})

    assert result is not None
    # idempotent: stays FINISHED, result not overwritten
    assert execution.status == "FINISHED"
    assert execution.json_result == "{}"


@pytest.mark.unit
def test_fail_task(service, mock_session_repo):
    session, _repo, cm = mock_session_repo
    execution = _fake_execution(status="RUNNING")
    _configure_session(session, execution)
    added = []
    session.add.side_effect = added.append

    with patch.object(service, "_get_session_and_repo", return_value=cm):
        result = service.fail_task("task-1", error_message="Something broke")

    assert result is not None
    assert result.id == "task-1"
    assert execution.status == "FAILED"
    assert "Something broke" in execution.json_result
    assert execution.finish_ts != 0
    state_records = [o for o in added if isinstance(o, TrendzTaskExecutionStateRecord)]
    assert len(state_records) == 1
    assert state_records[0].state == "WORKING"
