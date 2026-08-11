from __future__ import annotations

import uuid
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy.dialects import postgresql
from trendx.config import settings
from trendx.database.models import TrendzTaskExecutionRequest
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
    retried = MagicMock(id="task-1", enabled=False)
    repo.get.return_value = retried

    with patch.object(service, "_get_session_and_repo", return_value=cm):
        result = service.retry_task("task-1")

    assert result is not None
    assert result.enabled is True
    session.commit.assert_called_once()


@pytest.mark.unit
def test_retry_task_not_found(service, mock_session_repo):
    _, repo, cm = mock_session_repo
    repo.get.return_value = None

    with patch.object(service, "_get_session_and_repo", return_value=cm):
        result = service.retry_task("task-nonexistent")

    assert result is None
    repo.get.assert_called_once_with("task-nonexistent")


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


# ── update_progress — STUB (returns None) ───────────────────────────────


@pytest.mark.unit
@pytest.mark.xfail(strict=True, reason="update_progress is a stub returning None — " + _STUB_REASON)
def test_update_progress(service, mock_session_repo):
    session, repo, cm = mock_session_repo
    existing = MagicMock(id="task-1", payload={"progress": 0})
    repo.get.return_value = existing
    repo.update.return_value = existing

    with patch.object(service, "_get_session_and_repo", return_value=cm):
        result = service.update_progress("task-1", 50)

    assert result is not None


@pytest.mark.unit
@pytest.mark.xfail(
    strict=True, reason="update_progress is a stub — no progress validation — " + _STUB_REASON
)
def test_update_progress_invalid(service):
    with pytest.raises(ValueError, match="Progress must be between"):
        service.update_progress("task-1", -1)


# ── complete_task — STUB (returns None, signature mismatch) ────────────


@pytest.mark.unit
@pytest.mark.xfail(strict=True, reason="complete_task is a stub returning None — " + _STUB_REASON)
def test_complete_task(service, mock_session_repo):
    _, repo, cm = mock_session_repo
    completed = MagicMock(id="task-1")
    repo.complete.return_value = completed

    with patch.object(service, "_get_session_and_repo", return_value=cm):
        result = service.complete_task("task-1", result={"ok": True})

    assert result is not None
    assert result.id == "task-1"
    repo.complete.assert_called_once()


@pytest.mark.unit
@pytest.mark.skip(reason="complete_task is a stub returning None — " + _STUB_REASON)
def test_complete_task_not_found(service, mock_session_repo):
    _, repo, cm = mock_session_repo
    repo.complete.return_value = None

    with patch.object(service, "_get_session_and_repo", return_value=cm):
        result = service.complete_task("task-nonexistent")

    assert result is None


# ── fail_task — STUB (returns None, signature mismatch) ─────────────────


@pytest.mark.unit
@pytest.mark.xfail(strict=True, reason="fail_task is a stub returning None — " + _STUB_REASON)
def test_fail_task(service, mock_session_repo):
    _, repo, cm = mock_session_repo
    failed = MagicMock(id="task-1")
    repo.complete.return_value = failed

    with patch.object(service, "_get_session_and_repo", return_value=cm):
        result = service.fail_task("task-1", error_message="Something broke")

    assert result is not None
    assert result.id == "task-1"
