from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from trendx.services.tasks import TaskService


@pytest.fixture
def service():
    svc = TaskService()
    return svc


@pytest.fixture
def mock_session_repo():
    session = MagicMock()
    session.close = MagicMock()
    repo = MagicMock()
    return session, repo


@pytest.mark.unit
def test_create_task(service, mock_session_repo):
    session, repo = mock_session_repo
    repo.create.return_value = MagicMock(id="task-1", name="test-task")

    with (
        patch.object(service, "_get_session_and_repo", return_value=(session, repo)),
    ):
        task = service.create_task(
            task_type="forecast",
            payload={"device_id": "dev-001"},
            priority=5,
        )

    assert task.id == "task-1"
    repo.create.assert_called_once()


@pytest.mark.unit
def test_claim_task(service, mock_session_repo):
    session, repo = mock_session_repo
    pending_task = MagicMock(id="task-1")
    repo.find_pending.return_value = [pending_task]
    repo.claim.return_value = pending_task

    with (
        patch.object(service, "_get_session_and_repo", return_value=(session, repo)),
    ):
        claimed = service.claim_task(worker_id="worker-1")

    assert claimed is not None
    assert claimed.id == "task-1"
    repo.claim.assert_called_with("task-1", "worker-1")


@pytest.mark.unit
def test_claim_task_no_pending(service, mock_session_repo):
    session, repo = mock_session_repo
    repo.find_pending.return_value = []

    with (
        patch.object(service, "_get_session_and_repo", return_value=(session, repo)),
    ):
        claimed = service.claim_task(worker_id="worker-1")

    assert claimed is None


@pytest.mark.unit
def test_update_progress(service, mock_session_repo):
    session, repo = mock_session_repo
    existing = MagicMock(id="task-1", payload={"progress": 0})
    repo.get.return_value = existing
    repo.update.return_value = existing

    with (
        patch.object(service, "_get_session_and_repo", return_value=(session, repo)),
    ):
        result = service.update_progress("task-1", 50)

    assert result is not None


@pytest.mark.unit
def test_update_progress_invalid(service):
    with pytest.raises(ValueError, match="Progress must be between"):
        service.update_progress("task-1", -1)


@pytest.mark.unit
def test_complete_task(service, mock_session_repo):
    session, repo = mock_session_repo
    completed = MagicMock(id="task-1", status="COMPLETED")
    repo.complete.return_value = completed

    with (
        patch.object(service, "_get_session_and_repo", return_value=(session, repo)),
    ):
        result = service.complete_task("task-1", result={"ok": True})

    assert result is not None
    assert result.status == "COMPLETED"


@pytest.mark.unit
def test_complete_task_not_found(service, mock_session_repo):
    session, repo = mock_session_repo
    repo.complete.return_value = None

    with (
        patch.object(service, "_get_session_and_repo", return_value=(session, repo)),
    ):
        result = service.complete_task("task-nonexistent")

    assert result is None


@pytest.mark.unit
def test_fail_task(service, mock_session_repo):
    session, repo = mock_session_repo
    failed = MagicMock(id="task-1", status="FAILED")
    repo.complete.return_value = failed

    with (
        patch.object(service, "_get_session_and_repo", return_value=(session, repo)),
    ):
        result = service.fail_task("task-1", error_message="Something broke")

    assert result is not None


@pytest.mark.unit
def test_cancel_task(service, mock_session_repo):
    session, repo = mock_session_repo
    running = MagicMock(id="task-1", status="RUNNING", finished_at=None)
    repo.get.return_value = running

    with (
        patch.object(service, "_get_session_and_repo", return_value=(session, repo)),
    ):
        result = service.cancel_task("task-1")

    assert result is not None
    assert result.status == "CANCELLED"


@pytest.mark.unit
def test_cancel_task_not_found(service, mock_session_repo):
    session, repo = mock_session_repo
    repo.get.return_value = None

    with (
        patch.object(service, "_get_session_and_repo", return_value=(session, repo)),
    ):
        result = service.cancel_task("task-nonexistent")

    assert result is None


@pytest.mark.unit
def test_retry_task(service, mock_session_repo):
    session, repo = mock_session_repo
    retried = MagicMock(id="task-1", status="PENDING", retry_count=1)
    repo.mark_retry.return_value = retried

    with (
        patch.object(service, "_get_session_and_repo", return_value=(session, repo)),
    ):
        result = service.retry_task("task-1")

    assert result is not None
    repo.mark_retry.assert_called_with("task-1")


@pytest.mark.unit
def test_retry_task_not_found(service, mock_session_repo):
    session, repo = mock_session_repo
    repo.mark_retry.return_value = None

    with (
        patch.object(service, "_get_session_and_repo", return_value=(session, repo)),
    ):
        result = service.retry_task("task-nonexistent")

    assert result is None
