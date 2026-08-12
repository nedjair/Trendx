"""Wave 3 — API boundary tests for task endpoints + UUID coercion.

Regression scope (strict):
- Defect B: POST /api/v1/tasks/{task_id}/retry (and get/cancel) must coerce the
  string path param to uuid.UUID via the canonical `_uuid()` helper instead of
  handing a raw str to `repo.get` (which binds a UUID(as_uuid=True) column and
  raised ``'str' object has no attribute 'hex'`` => HTTP 500).
- An invalid UUID string must yield HTTP 400 (validation error), not HTTP 500.
- A valid UUID must reach the DB path without the `.hex` AttributeError.

The catalog DB is NOT touched: a self-contained in-memory SQLite engine is
injected by patching `db_manager.get_session` for the "catalog" key. A fresh
session is created inside the request thread to avoid SQLite cross-thread use.
"""

from __future__ import annotations

import uuid
from contextlib import contextmanager
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool
from trendx.config import settings
from trendx.database.models import (
    Base,
    TrendzTask,
    TrendzTaskExecution,
    TrendzTaskExecutionRequest,
    TrendzTaskExecutionStateRecord,
)
from trendx.main import app


def _make_engine():
    engine = create_engine(
        "sqlite://",
        poolclass=StaticPool,
        connect_args={"check_same_thread": False},
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
    return engine


def _seed_task(engine, task_id: uuid.UUID) -> None:
    tenant = uuid.uuid4()
    customer = uuid.uuid4()
    user = uuid.uuid4()
    with sessionmaker(bind=engine)() as session:
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
        session.add(task)
        session.commit()


def _count_requests(engine, task_id: uuid.UUID) -> list:
    with sessionmaker(bind=engine)() as session:
        return session.query(TrendzTaskExecutionRequest).filter_by(task_id=task_id).all()


@pytest.fixture
def client():
    with TestClient(app) as c:
        yield c


def _auth_headers() -> dict[str, str]:
    token = settings.trendx_api_token.get_secret_value()
    return {"Authorization": f"Bearer {token}"}


@pytest.mark.unit
def test_retry_valid_uuid_creates_pending_request_no_hex_error(client):
    engine = _make_engine()
    task_id = uuid.uuid4()
    _seed_task(engine, task_id)

    def _fake_get_session(key: str):
        @contextmanager
        def _cm():
            yield sessionmaker(bind=engine)()

        return _cm()

    with patch("trendx.main.db_manager.get_session", side_effect=_fake_get_session):
        resp = client.post(f"/api/v1/tasks/{task_id}/retry", headers=_auth_headers())

    assert resp.status_code == 200, resp.text
    requests = _count_requests(engine, task_id)
    assert len(requests) == 1
    assert requests[0].state == "PENDING"
    assert requests[0].scheduled is False
    assert requests[0].execution_id != task_id


@pytest.mark.unit
def test_retry_invalid_uuid_returns_400(client):
    engine = _make_engine()

    def _fake_get_session(key: str):
        @contextmanager
        def _cm():
            yield sessionmaker(bind=engine)()

        return _cm()

    with patch("trendx.main.db_manager.get_session", side_effect=_fake_get_session):
        resp = client.post("/api/v1/tasks/not-a-uuid/retry", headers=_auth_headers())
    assert resp.status_code == 400


@pytest.mark.unit
def test_get_task_valid_uuid_uses_coercion(client):
    engine = _make_engine()
    task_id = uuid.uuid4()
    _seed_task(engine, task_id)

    def _fake_get_session(key: str):
        @contextmanager
        def _cm():
            yield sessionmaker(bind=engine)()

        return _cm()

    with patch("trendx.main.db_manager.get_session", side_effect=_fake_get_session):
        resp = client.get(f"/api/v1/tasks/{task_id}", headers=_auth_headers())
    assert resp.status_code == 200, resp.text
    assert resp.json()["id"] == str(task_id)


@pytest.mark.unit
def test_cancel_task_valid_uuid_uses_coercion(client):
    engine = _make_engine()
    task_id = uuid.uuid4()
    _seed_task(engine, task_id)

    def _fake_get_session(key: str):
        @contextmanager
        def _cm():
            yield sessionmaker(bind=engine)()

        return _cm()

    with patch("trendx.main.db_manager.get_session", side_effect=_fake_get_session):
        resp = client.post(f"/api/v1/tasks/{task_id}/cancel", headers=_auth_headers())
    assert resp.status_code == 200, resp.text
    assert resp.json()["enabled"] is False
