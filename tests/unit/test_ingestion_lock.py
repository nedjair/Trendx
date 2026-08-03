from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from trendx.config import settings
from trendx.main import app


@pytest.fixture
def client():
    with TestClient(app) as c:
        yield c


def _auth_headers() -> dict[str, str]:
    """Jeton lu depuis settings (jamais de valeur codée en dur dans le test)."""
    token = settings.trendx_api_token.get_secret_value()
    return {"Authorization": f"Bearer {token}"}


@pytest.mark.unit
def test_trigger_refused_when_ingest_lock_closed(client, monkeypatch):
    """Verrou fermé (TRENDX_INGEST_ENABLED=false) => 409, aucune tâche créée."""
    monkeypatch.setattr(settings, "trendx_ingest_enabled", False)
    monkeypatch.setattr(settings, "tb_auth_configured", True)
    with patch("trendx.main.TaskService") as mock_svc:
        resp = client.post("/api/v1/ingestion/trigger", headers=_auth_headers())
    assert resp.status_code == 409
    assert "TRENDX_INGEST_ENABLED" in resp.json()["detail"]
    mock_svc.assert_not_called()


@pytest.mark.unit
def test_trigger_refused_when_tb_auth_not_configured(client, monkeypatch):
    """Verrou ingestion ouvert mais TB_AUTH_CONFIGURED!=true => 409."""
    monkeypatch.setattr(settings, "trendx_ingest_enabled", True)
    monkeypatch.setattr(settings, "tb_auth_configured", False)
    with patch("trendx.main.TaskService") as mock_svc:
        resp = client.post("/api/v1/ingestion/trigger", headers=_auth_headers())
    assert resp.status_code == 409
    assert "TB_AUTH_CONFIGURED" in resp.json()["detail"]
    mock_svc.assert_not_called()


@pytest.mark.unit
def test_trigger_refused_when_both_lock_and_auth_absent(client, monkeypatch):
    """Absence des deux flags (valeurs par défaut) => 409 sur le premier verrou."""
    monkeypatch.setattr(settings, "trendx_ingest_enabled", False)
    monkeypatch.setattr(settings, "tb_auth_configured", False)
    with patch("trendx.main.TaskService") as mock_svc:
        resp = client.post("/api/v1/ingestion/trigger", headers=_auth_headers())
    assert resp.status_code == 409
    mock_svc.assert_not_called()


@pytest.mark.unit
def test_trigger_allowed_only_when_lock_open_and_auth_configured(client, monkeypatch):
    """Les deux verrous ouverts => 202 et tâche planifiée (TaskService mocké)."""
    monkeypatch.setattr(settings, "trendx_ingest_enabled", True)
    monkeypatch.setattr(settings, "tb_auth_configured", True)
    fake_task = MagicMock(id="task-abc")
    with patch("trendx.main.TaskService") as mock_svc:
        mock_svc.return_value.create_task.return_value = fake_task
        resp = client.post("/api/v1/ingestion/trigger", headers=_auth_headers())
    assert resp.status_code == 202
    body = resp.json()
    assert body["status"] == "triggered"
    assert body["task_id"] == "task-abc"
    mock_svc.return_value.create_task.assert_called_once()
