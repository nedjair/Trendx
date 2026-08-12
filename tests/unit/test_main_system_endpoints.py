"""Endpoints système exposant la configuration.

Régression Vague 0.1 : GET / et GET /api/v1/admin/config lisaient
settings.grafana_host_port, champ absent de Settings => AttributeError => 500.
Aucun test ne couvrait ces deux routes, le défaut passait donc inaperçu.
"""

from __future__ import annotations

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
def test_root_exposes_stack_services(client):
    resp = client.get("/")
    assert resp.status_code == 200
    services = resp.json()["stack_services"]
    assert services["grafana"].endswith(f":{settings.grafana_host_port}")


@pytest.mark.unit
def test_admin_config_returns_full_payload(client):
    resp = client.get("/api/v1/admin/config", headers=_auth_headers())
    assert resp.status_code == 200
    body = resp.json()
    assert body["grafana_host_port"] == settings.grafana_host_port
    assert body["trendx_api_port"] == settings.trendx_api_port


@pytest.mark.unit
def test_admin_config_requires_authentication(client):
    assert client.get("/api/v1/admin/config").status_code == 401
