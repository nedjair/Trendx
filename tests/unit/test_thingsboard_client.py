from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, Mock, PropertyMock, patch

import httpx
import pytest
from faker import Faker

from trendx.thingsboard.client import (
    Device,
    EntityId,
    LoginResponse,
    PageData,
    ThingsBoardAuthError,
    ThingsBoardClient,
    ThingsBoardError,
    ThingsBoardRateLimitError,
    ThingsBoardWriteDisabledError,
    TimeseriesEntry,
)

fake = Faker()


@pytest.fixture
def mock_client():
    client = ThingsBoardClient(
        base_url="http://test.tb:8080",
        username="test@test.com",
        password="test_pass",
        request_timeout=10,
        retry_max_attempts=1,
        retry_backoff_seconds=1,
        jwt_leeway_seconds=300,
        page_size=10,
    )
    client._token = "test_token"
    client._refresh_token = "test_refresh"
    client._token_expiry = datetime.now(timezone.utc) + timedelta(hours=1)
    return client


@pytest.mark.unit
@pytest.mark.asyncio
async def test_login_success(mock_client):
    login_data = LoginResponse(token="jwt_token_abc", refreshToken="refresh_xyz")
    mock_resp = Mock(spec=httpx.Response)
    mock_resp.status_code = 200
    mock_resp.json.return_value = login_data.model_dump()

    with patch.object(mock_client._http_client, "post", new=AsyncMock(return_value=mock_resp)):
        result = await mock_client.login()

    assert result.token == "jwt_token_abc"
    assert result.refreshToken == "refresh_xyz"
    assert mock_client._token == "jwt_token_abc"
    assert mock_client.is_authenticated


@pytest.mark.unit
@pytest.mark.asyncio
async def test_login_failure(mock_client):
    mock_resp = Mock(spec=httpx.Response)
    mock_resp.status_code = 401
    mock_resp.json.return_value = {"message": "Invalid credentials"}

    with patch.object(mock_client._http_client, "post", new=AsyncMock(return_value=mock_resp)):
        with pytest.raises(ThingsBoardAuthError) as exc_info:
            await mock_client.login()

    assert exc_info.value.status == 401


@pytest.mark.unit
@pytest.mark.asyncio
async def test_token_renewal(mock_client):
    mock_client._token_expiry = datetime.now(timezone.utc) - timedelta(minutes=5)
    mock_client._token = None

    login_data = LoginResponse(token="new_jwt", refreshToken="new_refresh")
    mock_login_resp = Mock(spec=httpx.Response)
    mock_login_resp.status_code = 200
    mock_login_resp.json.return_value = login_data.model_dump()

    mock_data_resp = Mock(spec=httpx.Response)
    mock_data_resp.status_code = 200
    mock_data_resp.json.return_value = {"data": [], "totalPages": 0, "totalElements": 0, "hasNext": False}

    with (
        patch.object(mock_client._http_client, "post", new=AsyncMock(return_value=mock_login_resp)),
        patch.object(mock_client._http_client, "request", new=AsyncMock(return_value=mock_data_resp)),
    ):
        result = await mock_client.get_devices()

    assert mock_client._token == "new_jwt"
    assert isinstance(result, PageData)


@pytest.mark.unit
@pytest.mark.asyncio
async def test_get_devices_paginated(mock_client):
    device_data = {
        "data": [
            {
                "id": {"entityType": "DEVICE", "id": fake.uuid4()},
                "name": fake.word(),
                "type": "default",
                "label": "",
            }
            for _ in range(3)
        ],
        "totalPages": 1,
        "totalElements": 3,
        "hasNext": False,
    }
    mock_resp = Mock(spec=httpx.Response)
    mock_resp.status_code = 200
    mock_resp.json.return_value = device_data

    with patch.object(mock_client._http_client, "request", new=AsyncMock(return_value=mock_resp)):
        result = await mock_client.get_devices(page=0, page_size=10)

    assert result.totalElements == 3
    assert len(result.data) == 3
    assert all(isinstance(d, Device) for d in result.data)


@pytest.mark.unit
@pytest.mark.asyncio
async def test_get_timeseries(mock_client):
    ts_data = {
        "temperature": [
            {"ts": 1700000000000, "value": 22.5},
            {"ts": 1700000001000, "value": 23.0},
        ]
    }
    mock_resp = Mock(spec=httpx.Response)
    mock_resp.status_code = 200
    mock_resp.json.return_value = ts_data

    with patch.object(mock_client._http_client, "request", new=AsyncMock(return_value=mock_resp)):
        result = await mock_client.get_timeseries("DEVICE", "dev-123", keys=["temperature"])

    assert "temperature" in result
    assert len(result["temperature"]) == 2
    assert all(isinstance(e, TimeseriesEntry) for e in result["temperature"])
    assert result["temperature"][0].value == 22.5


@pytest.mark.unit
@pytest.mark.asyncio
async def test_retry_on_401(mock_client):
    resp_401 = Mock(spec=httpx.Response)
    resp_401.status_code = 401
    resp_401.json.return_value = {"message": "Unauthorized"}

    resp_200 = Mock(spec=httpx.Response)
    resp_200.status_code = 200
    resp_200.json.return_value = {"data": [], "totalPages": 0, "totalElements": 0, "hasNext": False}

    login_resp = Mock(spec=httpx.Response)
    login_resp.status_code = 200
    login_resp.json.return_value = {"token": "renewed", "refreshToken": "ref"}

    with (
        patch.object(mock_client._http_client, "request", new=AsyncMock(side_effect=[resp_401, resp_200])),
        patch.object(mock_client._http_client, "post", new=AsyncMock(return_value=login_resp)),
    ):
        result = await mock_client.get_devices()

    assert isinstance(result, PageData)
    assert mock_client._token == "renewed"


@pytest.mark.unit
@pytest.mark.asyncio
async def test_retry_on_429(mock_client):
    resp_429 = Mock(spec=httpx.Response)
    resp_429.status_code = 429
    resp_429.json.return_value = {"message": "Too many requests"}

    resp_200 = Mock(spec=httpx.Response)
    resp_200.status_code = 200
    resp_200.json.return_value = {"data": [], "totalPages": 0, "totalElements": 0, "hasNext": False}

    with patch.object(mock_client._http_client, "request", new=AsyncMock(side_effect=[resp_429, resp_200])):
        result = await mock_client.get_devices()

    assert isinstance(result, PageData)


@pytest.mark.unit
@pytest.mark.asyncio
async def test_context_manager(mock_client):
    mock_client._client = AsyncMock()
    aclose_mock = AsyncMock()
    mock_client._client.aclose = aclose_mock

    async with mock_client as cm:
        assert cm is mock_client

    aclose_mock.assert_awaited_once()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_whitelist_allows_get(mock_client):
    mock_resp = Mock(spec=httpx.Response)
    mock_resp.status_code = 200
    mock_resp.json.return_value = {"data": [], "totalPages": 0, "totalElements": 0, "hasNext": False}

    with patch.object(mock_client._http_client, "request", new=AsyncMock(return_value=mock_resp)):
        result = await mock_client.get_devices()

    assert isinstance(result, PageData)


@pytest.mark.unit
@pytest.mark.asyncio
async def test_whitelist_allows_login_post(mock_client):
    login_data = LoginResponse(token="jwt_token_abc", refreshToken="refresh_xyz")
    mock_resp = Mock(spec=httpx.Response)
    mock_resp.status_code = 200
    mock_resp.json.return_value = login_data.model_dump()

    with patch.object(mock_client._http_client, "post", new=AsyncMock(return_value=mock_resp)):
        result = await mock_client.login()

    assert result.token == "jwt_token_abc"


@pytest.mark.unit
@pytest.mark.asyncio
async def test_whitelist_blocks_telemetry_post(mock_client):
    # post_telemetry short-circuits on tb_writeback_enabled=False; test the whitelist at _request level
    with pytest.raises(ThingsBoardWriteDisabledError, match="Whitelist violation"):
        await mock_client._request(
            "POST",
            "/api/plugins/telemetry/DEVICE/dev-001/timeseries/ANY",
            json_data=[{"ts": 1700000000000, "value": 22.5}],
        )


@pytest.mark.unit
@pytest.mark.asyncio
async def test_whitelist_blocks_put(mock_client):
    with pytest.raises(ThingsBoardWriteDisabledError, match="Whitelist violation"):
        await mock_client._request("PUT", "/api/alarm/alarm-id/ack")


@pytest.mark.unit
@pytest.mark.asyncio
async def test_whitelist_blocks_delete(mock_client):
    with pytest.raises(ThingsBoardWriteDisabledError, match="Whitelist violation"):
        await mock_client._request("DELETE", "/api/device/dev-id")
