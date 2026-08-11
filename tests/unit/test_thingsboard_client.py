from __future__ import annotations

from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, Mock, patch

import httpx
import pytest
from faker import Faker
from trendx.thingsboard.client import (
    Asset,
    AttributeEntry,
    Device,
    LoginResponse,
    PageData,
    ThingsBoardAuthError,
    ThingsBoardClient,
    ThingsBoardError,
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
    client._token_expiry = datetime.now(UTC) + timedelta(hours=1)
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
    mock_client._token_expiry = datetime.now(UTC) - timedelta(minutes=5)
    mock_client._token = None

    login_data = LoginResponse(token="new_jwt", refreshToken="new_refresh")
    mock_login_resp = Mock(spec=httpx.Response)
    mock_login_resp.status_code = 200
    mock_login_resp.json.return_value = login_data.model_dump()

    mock_data_resp = Mock(spec=httpx.Response)
    mock_data_resp.status_code = 200
    mock_data_resp.json.return_value = {
        "data": [],
        "totalPages": 0,
        "totalElements": 0,
        "hasNext": False,
    }

    with (
        patch.object(mock_client._http_client, "post", new=AsyncMock(return_value=mock_login_resp)),
        patch.object(
            mock_client._http_client, "request", new=AsyncMock(return_value=mock_data_resp)
        ),
    ):
        result = await mock_client.get_devices()

    assert mock_client._token == "new_jwt"
    assert isinstance(result, PageData)


@pytest.mark.unit
def test_device_label_null_normalized_to_empty_string():
    """ThingsBoard returns null labels; the internal contract requires str (bug #FAILED+WORKING)."""
    dev = Device.model_validate(
        {"id": {"entityType": "DEVICE", "id": fake.uuid4()}, "name": "d1", "label": None}
    )
    assert dev.label == ""
    assert isinstance(dev.label, str)


@pytest.mark.unit
def test_device_label_missing_defaults_to_empty_string():
    dev = Device.model_validate({"id": {"entityType": "DEVICE", "id": fake.uuid4()}, "name": "d1"})
    assert dev.label == ""


@pytest.mark.unit
def test_device_label_present_preserved():
    dev = Device.model_validate(
        {"id": {"entityType": "DEVICE", "id": fake.uuid4()}, "name": "d1", "label": "sensor-1"}
    )
    assert dev.label == "sensor-1"


@pytest.mark.unit
def test_asset_label_null_normalized_to_empty_string():
    """Asset labels are null in the wild TB data as well."""
    asset = Asset.model_validate(
        {"id": {"entityType": "ASSET", "id": fake.uuid4()}, "name": "a1", "label": None}
    )
    assert asset.label == ""
    assert isinstance(asset.label, str)


@pytest.mark.unit
def test_asset_label_present_preserved():
    asset = Asset.model_validate(
        {"id": {"entityType": "ASSET", "id": fake.uuid4()}, "name": "a1", "label": "zone-1"}
    )
    assert asset.label == "zone-1"


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
        patch.object(
            mock_client._http_client, "request", new=AsyncMock(side_effect=[resp_401, resp_200])
        ),
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

    with patch.object(
        mock_client._http_client, "request", new=AsyncMock(side_effect=[resp_429, resp_200])
    ):
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
    mock_resp.json.return_value = {
        "data": [],
        "totalPages": 0,
        "totalElements": 0,
        "hasNext": False,
    }

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


@pytest.mark.unit
@pytest.mark.asyncio
async def test_get_attributes_parses_tb_object_payload(mock_client):
    """TB returns an object keyed by attribute name; iterate over values().

    Regression guard for the attribute-mapping defect (run failed with
    'Input should be a valid dictionary or instance of AttributeEntry,
    input_value='message'' because code iterated over the dict keys).
    """
    attr_data = {
        "message": {"value": "hello", "lastUpdateTs": 123},
        "foo": {"value": "bar", "lastUpdateTs": 456},
    }
    mock_resp = Mock(spec=httpx.Response)
    mock_resp.status_code = 200
    mock_resp.json.return_value = attr_data

    with patch.object(mock_client._http_client, "request", new=AsyncMock(return_value=mock_resp)):
        result = await mock_client.get_attributes("DEVICE", "dev-1", scope="SERVER_SCOPE")

    assert len(result) == 2
    assert all(isinstance(e, AttributeEntry) for e in result)
    # TB object payload: the key is the dict key, injected into each entry.
    assert {e.key for e in result} == {"message", "foo"}
    by_key = {e.key: e for e in result}
    assert by_key["message"].value == "hello"
    assert by_key["message"].lastUpdateTs == 123
    assert by_key["foo"].lastUpdateTs == 456


@pytest.mark.unit
@pytest.mark.asyncio
async def test_get_attributes_handles_unexpected_scalar_gracefully(mock_client):
    """A non-dict/non-object value must raise a clear error, not a cryptic one."""
    attr_data = {"message": "plain-string-instead-of-object"}
    mock_resp = Mock(spec=httpx.Response)
    mock_resp.status_code = 200
    mock_resp.json.return_value = attr_data

    with patch.object(mock_client._http_client, "request", new=AsyncMock(return_value=mock_resp)):
        with pytest.raises(ThingsBoardError):
            await mock_client.get_attributes("DEVICE", "dev-1", scope="SERVER_SCOPE")


@pytest.mark.unit
@pytest.mark.asyncio
async def test_get_attributes_handles_list_payload(mock_client):
    """A list-shaped response (unexpected but tolerated) is validated per item."""
    attr_data = [
        {"key": "message", "value": "hello", "lastUpdateTs": 123},
        {"key": "foo", "value": "bar", "lastUpdateTs": 456},
    ]
    mock_resp = Mock(spec=httpx.Response)
    mock_resp.status_code = 200
    mock_resp.json.return_value = attr_data

    with patch.object(mock_client._http_client, "request", new=AsyncMock(return_value=mock_resp)):
        result = await mock_client.get_attributes("DEVICE", "dev-1", scope="SERVER_SCOPE")

    assert len(result) == 2
    assert {e.key for e in result} == {"message", "foo"}
