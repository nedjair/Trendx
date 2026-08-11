from __future__ import annotations

import logging
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import Any, Self, cast

import httpx
from loguru import logger
from pydantic import BaseModel, ConfigDict, Field, field_validator
from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)
from trendx.config import settings


class LoginRequest(BaseModel):
    username: str
    password: str


class LoginResponse(BaseModel):
    token: str
    refreshToken: str  # noqa: N815


class EntityId(BaseModel):
    entityType: str  # noqa: N815
    id: str


def _normalize_label(value: Any) -> str:
    """ThingsBoard may return a null label; the internal contract requires str."""
    return "" if value is None else cast("str", value)


class Device(BaseModel):
    id: EntityId | None = None
    name: str = ""
    type: str = ""
    label: str = ""
    tenantId: EntityId | None = None  # noqa: N815
    customerId: EntityId | None = None  # noqa: N815
    deviceProfileId: EntityId | None = None  # noqa: N815
    additionalInfo: dict[str, Any] | None = None  # noqa: N815

    @field_validator("label", mode="before")
    @classmethod
    def _coerce_label(cls, value: Any) -> str:
        return _normalize_label(value)


class DeviceProfile(BaseModel):
    id: EntityId | None = None
    name: str = ""
    type: str = ""
    description: str = ""
    default: bool = False
    transportType: str = "DEFAULT"  # noqa: N815


class Asset(BaseModel):
    id: EntityId | None = None
    name: str = ""
    type: str = ""
    label: str = ""
    tenantId: EntityId | None = None  # noqa: N815
    customerId: EntityId | None = None  # noqa: N815
    additionalInfo: dict[str, Any] | None = None  # noqa: N815

    @field_validator("label", mode="before")
    @classmethod
    def _coerce_label(cls, value: Any) -> str:
        return _normalize_label(value)


class Relation(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    from_: EntityId = Field(alias="from")
    to: EntityId
    type: str = ""
    typeGroup: str = "COMMON"  # noqa: N815
    additionalInfo: dict[str, Any] | None = None  # noqa: N815


class Tenant(BaseModel):
    id: EntityId | None = None
    title: str = ""
    email: str = ""
    region: str = ""
    additionalInfo: dict[str, Any] | None = None  # noqa: N815


class Customer(BaseModel):
    id: EntityId | None = None
    title: str = ""
    email: str = ""
    tenantId: EntityId | None = None  # noqa: N815
    additionalInfo: dict[str, Any] | None = None  # noqa: N815


class PageData(BaseModel):
    data: list[Any] = Field(default_factory=list)
    totalPages: int = 0  # noqa: N815
    totalElements: int = 0  # noqa: N815
    hasNext: bool = False  # noqa: N815


class TimeseriesEntry(BaseModel):
    ts: int
    value: Any


class AttributeEntry(BaseModel):
    key: str
    value: Any
    lastUpdateTs: int = 0  # noqa: N815


class AlarmData(BaseModel):
    id: EntityId | None = None
    type: str = ""
    originator: EntityId
    severity: str = "CRITICAL"
    status: str = "ACTIVE_UNACK"
    startTs: int = 0  # noqa: N815
    endTs: int = 0  # noqa: N815
    ackTs: int = 0  # noqa: N815
    clearTs: int = 0  # noqa: N815
    details: dict[str, Any] = Field(default_factory=dict)
    propagate: bool = True
    propagateRelationTypes: list[str] = Field(default_factory=list)  # noqa: N815


class ThingsBoardError(Exception):
    def __init__(self, status: int, body: Any = None) -> None:
        self.status = status
        self.body = body
        body_str = str(body) if body is not None else "None"
        # Échapper les accolades pour éviter les problèmes de format string
        # avec loguru / tenacity quand le body contient des dicts.
        safe_body = body_str.replace("{", "{{").replace("}", "}}")
        super().__init__(f"ThingsBoard API error {status}: {safe_body}")


class ThingsBoardAuthError(ThingsBoardError):
    pass


class ThingsBoardNotFoundError(ThingsBoardError):
    pass


class ThingsBoardConflictError(ThingsBoardError):
    pass


class ThingsBoardRateLimitError(ThingsBoardError):
    pass


class ThingsBoardWriteDisabledError(ThingsBoardError):
    """Ecriture ThingsBoard bloquée par la whitelist client."""


def _raise_on_status(status: int, body: Any = None) -> None:
    if status == 401:
        raise ThingsBoardAuthError(status, body)
    if status == 403:
        raise ThingsBoardError(status, body)
    if status == 404:
        raise ThingsBoardNotFoundError(status, body)
    if status == 409:
        raise ThingsBoardConflictError(status, body)
    if status == 429:
        raise ThingsBoardRateLimitError(status, body)
    if 500 <= status < 600:
        raise ThingsBoardError(status, body)


def _sanitize_url(url: str) -> str:
    return url


class ThingsBoardClient:
    def __init__(
        self,
        base_url: str | None = None,
        username: str | None = None,
        password: str | None = None,
        request_timeout: int | None = None,
        retry_max_attempts: int | None = None,
        retry_backoff_seconds: int | None = None,
        jwt_leeway_seconds: int | None = None,
        page_size: int | None = None,
        *,
        verify_ssl: bool = False,
    ) -> None:
        self._base_url = (base_url or settings.tb_base_url).rstrip("/")
        self._username = username or settings.tb_username
        self._password = password or settings.tb_password.get_secret_value()
        self._request_timeout = request_timeout or settings.tb_request_timeout_seconds
        self._retry_max_attempts = retry_max_attempts or settings.tb_retry_max_attempts
        self._retry_backoff_seconds = retry_backoff_seconds or settings.tb_retry_backoff_seconds
        self._jwt_leeway_seconds = jwt_leeway_seconds or settings.tb_jwt_leeway_seconds
        self._page_size = page_size or settings.tb_page_size
        self._verify_ssl = verify_ssl

        self._token: str | None = None
        self._refresh_token: str | None = None
        self._token_expiry: datetime | None = None
        self._client: httpx.AsyncClient | None = None
        self._auth_retry_count: int = 0

    @property
    def is_authenticated(self) -> bool:
        if self._token is None or self._token_expiry is None:
            return False
        return datetime.now(UTC) < self._token_expiry

    @property
    def token_expiry(self) -> datetime | None:
        return self._token_expiry

    @property
    def _http_client(self) -> httpx.AsyncClient:
        if self._client is None:
            self._client = httpx.AsyncClient(
                base_url=self._base_url,
                timeout=httpx.Timeout(self._request_timeout),
                verify=self._verify_ssl,
                headers={"Content-Type": "application/json"},
            )
        return self._client

    async def _ensure_auth_header(self) -> dict[str, str]:
        if not self.is_authenticated:
            await self.login()
        return {"X-Authorization": f"Bearer {self._token}"}

    def _check_token_expiry(self, token: str) -> datetime:
        try:
            import base64
            import json

            parts = token.split(".")
            if len(parts) >= 2:
                padded = parts[1] + "=" * (4 - len(parts[1]) % 4)
                payload = json.loads(base64.urlsafe_b64decode(padded))
                exp = payload.get("exp", 0)
                return datetime.fromtimestamp(exp, tz=UTC)
        except Exception:
            logger.warning("Could not decode JWT to extract expiry")
        return datetime.now(UTC) + timedelta(hours=1)

    async def login(self) -> LoginResponse:
        url = "/api/auth/login"
        req = LoginRequest(username=self._username, password=self._password)
        logger.debug("Authenticating to ThingsBoard at {url}", url=_sanitize_url(url))
        try:
            response = await self._http_client.post(url, json=req.model_dump())
        except httpx.ConnectError as exc:
            logger.error("Connection refused to ThingsBoard: {exc}", exc=exc)
            raise ThingsBoardError(0, f"Connection refused: {exc}") from exc
        except httpx.TimeoutException as exc:
            logger.error("Login timeout to ThingsBoard: {exc}", exc=exc)
            raise ThingsBoardError(0, f"Login timeout: {exc}") from exc

        if response.status_code != 200:
            _raise_on_status(response.status_code, self._safe_body(response))

        data = LoginResponse.model_validate(response.json())
        self._token = data.token
        self._refresh_token = data.refreshToken
        self._token_expiry = self._check_token_expiry(data.token)
        self._auth_retry_count = 0
        logger.info(
            "Authenticated to ThingsBoard, token expires at {exp}",
            exp=self._token_expiry.isoformat(),
        )
        return data

    async def _reauthenticate(self) -> None:
        self._token = None
        self._token_expiry = None
        self._auth_retry_count += 1
        if self._auth_retry_count > 1:
            logger.error(
                "Re-authentication failed after {count} attempts", count=self._auth_retry_count
            )
            raise ThingsBoardAuthError(401, "Re-authentication failed")
        logger.info(
            "Re-authenticating to ThingsBoard (attempt {count})", count=self._auth_retry_count
        )
        await self.login()

    def _safe_body(self, response: httpx.Response) -> Any:
        try:
            return response.json()
        except Exception:
            return response.text[:500]

    def _check_method_allowed(self, method: str, path: str) -> None:
        """Whitelist client-side : GET autorisé, POST /api/auth/login autorisé, tout le reste bloqué."""
        method_upper = method.upper()
        if method_upper == "GET":
            return
        if method_upper == "POST" and path.rstrip("/") == "/api/auth/login":
            return
        logger.error(
            "ThingsBoard whitelist violation: {method} {path} blocked (GET + POST /api/auth/login only)",
            method=method_upper,
            path=path,
        )
        raise ThingsBoardWriteDisabledError(
            403,
            f"Whitelist violation: {method_upper} {path} is not allowed. "
            "Only GET and POST /api/auth/login are permitted.",
        )

    @cast(
        "Callable[..., Any]",
        retry(
            retry=retry_if_exception_type(
                (
                    httpx.TimeoutException,
                    httpx.NetworkError,
                    httpx.RemoteProtocolError,
                    ThingsBoardRateLimitError,
                )
            ),
            wait=wait_exponential(multiplier=2, min=1, max=30),
            stop=stop_after_attempt(3),
            before_sleep=before_sleep_log(cast("logging.Logger", logger), cast("int", "DEBUG")),
            reraise=True,
        ),
    )
    async def _request(
        self,
        method: str,
        path: str,
        params: dict[str, Any] | None = None,
        json_data: Any = None,
    ) -> httpx.Response:
        self._check_method_allowed(method, path)

        headers = await self._ensure_auth_header()

        logger.debug(
            "ThingsBoard API {method} {path}",
            method=method,
            path=path,
        )

        response = await self._http_client.request(
            method=method,
            url=path,
            params=params,
            json=json_data,
            headers=headers,
        )

        if response.status_code == 401:
            logger.info("Received 401, attempting re-authentication")
            await self._reauthenticate()
            headers = await self._ensure_auth_header()
            response = await self._http_client.request(
                method=method,
                url=path,
                params=params,
                json=json_data,
                headers=headers,
            )

        if response.status_code >= 400:
            body = self._safe_body(response)
            _raise_on_status(response.status_code, body)

        return response

    async def _get(self, path: str, params: dict[str, Any] | None = None) -> Any:
        response = await self._request("GET", path, params=params)
        return response.json()

    async def _post(self, path: str, json_data: Any = None) -> Any:
        response = await self._request("POST", path, json_data=json_data)
        if response.status_code == 204 or not response.content:
            return None
        return response.json()

    async def _put(self, path: str, json_data: Any = None) -> Any:
        response = await self._request("PUT", path, json_data=json_data)
        if response.status_code == 204 or not response.content:
            return None
        return response.json()

    async def _delete(self, path: str) -> Any:
        response = await self._request("DELETE", path)
        if response.status_code == 204 or not response.content:
            return None
        return response.json()

    async def get_tenants(self, page: int = 0, page_size: int | None = None) -> PageData:
        params = {"pageSize": page_size or self._page_size, "page": page}
        data = await self._get("/api/tenants", params=params)
        return PageData.model_validate(data)

    async def get_customers(
        self, tenant_id: str, page: int = 0, page_size: int | None = None
    ) -> PageData:
        params = {"tenantId": tenant_id, "pageSize": page_size or self._page_size, "page": page}
        data = await self._get("/api/customers", params=params)
        return PageData.model_validate(data)

    async def get_devices(self, page: int = 0, page_size: int | None = None) -> PageData:
        params = {"pageSize": page_size or self._page_size, "page": page}
        data = await self._get("/api/tenant/devices", params=params)
        result = PageData.model_validate(data)
        result.data = [Device.model_validate(item) for item in result.data]
        return result

    async def get_device_by_id(self, device_id: str) -> Device:
        data = await self._get(f"/api/device/{device_id}")
        return Device.model_validate(data)

    async def get_device_profiles(self, page: int = 0, page_size: int | None = None) -> PageData:
        params = {"pageSize": page_size or self._page_size, "page": page}
        data = await self._get("/api/deviceProfiles", params=params)
        return PageData.model_validate(data)

    async def get_assets(self, page: int = 0, page_size: int | None = None) -> PageData:
        params = {"pageSize": page_size or self._page_size, "page": page}
        data = await self._get("/api/tenant/assets", params=params)
        return PageData.model_validate(data)

    async def get_relations(
        self,
        entity_type: str,
        entity_id: str,
        page: int = 0,
        page_size: int | None = None,
    ) -> list[dict[str, Any]]:
        params = {
            "entityType": entity_type,
            "entityId": entity_id,
            "pageSize": page_size or self._page_size,
            "page": page,
        }
        data = await self._get("/api/relations", params=params)
        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            return cast("list[dict[str, Any]]", data.get("data", data.get("list", [])))
        return []

    async def get_attributes(
        self,
        entity_type: str,
        entity_id: str,
        scope: str = "SERVER_SCOPE",
    ) -> list[AttributeEntry]:
        data = await self._get(
            f"/api/plugins/telemetry/{entity_type}/{entity_id}/attributes/{scope}",
        )
        # ThingsBoard returns an object keyed by attribute name:
        #   {"message": {"value": "...", "lastUpdateTs": 123}, ...}
        # The dict key IS the attribute name; the value object carries
        # value/lastUpdateTs but NOT the key itself. Merge the key in so
        # AttributeEntry validates. Guard unexpected shapes so a bad payload
        # yields a clear error instead of a cryptic one.
        entries: list[AttributeEntry] = []
        if isinstance(data, dict):
            for key, value in data.items():
                if isinstance(value, AttributeEntry):
                    entries.append(value)
                elif isinstance(value, dict):
                    if "value" in value:
                        # ThingsBoard {value, lastUpdateTs} wrapper.
                        entry = dict(value)
                        entry.setdefault("key", key)
                        entries.append(AttributeEntry.model_validate(entry))
                    else:
                        # A JSON object that is not a TB wrapper: conserve the
                        # whole object as-is (valid, representable JSON).
                        entries.append(AttributeEntry(key=key, value=value))
                elif value is None:
                    # TB returned an explicit null for this key: no information
                    # to persist, so skip it rather than invent a value.
                    continue
                else:
                    # ThingsBoard may return a bare JSON scalar (str/int/float/
                    # bool) or a JSON object that is not the {value,lastUpdateTs}
                    # wrapper. Both are valid, unambiguously representable JSON,
                    # so conserve the value as-is. lastUpdateTs is left at its
                    # model default (0) because TB supplied no timestamp.
                    entries.append(AttributeEntry(key=key, value=value))
        elif isinstance(data, list):
            for item in data:
                if isinstance(item, AttributeEntry):
                    entries.append(item)
                elif isinstance(item, dict):
                    entries.append(AttributeEntry.model_validate(item))
                elif item is None:
                    continue
                else:
                    entries.append(AttributeEntry.model_validate({"value": item}))
        return entries

    async def get_timeseries_keys(self, entity_type: str, entity_id: str) -> list[str]:
        data = await self._get(
            f"/api/plugins/telemetry/{entity_type}/{entity_id}/keys/timeseries",
        )
        return list(data)

    async def get_timeseries(
        self,
        entity_type: str,
        entity_id: str,
        keys: list[str],
        start_ts: int | None = None,
        end_ts: int | None = None,
        limit: int = 10000,
        agg: str | None = None,
        interval: int | None = None,
    ) -> dict[str, list[TimeseriesEntry]]:
        params: dict[str, Any] = {
            "keys": ",".join(keys),
            "limit": limit,
        }
        if start_ts is not None:
            params["startTs"] = start_ts
        if end_ts is not None:
            params["endTs"] = end_ts
        if agg is not None:
            params["agg"] = agg
        if interval is not None:
            params["interval"] = interval

        data = await self._get(
            f"/api/plugins/telemetry/{entity_type}/{entity_id}/values/timeseries",
            params=params,
        )
        result: dict[str, list[TimeseriesEntry]] = {}
        if isinstance(data, dict):
            for key, values in data.items():
                result[key] = [TimeseriesEntry.model_validate(v) for v in values]
        return result

    async def post_telemetry(
        self,
        entity_type: str,
        entity_id: str,
        values: dict[str, Any] | list[dict[str, Any]],
    ) -> None:
        if not settings.tb_writeback_enabled:
            logger.warning("Writeback disabled via config, skipping telemetry write")
            return
        if not values:
            logger.warning("Empty values provided to post_telemetry, skipping")
            return
        logger.info(
            "Writing telemetry to {entity_type}/{entity_id} ({count} values)",
            entity_type=entity_type,
            entity_id=entity_id,
            count=len(values) if isinstance(values, list) else 1,
        )
        await self._post(
            f"/api/plugins/telemetry/{entity_type}/{entity_id}/timeseries/ANY",
            json_data=values,
        )

    async def save_alarm(self, alarm_data: AlarmData) -> dict[str, Any]:
        if not settings.tb_alarms_enabled:
            logger.warning("Alarms disabled via config, skipping alarm save")
            return {}
        logger.info(
            "Saving alarm type={alarm_type} severity={sev} for {entity_type}/{entity_id}",
            alarm_type=alarm_data.type,
            sev=alarm_data.severity,
            entity_type=alarm_data.originator.entityType,
            entity_id=alarm_data.originator.id,
        )
        result = await self._post(
            "/api/alarm", json_data=alarm_data.model_dump(exclude={"id"}, by_alias=True)
        )
        return result or {}

    async def get_alarms(
        self,
        entity_type: str,
        entity_id: str,
        status: str = "ACTIVE_UNACK",
        limit: int = 100,
        page: int = 0,
        page_size: int | None = None,
    ) -> PageData:
        params: dict[str, Any] = {
            "entityType": entity_type,
            "entityId": entity_id,
            "status": status,
            "limit": limit,
            "pageSize": page_size or self._page_size,
            "page": page,
        }
        data = await self._get("/api/alarm", params=params)
        return PageData.model_validate(data)

    async def ack_alarm(self, alarm_id: str) -> dict[str, Any]:
        result = await self._put(f"/api/alarm/{alarm_id}/ack")
        return result or {}

    async def clear_alarm(self, alarm_id: str) -> dict[str, Any]:
        result = await self._put(f"/api/alarm/{alarm_id}/clear")
        return result or {}

    async def get_server_info(self) -> dict[str, Any]:
        return cast("dict[str, Any]", (await self._get("/api/info")) or {})

    async def close(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None
        logger.debug("ThingsBoard client closed")

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> None:
        await self.close()
