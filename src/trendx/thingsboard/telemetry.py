from __future__ import annotations

from typing import Any

from loguru import logger
from pydantic import BaseModel, Field
from trendx.config import settings
from trendx.thingsboard.client import (
    ThingsBoardClient,
    ThingsBoardError,
)


class TelemetryPoint(BaseModel):
    ts: int
    value: float | str | None = None


class TelemetryBatch(BaseModel):
    entity_type: str
    entity_id: str
    key: str
    points: list[TelemetryPoint] = Field(default_factory=list)


class TelemetryReader:
    def __init__(
        self,
        client: ThingsBoardClient | None = None,
        page_size: int = 10000,
        max_window_hours: int = 24,
    ) -> None:
        self._client = client or ThingsBoardClient()
        self._page_size = page_size
        self._max_window_hours = max_window_hours
        self._sql_available: bool | None = None
        self._sql_conn: Any = None

    async def _check_sql_channel(self) -> bool:
        if self._sql_available is not None:
            return self._sql_available
        dsn = settings.tb_db_readonly_dsn()
        if not dsn:
            logger.info("SQL read channel not configured (missing TB_DB_READONLY_USER/PASSWORD)")
            self._sql_available = False
            return False
        try:
            import psycopg2

            conn = psycopg2.connect(
                dsn,
                connect_timeout=settings.tb_db_connect_timeout,
                sslmode=settings.tb_db_sslmode,
            )
            conn.close()
            self._sql_available = True
            logger.info("SQL read channel available for high-volume telemetry extraction")
            return True
        except Exception as exc:
            logger.warning("SQL read channel unavailable, falling back to API: {exc}", exc=exc)
            self._sql_available = False
            return False

    async def _open_sql_connection(self) -> Any:
        if self._sql_conn is not None:
            try:
                self._sql_conn.cursor().execute("SELECT 1")
                return self._sql_conn
            except Exception:
                self._sql_conn = None
        dsn = settings.tb_db_readonly_dsn()
        if not dsn:
            return None
        try:
            import psycopg2

            conn = psycopg2.connect(
                dsn,
                connect_timeout=settings.tb_db_connect_timeout,
                sslmode=settings.tb_db_sslmode,
            )
            # Double assurance : read-only + statement_timeout. Le rôle
            # trendx_ro porte déjà ces paramètres ; on les (ré)applique côté
            # session pour couvrir tout changement de configuration du rôle.
            conn.set_session(readonly=True, autocommit=True)
            cur = conn.cursor()
            cur.execute(f"SET statement_timeout = {settings.tb_db_statement_timeout}")
            cur.close()
            self._sql_conn = conn
            return conn
        except Exception as exc:
            logger.warning("Failed to open SQL connection: {exc}", exc=exc)
            return None

    def _close_sql_connection(self) -> None:
        if self._sql_conn is not None:
            try:
                self._sql_conn.close()
            except Exception as exc:
                logger.debug("Erreur ignorée à la fermeture SQL : {}", exc)
            self._sql_conn = None

    async def _read_via_sql(
        self,
        entity_type: str,
        entity_id: str,
        keys: list[str],
        start_ts: int,
        end_ts: int,
    ) -> dict[str, list[TelemetryPoint]] | None:
        conn = await self._open_sql_connection()
        if conn is None:
            return None
        try:
            result: dict[str, list[TelemetryPoint]] = {k: [] for k in keys}
            if entity_type != "DEVICE":
                return None
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT key, ts, bool_v, str_v, long_v, dbl_v
                FROM ts_kv
                WHERE entity_id = %s
                  AND ts >= %s
                  AND ts < %s
                  AND key = ANY(%s)
                ORDER BY ts ASC
                """,
                (entity_id, start_ts, end_ts, keys),
            )
            for row in cursor.fetchall():
                key, ts, bool_v, str_v, long_v, dbl_v = row
                value: float | str | None = None
                if dbl_v is not None:
                    value = float(dbl_v)
                elif long_v is not None:
                    value = float(long_v)
                elif bool_v is not None:
                    value = float(bool_v)
                elif str_v is not None:
                    value = str_v
                if key in result:
                    result[key].append(TelemetryPoint(ts=ts, value=value))
            cursor.close()
            logger.info(
                "SQL read {etype}/{eid} {keys} ({start}->{end}): {count} points",
                etype=entity_type,
                eid=entity_id[:12],
                keys=keys,
                start=start_ts,
                end=end_ts,
                count=sum(len(v) for v in result.values()),
            )
            return result
        except Exception as exc:
            logger.warning("SQL read failed, will fall back to API: {exc}", exc=exc)
            return None

    async def _read_via_api(
        self,
        entity_type: str,
        entity_id: str,
        keys: list[str],
        start_ts: int,
        end_ts: int,
    ) -> dict[str, list[TelemetryPoint]]:
        all_data: dict[str, list[TelemetryPoint]] = {k: [] for k in keys}
        remaining = set(keys)
        offset_ts = start_ts
        batch_count = 0
        while remaining and offset_ts < end_ts:
            try:
                key_list = list(remaining)
                raw = await self._client.get_timeseries(
                    entity_type=entity_type,
                    entity_id=entity_id,
                    keys=key_list,
                    start_ts=offset_ts,
                    end_ts=end_ts,
                    limit=self._page_size,
                )
                batch_count += 1
                for key in list(remaining):
                    entries = raw.get(key, [])
                    for entry in entries:
                        all_data[key].append(TelemetryPoint(ts=entry.ts, value=entry.value))
                    if len(entries) < self._page_size:
                        remaining.discard(key)
                if raw:
                    max_ts = 0
                    for vals in raw.values():
                        for v in vals:
                            if v.ts > max_ts:
                                max_ts = v.ts
                    offset_ts = max_ts + 1
                else:
                    break
                if offset_ts >= end_ts:
                    break
            except ThingsBoardError as exc:
                logger.error(
                    "API read error for {etype}/{eid}: {exc}",
                    etype=entity_type,
                    eid=entity_id,
                    exc=exc,
                )
                break
        for key in list(all_data.keys()):
            all_data[key].sort(key=lambda p: p.ts)
        logger.info(
            "API read {etype}/{eid} ({start}->{end}): {keys} keys, {batches} batches, {total} points",
            etype=entity_type,
            eid=entity_id[:12],
            start=start_ts,
            end=end_ts,
            keys=len(keys),
            batches=batch_count,
            total=sum(len(v) for v in all_data.values()),
        )
        return all_data

    def _split_window(
        self,
        start_ts: int,
        end_ts: int,
    ) -> list[tuple[int, int]]:
        if self._max_window_hours <= 0:
            return [(start_ts, end_ts)]
        window_ms = self._max_window_hours * 3600 * 1000
        windows: list[tuple[int, int]] = []
        current = start_ts
        while current < end_ts:
            next_ts = min(current + window_ms, end_ts)
            windows.append((current, next_ts))
            current = next_ts
        return windows

    async def read_historical(
        self,
        entity_type: str,
        entity_id: str,
        keys: list[str],
        start_ts: int,
        end_ts: int,
    ) -> dict[str, list[TelemetryPoint]]:
        if not keys:
            return {}
        if start_ts >= end_ts:
            logger.warning(
                "Invalid time range: start_ts >= end_ts ({s} >= {e})", s=start_ts, e=end_ts
            )
            return {k: [] for k in keys}

        sql_ok = await self._check_sql_channel()
        result: dict[str, list[TelemetryPoint]] = {k: [] for k in keys}

        if sql_ok:
            logger.debug(
                "Reading via SQL channel: {etype}/{eid} {keys} ({start}->{end})",
                etype=entity_type,
                eid=entity_id[:12],
                keys=keys,
                start=start_ts,
                end=end_ts,
            )
            sql_data = await self._read_via_sql(entity_type, entity_id, keys, start_ts, end_ts)
            if sql_data is not None:
                for k in keys:
                    result[k] = sql_data.get(k, [])
                return result

        windows = self._split_window(start_ts, end_ts)
        logger.info(
            "Reading via API channel: {etype}/{eid} {keys} ({wins} windows, {start}->{end})",
            etype=entity_type,
            eid=entity_id[:12],
            keys=keys,
            wins=len(windows),
            start=start_ts,
            end=end_ts,
        )
        for wstart, wend in windows:
            batch = await self._read_via_api(entity_type, entity_id, keys, wstart, wend)
            for k in keys:
                result[k].extend(batch.get(k, []))
        for k in keys:
            result[k].sort(key=lambda p: p.ts)
        return result

    async def read_latest(
        self,
        entity_type: str,
        entity_id: str,
        keys: list[str],
    ) -> dict[str, TelemetryPoint | None]:
        if not keys:
            return {}
        try:
            raw = await self._client.get_timeseries(
                entity_type=entity_type,
                entity_id=entity_id,
                keys=keys,
                limit=1,
            )
            result: dict[str, TelemetryPoint | None] = {}
            for key in keys:
                entries = raw.get(key, [])
                result[key] = (
                    TelemetryPoint(ts=entries[0].ts, value=entries[0].value) if entries else None
                )
            return result
        except ThingsBoardError as exc:
            logger.error(
                "Failed to read latest values for {etype}/{eid}: {exc}",
                etype=entity_type,
                eid=entity_id,
                exc=exc,
            )
            return {k: None for k in keys}

    async def close(self) -> None:
        self._close_sql_connection()
        logger.debug("TelemetryReader closed")
