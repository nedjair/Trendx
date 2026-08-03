from __future__ import annotations

import os
import shutil
import time
import uuid
from collections.abc import Sequence
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

import numpy as np
import pandas as pd
from loguru import logger
from sqlalchemy import text

from trendx.config import settings
from trendx.database.connection import DatabaseManager, manager as db_manager
from trendx.database.repositories import (
    BusinessEntityRepository,
    CheckpointRepository,
    MetricDefinitionRepository,
)
from trendx.thingsboard.telemetry import TelemetryReader, TelemetryPoint


class DiskCapacityError(RuntimeError):
    """Espace disque libre sous le seuil : ingestion arrêtée automatiquement."""


def _default_monitor_mounts() -> tuple[str, ...]:
    mounts_env = os.environ.get("TRENDX_DISK_MONITOR_MOUNTS", "").strip()
    if mounts_env:
        return tuple(m.strip() for m in mounts_env.split(",") if m.strip())
    return ("/",)


def check_disk_min_free(mount: str = "/") -> float:
    """Retourne l'espace libre (GB) sur `mount`, ou lève DiskCapacityError sous le seuil."""
    usage = shutil.disk_usage(mount)
    free_gb = usage.free / (1024**3)
    if free_gb < settings.trendx_disk_min_free_gb:
        raise DiskCapacityError(
            f"Espace libre {mount}: {free_gb:.1f} GB < TRENDX_DISK_MIN_FREE_GB="
            f"{settings.trendx_disk_min_free_gb} GB. Ingestion arrêtée."
        )
    return free_gb


class IngestionService:
    PIPELINE_NAME = "ingestion"

    def __init__(
        self,
        database_manager: DatabaseManager | None = None,
        telemetry_reader: TelemetryReader | None = None,
        batch_size: int = 10000,
        window_hours: int = 24,
    ) -> None:
        self._db = database_manager or db_manager
        self._reader = telemetry_reader or TelemetryReader(
            page_size=batch_size,
            max_window_hours=window_hours,
        )
        self._batch_size = batch_size
        self._window_hours = window_hours
        self._total_processed: int = 0
        self._total_errors: int = 0

    def ensure_disk_available(self, mounts: Sequence[str] | None = None) -> None:
        targets = list(mounts if mounts is not None else _default_monitor_mounts())
        resolved: list[str] = []
        for mount in targets:
            if os.path.isdir(mount):
                check_disk_min_free(mount)
                resolved.append(mount)
        if not resolved:
            raise DiskCapacityError(
                "Aucun point de montage de TRENDX_DISK_MONITOR_MOUNTS n'est résolvable "
                f"depuis ce conteneur : {targets}. Ingestion arrêtée (fail-closed)."
            )
        logger.info(
            "[disk] points de montage surveillés résolus : {}",
            ", ".join(f"{m}={shutil.disk_usage(m).free / (1024**3):.1f} GB libres" for m in resolved),
        )

    @property
    def total_processed(self) -> int:
        return self._total_processed

    @property
    def total_errors(self) -> int:
        return self._total_errors

    def _discover_devices(self) -> list[dict[str, Any]]:
        with self._db.get_session("catalog") as session:
            entity_repo = BusinessEntityRepository(session)
            entities = entity_repo.list()
            metric_repo = MetricDefinitionRepository(session)
            metrics = metric_repo.list()

        device_map: dict[str, dict[str, Any]] = {}
        for ent in entities:
            device_map[str(ent.id)] = {
                "entity_id": str(ent.id),
                "name": ent.name,
                "entity_type": "DEVICE",
            }

        for ent in entities:
            eid = str(ent.id)
            if eid in device_map:
                device_map[eid]["metrics"] = [
                    {"key": m.item_name, "name": m.item_name}
                    for m in metrics
                ]

        result = [d for d in device_map.values() if d.get("metrics")]
        logger.info(
            "Discovered {count} devices with metrics",
            count=len(result),
        )
        return result

    # TODO: checkpoint table does not exist in Trendz 1.15.0 schema.
    #       Watermark is currently kept in-memory only; re-ingestion may duplicate data.
    def _get_checkpoint(
        self, entity_id: str, metric_key: str
    ) -> Optional[datetime]:
        with self._db.get_session("catalog") as session:
            repo = CheckpointRepository(session)
            row = repo.get_watermark("ingestion", entity_id, metric_key)
            if row is None:
                return None
            ts = row.get("watermark_ts") if isinstance(row, dict) else getattr(row, "watermark_ts", None)
            return ts if ts is None else (ts if isinstance(ts, datetime) else datetime.fromisoformat(str(ts)))

    def _update_checkpoint(
        self,
        entity_id: str,
        metric_key: str,
        watermark_ts: datetime,
        records_count: int,
        batch_id: str | None = None,
    ) -> None:
        with self._db.get_session("catalog") as session:
            repo = CheckpointRepository(session)
            repo.upsert_watermark(
                "ingestion",
                entity_id,
                watermark_ts,
                metric_key,
                records_processed=records_count,
                last_batch_id=batch_id,
            )

    def _validate_data(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df

        required = {"ts", "value"}
        missing = required - set(df.columns)
        if missing:
            logger.warning("DataFrame missing columns: {cols}", cols=missing)
            return pd.DataFrame()

        df = df.copy()
        df["ts"] = pd.to_datetime(df["ts"], unit="ms", errors="coerce")
        df = df.dropna(subset=["ts"])

        mask = df["value"].notna()
        df.loc[mask, "value"] = pd.to_numeric(df.loc[mask, "value"], errors="coerce")

        df = df.drop_duplicates(subset=["ts"])
        df = df.sort_values("ts").reset_index(drop=True)

        return df

    def _store_telemetry(
        self,
        df: pd.DataFrame,
        entity_id: str,
        metric_key: str,
        source: str = "thingsboard",
        ingestion_id: str | None = None,
    ) -> int:
        if df.empty:
            return 0

        engine = self._db.get_engine("analytics")
        records = []
        for _, row in df.iterrows():
            ts_val = row["ts"]
            value = row.get("value")
            if pd.isna(ts_val) or pd.isna(value):
                continue

            if isinstance(ts_val, datetime):
                ts_db = ts_val
            else:
                ts_db = pd.Timestamp(ts_val).to_pydatetime()

            val_float = float(value)
            records.append({
                "ts": ts_db,
                "entity_id": entity_id,
                "metric_key": metric_key,
                "dbl_v": val_float,
                "source": source,
                "ingestion_id": ingestion_id,
            })

        if not records:
            return 0

        inserted = 0
        with engine.begin() as conn:
            for batch_start in range(0, len(records), self._batch_size):
                batch = records[batch_start : batch_start + self._batch_size]
                rows = []
                for r in batch:
                    rows.append(
                        f"({self._lit(r['ts'])}, {self._lit_uuid(r['entity_id'])}, "
                        f"{self._lit(r['metric_key'])}, {self._lit(r['dbl_v'])}, "
                        f"{self._lit(r['source'])}, {self._lit(r['ingestion_id'])})"
                    )
                values_clause = ",\n".join(rows)
                stmt = text(
                    f"""
                    INSERT INTO ts_kv (ts, entity_id, metric_key, dbl_v, source, ingestion_id)
                    VALUES {values_clause}
                    ON CONFLICT (ts, entity_id, metric_key)
                    DO UPDATE SET
                        dbl_v = EXCLUDED.dbl_v,
                        source = EXCLUDED.source,
                        ingestion_id = EXCLUDED.ingestion_id,
                        created_at = NOW()
                    """
                )
                result = conn.execute(stmt)
                inserted += result.rowcount

                try:
                    latest_stmt = text(
                        f"""
                        INSERT INTO ts_kv_latest (entity_id, metric_key, ts, dbl_v, source)
                        VALUES {values_clause}
                        ON CONFLICT (entity_id, metric_key)
                        DO UPDATE SET
                            ts = EXCLUDED.ts,
                            dbl_v = EXCLUDED.dbl_v,
                            source = EXCLUDED.source,
                            updated_at = NOW()
                        """
                    )
                    conn.execute(latest_stmt)
                except Exception as exc:
                    logger.warning(
                        "Failed to update ts_kv_latest for {eid}/{key}: {exc}",
                        eid=entity_id[:12],
                        key=metric_key,
                        exc=exc,
                    )

        logger.debug(
            "Stored {n} telemetry rows for {eid}/{key}",
            n=inserted,
            eid=entity_id[:12],
            key=metric_key,
        )
        return inserted

    @staticmethod
    def _lit(value: Any) -> str:
        if value is None:
            return "NULL"
        if isinstance(value, bool):
            return "TRUE" if value else "FALSE"
        if isinstance(value, (int, float)):
            if np.isnan(value) or np.isinf(value):
                return "NULL"
            return str(value)
        if isinstance(value, datetime):
            return f"'{value.isoformat()}'::timestamptz"
        escaped = str(value).replace("'", "''")
        return f"'{escaped}'"

    @staticmethod
    def _lit_uuid(value: str) -> str:
        return f"'{value}'::uuid"

    def _deduplicate(
        self, entity_id: str, metric_key: str, start_ts: datetime, end_ts: datetime
    ) -> int:
        engine = self._db.get_engine("analytics")
        stmt = text(
            """
            DELETE FROM ts_kv
            WHERE entity_id = :eid
              AND metric_key = :key
              AND ts >= :start
              AND ts < :end
              AND ctid NOT IN (
                  SELECT min(ctid)
                  FROM ts_kv
                  WHERE entity_id = :eid2
                    AND metric_key = :key2
                    AND ts >= :start2
                    AND ts < :end2
                  GROUP BY ts
              )
            """
        )
        with engine.begin() as conn:
            result = conn.execute(
                stmt,
                {
                    "eid": entity_id,
                    "key": metric_key,
                    "start": start_ts,
                    "end": end_ts,
                    "eid2": entity_id,
                    "key2": metric_key,
                    "start2": start_ts,
                    "end2": end_ts,
                },
            )
            removed = result.rowcount
            if removed:
                logger.info(
                    "Deduplicated {n} rows for {eid}/{key}",
                    n=removed,
                    eid=entity_id[:12],
                    key=metric_key,
                )
            return removed

    def _write_dead_letter(
        self,
        entity_id: str,
        metric_key: str,
        payload: str,
        error: str,
    ) -> None:
        engine = self._db.get_engine("analytics")
        stmt = text(
            """
            INSERT INTO ts_kv (ts, entity_id, metric_key, str_v, source, ingestion_id)
            VALUES (NOW(), :eid, :key, :payload, 'dead_letter', :error)
            ON CONFLICT (ts, entity_id, metric_key) DO NOTHING
            """
        )
        try:
            with engine.begin() as conn:
                conn.execute(
                    stmt,
                    {
                        "eid": entity_id,
                        "key": f"{metric_key}__dead_letter",
                        "payload": payload[:1000],
                        "error": str(error)[:500],
                    },
                )
        except Exception as exc:
            logger.error(
                "Failed to write dead-letter for {eid}/{key}: {exc}",
                eid=entity_id[:12],
                key=metric_key,
                exc=exc,
            )

    async def _ingest_metric_batch(
        self,
        entity_id: str,
        metric_key: str,
        start_dt: datetime,
        end_dt: datetime,
        ingestion_id: str | None = None,
    ) -> int:
        start_ms = int(start_dt.timestamp() * 1000)
        end_ms = int(end_dt.timestamp() * 1000)

        raw = await self._reader.read_historical(
            entity_type="DEVICE",
            entity_id=entity_id,
            keys=[metric_key],
            start_ts=start_ms,
            end_ts=end_ms,
        )

        points = raw.get(metric_key, [])
        if not points:
            return 0

        records = [{"ts": p.ts, "value": p.value} for p in points]
        df = pd.DataFrame(records)
        df = self._validate_data(df)

        if df.empty:
            logger.debug(
                "No valid data after validation for {eid}/{key}",
                eid=entity_id[:12],
                key=metric_key,
            )
            return 0

        stored = self._store_telemetry(
            df=df,
            entity_id=entity_id,
            metric_key=metric_key,
            ingestion_id=ingestion_id,
        )
        return stored

    def _build_time_windows(
        self, start_dt: datetime, end_dt: datetime
    ) -> list[tuple[datetime, datetime]]:
        windows: list[tuple[datetime, datetime]] = []
        current = start_dt
        delta = timedelta(hours=self._window_hours)
        while current < end_dt:
            next_ts = min(current + delta, end_dt)
            windows.append((current, next_ts))
            current = next_ts
        return windows

    async def ingest_device_metric(
        self,
        entity_id: str,
        metric_key: str,
        start_ts: datetime,
        end_ts: datetime,
    ) -> dict[str, Any]:
        logger.info(
            "Ingesting {eid}/{key} from {start} to {end}",
            eid=entity_id[:12],
            key=metric_key,
            start=start_ts.isoformat(),
            end=end_ts.isoformat(),
        )
        self.ensure_disk_available()

        batch_id = str(uuid.uuid4())[:12]
        windows = self._build_time_windows(start_ts, end_ts)
        total_stored = 0
        window_results: list[dict[str, Any]] = []

        for wstart, wend in windows:
            try:
                stored = await self._ingest_metric_batch(
                    entity_id=entity_id,
                    metric_key=metric_key,
                    start_dt=wstart,
                    end_dt=wend,
                    ingestion_id=batch_id,
                )
                total_stored += stored
                window_results.append({
                    "window_start": wstart.isoformat(),
                    "window_end": wend.isoformat(),
                    "stored": stored,
                })
            except Exception as exc:
                logger.error(
                    "Ingestion failed for {eid}/{key} window [{ws}, {we}): {exc}",
                    eid=entity_id[:12],
                    key=metric_key,
                    ws=wstart.isoformat(),
                    we=wend.isoformat(),
                    exc=exc,
                )
                self._total_errors += 1
                self._write_dead_letter(
                    entity_id, metric_key, f"window:{wstart}-{wend}", str(exc)
                )

        if total_stored > 0:
            self._update_checkpoint(
                entity_id=entity_id,
                metric_key=metric_key,
                watermark_ts=end_ts,
                records_count=total_stored,
                batch_id=batch_id,
            )

        self._total_processed += total_stored

        result = {
            "entity_id": entity_id,
            "metric_key": metric_key,
            "start_ts": start_ts.isoformat(),
            "end_ts": end_ts.isoformat(),
            "total_stored": total_stored,
            "windows": len(windows),
            "window_results": window_results,
            "batch_id": batch_id,
        }
        logger.info(
            "Ingestion complete for {eid}/{key}: {stored} records in {wins} windows",
            eid=entity_id[:12],
            key=metric_key,
            stored=total_stored,
            wins=len(windows),
        )
        return result

    async def run_full_backfill(
        self,
        entity_id: str,
        metric_key: str,
        start_date: datetime,
        end_date: datetime | None = None,
    ) -> dict[str, Any]:
        end = end_date or datetime.now(timezone.utc)
        logger.info(
            "Backfill {eid}/{key} from {start} to {end}",
            eid=entity_id[:12],
            key=metric_key,
            start=start_date.isoformat(),
            end=end.isoformat(),
        )
        self.ensure_disk_available()

        start_time = time.monotonic()
        result = await self.ingest_device_metric(
            entity_id=entity_id,
            metric_key=metric_key,
            start_ts=start_date,
            end_ts=end,
        )
        duration = time.monotonic() - start_time

        self._deduplicate(entity_id, metric_key, start_date, end)

        result["duration_seconds"] = round(duration, 2)
        result["operation"] = "backfill"
        logger.info(
            "Backfill complete for {eid}/{key}: {stored} records in {dur}s",
            eid=entity_id[:12],
            key=metric_key,
            stored=result["total_stored"],
            dur=round(duration, 2),
        )
        return result

    async def run_incremental_ingest(self) -> list[dict[str, Any]]:
        logger.info("Starting incremental ingestion for all devices")
        self.ensure_disk_available()
        devices = self._discover_devices()
        if not devices:
            logger.warning("No devices with active metrics discovered")
            return []

        now = datetime.now(timezone.utc)
        results: list[dict[str, Any]] = []

        for device in devices:
            eid = device["entity_id"]
            for metric in device.get("metrics", []):
                key = metric["key"]
                try:
                    cp = self._get_checkpoint(eid, key)
                    if cp is not None:
                        start_ts = cp
                    else:
                        start_ts = now - timedelta(
                            days=settings.training_lookback_days
                        )

                    if start_ts >= now:
                        logger.debug(
                            "Checkpoint at future for {eid}/{key}, skipping",
                            eid=eid[:12],
                            key=key,
                        )
                        continue

                    result = await self.ingest_device_metric(
                        entity_id=eid,
                        metric_key=key,
                        start_ts=start_ts,
                        end_ts=now,
                    )
                    result["operation"] = "incremental"
                    results.append(result)

                except Exception as exc:
                    self._total_errors += 1
                    logger.error(
                        "Incremental ingest failed for {eid}/{key}: {exc}",
                        eid=eid[:12],
                        key=key,
                        exc=exc,
                    )
                    self._write_dead_letter(eid, key, "incremental_run", str(exc))

        logger.info(
            "Incremental ingestion complete: {processed} processed, {errors} errors across {devices} devices",
            processed=self._total_processed,
            errors=self._total_errors,
            devices=len(devices),
        )
        return results

    async def close(self) -> None:
        await self._reader.close()
        logger.info(
            "IngestionService closed: {processed} processed, {errors} errors",
            processed=self._total_processed,
            errors=self._total_errors,
        )
