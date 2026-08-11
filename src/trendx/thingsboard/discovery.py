from __future__ import annotations

import asyncio
import re
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from loguru import logger
from trendx.config import settings
from trendx.thingsboard.client import (
    Asset,
    Device,
    DeviceProfile,
    PageData,
    Relation,
    ThingsBoardClient,
    ThingsBoardError,
)


@dataclass
class CatalogEntry:
    entity_type: str
    entity_id: str
    name: str
    label: str = ""
    entity_data: dict[str, Any] = field(default_factory=dict)
    first_seen: float = 0.0
    last_seen: float = 0.0
    attributes: dict[str, Any] = field(default_factory=dict)
    telemetry_keys: list[str] = field(default_factory=list)


@dataclass
class Catalog:
    devices: dict[str, CatalogEntry] = field(default_factory=dict)
    assets: dict[str, CatalogEntry] = field(default_factory=dict)
    device_profiles: list[DeviceProfile] = field(default_factory=list)
    relations: list[Relation] = field(default_factory=list)
    last_sync_ts: float = 0.0
    sync_count: int = 0


@dataclass
class InclusionRules:
    tenant_ids: set[str] = field(default_factory=set)
    customer_ids: set[str] = field(default_factory=set)
    profile_names: set[str] = field(default_factory=set)
    device_name_patterns: list[re.Pattern] = field(default_factory=list)
    asset_name_patterns: list[re.Pattern] = field(default_factory=list)

    def include_device(self, device: Device) -> bool:
        if self.tenant_ids and device.tenantId and device.tenantId.id not in self.tenant_ids:
            return False
        if (
            self.customer_ids
            and device.customerId
            and device.customerId.id not in self.customer_ids
        ):
            return False
        if self.profile_names and device.type not in self.profile_names:
            return False
        if self.device_name_patterns and not any(
            p.search(device.name) for p in self.device_name_patterns
        ):
            return False
        return True

    def include_asset(self, asset: Asset) -> bool:
        if self.tenant_ids and asset.tenantId and asset.tenantId.id not in self.tenant_ids:
            return False
        if self.customer_ids and asset.customerId and asset.customerId.id not in self.customer_ids:
            return False
        if self.asset_name_patterns and not any(
            p.search(asset.name) for p in self.asset_name_patterns
        ):
            return False
        return True


class TokenBucketRateLimiter:
    def __init__(self, rate: float = 10.0, burst: int = 20) -> None:
        self._rate = rate
        self._burst = burst
        self._tokens = float(burst)
        self._last_refill = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        async with self._lock:
            now = time.monotonic()
            elapsed = now - self._last_refill
            self._tokens = min(float(self._burst), self._tokens + elapsed * self._rate)
            self._last_refill = now
            if self._tokens < 1.0:
                wait = (1.0 - self._tokens) / self._rate
                self._last_refill = now + wait
                self._tokens = 0.0
                await asyncio.sleep(wait)
            else:
                self._tokens -= 1.0


class TopologyDiscoveryService:
    def __init__(
        self,
        client: ThingsBoardClient | None = None,
        rules: InclusionRules | None = None,
        requests_per_second: float = 10.0,
        requests_burst: int = 20,
    ) -> None:
        self._client = client or ThingsBoardClient()
        self._rules = rules or InclusionRules()
        self._rate_limiter = TokenBucketRateLimiter(requests_per_second, requests_burst)
        self._catalog = Catalog()
        self._error_counts: dict[str, int] = {}

    @property
    def catalog(self) -> Catalog:
        return self._catalog

    async def _paginate(self, fetch_page, *args, **kwargs) -> list[Any]:
        items: list[Any] = []
        page = 0
        while True:
            await self._rate_limiter.acquire()
            result = await fetch_page(*args, page=page, **kwargs)
            if isinstance(result, PageData):
                items.extend(result.data)
                if not result.hasNext:
                    break
            elif isinstance(result, list):
                items.extend(result)
                if not result:
                    break
            page += 1
        return items

    async def discover_devices(self) -> list[Device]:
        logger.info("Discovering all devices")
        raw = await self._paginate(self._client.get_devices, page_size=settings.tb_page_size)
        devices = [Device.model_validate(d) if not isinstance(d, Device) else d for d in raw]
        filtered = [d for d in devices if self._rules.include_device(d)]
        logger.info(
            "Discovered {total} devices, {kept} after filtering",
            total=len(devices),
            kept=len(filtered),
        )
        for d in filtered:
            eid = d.id.id if d.id else ""
            self._catalog.devices[eid] = CatalogEntry(
                entity_type="DEVICE",
                entity_id=eid,
                name=d.name,
                label=d.label,
                entity_data=d.model_dump(),
                first_seen=time.time(),
                last_seen=time.time(),
            )
        return filtered

    async def discover_assets(self) -> list[Asset]:
        logger.info("Discovering all assets")
        raw = await self._paginate(self._client.get_assets, page_size=settings.tb_page_size)
        assets = [Asset.model_validate(a) if not isinstance(a, Asset) else a for a in raw]
        filtered = [a for a in assets if self._rules.include_asset(a)]
        logger.info(
            "Discovered {total} assets, {kept} after filtering",
            total=len(assets),
            kept=len(filtered),
        )
        for a in filtered:
            eid = a.id.id if a.id else ""
            self._catalog.assets[eid] = CatalogEntry(
                entity_type="ASSET",
                entity_id=eid,
                name=a.name,
                label=a.label,
                entity_data=a.model_dump(),
                first_seen=time.time(),
                last_seen=time.time(),
            )
        return filtered

    async def discover_profiles(self) -> list[DeviceProfile]:
        logger.info("Discovering device profiles")
        raw = await self._paginate(
            self._client.get_device_profiles, page_size=settings.tb_page_size
        )
        profiles = [
            DeviceProfile.model_validate(p) if not isinstance(p, DeviceProfile) else p for p in raw
        ]
        self._catalog.device_profiles = profiles
        logger.info("Discovered {count} device profiles", count=len(profiles))
        return profiles

    async def discover_relations(self) -> list[Relation]:
        logger.info("Discovering relations")
        relations: list[Relation] = []
        for eid in list(self._catalog.devices.keys()):
            await self._rate_limiter.acquire()
            try:
                raw = await self._client.get_relations("DEVICE", eid)
                for r in raw:
                    if isinstance(r, dict):
                        relations.append(Relation.model_validate(r))
            except ThingsBoardError as exc:
                self._track_error("DEVICE", eid, exc)
                logger.warning("Failed to get relations for device {eid}: {exc}", eid=eid, exc=exc)
        for eid in list(self._catalog.assets.keys()):
            await self._rate_limiter.acquire()
            try:
                raw = await self._client.get_relations("ASSET", eid)
                for r in raw:
                    if isinstance(r, dict):
                        relations.append(Relation.model_validate(r))
            except ThingsBoardError as exc:
                self._track_error("ASSET", eid, exc)
                logger.warning("Failed to get relations for asset {eid}: {exc}", eid=eid, exc=exc)
        self._catalog.relations = relations
        logger.info("Discovered {count} relations", count=len(relations))
        return relations

    async def discover_attributes(self, entity_type: str, entity_id: str) -> dict[str, Any]:
        attributes: dict[str, Any] = {}
        for scope in ("SERVER_SCOPE", "SHARED_SCOPE", "CLIENT_SCOPE"):
            await self._rate_limiter.acquire()
            try:
                entries = await self._client.get_attributes(entity_type, entity_id, scope=scope)
                for entry in entries:
                    attributes[entry.key] = entry.value
            except ThingsBoardError as exc:
                self._track_error(entity_type, entity_id, exc)
                logger.warning(
                    "Failed to get attributes for {etype}/{eid} scope={scope}: {exc}",
                    etype=entity_type,
                    eid=entity_id,
                    scope=scope,
                    exc=exc,
                )
        return attributes

    async def _discover_all_attributes(self) -> None:
        logger.info("Discovering attributes")
        for eid, entry in self._catalog.devices.items():
            attrs = await self.discover_attributes("DEVICE", eid)
            entry.attributes = attrs
        for eid, entry in self._catalog.assets.items():
            attrs = await self.discover_attributes("ASSET", eid)
            entry.attributes = attrs

    async def discover_telemetry_keys(self) -> dict[str, list[str]]:
        logger.info("Discovering telemetry keys")
        telemetry_map: dict[str, list[str]] = {}
        for eid, entry in self._catalog.devices.items():
            await self._rate_limiter.acquire()
            try:
                keys = await self._client.get_timeseries_keys("DEVICE", eid)
                entry.telemetry_keys = keys
                telemetry_map[eid] = keys
            except ThingsBoardError as exc:
                self._track_error("DEVICE", eid, exc)
                logger.warning(
                    "Failed to get telemetry keys for device {eid}: {exc}", eid=eid, exc=exc
                )
        for eid, entry in self._catalog.assets.items():
            await self._rate_limiter.acquire()
            try:
                keys = await self._client.get_timeseries_keys("ASSET", eid)
                entry.telemetry_keys = keys
                telemetry_map[eid] = keys
            except ThingsBoardError as exc:
                self._track_error("ASSET", eid, exc)
                logger.warning(
                    "Failed to get telemetry keys for asset {eid}: {exc}", eid=eid, exc=exc
                )
        logger.info("Discovered telemetry keys for {count} entities", count=len(telemetry_map))
        return telemetry_map

    def _track_error(self, entity_type: str, entity_id: str, exc: Exception) -> None:
        key = f"{entity_type}:{entity_id}"
        self._error_counts[key] = self._error_counts.get(key, 0) + 1

    async def full_sync(self) -> Catalog:
        logger.info("Starting full topology sync")
        await self._client.login()
        await self.discover_profiles()
        await self.discover_devices()
        await self.discover_assets()
        await self.discover_relations()
        await self._discover_all_attributes()
        await self.discover_telemetry_keys()
        self._catalog.last_sync_ts = time.time()
        self._catalog.sync_count += 1
        logger.info(
            "Full sync complete: {devices} devices, {assets} assets, {profiles} profiles, {relations} relations, {errors} errors",
            devices=len(self._catalog.devices),
            assets=len(self._catalog.assets),
            profiles=len(self._catalog.device_profiles),
            relations=len(self._catalog.relations),
            errors=len(self._error_counts),
        )
        return self._catalog

    async def incremental_sync(self) -> Catalog:
        logger.info("Starting incremental topology sync")
        await self._client.login()
        previous_ids = set(self._catalog.devices.keys()) | set(self._catalog.assets.keys())
        await self.discover_devices()
        await self.discover_assets()
        current_ids = set(self._catalog.devices.keys()) | set(self._catalog.assets.keys())
        new_ids = current_ids - previous_ids
        removed_ids = previous_ids - current_ids
        if removed_ids:
            logger.info("Removed {count} entities from catalog", count=len(removed_ids))
            for eid in removed_ids:
                self._catalog.devices.pop(eid, None)
                self._catalog.assets.pop(eid, None)
        if new_ids:
            logger.info("Found {count} new entities", count=len(new_ids))
            await self.discover_relations()
            for eid in new_ids:
                etype = "DEVICE" if eid in self._catalog.devices else "ASSET"
                attrs = await self.discover_attributes(etype, eid)
                entry = self._catalog.devices.get(eid) or self._catalog.assets.get(eid)
                if entry:
                    entry.attributes = attrs
                if etype == "DEVICE":
                    await self._rate_limiter.acquire()
                    try:
                        keys = await self._client.get_timeseries_keys("DEVICE", eid)
                        if entry:
                            entry.telemetry_keys = keys
                    except ThingsBoardError as exc:
                        self._track_error(etype, eid, exc)
        else:
            logger.info("No new entities detected")
        self._catalog.last_sync_ts = time.time()
        self._catalog.sync_count += 1
        return self._catalog

    async def validate_topology(self) -> list[str]:
        issues: list[str] = []
        for eid, entry in self._catalog.devices.items():
            if not entry.name:
                issues.append(f"Device {eid} has no name")
        for eid, entry in self._catalog.assets.items():
            if not entry.name:
                issues.append(f"Asset {eid} has no name")
        relation_keys: set[str] = set()
        for rel in self._catalog.relations:
            key = f"{rel.from_.entityType}:{rel.from_.id}->{rel.type}->{rel.to.entityType}:{rel.to.id}"
            if key in relation_keys:
                issues.append(f"Duplicate relation: {key}")
            relation_keys.add(key)
        total_entities = len(self._catalog.devices) + len(self._catalog.assets)
        if total_entities == 0:
            issues.append("Catalog is empty: no devices or assets discovered")
        if not self._catalog.device_profiles:
            issues.append("No device profiles discovered")
        if issues:
            for issue in issues:
                logger.warning("Topology validation issue: {issue}", issue=issue)
        else:
            logger.info("Topology validation passed with no issues")
        return issues

    async def update_catalog(self) -> None:
        # NOTE: Trendz 1.15.0 real schema uses `relation` table, not `entity_relation`.
        # The local topology persistence below creates separate tables and does not
        # touch the main `relation` table to avoid coupling with the read-only TB DB.
        # A successful return means the catalog was actually written and committed.
        # Persistence failures are NOT swallowed: they propagate to the caller
        # (e.g. the worker execution loop), which decides how to handle them
        # (typically marking the task FAILED rather than FINISHED).
        from sqlalchemy import JSON, Column, DateTime, Integer, String, Text, create_engine
        from sqlalchemy.orm import declarative_base, sessionmaker

        dsn = settings.catalog_dsn_app()
        engine = create_engine(dsn)
        Base = declarative_base()  # noqa: N806

        class TopologyEntity(Base):
            __tablename__ = "topology_entities"
            id = Column(Integer, primary_key=True, autoincrement=True)
            entity_type = Column(String(32), nullable=False)
            entity_id = Column(String(64), nullable=False, index=True)
            name = Column(String(255), nullable=False)
            label = Column(String(255), default="")
            entity_data = Column(JSON)
            attributes = Column(JSON)
            telemetry_keys = Column(JSON)
            first_seen = Column(DateTime, default=datetime.utcnow)
            last_seen = Column(DateTime, default=datetime.utcnow)

        class TopologyRelation(Base):
            __tablename__ = "topology_relations"
            id = Column(Integer, primary_key=True, autoincrement=True)
            from_type = Column(String(32), nullable=False)
            from_id = Column(String(64), nullable=False)
            to_type = Column(String(32), nullable=False)
            to_id = Column(String(64), nullable=False)
            relation_type = Column(String(64), nullable=False)
            relation_data = Column(JSON)

        class SyncMetadata(Base):
            __tablename__ = "sync_metadata"
            id = Column(Integer, primary_key=True, autoincrement=True)
            sync_key = Column(String(128), unique=True, nullable=False)
            sync_value = Column(Text, default="")

        Base.metadata.create_all(engine)
        Session = sessionmaker(bind=engine)  # noqa: N806
        session = Session()

        try:
            session.query(TopologyEntity).delete()
            session.query(TopologyRelation).delete()
            now = datetime.utcnow()
            for entry in self._catalog.devices.values():
                session.add(
                    TopologyEntity(
                        entity_type="DEVICE",
                        entity_id=entry.entity_id,
                        name=entry.name,
                        label=entry.label,
                        entity_data=entry.entity_data,
                        attributes=entry.attributes,
                        telemetry_keys=entry.telemetry_keys,
                        first_seen=datetime.fromtimestamp(entry.first_seen)
                        if entry.first_seen
                        else now,
                        last_seen=now,
                    )
                )
            for entry in self._catalog.assets.values():
                session.add(
                    TopologyEntity(
                        entity_type="ASSET",
                        entity_id=entry.entity_id,
                        name=entry.name,
                        label=entry.label,
                        entity_data=entry.entity_data,
                        attributes=entry.attributes,
                        telemetry_keys=entry.telemetry_keys,
                        first_seen=datetime.fromtimestamp(entry.first_seen)
                        if entry.first_seen
                        else now,
                        last_seen=now,
                    )
                )
            for rel in self._catalog.relations:
                session.add(
                    TopologyRelation(
                        from_type=rel.from_.entityType,
                        from_id=rel.from_.id,
                        to_type=rel.to.entityType,
                        to_id=rel.to.id,
                        relation_type=rel.type,
                        relation_data=rel.additionalInfo or {},
                    )
                )
            meta = SyncMetadata(sync_key="last_sync_ts", sync_value=str(self._catalog.last_sync_ts))
            session.merge(meta)
            session.commit()
            logger.info(
                "Catalog persisted to database ({devices} devices, {assets} assets, {relations} relations)",
                devices=len(self._catalog.devices),
                assets=len(self._catalog.assets),
                relations=len(self._catalog.relations),
            )
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()
            engine.dispose()

    async def close(self) -> None:
        logger.info(
            "TopologyDiscoveryService closed ({errors} errors tracked)",
            errors=len(self._error_counts),
        )
