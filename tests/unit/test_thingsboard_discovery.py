from __future__ import annotations

import re
from unittest.mock import AsyncMock, Mock

import pytest
from faker import Faker
from trendx.thingsboard.client import (
    Asset,
    AttributeEntry,
    Device,
    DeviceProfile,
    EntityId,
    PageData,
    Relation,
)
from trendx.thingsboard.discovery import InclusionRules, TopologyDiscoveryService

fake = Faker()


def _fake_device(name=None, eid=None):
    return Device(
        id=EntityId(entityType="DEVICE", id=eid or fake.uuid4()),
        name=name or fake.word(),
        type="sensor",
        label="",
    )


def _fake_asset(name=None, eid=None):
    return Asset(
        id=EntityId(entityType="ASSET", id=eid or fake.uuid4()),
        name=name or fake.word(),
        type="gateway",
        label="",
    )


def _fake_relation(from_id, to_id):
    return Relation(
        from_=EntityId(entityType="DEVICE", id=from_id),
        to=EntityId(entityType="DEVICE", id=to_id),
        type="Contains",
    )


@pytest.fixture
def service():
    client = Mock()
    client.login = AsyncMock()
    client.get_devices = AsyncMock(
        return_value=PageData(data=[], totalPages=1, totalElements=0, hasNext=False)
    )
    client.get_assets = AsyncMock(
        return_value=PageData(data=[], totalPages=1, totalElements=0, hasNext=False)
    )
    client.get_device_profiles = AsyncMock(
        return_value=PageData(data=[], totalPages=1, totalElements=0, hasNext=False)
    )
    client.get_relations = AsyncMock(return_value=[])
    client.get_attributes = AsyncMock(return_value=[])
    client.get_timeseries_keys = AsyncMock(return_value=[])
    svc = TopologyDiscoveryService(
        client=client,
        requests_per_second=1000,
        requests_burst=1000,
    )
    return svc


@pytest.mark.unit
@pytest.mark.asyncio
async def test_full_sync(service):
    d1 = _fake_device(name="temp-sensor")
    d2 = _fake_device(name="humidity-sensor")
    p1 = DeviceProfile(name="default", type="DEFAULT")
    rel = _fake_relation(d1.id.id, d2.id.id)

    service._client.get_devices = AsyncMock(
        return_value=PageData(data=[d1, d2], totalPages=1, totalElements=2, hasNext=False)
    )
    service._client.get_device_profiles = AsyncMock(
        return_value=PageData(data=[p1], totalPages=1, totalElements=1, hasNext=False)
    )
    service._client.get_assets = AsyncMock(
        return_value=PageData(data=[], totalPages=1, totalElements=0, hasNext=False)
    )
    service._client.get_relations = AsyncMock(
        side_effect=[
            [rel.model_dump(by_alias=True)],
            [],
        ]
    )
    service._client.get_attributes = AsyncMock(return_value=[])
    service._client.get_timeseries_keys = AsyncMock(return_value=["temperature"])

    catalog = await service.full_sync()

    assert len(catalog.devices) == 2
    assert len(catalog.device_profiles) == 1
    assert len(catalog.relations) == 1
    assert catalog.sync_count == 1


@pytest.mark.unit
@pytest.mark.asyncio
async def test_incremental_sync(service):
    d1 = _fake_device(name="existing-device", eid="dev-001")
    service._catalog.devices["dev-001"] = service._catalog.devices.get("dev-001") or Mock()
    service._catalog.devices["dev-001"].entity_id = "dev-001"
    service._catalog.devices["dev-001"].name = "existing-device"
    service._catalog.devices["dev-001"].entity_type = "DEVICE"
    service._catalog.devices["dev-001"].entity_data = {}

    d_new = _fake_device(name="new-device", eid="dev-002")

    service._client.get_devices = AsyncMock(
        return_value=PageData(data=[d1, d_new], totalPages=1, totalElements=2, hasNext=False)
    )
    service._client.get_assets = AsyncMock(
        return_value=PageData(data=[], totalPages=1, totalElements=0, hasNext=False)
    )
    service._client.get_relations = AsyncMock(return_value=[])
    service._client.get_attributes = AsyncMock(return_value=[])
    service._client.get_timeseries_keys = AsyncMock(return_value=["temperature"])

    catalog = await service.incremental_sync()

    assert "dev-002" in catalog.devices
    assert catalog.sync_count == 1


@pytest.mark.unit
@pytest.mark.asyncio
async def test_discover_devices(service):
    d1 = _fake_device(name="alpha")
    d2 = _fake_device(name="beta")
    service._client.get_devices = AsyncMock(
        return_value=PageData(data=[d1, d2], totalPages=1, totalElements=2, hasNext=False)
    )

    devices = await service.discover_devices()

    assert len(devices) == 2
    assert devices[0].name == "alpha"
    assert d1.id.id in service.catalog.devices


@pytest.mark.unit
@pytest.mark.asyncio
async def test_discover_attributes_keeps_scalar_scope_without_dropping_others(service):
    """A scope returning bare JSON scalars (which used to raise and drop the
    whole scope) must now be conserved; other scopes/entities are unaffected."""
    scalar_attrs = [
        AttributeEntry(key="message", value="hello"),
        AttributeEntry(key="count", value=3),
    ]
    wrapper_attrs = [
        AttributeEntry(key="temperature", value=25, lastUpdateTs=123456789),
    ]
    service._client.get_attributes = AsyncMock(
        side_effect=[
            scalar_attrs,  # SERVER_SCOPE
            wrapper_attrs,  # SHARED_SCOPE
            [],  # CLIENT_SCOPE
        ]
    )

    attrs = await service.discover_attributes("DEVICE", "dev-1")

    assert attrs.get("message") == "hello"
    assert attrs.get("count") == 3
    assert attrs.get("temperature") == 25
    assert len(attrs) == 3


@pytest.mark.unit
@pytest.mark.asyncio
async def test_discover_relations(service):
    service._catalog.devices["dev-a"] = service._catalog.devices.get("dev-a") or Mock()
    service._catalog.devices["dev-a"].entity_id = "dev-a"
    service._catalog.devices["dev-b"] = service._catalog.devices.get("dev-b") or Mock()
    service._catalog.devices["dev-b"].entity_id = "dev-b"

    rel = _fake_relation("dev-a", "dev-b")
    service._client.get_relations = AsyncMock(
        side_effect=[
            [rel.model_dump(by_alias=True)],
            [],
        ]
    )

    relations = await service.discover_relations()

    assert len(relations) == 1
    assert relations[0].type == "Contains"


@pytest.mark.unit
@pytest.mark.asyncio
async def test_validate_topology(service):
    issues = await service.validate_topology()

    assert any("empty" in i.lower() for i in issues)

    service._catalog.devices["dev-ok"] = service._catalog.devices.get("dev-ok") or Mock()
    service._catalog.devices["dev-ok"].name = "good-device"
    service._catalog.device_profiles.append(DeviceProfile(name="prof", type="DEFAULT"))

    service._catalog.relations.append(
        Relation(
            from_=EntityId(entityType="DEVICE", id="a"),
            to=EntityId(entityType="DEVICE", id="b"),
            type="connects",
        )
    )
    service._catalog.relations.append(
        Relation(
            from_=EntityId(entityType="DEVICE", id="a"),
            to=EntityId(entityType="DEVICE", id="b"),
            type="connects",
        )
    )

    issues = await service.validate_topology()
    assert any("duplicate" in i.lower() for i in issues)


@pytest.mark.unit
def test_inclusion_rules():
    rules = InclusionRules(
        tenant_ids={"tenant-1"},
        device_name_patterns=[re.compile(r"temp-.*")],
    )

    included = _fake_device(name="temp-sensor", eid="d1")
    included.tenantId = EntityId(entityType="TENANT", id="tenant-1")
    excluded = _fake_device(name="pressure-sensor", eid="d2")
    excluded.tenantId = EntityId(entityType="TENANT", id="tenant-2")

    assert rules.include_device(included)
    assert not rules.include_device(excluded)


@pytest.mark.unit
def test_exclusion_rules():
    rules = InclusionRules(
        customer_ids={"cust-1"},
    )

    dev_not_in_customer = _fake_device(name="no-customer", eid="d1")
    dev_not_in_customer.customerId = EntityId(entityType="CUSTOMER", id="cust-2")

    assert not rules.include_device(dev_not_in_customer)

    dev_with_no_customer = _fake_device(name="no-customer", eid="d2")
    assert rules.include_device(dev_with_no_customer)
