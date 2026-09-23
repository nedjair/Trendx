"""Unit tests for ingestion TB-ID mapping fix (BE storage vs TB read).

Covers: positive mapping (reader TB UUID, store BE UUID), per-BE metric
filtering, empty catalogue, invalid item_id fail-closed, and the exact
production case (BE 7c5d... / TB 4fe36170... / temperature).
"""

from __future__ import annotations

import asyncio
import uuid
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from trendx.services.ingestion import IngestionService

BE_ID = "7c5d0442-6d93-5f05-be3e-2fc803d4c1b0"
TB_ID = "4fe36170-418a-11f1-8213-93759e228251"
TENANT = "89d43810-9b9e-11f0-8e3f-c909dc64d424"


def _run(coro):
    """Run coroutine while preserving a current event loop for legacy tests.

    asyncio.run() clears the thread-local loop, breaking older tests using
    asyncio.get_event_loop(). Recreate one afterwards (test-only helper).
    """
    result = asyncio.run(coro)
    try:
        asyncio.set_event_loop(asyncio.new_event_loop())
    except RuntimeError:
        pass
    return result


def _be(be_id: str, name: str = "BE-test-temperature") -> SimpleNamespace:
    return SimpleNamespace(id=uuid.UUID(be_id), name=name)


def _metric(
    met_id: str,
    be_id: str,
    item_id: object,
    item_name: str = "temperature",
) -> SimpleNamespace:
    return SimpleNamespace(
        id=uuid.UUID(met_id),
        business_entity_id=uuid.UUID(be_id),
        item_id=item_id,
        item_name=item_name,
    )


def _svc_with_catalog(entities: list, metrics: list) -> IngestionService:
    svc = IngestionService(
        database_manager=MagicMock(),
        telemetry_reader=MagicMock(),
        batch_size=100,
        window_hours=24,
    )
    mock_session = MagicMock()
    mock_session.__enter__ = MagicMock(return_value=mock_session)
    mock_session.__exit__ = MagicMock(return_value=None)
    svc._db.get_session.return_value = mock_session

    be_repo = MagicMock()
    be_repo.list.return_value = entities
    met_repo = MagicMock()
    met_repo.list.return_value = metrics

    patcher = patch(
        "trendx.services.ingestion.BusinessEntityRepository",
        return_value=be_repo,
    )
    patcher2 = patch(
        "trendx.services.ingestion.MetricDefinitionRepository",
        return_value=met_repo,
    )
    patcher.start()
    patcher2.start()
    # Keep patchers alive for the test duration via fixture finalizer is
    # overkill here; tests are synchronous for discovery so stop after call
    # is handled by explicit stop in each test.
    svc._test_patchers = (patcher, patcher2)  # type: ignore[attr-defined]
    return svc


def _stop_patchers(svc: IngestionService) -> None:
    for p in getattr(svc, "_test_patchers", ()):
        p.stop()


@pytest.mark.unit
def test_discover_positive_mapping() -> None:
    svc = _svc_with_catalog(
        [_be(BE_ID)],
        [
            _metric(
                "7a585cdc-6b59-51a9-9f85-5bc6802e5c58",
                BE_ID,
                uuid.UUID(TB_ID),
            )
        ],
    )
    try:
        devs = svc._discover_devices()
    finally:
        _stop_patchers(svc)
    assert len(devs) == 1
    assert devs[0]["entity_id"] == BE_ID
    assert devs[0]["entity_type"] == "DEVICE"
    assert len(devs[0]["metrics"]) == 1
    assert devs[0]["metrics"][0]["key"] == "temperature"
    assert devs[0]["metrics"][0]["tb_entity_id"] == TB_ID


@pytest.mark.unit
def test_reader_uses_tb_id_store_uses_be_id() -> None:
    svc = IngestionService(
        database_manager=MagicMock(),
        telemetry_reader=MagicMock(),
        batch_size=100,
        window_hours=24,
    )
    seen: dict = {}

    async def fake_batch(
        *,
        entity_id: str,
        metric_key: str,
        start_dt,
        end_dt,
        ingestion_id=None,
        tb_entity_id=None,
    ) -> int:
        seen["read_entity"] = tb_entity_id or entity_id
        seen["store_entity"] = entity_id
        seen["key"] = metric_key
        return 3

    svc._ingest_metric_batch = fake_batch  # type: ignore[method-assign]
    svc.ensure_disk_available = MagicMock()
    svc._update_checkpoint = MagicMock()
    from datetime import UTC, datetime

    start = datetime(2026, 9, 15, 22, 0, tzinfo=UTC)
    end = datetime(2026, 9, 16, 7, 0, tzinfo=UTC)
    res = _run(
        svc.ingest_device_metric(
            entity_id=BE_ID,
            metric_key="temperature",
            start_ts=start,
            end_ts=end,
            tb_entity_id=TB_ID,
        )
    )
    assert seen["read_entity"] == TB_ID
    assert seen["store_entity"] == BE_ID
    assert res["entity_id"] == BE_ID
    assert res["total_stored"] >= 0


@pytest.mark.unit
def test_filtering_per_business_entity() -> None:
    be_a = "11111111-1111-4111-8111-111111111111"
    be_b = "22222222-2222-4222-8222-222222222222"
    svc = _svc_with_catalog(
        [_be(be_a, "BE-A"), _be(be_b, "BE-B")],
        [
            _metric(
                "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
                be_a,
                uuid.UUID("aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee"),
                "metric-a",
            ),
            _metric(
                "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
                be_b,
                uuid.UUID("ffffffff-1111-4aaa-8aaa-aaaaaaaaaaaa"),
                "metric-b",
            ),
        ],
    )
    try:
        devs = svc._discover_devices()
    finally:
        _stop_patchers(svc)
    by_id = {d["entity_id"]: d for d in devs}
    assert set(by_id) == {be_a, be_b}
    assert [m["key"] for m in by_id[be_a]["metrics"]] == ["metric-a"]
    assert [m["key"] for m in by_id[be_b]["metrics"]] == ["metric-b"]


@pytest.mark.unit
def test_empty_catalogue_no_tb_call() -> None:
    svc = _svc_with_catalog([], [])
    svc._reader.read_historical = AsyncMock()
    try:
        devs = svc._discover_devices()
        assert devs == []
        res = _run(svc.run_incremental_ingest())
    finally:
        _stop_patchers(svc)
    assert res == []
    svc._reader.read_historical.assert_not_called()


@pytest.mark.unit
def test_invalid_item_id_fail_closed() -> None:
    svc = _svc_with_catalog(
        [_be(BE_ID)],
        [_metric("7a585cdc-6b59-51a9-9f85-5bc6802e5c58", BE_ID, None)],
    )
    svc._reader.read_historical = AsyncMock()
    try:
        devs = svc._discover_devices()
        assert devs == []
    finally:
        _stop_patchers(svc)
    svc._reader.read_historical.assert_not_called()


@pytest.mark.unit
def test_real_case_be_tb_mapping() -> None:
    """Exact production case: TB read 4fe..., store 7c5d...."""
    svc = _svc_with_catalog(
        [_be(BE_ID, "BE-test-temperature")],
        [
            _metric(
                "7a585cdc-6b59-51a9-9f85-5bc6802e5c58",
                BE_ID,
                uuid.UUID(TB_ID),
                "temperature",
            )
        ],
    )
    calls: dict = {}

    async def spy_batch(
        self_, *, entity_id, metric_key, start_dt, end_dt, ingestion_id=None, tb_entity_id=None
    ):
        calls["tb"] = tb_entity_id or entity_id
        calls["store"] = entity_id
        # Do not hit TB: simulate empty batch without network.
        return 0

    try:
        devs = svc._discover_devices()
        assert len(devs) == 1
        metric = devs[0]["metrics"][0]
        assert metric["tb_entity_id"] == TB_ID
        with patch.object(IngestionService, "_ingest_metric_batch", spy_batch):
            svc.ensure_disk_available = MagicMock()
            from datetime import UTC, datetime

            _run(
                svc.ingest_device_metric(
                    entity_id=devs[0]["entity_id"],
                    metric_key=metric["key"],
                    start_ts=datetime(2026, 9, 15, 22, 0, tzinfo=UTC),
                    end_ts=datetime(2026, 9, 16, 7, 0, tzinfo=UTC),
                    tb_entity_id=metric["tb_entity_id"],
                )
            )
    finally:
        _stop_patchers(svc)
    assert calls["tb"] == TB_ID
    assert calls["store"] == BE_ID
