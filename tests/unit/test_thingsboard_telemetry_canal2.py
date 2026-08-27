"""Unit tests for Canal 2 (SQL read-only) telemetry extraction + recovery window.

These tests do NOT require a real ThingsBoard or PostgreSQL: psycopg2.connect
and the cursor are mocked. They assert:
  * the SQL read channel is selected when configured and falls back to the API
    when unavailable (no silent Canal-1 usage);
  * statement_timeout / connect_timeout / sslmode are taken from settings;
  * the read-only session is requested on the SQL connection;
  * TB_INGEST_RECOVERY_WINDOW_HOURS overlaps the checkpoint window without
    breaking idempotence.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from trendx.config import settings
from trendx.services.ingestion import IngestionService

# --------------------------------------------------------------------------
# Canal 2 — TelemetryReader (SQL read-only channel)
# --------------------------------------------------------------------------


@pytest.fixture
def reader():
    from trendx.thingsboard.telemetry import TelemetryReader

    return TelemetryReader(page_size=100, max_window_hours=24)


def _fake_conn(rows):
    cur = MagicMock()
    cur.fetchall.return_value = rows
    cur.execute.side_effect = lambda *a, **k: None
    conn = MagicMock()
    conn.cursor.return_value = cur
    return conn


def _patch_dsn(monkeypatch, value):
    # tb_db_readonly_dsn is a method on the Settings *class*; override it on the
    # class (not the instance field) so both `settings` and module references see it.
    monkeypatch.setattr(type(settings), "tb_db_readonly_dsn", staticmethod(lambda: value))


@pytest.mark.unit
@pytest.mark.asyncio
async def test_sql_channel_unavailable_falls_back_to_api(reader, monkeypatch):
    # No DSN -> SQL channel disabled, API path is used.
    _patch_dsn(monkeypatch, None)
    reader._client = MagicMock()
    reader._client.get_timeseries = AsyncMock(return_value={"temp": [MagicMock(ts=1, value=2.0)]})
    data = await reader.read_historical("DEVICE", "dev-1", ["temp"], 0, 1000)
    assert "temp" in data
    assert reader._sql_available is False


@pytest.mark.unit
@pytest.mark.asyncio
async def test_sql_channel_uses_settings_for_connect_args(reader, monkeypatch):
    _patch_dsn(monkeypatch, "postgresql://reader:pass@tb-host:5432/thingsboard")
    monkeypatch.setattr("trendx.thingsboard.telemetry.settings.tb_db_connect_timeout", 15)
    monkeypatch.setattr("trendx.thingsboard.telemetry.settings.tb_db_sslmode", "require")
    monkeypatch.setattr("trendx.thingsboard.telemetry.settings.tb_db_statement_timeout", 30000)

    fake = _fake_conn([])
    with patch("psycopg2.connect", return_value=fake) as connect_mock:
        ok = await reader._check_sql_channel()
        assert ok is True
        _, kwargs = connect_mock.call_args
        assert kwargs["connect_timeout"] == 15
        assert kwargs["sslmode"] == "require"


@pytest.mark.unit
@pytest.mark.asyncio
async def test_sql_read_sets_readonly_and_statement_timeout(reader, monkeypatch):
    _patch_dsn(monkeypatch, "postgresql://reader:pass@tb-host:5432/thingsboard")
    monkeypatch.setattr("trendx.thingsboard.telemetry.settings.tb_db_connect_timeout", 10)
    monkeypatch.setattr("trendx.thingsboard.telemetry.settings.tb_db_sslmode", "prefer")
    monkeypatch.setattr("trendx.thingsboard.telemetry.settings.tb_db_statement_timeout", 60000)

    fake = _fake_conn([("temp", 1_700_000_000_000, None, None, 22.5, 22.5)])
    with patch("psycopg2.connect", return_value=fake):
        data = await reader.read_historical(
            "DEVICE", "dev-1", ["temp"], 1_700_000_000_000, 1_700_000_001_000
        )
    # read-only session requested + statement_timeout applied
    fake.set_session.assert_called_once_with(readonly=True, autocommit=True)
    set_calls = [
        c.args[0]
        for c in fake.cursor.return_value.method_calls
        if c.args and isinstance(c.args[0], str) and "statement_timeout" in c.args[0]
    ]
    assert set_calls, "statement_timeout must be SET on the SQL session"
    # data normalized from dbl_v
    assert data["temp"][0].value == 22.5


@pytest.mark.unit
@pytest.mark.asyncio
async def test_sql_read_non_device_returns_none(reader, monkeypatch):
    _patch_dsn(monkeypatch, "postgresql://reader:pass@tb-host:5432/thingsboard")
    monkeypatch.setattr("trendx.thingsboard.telemetry.settings.tb_db_connect_timeout", 10)
    monkeypatch.setattr("trendx.thingsboard.telemetry.settings.tb_db_sslmode", "prefer")
    monkeypatch.setattr("trendx.thingsboard.telemetry.settings.tb_db_statement_timeout", 60000)
    fake = _fake_conn([])
    with patch("psycopg2.connect", return_value=fake):
        result = await reader._read_via_sql("ASSET", "asset-1", ["temp"], 0, 1000)
    assert result is None


# --------------------------------------------------------------------------
# Recovery window (TB_INGEST_RECOVERY_WINDOW_HOURS)
# --------------------------------------------------------------------------


@pytest.mark.unit
def test_recovery_window_overlaps_checkpoint():
    svc = IngestionService(
        database_manager=MagicMock(),
        telemetry_reader=MagicMock(),
        batch_size=100,
        window_hours=24,
    )
    cp = datetime(2024, 6, 1, 12, 0, 0, tzinfo=UTC)
    start, _ = svc._build_recovery_window(cp)
    assert start == cp - timedelta(hours=1)


@pytest.mark.unit
def test_incremental_uses_overlap_window(monkeypatch):
    monkeypatch.setattr("trendx.services.ingestion.settings.tb_ingest_recovery_window_hours", 1)
    # Recent checkpoint -> small number of 24h windows, fast test.
    cp = datetime(2026, 8, 13, 16, 0, 0, tzinfo=UTC)

    svc = IngestionService(
        database_manager=MagicMock(),
        telemetry_reader=MagicMock(),
        batch_size=100,
        window_hours=24,
    )
    # Isolate the disk guard: this unit test validates the overlap recovery
    # window, not the production disk-capacity fail-closed guard. The guard
    # itself remains active and unchanged in src/trendx/services/ingestion.py.
    svc.ensure_disk_available = lambda *a, **k: None  # type: ignore[method-assign]
    mock_session = MagicMock()
    mock_session.__enter__ = MagicMock(return_value=mock_session)
    mock_session.__exit__ = MagicMock(return_value=None)
    mock_repo = MagicMock()
    mock_repo.get_watermark.return_value = {
        "watermark_ts": cp,
        "records_processed": 10,
        "last_batch_id": "b1",
    }
    svc._db.get_session.return_value = mock_session
    repo_patch = patch("trendx.services.ingestion.CheckpointRepository", return_value=mock_repo)
    repo_patch.start()

    # Keep the REAL ingest_device_metric (so the checkpoint-advance branch
    # executes). Only the SQL read and the store are stubbed.
    captured: dict[str, datetime] = {}

    async def spy_read(*a, **k):
        # Capture the FIRST read window only. The recovery window must start
        # exactly at (checkpoint - TB_INGEST_RECOVERY_WINDOW_HOURS); later
        # windows advance with `now` (24h window size), so letting a subsequent
        # call overwrite `captured` would make the assertion depend on the real
        # wall-clock date. Pinning the first call keeps the test deterministic
        # and verifiable against the overlap-recovery contract.
        captured.setdefault("start", k.get("start_ts"))
        captured.setdefault("end", k.get("end_ts"))
        # one telemetry point so _store_telemetry path is exercised
        return {"temp": [type("P", (), {"ts": 1_700_000_000_000, "value": 22.5})()]}

    svc._reader.read_historical = spy_read  # type: ignore[assignment]
    svc._store_telemetry = lambda *a, **k: 7  # type: ignore[assignment]
    svc._discover_devices = lambda: [  # type: ignore[assignment]
        {
            "entity_id": "dev-1",
            "name": "d",
            "entity_type": "DEVICE",
            "metrics": [{"key": "temp"}],
        }
    ]

    try:
        import asyncio

        asyncio.get_event_loop().run_until_complete(svc.run_incremental_ingest())
    finally:
        repo_patch.stop()

    # read_historical receives start_ts in epoch milliseconds
    expected_start_ms = int((cp - timedelta(hours=1)).timestamp() * 1000)
    assert captured["start"] == expected_start_ms
    # checkpoint advanced only after a successful (positive) storage run
    mock_repo.upsert_watermark.assert_called_once()
