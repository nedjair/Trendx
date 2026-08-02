from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pandas as pd
import pytest

from trendx.services.ingestion import IngestionService


@pytest.fixture
def service():
    svc = IngestionService(
        database_manager=MagicMock(),
        telemetry_reader=MagicMock(),
        batch_size=100,
        window_hours=24,
    )
    svc._total_processed = 0
    svc._total_errors = 0
    return svc


@pytest.mark.unit
def test_get_checkpoint(service):
    mock_session = MagicMock()
    mock_session.__enter__ = MagicMock(return_value=mock_session)
    mock_session.__exit__ = MagicMock(return_value=None)
    mock_repo = MagicMock()
    mock_repo.get_watermark.return_value = MagicMock(
        watermark_ts=datetime(2024, 1, 15, tzinfo=timezone.utc)
    )
    service._db.get_session.return_value = mock_session

    with patch("trendx.services.ingestion.CheckpointRepository", return_value=mock_repo):
        cp = service._get_checkpoint("dev-001", "temperature")

    assert cp is not None
    assert cp.year == 2024


@pytest.mark.unit
def test_update_checkpoint(service):
    mock_session = MagicMock()
    mock_session.__enter__ = MagicMock(return_value=mock_session)
    mock_session.__exit__ = MagicMock(return_value=None)
    mock_repo = MagicMock()
    service._db.get_session.return_value = mock_session

    with patch("trendx.services.ingestion.CheckpointRepository", return_value=mock_repo):
        service._update_checkpoint(
            "dev-001", "temperature",
            datetime(2024, 1, 15, tzinfo=timezone.utc),
            records_count=100,
        )

    mock_repo.upsert_watermark.assert_called_once()


@pytest.mark.unit
def test_validate_data(service):
    valid = pd.DataFrame({"ts": [1700000000000, 1700000001000], "value": [22.5, 23.0]})
    result = service._validate_data(valid)
    assert not result.empty
    assert "ts" in result.columns

    empty = pd.DataFrame()
    assert service._validate_data(empty).empty

    missing_cols = pd.DataFrame({"wrong": [1, 2]})
    assert service._validate_data(missing_cols).empty


@pytest.mark.unit
def test_error_isolation(service):
    service._total_errors = 0
    service._write_dead_letter = MagicMock()

    async def failing_read(*args, **kwargs):
        msg = "Connection failed"
        raise ConnectionError(msg)

    service._reader.read_historical = failing_read

    with pytest.raises(ConnectionError):
        service._reader.read_historical("DEVICE", "dev-001", ["temp"], 0, 1000)


@pytest.mark.unit
def test_dead_letter_queue(service):
    mock_engine = MagicMock()
    mock_conn = MagicMock()
    mock_engine.begin.return_value.__enter__ = MagicMock(return_value=mock_conn)
    mock_engine.begin.return_value.__exit__ = MagicMock(return_value=None)
    service._db.get_engine.return_value = mock_engine

    service._write_dead_letter("dev-001", "temperature", '{"bad": "data"}', "Parse error")
    mock_conn.execute.assert_called_once()


@pytest.mark.unit
def test_store_telemetry(service):
    mock_engine = MagicMock()
    mock_conn = MagicMock()
    mock_result = MagicMock()
    mock_result.rowcount = 5
    mock_conn.execute.return_value = mock_result
    mock_engine.begin.return_value.__enter__ = MagicMock(return_value=mock_conn)
    mock_engine.begin.return_value.__exit__ = MagicMock(return_value=None)
    service._db.get_engine.return_value = mock_engine

    df = pd.DataFrame({
        "ts": [datetime(2024, 1, 1, tzinfo=timezone.utc), datetime(2024, 1, 1, 1, tzinfo=timezone.utc)],
        "value": [22.5, 23.0],
    })
    count = service._store_telemetry(df, "dev-001", "temperature")
    assert count > 0


@pytest.mark.unit
def test_store_telemetry_empty(service):
    df = pd.DataFrame()
    count = service._store_telemetry(df, "dev-001", "temp")
    assert count == 0


@pytest.mark.unit
def test_build_time_windows(service):
    start = datetime(2024, 1, 1, tzinfo=timezone.utc)
    end = datetime(2024, 1, 3, tzinfo=timezone.utc)
    windows = service._build_time_windows(start, end)
    assert len(windows) >= 2
    for wstart, wend in windows:
        assert wstart < wend


@pytest.mark.unit
def test_lit_function():
    from datetime import datetime

    assert IngestionService._lit(None) == "NULL"
    assert IngestionService._lit(True) == "TRUE"
    assert IngestionService._lit(42) == "42"
    assert IngestionService._lit(3.14) == "3.14"
    assert IngestionService._lit("hello") == "'hello'"
    dt = datetime(2024, 1, 1, 12, 0, 0)
    result = IngestionService._lit(dt)
    assert result.endswith("::timestamptz")
