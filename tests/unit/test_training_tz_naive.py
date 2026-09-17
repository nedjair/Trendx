"""Correctif tz-naive Prophet pour TrainingService._prepare_data.

La colonne ds sortait tz-aware (timestamptz -> pandas), ce que Prophet refuse
(« Column ds has timezone specified »). Le correctif normalise ds en tz-naive
en conservant l'instant UTC, après resampling/interpolation.
"""

from __future__ import annotations

from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock

import pandas as pd
from trendx.forecasting.prophet import ProphetModel
from trendx.preprocessing.quality import DataQualityService
from trendx.preprocessing.resampling import Resampler
from trendx.services.training import TrainingService


def _svc() -> TrainingService:
    return TrainingService(
        mlflow_tracker=MagicMock(),
        model_registry=MagicMock(),
        data_quality=DataQualityService(),
        resampler=Resampler(),
        model_selector=MagicMock(),
    )


def _aware_series(n: int = 6) -> pd.DataFrame:
    base = datetime(2026, 9, 16, 22, 0, tzinfo=UTC)
    return pd.DataFrame(
        {
            "ts": [base + timedelta(hours=i) for i in range(n)],
            "value": [22.0 + 0.1 * i for i in range(n)],
        }
    )


def test_a_aware_utc_becomes_naive_same_wall_time() -> None:
    out = _svc()._prepare_data(_aware_series(6))
    assert out["ds"].dt.tz is None
    assert list(out["ds"].dt.strftime("%Y-%m-%d %H:%M")) == [
        "2026-09-16 22:00",
        "2026-09-16 23:00",
        "2026-09-17 00:00",
        "2026-09-17 01:00",
        "2026-09-17 02:00",
        "2026-09-17 03:00",
    ]


def test_b_series_preserves_rows_order_values_instants() -> None:
    df = _aware_series(8)
    out = _svc()._prepare_data(df)
    assert len(out) == 8
    assert list(out["y"]) == list(df["value"])
    assert (out["ds"].diff().dropna() == pd.Timedelta(hours=1)).all()
    assert out["ds"].iloc[0] == pd.Timestamp("2026-09-16 22:00")


def test_c_hourly_resampling_identical() -> None:
    df = _aware_series(10)
    out = _svc()._prepare_data(df)
    assert len(out) == 10
    assert (out["ds"].diff().dropna() == pd.Timedelta(hours=1)).all()


def test_d_prophet_accepts_final_frame() -> None:
    out = _svc()._prepare_data(_aware_series(10))
    assert out["ds"].dt.tz is None
    model = ProphetModel(
        yearly_seasonality=False, weekly_seasonality=False, daily_seasonality=False
    )
    model.fit(out[["ds", "y"]])
    forecast = model.predict(horizon=2)
    assert len(forecast.values) == 2


def test_e_exclusion_still_33_to_13(monkeypatch) -> None:
    from trendx.services import training as training_mod

    excluded = tuple(f"excluded-{i:02d}" for i in range(11))
    rows: list[dict] = []
    base = datetime(2026, 9, 16, 22, 0, tzinfo=UTC)
    for i in range(20):
        rows.append(
            {
                "ts": base - timedelta(hours=30 + i),
                "entity_id": "e",
                "metric_key": "m",
                "dbl_v": 1.0,
                "ingestion_id": excluded[i % 11],
            }
        )
    for i in range(13):
        rows.append(
            {
                "ts": base + timedelta(hours=i),
                "entity_id": "e",
                "metric_key": "m",
                "dbl_v": 2.0,
                "ingestion_id": "witness-batch",
            }
        )

    class _FakeResult:
        def __init__(self, rows: list[tuple]) -> None:
            self._rows = rows

        def fetchall(self) -> list[tuple]:
            return self._rows

    class _FakeConn:
        def execute(self, stmt: object, params: dict) -> _FakeResult:
            excluded_set = set(params.get("excluded", []) or [])
            kept = [
                (r["ts"], r["dbl_v"])
                for r in rows
                if r["dbl_v"] is not None
                and (r["ingestion_id"] is None or r["ingestion_id"] not in excluded_set)
            ]
            return _FakeResult(sorted(kept))

    class _FakeEngine:
        @contextmanager
        def connect(self):  # type: ignore[no-untyped-def]
            yield _FakeConn()

    monkeypatch.setattr(
        training_mod.settings,
        "training_excluded_ingestion_ids",
        ",".join(excluded),
    )
    monkeypatch.setattr(training_mod.db_manager, "get_engine", lambda _name: _FakeEngine())
    svc = _svc()
    fetched = svc._fetch_training_data("e", "m", lookback_days=90)
    assert len(fetched) == 13
    prepared = svc._prepare_data(fetched)
    assert len(prepared) == 13
    assert prepared["ds"].dt.tz is None


def test_f_witness_values_timestamps_exact() -> None:
    df = _aware_series(4)
    out = _svc()._prepare_data(df)
    assert list(out["y"]) == [22.0, 22.1, 22.2, 22.3]
    assert out["ds"].iloc[0] == pd.Timestamp("2026-09-16 22:00")
    assert out["ds"].iloc[-1] == pd.Timestamp("2026-09-17 01:00")
