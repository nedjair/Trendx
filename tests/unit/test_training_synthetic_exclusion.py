"""Isolement Train des 20 lignes synthétiques (11 ingestion_id).

A : 20 synthétiques -> 0 avec les 11 IDs exclus.
B : témoin valide (autre ingestion_id) conservé.
C : config vide -> aucune exclusion.
D : préfixe similaire non exclu (égalité stricte).
E : plusieurs IDs simultanés.
F : filtres existants inchangés (entity/metric/fenêtre/dbl_v NOT NULL).
G : niveau TrainingService._fetch_training_data (fake engine, pas d'utilitaire seul).
"""

from __future__ import annotations

from contextlib import contextmanager
from datetime import UTC, datetime
from unittest.mock import MagicMock

import pandas as pd
from trendx.services import training as training_mod
from trendx.services.training import (
    TrainingService,
    build_training_data_stmt,
    parse_excluded_ingestion_ids,
)

ENTITY = "7c5d0442-6d93-5f05-be3e-2fc803d4c1b0"
METRIC = "temperature"

SYNTHETIC_IDS = (
    "1c74878e-2c6",
    "20cb60c9-13d",
    "35b45778-125",
    "5b55fd02-043",
    "6123a926-d02",
    "8e88d1ba-0e0",
    "95b8debc-b40",
    "bd0c4482-dad",
    "cae64d2c-18c",
    "e6c0b750-6ba",
    "f90f3866-116",
)

# 20 lignes de référence : 10 premier batch + 10 unitaires (gate provenance).
_SYNTHETIC_ROWS: list[dict] = []
_ts = [
    "2026-09-15T22:00",
    "2026-09-15T23:00",
    "2026-09-16T00:00",
    "2026-09-16T01:00",
    "2026-09-16T02:00",
    "2026-09-16T03:00",
    "2026-09-16T04:00",
    "2026-09-16T05:00",
    "2026-09-16T06:00",
    "2026-09-16T07:00",
    "2026-09-16T12:00",
    "2026-09-16T13:00",
    "2026-09-16T14:00",
    "2026-09-16T15:00",
    "2026-09-16T16:00",
    "2026-09-16T17:00",
    "2026-09-16T18:00",
    "2026-09-16T19:00",
    "2026-09-16T20:00",
    "2026-09-16T21:00",
]
_vals = [21.0, 21.1, 21.2, 21.3, 21.4, 21.5, 21.6, 21.7, 21.8, 21.9] * 2
_ing_ids = ["5b55fd02-043"] * 10 + [
    "20cb60c9-13d",
    "95b8debc-b40",
    "8e88d1ba-0e0",
    "cae64d2c-18c",
    "35b45778-125",
    "1c74878e-2c6",
    "e6c0b750-6ba",
    "f90f3866-116",
    "6123a926-d02",
    "bd0c4482-dad",
]
for ts_raw, val, batch in zip(_ts, _vals, _ing_ids, strict=True):
    _SYNTHETIC_ROWS.append(
        {
            "ts": datetime.fromisoformat(ts_raw + ":00+00:00"),
            "entity_id": ENTITY,
            "metric_key": METRIC,
            "dbl_v": val,
            "ingestion_id": batch,
        }
    )


class _FakeResult:
    def __init__(self, rows: list[tuple]) -> None:
        self._rows = rows

    def fetchall(self) -> list[tuple]:
        return self._rows


class _FakeConn:
    """Filtre une table mémoire avec la même sémantique que la requête Train."""

    def __init__(self, table: list[dict]) -> None:
        self._table = table
        self.seen_params: dict = {}

    def execute(self, stmt: object, params: dict) -> _FakeResult:
        self.seen_params = dict(params)
        excluded = set(params.get("excluded", []) or [])
        out: list[tuple] = []
        for row in self._table:
            if row["entity_id"] != params["eid"]:
                continue
            if row["metric_key"] != params["key"]:
                continue
            if not (params["start"] <= row["ts"] < params["end"]):
                continue
            if row["dbl_v"] is None:
                continue
            ing = row.get("ingestion_id")
            if excluded and ing is not None and ing in excluded:
                continue
            out.append((row["ts"], row["dbl_v"]))
        out.sort(key=lambda r: r[0])
        return _FakeResult(out)


class _FakeEngine:
    def __init__(self, table: list[dict]) -> None:
        self._table = table
        self.last_conn: _FakeConn | None = None

    @contextmanager
    def connect(self):  # type: ignore[no-untyped-def]
        conn = _FakeConn(self._table)
        self.last_conn = conn
        yield conn


def _run_fetch(table: list[dict], excluded_csv: str, monkeypatch) -> pd.DataFrame:  # type: ignore[no-untyped-def]
    monkeypatch.setattr(training_mod.settings, "training_excluded_ingestion_ids", excluded_csv)
    engine = _FakeEngine(table)
    monkeypatch.setattr(training_mod.db_manager, "get_engine", lambda _name: engine)
    svc = TrainingService(
        mlflow_tracker=MagicMock(),
        model_registry=MagicMock(),
        data_quality=MagicMock(),
        resampler=MagicMock(),
        model_selector=MagicMock(),
    )
    df = svc._fetch_training_data(ENTITY, METRIC, lookback_days=90)
    return df


def _valid_row(ingestion_id: str = "valid-batch-001") -> dict:
    return {
        "ts": datetime(2026, 9, 16, 10, 0, tzinfo=UTC),
        "entity_id": ENTITY,
        "metric_key": METRIC,
        "dbl_v": 22.5,
        "ingestion_id": ingestion_id,
    }


def test_a_synthetic_20_excluded_to_0(monkeypatch) -> None:
    df = _run_fetch(list(_SYNTHETIC_ROWS), ",".join(SYNTHETIC_IDS), monkeypatch)
    assert len(df) == 0


def test_b_valid_witness_kept(monkeypatch) -> None:
    table = [*_SYNTHETIC_ROWS, _valid_row("valid-batch-001")]
    df = _run_fetch(table, ",".join(SYNTHETIC_IDS), monkeypatch)
    assert len(df) == 1
    assert float(df["value"].iloc[0]) == 22.5


def test_c_empty_config_no_exclusion(monkeypatch) -> None:
    df = _run_fetch(list(_SYNTHETIC_ROWS), "", monkeypatch)
    assert len(df) == 20
    assert parse_excluded_ingestion_ids("") == ()
    assert parse_excluded_ingestion_ids(None) == ()


def test_d_similar_prefix_not_excluded(monkeypatch) -> None:
    table = [_valid_row("5b55fd02-043-extra")]
    df = _run_fetch(table, ",".join(SYNTHETIC_IDS), monkeypatch)
    assert len(df) == 1


def test_e_multiple_exclusions_simultaneous(monkeypatch) -> None:
    table = [_valid_row("5b55fd02-043"), _valid_row("bd0c4482-dad"), _valid_row("keep-me")]
    df = _run_fetch(table, "5b55fd02-043, bd0c4482-dad", monkeypatch)
    assert len(df) == 1
    # Robustesse parsing : whitespace + doublons + vides.
    assert parse_excluded_ingestion_ids(" a , ,a,b , b ") == ("a", "b")


def test_f_existing_filters_unchanged(monkeypatch) -> None:
    other_entity = dict(_valid_row("keep-1"))
    other_entity["entity_id"] = "00000000-0000-0000-0000-000000000000"
    other_metric = dict(_valid_row("keep-2"))
    other_metric["metric_key"] = "humidity"
    null_val = dict(_valid_row("keep-3"))
    null_val["dbl_v"] = None
    out_of_window = dict(_valid_row("keep-4"))
    out_of_window["ts"] = datetime(2020, 1, 1, tzinfo=UTC)
    table = [other_entity, other_metric, null_val, out_of_window, _valid_row("keep-5")]
    df = _run_fetch(table, "", monkeypatch)
    assert len(df) == 1
    assert float(df["value"].iloc[0]) == 22.5
    # La clause SQL conserve les 4 filtres historiques.
    stmt_empty = str(build_training_data_stmt(()))
    assert "entity_id = :eid" in stmt_empty
    assert "metric_key = :key" in stmt_empty
    assert "ts >= :start" in stmt_empty
    assert "ts < :end" in stmt_empty
    assert "dbl_v IS NOT NULL" in stmt_empty
    assert "ingestion_id" not in stmt_empty
    stmt_full = str(build_training_data_stmt(("x",)))
    assert "ingestion_id NOT IN" in stmt_full


def test_g_null_ingestion_id_retained_with_exclusions(monkeypatch) -> None:
    """NULL ingestion_id conservé malgré les exclusions (clause IS NULL OR ...)."""
    null_row = _valid_row("keep-null")
    null_row["ingestion_id"] = None
    table = [*_SYNTHETIC_ROWS, null_row]
    df = _run_fetch(table, ",".join(SYNTHETIC_IDS), monkeypatch)
    assert len(df) == 1
    assert float(df["value"].iloc[0]) == 22.5
    stmt_full = str(build_training_data_stmt(("x",)))
    assert "ingestion_id IS NULL OR ingestion_id NOT IN" in stmt_full
