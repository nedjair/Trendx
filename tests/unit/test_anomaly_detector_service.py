from __future__ import annotations

import numpy as np
import pandas as pd
import pytest
from trendx.anomalies.detectors import (
    AnomalyDetectorService,
    TrainedDetector,
)
from trendx.anomalies.scoring import AnomalyEpisode

# ── Faux accès DB (aucune PostgreSQL / MLflow réel) ────────────────────────


class _FakeConn:
    def __init__(self, rows: list[tuple]) -> None:
        self._rows = rows

    def __enter__(self) -> _FakeConn:
        return self

    def __exit__(self, *exc: object) -> bool:
        return False

    def execute(self, stmt: object, params: object = None) -> _FakeResult:
        return _FakeResult(self._rows)


class _FakeResult:
    def __init__(self, rows: list[tuple]) -> None:
        self._rows = rows

    def fetchall(self) -> list[tuple]:
        return self._rows


class _FakeEngine:
    def __init__(self, rows: list[tuple], engines_called: list[str]) -> None:
        self._rows = rows
        self._engines_called = engines_called

    def connect(self) -> _FakeConn:
        return _FakeConn(self._rows)


class _FakeSession:
    def __init__(self, db: _FakeDB) -> None:
        self._db = db

    def __enter__(self) -> _FakeSession:
        return self

    def __exit__(self, *exc: object) -> bool:
        self._db.committed = True
        return False

    def add(self, obj: object) -> None:
        self._db.added.append(obj)


class _FakeDB:
    def __init__(self, rows: list[tuple]) -> None:
        self._rows = rows
        self.added: list[object] = []
        self.committed = False
        self.engines_called: list[str] = []
        self.sessions_called: list[str] = []

    def get_engine(self, name: str) -> _FakeEngine:
        self.engines_called.append(name)
        return _FakeEngine(self._rows, self.engines_called)

    def get_session(self, name: str) -> _FakeSession:
        self.sessions_called.append(name)
        return _FakeSession(self)


def _rows(n: int, *, spike: bool = True) -> list[tuple]:
    idx = pd.date_range("2024-01-01", periods=n, freq="1h", tz="UTC")
    rng = np.random.default_rng(7)
    vals = rng.normal(0, 1, n).astype(float)
    if spike:
        vals[n // 2] = 25.0
        vals[n // 2 + 1] = 28.0
    return [(ts, float(v)) for ts, v in zip(idx, vals, strict=False)]


@pytest.fixture
def fake_db(monkeypatch: pytest.MonkeyPatch) -> _FakeDB:
    db = _FakeDB(_rows(200))
    monkeypatch.setattr("trendx.anomalies.detectors.db_manager", db)
    return db


def test_train_creates_detector_and_populates_store(fake_db: _FakeDB) -> None:
    svc = AnomalyDetectorService()
    res = svc.train("ent-1", "temperature", "IsolationForest", window_size=24)

    assert isinstance(res, TrainedDetector)
    assert res.id
    assert res.entity_id == "ent-1"
    assert res.metric_key == "temperature"
    assert res.algorithm == "IsolationForest"
    # store mémoire alimenté
    assert res.id in svc._store
    assert svc._store[res.id].fitted is True


def test_train_accepts_iforest_alias(fake_db: _FakeDB) -> None:
    svc = AnomalyDetectorService()
    res = svc.train("ent-1", "temperature", "IForest", window_size=24)

    assert res is not None
    assert res.algorithm == "IsolationForest"
    assert res.id in svc._store


def test_train_insufficient_data_returns_none() -> None:
    import trendx.anomalies.detectors as det_mod

    db = _FakeDB([])

    class _NoRowsEngine:
        def connect(self) -> _FakeConn:
            return _FakeConn([])

    db.get_engine = lambda name: _NoRowsEngine()  # type: ignore[assignment]

    svc = AnomalyDetectorService()
    saved = det_mod.db_manager
    det_mod.db_manager = db
    try:
        res = svc.train("ent-1", "temperature", "IsolationForest", window_size=24)
    finally:
        det_mod.db_manager = saved

    assert res is None


def test_scan_stateless_returns_episodes_and_persists(fake_db: _FakeDB) -> None:
    svc = AnomalyDetectorService()
    # detector_id=None -> fit stateless à la volée
    result = svc.scan("ent-1", "temperature")

    assert isinstance(result, list)
    assert all(isinstance(e, AnomalyEpisode) for e in result)
    assert len(result) >= 1
    # persistance : au moins un épisode écrit dans la table anomaly (catalog)
    assert len(fake_db.added) >= 1
    assert fake_db.sessions_called == ["catalog"]


def test_scan_with_detector_id_uses_stored_detector(fake_db: _FakeDB) -> None:
    svc = AnomalyDetectorService()
    trained = svc.train("ent-1", "temperature", "IsolationForest", window_size=24)
    assert trained is not None

    result = svc.scan("ent-1", "temperature", detector_id=trained.id)
    assert isinstance(result, list)
    assert all(isinstance(e, AnomalyEpisode) for e in result)
    assert len(fake_db.added) >= 1


def test_scan_insufficient_data_returns_empty() -> None:
    import trendx.anomalies.detectors as det_mod

    db = _FakeDB([])

    class _NoRowsEngine:
        def connect(self) -> _FakeConn:
            return _FakeConn([])

    db.get_engine = lambda name: _NoRowsEngine()  # type: ignore[assignment]

    saved = det_mod.db_manager
    det_mod.db_manager = db
    try:
        svc = AnomalyDetectorService()
        result = svc.scan("ent-1", "temperature")
    finally:
        det_mod.db_manager = saved

    assert result == []


def test_scan_does_not_write_to_thingsboard(fake_db: _FakeDB) -> None:
    svc = AnomalyDetectorService()
    svc.scan("ent-1", "temperature")

    # seules les lectures analytics et la session catalog sont sollicitées
    assert "tb_readonly" not in fake_db.engines_called
    assert fake_db.sessions_called == ["catalog"]
    # aucun objet non-Anomaly (pas d'écriture TB / alarme) n'est persisté
    from trendx.database.models import Anomaly

    assert all(isinstance(o, Anomaly) for o in fake_db.added)
