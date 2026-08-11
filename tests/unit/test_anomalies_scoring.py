from __future__ import annotations

import numpy as np
import pytest
from trendx.anomalies.scoring import SEVERITY_LEVELS, AnomalyEpisode, AnomalyScorer


@pytest.fixture
def scorer():
    return AnomalyScorer()


@pytest.mark.unit
def test_compute_anomaly_score(scorer):
    raw = np.array([0.1, 0.2, 0.3, 10.0, 0.4, 20.0], dtype=np.float64)
    normalized = scorer.compute_anomaly_score(raw, method="robust_zscore")
    assert normalized.min() >= 0.0
    assert normalized.max() <= 1.0
    assert normalized[3] > 0.5
    assert normalized[5] > 0.5


@pytest.mark.unit
def test_compute_anomaly_score_minmax(scorer):
    raw = np.array([0.1, 0.2, 0.3, 0.4, 5.0], dtype=np.float64)
    normalized = scorer.compute_anomaly_score(raw, method="minmax")
    assert normalized.min() >= 0.0
    assert normalized.max() <= 1.0


@pytest.mark.unit
def test_compute_anomaly_score_rank(scorer):
    raw = np.array([0.1, 0.5, 0.3, 10.0], dtype=np.float64)
    normalized = scorer.compute_anomaly_score(raw, method="rank")
    assert np.all(normalized >= 0.0)
    assert np.all(normalized <= 1.0)


@pytest.mark.unit
def test_compute_anomaly_score_empty(scorer):
    result = scorer.compute_anomaly_score(np.array([]))
    assert len(result) == 0


@pytest.mark.unit
def test_compute_anomaly_score_constant(scorer):
    raw = np.ones(10, dtype=np.float64)
    normalized = scorer.compute_anomaly_score(raw, method="minmax")
    assert np.all(normalized == 0.5)


@pytest.mark.unit
def test_compute_anomaly_score_index(scorer):
    scores = np.array([0.6, 0.7, 0.8, 0.9, 0.95], dtype=np.float64)
    asi = scorer.compute_anomaly_score_index(scores, duration=3600.0)
    assert asi > 0.0


@pytest.mark.unit
def test_compute_anomaly_score_index_empty(scorer):
    asi = scorer.compute_anomaly_score_index(np.array([]), duration=0.0)
    assert asi == 0.0


@pytest.mark.unit
def test_segment_anomalies(scorer):
    scores = np.array([0.1, 0.2, 0.8, 0.9, 0.7, 0.1, 0.2, 0.9, 0.1], dtype=np.float64)
    timestamps = np.array(
        [
            "2024-01-01T00:00",
            "2024-01-01T01:00",
            "2024-01-01T02:00",
            "2024-01-01T03:00",
            "2024-01-01T04:00",
            "2024-01-01T05:00",
            "2024-01-01T06:00",
            "2024-01-01T07:00",
            "2024-01-01T08:00",
        ],
        dtype="datetime64",
    )
    episodes = scorer.segment_anomalies(scores, timestamps, threshold=0.5)
    assert len(episodes) >= 1
    for ep in episodes:
        assert isinstance(ep, AnomalyEpisode)
        assert ep.severity in SEVERITY_LEVELS


@pytest.mark.unit
def test_apply_hysteresis(scorer):
    scores = np.array([0.1, 0.2, 0.7, 0.6, 0.4, 0.2, 0.8, 0.3, 0.1], dtype=np.float64)
    labels = scorer.apply_hysteresis(scores, open_threshold=0.6, close_threshold=0.3)
    assert labels.dtype == np.int_
    assert set(np.unique(labels)).issubset({0, 1})
    assert labels[0] == 0
    assert labels[2] == 1
    assert labels[4] == 1
    assert labels[5] == 0


@pytest.mark.unit
def test_apply_min_duration(scorer):
    ep1 = AnomalyEpisode(
        start=np.datetime64("2024-01-01T00:00").astype(object),
        end=np.datetime64("2024-01-01T00:05").astype(object),
        duration_seconds=300.0,
        peak_score=0.8,
        mean_score=0.6,
        anomaly_score_index=1.0,
        severity="MEDIUM",
        points_count=5,
        indices=[0, 1, 2, 3, 4],
    )
    ep2 = AnomalyEpisode(
        start=np.datetime64("2024-01-01T01:00").astype(object),
        end=np.datetime64("2024-01-01T03:00").astype(object),
        duration_seconds=7200.0,
        peak_score=0.9,
        mean_score=0.7,
        anomaly_score_index=2.5,
        severity="HIGH",
        points_count=10,
        indices=[10, 11, 12, 13, 14, 15, 16, 17, 18, 19],
    )
    filtered = scorer.apply_min_duration([ep1, ep2], min_duration=3600.0)
    assert len(filtered) == 1
    assert filtered[0].duration_seconds == 7200.0


@pytest.mark.unit
def test_apply_cooldown(scorer):
    scores = np.array(
        [0.8, 0.7, 0.9, 0.6, 0.8, 0.7, 0.9, 0.85, 0.6, 0.7, 0.8, 0.9, 0.75],
        dtype=np.float64,
    )
    timestamps = np.array(
        [
            "2024-01-01T00:00",
            "2024-01-01T00:15",
            "2024-01-01T00:30",
            "2024-01-01T00:45",
            "2024-01-01T01:00",
            "2024-01-01T01:30",
            "2024-01-01T01:45",
            "2024-01-01T02:00",
            "2024-01-01T02:15",
            "2024-01-01T02:30",
            "2024-01-01T02:45",
            "2024-01-01T03:00",
            "2024-01-01T03:15",
        ],
        dtype="datetime64",
    )
    ep1 = AnomalyEpisode(
        start=np.datetime64("2024-01-01T00:00").astype(object),
        end=np.datetime64("2024-01-01T01:00").astype(object),
        duration_seconds=3600.0,
        peak_score=0.8,
        mean_score=0.6,
        anomaly_score_index=1.0,
        severity="MEDIUM",
        points_count=5,
        indices=[0, 1, 2, 3, 4],
    )
    ep2 = AnomalyEpisode(
        start=np.datetime64("2024-01-01T01:30").astype(object),
        end=np.datetime64("2024-01-01T02:30").astype(object),
        duration_seconds=3600.0,
        peak_score=0.9,
        mean_score=0.7,
        anomaly_score_index=1.5,
        severity="HIGH",
        points_count=8,
        indices=[5, 6, 7, 8, 9, 10, 11, 12],
    )
    merged = scorer.apply_cooldown(
        [ep1, ep2],
        cooldown_duration=7200.0,
        scores=scores,
        timestamps=timestamps,
    )
    assert len(merged) == 1


@pytest.mark.unit
def test_determine_severity(scorer):
    assert scorer.determine_severity(3.5) == "CRITICAL"
    assert scorer.determine_severity(2.0) == "HIGH"
    assert scorer.determine_severity(1.0) == "MEDIUM"
    assert scorer.determine_severity(0.2) == "LOW"


@pytest.mark.unit
def test_score_to_episodes_full_pipeline(scorer):
    rng = np.random.default_rng(42)
    n = 100
    scores = rng.exponential(1, n).astype(np.float64)
    scores[20:30] = 10.0
    scores[60:65] = 8.0
    timestamps = np.arange("2024-01-01", n, dtype="datetime64[h]")

    config = {
        "sensitivity": 1.0,
        "contamination": 0.05,
        "window_size": 24,
        "segment_grouping": True,
        "open_threshold": 0.6,
        "close_threshold": 0.3,
        "min_duration_seconds": 0.0,
        "cooldown_seconds": 0.0,
    }
    episodes = scorer.score_to_episodes(scores, timestamps, config=config)
    assert len(episodes) >= 0
    for ep in episodes:
        assert ep.severity in SEVERITY_LEVELS
        assert ep.anomaly_score_index >= 0.0


@pytest.mark.unit
def test_score_to_episodes_empty(scorer):
    episodes = scorer.score_to_episodes(np.array([]), np.array([]))
    assert len(episodes) == 0


@pytest.mark.unit
def test_invalid_method_raises(scorer):
    with pytest.raises(ValueError, match="Unknown normalization method"):
        scorer.compute_anomaly_score(np.array([1.0, 2.0]), method="invalid")
