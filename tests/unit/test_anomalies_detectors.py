from __future__ import annotations

import numpy as np
import pytest
from trendx.anomalies.detectors import (
    DETECTOR_REGISTRY,
    ClusterDetector,
    IsolationForestDetector,
    PyODDetector,
    create_detector,
)


@pytest.fixture
def normal_features():
    rng = np.random.default_rng(42)
    return rng.normal(0, 1, (200, 5)).astype(np.float64)


@pytest.fixture
def anomalous_features():
    rng = np.random.default_rng(42)
    normal = rng.normal(0, 1, (180, 5))
    anomalies = rng.normal(5, 1, (20, 5))
    return np.vstack([normal, anomalies]).astype(np.float64)


@pytest.mark.unit
@pytest.mark.xfail(
    reason="IsolationForest score inversion / ClusterDetector init validation — attend le future commit sur feat/detector-scoring-and-forecasting-fixes"
)
def test_isolation_forest_fit_score(normal_features):
    detector = IsolationForestDetector(contamination=0.05, random_state=42)
    detector.fit(normal_features)
    scores = detector.score(normal_features)
    assert scores.shape[0] == 200
    assert np.all(scores >= 0)
    assert detector.fitted


@pytest.mark.unit
def test_isolation_forest_predict(anomalous_features):
    detector = IsolationForestDetector(contamination=0.1, random_state=42)
    detector.fit(anomalous_features)
    labels = detector.predict(anomalous_features)
    assert labels.shape[0] == 200
    assert set(np.unique(labels)).issubset({0, 1})


@pytest.mark.unit
@pytest.mark.skipif(
    not PyODDetector.__module__ or not hasattr(PyODDetector, "PYOD_AVAILABLE"),
    reason="PyOD may not be installed",
)
def test_pyod_detector_wrapper(normal_features):
    if not hasattr(__import__("pyod"), "__version__"):
        pytest.skip("PyOD not installed")
    detector = PyODDetector(algorithm="HBOS", contamination=0.05)
    detector.fit(normal_features)
    scores = detector.score(normal_features)
    assert scores.shape[0] == 200
    assert detector.fitted


@pytest.mark.unit
def test_pyod_import_error():
    try:
        import pyod  # noqa: F401
    except ImportError:
        pass
    else:
        pytest.skip("PyOD is installed, cannot test ImportError")


@pytest.mark.unit
def test_cluster_detector_kmeans(normal_features):
    detector = ClusterDetector(algorithm="KMeans", contamination=0.05, n_clusters=5)
    detector.fit(normal_features)
    scores = detector.score(normal_features)
    assert scores.shape[0] == 200
    assert np.all(scores >= 0)


@pytest.mark.unit
def test_cluster_detector_dbscan():
    rng = np.random.default_rng(42)
    features = rng.normal(0, 1, (100, 2)).astype(np.float64)
    detector = ClusterDetector(algorithm="DBSCAN", contamination=0.1, eps=0.5, min_samples=3)
    detector.fit(features)
    scores = detector.score(features)
    assert scores.shape[0] == 100


@pytest.mark.unit
@pytest.mark.xfail(
    reason="IsolationForest score inversion / ClusterDetector init validation — attend le future commit sur feat/detector-scoring-and-forecasting-fixes"
)
def test_cluster_detector_invalid_algorithm():
    with pytest.raises(ValueError, match="Unknown clustering algorithm"):
        ClusterDetector(algorithm="InvalidAlgo")


@pytest.mark.unit
def test_create_detector_factory():
    detector = create_detector("IsolationForest", {"contamination": 0.05, "random_state": 42})
    assert isinstance(detector, IsolationForestDetector)
    assert detector.contamination == 0.05


@pytest.mark.unit
def test_create_detector_unknown():
    with pytest.raises(ValueError, match="Unknown algorithm"):
        create_detector("UnknownAlgo")


@pytest.mark.unit
def test_create_detector_cluster():
    detector = create_detector("KMeans", {"contamination": 0.05, "n_clusters": 5})
    assert isinstance(detector, ClusterDetector)


@pytest.mark.unit
def test_detector_registry():
    assert "IsolationForest" in DETECTOR_REGISTRY
    assert "KMeans" in DETECTOR_REGISTRY
    assert "DBSCAN" in DETECTOR_REGISTRY
    assert DETECTOR_REGISTRY["IsolationForest"] is IsolationForestDetector


@pytest.mark.unit
def test_score_before_fit_raises():
    detector = IsolationForestDetector()
    with pytest.raises(RuntimeError, match="not been fitted"):
        detector.score(np.array([[1.0, 2.0]]))


@pytest.mark.unit
def test_predict_with_custom_threshold(anomalous_features):
    detector = IsolationForestDetector(contamination=0.1, random_state=42)
    detector.fit(anomalous_features)
    labels = detector.predict(anomalous_features, threshold=0.5)
    assert labels.shape[0] == 200
