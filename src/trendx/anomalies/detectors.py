from __future__ import annotations

import abc
from typing import Any, ClassVar, Optional, cast

import numpy as np
import numpy.typing as npt
from loguru import logger
from sklearn.cluster import DBSCAN, KMeans
from sklearn.ensemble import IsolationForest

try:
    from pyod.models.cblof import CBLOF
    from pyod.models.hbos import HBOS
    from pyod.models.knn import KNN
    from pyod.models.lof import LOF

    PYOD_AVAILABLE = True
except ImportError:  # pragma: no cover
    PYOD_AVAILABLE = False


class BaseDetector(abc.ABC):
    """Abstract base for all anomaly detectors.

    Subclasses must implement ``fit``, ``score``, and ``predict``.
    """

    def __init__(self, name: str = "base", contamination: float = 0.01) -> None:
        self._name = name
        self._contamination = contamination
        self._fitted: bool = False

    @property
    def name(self) -> str:
        return self._name

    @property
    def fitted(self) -> bool:
        return self._fitted

    @property
    def contamination(self) -> float:
        return self._contamination

    @abc.abstractmethod
    def fit(self, features: npt.NDArray[np.float64]) -> BaseDetector:
        ...

    @abc.abstractmethod
    def score(self, features: npt.NDArray[np.float64]) -> npt.NDArray[np.float64]:
        """Return anomaly scores (higher = more anomalous)."""
        ...

    def predict(
        self,
        features: npt.NDArray[np.float64],
        threshold: float | None = None,
    ) -> npt.NDArray[np.int_]:
        """Return binary labels (1 = anomalous, 0 = normal).

        Parameters
        ----------
        features : ndarray of shape (n_samples, n_features)
        threshold : float, optional
            Score cutoff. Uses contamination quantile if None.

        Returns
        -------
        ndarray of int, shape (n_samples,)
        """
        scores = self.score(features)
        if threshold is not None:
            return np.where(scores >= threshold, 1, 0).astype(np.int_)
        cutoff = float(np.quantile(scores, 1.0 - self._contamination))
        return np.where(scores >= cutoff, 1, 0).astype(np.int_)


class IsolationForestDetector(BaseDetector):
    """Anomaly detector using scikit-learn IsolationForest."""

    def __init__(
        self,
        contamination: float = 0.01,
        n_estimators: int = 100,
        max_features: float = 1.0,
        random_state: int = 42,
        **kwargs: Any,
    ) -> None:
        super().__init__(name="IsolationForest", contamination=contamination)
        self._model = IsolationForest(
            contamination=contamination,
            n_estimators=n_estimators,
            max_features=max_features,
            random_state=random_state,
            **kwargs,
        )

    def fit(self, features: npt.NDArray[np.float64]) -> IsolationForestDetector:
        logger.info(
            "Fitting IsolationForest on {n} samples, {f} features",
            n=features.shape[0], f=features.shape[1] if features.ndim > 1 else 1,
        )
        self._model.fit(features)
        self._fitted = True
        return self

    def score(self, features: npt.NDArray[np.float64]) -> npt.NDArray[np.float64]:
        if not self._fitted:
            msg = "Detector has not been fitted yet. Call fit() first."
            raise RuntimeError(msg)
        # IsolationForest returns negative for anomalies; invert so higher = anomalous
        raw = self._model.decision_function(features)
        scores = -raw
        return cast(npt.NDArray[np.float64], scores.astype(np.float64))


class PyODDetector(BaseDetector):
    """Wraps PyOD anomaly detectors.

    Supported algorithms: ``"LOF"``, ``"kNN"``, ``"HBOS"``, ``"CBLOF"``.

    Parameters
    ----------
    algorithm : str
        PyOD detector class name.
    contamination : float
        Expected proportion of anomalies.
    **kwargs
        Forwarded to the PyOD constructor.
    """

    PYOD_MAP: ClassVar[dict[str, type]] = {}

    def __init__(
        self,
        algorithm: str = "LOF",
        contamination: float = 0.01,
        **kwargs: Any,
    ) -> None:
        super().__init__(name=f"PyOD-{algorithm}", contamination=contamination)

        if not PYOD_AVAILABLE:
            msg = "PyOD is not installed. Install with: pip install pyod"
            raise ImportError(msg)

        cls = self._resolve_pyod_class(algorithm)
        self._algorithm = algorithm
        self._model = cls(contamination=contamination, **kwargs)

    @classmethod
    def _resolve_pyod_class(cls, algorithm: str) -> type:
        if algorithm in cls.PYOD_MAP:
            return cls.PYOD_MAP[algorithm]
        mapping: dict[str, type] = {
            "LOF": LOF,
            "kNN": KNN,
            "HBOS": HBOS,
            "CBLOF": CBLOF,
        }
        cls_ = mapping.get(algorithm)
        if cls_ is None:
            known = ", ".join(mapping)
            msg = f"Unknown PyOD algorithm '{algorithm}'. Known: {known}"
            raise ValueError(msg)
        cls.PYOD_MAP[algorithm] = cls_
        return cls_

    def fit(self, features: npt.NDArray[np.float64]) -> PyODDetector:
        logger.info(
            "Fitting PyOD {algo} on {n} samples, {f} features",
            algo=self._algorithm, n=features.shape[0],
            f=features.shape[1] if features.ndim > 1 else 1,
        )
        self._model.fit(features)
        self._fitted = True
        return self

    def score(self, features: npt.NDArray[np.float64]) -> npt.NDArray[np.float64]:
        if not self._fitted:
            msg = "Detector has not been fitted yet. Call fit() first."
            raise RuntimeError(msg)
        return cast(
            npt.NDArray[np.float64], self._model.decision_function(features).astype(np.float64)
        )


class ClusterDetector(BaseDetector):
    """Anomaly detection based on clustering (KMeans or DBSCAN).

    For KMeans: anomaly score = distance to nearest centroid.
    For DBSCAN: anomaly score = -1 / (noise distance) for noise points,
    or a low score for cluster members.

    Parameters
    ----------
    algorithm : str
        ``"KMeans"`` or ``"DBSCAN"``.
    contamination : float
        Expected proportion of anomalies.
    **kwargs
        Forwarded to the cluster constructor.
    """

    def __init__(
        self,
        algorithm: str = "KMeans",
        contamination: float = 0.01,
        **kwargs: Any,
    ) -> None:
        super().__init__(name=f"Cluster-{algorithm}", contamination=contamination)
        self._algorithm = algorithm
        self._cluster_model: Any = None
        self._cluster_kwargs = kwargs

    def fit(self, features: npt.NDArray[np.float64]) -> ClusterDetector:
        logger.info(
            "Fitting {algo} cluster detector on {n} samples, {f} features",
            algo=self._algorithm, n=features.shape[0],
            f=features.shape[1] if features.ndim > 1 else 1,
        )

        if self._algorithm == "KMeans":
            n_clusters = self._cluster_kwargs.pop("n_clusters", 10)
            self._cluster_model = KMeans(
                n_clusters=n_clusters,
                random_state=self._cluster_kwargs.pop("random_state", 42),
                n_init=self._cluster_kwargs.pop("n_init", "auto"),
                **self._cluster_kwargs,
            )
        elif self._algorithm == "DBSCAN":
            eps = self._cluster_kwargs.pop("eps", 0.5)
            min_samples = self._cluster_kwargs.pop("min_samples", 5)
            self._cluster_model = DBSCAN(
                eps=eps,
                min_samples=min_samples,
                **self._cluster_kwargs,
            )
        else:
            msg = f"Unknown clustering algorithm '{self._algorithm}'. Use 'KMeans' or 'DBSCAN'."
            raise ValueError(msg)

        self._cluster_model.fit(features)
        self._fitted = True
        return self

    def score(self, features: npt.NDArray[np.float64]) -> npt.NDArray[np.float64]:
        if not self._fitted or self._cluster_model is None:
            msg = "Detector has not been fitted yet. Call fit() first."
            raise RuntimeError(msg)

        if self._algorithm == "KMeans":
            distances = self._cluster_model.transform(features)
            scores = distances.min(axis=1).astype(np.float64)
        elif self._algorithm == "DBSCAN":
            labels = self._cluster_model.fit_predict(features)
            scores = np.zeros(len(features), dtype=np.float64)
            noise_mask = labels == -1
            if noise_mask.any():
                core = self._cluster_model.components_
                if len(core) > 0:
                    from sklearn.metrics.pairwise import euclidean_distances

                    dists = euclidean_distances(features[noise_mask], core)
                    scores[noise_mask] = dists.min(axis=1)
                else:
                    scores[noise_mask] = 1.0
        else:
            msg = f"Unexpected algorithm '{self._algorithm}'"
            raise RuntimeError(msg)

        return cast(npt.NDArray[np.float64], scores)


DETECTOR_REGISTRY: dict[str, type[BaseDetector]] = {
    "IsolationForest": IsolationForestDetector,
    "LOF": PyODDetector,
    "kNN": PyODDetector,
    "HBOS": PyODDetector,
    "CBLOF": PyODDetector,
    "KMeans": ClusterDetector,
    "DBSCAN": ClusterDetector,
}


def create_detector(
    algorithm: str,
    params: Optional[dict[str, Any]] = None,
) -> BaseDetector:
    """Factory function — instantiate a detector by name.

    Parameters
    ----------
    algorithm : str
        Key in ``DETECTOR_REGISTRY``.
    params : dict or None
        Keyword arguments forwarded to the detector constructor.

    Returns
    -------
    BaseDetector instance.

    Raises
    ------
    ValueError
        If ``algorithm`` is not registered.
    """
    params = params or {}
    cls = DETECTOR_REGISTRY.get(algorithm)

    if cls is None:
        known = ", ".join(DETECTOR_REGISTRY)
        msg = f"Unknown algorithm '{algorithm}'. Known: {known}"
        raise ValueError(msg)

    # PyOD wrappers need the algorithm name forwarded
    if cls is PyODDetector:
        return PyODDetector(algorithm=algorithm, **params)

    return cls(**params)
