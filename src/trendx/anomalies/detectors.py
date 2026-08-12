from __future__ import annotations

import abc
import time
import uuid
from dataclasses import dataclass
from typing import Any, ClassVar, cast

import numpy as np
import numpy.typing as npt
import pandas as pd
from loguru import logger
from sklearn.cluster import DBSCAN, KMeans
from sklearn.ensemble import IsolationForest
from sqlalchemy import text

try:
    from pyod.models.cblof import CBLOF
    from pyod.models.hbos import HBOS
    from pyod.models.knn import KNN
    from pyod.models.lof import LOF

    PYOD_AVAILABLE = True
except ImportError:  # pragma: no cover
    PYOD_AVAILABLE = False

from trendx.anomalies.features import FeatureExtractor
from trendx.anomalies.scoring import AnomalyEpisode, AnomalyScorer
from trendx.config import settings
from trendx.database.connection import manager as db_manager
from trendx.database.models import Anomaly


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
    def fit(self, features: npt.NDArray[np.float64]) -> BaseDetector: ...

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
            n=features.shape[0],
            f=features.shape[1] if features.ndim > 1 else 1,
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
            algo=self._algorithm,
            n=features.shape[0],
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
            algo=self._algorithm,
            n=features.shape[0],
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
    params: dict[str, Any] | None = None,
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


# ─────────────────────────────────────────────────────────────────────────
# AnomalyDetectorService
# ─────────────────────────────────────────────────────────────────────────

# Mappe les alias de l'API (ex: "IForest") vers les clés du DETECTOR_REGISTRY.
_ALGO_ALIASES: dict[str, str] = {
    "iforest": "IsolationForest",
    "if": "IsolationForest",
    "isolationforest": "IsolationForest",
}


def _normalize_algorithm(algorithm: str) -> str:
    """Normalise un alias d'algorithme vers une clé du ``DETECTOR_REGISTRY``."""
    return _ALGO_ALIASES.get(algorithm.strip().lower(), algorithm)


@dataclass
class TrainedDetector:
    """Handle léger retourné par ``AnomalyDetectorService.train``.

    Le schéma Trendz 1.15.0 ne possède pas de table ORM pour les détecteurs
    d'anomalies entraînés : le service conserve le détecteur ajusté dans un
    store mémoire injecté (clé = ``id``) et journalise (best-effort) la
    lignée dans MLflow. L'objet expose au minimum ``.id``, ``.entity_id``,
    ``.metric_key`` et ``.algorithm`` (consommés par l'endpoint).
    """

    id: str
    entity_id: str
    metric_key: str
    algorithm: str
    model_uri: str | None = None


class AnomalyDetectorService:
    """Orchestre l'entraînement et le scan de détecteurs d'anomalies.

    Réutilise les abstractions existantes : ``FeatureExtractor`` (features),
    ``AnomalyScorer`` (score/ASI/épisodes), ``create_detector`` (détecteurs),
    et ``db_manager`` pour la lecture de télémétrie (analytics) et la
    persistance des épisodes (catalog). N'écrit jamais dans ThingsBoard.
    """

    def __init__(
        self,
        tracker: Any | None = None,
        detector_store: dict[str, BaseDetector] | None = None,
        window_size: int | None = None,
    ) -> None:
        # Tous les arguments ont une valeur par défaut : l'endpoint appelle
        # ``AnomalyDetectorService()`` sans argument.
        self._tracker = tracker
        self._store: dict[str, BaseDetector] = detector_store if detector_store is not None else {}
        self._window = window_size or settings.anomaly_window_size

    # ── Accès données (miroir TrainingService._fetch_training_data) ───────

    def _fetch_series(self, entity_id: str, metric_key: str, limit: int) -> pd.Series:
        engine = db_manager.get_engine("analytics")
        stmt = text(
            """
            SELECT ts, dbl_v AS value
            FROM ts_kv
            WHERE entity_id = :eid
              AND metric_key = :key
              AND dbl_v IS NOT NULL
            ORDER BY ts DESC
            LIMIT :lim
            """
        )
        with engine.connect() as conn:
            rows = conn.execute(
                stmt, {"eid": entity_id, "key": metric_key, "lim": limit}
            ).fetchall()
        if not rows:
            return pd.Series(dtype="float64")
        df = pd.DataFrame(rows, columns=["ts", "value"])
        df["ts"] = pd.to_datetime(df["ts"])
        df = df.sort_values("ts").reset_index(drop=True)
        series = pd.Series(
            df["value"].values.astype(np.float64),
            index=pd.DatetimeIndex(df["ts"].values),
        )
        return series

    @staticmethod
    def _build_features(
        series: pd.Series, window_size: int
    ) -> tuple[npt.NDArray[Any] | None, npt.NDArray[Any] | None]:
        """Construit la matrice de features normalisées + timestamps des fenêtres.

        Renvoie ``(None, None)`` si la série est trop courte pour former une
        fenêtre. La matrice est normalisée (robust) comme attendu par les
        détecteurs et par ``AnomalyScorer``.
        """
        extractor = FeatureExtractor(window_size=window_size)
        features_df = extractor.extract_features(series, window_size=window_size)
        if features_df.empty:
            return None, None
        feat_cols = [c for c in features_df.columns if c not in ("_start", "_end")]
        sub = features_df[feat_cols].select_dtypes(include=[np.number])
        if sub.empty:
            return None, None
        normalized = extractor.normalize_features(sub, method="robust")
        matrix = normalized.values.astype(np.float64)
        timestamps = features_df.index.values
        return matrix, timestamps

    @staticmethod
    def _as_uuid(value: Any) -> uuid.UUID | None:
        if not value:
            return None
        try:
            return uuid.UUID(str(value))
        except (ValueError, AttributeError, TypeError):
            return None

    # ── train ─────────────────────────────────────────────────────────────

    def train(
        self,
        entity_id: str,
        metric_key: str,
        algorithm: str,
        contamination: float = 0.01,
        window_size: int | None = None,
    ) -> TrainedDetector | None:
        w = window_size or self._window
        logger.info(
            "Training anomaly detector: {}/{} algorithm={} window={}",
            entity_id,
            metric_key,
            algorithm,
            w,
        )
        series = self._fetch_series(entity_id, metric_key, limit=max(w * 8, 200))
        if series.empty or len(series) < w:
            logger.error("Insufficient data for {}/{}", entity_id, metric_key)
            return None
        matrix, _ = self._build_features(series, w)
        if matrix is None or matrix.shape[0] < 2:
            logger.error("Insufficient windows for {}/{}", entity_id, metric_key)
            return None

        algo = _normalize_algorithm(algorithm)
        detector = create_detector(algo, {"contamination": contamination})
        detector.fit(matrix)

        detector_id = str(uuid.uuid4())
        self._store[detector_id] = detector

        model_uri = self._log_to_mlflow(detector_id, entity_id, metric_key, algo, contamination, w)

        return TrainedDetector(
            id=detector_id,
            entity_id=entity_id,
            metric_key=metric_key,
            algorithm=algo,
            model_uri=model_uri,
        )

    def _log_to_mlflow(
        self,
        detector_id: str,
        entity_id: str,
        metric_key: str,
        algorithm: str,
        contamination: float,
        window_size: int,
    ) -> str | None:
        """Journalise la lignée du détecteur dans MLflow (best-effort).

        Ne rend PAS le fonctionnement local dépendant d'un serveur MLflow :
        tout échec est ignoré et ``None`` est renvoyé.
        """
        if self._tracker is None:
            return None
        try:
            run_id = self._tracker.start_run(
                f"anomaly_{entity_id[:12]}_{metric_key}",
                run_name=f"{algorithm}_{int(time.time())}",
                tags={
                    "entity_id": entity_id,
                    "metric_key": metric_key,
                    "algorithm": algorithm,
                    "task": "anomaly_detector",
                },
            )
            self._tracker.log_params(
                {
                    "contamination": contamination,
                    "window_size": window_size,
                    "algorithm": algorithm,
                }
            )
            self._tracker.end_run("FINISHED")
            return f"runs:/{run_id}/model"
        except Exception as exc:  # pragma: no cover - best effort
            logger.warning("MLflow logging skipped for anomaly detector: {}", exc)
            return None

    # ── scan ──────────────────────────────────────────────────────────────

    def scan(
        self,
        entity_id: str,
        metric_key: str,
        detector_id: str | None = None,
    ) -> list[AnomalyEpisode]:
        w = self._window
        logger.info("Scanning anomalies: {}/{} detector={}", entity_id, metric_key, detector_id)
        series = self._fetch_series(entity_id, metric_key, limit=max(w * 4, 100))
        if series.empty or len(series) < w:
            logger.warning("Insufficient data for scan {}/{}", entity_id, metric_key)
            return []
        matrix, timestamps = self._build_features(series, w)
        if matrix is None or matrix.shape[0] < 1 or timestamps is None:
            return []

        detector = self._resolve_detector(detector_id)
        if detector is None:
            # Comportement stateless diagnostiqué : entraînement à la volée
            # sur les données récentes (aucun état partagé requis).
            detector = create_detector(
                "IsolationForest", {"contamination": settings.anomaly_contamination}
            )
            detector.fit(matrix)

        raw = detector.score(matrix)
        normalized = AnomalyScorer.compute_anomaly_score(raw)
        episodes = AnomalyScorer.score_to_episodes(normalized, timestamps)
        self._persist_episodes(episodes, entity_id, metric_key, detector_id)
        return episodes

    def _resolve_detector(self, detector_id: str | None) -> BaseDetector | None:
        if detector_id is None:
            return None
        detector = self._store.get(detector_id)
        if detector is None:
            logger.warning("Detector {} not found in store; using stateless scan", detector_id)
        return detector

    def _persist_episodes(
        self,
        episodes: list[AnomalyEpisode],
        entity_id: str,
        metric_key: str,
        detector_id: str | None,
    ) -> None:
        """Persiste les épisodes dans la table ORM ``anomaly`` existante.

        Utilise exclusivement ``db_manager.get_session("catalog")`` (commit
        automatique en sortie de contexte). Aucune écriture dans
        ``scored_point_anomaly`` (schéma inadapté) ni dans ThingsBoard.

        Risque de duplication connu (pas d'upsert/dispo en 0.4) : documenté,
        à traiter en phase fonctionnelle ultérieure.
        """
        if not episodes:
            return
        try:
            with db_manager.get_session("catalog") as session:
                for ep in episodes:
                    session.add(
                        Anomaly(
                            id=uuid.uuid4(),
                            item_id=self._as_uuid(entity_id),
                            item_name=metric_key,
                            start_ts=int(ep.start.timestamp() * 1000),
                            end_ts=int(ep.end.timestamp() * 1000),
                            cluster_id=None,
                            score=float(ep.peak_score),
                            score_index=int(round(ep.anomaly_score_index)),
                            model_id=self._as_uuid(detector_id) if detector_id else None,
                            alarm_id=None,
                        )
                    )
        except Exception as exc:  # non-fatal : le scan renvoie quand même les épisodes
            logger.error("Failed to persist anomaly episodes: {}", exc)
