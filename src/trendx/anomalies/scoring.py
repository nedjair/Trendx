from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

import numpy as np
import numpy.typing as npt
from loguru import logger
from scipy import stats as scipy_stats


@dataclass
class AnomalyEpisode:
    """Represents a contiguous anomaly event."""

    start: datetime
    end: datetime
    duration_seconds: float
    peak_score: float
    mean_score: float
    anomaly_score_index: float
    severity: str
    points_count: int
    indices: list[int] = field(default_factory=list)


@dataclass
class AnomalyScoringConfig:
    """Configuration for anomaly scoring and alerting."""

    sensitivity: float = 1.0
    contamination: float = 0.01
    window_size: int = 24
    segment_grouping: bool = True
    open_threshold: float = 0.6
    close_threshold: float = 0.3
    min_duration_seconds: float = 300.0
    cooldown_seconds: float = 3600.0


SEVERITY_LEVELS = ["LOW", "MEDIUM", "HIGH", "CRITICAL"]


class AnomalyScorer:
    """Normalize raw detector scores, segment episodes, and compute severity."""

    @staticmethod
    def compute_anomaly_score(
        raw_scores: npt.NDArray[np.float64],
        method: str = "robust_zscore",
    ) -> npt.NDArray[np.float64]:
        """Normalize raw anomaly scores into a [0, 1] range.

        Parameters
        ----------
        raw_scores : ndarray
            Raw scores from a detector (higher = more anomalous).
        method : str
            Normalization method:

            - ``"robust_zscore"`` — uses median and IQR (default).
            - ``"minmax"`` — linear rescale to [0, 1].
            - ``"rank"`` — percentile rank.

        Returns
        -------
        ndarray
            Normalized scores in the [0, 1] range.
        """
        scores = np.asarray(raw_scores, dtype=np.float64).ravel()

        if len(scores) == 0:
            return scores

        if method == "robust_zscore":
            median = np.median(scores)
            iqr = float(np.subtract(*np.percentile(scores, [75, 25])))
            if iqr < 1e-12:
                iqr = np.std(scores) if np.std(scores) > 1e-12 else 1.0
            z = (scores - median) / iqr
            normalized: npt.NDArray[np.float64] = scipy_stats.norm.cdf(z)
        elif method == "minmax":
            s_min, s_max = float(scores.min()), float(scores.max())
            if s_max - s_min < 1e-12:
                normalized = np.full_like(scores, 0.5)
            else:
                normalized = (scores - s_min) / (s_max - s_min)
        elif method == "rank":
            normalized = scipy_stats.rankdata(scores) / len(scores)
        else:
            msg = f"Unknown normalization method '{method}'. Use 'robust_zscore', 'minmax', or 'rank'."
            raise ValueError(msg)

        return np.clip(normalized, 0.0, 1.0).astype(np.float64)

    @staticmethod
    def compute_anomaly_score_index(
        scores: npt.NDArray[np.float64],
        duration: float,
        intensity_weight: float = 0.7,
    ) -> float:
        """Compute the Anomaly Score Index (ASI).

        ASI = intensity x duration, where intensity is the mean of the
        top-quartile scores within the episode.

        Parameters
        ----------
        scores : ndarray
            Normalized scores for the episode.
        duration : float
            Duration in seconds.
        intensity_weight : float
            Weight given to intensity vs. peak (0.7 = default).

        Returns
        -------
        float
        """
        if len(scores) == 0:
            return 0.0

        threshold = float(np.percentile(scores, 75))
        high_scores = scores[scores >= threshold]

        if len(high_scores) > 0:
            intensity = float(np.mean(high_scores))
        else:
            intensity = float(np.mean(scores))

        peak = float(np.max(scores))
        combined_intensity = intensity_weight * intensity + (1.0 - intensity_weight) * peak

        duration_hours = duration / 3600.0
        asi = combined_intensity * (1.0 + duration_hours)
        logger.debug(
            "ASI={asi:.4f} (intensity={i:.4f}, peak={p:.4f}, dur_h={d:.2f})",
            asi=asi,
            i=intensity,
            p=peak,
            d=duration_hours,
        )
        return asi

    @staticmethod
    def determine_severity(asi_value: float) -> str:
        """Map an ASI value to a severity level.

        Thresholds (configurable via sensitivity):
            >= 3.0  → CRITICAL
            >= 1.5  → HIGH
            >= 0.5  → MEDIUM
            <  0.5  → LOW

        Parameters
        ----------
        asi_value : float
            Computed Anomaly Score Index.

        Returns
        -------
        str
            One of ``"LOW"``, ``"MEDIUM"``, ``"HIGH"``, ``"CRITICAL"``.
        """
        if asi_value >= 3.0:
            return "CRITICAL"
        if asi_value >= 1.5:
            return "HIGH"
        if asi_value >= 0.5:
            return "MEDIUM"
        return "LOW"

    @staticmethod
    def apply_hysteresis(
        scores: npt.NDArray[np.float64],
        open_threshold: float = 0.6,
        close_threshold: float = 0.3,
    ) -> npt.NDArray[np.int_]:
        """Apply hysteresis to prevent flapping between normal/anomalous.

        A point is anomalous once its score exceeds ``open_threshold`` and
        remains anomalous until its score drops below ``close_threshold``.

        Parameters
        ----------
        scores : ndarray
            Normalized scores.
        open_threshold : float
            Score above which a point becomes anomalous.
        close_threshold : float
            Score below which a point returns to normal.

        Returns
        -------
        ndarray of int
            Binary labels (1 = anomalous, 0 = normal).
        """
        labels = np.zeros(len(scores), dtype=np.int_)
        in_anomaly = False

        for i in range(len(scores)):
            if in_anomaly:
                if scores[i] < close_threshold:
                    in_anomaly = False
                    labels[i] = 0
                else:
                    labels[i] = 1
            else:
                if scores[i] > open_threshold:
                    in_anomaly = True
                    labels[i] = 1

        return labels

    @staticmethod
    def segment_anomalies(
        scores: npt.NDArray[np.float64],
        timestamps: npt.NDArray[np.datetime64],
        threshold: float = 0.5,
    ) -> list[AnomalyEpisode]:
        """Group consecutive anomalous points into episodes.

        Parameters
        ----------
        scores : ndarray
            Normalized scores.
        timestamps : ndarray of datetime64
            Corresponding timestamps.
        threshold : float
            Score threshold for labelling a point as anomalous.

        Returns
        -------
        list of AnomalyEpisode
        """
        if len(scores) == 0:
            return []

        labels = (scores >= threshold).astype(np.int_)
        return AnomalyScorer._labels_to_episodes(
            labels,
            scores,
            timestamps,
            min_duration=0.0,
            cooldown=0.0,
        )

    @staticmethod
    def _labels_to_episodes(
        labels: npt.NDArray[np.int_],
        scores: npt.NDArray[np.float64],
        timestamps: npt.NDArray[np.datetime64],
        min_duration: float = 0.0,
        cooldown: float = 0.0,
    ) -> list[AnomalyEpisode]:
        """Convert binary labels to episode list with filtering."""
        episodes: list[AnomalyEpisode] = []
        current: list[int] = []

        for i in range(len(labels)):
            if labels[i] == 1:
                current.append(i)
            else:
                if current:
                    ep = AnomalyScorer._build_episode(current, scores, timestamps)
                    if ep.duration_seconds >= min_duration:
                        episodes.append(ep)
                    current = []

        if current:
            ep = AnomalyScorer._build_episode(current, scores, timestamps)
            if ep.duration_seconds >= min_duration:
                episodes.append(ep)

        if cooldown > 0.0 and len(episodes) > 1:
            filtered: list[AnomalyEpisode] = [episodes[0]]
            for ep in episodes[1:]:
                gap = (ep.start - filtered[-1].end).total_seconds()
                if gap >= cooldown:
                    filtered.append(ep)
                else:
                    # Merge with previous episode
                    prev = filtered[-1]
                    merged_indices = prev.indices + ep.indices
                    merged_ep = AnomalyScorer._build_episode(
                        merged_indices,
                        scores,
                        timestamps,
                    )
                    if merged_ep.duration_seconds >= min_duration:
                        filtered[-1] = merged_ep
            episodes = filtered

        return episodes

    @staticmethod
    def _build_episode(
        indices: list[int],
        scores: npt.NDArray[np.float64],
        timestamps: npt.NDArray[np.datetime64],
    ) -> AnomalyEpisode:
        ep_scores = scores[indices]
        ep_times = timestamps[indices]

        start_ts = datetime.utcfromtimestamp(
            float(np.datetime64(ep_times[0], "s").astype(np.int64))
        )
        end_ts = datetime.utcfromtimestamp(float(np.datetime64(ep_times[-1], "s").astype(np.int64)))
        duration = (end_ts - start_ts).total_seconds()
        peak = float(np.max(ep_scores))
        mean = float(np.mean(ep_scores))
        asi = AnomalyScorer.compute_anomaly_score_index(ep_scores, duration)
        severity = AnomalyScorer.determine_severity(asi)

        return AnomalyEpisode(
            start=start_ts,
            end=end_ts,
            duration_seconds=duration,
            peak_score=peak,
            mean_score=mean,
            anomaly_score_index=asi,
            severity=severity,
            points_count=len(indices),
            indices=indices,
        )

    @staticmethod
    def apply_min_duration(
        episodes: list[AnomalyEpisode],
        min_duration: float,
    ) -> list[AnomalyEpisode]:
        """Filter out episodes shorter than ``min_duration`` (seconds)."""
        return [ep for ep in episodes if ep.duration_seconds >= min_duration]

    @staticmethod
    def apply_cooldown(
        episodes: list[AnomalyEpisode],
        cooldown_duration: float,
        scores: npt.NDArray[np.float64],
        timestamps: npt.NDArray[np.datetime64],
    ) -> list[AnomalyEpisode]:
        """Merge episodes separated by less than ``cooldown_duration`` seconds.

        Parameters
        ----------
        episodes : list of AnomalyEpisode
            Already-filtered episodes.
        cooldown_duration : float
            Minimum gap (seconds) between distinct episodes.
        scores : ndarray of float
            Original detector scores, indexed by ``episode.indices``. Required
            to recompute the merged episode's real peak/mean/asi.
        timestamps : ndarray of datetime64
            Original timestamps, indexed by ``episode.indices``. Required to
            recompute the merged episode's real start/end/duration.

        Returns
        -------
        list of AnomalyEpisode
        """
        if not episodes or cooldown_duration <= 0.0:
            return episodes

        merged: list[AnomalyEpisode] = [episodes[0]]
        for ep in episodes[1:]:
            gap = (ep.start - merged[-1].end).total_seconds()
            if gap < cooldown_duration:
                prev = merged[-1]
                merged_indices = prev.indices + ep.indices
                merged_ep = AnomalyScorer._build_episode(
                    merged_indices,
                    scores,
                    timestamps,
                )
                merged[-1] = merged_ep
            else:
                merged.append(ep)

        return merged

    @classmethod
    def score_to_episodes(
        cls,
        scores: npt.NDArray[np.float64],
        timestamps: npt.NDArray[np.datetime64],
        config: dict[str, Any] | None = None,
    ) -> list[AnomalyEpisode]:
        """End-to-end pipeline: scores → normalized → segmented → episodes.

        Parameters
        ----------
        scores : ndarray
            Raw detector scores.
        timestamps : ndarray of datetime64
            Corresponding timestamps.
        config : dict, optional
            Overrides for ``AnomalyScoringConfig`` fields.

        Returns
        -------
        list of AnomalyEpisode
        """
        cfg = AnomalyScoringConfig()
        if config:
            for key, value in config.items():
                if hasattr(cfg, key):
                    setattr(cfg, key, value)

        normalized = cls.compute_anomaly_score(scores)

        effective_open = cfg.open_threshold * cfg.sensitivity
        effective_close = cfg.close_threshold * cfg.sensitivity

        if cfg.segment_grouping:
            hysteresis_labels = cls.apply_hysteresis(
                normalized,
                open_threshold=effective_open,
                close_threshold=effective_close,
            )
            episodes = cls._labels_to_episodes(
                hysteresis_labels,
                normalized,
                timestamps,
                min_duration=cfg.min_duration_seconds,
                cooldown=cfg.cooldown_seconds,
            )
        else:
            episodes = cls._labels_to_episodes(
                (normalized >= effective_open).astype(np.int_),
                normalized,
                timestamps,
                min_duration=cfg.min_duration_seconds,
                cooldown=cfg.cooldown_seconds,
            )

        logger.info(
            "score_to_episodes: {n} episode(s) from {pts} points",
            n=len(episodes),
            pts=len(normalized),
        )
        return episodes
