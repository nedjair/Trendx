from __future__ import annotations

from collections.abc import Generator
from typing import Any, ClassVar

import numpy as np
import pandas as pd
from loguru import logger
from scipy import stats as scipy_stats
from sklearn.preprocessing import MinMaxScaler, RobustScaler, StandardScaler


class FeatureExtractor:
    """Extract statistical features from time series windows for anomaly detection."""

    SCALER_TYPES: ClassVar[dict[str, type]] = {
        "standard": StandardScaler,
        "robust": RobustScaler,
        "minmax": MinMaxScaler,
    }

    def __init__(self, window_size: int = 24, step_size: int | None = None) -> None:
        if window_size < 2:
            msg = f"window_size must be >= 2, got {window_size}"
            raise ValueError(msg)
        self._window_size = window_size
        self._step_size = step_size or window_size

    @property
    def window_size(self) -> int:
        return self._window_size

    @property
    def step_size(self) -> int:
        return self._step_size

    def create_windows(
        self,
        series: pd.Series,
        window_size: int | None = None,
        step_size: int | None = None,
    ) -> Generator[tuple[pd.Series, int, int], None, None]:
        """Yield (window, start_index, end_index) tuples from a series.

        Parameters
        ----------
        series : pd.Series
            Time series values with DatetimeIndex or integer index.
        window_size : int, optional
            Number of points per window (defaults to instance value).
        step_size : int, optional
            Step between window starts (defaults to instance value).

        Yields
        ------
        tuple of (pd.Series, int, int)
            Window slice, start index, end index.
        """
        w = window_size or self._window_size
        s = step_size or self._step_size
        n = len(series)
        if n < w:
            logger.warning("Series length ({n}) < window size ({w}), returning empty", n=n, w=w)
            return

        for start in range(0, n - w + 1, s):
            end = start + w
            yield series.iloc[start:end], start, end

    def _compute_window_features(
        self, window: pd.Series, prev_mean: float | None = None
    ) -> dict[str, float]:
        """Compute all required features for a single window."""
        values = window.values.astype(np.float64)
        n_total = len(values)
        nan_mask = np.isnan(values)
        n_nan = int(nan_mask.sum())
        finite = values[~nan_mask]

        features: dict[str, float] = {}

        if n_nan == n_total:
            features.update(
                {
                    "mean": 0.0,
                    "std": 0.0,
                    "min": 0.0,
                    "max": 0.0,
                    "slope": 0.0,
                    "amplitude": 0.0,
                    "spectral_energy": 0.0,
                    "diff_from_previous": 0.0 if prev_mean is not None else 0.0,
                    "nan_ratio": 1.0,
                    "kurtosis": 0.0,
                    "skewness": 0.0,
                    "percentile_25": 0.0,
                    "percentile_50": 0.0,
                    "percentile_75": 0.0,
                }
            )
            logger.debug("Window with all NaN values")
            return features

        mean_v = float(np.mean(finite))
        std_v = float(np.std(finite, ddof=1)) if len(finite) > 1 else 0.0
        min_v = float(np.min(finite))
        max_v = float(np.max(finite))

        if len(finite) > 1:
            x = np.arange(len(finite), dtype=np.float64)
            slope, _ = np.polyfit(x, finite, 1)
        else:
            slope = 0.0

        fft_vals = np.fft.fft(finite - mean_v)
        spectral_energy = float(np.sum(np.abs(fft_vals) ** 2))

        diff = 0.0
        if prev_mean is not None:
            diff = mean_v - prev_mean

        kurt_val = float(scipy_stats.kurtosis(finite, bias=False)) if len(finite) >= 4 else 0.0
        skew_val = float(scipy_stats.skew(finite, bias=False)) if len(finite) >= 3 else 0.0
        q25, q50, q75 = (float(v) for v in np.percentile(finite, [25, 50, 75]))

        features["mean"] = mean_v
        features["std"] = std_v
        features["min"] = min_v
        features["max"] = max_v
        features["slope"] = slope
        features["amplitude"] = max_v - min_v
        features["spectral_energy"] = spectral_energy
        features["diff_from_previous"] = diff
        features["nan_ratio"] = n_nan / n_total if n_total > 0 else 0.0
        features["kurtosis"] = kurt_val
        features["skewness"] = skew_val
        features["percentile_25"] = q25
        features["percentile_50"] = q50
        features["percentile_75"] = q75

        return features

    def extract_features(
        self,
        series: pd.Series,
        window_size: int | None = None,
    ) -> pd.DataFrame:
        """Extract one feature vector per sliding window.

        Parameters
        ----------
        series : pd.Series
            Time series values with DatetimeIndex or integer index.
        window_size : int, optional
            Override the default window size.

        Returns
        -------
        pd.DataFrame
            Rows indexed by window midpoint, columns are feature names.
        """
        w = window_size or self._window_size
        rows: list[dict[str, Any]] = []
        prev_mean: float | None = None

        for window, start, end in self.create_windows(series, window_size=w):
            features = self._compute_window_features(window, prev_mean=prev_mean)
            prev_mean = features["mean"]

            mid_idx = start + (end - start) // 2
            if isinstance(series.index, pd.DatetimeIndex):
                try:
                    idx = series.index[mid_idx]
                except IndexError:
                    idx = mid_idx
            else:
                idx = mid_idx

            features["_start"] = start
            features["_end"] = end
            features["_index"] = idx
            rows.append(features)

        if not rows:
            logger.warning("No windows extracted from series of length {n}", n=len(series))
            return pd.DataFrame()

        df = pd.DataFrame(rows)
        df = df.set_index("_index")
        df.index.name = "timestamp" if isinstance(series.index, pd.DatetimeIndex) else "index"
        logger.debug(
            "Extracted {n} windows with {features} features from series of length {len}",
            n=len(df),
            features=len(df.columns) - 2,
            len=len(series),
        )
        return df

    def extract_features_multivariate(
        self,
        df: pd.DataFrame,
        window_size: int | None = None,
    ) -> pd.DataFrame:
        """Extract features from multiple metric columns.

        Each column is windowed independently and the resulting feature
        vectors are concatenated side-by-side, prefixed by the column name.

        Parameters
        ----------
        df : pd.DataFrame
            Each column is a metric (time series).
        window_size : int, optional
            Override the default window size.

        Returns
        -------
        pd.DataFrame
            One row per aligned window midpoint. Column names follow the
            pattern ``<metric>_<feature>``.
        """
        w = window_size or self._window_size
        all_features: list[pd.DataFrame] = []

        for col in df.columns:
            series = df[col].dropna()
            if len(series) < w:
                logger.warning(
                    "Column '{col}' has {n} points (< {w}), skipping",
                    col=col,
                    n=len(series),
                    w=w,
                )
                continue
            feat = self.extract_features(series, window_size=w)
            feat = feat.rename(
                columns=lambda c, _col=col: f"{_col}_{c}" if not c.startswith("_") else c
            )
            all_features.append(feat)

        if not all_features:
            logger.warning("No valid columns for multivariate feature extraction")
            return pd.DataFrame()

        merged = pd.concat(all_features, axis=1)
        logger.info(
            "Extracted multivariate features: {rows} windows, {cols} columns",
            rows=len(merged),
            cols=len(merged.columns),
        )
        return merged

    def normalize_features(
        self,
        features_df: pd.DataFrame,
        method: str = "robust",
    ) -> pd.DataFrame:
        """Normalize extracted features in-place.

        Parameters
        ----------
        features_df : pd.DataFrame
            Output of ``extract_features`` or ``extract_features_multivariate``.
        method : str
            One of ``"robust"``, ``"standard"``, or ``"minmax"``.

        Returns
        -------
        pd.DataFrame
            Copy with numeric feature columns scaled.
        """
        if features_df.empty:
            return features_df

        scaler_cls = self.SCALER_TYPES.get(method)
        if scaler_cls is None:
            msg = f"Unknown normalization method '{method}'. Use 'robust', 'standard', or 'minmax'."
            raise ValueError(msg)

        feature_cols = features_df.select_dtypes(include=[np.number]).columns
        internal_cols = {"_start", "_end"}
        cols_to_scale = [c for c in feature_cols if c not in internal_cols]

        if not cols_to_scale:
            logger.warning("No feature columns to normalize")
            return features_df.copy()

        scaler = scaler_cls()
        result = features_df.copy()
        values = result[cols_to_scale].values.astype(np.float64)

        finite_mask = ~np.isnan(values).all(axis=1)
        if finite_mask.sum() == 0:
            logger.warning("All values NaN, returning unnormalized")
            return result

        scaler.fit(values[finite_mask])
        scaled = np.full_like(values, np.nan, dtype=np.float64)
        scaled[finite_mask] = scaler.transform(values[finite_mask])
        result[cols_to_scale] = scaled

        logger.info(
            "Normalized {n} windows, {c} features using {method}",
            n=len(result),
            c=len(cols_to_scale),
            method=method,
        )
        return result
