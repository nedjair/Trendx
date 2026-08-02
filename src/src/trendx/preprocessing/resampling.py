from __future__ import annotations

from typing import Any, Optional

import numpy as np
import pandas as pd
from loguru import logger


class Resampler:
    SUPPORTED_METHODS = {"mean", "sum", "min", "max", "count", "first", "last", "median", "std"}

    def __init__(self) -> None:
        self._detected_freq: str | None = None

    @property
    def detected_frequency(self) -> str | None:
        return self._detected_freq

    def detect_frequency(
        self,
        df: pd.DataFrame,
        ts_col: str = "ts",
    ) -> str:
        if df.empty or len(df) < 2:
            logger.warning("Cannot detect frequency: need at least 2 data points")
            return "1h"

        ts = pd.to_datetime(df[ts_col]).sort_values()
        deltas = ts.diff().dt.total_seconds().iloc[1:]

        if len(deltas) == 0:
            return "1h"

        median_delta = deltas.median()
        mode_result = deltas.mode()
        mode_delta = mode_result.iloc[0] if not mode_result.empty else median_delta

        self._detected_freq = self._seconds_to_freq(mode_delta)

        logger.debug(
            "Detected frequency: {freq} (median={med:.1f}s, mode={mode:.1f}s)",
            freq=self._detected_freq,
            med=median_delta,
            mode=mode_delta,
        )
        return self._detected_freq

    def resample(
        self,
        df: pd.DataFrame,
        frequency: str,
        method: str = "mean",
        ts_col: str = "ts",
        value_col: str | None = None,
    ) -> pd.DataFrame:
        if df.empty:
            logger.warning("Cannot resample empty DataFrame")
            return df

        if method not in self.SUPPORTED_METHODS:
            msg = f"Unsupported resample method '{method}'. Choose from {self.SUPPORTED_METHODS}"
            raise ValueError(msg)

        df = df.copy()
        df[ts_col] = pd.to_datetime(df[ts_col])

        if value_col is None:
            value_col = self._detect_value_col(df)

        ts_df = df.set_index(ts_col)
        numeric = ts_df.select_dtypes(include=[np.number])

        if value_col not in numeric.columns:
            if value_col in ts_df.columns:
                ts_df[value_col] = pd.to_numeric(ts_df[value_col], errors="coerce")
                numeric = ts_df[[value_col]]
            else:
                numeric = ts_df

        if numeric.empty:
            logger.warning("No numeric columns to resample")
            return pd.DataFrame(columns=[ts_col, value_col])

        agg_map: dict[str, Any] = {
            col: method for col in numeric.columns
        }

        resampled = numeric.resample(frequency).agg(agg_map)
        resampled = resampled.dropna(how="all").reset_index()
        resampled = resampled.rename(columns={ts_col: ts_col})

        logger.info(
            "Resampled to {freq} using {method}: {before} -> {after} points",
            freq=frequency,
            method=method,
            before=len(df),
            after=len(resampled),
        )
        return resampled

    def aggregate(
        self,
        df: pd.DataFrame,
        frequency: str,
        aggregations: dict[str, str | list[str]],
        ts_col: str = "ts",
    ) -> pd.DataFrame:
        if df.empty:
            return df

        df = df.copy()
        df[ts_col] = pd.to_datetime(df[ts_col])
        ts_df = df.set_index(ts_col)

        for col, agg in aggregations.items():
            if isinstance(agg, str):
                if agg not in self.SUPPORTED_METHODS:
                    msg = f"Unsupported aggregation '{agg}' for column '{col}'"
                    raise ValueError(msg)
            elif isinstance(agg, list):
                for a in agg:
                    if a not in self.SUPPORTED_METHODS:
                        msg = f"Unsupported aggregation '{a}' for column '{col}'"
                        raise ValueError(msg)

        resampled = ts_df.resample(frequency).agg(aggregations)
        resampled = resampled.dropna(how="all").reset_index()

        logger.info(
            "Aggregated to {freq} with {aggs}: {before} -> {after} points",
            freq=frequency,
            aggs=aggregations,
            before=len(df),
            after=len(resampled),
        )
        return resampled

    def interpolate_missing(
        self,
        df: pd.DataFrame,
        method: str = "linear",
        ts_col: str = "ts",
        value_col: str | None = None,
        limit: int | None = None,
    ) -> pd.DataFrame:
        if df.empty:
            return df

        df = df.copy()
        df[ts_col] = pd.to_datetime(df[ts_col])

        if value_col is None:
            value_col = self._detect_value_col(df)

        if value_col not in df.columns:
            logger.warning("Value column '{col}' not found, returning original", col=value_col)
            return df

        df = df.set_index(ts_col)
        full_range = pd.date_range(
            start=df.index.min(),
            end=df.index.max(),
            freq=self.detect_frequency(df.reset_index(), ts_col),
        )
        df = df.reindex(full_range)

        before_nulls = df[value_col].isna().sum()

        interp_kwargs: dict[str, Any] = {"method": method}
        if limit is not None:
            interp_kwargs["limit"] = limit

        df[value_col] = df[value_col].interpolate(**interp_kwargs)
        if method in ("ffill", "pad"):
            df[value_col] = df[value_col].ffill(limit=limit)
        elif method == "bfill":
            df[value_col] = df[value_col].bfill(limit=limit)

        after_nulls = df[value_col].isna().sum()
        filled = before_nulls - after_nulls

        df = df.reset_index().rename(columns={"index": ts_col})

        logger.info(
            "Interpolated {filled} nulls using '{method}' ({before} -> {after} nulls)",
            filled=filled,
            method=method,
            before=before_nulls,
            after=after_nulls,
        )
        return df

    def _detect_value_col(self, df: pd.DataFrame) -> str:
        for col in ("value", "dbl_v", "val", "double_value"):
            if col in df.columns:
                return col
        numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
        if numeric_cols:
            return numeric_cols[0]
        return "value"

    @staticmethod
    def _seconds_to_freq(seconds: float) -> str:
        if seconds < 1:
            return f"{int(seconds * 1000)}ms"
        if seconds < 60:
            if abs(seconds - round(seconds)) < 0.01:
                return f"{int(round(seconds))}s"
            return "1s"
        if seconds < 3600:
            minutes = seconds / 60
            if abs(minutes - round(minutes)) < 0.01:
                return f"{int(round(minutes))}min"
            return "1min"
        if seconds < 86400:
            hours = seconds / 3600
            if abs(hours - round(hours)) < 0.01:
                return f"{int(round(hours))}h"
            return "1h"
        days = seconds / 86400
        if abs(days - round(days)) < 0.01:
            return f"{int(round(days))}d"
        return "1d"

    def upsample(
        self,
        df: pd.DataFrame,
        frequency: str,
        method: str = "linear",
        ts_col: str = "ts",
        value_col: str | None = None,
    ) -> pd.DataFrame:
        resampled = self.resample(df, frequency, method="first", ts_col=ts_col, value_col=value_col)
        return self.interpolate_missing(
            resampled, method=method, ts_col=ts_col,
            value_col=value_col,
        )

    def downsample(
        self,
        df: pd.DataFrame,
        frequency: str,
        method: str = "mean",
        ts_col: str = "ts",
        value_col: str | None = None,
    ) -> pd.DataFrame:
        return self.resample(df, frequency, method=method, ts_col=ts_col, value_col=value_col)
