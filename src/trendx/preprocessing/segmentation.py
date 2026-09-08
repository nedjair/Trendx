from __future__ import annotations

from typing import cast

import numpy as np
import pandas as pd
from loguru import logger


class Segmenter:
    def split_by_time(
        self,
        df: pd.DataFrame,
        segment_size: str,
        overlap: str = "0s",
        ts_col: str = "ts",
    ) -> list[pd.DataFrame]:
        if df.empty:
            return []

        df = df.copy()
        df[ts_col] = pd.to_datetime(df[ts_col])
        df = df.sort_values(ts_col).reset_index(drop=True)

        size_delta = pd.Timedelta(segment_size)
        overlap_delta = pd.Timedelta(overlap)
        step_delta = size_delta - overlap_delta

        if step_delta <= pd.Timedelta(0):
            msg = f"Overlap ({overlap}) must be smaller than segment_size ({segment_size})"
            raise ValueError(msg)

        t_min = df[ts_col].min()
        t_max = df[ts_col].max()

        segments: list[pd.DataFrame] = []
        seg_start = t_min
        while seg_start < t_max:
            seg_end = seg_start + size_delta
            mask = (df[ts_col] >= seg_start) & (df[ts_col] < seg_end)
            segment = df[mask].copy()
            if not segment.empty:
                segments.append(segment)
            seg_start += step_delta

        logger.info(
            "Split into {n} time-based segments (size={size}, overlap={over})",
            n=len(segments),
            size=segment_size,
            over=overlap,
        )
        return segments

    def split_by_calendar(
        self,
        df: pd.DataFrame,
        period: str = "D",
        ts_col: str = "ts",
    ) -> dict[str, pd.DataFrame]:
        if df.empty:
            return {}

        df = df.copy()
        df[ts_col] = pd.to_datetime(df[ts_col])

        period_map: dict[str, str] = {
            "D": "daily",
            "W": "weekly",
            "M": "monthly",
            "Q": "quarterly",
            "Y": "yearly",
            "h": "hourly",
        }
        label = period_map.get(period, period)

        df["_period_key"] = df[ts_col].dt.to_period(period)
        groups: dict[str, pd.DataFrame] = {}

        for key, group in df.groupby("_period_key", sort=True):
            period_label = str(key)
            dropped = group.drop(columns=["_period_key"])
            groups[period_label] = dropped.reset_index(drop=True)

        df = df.drop(columns=["_period_key"])

        logger.info(
            "Split into {n} calendar-based segments (period={period})",
            n=len(groups),
            period=label,
        )
        return groups

    def split_train_test(
        self,
        df: pd.DataFrame,
        test_ratio: float = 0.2,
        method: str = "temporal",
        ts_col: str = "ts",
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        if df.empty:
            return pd.DataFrame(), pd.DataFrame()

        df = df.copy()
        df[ts_col] = pd.to_datetime(df[ts_col])
        df = df.sort_values(ts_col).reset_index(drop=True)

        if method == "temporal":
            split_idx = int(len(df) * (1.0 - test_ratio))
            split_idx = max(1, min(split_idx, len(df) - 1))
            train = df.iloc[:split_idx].reset_index(drop=True)
            test = df.iloc[split_idx:].reset_index(drop=True)
        elif method == "random":
            shuffled = df.sample(frac=1.0, random_state=42)
            split_idx = int(len(shuffled) * (1.0 - test_ratio))
            split_idx = max(1, min(split_idx, len(shuffled) - 1))
            train = shuffled.iloc[:split_idx].sort_values(ts_col).reset_index(drop=True)
            test = shuffled.iloc[split_idx:].sort_values(ts_col).reset_index(drop=True)
        else:
            msg = f"Unknown split method '{method}'. Use 'temporal' or 'random'."
            raise ValueError(msg)

        logger.info(
            "Train/test split ({method}): {train} train, {test} test ({ratio:.0%} test)",
            method=method,
            train=len(train),
            test=len(test),
            ratio=test_ratio,
        )
        return train, test

    def walk_forward_windows(
        self,
        df: pd.DataFrame,
        n_windows: int = 5,
        test_size: int = 48,
        step: int | None = None,
        ts_col: str = "ts",
    ) -> list[tuple[pd.DataFrame, pd.DataFrame]]:
        if df.empty:
            return []

        df = df.copy()
        df[ts_col] = pd.to_datetime(df[ts_col])
        df = df.sort_values(ts_col).reset_index(drop=True)

        if step is None:
            step = test_size

        total = len(df)
        min_train = test_size + 1
        windows: list[tuple[pd.DataFrame, pd.DataFrame]] = []

        for w in range(n_windows):
            test_end = total - (n_windows - w - 1) * step
            test_start = test_end - test_size

            if test_start < min_train:
                logger.warning(
                    "Window {w}: insufficient data ({start} < {min}), stopping",
                    w=w + 1,
                    start=test_start,
                    min=min_train,
                )
                break

            train = df.iloc[:test_start].reset_index(drop=True)
            test = df.iloc[test_start:test_end].reset_index(drop=True)
            windows.append((train, test))

        logger.info(
            "Created {n} walk-forward windows (test_size={ts}, step={step})",
            n=len(windows),
            ts=test_size,
            step=step,
        )
        return windows

    def sliding_windows(
        self,
        df: pd.DataFrame,
        window_size: int,
        step_size: int = 1,
        ts_col: str = "ts",
        value_col: str | None = None,
    ) -> tuple[np.ndarray, np.ndarray | None]:
        if df.empty or len(df) < window_size:
            logger.warning(
                "DataFrame too short ({n}) for window_size={ws}",
                n=len(df),
                ws=window_size,
            )
            empty_2d = np.empty((0, window_size))
            return empty_2d, None

        df = df.copy()
        df[ts_col] = pd.to_datetime(df[ts_col])
        df = df.sort_values(ts_col).reset_index(drop=True)

        if value_col is None:
            value_col = self._detect_value_col(df)

        if value_col not in df.columns:
            logger.warning("Value column '{col}' not found", col=value_col)
            empty_2d = np.empty((0, window_size))
            return empty_2d, None

        values = df[value_col].values
        n = len(values)

        n_windows = (n - window_size) // step_size + 1
        windows = np.array(
            [values[i : i + window_size] for i in range(0, n - window_size + 1, step_size)]
        )

        if n_windows > 0 and len(windows) > 0:
            targets = np.array(
                [
                    values[i + window_size] if i + window_size < n else np.nan
                    for i in range(0, n - window_size + 1, step_size)
                ]
            )
            targets = targets[~np.isnan(targets)]
        else:
            targets = None

        logger.info(
            "Created {n} sliding windows (size={ws}, step={ss})",
            n=len(windows),
            ws=window_size,
            ss=step_size,
        )
        return windows, targets

    @staticmethod
    def _detect_value_col(df: pd.DataFrame) -> str:
        for col in ("value", "dbl_v", "val", "double_value"):
            if col in df.columns:
                return col
        numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
        if numeric_cols:
            return cast(str, numeric_cols[0])
        return "value"

    def expand_window(
        self,
        df: pd.DataFrame,
        min_train_size: int = 100,
        step: int = 10,
        ts_col: str = "ts",
    ) -> list[tuple[pd.DataFrame, pd.DataFrame]]:
        if df.empty or len(df) < min_train_size + 1:
            return []

        df = df.copy()
        df[ts_col] = pd.to_datetime(df[ts_col])
        df = df.sort_values(ts_col).reset_index(drop=True)

        total = len(df)
        windows: list[tuple[pd.DataFrame, pd.DataFrame]] = []

        for end_train in range(min_train_size, total - 1, step):
            train = df.iloc[:end_train].reset_index(drop=True)
            test = df.iloc[[end_train]].reset_index(drop=True)
            windows.append((train, test))

        logger.info(
            "Created {n} expanding windows (min_train={min}, step={step})",
            n=len(windows),
            min=min_train_size,
            step=step,
        )
        return windows
