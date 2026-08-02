from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

import numpy as np
import pandas as pd
from loguru import logger
from scipy import stats as scipy_stats
from sqlalchemy import text

from trendx.database.connection import manager as db_manager
from trendx.database.repositories import BusinessEntityRepository


class DataQualityService:
    def __init__(
        self,
        iqr_multiplier: float = 1.5,
        zscore_threshold: float = 3.0,
        min_periods_ratio: float = 0.5,
    ) -> None:
        self._iqr_multiplier = iqr_multiplier
        self._zscore_threshold = zscore_threshold
        self._min_periods_ratio = min_periods_ratio

    def check_completeness(
        self,
        df: pd.DataFrame,
        expected_frequency: str,
    ) -> dict[str, Any]:
        if df.empty:
            return {
                "expected_points": 0,
                "actual_points": 0,
                "missing_points": 0,
                "completeness_ratio": 0.0,
                "gap_seconds": 0,
            }

        df = df.sort_values("ts")
        full_range = pd.date_range(
            start=df["ts"].min(),
            end=df["ts"].max(),
            freq=expected_frequency,
        )
        expected_total = len(full_range)
        actual_total = len(df)
        missing = max(0, expected_total - actual_total)
        ratio = actual_total / expected_total if expected_total > 0 else 0.0

        gaps_seconds = 0
        if len(df) > 1:
            deltas = df["ts"].diff().dt.total_seconds().iloc[1:]
            freq_seconds = self._parse_freq_seconds(expected_frequency)
            gap_deltas = deltas[deltas > freq_seconds * 1.5]
            if not gap_deltas.empty:
                gaps_seconds = int(gap_deltas.sum())

        return {
            "expected_points": expected_total,
            "actual_points": actual_total,
            "missing_points": missing,
            "completeness_ratio": round(ratio, 4),
            "gap_seconds": gaps_seconds,
        }

    def check_nulls(self, df: pd.DataFrame) -> dict[str, Any]:
        if df.empty:
            return {"null_count": 0, "null_ratio": 0.0, "null_columns": {}}

        value_col = self._value_column(df)
        if value_col is None:
            return {"null_count": 0, "null_ratio": 0.0, "null_columns": {}}

        nulls = df[value_col].isna().sum()
        null_ratio = nulls / len(df) if len(df) > 0 else 0.0

        col_nulls: dict[str, int] = {}
        for col in df.columns:
            n = int(df[col].isna().sum())
            if n > 0:
                col_nulls[col] = n

        return {
            "null_count": int(nulls),
            "null_ratio": round(null_ratio, 4),
            "null_columns": col_nulls,
        }

    def check_duplicates(self, df: pd.DataFrame) -> dict[str, Any]:
        if df.empty:
            return {"duplicate_count": 0, "duplicate_ratio": 0.0}

        if "ts" not in df.columns:
            return {"duplicate_count": 0, "duplicate_ratio": 0.0}

        dups = df["ts"].duplicated(keep="first").sum()
        dup_ratio = dups / len(df) if len(df) > 0 else 0.0

        return {
            "duplicate_count": int(dups),
            "duplicate_ratio": round(dup_ratio, 4),
        }

    def check_outliers(
        self,
        df: pd.DataFrame,
        method: str = "iqr",
    ) -> dict[str, Any]:
        if df.empty:
            return {"outlier_count": 0, "outlier_ratio": 0.0, "method": method}

        value_col = self._value_column(df)
        if value_col is None:
            return {"outlier_count": 0, "outlier_ratio": 0.0, "method": method}

        values = df[value_col].dropna().values
        if len(values) == 0:
            return {"outlier_count": 0, "outlier_ratio": 0.0, "method": method}

        if method == "iqr":
            q1, q3 = np.percentile(values, [25, 75])
            iqr = q3 - q1
            lower = q1 - self._iqr_multiplier * iqr
            upper = q3 + self._iqr_multiplier * iqr
            outliers = (values < lower) | (values > upper)
        elif method == "zscore":
            z = np.abs(scipy_stats.zscore(values, nan_policy="omit"))
            outliers = z > self._zscore_threshold
        else:
            logger.warning("Unknown outlier method '{method}', using IQR", method=method)
            return self.check_outliers(df, method="iqr")

        outlier_count = int(outliers.sum())
        outlier_ratio = outlier_count / len(values) if len(values) > 0 else 0.0

        return {
            "outlier_count": outlier_count,
            "outlier_ratio": round(outlier_ratio, 4),
            "method": method,
        }

    def check_range(
        self,
        df: pd.DataFrame,
        min_val: float | None = None,
        max_val: float | None = None,
    ) -> dict[str, Any]:
        if df.empty:
            return {"below_min": 0, "above_max": 0, "in_range_ratio": 0.0}

        value_col = self._value_column(df)
        if value_col is None:
            return {"below_min": 0, "above_max": 0, "in_range_ratio": 0.0}

        values = df[value_col].dropna()
        if len(values) == 0:
            return {"below_min": 0, "above_max": 0, "in_range_ratio": 0.0}

        below = int((values < min_val).sum()) if min_val is not None else 0
        above = int((values > max_val).sum()) if max_val is not None else 0
        in_range = len(values) - below - above
        ratio = in_range / len(values) if len(values) > 0 else 0.0

        return {
            "below_min": below,
            "above_max": above,
            "in_range_ratio": round(ratio, 4),
        }

    def check_frequency(
        self,
        df: pd.DataFrame,
        expected_freq: str,
    ) -> dict[str, Any]:
        if df.empty or len(df) < 2:
            return {
                "detected_frequency": None,
                "expected_frequency": expected_freq,
                "is_regular": False,
                "irregular_ratio": 0.0,
            }

        df = df.sort_values("ts")
        deltas = df["ts"].diff().dt.total_seconds().iloc[1:]
        if len(deltas) == 0:
            return {
                "detected_frequency": None,
                "expected_frequency": expected_freq,
                "is_regular": False,
                "irregular_ratio": 0.0,
            }

        median_delta = deltas.median()
        detected_freq = self._seconds_to_freq(median_delta)
        expected_seconds = self._parse_freq_seconds(expected_freq)

        tolerance = expected_seconds * 0.5
        irregular = ((deltas - expected_seconds).abs() > tolerance).sum()
        irregular_ratio = irregular / len(deltas) if len(deltas) > 0 else 0.0

        return {
            "detected_frequency": detected_freq,
            "expected_frequency": expected_freq,
            "is_regular": irregular_ratio < 0.1,
            "irregular_ratio": round(irregular_ratio, 4),
        }

    def check_type_mismatch(
        self,
        df: pd.DataFrame,
        expected_type: str = "float",
    ) -> dict[str, Any]:
        if df.empty:
            return {"mismatch_count": 0, "mismatch_ratio": 0.0, "actual_types": {}}

        value_col = self._value_column(df)
        if value_col is None:
            return {"mismatch_count": 0, "mismatch_ratio": 0.0, "actual_types": {}}

        actual_types: dict[str, int] = {}
        mismatch_count = 0

        for val in df[value_col].dropna():
            tname = type(val).__name__
            actual_types[tname] = actual_types.get(tname, 0) + 1
            if expected_type == "float" and not isinstance(val, (int, float, np.floating)):
                mismatch_count += 1
            elif expected_type == "int" and not isinstance(val, (int, np.integer)):
                mismatch_count += 1
            elif expected_type == "str" and not isinstance(val, str):
                mismatch_count += 1

        total = len(df[value_col].dropna())
        mismatch_ratio = mismatch_count / total if total > 0 else 0.0

        return {
            "mismatch_count": mismatch_count,
            "mismatch_ratio": round(mismatch_ratio, 4),
            "actual_types": actual_types,
        }

    def _value_column(self, df: pd.DataFrame) -> str | None:
        for col in ("value", "dbl_v", "val", "double_value"):
            if col in df.columns:
                return col
        numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
        if numeric_cols:
            return numeric_cols[0]
        return None

    @staticmethod
    def _parse_freq_seconds(freq: str) -> float:
        freq = freq.lower().strip()
        if freq.endswith("s"):
            return float(freq.rstrip("s"))
        if freq.endswith("min"):
            return float(freq.replace("min", "")) * 60
        if freq.endswith("h"):
            return float(freq.rstrip("h")) * 3600
        if freq.endswith("d"):
            return float(freq.rstrip("d")) * 86400
        if freq.endswith("w"):
            return float(freq.rstrip("w")) * 604800
        try:
            return float(freq)
        except ValueError:
            return 3600.0

    @staticmethod
    def _seconds_to_freq(seconds: float) -> str:
        if seconds < 60:
            return f"{int(seconds)}s"
        if seconds < 3600:
            return f"{int(seconds // 60)}min"
        if seconds < 86400:
            return f"{int(seconds // 3600)}h"
        return f"{int(seconds // 86400)}d"

    def generate_report(
        self,
        entity_id: str,
        metric_key: str,
        start_ts: datetime,
        end_ts: datetime,
        df: pd.DataFrame,
        expected_frequency: str = "1h",
        min_val: float | None = None,
        max_val: float | None = None,
        expected_type: str = "float",
    ) -> dict[str, Any]:
        logger.info(
            "Generating quality report for {eid}/{key} ({start} -> {end})",
            eid=entity_id[:12],
            key=metric_key,
            start=start_ts.isoformat(),
            end=end_ts.isoformat(),
        )

        completeness = self.check_completeness(df, expected_frequency)
        nulls = self.check_nulls(df)
        duplicates = self.check_duplicates(df)
        outliers = self.check_outliers(df, method="iqr")
        range_check = self.check_range(df, min_val, max_val)
        freq_check = self.check_frequency(df, expected_frequency)
        type_check = self.check_type_mismatch(df, expected_type)

        numeric_values = df.select_dtypes(include=[np.number]).values.flatten()
        numeric_values = numeric_values[~np.isnan(numeric_values)]

        report = {
            "entity_id": entity_id,
            "metric_key": metric_key,
            "period_start": start_ts.isoformat(),
            "period_end": end_ts.isoformat(),
            "total_points_expected": completeness["expected_points"],
            "total_points_actual": completeness["actual_points"],
            "completeness_ratio": completeness["completeness_ratio"],
            "null_ratio": nulls["null_ratio"],
            "duplicate_ratio": duplicates["duplicate_ratio"],
            "outlier_ratio": outliers["outlier_ratio"],
            "outlier_method": outliers["method"],
            "mean_value": round(float(np.mean(numeric_values)), 4) if len(numeric_values) > 0 else None,
            "stddev_value": round(float(np.std(numeric_values)), 4) if len(numeric_values) > 0 else None,
            "min_value": round(float(np.min(numeric_values)), 4) if len(numeric_values) > 0 else None,
            "max_value": round(float(np.max(numeric_values)), 4) if len(numeric_values) > 0 else None,
            "frequency": freq_check.get("detected_frequency"),
            "is_regular": freq_check.get("is_regular", False),
            "range_in_range_ratio": range_check["in_range_ratio"],
            "type_mismatch_ratio": type_check["mismatch_ratio"],
            "gap_seconds": completeness["gap_seconds"],
        }

        self._store_report(report)

        issues: list[dict[str, Any]] = []
        if nulls["null_ratio"] > 0.01:
            issues.append(self._build_issue(
                entity_id, metric_key, "null_values",
                nulls["null_ratio"], "MEDIUM" if nulls["null_ratio"] > 0.05 else "LOW",
                start_ts, end_ts, nulls["null_count"],
            ))
        if duplicates["duplicate_ratio"] > 0.0:
            issues.append(self._build_issue(
                entity_id, metric_key, "duplicate_timestamps",
                duplicates["duplicate_ratio"], "LOW",
                start_ts, end_ts, duplicates["duplicate_count"],
            ))
        if outliers["outlier_ratio"] > 0.01:
            issues.append(self._build_issue(
                entity_id, metric_key, "statistical_outliers",
                outliers["outlier_ratio"], "MEDIUM" if outliers["outlier_ratio"] > 0.05 else "LOW",
                start_ts, end_ts, outliers["outlier_count"],
            ))
        if completeness["completeness_ratio"] < 0.95:
            issues.append(self._build_issue(
                entity_id, metric_key, "incomplete_data",
                1.0 - completeness["completeness_ratio"], "HIGH" if completeness["completeness_ratio"] < 0.8 else "MEDIUM",
                start_ts, end_ts, completeness["missing_points"],
            ))
        if range_check["in_range_ratio"] < 0.95:
            issues.append(self._build_issue(
                entity_id, metric_key, "out_of_range",
                1.0 - range_check["in_range_ratio"], "HIGH",
                start_ts, end_ts, range_check["below_min"] + range_check["above_max"],
            ))
        if type_check["mismatch_ratio"] > 0.0:
            issues.append(self._build_issue(
                entity_id, metric_key, "type_mismatch",
                type_check["mismatch_ratio"], "MEDIUM",
                start_ts, end_ts, type_check["mismatch_count"],
            ))

        for issue in issues:
            self._store_issue(issue, report_id=None)

        report["issues"] = issues
        logger.info(
            "Quality report for {eid}/{key}: {issues} issue(s) found",
            eid=entity_id[:12],
            key=metric_key,
            issues=len(issues),
        )
        return report

    @staticmethod
    def _build_issue(
        entity_id: str,
        metric_key: str,
        issue_type: str,
        severity_ratio: float,
        severity: str,
        ts_start: datetime,
        ts_end: datetime,
        affected_points: int,
    ) -> dict[str, Any]:
        return {
            "entity_id": entity_id,
            "metric_key": metric_key,
            "issue_type": issue_type,
            "severity": severity,
            "ts_start": ts_start,
            "ts_end": ts_end,
            "affected_points": affected_points,
            "description": f"{issue_type}: {affected_points} points affected "
                           f"({severity_ratio:.2%})",
            "resolution_status": "OPEN",
        }

    def _store_report(self, report: dict[str, Any]) -> int | None:
        engine = db_manager.get_engine("analytics")
        stmt = text(
            """
            INSERT INTO data_quality (
                period, entity_id, metric_key, period_length,
                expected_points, actual_points, null_points,
                duplicate_points, gap_points, outlier_points,
                completeness_ratio, min_v, max_v, avg_v, stddev_v
            ) VALUES (
                :period, :entity_id, :metric_key, :period_length,
                :expected_points, :actual_points, :null_points,
                :duplicate_points, :gap_points, :outlier_points,
                :completeness_ratio, :min_v, :max_v, :avg_v, :stddev_v
            ) ON CONFLICT (period, entity_id, metric_key, period_length)
              DO UPDATE SET
                expected_points = EXCLUDED.expected_points,
                actual_points = EXCLUDED.actual_points,
                null_points = EXCLUDED.null_points,
                duplicate_points = EXCLUDED.duplicate_points,
                gap_points = EXCLUDED.gap_points,
                outlier_points = EXCLUDED.outlier_points,
                completeness_ratio = EXCLUDED.completeness_ratio,
                min_v = EXCLUDED.min_v,
                max_v = EXCLUDED.max_v,
                avg_v = EXCLUDED.avg_v,
                stddev_v = EXCLUDED.stddev_v
            """
        )
        period_start = datetime.fromisoformat(report["period_start"])
        period_end = datetime.fromisoformat(report["period_end"])
        period_mid = period_start + (period_end - period_start) / 2

        params = {
            "period": period_mid,
            "entity_id": report["entity_id"],
            "metric_key": report["metric_key"],
            "period_length": "custom",
            "expected_points": report["total_points_expected"],
            "actual_points": report["total_points_actual"],
            "null_points": int(report.get("null_ratio", 0) * (report["total_points_actual"] or 0)),
            "duplicate_points": int(report.get("duplicate_ratio", 0) * (report["total_points_actual"] or 0)),
            "gap_points": report.get("gap_seconds", 0),
            "outlier_points": int(report.get("outlier_ratio", 0) * (report["total_points_actual"] or 0)),
            "completeness_ratio": report["completeness_ratio"],
            "min_v": report["min_value"],
            "max_v": report["max_value"],
            "avg_v": report["mean_value"],
            "stddev_v": report["stddev_value"],
        }

        try:
            with engine.begin() as conn:
                result = conn.execute(stmt, params)
                return result.rowcount
        except Exception as exc:
            logger.warning(
                "Failed to store quality report for {eid}/{key}: {exc}",
                eid=report["entity_id"][:12],
                key=report["metric_key"],
                exc=exc,
            )
            return None

    # TODO: data_quality_issue table does not exist in Trendz 1.15.0 schema.
    def _store_issue(
        self,
        issue: dict[str, Any],
        report_id: int | None = None,
    ) -> int | None:
        logger.debug(
            "Skipping data_quality_issue insert for {eid}/{key} (table missing)",
            eid=issue["entity_id"][:12],
            key=issue["metric_key"],
        )
        return 0

    def fix_issues(
        self,
        df: pd.DataFrame,
        config: dict[str, Any] | None = None,
    ) -> pd.DataFrame:
        if df.empty:
            return df

        cfg = config or {}
        result = df.copy()

        if cfg.get("remove_duplicates", True) and "ts" in result.columns:
            before = len(result)
            result = result.drop_duplicates(subset=["ts"], keep="first")
            removed = before - len(result)
            if removed:
                logger.info("Removed {n} duplicate rows", n=removed)

        value_col = self._value_column(result)
        if value_col is None:
            return result

        null_method = cfg.get("null_fill_method", "interpolate")
        if null_method == "interpolate":
            result[value_col] = result[value_col].interpolate(method="linear", limit=cfg.get("interpolate_limit", 3))
        elif null_method == "ffill":
            result[value_col] = result[value_col].ffill(limit=cfg.get("fill_limit", 3))
        elif null_method == "bfill":
            result[value_col] = result[value_col].bfill(limit=cfg.get("fill_limit", 3))
        elif null_method == "drop":
            result = result.dropna(subset=[value_col])

        if cfg.get("clip_outliers", False):
            method = cfg.get("outlier_method", "iqr")
            values = result[value_col].dropna().values
            if len(values) > 0:
                if method == "iqr":
                    q1, q3 = np.percentile(values, [25, 75])
                    iqr = q3 - q1
                    lower = q1 - self._iqr_multiplier * iqr
                    upper = q3 + self._iqr_multiplier * iqr
                else:
                    mean = np.mean(values)
                    std = np.std(values)
                    lower = mean - self._zscore_threshold * std
                    upper = mean + self._zscore_threshold * std

                result[value_col] = result[value_col].clip(lower=lower, upper=upper)
                logger.info(
                    "Clipped outliers to [{lower:.2f}, {upper:.2f}]",
                    lower=lower,
                    upper=upper,
                )

        if cfg.get("remove_out_of_range", False):
            min_val = cfg.get("min_val")
            max_val = cfg.get("max_val")
            if min_val is not None:
                result = result[result[value_col] >= min_val]
            if max_val is not None:
                result = result[result[value_col] <= max_val]

        result = result.reset_index(drop=True)
        return result

    def full_check(
        self,
        df: pd.DataFrame,
        expected_frequency: str = "1h",
        min_val: float | None = None,
        max_val: float | None = None,
        expected_type: str = "float",
    ) -> dict[str, Any]:
        return {
            "completeness": self.check_completeness(df, expected_frequency),
            "nulls": self.check_nulls(df),
            "duplicates": self.check_duplicates(df),
            "outliers": self.check_outliers(df, method="iqr"),
            "range": self.check_range(df, min_val, max_val),
            "frequency": self.check_frequency(df, expected_frequency),
            "type": self.check_type_mismatch(df, expected_type),
        }
