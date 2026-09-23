"""Generic forecast dataset builder (W86): request + resolved set -> X/Y/ds.

Pure assembly layer (no DB, no network, no training, no prediction):

    ForecastRequest + ResolvedFeatureSet + frames provider
        ↓ DatasetBuilder
    ForecastDataset (ds UTC-naive, Y = target, X = features)

Data access goes through the caller-supplied ``frame_provider``; the default
provider reuses ``TrainingService.fetch_training_frame`` (bounded existing
read). Resampling reuses ``Resampler``; quality reporting reuses
``DataQualityService``. Lag responsibility: the resolver declares lag, the
builder applies the shift exactly once (documented here).

No metric-name branching exists in this module.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import Any

import pandas as pd
from trendx.forecasting.contract import ForecastRequest
from trendx.forecasting.resolution import (
    ResolutionStatus,
    ResolvedFeatureSet,
)

FrameProvider = Callable[[str, str], pd.DataFrame]

_SUPPORTED_TRANSFORMATIONS = frozenset({"", "none", "identity"})


@dataclass
class ForecastDataset:
    """Canonical ML dataset: timestamps + target Y + features X + metadata."""

    ds: pd.Series
    target: pd.Series
    X: pd.DataFrame
    metadata: dict[str, Any] = field(default_factory=dict)


def _normalize_ts(frame: pd.DataFrame) -> pd.DataFrame:
    """UTC-naive canonical timestamps (same contract as W81/W83)."""
    out = frame.copy()
    out["ts"] = pd.to_datetime(out["ts"], utc=True).dt.tz_localize(None)
    return out.sort_values("ts").reset_index(drop=True)


class DatasetBuilder:
    """Assemble X/Y/ds from a resolved feature set (metric-generic)."""

    def __init__(
        self,
        frame_provider: FrameProvider | None = None,
        aggregation: Mapping[str, str] | str = "mean",
    ) -> None:
        self._provider = frame_provider or _default_provider
        self._aggregation = aggregation

    def build(
        self,
        request: ForecastRequest,
        resolved: ResolvedFeatureSet,
        execution_id: str = "",
        frames: Mapping[tuple[str, str], pd.DataFrame] | None = None,
    ) -> ForecastDataset:
        if not resolved.ok:
            msg = f"Unresolved feature set: {list(resolved.errors)}"
            raise ValueError(msg)
        if resolved.entity_id != request.entity_id:
            msg = "Resolved set entity does not match request entity"
            raise ValueError(msg)
        if resolved.target_metric != request.target_metric:
            msg = "Resolved set target does not match request target"
            raise ValueError(msg)
        from trendx.preprocessing.resampling import Resampler

        resampler = Resampler()
        provider = self._provider
        if frames is not None:
            snapshot = {k: v.copy() for k, v in frames.items()}

            def provider(entity_id: str, metric: str) -> pd.DataFrame:
                frame = snapshot.get((entity_id, metric))
                return frame.copy() if frame is not None else pd.DataFrame()

        target = self._series(
            resampler,
            request,
            request.entity_id,
            request.target_metric,
            "target",
            provider=provider,
        )
        if target.empty:
            msg = "Missing target: no usable rows"
            raise ValueError(msg)
        wide = pd.DataFrame({"ds": target["ds"], "Y": target["y"]})
        dropped_optional: list[str] = []
        for res in resolved.resolutions:
            if res.status is not ResolutionStatus.RESOLVED:
                if res.required:
                    msg = f"Missing required feature: {res.name!r}"
                    raise ValueError(msg)
                dropped_optional.append(res.name)
                continue
            series = self._series(
                resampler,
                request,
                res.entity_id,
                res.metric or request.target_metric,
                f"feature {res.name!r}",
                provider=provider,
            )
            col = self._apply_feature_ops(series, res.lag, res.transformation, res.name)
            wide = wide.merge(col, on="ds", how="outer")
        wide = wide.sort_values("ds").reset_index(drop=True)
        usable = wide.dropna(subset=["Y"]).reset_index(drop=True)
        if usable.empty:
            msg = "No usable rows after alignment (target all missing)"
            raise ValueError(msg)
        feature_cols = [c for c in usable.columns if c not in ("ds", "Y")]
        missingness = {c: float(usable[c].isna().mean()) for c in ("Y", *feature_cols)}
        metadata = {
            "tenant_id": request.tenant_id,
            "entity_id": request.entity_id,
            "target_metric": request.target_metric,
            "frequency": request.frequency,
            "horizon": request.horizon,
            "algorithm": request.algorithm.value,
            "feature_schema_fingerprint": resolved.schema_fingerprint,
            "feature_columns": feature_cols,
            "dropped_optional": dropped_optional,
            "usable_rows": int(len(usable)),
            "invalid_rows": int(len(wide) - len(usable)),
            "missingness": missingness,
            "execution_id": execution_id,
            "cutoff": str(usable["ds"].max()),
        }
        return ForecastDataset(
            ds=usable["ds"],
            target=usable["Y"].rename(request.target_metric),
            X=usable[feature_cols],
            metadata=metadata,
        )

    def _series(
        self,
        resampler: Any,
        request: ForecastRequest,
        entity_id: str,
        metric: str,
        what: str,
        provider: FrameProvider | None = None,
    ) -> pd.DataFrame:
        frame = (provider or self._provider)(entity_id, metric)
        if frame is None or frame.empty:
            return pd.DataFrame(columns=["ds", "y"])
        if "ts" not in frame.columns or "value" not in frame.columns:
            msg = f"Invalid frame for {what}: need ts/value columns"
            raise ValueError(msg)
        norm = _normalize_ts(frame[["ts", "value"]])
        method = (
            self._aggregation
            if isinstance(self._aggregation, str)
            else (self._aggregation.get(metric, "mean"))
        )
        out = resampler.resample(norm, frequency=request.frequency, method=method)
        out = out.rename(columns={"ts": "ds", "value": "y"})
        return out.sort_values("ds").reset_index(drop=True)

    @staticmethod
    def _apply_feature_ops(
        series: pd.DataFrame, lag: str, transformation: str, name: str
    ) -> pd.DataFrame:
        if (transformation or "") not in _SUPPORTED_TRANSFORMATIONS:
            msg = f"Unsupported transformation {transformation!r} for feature {name!r}"
            raise ValueError(msg)
        col = series.rename(columns={"y": name})[["ds", name]]
        lag = (lag or "").strip()
        if lag:
            try:
                delta = pd.Timedelta(lag)
            except ValueError as exc:
                msg = f"Invalid lag {lag!r} for feature {name!r}"
                raise ValueError(msg) from exc
            col = col.copy()
            col["ds"] = col["ds"] + delta
        return col


def _default_provider(entity_id: str, metric: str) -> pd.DataFrame:
    from trendx.services.training import TrainingService

    return TrainingService().fetch_training_frame(entity_id, metric)
