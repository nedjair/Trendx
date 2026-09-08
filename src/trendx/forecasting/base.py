from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Protocol, Self, runtime_checkable

import numpy as np
import numpy.typing as npt
from loguru import logger


class Strategy(Enum):
    PER_DEVICE = auto()
    PER_PROFILE = auto()
    GLOBAL = auto()
    AUTO = auto()


@runtime_checkable
class ForecastModel(Protocol):
    def fit(self, data: Any, *, context: dict[str, Any] | None = None) -> Self: ...

    def predict(self, horizon: int, *, context: dict[str, Any] | None = None) -> ForecastResult: ...

    def save(self, path: str) -> None: ...

    @classmethod
    def load(cls, path: str) -> Self: ...


@dataclass
class ForecastMetrics:
    mae: float = 0.0
    rmse: float = 0.0
    smape: float = 0.0
    mape: float = 0.0
    coverage: float = 0.0
    interval_width: float = 0.0
    bias: float = 0.0
    train_duration: float = 0.0
    inference_duration: float = 0.0

    def to_dict(self) -> dict[str, float]:
        return {
            "mae": self.mae,
            "rmse": self.rmse,
            "smape": self.smape,
            "mape": self.mape,
            "coverage": self.coverage,
            "interval_width": self.interval_width,
            "bias": self.bias,
            "train_duration": self.train_duration,
            "inference_duration": self.inference_duration,
        }

    @classmethod
    def from_dict(cls, data: dict[str, float]) -> ForecastMetrics:
        return cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})


@dataclass
class ForecastResult:
    values: npt.NDArray[np.float64]
    lower_bound: npt.NDArray[np.float64]
    upper_bound: npt.NDArray[np.float64]
    timestamps: npt.NDArray[np.datetime64] | None = None
    model_name: str = ""
    metrics: ForecastMetrics = field(default_factory=ForecastMetrics)

    def to_dict(self) -> dict[str, Any]:
        return {
            "values": self.values.tolist(),
            "lower_bound": self.lower_bound.tolist(),
            "upper_bound": self.upper_bound.tolist(),
            "timestamps": (
                [str(ts) for ts in self.timestamps] if self.timestamps is not None else None
            ),
            "model_name": self.model_name,
            "metrics": self.metrics.to_dict(),
        }


def compute_metrics(
    y_true: npt.NDArray[np.float64],
    y_pred: npt.NDArray[np.float64],
    y_lower: npt.NDArray[np.float64] | None = None,
    y_upper: npt.NDArray[np.float64] | None = None,
) -> ForecastMetrics:
    y_true = np.asarray(y_true, dtype=np.float64).ravel()
    y_pred = np.asarray(y_pred, dtype=np.float64).ravel()

    if len(y_true) == 0 or len(y_pred) == 0:
        logger.warning("Empty arrays passed to compute_metrics")
        return ForecastMetrics()

    n = len(y_true)

    abs_error = np.abs(y_true - y_pred)
    mae = float(np.mean(abs_error))

    rmse = float(np.sqrt(np.mean((y_true - y_pred) ** 2)))

    denominator_smape = np.abs(y_true) + np.abs(y_pred)
    denominator_smape_safe = np.where(denominator_smape < 1e-12, 1e-12, denominator_smape)
    smape = float(np.mean(2.0 * abs_error / denominator_smape_safe) * 100.0)

    denominator_mape = np.abs(y_true)
    denominator_mape_safe = np.where(denominator_mape < 1e-12, 1e-12, denominator_mape)
    mape = float(np.mean(abs_error / denominator_mape_safe) * 100.0)

    bias = float(np.mean(y_pred - y_true))

    coverage = 0.0
    interval_width = 0.0
    if y_lower is not None and y_upper is not None:
        y_lower = np.asarray(y_lower, dtype=np.float64).ravel()
        y_upper = np.asarray(y_upper, dtype=np.float64).ravel()
        if len(y_lower) == n and len(y_upper) == n:
            inside = np.logical_and(y_true >= y_lower, y_true <= y_upper)
            coverage = float(np.mean(inside) * 100.0)
            interval_width = float(np.mean(y_upper - y_lower))

    return ForecastMetrics(
        mae=mae,
        rmse=rmse,
        smape=smape,
        mape=mape,
        coverage=coverage,
        interval_width=interval_width,
        bias=bias,
    )
