"""Generic evaluation + model selection (W92): temporal backtest, no leakage.

    ForecastDataset -> walk-forward folds (train < validation, no shuffle)
        -> per-candidate ForecastModel fit/predict -> ForecastMetrics
        -> deterministic selection (primary metric, documented tie-break)

No metric branching, no scheduler/worker involvement, no persistence,
no external tracking, no network. Candidate models arrive via injected loaders only.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

import numpy as np
import numpy.typing as npt
import pandas as pd
from trendx.forecasting.base import compute_metrics
from trendx.forecasting.registry import RegisteredModel, check_compatibility

NO_VALID_CANDIDATE = "no-valid-candidate"


@dataclass(frozen=True)
class CandidateSpec:
    model: RegisteredModel
    loader: Callable[[RegisteredModel], Any]


@dataclass(frozen=True)
class FoldResult:
    fold_index: int
    train_end: str
    test_start: str
    test_end: str
    scores: dict[str, float] = field(default_factory=dict)
    status: str = "ok"
    error: str = ""


@dataclass(frozen=True)
class CandidateEvaluation:
    model_id: str
    algorithm: str
    folds: tuple[FoldResult, ...] = ()
    mean_scores: dict[str, float] = field(default_factory=dict)
    valid_folds: int = 0
    status: str = "ok"


@dataclass(frozen=True)
class SelectionResult:
    winner_model_id: str | None
    reason: str
    criterion: str = "mae"
    evaluations: tuple[CandidateEvaluation, ...] = ()
    detail: str = ""


def _finite_scores(scores: dict[str, float]) -> bool:
    return all(np.isfinite(v) for v in scores.values())


def walk_folds(
    ds: pd.Series, n_folds: int = 3, test_size: int = 24
) -> list[tuple[npt.NDArray[np.intp], npt.NDArray[np.intp]]]:
    """Temporal index folds: train strictly before validation, no shuffle."""
    n = len(ds)
    if n_folds <= 0 or test_size <= 0:
        msg = f"Invalid fold config: n_folds={n_folds} test_size={test_size}"
        raise ValueError(msg)
    folds: list[tuple[npt.NDArray[np.intp], npt.NDArray[np.intp]]] = []
    for w in range(n_folds):
        test_end = n - (n_folds - w - 1) * test_size
        test_start = test_end - test_size
        if test_start < test_size + 1:
            break
        folds.append((np.arange(test_start), np.arange(test_start, test_end)))
    return folds


def evaluate_candidate(
    spec: CandidateSpec,
    frame: pd.DataFrame,
    n_folds: int = 3,
    test_size: int = 24,
    primary: str = "mae",
) -> CandidateEvaluation:
    """Backtest one candidate on (ds, y) frame; failures isolated per fold."""
    _ = primary
    clean = frame.dropna(subset=["y"]).reset_index(drop=True)
    folds = walk_folds(clean["ds"], n_folds=n_folds, test_size=test_size)
    results: list[FoldResult] = []
    for i, (train_idx, test_idx) in enumerate(folds):
        train = clean.iloc[train_idx]
        test = clean.iloc[test_idx]
        try:
            engine = spec.loader(spec.model)
            engine.fit(train[["ds", "y"]])
            forecast = engine.predict(len(test))
            values = np.asarray(forecast.values, dtype=np.float64).ravel()
            if len(values) != len(test) or not np.all(np.isfinite(values)):
                raise ValueError("misaligned or non-finite predictions")
            metrics = compute_metrics(test["y"].to_numpy(dtype=np.float64), values).to_dict()
            if not _finite_scores(metrics):
                raise ValueError("non-finite scores")
            results.append(
                FoldResult(
                    fold_index=i,
                    train_end=str(train["ds"].iloc[-1]),
                    test_start=str(test["ds"].iloc[0]),
                    test_end=str(test["ds"].iloc[-1]),
                    scores=metrics,
                )
            )
        except Exception as exc:  # isolated per fold by design
            results.append(
                FoldResult(
                    fold_index=i,
                    train_end="",
                    test_start="",
                    test_end="",
                    status="failed",
                    error=str(exc)[:200],
                )
            )
    valid = [r for r in results if r.status == "ok"]
    means: dict[str, float] = {}
    if valid:
        keys = list(valid[0].scores)
        means = {k: float(np.mean([r.scores[k] for r in valid])) for k in keys}
    return CandidateEvaluation(
        model_id=spec.model.model_id,
        algorithm=spec.model.algorithm.value,
        folds=tuple(results),
        mean_scores=means,
        valid_folds=len(valid),
        status="ok" if valid else "invalid",
    )


def select_best(
    evaluations: tuple[CandidateEvaluation, ...] | list[CandidateEvaluation],
    primary: str = "mae",
    secondary: str = "rmse",
) -> SelectionResult:
    """Deterministic selection: primary mean, then secondary, folds, order."""
    valid = [e for e in evaluations if e.status == "ok" and e.valid_folds > 0]
    if not valid:
        return SelectionResult(
            winner_model_id=None,
            reason=NO_VALID_CANDIDATE,
            criterion=primary,
            evaluations=tuple(evaluations),
        )

    def _key(e: CandidateEvaluation) -> tuple[float, float, int, str, str]:
        return (
            float(e.mean_scores.get(primary, float("inf"))),
            float(e.mean_scores.get(secondary, float("inf"))),
            -e.valid_folds,
            e.algorithm,
            e.model_id,
        )

    ordered = sorted(valid, key=_key)
    winner = ordered[0]
    detail = f"{primary}={winner.mean_scores.get(primary)} over {winner.valid_folds} folds"
    return SelectionResult(
        winner_model_id=winner.model_id,
        reason="best-score",
        criterion=primary,
        evaluations=tuple(evaluations),
        detail=detail,
    )


def check_dataset_compatibility(model: RegisteredModel, dataset: Any) -> Any:
    """W87 gate reused verbatim for evaluation inputs."""
    return check_compatibility(model, dataset)


__all__ = [
    "NO_VALID_CANDIDATE",
    "CandidateEvaluation",
    "CandidateSpec",
    "FoldResult",
    "SelectionResult",
    "evaluate_candidate",
    "select_best",
    "walk_folds",
]
