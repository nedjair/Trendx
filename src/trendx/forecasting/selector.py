from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

import numpy as np
import pandas as pd
from loguru import logger
from trendx.forecasting.base import (
    ForecastMetrics,
    ForecastModel,
    Strategy,
    compute_metrics,
)


@dataclass
class BacktestWindowResult:
    window_order: int
    train_start: datetime
    train_end: datetime
    forecast_start: datetime
    forecast_end: datetime
    metrics: ForecastMetrics = field(default_factory=ForecastMetrics)


@dataclass
class CompetitionResult:
    algorithm: str
    params: dict[str, Any]
    backtest_windows: list[BacktestWindowResult] = field(default_factory=list)
    aggregated_metrics: ForecastMetrics = field(default_factory=ForecastMetrics)
    stability_score: float = 1.0
    is_champion: bool = False
    previous_champion_id: str | None = None


class ModelSelector:
    """Selects and promotes champion forecast models via walk-forward backtesting.

    Parameters
    ----------
    strategy : Strategy
        Selection strategy (PER_DEVICE, PER_PROFILE, GLOBAL, AUTO).
    n_windows : int
        Number of walk-forward windows (default 5).
    test_size : float
        Fraction of data held out for each test window (default 0.2).
    min_stable_windows : int
        Minimum windows a model must be stable across (default 3).
    marginal_gain_threshold : float
        Minimum relative improvement to promote (default 0.02 = 2%).
    """

    def __init__(
        self,
        strategy: Strategy = Strategy.PER_DEVICE,
        n_windows: int = 5,
        test_size: float = 0.2,
        min_stable_windows: int = 3,
        marginal_gain_threshold: float = 0.02,
    ) -> None:
        self.strategy = strategy
        self.n_windows = n_windows
        self.test_size = test_size
        self.min_stable_windows = min_stable_windows
        self.marginal_gain_threshold = marginal_gain_threshold

    def backtest(
        self,
        model_class: type[ForecastModel],
        params: dict[str, Any] | None,
        data: pd.DataFrame,
        n_windows: int | None = None,
        test_size: float | None = None,
    ) -> list[BacktestWindowResult]:
        """Walk-forward backtest on a single model configuration.

        Parameters
        ----------
        model_class : type
            ForecastModel subclass.
        params : dict or None
            Constructor arguments for the model.
        data : pd.DataFrame
            Time series with ``ds`` and ``y`` columns.
        n_windows : int or None
            Override the default number of windows.
        test_size : float or None
            Override the default test fraction.

        Returns
        -------
        list of BacktestWindowResult, one per window.
        """
        n_windows = n_windows or self.n_windows
        test_size = test_size or self.test_size

        df = data.sort_values("ds").reset_index(drop=True)
        n = len(df)
        if n < n_windows + 2:
            logger.warning("Too few data points ({}) for {} windows", n, n_windows)
            return []

        results: list[BacktestWindowResult] = []
        window_size = int(n * test_size)
        if window_size < 1:
            window_size = 1

        for i in range(n_windows):
            train_end_idx = n - (n_windows - i) * window_size
            if train_end_idx < 2:
                logger.warning("Window {}: insufficient training data, skipping", i)
                continue

            train_df = df.iloc[:train_end_idx].copy()
            test_df = df.iloc[train_end_idx : train_end_idx + window_size].copy()

            if len(test_df) < 1:
                logger.warning("Window {}: empty test set, skipping", i)
                continue

            try:
                model = model_class(**(params or {}))
                model.fit(train_df)

                horizon = len(test_df)
                result = model.predict(horizon=horizon)

                min_len = min(len(test_df), len(result.values))
                if min_len < 1:
                    continue

                y_true = test_df["y"].values[:min_len].astype(np.float64)
                y_pred = result.values[:min_len]
                y_lower = (
                    result.lower_bound[:min_len] if len(result.lower_bound) >= min_len else None
                )
                y_upper = (
                    result.upper_bound[:min_len] if len(result.upper_bound) >= min_len else None
                )

                metrics = compute_metrics(y_true, y_pred, y_lower, y_upper)

                window_result = BacktestWindowResult(
                    window_order=i,
                    train_start=train_df["ds"].min().to_pydatetime().replace(tzinfo=UTC)
                    if hasattr(train_df["ds"].min(), "to_pydatetime")
                    else train_df["ds"].min(),
                    train_end=train_df["ds"].max().to_pydatetime().replace(tzinfo=UTC)
                    if hasattr(train_df["ds"].max(), "to_pydatetime")
                    else train_df["ds"].max(),
                    forecast_start=test_df["ds"].iloc[0].to_pydatetime().replace(tzinfo=UTC)
                    if hasattr(test_df["ds"].iloc[0], "to_pydatetime")
                    else test_df["ds"].iloc[0],
                    forecast_end=test_df["ds"].iloc[-1].to_pydatetime().replace(tzinfo=UTC)
                    if hasattr(test_df["ds"].iloc[-1], "to_pydatetime")
                    else test_df["ds"].iloc[-1],
                    metrics=metrics,
                )
                results.append(window_result)

                logger.debug(
                    "Window {}: MAE={:.4f}, RMSE={:.4f}, sMAPE={:.2f}%",
                    i,
                    metrics.mae,
                    metrics.rmse,
                    metrics.smape,
                )

            except Exception as exc:
                logger.error("Window {} failed: {}", i, exc)
                continue

        return results

    def _aggregate_metrics(self, windows: list[BacktestWindowResult]) -> ForecastMetrics:
        if not windows:
            return ForecastMetrics()

        mae_vals = [w.metrics.mae for w in windows]
        rmse_vals = [w.metrics.rmse for w in windows]
        smape_vals = [w.metrics.smape for w in windows]
        mape_vals = [w.metrics.mape for w in windows]
        coverage_vals = [w.metrics.coverage for w in windows]
        width_vals = [w.metrics.interval_width for w in windows]
        bias_vals = [w.metrics.bias for w in windows]

        return ForecastMetrics(
            mae=float(np.mean(mae_vals)),
            rmse=float(np.mean(rmse_vals)),
            smape=float(np.mean(smape_vals)),
            mape=float(np.mean(mape_vals)),
            coverage=float(np.mean(coverage_vals)),
            interval_width=float(np.mean(width_vals)),
            bias=float(np.mean(bias_vals)),
        )

    def _stability_score(self, windows: list[BacktestWindowResult]) -> float:
        if len(windows) < 2:
            return 1.0

        smape_vals = [w.metrics.smape for w in windows]
        mean_smape = float(np.mean(smape_vals))
        std_smape = float(np.std(smape_vals))

        if mean_smape < 1e-12:
            return 1.0

        cv = std_smape / mean_smape
        return max(0.0, 1.0 - cv)

    def run_competition(
        self,
        entity_id: str,
        metric_key: str,
        data: pd.DataFrame,
        candidates: list[tuple[str, dict[str, Any] | None]],
        n_windows: int | None = None,
    ) -> list[CompetitionResult]:
        """Run a competition among several model configurations.

        Parameters
        ----------
        entity_id : str
            Entity identifier (for logging).
        metric_key : str
            Metric key (for logging).
        data : pd.DataFrame
            Time series with ``ds`` and ``y`` columns.
        candidates : list of (algorithm, params)
            Each candidate is a (model_name, params_dict) pair.
        n_windows : int or None
            Override number of backtest windows.

        Returns
        -------
        list of CompetitionResult sorted by aggregated sMAPE (ascending).
        """
        from trendx.forecasting import MODEL_REGISTRY

        logger.info(
            "Running model competition for {} / {} with {} candidates",
            entity_id,
            metric_key,
            len(candidates),
        )

        results: list[CompetitionResult] = []

        for algorithm, params in candidates:
            model_class = MODEL_REGISTRY.get(algorithm)
            if model_class is None:
                logger.warning("Unknown algorithm '{}', skipping", algorithm)
                continue

            logger.info("Backtesting {} with params={}", algorithm, params)
            windows = self.backtest(model_class, params, data, n_windows=n_windows)

            if not windows:
                logger.warning("No valid windows for {}", algorithm)
                continue

            aggregated = self._aggregate_metrics(windows)
            stability = self._stability_score(windows)

            result = CompetitionResult(
                algorithm=algorithm,
                params=params or {},
                backtest_windows=windows,
                aggregated_metrics=aggregated,
                stability_score=stability,
            )
            results.append(result)

            logger.info(
                "{}: sMAPE={:.2f}%, MAE={:.4f}, stability={:.3f}",
                algorithm,
                aggregated.smape,
                aggregated.mae,
                stability,
            )

        results.sort(key=lambda r: r.aggregated_metrics.smape)
        return results

    def select_champion(
        self,
        results: list[CompetitionResult],
        metric: str = "sMAPE",
    ) -> CompetitionResult | None:
        """Select the best model from competition results.

        Applies stability and marginal-gain checks.

        Parameters
        ----------
        results : list of CompetitionResult
            Sorted competition results (ascending by metric).
        metric : str
            Metric to use for ranking ('sMAPE', 'MAE', 'RMSE').

        Returns
        -------
        CompetitionResult or None if no candidate qualifies.
        """
        if not results:
            logger.warning("No candidates to select from")
            return None

        metric_key = metric.lower()

        def get_metric_value(r: CompetitionResult) -> float:
            return getattr(r.aggregated_metrics, metric_key, r.aggregated_metrics.smape)

        sorted_results = sorted(results, key=get_metric_value)

        best = sorted_results[0]

        if len(best.backtest_windows) < self.min_stable_windows:
            logger.warning(
                "Best candidate {} has only {} windows, need {}",
                best.algorithm,
                len(best.backtest_windows),
                self.min_stable_windows,
            )
            return None

        second = sorted_results[1] if len(sorted_results) > 1 else None
        if second is not None:
            best_val = get_metric_value(best)
            second_val = get_metric_value(second)
            if second_val > 0:
                margin = (second_val - best_val) / second_val
            else:
                margin = 0.0

            if margin < self.marginal_gain_threshold:
                logger.info(
                    "Marginal gain {:.2f}% < threshold {:.2f}%, not promoting",
                    margin * 100,
                    self.marginal_gain_threshold * 100,
                )
                return None

            logger.info(
                "Model {} selected: {}={:.4f}, gain={:.2f}% over {}",
                best.algorithm,
                metric,
                best_val,
                margin * 100,
                second.algorithm,
            )

        best.is_champion = True
        return best

    def promote_champion(
        self,
        entity_id: str,
        metric_key: str,
        candidate_model: CompetitionResult,
        previous_champion_id: str | None = None,
    ) -> None:
        """Log champion promotion.

        In production this would write to the database via
        PredictionModelRepository.set_champion().

        Parameters
        ----------
        entity_id : str
            Entity identifier.
        metric_key : str
            Metric key.
        candidate_model : CompetitionResult
            The winning candidate.
        previous_champion_id : str or None
            ID of previous champion for rollback support.
        """
        logger.info(
            "Champion promoted: entity={}, metric={}, algorithm={}, "
            "sMAPE={:.2f}%, previous_champion={}",
            entity_id,
            metric_key,
            candidate_model.algorithm,
            candidate_model.aggregated_metrics.smape,
            previous_champion_id or "none",
        )

    def run_auto_strategy(
        self,
        entity_id: str,
        metric_key: str,
        data: pd.DataFrame,
        candidates: list[tuple[str, dict[str, Any] | None]],
    ) -> CompetitionResult | None:
        """Run AUTO strategy: compare PER_DEVICE and GLOBAL if applicable.

        For MVP purposes, this delegates to ``run_competition``.

        Parameters
        ----------
        entity_id : str
            Entity identifier.
        metric_key : str
            Metric key.
        data : pd.DataFrame
            Time series with ``ds`` and ``y`` columns.
        candidates : list of (algorithm, params)
            Candidate model configurations.

        Returns
        -------
        CompetitionResult or None.
        """
        results = self.run_competition(entity_id, metric_key, data, candidates)
        champion = self.select_champion(results)
        if champion is not None:
            self.promote_champion(entity_id, metric_key, champion)
        return champion
