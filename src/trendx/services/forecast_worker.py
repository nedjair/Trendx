"""Generic forecast task execution (W90): W89 payload -> W88 pipeline.

Thin orchestration layer only: validate payload, rebuild the W84
``ForecastRequest``, delegate to the injected W88 ``ForecastPipeline``,
propagate execution provenance. No metric branching, no direct calls to
FeatureResolver, DatasetBuilder, ModelRegistry, ForecastModel, Prophet,
MLflow, or ThingsBoard.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from trendx.forecasting.contract import ForecastRequest
from trendx.forecasting.pipeline import ForecastPipeline


class ForecastTaskError(ValueError):
    """Controlled rejection of an invalid forecast task payload."""


def payload_to_request(payload: Mapping[str, Any]) -> ForecastRequest:
    """Rebuild a ForecastRequest from a W89 generic payload (strict)."""
    if not isinstance(payload, dict) or not payload:
        msg = "Empty or malformed forecast payload"
        raise ForecastTaskError(msg)
    try:
        from trendx.forecasting.contract import FeatureDefinition

        features = tuple(FeatureDefinition.from_dict(f) for f in payload.get("features", []))
        return ForecastRequest(
            tenant_id=payload.get("tenant_id", ""),
            entity_type=payload.get("entity_type", ""),
            entity_id=payload.get("entity_id", ""),
            target_metric=payload.get("target_metric", ""),
            horizon=payload.get("horizon", 24),
            frequency=payload.get("frequency", "1h"),
            features=features,
            algorithm=payload.get("algorithm", "AUTO"),
        )
    except (ValueError, TypeError, KeyError, AttributeError) as exc:
        msg = f"Invalid forecast payload: {exc}"
        raise ForecastTaskError(msg) from exc


def execute_forecast_task(
    payload: dict[str, Any],
    pipeline: ForecastPipeline | None = None,
    frames: dict[tuple[str, str], Any] | None = None,
    execution_id: str = "",
) -> dict[str, Any]:
    """Execute one generic forecast task via the W88 pipeline.

    Returns the envelope dict on success; raises ``ForecastTaskError`` for
    invalid payloads and ``RuntimeError`` for pipeline refusals (the caller
    maps these to fail_task, mirroring existing worker conventions).
    """
    request = payload_to_request(payload)
    active = pipeline or ForecastPipeline()
    outcome = active.run(
        request, frames or {}, execution_id=execution_id or str(payload.get("execution_id", ""))
    )
    if outcome.status != "ok" or outcome.envelope is None:
        msg = f"Forecast pipeline refused: {outcome.reason}"
        raise RuntimeError(msg)
    result: dict[str, Any] = outcome.envelope.to_dict()
    result["reference_key"] = payload.get("reference_key", "")
    return result


def _run_generic_forecast(
    json_job: dict[str, Any], task_id: str, execution_id: str
) -> dict[str, Any]:
    """JOB_DISPATCH adapter: (json_job, task_id, execution_id) contract."""
    return execute_forecast_task(json_job, execution_id=execution_id or task_id)
