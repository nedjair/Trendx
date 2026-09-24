"""Generic forecast task execution (W90/W97): W89 payload -> W96 pipeline.

Thin orchestration layer only: validate payload, rebuild the W84
``ForecastRequest``, delegate to the injected ``ForecastPipeline``, and
propagate execution provenance. No metric branching, no direct calls to
FeatureResolver, DatasetBuilder, ModelRegistry, ForecastModel, Prophet,
MLflow, persistence internals, or ThingsBoard.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any

from trendx.forecasting.contract import Algorithm, ForecastRequest
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

        job_type = str(payload.get("job_type") or "")
        if job_type not in ("", "trendx_forecast", "generic_forecast"):
            msg = f"Unsupported forecast job_type: {job_type!r}"
            raise ValueError(msg)
        target_metric = payload.get("target_metric") or payload.get("metric_name", "")
        features = tuple(FeatureDefinition.from_dict(f) for f in payload.get("features", []))
        return ForecastRequest(
            tenant_id=payload.get("tenant_id", ""),
            entity_type=payload.get("entity_type", ""),
            entity_id=payload.get("entity_id", ""),
            target_metric=target_metric,
            horizon=payload.get("horizon", 24),
            frequency=payload.get("frequency", "1h"),
            features=features,
            algorithm=Algorithm.coerce(payload.get("algorithm") or "AUTO"),
        )
    except (ValueError, TypeError, KeyError, AttributeError) as exc:
        msg = f"Invalid forecast payload: {exc}"
        raise ForecastTaskError(msg) from exc


def execute_forecast_task(
    payload: dict[str, Any],
    pipeline: ForecastPipeline | None = None,
    frames: Mapping[tuple[str, str], Any] | None = None,
    execution_id: str = "",
) -> dict[str, Any]:
    """Execute one generic forecast task via the injected W96/W88 pipeline.

    Returns the envelope dict on success; raises ``ForecastTaskError`` for
    invalid payloads and ``RuntimeError`` for pipeline refusals (the caller
    maps these to fail_task, mirroring existing worker conventions).
    """
    request = payload_to_request(payload)
    active = pipeline or ForecastPipeline()
    outcome = active.run(
        request,
        frames or {},
        execution_id=execution_id or str(payload.get("execution_id", "")),
        reference_key=str(payload.get("reference_key", "")),
    )
    if outcome.status != "ok":
        msg = f"Forecast pipeline refused: {outcome.reason}"
        raise RuntimeError(msg)
    if outcome.envelope is not None:
        result: dict[str, Any] = outcome.envelope.to_dict()
    elif outcome.duplicate and outcome.execution_record is not None:
        result = dict(outcome.execution_record.result or {})
        if not result:
            msg = "Duplicate execution has no persisted result"
            raise RuntimeError(msg)
    else:
        msg = f"Forecast pipeline returned no envelope: {outcome.reason}"
        raise RuntimeError(msg)
    result["reference_key"] = payload.get("reference_key", "")
    return result


ForecastTaskHandler = Callable[[dict[str, Any], str, str], dict[str, Any]]


def build_forecast_task_handler(
    pipeline: ForecastPipeline,
    frames: Mapping[tuple[str, str], Any] | None = None,
) -> ForecastTaskHandler:
    """Bind a generic W90-style task handler to an injected W96 pipeline.

    The handler owns no model, registry, or persistence knowledge.  The
    scheduler's execution identifier is preserved when present; otherwise the
    worker execution/task identifier is used by ``execute_forecast_task``.
    """

    def _handler(json_job: dict[str, Any], task_id: str, execution_id: str) -> dict[str, Any]:
        resolved_execution_id = str(json_job.get("execution_id") or execution_id or task_id)
        return execute_forecast_task(
            json_job,
            pipeline,
            frames,
            execution_id=resolved_execution_id,
        )

    return _handler


def _run_generic_forecast(
    json_job: dict[str, Any],
    task_id: str,
    execution_id: str,
    *,
    pipeline: ForecastPipeline | None = None,
    frames: Mapping[tuple[str, str], Any] | None = None,
) -> dict[str, Any]:
    """JOB_DISPATCH adapter with optional injected W96 pipeline."""
    resolved_execution_id = str(json_job.get("execution_id") or execution_id or task_id)
    return execute_forecast_task(
        json_job,
        pipeline,
        frames,
        execution_id=resolved_execution_id,
    )
