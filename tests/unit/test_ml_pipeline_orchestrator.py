"""W49 — Unit tests for the ML pipeline orchestrator (deterministic fakes).

Covers: nominal run, step order, context propagation, per-step failures,
idempotence, dedup, entity/metric isolation, no-writeback, no-alarm guards.
No DB, no MLflow, no ThingsBoard. Real preprocessing services are used with
synthetic frames; training/inference/anomaly/registry are fakes.
"""

from __future__ import annotations

import uuid
from types import SimpleNamespace
from typing import Any

import numpy as np
import pandas as pd
import pytest
from trendx.forecasting.base import ForecastResult
from trendx.preprocessing.normalizer import Normalizer
from trendx.preprocessing.quality import DataQualityService
from trendx.preprocessing.resampling import Resampler
from trendx.services import worker as worker_mod
from trendx.services.pipeline import (
    STEPS,
    MLPipelineOrchestrator,
    PipelineContext,
)


def _ctx(entity: str | None = None, metric: str = "temperature", **kw: Any) -> PipelineContext:
    return PipelineContext(
        tenant_id=str(uuid.uuid4()),
        entity_type="DEVICE",
        entity_id=entity or str(uuid.uuid4()),
        metric_name=metric,
        **kw,
    )


def _frame(n: int = 120, seed: int = 7) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    ts = pd.date_range("2024-01-01", periods=n, freq="1h", tz="UTC")
    y = 20 + 5 * np.sin(2 * np.pi * np.arange(n) / 24) + rng.normal(0, 0.3, n)
    return pd.DataFrame({"ts": ts, "value": y})


def _forecast(n: int = 24) -> ForecastResult:
    return ForecastResult(
        values=np.full(n, 20.0, dtype=np.float64),
        lower_bound=np.full(n, 19.0, dtype=np.float64),
        upper_bound=np.full(n, 21.0, dtype=np.float64),
        timestamps=None,
        model_name="FakeProphet",
    )


class FakeTraining:
    def __init__(self, model: Any | None = "auto", fail: Exception | None = None) -> None:
        self.calls: list[dict[str, Any]] = []
        self._model = model
        self._fail = fail

    def train_model(self, **kw: Any) -> Any:
        self.calls.append(kw)
        if self._fail is not None:
            raise self._fail
        if self._model is None:
            return None
        if self._model == "auto":
            return SimpleNamespace(id=str(uuid.uuid4()))
        return self._model


class FakeRegistry:
    """Placeholder registry fake (promotion owned by register(); unused here)."""


class FakeInference:
    def __init__(
        self,
        forecast: Any | None = "auto",
        saved: int = 24,
        fail_save: Exception | None = None,
    ) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self.dry_run = True
        self._forecast = forecast
        self._saved = saved
        self._fail_save = fail_save

    def generate_forecast(self, entity_id: str, metric_key: str, horizon: Any = None) -> Any:
        self.calls.append(("generate", {"entity_id": entity_id, "metric_key": metric_key}))
        if self._forecast == "auto":
            return _forecast()
        return self._forecast

    def save_forecast_results(self, entity_id: str, metric_key: str, forecast: Any) -> int:
        self.calls.append(("save", {"entity_id": entity_id, "metric_key": metric_key}))
        if self._fail_save is not None:
            raise self._fail_save
        return self._saved


class FakeAnomaly:
    def __init__(self, episodes: Any | None = "auto", fail: Exception | None = None) -> None:
        self.calls: list[dict[str, Any]] = []
        self._episodes = episodes
        self._fail = fail

    def scan(self, entity_id: str, metric_key: str) -> Any:
        self.calls.append({"entity_id": entity_id, "metric_key": metric_key})
        if self._fail is not None:
            raise self._fail
        if self._episodes == "auto":
            return [SimpleNamespace(start="s", end="e")]
        return self._episodes


def _orchestrator(df: pd.DataFrame, **kw: Any) -> MLPipelineOrchestrator:
    return MLPipelineOrchestrator(
        quality=DataQualityService(),
        resampler=Resampler(),
        normalizer=Normalizer(method="auto"),
        fetcher=lambda _ctx: df,
        **kw,
    )


# 1. nominal + 2. order -----------------------------------------------------------
@pytest.mark.unit
def test_01_nominal_run_and_step_order() -> None:
    training, registry, inference, anomaly = (
        FakeTraining(),
        FakeRegistry(),
        FakeInference(),
        FakeAnomaly(),
    )
    orch = _orchestrator(
        _frame(), training=training, registry=registry, inference=inference, anomaly=anomaly
    )
    result = orch.run_single(_ctx())
    assert result.status == "ok"
    assert list(result.steps.keys()) == list(STEPS)
    assert all(step.status == "ok" for step in result.steps.values())
    assert result.model_id is not None
    assert result.forecast_points == 24 and result.forecast_saved == 24
    assert result.anomaly_episodes == 1
    assert result.writeback == "disabled" and result.alarms == "disabled"
    assert result.error is None


# 3. context propagation ----------------------------------------------------------
@pytest.mark.unit
def test_02_context_propagation() -> None:
    training, registry, inference, anomaly = (
        FakeTraining(),
        FakeRegistry(),
        FakeInference(),
        FakeAnomaly(),
    )
    orch = _orchestrator(
        _frame(), training=training, registry=registry, inference=inference, anomaly=anomaly
    )
    context = _ctx(
        entity="11111111-2222-4333-8444-555555555555",
        customer_id="8a40b580-9b9e-11f0-8e3f-c909dc64d424",
    )
    result = orch.run_single(context)
    assert result.status == "ok"
    assert training.calls[0]["entity_id"] == context.entity_id
    assert training.calls[0]["metric_key"] == "temperature"
    assert training.calls[0]["customer_id"] == "8a40b580-9b9e-11f0-8e3f-c909dc64d424"
    assert inference.calls[0][1]["entity_id"] == context.entity_id
    assert anomaly.calls[0]["entity_id"] == context.entity_id
    assert result.execution_id == context.execution_id
    assert result.steps["quality"].detail["execution_id"] == context.execution_id


# 4-8. per-step failures ----------------------------------------------------------
@pytest.mark.unit
def test_03_quality_failure_empty_frame() -> None:
    orch = _orchestrator(_frame(0), training=FakeTraining())
    result = orch.run_single(_ctx())
    assert result.status == "failed"
    assert result.steps["quality"].status == "failed"
    assert result.steps["preprocessing"].status == "skipped"


@pytest.mark.unit
def test_04_preprocessing_failure() -> None:
    class EmptyResampler(Resampler):
        def resample(self, df: pd.DataFrame, **kw: Any) -> pd.DataFrame:
            return df.iloc[0:0]

    orch = MLPipelineOrchestrator(
        quality=DataQualityService(),
        resampler=EmptyResampler(),
        normalizer=Normalizer(method="auto"),
        training=FakeTraining(),
        fetcher=lambda _c: _frame(),
    )
    result = orch.run_single(_ctx())
    assert result.status == "failed"
    assert result.steps["quality"].status == "ok"
    assert result.steps["preprocessing"].status == "failed"
    assert "insufficient data" in (result.steps["preprocessing"].error or "")
    assert result.steps["train"].status == "skipped"


@pytest.mark.unit
def test_05_training_failure_no_model() -> None:
    orch = _orchestrator(_frame(), training=FakeTraining(model=None), registry=FakeRegistry())
    result = orch.run_single(_ctx())
    assert result.status == "failed"
    assert result.steps["train"].status == "failed"
    assert result.steps["forecast"].status == "skipped"


@pytest.mark.unit
def test_06_training_exception() -> None:
    orch = _orchestrator(
        _frame(), training=FakeTraining(fail=RuntimeError("boom")), registry=FakeRegistry()
    )
    result = orch.run_single(_ctx())
    assert result.status == "failed" and "boom" in (result.error or "")


@pytest.mark.unit
def test_07_forecast_failure_no_forecast() -> None:
    orch = _orchestrator(
        _frame(),
        training=FakeTraining(),
        registry=FakeRegistry(),
        inference=FakeInference(forecast=None),
    )
    result = orch.run_single(_ctx())
    assert result.status == "failed"
    assert result.steps["forecast"].status == "failed"
    assert result.steps["anomaly"].status == "skipped"


@pytest.mark.unit
def test_08_anomaly_failure() -> None:
    orch = _orchestrator(
        _frame(),
        training=FakeTraining(),
        registry=FakeRegistry(),
        inference=FakeInference(),
        anomaly=FakeAnomaly(fail=RuntimeError("detector down")),
    )
    result = orch.run_single(_ctx())
    assert result.status == "failed"
    assert result.steps["anomaly"].status == "failed"
    assert "detector down" in (result.error or "")


# 9-10. idempotence / dedup -------------------------------------------------------
@pytest.mark.unit
def test_09_idempotence_same_context_twice() -> None:
    training, inference, anomaly = FakeTraining(), FakeInference(), FakeAnomaly()
    orch = _orchestrator(
        _frame(), training=training, registry=FakeRegistry(), inference=inference, anomaly=anomaly
    )
    context = _ctx()
    first = orch.run_single(context)
    second = orch.run_single(context)
    assert first.status == "ok" and second.status == "ok"
    assert second.deduped is True
    assert len(training.calls) == 1
    assert len(anomaly.calls) == 1


@pytest.mark.unit
def test_10_failed_runs_are_not_cached() -> None:
    training = FakeTraining(model=None)
    orch = _orchestrator(_frame(), training=training, registry=FakeRegistry())
    context = _ctx()
    assert orch.run_single(context).status == "failed"
    assert orch.run_single(context).status == "failed"
    assert len(training.calls) == 2


# 11. isolation -------------------------------------------------------------------
@pytest.mark.unit
def test_11_isolation_multi_entity() -> None:
    training = FakeTraining()
    good = _ctx(entity="aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee")
    bad = _ctx(entity="ffffffff-0000-4111-8222-333333333333")
    orch_mixed = MLPipelineOrchestrator(
        quality=DataQualityService(),
        resampler=Resampler(),
        normalizer=Normalizer(method="auto"),
        training=training,
        registry=FakeRegistry(),
        inference=FakeInference(),
        anomaly=FakeAnomaly(episodes=[]),
        fetcher=lambda c: _frame(0) if c.entity_id == bad.entity_id else _frame(),
    )
    out = orch_mixed.run_many([good, bad])
    assert out[0].status == "ok"
    assert out[1].status == "failed"
    assert out[0].anomaly_episodes == 0
    assert bad.entity_id not in str(out[0].to_dict())


# 12-13. no writeback / no alarm --------------------------------------------------
@pytest.mark.unit
def test_12_no_writeback_no_alarm() -> None:
    from pathlib import Path

    inference = FakeInference()
    orch = _orchestrator(
        _frame(),
        training=FakeTraining(),
        registry=FakeRegistry(),
        inference=inference,
        anomaly=FakeAnomaly(episodes=[]),
    )
    result = orch.run_single(_ctx())
    assert result.status == "ok"
    assert inference.dry_run is True
    assert result.writeback == "disabled" and result.alarms == "disabled"
    source = Path("src/trendx/services/pipeline.py").read_text()
    for forbidden in (
        "writeback_forecast(",
        "post_telemetry(",
        "AlertingService(",
        "from trendx.thingsboard",
        "import thingsboard",
    ):
        assert forbidden not in source


@pytest.mark.unit
def test_13_dispatch_registers_pipeline_tasks() -> None:
    from trendx.scheduler.handlers import JOB_HANDLERS

    for registry in (worker_mod.JOB_DISPATCH, JOB_HANDLERS):
        assert "ml_pipeline" in registry
        assert "anomaly_scan" in registry
        assert "trendx_train" in registry
        assert "trendx_forecast" in registry
        assert "ingestion" in registry
    assert worker_mod.JOB_DISPATCH["ml_pipeline"] is JOB_HANDLERS["ml_pipeline"]
    assert worker_mod.JOB_DISPATCH["anomaly_scan"] is JOB_HANDLERS["anomaly_scan"]


@pytest.mark.unit
def test_14_customer_fail_closed() -> None:
    orch = _orchestrator(
        _frame(),
        training=FakeTraining(fail=ValueError("customer_id explicite requis")),
        registry=FakeRegistry(),
    )
    result = orch.run_single(_ctx())
    assert result.status == "failed"
    assert result.steps["train"].status == "failed"
    assert "customer_id" in (result.error or "")
    assert result.steps["forecast"].status == "skipped"
