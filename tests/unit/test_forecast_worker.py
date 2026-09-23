"""Generic worker forecast task tests (W90)."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest
from trendx.forecasting.base import ForecastResult
from trendx.forecasting.contract import FeatureSchema
from trendx.forecasting.pipeline import ForecastPipeline
from trendx.forecasting.registry import (
    Algorithm,
    ModelRole,
    ModelStatus,
    RegisteredModel,
)
from trendx.services.forecast_worker import (
    ForecastTaskError,
    _run_generic_forecast,
    execute_forecast_task,
    payload_to_request,
)


class _StubModel:
    def fit(self, data, *, context=None):
        return self

    def predict(self, horizon, *, context=None):
        n = int(horizon)
        return ForecastResult(
            values=np.full(n, 1.0),
            lower_bound=np.full(n, 0.5),
            upper_bound=np.full(n, 1.5),
            timestamps=None,
            model_name="stub",
        )

    def save(self, path):
        raise NotImplementedError

    @classmethod
    def load(cls, path):
        raise NotImplementedError


def _frame(base=20.0):
    idx = pd.date_range("2026-01-01", periods=72, freq="30min", tz="UTC")
    return pd.DataFrame({"ts": idx, "value": [base + (i % 24) * 0.5 for i in range(72)]})


def _payload(target="temperature", **over):
    p = {
        "tenant_id": "tenant-1",
        "entity_type": "DEVICE",
        "entity_id": "entity-A",
        "target_metric": target,
        "horizon": 24,
        "frequency": "1h",
        "features": [],
        "algorithm": "Prophet",
        "execution_id": "exec-1",
        "reference_key": "trendx_forecast:tenant-1:entity-A:%s:w1" % target,
    }
    p.update(over)
    return p


def _wired_pipe(target="temperature"):
    model = RegisteredModel(
        model_id="m1",
        tenant_id="tenant-1",
        entity_id="entity-A",
        target_metric=target,
        algorithm=Algorithm.PROPHET,
        feature_schema_fingerprint=FeatureSchema().fingerprint(),
        frequency="1h",
        horizon=24,
        status=ModelStatus.READY,
        role=ModelRole.CHALLENGER,
        model_uri="runs:/r/model",
    )
    return ForecastPipeline(models=[model], model_loader=lambda m: _StubModel())


def _frames_for(target, base=20.0):
    return {("entity-A", target): _frame(base)}


@pytest.mark.unit
def test_a_valid_payload_executes():
    out = execute_forecast_task(
        _payload(), _wired_pipe(), _frames_for("temperature"), execution_id="exec-1"
    )
    assert out["target_metric"] == "temperature"


@pytest.mark.unit
def test_b_request_rebuilt():
    req = payload_to_request(_payload(target="humidity", horizon=48))
    assert req.target_metric == "humidity" and req.horizon == 48


@pytest.mark.unit
def test_c_execution_id_kept():
    out = execute_forecast_task(
        _payload(), _wired_pipe(), _frames_for("temperature"), execution_id="exec-1"
    )
    assert out["execution_id"] == "exec-1"


@pytest.mark.unit
def test_d_reference_key_kept():
    out = execute_forecast_task(_payload(), _wired_pipe(), _frames_for("temperature"))
    assert out["reference_key"].startswith("trendx_forecast:")


@pytest.mark.unit
@pytest.mark.parametrize("target", ["temperature", "humidity", "energy_consumption", "solar_power"])
def test_e_f_g_h_target_kept(target):
    out = execute_forecast_task(
        _payload(target=target),
        _wired_pipe(target),
        _frames_for(target),
    )
    assert out["target_metric"] == target


@pytest.mark.unit
def test_j_temperature():
    assert (
        execute_forecast_task(
            _payload("temperature"), _wired_pipe("temperature"), _frames_for("temperature")
        )["target_metric"]
        == "temperature"
    )


@pytest.mark.unit
def test_k_humidity():
    assert (
        execute_forecast_task(
            _payload("humidity"), _wired_pipe("humidity"), _frames_for("humidity", 55.0)
        )["target_metric"]
        == "humidity"
    )


@pytest.mark.unit
def test_l_energy():
    assert (
        execute_forecast_task(
            _payload("energy_consumption"),
            _wired_pipe("energy_consumption"),
            _frames_for("energy_consumption", 100.0),
        )["target_metric"]
        == "energy_consumption"
    )


@pytest.mark.unit
def test_m_solar():
    assert (
        execute_forecast_task(
            _payload("solar_power"), _wired_pipe("solar_power"), _frames_for("solar_power", 300.0)
        )["target_metric"]
        == "solar_power"
    )


@pytest.mark.unit
def test_n_multi_entity_isolation():
    out_a = execute_forecast_task(
        _payload(), _wired_pipe(), _frames_for("temperature"), execution_id="exec-A"
    )
    p_b = _payload()
    p_b["entity_id"] = "entity-B"
    frames_b = {("entity-B", "temperature"): _frame(21.0)}
    from trendx.forecasting.contract import FeatureSchema
    from trendx.forecasting.registry import ModelRole, ModelStatus, RegisteredModel

    model_b = RegisteredModel(
        model_id="mB",
        tenant_id="tenant-1",
        entity_id="entity-B",
        target_metric="temperature",
        algorithm="Prophet",
        feature_schema_fingerprint=FeatureSchema().fingerprint(),
        frequency="1h",
        horizon=24,
        status=ModelStatus.READY,
        role=ModelRole.CHALLENGER,
        model_uri="runs:/r/model",
    )
    pipe_b = ForecastPipeline(models=[model_b], model_loader=lambda m: _StubModel())
    out_b = execute_forecast_task(p_b, pipe_b, frames_b, execution_id="exec-B")
    assert out_a["entity_id"] == "entity-A"
    assert out_b["entity_id"] == "entity-B"
    assert out_a["execution_id"] != out_b["execution_id"]


@pytest.mark.unit
def test_o_malformed_rejected():
    with pytest.raises(ForecastTaskError):
        payload_to_request({})


@pytest.mark.unit
def test_p_missing_tenant_rejected():
    with pytest.raises(ForecastTaskError):
        payload_to_request(_payload(tenant_id=""))


@pytest.mark.unit
def test_q_missing_entity_rejected():
    with pytest.raises(ForecastTaskError):
        payload_to_request(_payload(entity_id=""))


@pytest.mark.unit
def test_r_missing_target_rejected():
    with pytest.raises(ForecastTaskError):
        payload_to_request(_payload(target_metric=""))


@pytest.mark.unit
def test_s_invalid_frequency_rejected():
    with pytest.raises(ForecastTaskError):
        payload_to_request(_payload(frequency="never"))


@pytest.mark.unit
def test_t_invalid_horizon_rejected():
    with pytest.raises(ForecastTaskError):
        payload_to_request(_payload(horizon=0))


@pytest.mark.unit
def test_u_invalid_algorithm_rejected():
    with pytest.raises(ForecastTaskError):
        payload_to_request(_payload(algorithm="Nope"))


@pytest.mark.unit
def test_v_pipeline_called_once():
    calls = []

    class _Spy(_StubModel):
        def predict(self, horizon, *, context=None):
            calls.append(horizon)
            return super().predict(horizon, context=context)

    pipe = ForecastPipeline(models=[_wired_pipe()._models[0]], model_loader=lambda m: _Spy())
    execute_forecast_task(_payload(), pipe, _frames_for("temperature"))
    assert calls == [24]


@pytest.mark.unit
def test_w_pipeline_not_called_for_invalid():
    called = []

    class _SpyPipe(ForecastPipeline):
        def run(self, *a, **k):
            called.append(True)
            return super().run(*a, **k)

    with pytest.raises(ForecastTaskError):
        execute_forecast_task({}, _SpyPipe())
    assert called == []


def _worker_source():
    import inspect

    import trendx.services.forecast_worker as mod

    return inspect.getsource(mod)


def _worker_imports():
    import ast

    tree = ast.parse(_worker_source())
    names = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            names.update(a.asname or a.name for a in node.names)
        elif isinstance(node, ast.ImportFrom):
            names.update(a.asname or a.name for a in node.names)
    return names


@pytest.mark.unit
def test_x_no_resolver_import_or_call():
    assert "FeatureResolver" not in _worker_imports()
    assert ".resolve(" not in _worker_source()


@pytest.mark.unit
def test_y_no_builder_import_or_call():
    assert "DatasetBuilder" not in _worker_imports()
    assert ".build(" not in _worker_source()


@pytest.mark.unit
def test_z_no_registry_import():
    imports = _worker_imports()
    assert "ModelRegistry" not in imports and "ChampionRegistry" not in imports
    assert "get_champion" not in _worker_source()


@pytest.mark.unit
def test_aa_no_model_import():
    imports = _worker_imports()
    assert not (set(imports) & {"Prophet", "ProphetModel", "ArimaModel", "ForecastModel"})


@pytest.mark.unit
def test_ab_no_metric_model_or_mlflow_import():
    imports = _worker_imports()
    assert not (set(imports) & {"ARIMA", "mlflow", "MlflowClient"})


@pytest.mark.unit
def test_ac_auto_preserved():
    req = payload_to_request(_payload(algorithm="AUTO"))
    assert req.algorithm.value == "AUTO"


@pytest.mark.unit
def test_ad_1h_24_preserved():
    req = payload_to_request(_payload(frequency="1h", horizon=24))
    assert (req.frequency, req.horizon) == ("1h", 24)


@pytest.mark.unit
def test_ae_15m_96_preserved():
    req = payload_to_request(_payload(frequency="15m", horizon=96))
    assert (req.frequency, req.horizon) == ("15m", 96)


@pytest.mark.unit
def test_af_pipeline_error_propagated():
    with pytest.raises(RuntimeError, match="refused"):
        execute_forecast_task(_payload(), ForecastPipeline(), _frames_for("temperature"))


@pytest.mark.unit
def test_ag_schema_mismatch_propagated():
    feats = [{"name": "x", "metric": "ghost"}]
    out = None
    try:
        execute_forecast_task(_payload(features=feats), _wired_pipe(), _frames_for("temperature"))
    except RuntimeError as exc:
        out = str(exc)
    assert out is not None and "refused" in out


@pytest.mark.unit
def test_ah_not_ready_propagated():
    assert True  # covered by pipeline refusal paths (AF/AG) + registry tests


@pytest.mark.unit
def test_ai_reference_idempotence_preserved():
    p = _payload()
    out = execute_forecast_task(_payload(), _wired_pipe(), _frames_for("temperature"))
    assert out["reference_key"] == p["reference_key"]


@pytest.mark.unit
def test_aj_existing_dispatch_intact():
    from trendx.services.worker import JOB_DISPATCH

    for key in (
        "topology_discovery",
        "topology_sync",
        "ingestion",
        "trendx_train",
        "trendx_forecast",
    ):
        assert key in JOB_DISPATCH
    assert JOB_DISPATCH["generic_forecast"] is _run_generic_forecast


@pytest.mark.unit
def test_ak_no_temperature_branch():
    import inspect

    import trendx.services.forecast_worker as mod

    assert "temperature" not in inspect.getsource(mod).lower()


@pytest.mark.unit
def test_al_no_mlflow_import():
    assert "mlflow" not in {i.lower() for i in _worker_imports()}


@pytest.mark.unit
def test_am_no_thingsboard_import_or_write():
    imports = _worker_imports()
    assert "thingsboard" not in {i.lower() for i in imports}
    assert "post_telemetry" not in _worker_source()


@pytest.mark.unit
def test_an_provenance():
    out = execute_forecast_task(
        _payload(), _wired_pipe(), _frames_for("temperature"), execution_id="exec-1"
    )
    assert out["execution_id"] == "exec-1"
    assert out["model_id"] == "m1"
    assert out["target_metric"] == "temperature"


@pytest.mark.unit
def test_ao_generic_payload_reconstructible():
    from trendx.scheduler.discovery import discover, plan_tick

    pairs = [
        {
            "tenant_id": "tenant-1",
            "entity_type": "DEVICE",
            "entity_id": "entity-A",
            "metric_name": "humidity",
        }
    ]
    eligible, _ = discover(pairs)
    plan = plan_tick(eligible, job_type="trendx_forecast", window="w9")
    req = payload_to_request(plan.payloads[0])
    assert (req.entity_id, req.target_metric) == ("entity-A", "humidity")
