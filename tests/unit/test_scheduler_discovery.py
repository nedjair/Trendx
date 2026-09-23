"""Generic scheduler discovery + dispatch tests (W89)."""

from __future__ import annotations

from pathlib import Path

import pytest
from trendx.forecasting.contract import Algorithm, FeatureDefinition
from trendx.scheduler.discovery import (
    ForecastPolicy,
    MetricCandidate,
    MetricPolicy,
    build_request,
    discover,
    plan_tick,
)

METRICS_4 = ("temperature", "humidity", "energy_consumption", "solar_power")


def _pairs(metrics=METRICS_4, entity="entity-A", tenant="tenant-1"):
    return [
        {
            "tenant_id": tenant,
            "entity_type": "DEVICE",
            "entity_id": entity,
            "metric_name": m,
        }
        for m in metrics
    ]


@pytest.mark.unit
def test_a_baseline_existing_discovery_shape():
    pairs = _pairs(("temperature",))
    assert pairs[0]["metric_name"] == "temperature"


@pytest.mark.unit
def test_b_discover_one_metric():
    eligible, rejected = discover(_pairs(("temperature",)))
    assert len(eligible) == 1 and not rejected


@pytest.mark.unit
def test_c_discover_four_metrics():
    eligible, rejected = discover(_pairs())
    assert [c.metric_name for c in eligible] == list(METRICS_4)
    assert rejected == []


@pytest.mark.unit
def test_d_build_request():
    req = build_request(
        MetricCandidate("tenant-1", "DEVICE", "entity-A", "humidity", MetricPolicy())
    )
    assert req.target_metric == "humidity"
    assert req.entity_id == "entity-A"


@pytest.mark.unit
def test_e_entities_isolated():
    eligible, _ = discover([*_pairs(entity="entity-A"), *_pairs(entity="entity-B")])
    ids = {(c.entity_id, c.metric_name) for c in eligible}
    assert ("entity-A", "temperature") in ids
    assert ("entity-B", "temperature") in ids
    assert len(ids) == 8


@pytest.mark.unit
def test_f_frequency_propagated():
    policy = ForecastPolicy(overrides={"energy_consumption": MetricPolicy(frequency="15m")})
    eligible, _ = discover(_pairs(), policy)
    by_metric = {c.metric_name: c for c in eligible}
    assert by_metric["energy_consumption"].policy.frequency == "15m"
    assert by_metric["temperature"].policy.frequency == "1h"


@pytest.mark.unit
def test_g_horizon_propagated():
    policy = ForecastPolicy(overrides={"energy_consumption": MetricPolicy(horizon=96)})
    eligible, _ = discover(_pairs(), policy)
    by_metric = {c.metric_name: c for c in eligible}
    assert by_metric["energy_consumption"].policy.horizon == 96
    assert build_request(by_metric["energy_consumption"]).horizon == 96


@pytest.mark.unit
def test_h_algorithm_propagated():
    policy = ForecastPolicy(overrides={"temperature": MetricPolicy(algorithm="ARIMA")})
    eligible, _ = discover(_pairs(), policy)
    assert (
        build_request(next(c for c in eligible if c.metric_name == "temperature")).algorithm.value
        == "ARIMA"
    )


@pytest.mark.unit
def test_i_auto_preserved():
    req = build_request(MetricCandidate("t", "DEVICE", "e", "m", MetricPolicy()))
    assert req.algorithm is Algorithm.AUTO


@pytest.mark.unit
def test_j_feature_policy_no_resolution():
    policy = ForecastPolicy(
        overrides={
            "energy_consumption": MetricPolicy(
                features=(FeatureDefinition(name="temperature", metric="temperature"),)
            )
        }
    )
    eligible, _ = discover(_pairs(), policy)
    req = build_request(next(c for c in eligible if c.metric_name == "energy_consumption"))
    assert req.features[0].name == "temperature"
    assert isinstance(req.features[0], FeatureDefinition)


@pytest.mark.unit
def test_k_disabled_no_dispatch():
    policy = ForecastPolicy(overrides={"humidity": MetricPolicy(forecast_enabled=False)})
    eligible, rejected = discover(_pairs(), policy)
    assert {c.metric_name for c in eligible} == {
        "temperature",
        "energy_consumption",
        "solar_power",
    }
    assert any(r.metric_name == "humidity" for r in rejected)


@pytest.mark.unit
def test_l_unknown_metric_rejected_when_invalid():
    eligible, rejected = discover(
        [{"tenant_id": "t", "entity_type": "DEVICE", "entity_id": "", "metric_name": "x"}]
    )
    assert eligible == [] and len(rejected) == 1


@pytest.mark.unit
def test_m_invalid_frequency_rejected():
    policy = ForecastPolicy(overrides={"m": MetricPolicy(frequency="fortnightly")})
    eligible, rejected = discover(_pairs(("m",)), policy)
    assert eligible == [] and rejected[0].reason.startswith("invalid config")


@pytest.mark.unit
def test_n_invalid_horizon_rejected():
    policy = ForecastPolicy(overrides={"m": MetricPolicy(horizon=0)})
    eligible, rejected = discover(_pairs(("m",)), policy)
    assert eligible == [] and len(rejected) == 1


@pytest.mark.unit
def test_o_invalid_entity_rejected():
    eligible, rejected = discover(_pairs(entity=""))
    assert eligible == [] and len(rejected) == 4


@pytest.mark.unit
def test_p_one_bad_metric_does_not_block_others():
    pairs = [
        *_pairs(),
        {"tenant_id": "t", "entity_type": "DEVICE", "entity_id": "", "metric_name": "bad"},
    ]
    eligible, rejected = discover(pairs)
    assert len(eligible) == 4 and len(rejected) == 1


@pytest.mark.unit
def test_q_no_temperature_branch_in_discovery():
    path = (
        Path(__file__).resolve().parent.parent.parent
        / "src"
        / "trendx"
        / "scheduler"
        / "discovery.py"
    )
    assert "temperature" not in path.read_text(encoding="utf-8").lower()


@pytest.mark.unit
def test_r_same_tick_no_duplicate():
    eligible, _ = discover(_pairs())
    seen: set[str] = set()
    first = plan_tick(eligible, job_type="trendx_forecast", window="w1", dispatched=seen)
    second = plan_tick(eligible, job_type="trendx_forecast", window="w1", dispatched=seen)
    assert len(first.payloads) == 4
    assert len(second.payloads) == 0
    assert len(second.skipped_duplicates) == 4


@pytest.mark.unit
def test_s_distinct_requests_per_metric():
    eligible, _ = discover(_pairs())
    plan = plan_tick(eligible, job_type="trendx_forecast", window="w1")
    keys = {(p["entity_id"], p["metric_name"]) for p in plan.payloads}
    assert len(keys) == 4


@pytest.mark.unit
def test_t_execution_identifiers():
    eligible, _ = discover(_pairs(("temperature",)))
    plan = plan_tick(eligible, job_type="trendx_forecast", window="w2")
    payload = plan.payloads[0]
    assert payload["reference_key"].startswith("trendx_forecast:")
    assert payload["window"] == "w2"


@pytest.mark.unit
def test_u_payload_reconstructible():
    eligible, _ = discover(_pairs(("humidity",)))
    plan = plan_tick(eligible, job_type="trendx_forecast", window="w3")
    p = plan.payloads[0]
    for k in (
        "tenant_id",
        "entity_type",
        "entity_id",
        "metric_name",
        "frequency",
        "horizon",
        "algorithm",
        "features",
        "target_metric",
    ):
        assert k in p


_DISCOVERY_SOURCE = (
    Path(__file__).resolve().parent.parent.parent / "src" / "trendx" / "scheduler" / "discovery.py"
).read_text(encoding="utf-8")


@pytest.mark.unit
def test_v_no_resolver_import_in_scheduler_discovery():
    import trendx.scheduler.discovery as mod

    assert not hasattr(mod, "FeatureResolver")
    assert "FeatureResolver" not in _DISCOVERY_SOURCE


@pytest.mark.unit
def test_w_no_dataset_builder_import():
    import trendx.scheduler.discovery as mod

    assert not hasattr(mod, "DatasetBuilder")
    assert "DatasetBuilder" not in _DISCOVERY_SOURCE


@pytest.mark.unit
def test_x_no_forecast_model_import():
    import trendx.scheduler.discovery as mod

    assert not hasattr(mod, "ForecastModel")
    assert "ForecastModel" not in _DISCOVERY_SOURCE


@pytest.mark.unit
def test_y_no_model_registry_selection():
    import trendx.scheduler.discovery as mod

    assert not hasattr(mod, "ModelRegistry")
    assert not hasattr(mod, "ChampionRegistry")
    assert "ModelRegistry" not in _DISCOVERY_SOURCE
    assert "ChampionRegistry" not in _DISCOVERY_SOURCE
    assert "MLflow" not in _DISCOVERY_SOURCE and "mlflow" not in _DISCOVERY_SOURCE


@pytest.mark.unit
def test_z_flags_untouched_by_discovery():
    import os

    assert os.environ.get("TRENDX_SCHEDULER_FORECAST_ENABLED", "false") == "false"
