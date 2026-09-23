"""Generic forecast contract tests (W84): ENTITY + TARGET METRIC + FEATURES."""

from __future__ import annotations

from pathlib import Path

import pytest
from trendx.forecasting.contract import (
    ARCHITECTURE_RULE,
    Algorithm,
    FeatureCategory,
    FeatureDefinition,
    FeatureSchema,
    ForecastEnvelope,
    ForecastRequest,
)


def _base(**overrides):
    params = {
        "tenant_id": "tenant-1",
        "entity_type": "DEVICE",
        "entity_id": "entity-A",
        "target_metric": "temperature",
        "horizon": 24,
        "frequency": "1h",
    }
    params.update(overrides)
    return ForecastRequest(**params)


@pytest.mark.unit
def test_a_temperature_contract_valid():
    req = _base()
    assert req.target_metric == "temperature"
    assert req.horizon == 24


@pytest.mark.unit
def test_b_humidity_same_contract_no_branch():
    req = _base(target_metric="humidity")
    assert req.target_metric == "humidity"
    assert req.features == ()


@pytest.mark.unit
def test_c_energy_with_features():
    req = _base(
        entity_id="entity-B",
        target_metric="energy_consumption",
        features=(
            FeatureDefinition(name="temperature", category=FeatureCategory.HISTORICAL),
            FeatureDefinition(name="humidity", category=FeatureCategory.HISTORICAL),
            FeatureDefinition(name="occupancy", category=FeatureCategory.KNOWN_FUTURE),
        ),
    )
    assert req.feature_schema().names() == ("temperature", "humidity", "occupancy")


@pytest.mark.unit
def test_d_solar_with_external_features():
    req = _base(
        entity_id="entity-C",
        target_metric="solar_power",
        features=(
            FeatureDefinition(name="irradiance", category=FeatureCategory.EXTERNAL_FORECAST),
            FeatureDefinition(name="cloud_cover", category=FeatureCategory.EXTERNAL_FORECAST),
            FeatureDefinition(name="temperature", category=FeatureCategory.HISTORICAL),
        ),
    )
    cats = {f.category for f in req.features}
    assert FeatureCategory.EXTERNAL_FORECAST in cats


@pytest.mark.unit
def test_e_feature_categories_representable():
    assert {c.value for c in FeatureCategory} == {
        "historical",
        "known_future",
        "external_forecast",
    }
    fdef = FeatureDefinition(
        name="hour_of_day",
        source="calendar",
        category=FeatureCategory.KNOWN_FUTURE,
        required=True,
    )
    assert fdef.required is True


@pytest.mark.unit
def test_f_schemas_distinguishable():
    s1 = FeatureSchema(
        schema_version="v1",
        features=(
            FeatureDefinition(name="temperature"),
            FeatureDefinition(name="humidity"),
        ),
    )
    s2 = FeatureSchema(
        schema_version="v2",
        features=(
            FeatureDefinition(name="temperature"),
            FeatureDefinition(name="humidity"),
            FeatureDefinition(name="occupancy"),
        ),
    )
    assert s1.fingerprint() != s2.fingerprint()
    assert s1.names() != s2.names()


@pytest.mark.unit
def test_g_serialization_roundtrip():
    req = _base(
        target_metric="energy_consumption",
        features=(FeatureDefinition(name="temperature", lag="24h"),),
        algorithm=Algorithm.AUTO,
    )
    clone = ForecastRequest.from_dict(req.to_dict())
    assert clone.to_dict() == req.to_dict()


@pytest.mark.unit
def test_h_incomplete_request_rejected():
    with pytest.raises(ValueError):
        _base(entity_id="")
    with pytest.raises(ValueError):
        _base(target_metric="")
    with pytest.raises(ValueError):
        _base(horizon=0)
    with pytest.raises(ValueError):
        _base(horizon=-5)
    with pytest.raises(ValueError):
        _base(frequency="fortnightly")


@pytest.mark.unit
def test_i_no_temperature_branch_in_contract():
    path = (
        Path(__file__).resolve().parent.parent.parent
        / "src"
        / "trendx"
        / "forecasting"
        / "contract.py"
    )
    source = path.read_text(encoding="utf-8").lower()
    assert "temperature" not in source
    assert ARCHITECTURE_RULE.startswith("NO NEW FORECAST METRIC")


@pytest.mark.unit
def test_w82_backward_compatible_temperature_prophet_1h_24():
    req = _base(
        tenant_id="89d43810-9b9e-11f0-8e3f-c909dc64d424",
        entity_id="7c5d0442-6d93-5f05-be3e-2fc803d4c1b0",
        target_metric="temperature",
        algorithm="Prophet",
    )
    assert req.algorithm is Algorithm.PROPHET
    env = ForecastEnvelope(
        execution_id="exec-1",
        tenant_id=req.tenant_id,
        entity_id=req.entity_id,
        target_metric=req.target_metric,
    )
    assert env.to_dict()["target_metric"] == "temperature"
