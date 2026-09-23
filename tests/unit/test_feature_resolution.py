"""Generic feature resolution tests (W85)."""

from __future__ import annotations

from pathlib import Path

import pytest
from trendx.forecasting.contract import (
    FeatureCategory,
    FeatureDefinition,
    FeatureSchema,
    ForecastRequest,
)
from trendx.forecasting.resolution import (
    FeatureResolver,
    ResolutionStatus,
    ResolvedFeatureSet,
)

E_A = "entity-A"
E_B = "entity-B"
AVAIL = {(E_A, "temperature"), (E_A, "humidity"), (E_B, "temperature")}


def _req(target="temperature", entity=E_A, features=()):
    return ForecastRequest(
        tenant_id="tenant-1",
        entity_type="DEVICE",
        entity_id=entity,
        target_metric=target,
        horizon=24,
        frequency="1h",
        features=features,
    )


def _schema(*fdefs):
    return FeatureSchema(schema_version="v1", features=fdefs)


@pytest.mark.unit
def test_a_historical_resolved():
    r = FeatureResolver(available=AVAIL).resolve(
        _req(), _schema(FeatureDefinition(name="humidity", metric="humidity"))
    )
    assert r.ok
    assert r.resolved_names() == ("humidity",)


@pytest.mark.unit
def test_b_humidity_target_no_special_path():
    r = FeatureResolver(available=AVAIL).resolve(
        _req(target="humidity"),
        _schema(FeatureDefinition(name="temperature", metric="temperature")),
    )
    assert r.ok


@pytest.mark.unit
def test_c_energy_generic():
    req = _req(target="energy_consumption")
    schema = _schema(
        FeatureDefinition(name="temperature", metric="temperature"),
        FeatureDefinition(name="humidity", metric="humidity"),
        FeatureDefinition(
            name="occupancy", metric="occupancy", category=FeatureCategory.KNOWN_FUTURE
        ),
    )
    avail = AVAIL | {(E_A, "energy_consumption")}
    r = FeatureResolver(available=avail).resolve(req, schema)
    assert r.ok
    assert r.resolved_names() == ("temperature", "humidity", "occupancy")


@pytest.mark.unit
def test_d_solar_external_representable():
    schema = _schema(
        FeatureDefinition(
            name="irradiance",
            metric="irradiance",
            category=FeatureCategory.EXTERNAL_FORECAST,
            source="weather-provider",
        ),
        FeatureDefinition(
            name="cloud_cover",
            metric="cloud_cover",
            category=FeatureCategory.EXTERNAL_FORECAST,
        ),
    )
    req = _req(target="solar_power")
    r = FeatureResolver(available=AVAIL).resolve(req, schema)
    assert r.ok
    by_name = {x.name: x for x in r.resolutions}
    assert by_name["irradiance"].status is ResolutionStatus.UNAVAILABLE
    assert by_name["cloud_cover"].status is ResolutionStatus.UNAVAILABLE
    assert r.resolved_names() == ()


@pytest.mark.unit
def test_e_entity_scope_no_cross_entity():
    schema = _schema(FeatureDefinition(name="x", metric="temperature", entity_scope=E_B))
    r = FeatureResolver(available=AVAIL).resolve(_req(entity=E_A), schema)
    assert not r.ok
    assert r.resolutions[0].status is ResolutionStatus.INVALID


@pytest.mark.unit
def test_f_target_feature_separation():
    schema = _schema(
        FeatureDefinition(name="self-now", metric="energy_consumption"),
        FeatureDefinition(name="self-lag", metric="energy_consumption", lag="1h"),
    )
    avail = {(E_A, "energy_consumption")}
    r = FeatureResolver(available=avail).resolve(_req(target="energy_consumption"), schema)
    by_name = {x.name: x for x in r.resolutions}
    assert by_name["self-now"].status is ResolutionStatus.INVALID
    assert by_name["self-lag"].status is ResolutionStatus.RESOLVED


@pytest.mark.unit
def test_g_lag_preserved():
    schema = _schema(FeatureDefinition(name="t-1", metric="temperature", lag="1h"))
    r = FeatureResolver(available=AVAIL).resolve(_req(), schema)
    assert r.resolutions[0].lag == "1h"


@pytest.mark.unit
def test_h_transformation_preserved():
    schema = _schema(
        FeatureDefinition(name="e-roll", metric="energy_consumption", transformation="rolling_mean")
    )
    r = FeatureResolver(available={(E_A, "energy_consumption")}).resolve(
        _req(target="humidity"), schema
    )
    assert r.resolutions[0].transformation == "rolling_mean"


@pytest.mark.unit
def test_i_required_missing_controlled_error():
    schema = _schema(FeatureDefinition(name="must", metric="absent_metric", required=True))
    r = FeatureResolver(available=AVAIL).resolve(_req(), schema)
    assert not r.ok
    assert any("must" in e for e in r.errors)
    assert isinstance(r, ResolvedFeatureSet)


@pytest.mark.unit
def test_j_optional_missing_explicit_no_crash():
    schema = _schema(FeatureDefinition(name="maybe", metric="absent_metric"))
    r = FeatureResolver(available=AVAIL).resolve(_req(), schema)
    assert r.ok
    assert r.resolutions[0].status is ResolutionStatus.UNAVAILABLE


@pytest.mark.unit
def test_k_external_no_provider_unavailable():
    schema = _schema(
        FeatureDefinition(
            name="wx", metric="irradiance", category=FeatureCategory.EXTERNAL_FORECAST
        )
    )
    r = FeatureResolver(available=AVAIL).resolve(_req(target="solar_power"), schema)
    assert r.resolutions[0].status is ResolutionStatus.UNAVAILABLE
    assert r.resolutions[0].detail == "no external provider"


@pytest.mark.unit
def test_l_schema_mismatch_explicit():
    schema = _schema(FeatureDefinition(name="a", metric="humidity"))
    extra = FeatureDefinition(name="intruder", metric="humidity")
    req = _req(features=(extra,))
    # Schema is authoritative: non-member request features are not injected.
    r = FeatureResolver(available=AVAIL).resolve(req, schema)
    assert r.ok
    assert r.resolved_names() == ("a",)
    # Direct mismatch path is explicit INVALID.
    rogue = FeatureResolver(available=AVAIL)._resolve_one(req, set(schema.names()), extra)[0]
    assert rogue.status is ResolutionStatus.INVALID


@pytest.mark.unit
def test_m_determinism():
    schema = _schema(
        FeatureDefinition(name="temperature", metric="temperature"),
        FeatureDefinition(name="humidity", metric="humidity"),
    )
    req = _req()
    resolver = FeatureResolver(available=AVAIL)
    assert resolver.resolve(req, schema).to_dict() == resolver.resolve(req, schema).to_dict()


@pytest.mark.unit
def test_n_no_temperature_branch_in_resolver():
    path = (
        Path(__file__).resolve().parent.parent.parent
        / "src"
        / "trendx"
        / "forecasting"
        / "resolution.py"
    )
    source = path.read_text(encoding="utf-8").lower()
    assert "temperature" not in source
