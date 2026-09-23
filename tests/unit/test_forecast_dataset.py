"""Generic dataset builder tests (W86)."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest
from trendx.forecasting.contract import (
    FeatureCategory,
    FeatureDefinition,
    FeatureSchema,
    ForecastRequest,
)
from trendx.forecasting.dataset import DatasetBuilder, ForecastDataset
from trendx.forecasting.resolution import FeatureResolver


def _frame(start="2026-01-01", periods=72, freq="30min", tz="Europe/Paris", base=20.0):
    idx = pd.date_range(start, periods=periods, freq=freq, tz=tz)
    vals = [base + (i % 24) * 0.5 for i in range(periods)]
    return pd.DataFrame({"ts": idx, "value": vals})


def _req(target, features=(), entity="entity-A"):
    return ForecastRequest(
        tenant_id="tenant-1",
        entity_type="DEVICE",
        entity_id=entity,
        target_metric=target,
        horizon=24,
        frequency="1h",
        features=features,
    )


def _resolved(req, schema, avail):
    return FeatureResolver(available=avail).resolve(req, schema)


def _provider(store):
    def _get(entity_id, metric):
        return store.get((entity_id, metric), pd.DataFrame()).copy()

    return _get


AV_T = {("entity-A", "temperature")}
AV_TH = {("entity-A", "temperature"), ("entity-A", "humidity")}


@pytest.mark.unit
def test_a_temperature_dataset():
    req = _req("temperature")
    schema = FeatureSchema(
        features=(FeatureDefinition(name="temperature", metric="temperature", lag="1h"),)
    )
    # target itself excluded; use lagged self-feature
    store = {("entity-A", "temperature"): _frame()}
    ds = DatasetBuilder(frame_provider=_provider(store)).build(req, _resolved(req, schema, AV_T))
    assert isinstance(ds, ForecastDataset)
    assert ds.target.name == "temperature"
    assert len(ds.target) > 0


@pytest.mark.unit
def test_b_humidity_same_builder():
    req = _req("humidity")
    schema = FeatureSchema(
        features=(FeatureDefinition(name="humidity", metric="humidity", lag="1h"),)
    )
    store = {("entity-A", "humidity"): _frame(base=55.0)}
    ds = DatasetBuilder(frame_provider=_provider(store)).build(
        req, _resolved(req, schema, {("entity-A", "humidity")})
    )
    assert ds.target.name == "humidity"


@pytest.mark.unit
def test_c_energy_xy_separation():
    req = _req(
        "energy_consumption",
        features=(
            FeatureDefinition(name="temperature", metric="temperature"),
            FeatureDefinition(name="humidity", metric="humidity"),
            FeatureDefinition(
                name="occupancy", metric="occupancy", category=FeatureCategory.KNOWN_FUTURE
            ),
        ),
    )
    schema = FeatureSchema(features=req.features)
    store = {
        ("entity-A", "energy_consumption"): _frame(base=100.0),
        ("entity-A", "temperature"): _frame(base=20.0),
        ("entity-A", "humidity"): _frame(base=55.0),
        ("entity-A", "occupancy"): _frame(base=3.0),
    }
    avail = set(store) | {("entity-A", "occupancy")}
    ds = DatasetBuilder(frame_provider=_provider(store)).build(req, _resolved(req, schema, avail))
    assert list(ds.X.columns) == ["temperature", "humidity", "occupancy"]
    assert "energy_consumption" not in ds.X.columns
    assert ds.target.name == "energy_consumption"


@pytest.mark.unit
def test_d_solar_no_specific_code():
    req = _req(
        "solar_power",
        features=(
            FeatureDefinition(name="irradiance", metric="irradiance"),
            FeatureDefinition(name="cloud_cover", metric="cloud_cover"),
            FeatureDefinition(name="temperature", metric="temperature"),
        ),
    )
    schema = FeatureSchema(features=req.features)
    store = {
        ("entity-A", "solar_power"): _frame(base=300.0),
        ("entity-A", "irradiance"): _frame(base=500.0),
        ("entity-A", "cloud_cover"): _frame(base=0.3),
        ("entity-A", "temperature"): _frame(base=20.0),
    }
    ds = DatasetBuilder(frame_provider=_provider(store)).build(
        req, _resolved(req, schema, set(store))
    )
    assert list(ds.X.columns) == ["irradiance", "cloud_cover", "temperature"]


@pytest.mark.unit
def test_e_target_not_auto_feature():
    req = _req("energy_consumption")
    schema = FeatureSchema(features=())
    store = {("entity-A", "energy_consumption"): _frame(base=100.0)}
    ds = DatasetBuilder(frame_provider=_provider(store)).build(
        req, _resolved(req, schema, set(store))
    )
    assert list(ds.X.columns) == []


@pytest.mark.unit
def test_f_lag_shift():
    req = _req("humidity")
    schema = FeatureSchema(
        features=(FeatureDefinition(name="t_lag", metric="temperature", lag="1h"),)
    )
    store = {
        ("entity-A", "humidity"): _frame(),
        ("entity-A", "temperature"): _frame(),
    }
    ds = DatasetBuilder(frame_provider=_provider(store)).build(
        req, _resolved(req, schema, set(store))
    )
    # lagged feature timestamps shifted +1h vs target grid
    assert (ds.X["t_lag"].notna()).any()
    assert ds.metadata["usable_rows"] > 0


@pytest.mark.unit
def test_g_multi_feature_alignment():
    req = _req(
        "energy_consumption",
        features=(
            FeatureDefinition(name="temperature", metric="temperature"),
            FeatureDefinition(name="humidity", metric="humidity"),
        ),
    )
    schema = FeatureSchema(features=req.features)
    temp = _frame()
    hum = _frame().iloc[4:].reset_index(drop=True)  # shifted/missing head
    store = {
        ("entity-A", "energy_consumption"): _frame(base=100.0),
        ("entity-A", "temperature"): temp,
        ("entity-A", "humidity"): hum,
    }
    ds = DatasetBuilder(frame_provider=_provider(store)).build(
        req, _resolved(req, schema, set(store))
    )
    assert (ds.ds.diff().dropna() == pd.Timedelta("1h")).all()
    assert len(ds.target) == len(ds.X)


@pytest.mark.unit
def test_h_timezone_instant_preserved():
    req = _req("temperature")
    schema = FeatureSchema(
        features=(FeatureDefinition(name="temperature", metric="temperature", lag="1h"),)
    )
    store = {("entity-A", "temperature"): _frame(tz="Europe/Paris")}
    ds = DatasetBuilder(frame_provider=_provider(store)).build(req, _resolved(req, schema, AV_T))
    assert str(ds.ds.dt.tz) == "None"
    # 2026-01-01 00:00+01:00 == 2025-12-31 23:00 UTC naive
    assert ds.ds.iloc[0] == pd.Timestamp("2025-12-31 23:00:00")


@pytest.mark.unit
def test_i_irregular_to_hourly():
    idx = pd.to_datetime(
        ["2026-01-01 00:07:00", "2026-01-01 00:52:00", "2026-01-01 01:20:00", "2026-01-01 02:05:00"]
    ).tz_localize("UTC")
    req = _req("temperature")
    schema = FeatureSchema(
        features=(FeatureDefinition(name="temperature", metric="temperature", lag="1h"),)
    )
    store = {("entity-A", "temperature"): pd.DataFrame({"ts": idx, "value": [1.0, 2.0, 3.0, 4.0]})}
    ds = DatasetBuilder(frame_provider=_provider(store)).build(req, _resolved(req, schema, AV_T))
    assert (ds.ds.diff().dropna() == pd.Timedelta("1h")).all()


@pytest.mark.unit
def test_j_missing_required_controlled():
    req = _req("temperature")
    schema = FeatureSchema(
        features=(FeatureDefinition(name="must", metric="ghost", required=True),)
    )
    resolved = _resolved(req, schema, AV_T)
    assert not resolved.ok
    with pytest.raises(ValueError, match="Unresolved"):
        DatasetBuilder(frame_provider=_provider({})).build(req, resolved)


@pytest.mark.unit
def test_k_optional_unavailable_explicit():
    req = _req("temperature")
    schema = FeatureSchema(features=(FeatureDefinition(name="maybe", metric="ghost"),))
    resolved = _resolved(req, schema, AV_T)
    assert resolved.ok
    store = {("entity-A", "temperature"): _frame()}
    ds = DatasetBuilder(frame_provider=_provider(store)).build(req, resolved)
    assert "maybe" not in ds.X.columns
    assert ds.metadata["dropped_optional"] == ["maybe"]


@pytest.mark.unit
def test_l_schema_fingerprint_kept():
    req = _req("temperature")
    schema = FeatureSchema(
        schema_version="v2",
        features=(FeatureDefinition(name="temperature", metric="temperature", lag="1h"),),
    )
    resolved = _resolved(req, schema, AV_T)
    store = {("entity-A", "temperature"): _frame()}
    ds = DatasetBuilder(frame_provider=_provider(store)).build(req, resolved)
    assert ds.metadata["feature_schema_fingerprint"] == schema.fingerprint()


@pytest.mark.unit
def test_m_determinism():
    req = _req(
        "energy_consumption",
        features=(
            FeatureDefinition(name="temperature", metric="temperature"),
            FeatureDefinition(name="humidity", metric="humidity"),
        ),
    )
    schema = FeatureSchema(features=req.features)
    store = {
        ("entity-A", "energy_consumption"): _frame(base=100.0),
        ("entity-A", "temperature"): _frame(base=20.0),
        ("entity-A", "humidity"): _frame(base=55.0),
    }
    builder = DatasetBuilder(frame_provider=_provider(store))
    resolved = _resolved(req, schema, set(store))
    a = builder.build(req, resolved).metadata
    b = builder.build(req, resolved).metadata
    assert a == b


@pytest.mark.unit
def test_n_no_temperature_branch_in_builder():
    path = (
        Path(__file__).resolve().parent.parent.parent
        / "src"
        / "trendx"
        / "forecasting"
        / "dataset.py"
    )
    assert "temperature" not in path.read_text(encoding="utf-8").lower()


@pytest.mark.unit
def test_o_empty_data_explicit():
    req = _req("temperature")
    schema = FeatureSchema(features=())
    resolved = _resolved(req, schema, AV_T)
    with pytest.raises(ValueError, match="Missing target"):
        DatasetBuilder(frame_provider=_provider({})).build(req, resolved)


@pytest.mark.unit
def test_p_invalid_frame_controlled():
    req = _req("temperature")
    schema = FeatureSchema(features=())
    resolved = _resolved(req, schema, AV_T)
    bad = {("entity-A", "temperature"): pd.DataFrame({"nope": [1.0]})}
    with pytest.raises(ValueError, match="ts/value"):
        DatasetBuilder(frame_provider=_provider(bad)).build(req, resolved)
