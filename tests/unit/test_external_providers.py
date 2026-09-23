"""External feature provider tests (W91)."""

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
from trendx.forecasting.providers import (
    ExternalFeatureProvider,
    ExternalFeatureQuery,
    InMemoryProvider,
    ProviderInvalidDataError,
    WeatherAdapter,
    collect_external_frames,
)
from trendx.forecasting.resolution import FeatureResolver, ResolutionStatus


def _frame(periods=48, tz="UTC", base=10.0):
    idx = pd.date_range("2026-01-01", periods=periods, freq="30min", tz=tz)
    return pd.DataFrame({"ts": idx, "value": [base + (i % 24) * 0.2 for i in range(periods)]})


def _req(target="energy_consumption", features=()):
    return ForecastRequest(
        tenant_id="tenant-1",
        entity_type="DEVICE",
        entity_id="entity-A",
        target_metric=target,
        horizon=24,
        frequency="1h",
        features=features,
    )


def _resolve(req, schema, providers=()):
    return FeatureResolver(
        available={("entity-A", req.target_metric)},
        external_providers=providers,
    ).resolve(req, schema)


WX = FeatureDefinition(
    name="cloud_cover",
    metric="cloud_cover",
    category=FeatureCategory.EXTERNAL_FORECAST,
    source="weather",
)


@pytest.mark.unit
def test_a_provider_contract():
    assert isinstance(InMemoryProvider(frames={}), ExternalFeatureProvider)


@pytest.mark.unit
def test_b_fake_injectable():
    p = InMemoryProvider(frames={("loc-A", "cloud_cover"): _frame()})
    df = p.fetch(
        ExternalFeatureQuery(
            feature_name="cloud_cover",
            metric="cloud_cover",
            entity_id="entity-A",
            location="loc-A",
        )
    )
    assert list(df.columns) == ["ts", "value"]


@pytest.mark.unit
def test_c_external_category():
    assert FeatureCategory.EXTERNAL_FORECAST.value == "external_forecast"


@pytest.mark.unit
def test_d_weather_resolved_with_provider():
    req = _req(features=(WX,))
    schema = FeatureSchema(features=(WX,))
    r = _resolve(req, schema, providers=("weather",))
    by_name = {x.name: x for x in r.resolutions}
    assert by_name["cloud_cover"].status is ResolutionStatus.RESOLVED


@pytest.mark.unit
def test_e_optional_unavailable():
    req = _req(features=(WX,))
    schema = FeatureSchema(features=(WX,))
    r = _resolve(req, schema, providers=())
    assert r.ok
    assert r.resolutions[0].status is ResolutionStatus.UNAVAILABLE


@pytest.mark.unit
def test_f_required_unavailable_fails():
    w = FeatureDefinition(
        name="cloud_cover",
        metric="cloud_cover",
        category=FeatureCategory.EXTERNAL_FORECAST,
        source="weather",
        required=True,
    )
    req = _req(features=(w,))
    r = _resolve(req, FeatureSchema(features=(w,)), providers=())
    assert not r.ok


@pytest.mark.unit
def test_g_malformed_provider_invalid():
    p = InMemoryProvider(frames={("loc-A", "x"): pd.DataFrame({"nope": [1.0]})})
    with pytest.raises(ProviderInvalidDataError):
        p.fetch(ExternalFeatureQuery("x", "x", "entity-A", location="loc-A"))


@pytest.mark.unit
def test_h_no_provider_configured():
    req = _req(features=(WX,))
    schema = FeatureSchema(features=(WX,))
    r = _resolve(req, schema, providers=())
    frames, records = collect_external_frames(r, providers={})
    assert frames == {}
    assert records == []


@pytest.mark.unit
def test_i_no_external_features_request():
    req = _req(target="energy_consumption")
    schema = FeatureSchema(features=())
    r = _resolve(req, schema, providers=())
    assert r.ok
    frames, records = collect_external_frames(r, providers={})
    assert frames == {}


@pytest.mark.unit
def test_j_weather_plus_historical():
    hist = FeatureDefinition(name="temperature", metric="temperature")
    req = _req(features=(hist, WX))
    schema = FeatureSchema(features=(hist, WX))
    avail = {("entity-A", "energy_consumption"), ("entity-A", "temperature")}
    r = FeatureResolver(available=avail, external_providers=("weather",)).resolve(req, schema)
    by_name = {x.name: x for x in r.resolutions}
    assert by_name["temperature"].status is ResolutionStatus.RESOLVED
    assert by_name["cloud_cover"].status is ResolutionStatus.RESOLVED


@pytest.mark.unit
def test_k_location_isolation():
    inner = InMemoryProvider(
        frames={
            ("loc-A", "cloud_cover"): _frame(base=0.1),
            ("loc-B", "cloud_cover"): _frame(base=0.9),
        }
    )
    adapter = WeatherAdapter(inner, {"entity-A": "loc-A", "entity-B": "loc-B"})
    a = adapter.fetch(ExternalFeatureQuery("c", "cloud_cover", "entity-A"))
    b = adapter.fetch(ExternalFeatureQuery("c", "cloud_cover", "entity-B"))
    assert abs(a["value"].mean() - b["value"].mean()) > 0.5


@pytest.mark.unit
def test_l_1h_frequency_carried():
    q = ExternalFeatureQuery("c", "cloud_cover", "entity-A", frequency="1h")
    assert q.frequency == "1h"


@pytest.mark.unit
def test_m_15m_frequency_carried():
    q = ExternalFeatureQuery("c", "cloud_cover", "entity-A", frequency="15m")
    assert q.frequency == "15m"


@pytest.mark.unit
def test_n_horizon_24_carried():
    assert ExternalFeatureQuery("c", "m", "e", horizon=24).horizon == 24


@pytest.mark.unit
def test_o_horizon_96_carried():
    assert ExternalFeatureQuery("c", "m", "e", horizon=96).horizon == 96


@pytest.mark.unit
def test_p_timezone_frames_usable_by_builder():
    from trendx.forecasting.dataset import DatasetBuilder

    req = _req(target="temperature", features=())
    schema = FeatureSchema(features=())
    r = _resolve(req, schema)
    store = {("entity-A", "temperature"): _frame(tz="Europe/Paris")}
    ds = DatasetBuilder(frame_provider=lambda e, m: store[(e, m)].copy()).build(req, r)
    assert str(ds.ds.dt.tz) == "None"


@pytest.mark.unit
def test_q_deterministic_collect():
    req = _req(features=(WX,))
    schema = FeatureSchema(features=(WX,))
    r = _resolve(req, schema, providers=("weather",))
    p = InMemoryProvider(frames={("entity-A", "cloud_cover"): _frame()})
    f1, _ = collect_external_frames(r, {"weather": p})
    f2, _ = collect_external_frames(r, {"weather": p})
    assert f1[("entity-A", "cloud_cover")].equals(f2[("entity-A", "cloud_cover")])


@pytest.mark.unit
def test_r_fingerprint_changes_with_external_feature():
    s1 = FeatureSchema(features=(FeatureDefinition(name="temperature", metric="temperature"),))
    s2 = FeatureSchema(
        features=(
            FeatureDefinition(name="temperature", metric="temperature"),
            WX,
        )
    )
    assert s1.fingerprint() != s2.fingerprint()


@pytest.mark.unit
def test_s_w87_mismatch_v1_vs_v2():
    from trendx.forecasting.dataset import DatasetBuilder
    from trendx.forecasting.registry import (
        Algorithm,
        IncompatibilityReason,
        ModelRole,
        ModelStatus,
        RegisteredModel,
        check_compatibility,
    )

    s1 = FeatureSchema(
        schema_version="v1",
        features=(
            FeatureDefinition(name="temperature", metric="temperature", lag="1h"),
            FeatureDefinition(name="humidity", metric="humidity"),
        ),
    )
    s2 = FeatureSchema(
        schema_version="v2",
        features=(
            FeatureDefinition(name="temperature", metric="temperature", lag="1h"),
            FeatureDefinition(name="humidity", metric="humidity"),
            FeatureDefinition(name="cloud_cover", metric="cloud_cover"),
        ),
    )
    frames = {
        ("entity-A", "temperature"): _frame(base=20.0),
        ("entity-A", "humidity"): _frame(base=55.0),
        ("entity-A", "cloud_cover"): _frame(base=0.3),
    }
    req = ForecastRequest(
        tenant_id="t",
        entity_type="DEVICE",
        entity_id="entity-A",
        target_metric="temperature",
        horizon=24,
        frequency="1h",
        features=s2.features,
    )
    resolved = FeatureResolver(available=set(frames)).resolve(req, s2)
    assert resolved.ok
    ds = DatasetBuilder(frame_provider=lambda e, m: frames[(e, m)].copy()).build(req, resolved)
    model = RegisteredModel(
        model_id="m",
        tenant_id="t",
        entity_id="entity-A",
        target_metric="temperature",
        algorithm=Algorithm.PROPHET,
        feature_schema_version="v1",
        feature_schema_fingerprint=s1.fingerprint(),
        frequency="1h",
        horizon=24,
        status=ModelStatus.READY,
        role=ModelRole.CHALLENGER,
        model_uri="runs:/r/model",
    )
    c = check_compatibility(model, ds)
    assert not c.ok and c.reason is IncompatibilityReason.FEATURE_SCHEMA_MISMATCH


@pytest.mark.unit
def test_t_no_opportunistic_features():
    req = _req(features=(WX,))
    schema = FeatureSchema(features=(WX,))
    r = _resolve(req, schema, providers=("weather",))
    p = InMemoryProvider(
        frames={
            ("entity-A", "cloud_cover"): _frame(),
            ("entity-A", "wind_speed"): _frame(),
        }
    )
    frames, _ = collect_external_frames(r, {"weather": p})
    assert set(frames) == {("entity-A", "cloud_cover")}


@pytest.mark.unit
def test_u_no_removal_of_external_feature():
    req = _req(features=(WX,))
    schema = FeatureSchema(features=(WX,))
    r = _resolve(req, schema, providers=("weather",))
    assert [x.name for x in r.resolutions] == ["cloud_cover"]


@pytest.mark.unit
def test_v_generic_provider_not_weather_specific():
    assert isinstance(InMemoryProvider(frames={}), ExternalFeatureProvider)
    assert "weather" not in type(InMemoryProvider).__name__.lower()


@pytest.mark.unit
def test_w_scada_generic_source():
    p = InMemoryProvider(frames={("plant-1", "grid_load_forecast"): _frame()})
    df = p.fetch(ExternalFeatureQuery("g", "grid_load_forecast", "plant-1", location="plant-1"))
    assert len(df) == 48


@pytest.mark.unit
def test_x_scheduler_unchanged():
    import inspect

    import trendx.scheduler.discovery as mod

    src = inspect.getsource(mod)
    assert "ExternalFeatureProvider" not in src and "WeatherAdapter" not in src


@pytest.mark.unit
def test_y_worker_unchanged():
    import inspect

    import trendx.services.forecast_worker as mod

    src = inspect.getsource(mod)
    assert "ExternalFeatureProvider" not in src and "WeatherAdapter" not in src


@pytest.mark.unit
def test_z_no_network_imports():
    import inspect

    import trendx.forecasting.providers as mod

    src = inspect.getsource(mod).lower()
    assert "urllib" not in src
    assert "requests" not in src
    assert "http" not in src
    assert "socket" not in src


@pytest.mark.unit
def test_aa_no_mlflow():
    import inspect

    import trendx.forecasting.providers as mod

    assert "mlflow" not in inspect.getsource(mod).lower()


@pytest.mark.unit
def test_ab_no_thingsboard():
    import inspect

    import trendx.forecasting.providers as mod

    assert "thingsboard" not in inspect.getsource(mod).lower()


@pytest.mark.unit
def test_ac_no_metric_branch():
    path = (
        Path(__file__).resolve().parent.parent.parent
        / "src"
        / "trendx"
        / "forecasting"
        / "providers.py"
    )
    src = path.read_text(encoding="utf-8").lower()
    assert "temperature" not in src
    assert "if metric ==" not in src


@pytest.mark.unit
def test_ad_no_migration_files():
    import subprocess

    out = subprocess.run(
        ["git", "status", "--short", "--", "migrations/"],
        capture_output=True,
        text=True,
        check=False,
    )
    assert out.stdout.strip() == ""
