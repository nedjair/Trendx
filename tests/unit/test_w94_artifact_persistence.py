"""W94 — Model artifact persistence + explicit champion promotion.

Covers the full lifecycle:

    Training → Artifact Save → RegisteredModel READY
    → Artifact Load → Compatibility Check → Predict

and explicit promotion:

    promote(B) → Champion B  (A → challenger)

All tests are unit-level (no DB, no scheduler, no worker, no Weather,
no SCADA, no MLflow, no ThingsBoard writes, no migrations).
"""

from __future__ import annotations

import inspect
import subprocess
from pathlib import Path
from unittest.mock import MagicMock

import numpy as np
import pandas as pd
import pytest
from numpy.testing import assert_allclose
from trendx.forecasting.artifact import (
    ArtifactError,
    ArtifactIntegrityError,
    ArtifactNotFoundError,
    FileSystemArtifactStore,
    NullArtifactStore,
    load_registered_model,
)
from trendx.forecasting.contract import (
    Algorithm,
    FeatureCategory,
    FeatureDefinition,
    FeatureSchema,
    ForecastRequest,
)
from trendx.forecasting.dataset import DatasetBuilder
from trendx.forecasting.evaluation import SelectionResult
from trendx.forecasting.registry import (
    ChampionRegistry,
    IncompatibilityReason,
    ModelRole,
    ModelStatus,
    RegisteredModel,
    check_compatibility,
)
from trendx.forecasting.resolution import FeatureResolver
from trendx.forecasting.training import ModelTrainer, TrainingRequest

# ---------------------------------------------------------------------------
# Fixtures and helpers
# ---------------------------------------------------------------------------


def _frame(target_base=20.0, periods=72, freq="30min"):
    idx = pd.date_range("2026-01-01", periods=periods, freq=freq, tz="UTC")
    return pd.DataFrame(
        {"ts": idx, "value": [target_base + (i % 24) * 0.5 for i in range(periods)]}
    )


def _req(
    target="temperature",
    features=(),
    entity_type="DEVICE",
    entity_id="e1",
    horizon=24,
    frequency="1h",
):
    return ForecastRequest(
        tenant_id="tenant-1",
        entity_type=entity_type,
        entity_id=entity_id,
        target_metric=target,
        horizon=horizon,
        frequency=frequency,
        features=features,
    )


def _schema(features=()):
    return FeatureSchema(features=features)


def _dataset(target="temperature", features=(), freq="1h", hz=24, entity="e1"):
    req = _req(target, features, entity_id=entity, horizon=hz, frequency=freq)
    schema = _schema(features)
    frames = {(entity, target): _frame(20.0)}
    for f in features:
        frames[(entity, f.metric or target)] = _frame(30.0)
    resolved = FeatureResolver(available=set(frames)).resolve(req, schema)
    assert resolved.ok
    builder = DatasetBuilder(frame_provider=lambda e, m: frames[(e, m)].copy())
    ds = builder.build(req, resolved)
    return ds


def _fp(features=()):
    return _schema(features).fingerprint()


def _candidate(
    mid="m1",
    target="temperature",
    features=(),
    fp=None,
    freq="1h",
    hz=24,
    entity="e1",
    algo=Algorithm.PROPHET,
    status=ModelStatus.READY,
    role=ModelRole.CHALLENGER,
    uri="file:///dev/null",
    version="1",
):
    return RegisteredModel(
        model_id=mid,
        tenant_id="tenant-1",
        entity_type="DEVICE",
        entity_id=entity,
        target_metric=target,
        algorithm=algo,
        feature_schema_version="v1",
        feature_schema_fingerprint=fp if fp is not None else _fp(features),
        frequency=freq,
        horizon=hz,
        training_window="90d",
        model_version=version,
        status=status,
        role=role,
        model_uri=uri,
    )


def _selection(model):
    from trendx.forecasting.evaluation import CandidateEvaluation

    ev = CandidateEvaluation(
        model_id=model.model_id,
        algorithm=model.algorithm.value,
        folds=(),
        mean_scores={"mae": 1.0},
        valid_folds=1,
        status="ok",
    )
    return SelectionResult(
        winner_model_id=model.model_id,
        reason="best-score",
        criterion="mae",
        evaluations=(ev,),
    )


def _real_model():
    """Return a fitted FourierModel (fast, no external service)."""
    from trendx.forecasting import create_model

    df = _fourier_train_df()
    model = create_model("Fourier")
    model.fit(df)
    return model


def _fourier_train_df():
    stamps = pd.date_range("2026-01-01", periods=120, freq="1h")
    y = 20.0 + 0.05 * np.arange(120) + 5.0 * np.sin(np.arange(120) / 12.0)
    return pd.DataFrame({"ds": stamps, "y": y})


def _trainer(store=None, **kw):
    kw.setdefault("model_factory", lambda algo: _real_model())
    if store is not None:
        kw["artifact_store"] = store
    kw.setdefault("registry", ChampionRegistry())
    return ModelTrainer(**kw)


def _treq(target="temperature", **kw):
    params = {
        "tenant_id": "tenant-1",
        "entity_type": "DEVICE",
        "entity_id": "e1",
        "target_metric": target,
        "frequency": "1h",
        "horizon": 24,
        "training_window": "90d",
    }
    params.update(kw)
    return TrainingRequest(**params)


def _promote(reg, model_id, dataset=None, *, artifact_store=None):
    """Call the W94 promotion API with an explicit W87 reference dataset."""
    return reg.promote(
        model_id,
        dataset if dataset is not None else _dataset(),
        artifact_store=artifact_store,
    )


# ---------------------------------------------------------------------------
# A: Artifact save
# ---------------------------------------------------------------------------


@pytest.fixture
def store(tmp_path):
    return FileSystemArtifactStore(base_path=tmp_path / "artifacts")


@pytest.mark.unit
def test_a_artifact_save(store):
    """save() persists model + checksum + metadata, returns file:// URI."""
    model = _real_model()
    uri = store.save(
        "m1", model, provenance={"algorithm": "Fourier", "feature_schema_fingerprint": _fp()}
    )
    assert uri.startswith("file://")
    assert store.exists(uri)
    meta = store.metadata(uri)
    assert meta.algorithm == "Fourier"
    assert meta.model_id == "m1"
    assert meta.checksum  # non-empty


# ---------------------------------------------------------------------------
# B: Artifact load
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_b_artifact_load(store):
    """load() reconstructs a model that can predict."""
    model = _real_model()
    uri = store.save("m2", model, provenance={"algorithm": "Fourier"})
    loaded = store.load(uri, "Fourier")
    assert loaded is not None
    assert hasattr(loaded, "predict")


# ---------------------------------------------------------------------------
# C: Save → load → predict
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_c_save_load_predict(store):
    """Loaded model produces identical predictions to original."""
    model = _real_model()
    original_pred = model.predict(6)
    uri = store.save("m3", model, provenance={"algorithm": "Fourier"})
    loaded = store.load(uri, "Fourier")
    loaded_pred = loaded.predict(6)
    assert len(loaded_pred.values) == 6
    assert_allclose(original_pred.values, loaded_pred.values, rtol=1e-9)


# ---------------------------------------------------------------------------
# D: Artifact absent
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_d_artifact_absent(store):
    """Loading a non-existent URI raises ArtifactNotFoundError."""
    with pytest.raises(ArtifactNotFoundError):
        store.load("file:///nonexistent/path", "Fourier")
    assert not store.exists("file:///nonexistent/path")


# ---------------------------------------------------------------------------
# E: Artifact corrupted
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_e_artifact_corrupted(store):
    """Tampering with the artifact triggers ArtifactIntegrityError on load."""
    model = _real_model()
    uri = store.save("m4", model, provenance={"algorithm": "Fourier"})
    # Corrupt the model file
    model_path = store._dir_for(uri) / "model.bin"
    with open(model_path, "ab") as f:
        f.write(b"corrupted-garbage")
    assert not store.integrity_check(uri)
    with pytest.raises(ArtifactIntegrityError):
        store.load(uri, "Fourier")


# ---------------------------------------------------------------------------
# F: Registry ↔ artifact linkage
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_f_registry_artifact_linkage(store):
    """A RegisteredModel created by training has a model_uri that the
    store can actually resolve (linkage survives)."""
    m = _candidate("m1", uri="", status=ModelStatus.READY)
    ds = _dataset()
    out = _trainer(store=store, registry=ChampionRegistry()).train(
        _selection(m), ds, _treq(), {m.model_id: m}
    )
    assert out.status == "ok"
    assert (out.model.model_uri or "").startswith("file://")
    assert store.exists(out.model.model_uri)


# ---------------------------------------------------------------------------
# G: Fingerprint conserved through save/load
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_g_fingerprint_conserved(store):
    """metadata.json stores the fingerprint and it equals the original."""
    feats = (FeatureDefinition(name="temperature", metric="temperature", lag="1h"),)
    fp = _fp(feats)
    model = _real_model()
    uri = store.save(
        "fp-test",
        model,
        provenance={
            "algorithm": "Fourier",
            "feature_schema_fingerprint": fp,
        },
    )
    meta = store.metadata(uri)
    assert meta.feature_schema_fingerprint == fp


# ---------------------------------------------------------------------------
# H: Schema mismatch → refus
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_h_schema_mismatch(store):
    """check_compatibility gates on fingerprint equality."""
    fp_v1 = _fp((FeatureDefinition(name="a", metric="a"),))
    fp_v2 = _fp((FeatureDefinition(name="a", metric="a"), FeatureDefinition(name="b", metric="b")))
    m = _candidate("m1", features=(), fp=fp_v1)
    ds = _dataset(
        features=(FeatureDefinition(name="a", metric="a"), FeatureDefinition(name="b", metric="b"))
    )
    ds.metadata["feature_schema_fingerprint"] = fp_v2
    c = check_compatibility(m, ds)
    assert not c.ok and c.reason is IncompatibilityReason.FEATURE_SCHEMA_MISMATCH


# ---------------------------------------------------------------------------
# I: Frequency mismatch → refus
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_i_frequency_mismatch():
    m = _candidate("m1", features=(), freq="1h")
    ds = _dataset(features=(), freq="15m")
    c = check_compatibility(m, ds)
    assert not c.ok and c.reason is IncompatibilityReason.FREQUENCY_MISMATCH


# ---------------------------------------------------------------------------
# J: Horizon mismatch → refus
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_j_horizon_mismatch():
    m = _candidate("m1", features=(), hz=24)
    ds = _dataset(features=(), hz=48)
    c = check_compatibility(m, ds)
    assert not c.ok and c.reason is IncompatibilityReason.HORIZON_MISMATCH


# ---------------------------------------------------------------------------
# K: Target / entity mismatch → refus
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_k_target_entity_mismatch():
    m = _candidate("m1", target="temperature", features=(), entity="e1")
    ds = _dataset(target="humidity", features=(), entity="e1")
    c = check_compatibility(m, ds)
    assert not c.ok and c.reason is IncompatibilityReason.TARGET_MISMATCH

    m2 = _candidate("m2", target="temperature", features=(), entity="e1")
    ds2 = _dataset(target="temperature", features=(), entity="e2")
    c2 = check_compatibility(m2, ds2)
    assert not c2.ok and c2.reason is IncompatibilityReason.ENTITY_MISMATCH


# ---------------------------------------------------------------------------
# L: Fit success + save failure → never READY
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_l_fit_ok_save_fail_never_ready():
    """If the store raises during save, the outcome is 'failed', not 'ok'."""

    class _ExplodingStore:
        def save(self, model_id, model, provenance=None):
            raise OSError("disk full")

        def load(self, uri, algorithm):
            raise ArtifactNotFoundError("nope")

        def exists(self, uri):
            return False

        def integrity_check(self, uri):
            return False

        def metadata(self, uri):
            raise ArtifactNotFoundError("nope")

    m = _candidate("m1", uri="")
    ds = _dataset()
    out = _trainer(store=_ExplodingStore()).train(_selection(m), ds, _treq(), {m.model_id: m})
    assert out.status == "failed"
    assert "artifact" in out.reason.lower()
    assert out.model is None


# ---------------------------------------------------------------------------
# M: Fit success + registry failure → never READY
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_m_fit_ok_registry_fail_never_ready():
    """If the registry rejects registration, outcome is 'failed'."""
    m = _candidate("m1", uri="")
    # Mock register to raise ValueError (simulating duplicate / collision)
    mock_reg = MagicMock(spec=ChampionRegistry)
    mock_reg.register.side_effect = ValueError("duplicate")
    mock_reg.champions.return_value = []
    mock_reg.get.return_value = None

    ds = _dataset()
    out = ModelTrainer(
        model_factory=lambda algo: _real_model(),
        registry=mock_reg,
    ).train(_selection(m), ds, _treq(), {m.model_id: m})
    assert out.status == "failed"
    assert "registry" in out.reason.lower()


# ---------------------------------------------------------------------------
# N: Explicit promotion only
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_n_explicit_promotion_only():
    """promote() must be called explicitly; training doesn't call it."""
    import inspect

    import trendx.forecasting.training as mod

    src = inspect.getsource(mod)
    # ModelTrainer.train must not call promote/promote_to_champion
    assert "promote" not in src.replace("no auto-promote", "").lower()


# ---------------------------------------------------------------------------
# O: Training never promotes
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_o_training_never_promotes(tmp_path):
    reg = ChampionRegistry()
    m = _candidate("m1", uri="")
    store = FileSystemArtifactStore(base_path=tmp_path / "art")
    ds = _dataset()
    out = ModelTrainer(
        model_factory=lambda algo: _real_model(),
        artifact_store=store,
        registry=reg,
    ).train(_selection(m), ds, _treq(), {m.model_id: m})
    assert out.status == "ok"
    assert out.model is not None
    assert out.model.role is ModelRole.CHALLENGER
    assert reg.champions() == []  # no champion after training


# ---------------------------------------------------------------------------
# P: Registration never promotes
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_p_registration_never_promotes(tmp_path):
    """Registering a model via ModelTrainer does not promote it."""
    reg = ChampionRegistry()
    m = _candidate("m1")
    store = FileSystemArtifactStore(base_path=tmp_path / "art")
    ds = _dataset()
    out = ModelTrainer(
        model_factory=lambda algo: _real_model(),
        artifact_store=store,
        registry=reg,
    ).train(_selection(m), ds, _treq(), {m.model_id: m})
    assert out.status == "ok"
    # The registered model is a CHALLENGER, not champion
    assert out.model.role is ModelRole.CHALLENGER
    assert reg.champions() == []


# ---------------------------------------------------------------------------
# Q: Evaluation never promotes
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_q_evaluation_never_promotes():
    """evaluate_candidate / select_best never touch ChampionRegistry."""
    import inspect

    import trendx.forecasting.evaluation as mod

    src = inspect.getsource(mod).lower()
    assert "promote" not in src
    assert "champion" not in src


# ---------------------------------------------------------------------------
# R: Champion A preserved after training B
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_r_champion_preserved_after_training(tmp_path):
    reg = ChampionRegistry()
    champ_a = _candidate("champ-A", uri="file:///dev/null")
    reg.register(champ_a)
    _promote(reg, "champ-A")
    assert reg.champions()[0].model_id == "champ-A"

    # Train B
    m = _candidate("m1", uri="")
    store = FileSystemArtifactStore(base_path=tmp_path / "art")
    ds = _dataset()
    out = ModelTrainer(
        model_factory=lambda algo: _real_model(),
        artifact_store=store,
        registry=reg,
    ).train(_selection(m), ds, _treq(), {m.model_id: m})
    assert out.status == "ok"
    # Champion still A
    assert [c.model_id for c in reg.champions()] == ["champ-A"]


# ---------------------------------------------------------------------------
# S: Champion A preserved after registration B
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_s_champion_preserved_after_registration(tmp_path):
    """Explicitly: register B (as CHALLENGER) — champion A untouched."""
    reg = ChampionRegistry()
    champ_a = _candidate("champ-A", uri="file:///dev/null")
    reg.register(champ_a)
    _promote(reg, "champ-A")

    m = _candidate("m1", uri="")
    store = FileSystemArtifactStore(base_path=tmp_path / "art")
    ds = _dataset()
    ModelTrainer(
        model_factory=lambda algo: _real_model(),
        artifact_store=store,
        registry=reg,
    ).train(_selection(m), ds, _treq(), {m.model_id: m})

    assert [c.model_id for c in reg.champions()] == ["champ-A"]


# ---------------------------------------------------------------------------
# T: promote(B) → B champion
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_t_promote_b_becomes_champion():
    reg = ChampionRegistry()
    reg.register(_candidate("champ-A", uri="file:///dev/null"))
    _promote(reg, "champ-A")

    b = _candidate("challenger-B", uri="file:///dev/null")
    reg.register(b)

    promoted = _promote(reg, "challenger-B")
    assert promoted.role is ModelRole.CHAMPION
    assert [c.model_id for c in reg.champions()] == ["challenger-B"]


# ---------------------------------------------------------------------------
# U: A becomes challenger after promote(B)
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_u_a_becomes_challenger_after_promote_b():
    reg = ChampionRegistry()
    a = _candidate("champ-A", uri="file:///dev/null")
    b = _candidate("challenger-B", uri="file:///dev/null")
    reg.register(a)
    reg.register(b)
    _promote(reg, "champ-A")
    assert reg.champions()[0].model_id == "champ-A"

    _promote(reg, "challenger-B")
    champs = reg.champions()
    assert [c.model_id for c in champs] == ["challenger-B"]
    a_after = reg.get("champ-A")
    assert a_after.role is ModelRole.CHALLENGER


# ---------------------------------------------------------------------------
# V: FAILED cannot be champion
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_v_failed_cannot_be_champion():
    reg = ChampionRegistry()
    failed = _candidate("fail-M", status=ModelStatus.FAILED, uri="file:///dev/null")
    reg.register(failed)
    with pytest.raises(ValueError, match="not READY"):
        _promote(reg, "fail-M")
    assert reg.champions() == []


# ---------------------------------------------------------------------------
# W: INCOMPATIBLE (UNKNOWN algo) cannot be champion
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_w_unknown_algorithm_cannot_be_champion():
    reg = ChampionRegistry()
    unk = _candidate("unk-M", algo=Algorithm.UNKNOWN, uri="file:///dev/null")
    reg.register(unk)
    with pytest.raises(ValueError, match="UNKNOWN"):
        _promote(reg, "unk-M")


def _assert_incompatible_promotion_refused(candidate, dataset, reason):
    reg = ChampionRegistry()
    champion = _candidate("champ-A")
    reg.register(champion)
    _promote(reg, "champ-A")

    reg.register(candidate)
    with pytest.raises(ValueError, match=reason.value):
        _promote(reg, candidate.model_id, dataset)

    assert [model.model_id for model in reg.champions()] == ["champ-A"]
    assert reg.get(candidate.model_id).role is ModelRole.CHALLENGER


@pytest.mark.unit
def test_schema_mismatch_promotion_refused():
    features = (FeatureDefinition(name="a", metric="a"),)
    candidate = _candidate("schema-mismatch", features=(), uri="file:///dev/null")
    dataset = _dataset(features=features)
    _assert_incompatible_promotion_refused(
        candidate,
        dataset,
        IncompatibilityReason.FEATURE_SCHEMA_MISMATCH,
    )


@pytest.mark.unit
def test_frequency_mismatch_promotion_refused():
    candidate = _candidate("frequency-mismatch", freq="1h", uri="file:///dev/null")
    dataset = _dataset(features=(), freq="15m")
    _assert_incompatible_promotion_refused(
        candidate,
        dataset,
        IncompatibilityReason.FREQUENCY_MISMATCH,
    )


@pytest.mark.unit
def test_horizon_mismatch_promotion_refused():
    candidate = _candidate("horizon-mismatch", hz=24, uri="file:///dev/null")
    dataset = _dataset(features=(), hz=48)
    _assert_incompatible_promotion_refused(
        candidate,
        dataset,
        IncompatibilityReason.HORIZON_MISMATCH,
    )


# ---------------------------------------------------------------------------
# X: Artifact absent cannot be champion (with store check)
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_x_artifact_absent_cannot_be_champion(tmp_path):
    reg = ChampionRegistry()
    # Register a READY model but with URI that the store says doesn't exist
    store = FileSystemArtifactStore(base_path=tmp_path / "art")
    m = _candidate("m1", uri="file:///nonexistent")
    reg.register(m)
    with pytest.raises(ArtifactError, match="artifact not found"):
        _promote(reg, "m1", artifact_store=store)
    assert reg.champions() == []


# ---------------------------------------------------------------------------
# Y: Promotion idempotent
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_y_promotion_idempotent():
    reg = ChampionRegistry()
    b = _candidate("B", uri="file:///dev/null")
    reg.register(b)
    first = _promote(reg, "B")
    second = _promote(reg, "B")
    assert first is second or first.model_id == second.model_id
    assert [c.model_id for c in reg.champions()] == ["B"]
    _promote(reg, "B")
    assert [c.model_id for c in reg.champions()] == ["B"]


# ---------------------------------------------------------------------------
# Z: Two entities isolated
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_z_two_entities_isolated():
    reg = ChampionRegistry()
    a1 = _candidate("A1", entity="entity-A", target="temperature")
    b1 = _candidate("B1", entity="entity-B", target="temperature")
    reg.register(a1)
    reg.register(b1)
    _promote(reg, "A1", _dataset(entity="entity-A"))
    _promote(reg, "B1", _dataset(entity="entity-B"))
    champs = reg.champions()
    assert len(champs) == 2
    assert {c.model_id for c in champs} == {"A1", "B1"}


# ---------------------------------------------------------------------------
# AA: Four metrics same path
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_aa_four_metrics_same_path(tmp_path):
    store = FileSystemArtifactStore(base_path=tmp_path / "art")
    reg = ChampionRegistry()
    for target in ("temperature", "humidity", "energy_consumption", "solar_power"):
        m = _candidate(f"m-{target}", target=target)
        ds = _dataset(target=target, features=())
        out = ModelTrainer(
            model_factory=lambda algo: _real_model(),
            artifact_store=store,
            registry=reg,
        ).train(_selection(m), ds, _treq(target), {m.model_id: m})
        assert out.status == "ok", f"failed for {target}"
        assert out.model.target_metric == target
    assert len(reg.champions()) == 0


# ---------------------------------------------------------------------------
# AB: External feature schema isolated
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_ab_external_feature_schema_isolated(tmp_path):
    """Model with external feature has different fingerprint → isolated."""
    feats_no_ext = (FeatureDefinition(name="temperature", metric="temperature", lag="1h"),)
    feats_with_ext = (
        FeatureDefinition(name="temperature", metric="temperature", lag="1h"),
        FeatureDefinition(
            name="cloud_cover",
            metric="cloud_cover",
            category=FeatureCategory.EXTERNAL_FORECAST,
        ),
    )
    fp_no = _fp(feats_no_ext)
    fp_with = _fp(feats_with_ext)
    assert fp_no != fp_with

    reg = ChampionRegistry()
    m_no = _candidate("no-ext", features=feats_no_ext, fp=fp_no)
    m_with = _candidate("with-ext", features=feats_with_ext, fp=fp_with)
    reg.register(m_no)
    reg.register(m_with)

    # Promote both — they should coexist as separate champions
    _promote(reg, "no-ext", _dataset(features=feats_no_ext))
    _promote(reg, "with-ext", _dataset(features=feats_with_ext))
    assert {c.model_id for c in reg.champions()} == {"no-ext", "with-ext"}


# ---------------------------------------------------------------------------
# AC: model_id / version / artifact distinct
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_ac_id_version_artifact_distinct(tmp_path):
    store = FileSystemArtifactStore(base_path=tmp_path / "art")
    reg = ChampionRegistry()
    m = _candidate("m-base", version="1")
    ds = _dataset()
    out = ModelTrainer(
        model_factory=lambda algo: _real_model(),
        artifact_store=store,
        registry=reg,
    ).train(_selection(m), ds, _treq(), {m.model_id: m})
    assert out.status == "ok"
    model = out.model
    # model_id includes version suffix
    assert model.model_id != model.model_version
    assert "@" in model.model_id
    # model_uri is a distinct artifact reference
    assert model.model_uri.startswith("file://")
    assert model.model_uri != model.model_id
    assert model.model_uri != model.model_version


# ---------------------------------------------------------------------------
# AD: Provenance complete
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_ad_provenance_complete(tmp_path):
    store = FileSystemArtifactStore(base_path=tmp_path / "art")
    feats = (FeatureDefinition(name="temperature", metric="temperature", lag="1h"),)
    m = _candidate("m1", uri="", features=feats)
    ds = _dataset(features=feats)
    out = ModelTrainer(
        model_factory=lambda algo: _real_model(),
        artifact_store=store,
        registry=ChampionRegistry(),
    ).train(_selection(m), ds, _treq(), {m.model_id: m})
    assert out.status == "ok"
    meta = store.metadata(out.model.model_uri)
    assert meta.model_id == out.model.model_id
    assert meta.algorithm == "Fourier"
    assert meta.feature_schema_fingerprint == out.model.feature_schema_fingerprint
    assert meta.tenant_id == "tenant-1"
    assert meta.target_metric == "temperature"
    assert meta.frequency == "1h"
    assert meta.horizon == 24
    assert meta.checksum  # non-empty integrity hash
    assert meta.created_at  # non-empty timestamp


# ---------------------------------------------------------------------------
# AE: Determinism
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_ae_determinism(tmp_path):
    store = FileSystemArtifactStore(base_path=tmp_path / "art")
    reg = ChampionRegistry()
    m = _candidate("m1", uri="")
    ds = _dataset()

    a = ModelTrainer(
        model_factory=lambda algo: _real_model(),
        artifact_store=store,
        registry=reg,
    ).train(_selection(m), ds, _treq(), {m.model_id: m})

    b = ModelTrainer(
        model_factory=lambda algo: _real_model(),
        artifact_store=store,
        registry=ChampionRegistry(),
    ).train(_selection(m), ds, _treq(), {m.model_id: m})

    assert a.model.model_id == b.model.model_id
    assert a.model.feature_schema_fingerprint == b.model.feature_schema_fingerprint
    assert a.model.model_version == b.model.model_version
    # Predictions identical
    loaded_a = store.load(a.model.model_uri, "Fourier")
    loaded_b = store.load(b.model.model_uri, "Fourier")
    pred_a = loaded_a.predict(6)
    pred_b = loaded_b.predict(6)
    assert_allclose(pred_a.values, pred_b.values, rtol=1e-9)


# ---------------------------------------------------------------------------
# AF: No Weather dependency in artifact.py
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_af_no_weather_in_artifact():
    import trendx.forecasting.artifact as mod

    src = inspect.getsource(mod).lower()
    assert "weatheradapter" not in src
    assert "weather" not in src.replace("no weather", "")


# ---------------------------------------------------------------------------
# AG: No SCADA / ExternalFeatureProvider concrete dependency in artifact.py
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_ag_no_scada_in_artifact():
    import trendx.forecasting.artifact as mod

    src = inspect.getsource(mod).lower()
    assert "scada" not in src
    assert "externalfeatureprovider" not in src


# ---------------------------------------------------------------------------
# AH: No MLflow
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_ah_no_mlflow_in_artifact():
    import trendx.forecasting.artifact as mod

    src = inspect.getsource(mod).lower()
    assert "mlflow" not in src


# ---------------------------------------------------------------------------
# AI: No ThingsBoard writes
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_ai_no_tb_write_in_artifact():
    import trendx.forecasting.artifact as mod

    src = inspect.getsource(mod).lower()
    assert "thingsboard" not in src
    assert "tb_writeback" not in src
    assert "tb_alarms" not in src


# ---------------------------------------------------------------------------
# AJ: No migration
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_aj_no_migration():
    result = subprocess.run(
        ["git", "status", "--short", "--", "migrations/"],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.stdout.strip() == ""


# ---------------------------------------------------------------------------
# AK: Scheduler untouched
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_ak_scheduler_untouched():
    import trendx.forecasting.artifact as art_mod
    import trendx.forecasting.registry as reg_mod
    import trendx.forecasting.training as tr_mod

    for mod in (art_mod, reg_mod, tr_mod):
        src = inspect.getsource(mod).lower()
        assert "scheduler" not in src
        assert "services.scheduler" not in src


# ---------------------------------------------------------------------------
# AL: Worker untouched
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_al_worker_untouched():
    import trendx.forecasting.artifact as art_mod
    import trendx.forecasting.registry as reg_mod
    import trendx.forecasting.training as tr_mod

    for mod in (art_mod, reg_mod, tr_mod):
        src = inspect.getsource(mod)
        assert "services.worker" not in src
        assert "forecast_worker" not in src


# ---------------------------------------------------------------------------
# AM: No business-specific branch
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_am_no_metric_branch_in_artifact():
    path = (
        Path(__file__).resolve().parent.parent.parent
        / "src"
        / "trendx"
        / "forecasting"
        / "artifact.py"
    )
    src = path.read_text(encoding="utf-8").lower()
    assert "if metric ==" not in src
    assert "if temperature" not in src
    assert "if humidity" not in src
    assert "if energy" not in src
    assert "if solar" not in src


@pytest.mark.unit
def test_am_no_metric_branch_in_training():
    path = (
        Path(__file__).resolve().parent.parent.parent
        / "src"
        / "trendx"
        / "forecasting"
        / "training.py"
    )
    src = path.read_text(encoding="utf-8").lower()
    assert "if metric ==" not in src
    assert "if temperature" not in src


# ---------------------------------------------------------------------------
# AN: Full W84-W93 regression
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_an_w84_w93_regression():
    """The full regression suite for W84-W93 must still pass."""
    result = subprocess.run(
        [
            "python",
            "-m",
            "pytest",
            "tests/unit/test_forecast_contract_generic.py",
            "tests/unit/test_feature_resolution.py",
            "tests/unit/test_forecast_dataset.py",
            "tests/unit/test_model_registry_contract.py",
            "tests/unit/test_evaluation_selection.py",
            "tests/unit/test_forecast_pipeline.py",
            "tests/unit/test_training_registration.py",
            "-q",
            "--no-header",
            "--no-cov",
        ],
        capture_output=True,
        text=True,
        check=False,
        cwd=Path(__file__).resolve().parent.parent.parent,
    )
    assert result.returncode == 0, f"Regression failed:\n{result.stderr[-2000:]}"


# ---------------------------------------------------------------------------
# Additional integration scenarios for reload + compatibility
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_load_registered_model_success(store):
    """load_registered_model returns a working model after save."""
    model = _real_model()
    uri = store.save("rel-1", model, provenance={"algorithm": "Fourier"})
    m = _candidate("rel-1@1", uri=uri, algo=Algorithm.FOURIER)
    reg = ChampionRegistry()
    reg.register(m)

    loaded = load_registered_model(m.model_id, reg.get, store)
    assert loaded is not None
    pred = loaded.predict(6)
    assert len(pred.values) == 6


@pytest.mark.unit
def test_load_registered_model_not_found(store):
    """Loading an unregistered model_id raises."""
    reg = ChampionRegistry()
    with pytest.raises(ArtifactError, match="not found in registry"):
        load_registered_model("nope", reg.get, store)


@pytest.mark.unit
def test_load_registered_model_not_ready(store):
    """Loading a non-READY model raises."""
    m = _candidate("M", status=ModelStatus.FAILED, uri="file:///dev/null")
    reg = ChampionRegistry()
    reg.register(m)
    with pytest.raises(ArtifactError, match="not READY"):
        load_registered_model("M", reg.get, store)


@pytest.mark.unit
def test_load_registered_model_empty_uri(store):
    """Loading a model with empty model_uri raises."""
    m = _candidate("M", uri="")
    reg = ChampionRegistry()
    reg.register(m)
    with pytest.raises(ArtifactError, match="empty model_uri"):
        load_registered_model("M", reg.get, store)


@pytest.mark.unit
def test_load_registered_model_missing_artifact(store):
    """Loading when the artifact doesn't exist on disk raises."""
    m = _candidate("M", uri="file:///nonexistent/path")
    reg = ChampionRegistry()
    reg.register(m)
    with pytest.raises((ArtifactNotFoundError, ArtifactError)):
        load_registered_model("M", reg.get, store)


@pytest.mark.unit
def test_promote_with_store_passing_integrity(store):
    """promote() succeeds when the artifact exists and is intact."""
    model = _real_model()
    uri = store.save("p1", model, provenance={"algorithm": "Fourier"})
    m = _candidate("champ", uri=uri, algo=Algorithm.FOURIER)
    reg = ChampionRegistry()
    reg.register(m)
    promoted = _promote(reg, "champ", artifact_store=store)
    assert promoted.role is ModelRole.CHAMPION


@pytest.mark.unit
def test_promote_with_corrupted_artifact_fails(store):
    """promote() refuses a model whose artifact has a bad checksum."""
    model = _real_model()
    uri = store.save("p2", model, provenance={"algorithm": "Fourier"})
    # Corrupt the artifact
    artifact_path = store._dir_for(uri) / "model.bin"
    with open(artifact_path, "ab") as f:
        f.write(b"tampered")
    m = _candidate("p2", uri=uri, algo=Algorithm.FOURIER)
    reg = ChampionRegistry()
    reg.register(m)
    with pytest.raises(ArtifactIntegrityError, match="integrity"):
        _promote(reg, "p2", artifact_store=store)
    assert reg.champions() == []


@pytest.mark.unit
def test_null_store_save_returns_empty_uri():
    """NullArtifactStore.save returns empty URI (honest default)."""
    store = NullArtifactStore()
    uri = store.save("x", _real_model(), provenance={})
    assert uri == ""
    assert not store.exists("")


@pytest.mark.unit
def test_null_store_load_raises():
    """NullArtifactStore.load always raises."""
    store = NullArtifactStore()
    with pytest.raises(ArtifactNotFoundError):
        store.load("file:///whatever", "Fourier")


@pytest.mark.unit
def test_champion_registry_get_returns_model():
    """ChampionRegistry.get returns the registered model or None."""
    reg = ChampionRegistry()
    m = _candidate("test-get")
    reg.register(m)
    assert reg.get("test-get") is m
    assert reg.get("nonexistent") is None


@pytest.mark.unit
def test_model_trainer_backward_compat_callable_store():
    """ModelTrainer accepts the legacy callable ArtifactStore."""
    reg = ChampionRegistry()
    m = _candidate("m1", uri="")
    ds = _dataset()
    # Legacy callable store (W93 pattern)
    trainer = ModelTrainer(
        model_factory=lambda algo: _real_model(),
        artifact_store=lambda mid, eng: f"memory://{mid}/model",
        registry=reg,
    )
    out = trainer.train(_selection(m), ds, _treq(), {m.model_id: m})
    assert out.status == "ok"
    assert out.model.model_uri == "memory://m1@1/model"


@pytest.mark.unit
def test_reload_contract_compatibility_chain(store):
    """Full reload: save → register → load_registered_model → check_compatibility."""
    feats = (FeatureDefinition(name="temperature", metric="temperature", lag="1h"),)
    fp = _fp(feats)
    model = _real_model()
    uri = store.save(
        "chain-1",
        model,
        provenance={
            "algorithm": "Fourier",
            "feature_schema_fingerprint": fp,
            "frequency": "1h",
            "horizon": 24,
        },
    )

    m = RegisteredModel(
        model_id="chain-1@1",
        tenant_id="tenant-1",
        entity_type="DEVICE",
        entity_id="e1",
        target_metric="temperature",
        algorithm=Algorithm.FOURIER,
        feature_schema_version="v1",
        feature_schema_fingerprint=fp,
        frequency="1h",
        horizon=24,
        model_version="1",
        status=ModelStatus.READY,
        role=ModelRole.CHALLENGER,
        model_uri=uri,
    )
    reg = ChampionRegistry()
    reg.register(m)

    # Load the artifact
    loaded = load_registered_model(m.model_id, reg.get, store)
    assert hasattr(loaded, "predict")

    # Compatibility check should pass with a matching dataset
    ds = _dataset(features=feats)
    compat = check_compatibility(m, ds)
    assert compat.ok
