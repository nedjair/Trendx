"""Generic model artifact persistence (W94): save / load / integrity.

Injection port — ``ModelArtifactStore`` Protocol.  No cloud / external-storage
coupling: the default concrete implementation
``FileSystemArtifactStore`` writes to an injectable local directory.

Structure per artifact::

    <base>/<safe_model_id>/
        model.bin          # whatever model.save() writes
        checksum.sha256    # SHA-256 hex digest of model.bin
        metadata.json      # provenance + algorithm + fingerprint + etc.

The ``load_registered_model`` helper ties the store to the W87
``ChampionRegistry``: it resolves a model_id, validates status + URI,
verifies integrity, then reconstructs the ``ForecastModel``.
"""

from __future__ import annotations

import hashlib
import json
import re
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

import pandas as pd
from loguru import logger

if TYPE_CHECKING:
    from trendx.forecasting.registry import RegisteredModel


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------


class ArtifactError(Exception):
    """Base error for artifact store operations."""


class ArtifactNotFoundError(ArtifactError):
    """Artifact URI resolves to nothing on disk."""


class ArtifactIntegrityError(ArtifactError):
    """Checksum mismatch — artifact may be corrupted or tampered."""


# ---------------------------------------------------------------------------
# Provenance metadata
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ArtifactMetadata:
    """Provenance captured at save time (immutable, serializable)."""

    model_id: str
    algorithm: str
    feature_schema_version: str = ""
    feature_schema_fingerprint: str = ""
    frequency: str = ""
    horizon: int = 0
    training_window: str = ""
    tenant_id: str = ""
    entity_type: str = ""
    entity_id: str = ""
    target_metric: str = ""
    model_version: str = ""
    created_at: str = ""
    checksum: str = ""
    extra: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "model_id": self.model_id,
            "algorithm": self.algorithm,
            "feature_schema_version": self.feature_schema_version,
            "feature_schema_fingerprint": self.feature_schema_fingerprint,
            "frequency": self.frequency,
            "horizon": self.horizon,
            "training_window": self.training_window,
            "tenant_id": self.tenant_id,
            "entity_type": self.entity_type,
            "entity_id": self.entity_id,
            "target_metric": self.target_metric,
            "model_version": self.model_version,
            "created_at": self.created_at,
            "checksum": self.checksum,
            "extra": self.extra,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> ArtifactMetadata:
        return cls(
            model_id=str(data.get("model_id", "")),
            algorithm=str(data.get("algorithm", "")),
            feature_schema_version=str(data.get("feature_schema_version", "")),
            feature_schema_fingerprint=str(data.get("feature_schema_fingerprint", "")),
            frequency=str(data.get("frequency", "")),
            horizon=int(data.get("horizon", 0)),
            training_window=str(data.get("training_window", "")),
            tenant_id=str(data.get("tenant_id", "")),
            entity_type=str(data.get("entity_type", "")),
            entity_id=str(data.get("entity_id", "")),
            target_metric=str(data.get("target_metric", "")),
            model_version=str(data.get("model_version", "")),
            created_at=str(data.get("created_at", "")),
            checksum=str(data.get("checksum", "")),
            extra=data.get("extra", {}),
        )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_PROVENANCE_KEYS = frozenset(ArtifactMetadata.__dataclass_fields__)


def _sha256_path(path: str | Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


_SAFE_RE = re.compile(r"[^a-zA-Z0-9._@-]+")


def _safe_name(name: str) -> str:
    return _SAFE_RE.sub("_", name or "unknown")


def _algorithm_for_model(model: Any) -> str:
    """Derive the algorithm name from a model object (reverse lookup in registry)."""
    from trendx.forecasting import MODEL_REGISTRY

    for reg_name, cls in MODEL_REGISTRY.items():
        if type(model) is cls or isinstance(model, cls):
            return reg_name
    raise ValueError(f"Unknown model type: {type(model).__name__}")


# ---------------------------------------------------------------------------
# Protocol + null adapter
# ---------------------------------------------------------------------------


@runtime_checkable
class ModelArtifactStore(Protocol):
    """Abstract artifact persistence (W94).

    Implementations must be injectable and decoupled from any specific
    backend (cloud / object-store / filesystem-specific).
    """

    def save(
        self,
        model_id: str,
        model: Any,
        provenance: dict[str, Any] | None = None,
    ) -> str:
        """Persist *model* and return an opaque URI resolvable by ``load``."""
        ...

    def load(self, uri: str, algorithm: str) -> Any:
        """Reconstruct and return a model from *uri*.

        Raises ``ArtifactNotFoundError`` or ``ArtifactIntegrityError`` on
        failure — never returns a silently-wrong model.
        """
        ...

    def exists(self, uri: str) -> bool:
        """True if the artifact referenced by *uri* is present."""
        ...

    def integrity_check(self, uri: str) -> bool:
        """True only if stored checksum matches actual artifact bytes."""
        ...

    def metadata(self, uri: str) -> ArtifactMetadata:
        """Return provenance metadata for the artifact (read-only)."""
        ...


class NullArtifactStore:
    """Empty/honest store — save returns empty URI, load always raises.

    Mirrors the W93 default where ``artifact_store`` defaults to a callable
    returning ``""``.  Models saved with this store can never be reloaded.
    """

    def save(
        self,
        model_id: str,
        model: Any,
        provenance: dict[str, Any] | None = None,
    ) -> str:
        logger.warning(
            "NullArtifactStore.save: no persistence backend configured for {}",
            model_id,
        )
        return ""

    def load(self, uri: str, algorithm: str) -> Any:
        raise ArtifactNotFoundError(
            "NullArtifactStore cannot load: no persistence backend configured"
        )

    def exists(self, uri: str) -> bool:
        return False

    def integrity_check(self, uri: str) -> bool:
        return False

    def metadata(self, uri: str) -> ArtifactMetadata:
        raise ArtifactNotFoundError(
            "NullArtifactStore has no metadata: no persistence backend configured"
        )


class _CallableArtifactStoreAdapter:
    """Adapter wrapping a plain ``Callable[[str, Any], str]`` to satisfy
    ``ModelArtifactStore``.

    Only ``save`` is functional (delegates to the callable).  ``load``,
    ``exists``, ``integrity_check`` and ``metadata`` are best-effort so that
    the W93 backward-compat path keeps working for save-only scenarios.
    """

    def __init__(self, save_fn: Any) -> None:
        self._save_fn = save_fn

    def save(
        self,
        model_id: str,
        model: Any,
        provenance: dict[str, Any] | None = None,
    ) -> str:
        return str(self._save_fn(model_id, model))

    def load(self, uri: str, algorithm: str) -> Any:
        if uri.startswith("memory://"):
            raise ArtifactNotFoundError(f"Callable store cannot reload in-memory artifact: {uri}")
        raise ArtifactNotFoundError(f"Callable store cannot load: {uri}")

    def exists(self, uri: str) -> bool:
        return bool(uri)

    def integrity_check(self, uri: str) -> bool:
        return bool(uri) and not uri.startswith("memory://")

    def metadata(self, uri: str) -> ArtifactMetadata:
        return ArtifactMetadata(model_id="", algorithm="")


# ---------------------------------------------------------------------------
# Concrete filesystem implementation
# ---------------------------------------------------------------------------


class FileSystemArtifactStore:
    """Concrete ``ModelArtifactStore`` on local disk.

    Parameters
    ----------
    base_path : str | Path | None
        Root directory for artifacts.  Defaults to
        ``<tempfile.gettempdir()>/trendx_artifacts`` so tests never require
        a pre-existing path and never pollute production volumes.
    """

    def __init__(self, base_path: str | Path | None = None) -> None:
        import tempfile

        if base_path is None:
            base_path = Path(tempfile.gettempdir()) / "trendx_artifacts"
        self._base = Path(base_path).resolve()
        self._base.mkdir(parents=True, exist_ok=True)
        logger.debug("FileSystemArtifactStore base={}", self._base)

    # -- internal path resolution ------------------------------------------------

    def _dir_for(self, uri: str) -> Path:
        if uri.startswith("file://"):
            p = Path(uri[7:])
        else:
            p = Path(uri)
        return p.resolve()

    def _artifact_path(self, model_dir: Path) -> Path:
        return model_dir / "model.bin"

    def _checksum_path(self, model_dir: Path) -> Path:
        return model_dir / "checksum.sha256"

    def _metadata_path(self, model_dir: Path) -> Path:
        return model_dir / "metadata.json"

    def _history_path(self, model_dir: Path) -> Path:
        """Companion CSV storing training history (``_train_df``) for models
        whose ``save()``/``load()`` don't persist it (e.g. FourierModel).
        """
        return model_dir / "train_history.csv"

    # -- history helpers (mirrors forecast_pyfunc export/import) ----------------

    @staticmethod
    def _export_history(model: Any, path: Path) -> None:
        """Persist ``_train_df`` (ds/y columns) as CSV for later re-prediction.

        Mirrors ``forecast_pyfunc.export_history`` but is self-contained so the
        artifact store has no dependency on the mlops module.
        """
        frame = getattr(model, "_train_df", None)
        if frame is None or len(frame) == 0:
            return  # nothing to persist — model didn't keep training history
        slim = pd.DataFrame(
            {
                "ds": pd.to_datetime(frame["ds"]),
                "y": pd.to_numeric(frame["y"], errors="coerce").astype(float),
            }
        )
        slim.to_csv(str(path), index=False)

    @staticmethod
    def _import_history(model: Any, path: Path) -> None:
        """Restore ``_train_df`` from the companion CSV produced by
        ``_export_history`` (mirrors ``forecast_pyfunc.import_history``).
        """
        if not path.exists():
            return  # backward-compatible: no history saved
        frame = pd.read_csv(str(path), parse_dates=["ds"])
        model._train_df = pd.DataFrame(
            {
                "ds": pd.to_datetime(frame["ds"]),
                "y": pd.to_numeric(frame["y"], errors="coerce").astype(float),
            }
        )

    # -- public API --------------------------------------------------------------

    def save(
        self,
        model_id: str,
        model: Any,
        provenance: dict[str, Any] | None = None,
    ) -> str:
        """Persist *model* to ``<base>/<safe_name>/`` and return ``file://`` URI."""
        prov = dict(provenance or {})

        # Derive algorithm from provenance or model type.
        algorithm = prov.get("algorithm", "")
        if not algorithm:
            try:
                algorithm = _algorithm_for_model(model)
            except ValueError:
                algorithm = "unknown"

        model_dir = self._base / _safe_name(model_id)
        model_dir.mkdir(parents=True, exist_ok=True)

        # Atomically write: temp file then rename.
        with tempfile.NamedTemporaryFile(
            dir=str(model_dir), suffix=".tmp", prefix="artifact_", delete=False
        ) as tmp:
            tmp_path = Path(tmp.name)
        try:
            model.save(str(tmp_path))
        except Exception:
            tmp_path.unlink(missing_ok=True)
            raise

        artifact_path = self._artifact_path(model_dir)
        tmp_path.rename(artifact_path)

        checksum = _sha256_path(artifact_path)
        (self._checksum_path(model_dir)).write_text(checksum, encoding="utf-8")

        self._export_history(model, self._history_path(model_dir))

        meta = ArtifactMetadata(
            model_id=model_id,
            algorithm=algorithm,
            feature_schema_version=prov.get("feature_schema_version", ""),
            feature_schema_fingerprint=prov.get("feature_schema_fingerprint", ""),
            frequency=prov.get("frequency", ""),
            horizon=int(prov.get("horizon", 0)),
            training_window=prov.get("training_window", ""),
            tenant_id=prov.get("tenant_id", ""),
            entity_type=prov.get("entity_type", ""),
            entity_id=prov.get("entity_id", ""),
            target_metric=prov.get("target_metric", ""),
            model_version=prov.get("model_version", ""),
            created_at=prov.get("created_at", ""),
            checksum=checksum,
            extra={k: v for k, v in prov.items() if k not in _PROVENANCE_KEYS and k != "algorithm"},
        )
        self._metadata_path(model_dir).write_text(
            json.dumps(meta.to_dict(), sort_keys=True, default=str),
            encoding="utf-8",
        )

        uri = f"file://{model_dir}"
        logger.info("Artifact saved: model_id={} uri={}", model_id, uri)
        return uri

    def load(self, uri: str, algorithm: str) -> Any:
        """Load and reconstruct a model from *uri*.

        Raises ``ArtifactNotFoundError`` if the artifact is absent and
        ``ArtifactIntegrityError`` if the checksum does not match.
        """
        model_dir = self._dir_for(uri)
        artifact_path = self._artifact_path(model_dir)
        if not artifact_path.exists():
            raise ArtifactNotFoundError(f"Model file not found: {artifact_path}")

        if not self.integrity_check(uri):
            raise ArtifactIntegrityError(
                f"Checksum mismatch — artifact may be corrupted or tampered: {uri}"
            )

        from trendx.forecasting import create_model

        blank = create_model(algorithm)
        model = type(blank).load(str(artifact_path))
        self._import_history(model, self._history_path(model_dir))
        logger.info("Artifact loaded: uri={} algorithm={}", uri, algorithm)
        return model

    def exists(self, uri: str) -> bool:
        try:
            model_dir = self._dir_for(uri)
            return self._artifact_path(model_dir).exists()
        except Exception:
            return False

    def integrity_check(self, uri: str) -> bool:
        """Verify that the on-disk checksum matches the actual artifact bytes."""
        try:
            model_dir = self._dir_for(uri)
            meta_path = self._metadata_path(model_dir)
            if not meta_path.exists():
                return False
            artifact_path = self._artifact_path(model_dir)
            if not artifact_path.exists():
                return False
            stored = json.loads(meta_path.read_text(encoding="utf-8")).get("checksum", "")
            if not stored:
                return False
            actual = _sha256_path(artifact_path)
            return bool(stored == actual)
        except Exception:
            return False

    def metadata(self, uri: str) -> ArtifactMetadata:
        model_dir = self._dir_for(uri)
        meta_path = self._metadata_path(model_dir)
        if not meta_path.exists():
            raise ArtifactNotFoundError(f"No metadata for artifact: {uri}")
        return ArtifactMetadata.from_dict(json.loads(meta_path.read_text(encoding="utf-8")))


# ---------------------------------------------------------------------------
# Reload: Registry ↔ Artifact linkage
# ---------------------------------------------------------------------------


def load_registered_model(
    model_id: str,
    model_lookup: Any,
    artifact_store: ModelArtifactStore,
) -> Any:
    """Resolve a model_id from the registry, verify integrity, reload.

    Parameters
    ----------
    model_id : str
        The ``model_id`` on the ``RegisteredModel`` row.
    model_lookup : Callable[[str], RegisteredModel | None]
        Injected lookup (e.g. ``ChampionRegistry.get``).
    artifact_store : ModelArtifactStore
        Store that knows how to ``load`` / ``exists`` / ``integrity_check``.

    Returns
    -------
    ForecastModel instance ready for ``predict``.

    Raises
    ------
    ArtifactError
        If the model is unknown, not READY, has no URI, the artifact is
        missing, or the checksum fails.
    """
    from trendx.forecasting.registry import ModelStatus

    model: RegisteredModel | None = model_lookup(model_id)
    if model is None:
        raise ArtifactError(f"Model not found in registry: {model_id!r}")

    if model.status is not ModelStatus.READY:
        raise ArtifactError(f"Model {model_id!r} not READY (status={model.status.value})")

    if not (model.model_uri or "").strip():
        raise ArtifactError(f"Model {model_id!r} has empty model_uri")

    if not artifact_store.exists(model.model_uri):
        raise ArtifactNotFoundError(f"Artifact missing for model {model_id!r}: {model.model_uri}")

    if not artifact_store.integrity_check(model.model_uri):
        raise ArtifactIntegrityError(f"Artifact integrity check failed for model {model_id!r}")

    return artifact_store.load(model.model_uri, model.algorithm.value)


__all__ = [
    "ArtifactError",
    "ArtifactIntegrityError",
    "ArtifactMetadata",
    "ArtifactNotFoundError",
    "FileSystemArtifactStore",
    "ModelArtifactStore",
    "NullArtifactStore",
    "load_registered_model",
]
