from __future__ import annotations

import hashlib
import json
import shutil
import subprocess  # nosec B404 -- seul usage : compute_git_hash avec argv fixe (voir nosec B603).
import tempfile
import uuid
from pathlib import Path
from typing import Any, cast

try:
    import mlflow
    from mlflow.entities import Run
    from mlflow.tracking import MlflowClient

    HAS_MLFLOW = True
except ImportError:
    HAS_MLFLOW = False
    mlflow = None
    Run = None
    MlflowClient = None
from loguru import logger
from trendx.config import settings


class MLflowTracker:
    """MLflow experiment tracking wrapper for Trendx.

    Handles experiment creation, run management, parameter/metric logging,
    model artifact storage, and Model Registry operations.
    """

    def __init__(
        self,
        tracking_uri: str | None = None,
        client: MlflowClient | None = None,
    ) -> None:
        self._tracking_uri = tracking_uri or settings.mlflow_tracking_uri
        mlflow.set_tracking_uri(self._tracking_uri)
        self._client = client or MlflowClient(self._tracking_uri)
        # NOTE (W49): the active run is the client-created Run entity, never a
        # fluent ActiveRun. All operations below use the explicit client bound
        # to this tracking URI, so behavior never depends on the mlflow global
        # fluent state (mlflow>=2.x compat; the fluent nested start in
        # log_model issued cross-store runs/get retries when the global URI
        # diverged from the client URI).
        self._active_run: Any | None = None

    @property
    def active_run(self) -> Any | None:
        return self._active_run

    @property
    def client(self) -> MlflowClient:
        return self._client

    # ── Experiment management ──────────────────────────────────────────

    def create_experiment(
        self,
        name: str,
        tags: dict[str, str] | None = None,
    ) -> str:
        existing = self._client.get_experiment_by_name(name)
        if existing is not None:
            logger.debug("Experiment '{}' already exists (id={})", name, existing.experiment_id)
            return str(existing.experiment_id)
        experiment_id = self._client.create_experiment(name, tags=tags or {})
        logger.info("Created experiment '{}' (id={})", name, experiment_id)
        return str(experiment_id)

    # ── Run management ─────────────────────────────────────────────────

    def start_run(
        self,
        experiment_name: str,
        run_name: str | None = None,
        tags: dict[str, str] | None = None,
    ) -> str:
        experiment_id = self.create_experiment(experiment_name)
        run = self._client.create_run(experiment_id, run_name=run_name, tags=tags or {})
        self._active_run = run
        logger.info(
            "Started MLflow run '{}' (id={}) in experiment '{}'",
            run_name or "unnamed",
            run.info.run_id,
            experiment_name,
        )
        return str(run.info.run_id)

    def end_run(self, status: str = "FINISHED") -> None:
        if self._active_run is not None:
            self._client.set_terminated(self._active_run.info.run_id, status=status)
            run_id = self._active_run.info.run_id
            self._active_run = None
            logger.info("Ended MLflow run {} with status '{}'", run_id, status)

    # ── Context manager ────────────────────────────────────────────────

    def __enter__(self) -> MLflowTracker:
        if self._active_run is None:
            self.start_run(experiment_name="default", run_name="default_run")
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> None:
        status = "FAILED" if exc_type is not None else "FINISHED"
        self.end_run(status=status)

    # ── Logging helpers ────────────────────────────────────────────────

    def log_params(self, params: dict[str, Any]) -> None:
        self._client.log_batch(
            self._get_run_id(),
            metrics=[],
            params=[mlflow.entities.Param(key=str(k), value=str(v)) for k, v in params.items()],
        )

    def log_metrics(
        self,
        metrics: dict[str, float],
        step: int | None = None,
    ) -> None:
        run_id = self._get_run_id()
        for key, value in metrics.items():
            self._client.log_metric(run_id, key, value, step=step)

    def log_model(
        self,
        model: Any,
        artifact_path: str,
        model_name: str | None = None,
    ) -> str:
        # mlflow>=2.x only accepts a PythonModel/callable here, which our
        # ForecastModel implementations are not (proven MlflowException), and
        # the nested fluent start issued cross-store runs/get retries when the
        # global fluent URI diverged from this client URI. Persist the fitted
        # object as a cloudpickle artifact via the explicit client instead:
        # deterministic in every environment, same runs:/ URI shape. The URI
        # stays provenance-only (inference refits from hyperparameters; no
        # consumer loads models via pyfunc/models:/).
        run_id = self._get_run_id()
        import cloudpickle

        with tempfile.NamedTemporaryFile(
            mode="wb", suffix=".pkl", delete=False, prefix="model_"
        ) as f:
            cloudpickle.dump(model, f)
            tmp_path = f.name
        try:
            self._client.log_artifact(run_id, tmp_path, artifact_path=artifact_path)
        finally:
            Path(tmp_path).unlink(missing_ok=True)
        logger.info("Logged model artifact to 'runs:/{}/{}'", run_id, artifact_path)
        model_uri = f"runs:/{run_id}/{artifact_path}"
        if model_name is not None:
            self.register_model(run_id, model_name, alias="challenger")
        return model_uri

    def log_artifact(self, local_path: str | Path) -> None:
        self._client.log_artifact(self._get_run_id(), str(local_path))

    def log_scaler(
        self,
        scaler_params: dict[str, Any],
        name: str = "scaler.json",
    ) -> None:
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".json", delete=False, prefix="scaler_"
        ) as f:
            json.dump(scaler_params, f, indent=2, default=str)
            tmp_path = f.name
        try:
            self._client.log_artifact(self._get_run_id(), tmp_path, artifact_path="scaler")
            logger.debug("Logged scaler '{}' as artifact", name)
        finally:
            Path(tmp_path).unlink(missing_ok=True)

    def log_data_version(self, version_hash: str) -> None:
        self.log_params({"data_version": version_hash})

    def log_tags(self, tags: dict[str, str]) -> None:
        # MlflowClient has no bulk set_tags in mlflow 2.x: loop singular set_tag
        # (same per-key style as log_metrics above; cf. ActiveRun compat fix).
        run_id = self._get_run_id()
        for key, value in tags.items():
            self._client.set_tag(run_id, str(key), str(value))

    # ── Search & selection ─────────────────────────────────────────────

    def search_runs(
        self,
        experiment_name: str,
        filter_string: str = "",
    ) -> list[Run]:
        experiment = self._client.get_experiment_by_name(experiment_name)
        if experiment is None:
            logger.warning("Experiment '{}' not found", experiment_name)
            return []
        return cast(
            list[Run],
            self._client.search_runs(
                experiment_ids=[experiment.experiment_id],
                filter_string=filter_string,
            ),
        )

    def get_best_run(
        self,
        experiment_name: str,
        metric: str = "sMAPE",
        mode: str = "min",
    ) -> Run | None:
        filter_string = f"metrics.{metric} < 1e9"
        runs = self.search_runs(experiment_name, filter_string)
        if not runs:
            return None
        valid = [r for r in runs if r.data.metrics.get(metric) is not None]
        if not valid:
            return None
        valid.sort(key=lambda r: r.data.metrics[metric], reverse=(mode == "max"))
        return valid[0]

    # ── Model Registry ─────────────────────────────────────────────────

    def register_model(
        self,
        run_id: str,
        model_name: str,
        alias: str = "challenger",
    ) -> str:
        result = mlflow.register_model(
            model_uri=f"runs:/{run_id}/model",
            name=model_name,
        )
        self._client.set_registered_model_alias(model_name, alias, result.version)
        logger.info(
            "Registered model '{}' version {} with alias '{}'",
            model_name,
            result.version,
            alias,
        )
        return str(result.version)

    def get_model_version(
        self,
        model_name: str,
        alias: str,
    ) -> str | None:
        try:
            mv = self._client.get_model_version_by_alias(model_name, alias)
            return str(mv.version)
        except Exception:
            return None

    def transition_model_version(
        self,
        model_name: str,
        version: str,
        stage: str,
    ) -> None:
        self._client.transition_model_version_stage(
            name=model_name,
            version=version,
            stage=stage,
        )
        logger.info("Transitioned model '{}' v{} to stage '{}'", model_name, version, stage)

    # ── Helpers ────────────────────────────────────────────────────────

    def _get_run_id(self) -> str:
        if self._active_run is not None:
            return str(self._active_run.info.run_id)
        active = mlflow.active_run()
        if active is not None:
            return str(active.info.run_id)
        msg = "No active MLflow run. Call start_run() first or use context manager."
        raise RuntimeError(msg)

    @staticmethod
    def compute_git_hash() -> str:
        git_bin = shutil.which("git")
        if git_bin is None:
            return "unknown"
        try:
            result = subprocess.run(  # nosec B603 -- argv fixe ["git", "rev-parse", "HEAD"], shell=False, timeout borné, aucune entrée utilisateur.
                [git_bin, "rev-parse", "HEAD"],
                capture_output=True,
                text=True,
                timeout=10,
            )
            if result.returncode == 0:
                return result.stdout.strip()
        except Exception as exc:
            logger.debug("Hash git indisponible (ignoré) : {}", exc)
        return "unknown"

    @staticmethod
    def compute_data_hash(df: Any) -> str:
        try:
            import pandas as pd

            if isinstance(df, pd.DataFrame):
                raw = pd.util.hash_pandas_object(df).values
                return hashlib.sha256(raw.tobytes()).hexdigest()[:16]
        except Exception as exc:
            logger.debug("Hash dataframe indisponible, UUID de repli : {}", exc)
        return str(uuid.uuid4())[:16]
