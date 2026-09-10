"""Wrapper MLflow pyfunc pour les ForecastModel Trendx (MLflow 2.14.3).

Contexte : ``mlflow.pyfunc.log_model(python_model=...)`` exige un objet
conforme au contrat ``PythonModel`` (``predict(context, model_input)``).
Les ``ForecastModel`` Trendx (Protocol fit/predict/save/load) ne le sont pas.

Ce wrapper :
- dérive de ``mlflow.pyfunc.PythonModel`` ;
- transporte uniquement ``algorithm`` + ``horizon`` par défaut (picklables) ;
- persiste l'état ajusté via le couple ``save()``/``load()`` propre à chaque
  implémentation (artefact ``forecast_model``), jamais de cloudpickle du
  moteur (Stan & co exclus par construction) ;
- recharge dans ``load_context`` et délègue ``predict`` au ForecastModel ;
- ne touche ni métriques, ni champion, ni normalisation, ni tags.
"""

from __future__ import annotations

from typing import Any

import mlflow.pyfunc
import numpy as np
import pandas as pd

ARTIFACT_NAME = "forecast_model"
HISTORY_NAME = "train_history_csv"
DEFAULT_HORIZON = 24
_HISTORY_COLUMNS = ("ds", "y")


def export_history(model: Any, path: str) -> None:
    """Persiste l'historique d'entraînement (ds/y) pour une re-prédiction.

    Couplage documenté : tous les moteurs actuels conservent ``_train_df``
    (vérifié : linear/OLS/ARIMA/Fourier) et l'exigent dans ``predict``,
    mais ne le sérialisent pas dans ``save()``. Le wrapper en est
    propriétaire pour ne modifier aucun moteur (périmètre MLflow seul).
    """
    frame = getattr(model, "_train_df", None)
    if frame is None or len(frame) == 0:
        raise ValueError("Modèle non ajusté : historique _train_df absent ou vide")
    slim = pd.DataFrame(
        {"ds": pd.to_datetime(frame["ds"]), "y": np.asarray(frame["y"], dtype=float)}
    )
    slim.to_csv(path, index=False)


def import_history(path: str) -> pd.DataFrame:
    frame = pd.read_csv(path, parse_dates=["ds"])
    return pd.DataFrame(
        {"ds": pd.to_datetime(frame["ds"]), "y": np.asarray(frame["y"], dtype=float)}
    )


def _single_file(path: str) -> str:
    import os

    if not os.path.isdir(path):
        return path
    entries = sorted(os.listdir(path))
    if len(entries) != 1:
        raise ValueError(f"Artefact ambigu ({len(entries)} fichiers) : {path}")
    return os.path.join(path, entries[0])


class ForecastPyfuncWrapper(mlflow.pyfunc.PythonModel):  # type: ignore[misc]
    """Adaptateur pyfunc autour d'un ForecastModel ajusté."""

    def __init__(self, algorithm: str = "", horizon: int = DEFAULT_HORIZON) -> None:
        self.algorithm = algorithm
        self.horizon = int(horizon)
        self._model: Any = None

    def load_context(self, context: Any) -> None:
        from trendx.forecasting import create_model

        if not self.algorithm:
            raise ValueError("ForecastPyfuncWrapper: algorithm manquant (artefact incomplet)")
        model_path = _single_file(context.artifacts[ARTIFACT_NAME])
        blank = create_model(self.algorithm)
        self._model = type(blank).load(model_path)
        history_path = _single_file(context.artifacts[HISTORY_NAME])
        # Couplage documenté (voir export_history) : restauration de l'état
        # d'entraînement que les moteurs exigent dans predict().
        self._model._train_df = import_history(history_path)  # type: ignore[attr-defined]

    def _resolve_horizon(self, model_input: Any, params: dict[str, Any] | None) -> int:
        if params and params.get("horizon") is not None:
            return int(params["horizon"])
        if isinstance(model_input, pd.DataFrame) and "horizon" in model_input.columns:
            return int(model_input["horizon"].iloc[0])
        return self.horizon

    def predict(
        self,
        context: Any,
        model_input: Any,
        params: dict[str, Any] | None = None,
    ) -> pd.DataFrame:
        if self._model is None:
            raise RuntimeError("Wrapper non chargé : load_context requis avant predict")
        horizon = self._resolve_horizon(model_input, params)
        result = self._model.predict(horizon)
        frame = pd.DataFrame(
            {
                "values": list(result.values),
                "lower_bound": list(result.lower_bound),
                "upper_bound": list(result.upper_bound),
                "horizon_step": list(range(1, len(result.values) + 1)),
            }
        )
        if getattr(result, "timestamps", None) is not None:
            frame["ts"] = [str(ts) for ts in result.timestamps]
        return frame
