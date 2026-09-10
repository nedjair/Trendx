"""Round-trip MLflow pyfunc du wrapper ForecastModel (MLflow 2.14.3 réel).

Prouve sur backend filesystem jetable :
- log_model accepte un ForecastModel (ancien crash : python_model rejeté) ;
- model_uri exploitable, modèle rechargeable sans l'objet d'origine ;
- prédictions rechargées identiques (tolérance explicite) ;
- type inconnu = erreur explicite (fail-closed).
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest
from numpy.testing import assert_allclose

pytestmark = [pytest.mark.unit]

HORIZON = 6
RTOL = 1e-9


def _fitted_linear():
    from trendx.forecasting.linear import LinearRegressionModel

    stamps = pd.date_range("2026-09-01", periods=72, freq="1h")
    df = pd.DataFrame(
        {"ds": stamps, "y": 20.0 + 0.05 * np.arange(72) + np.sin(np.arange(72) / 6.0)}
    )
    model = LinearRegressionModel()
    model.fit(df)
    return model


def test_roundtrip_values_dims_timestamps(tmp_path) -> None:
    import mlflow
    from trendx.mlops.tracking import MLflowTracker

    model = _fitted_linear()
    tracker = MLflowTracker(tracking_uri=f"file://{tmp_path}/mlruns")
    tracker.start_run(experiment_name="b1-pyfunc", run_name="roundtrip")
    uri = tracker.log_model(model, artifact_path="model")
    tracker.end_run("FINISHED")
    assert uri.startswith("runs:/"), uri

    del model  # plus aucune dépendance à l'objet d'origine
    loaded = mlflow.pyfunc.load_model(uri)
    frame = loaded.predict(pd.DataFrame({"horizon": [HORIZON]}))
    assert list(frame.columns)[:4] == ["values", "lower_bound", "upper_bound", "horizon_step"]
    assert len(frame) == HORIZON
    assert list(frame["horizon_step"]) == [1, 2, 3, 4, 5, 6]
    assert np.all(np.isfinite(frame["values"].to_numpy(dtype=float)))


def test_roundtrip_matches_original_within_tolerance(tmp_path) -> None:
    import mlflow
    from trendx.mlops.tracking import MLflowTracker

    model = _fitted_linear()
    expected = model.predict(HORIZON)
    tracker = MLflowTracker(tracking_uri=f"file://{tmp_path}/mlruns")
    tracker.start_run(experiment_name="b1-pyfunc-eq")
    uri = tracker.log_model(model, artifact_path="model")
    tracker.end_run("FINISHED")
    reloaded = mlflow.pyfunc.load_model(uri).predict(pd.DataFrame({"horizon": [HORIZON]}))
    assert_allclose(
        reloaded["values"].to_numpy(dtype=float),
        np.asarray(expected.values, dtype=float),
        rtol=RTOL,
        atol=0.0,
    )
    assert_allclose(
        reloaded["lower_bound"].to_numpy(dtype=float),
        np.asarray(expected.lower_bound, dtype=float),
        rtol=RTOL,
        atol=0.0,
    )
    # Déterminisme : deux chargements -> mêmes valeurs.
    again = mlflow.pyfunc.load_model(uri).predict(pd.DataFrame({"horizon": [HORIZON]}))
    assert_allclose(
        again["values"].to_numpy(dtype=float),
        reloaded["values"].to_numpy(dtype=float),
        rtol=0.0,
        atol=0.0,
    )


def test_unknown_model_type_fails_closed(tmp_path) -> None:
    from trendx.mlops.tracking import MLflowTracker

    class NotRegistered:
        def save(self, path: str) -> None:
            raise AssertionError("ne doit jamais être appelé")

    tracker = MLflowTracker(tracking_uri=f"file://{tmp_path}/mlruns")
    tracker.start_run(experiment_name="b1-pyfunc-bad")
    with pytest.raises(ValueError, match="MODEL_REGISTRY"):
        tracker.log_model(NotRegistered(), artifact_path="model")
    tracker.end_run("FAILED")
