from __future__ import annotations

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from trendx.services.training import TrainingService


@pytest.fixture
def training_service():
    svc = TrainingService(
        mlflow_tracker=MagicMock(),
        model_registry=MagicMock(),
        data_quality=MagicMock(),
        resampler=MagicMock(),
        model_selector=MagicMock(),
    )
    svc._tracker.start_run.return_value = "run-x"
    svc._tracker.log_model.return_value = "runs:/run-x/model"
    svc._tracker.end_run.return_value = None
    svc._resampler.resample.return_value = None
    svc._resampler.resample.side_effect = lambda df, *a, **k: df
    return svc


def _make_df() -> pd.DataFrame:
    n = 12
    return pd.DataFrame(
        {
            "ds": pd.date_range("2024-01-01", periods=n, freq="1h"),
            "y": list(range(n)),
            "linf": list(range(n)),
            "lsup": list(range(n)),
        }
    )


# Kwarg WIP (interface future non supportée par le schéma Trendz 1.15.0) qui
# doivent être ABSENTS de l'appel à register().
_WIP_KWARGS = {
    "entity_id",
    "metric_key",
    "algorithm",
    "metrics",
    "hyperparameters",
    "scaler",
    "frequency",
    "horizon",
    "lookback_days",
    "data_version",
    "train_period_start",
    "train_period_end",
    "code_version",
    "mlflow_run_id",
}


@patch("trendx.services.training.Normalizer")
@patch("trendx.services.training.compute_metrics")
@patch("trendx.services.training.ForecastMetrics")
@patch("trendx.services.training.create_model")
@patch.object(TrainingService, "_prepare_data", lambda self, df, *a, **k: df)
@patch.object(TrainingService, "_fetch_training_data")
def test_train_model_registers_with_current_contract(
    mock_fetch,
    mock_create_model,
    mock_forecast_metrics,
    mock_compute_metrics,
    mock_normalizer,
    training_service: TrainingService,
):
    df = _make_df()
    mock_fetch.return_value = df

    fake_model = MagicMock()
    fake_forecast = MagicMock()
    fake_forecast.values = [1.0, 2.0, 3.0, 4.0]
    fake_forecast.lower_bound = [1.0, 2.0, 3.0, 4.0]
    fake_forecast.upper_bound = [1.0, 2.0, 3.0, 4.0]
    fake_model.fit.return_value = None
    fake_model.predict.return_value = fake_forecast
    mock_create_model.return_value = fake_model

    metrics = MagicMock()
    metrics.to_dict.return_value = {"mae": 0.1}
    mock_forecast_metrics.return_value = metrics
    mock_compute_metrics.return_value = metrics

    normalizer = MagicMock()
    normalizer.method = "auto"
    normalizer.get_params.return_value = {}
    mock_normalizer.return_value = normalizer

    champion = MagicMock()
    training_service._registry.register.return_value = champion

    result = training_service.train_model(
        entity_id="dev-001",
        metric_key="temperature",
        algorithm="Prophet",
        frequency="1h",
        horizon=4,
        customer_id="11111111-1111-1111-1111-111111111111",
    )

    # register() appelé exactement une fois.
    training_service._registry.register.assert_called_once()
    call_kwargs = training_service._registry.register.call_args.kwargs

    # Contrat courant (schéma/ORM réel) + customer explicite (OPTION A).
    assert call_kwargs == {
        "business_entity_id": "dev-001",
        "tb_telemetry_key": "temperature",
        "model_type": "Prophet",
        "model_uri": "runs:/run-x/model",
        "customer_id": "11111111-1111-1111-1111-111111111111",
    }
    # Aucun kwarg WIP ne doit fuiter vers register().
    assert _WIP_KWARGS.isdisjoint(call_kwargs.keys())

    # Le retour de register() est propagé.
    assert result is champion
