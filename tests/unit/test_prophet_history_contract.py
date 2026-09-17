"""Contrat d'historique Prophet pour forecast_pyfunc.export_history.

ProphetModel conservait l'historique dans _last_df (écrit jamais lu) alors
que l'export MLflow exige _train_df (contrat linear/OLS/ARIMA/Fourier).
Le fit aligne désormais Prophet sur _train_df.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pandas as pd
from trendx.forecasting.fourier import FourierModel
from trendx.forecasting.linear import LinearRegressionModel
from trendx.forecasting.prophet import ProphetModel
from trendx.mlops.forecast_pyfunc import export_history, import_history


def _frame(n: int = 12) -> pd.DataFrame:
    base = datetime(2026, 9, 16, 22, 0)
    return pd.DataFrame(
        {
            "ds": [base + timedelta(hours=i) for i in range(n)],
            "y": [22.0 + 0.1 * i for i in range(n)],
        }
    )


def _fitted_prophet() -> ProphetModel:
    model = ProphetModel(
        yearly_seasonality=False, weekly_seasonality=False, daily_seasonality=False
    )
    model.fit(_frame())
    return model


def test_a_train_df_exists_after_fit() -> None:
    model = _fitted_prophet()
    assert model._train_df is not None
    assert len(model._train_df) == 12


def test_b_history_rows_timestamps_values_order() -> None:
    model = _fitted_prophet()
    frame = model._train_df
    assert frame is not None
    assert list(frame["y"]) == [22.0 + 0.1 * i for i in range(12)]
    assert frame["ds"].iloc[0] == pd.Timestamp("2026-09-16 22:00")
    assert frame["ds"].iloc[-1] == pd.Timestamp("2026-09-17 09:00")
    assert (frame["ds"].diff().dropna() == pd.Timedelta(hours=1)).all()


def test_c_export_history_no_longer_raises(tmp_path) -> None:  # type: ignore[no-untyped-def]
    model = _fitted_prophet()
    target = tmp_path / "history.csv"
    export_history(model, str(target))
    reloaded = import_history(str(target))
    assert len(reloaded) == 12
    assert list(reloaded.columns) == ["ds", "y"]


def test_d_existing_models_still_functional() -> None:
    linear = LinearRegressionModel()
    linear.fit(_frame())
    assert linear._train_df is not None and len(linear._train_df) == 12
    fourier = FourierModel()
    fourier.fit(_frame())
    assert fourier._train_df is not None and len(fourier._train_df) == 12


def test_e_local_fit_predict_export() -> None:
    model = _fitted_prophet()
    forecast = model.predict(horizon=2)
    assert len(forecast.values) == 2
    assert model._train_df is not None and len(model._train_df) == 12


def test_f_service_chain_fit_predict_export(monkeypatch) -> None:
    """Chaîne service -> Prophet -> fit -> predict -> export (tracker/registre mockés).

    N'utilise ni DB, ni ThingsBoard, ni MLflow production, ni prediction_model.
    """
    from unittest.mock import MagicMock

    from trendx.services import training as training_mod
    from trendx.services.training import TrainingService

    base = datetime(2026, 9, 16, 22, 0, tzinfo=UTC)
    rows = [(base + timedelta(hours=i), 22.0 + 0.1 * i) for i in range(12)]

    class _FakeResult:
        def fetchall(self) -> list[tuple]:
            return rows

    class _FakeConn:
        def execute(self, stmt: object, params: dict) -> _FakeResult:
            return _FakeResult()

    from contextlib import contextmanager

    class _Engine:
        @contextmanager
        def connect(self):  # type: ignore[no-untyped-def]
            yield _FakeConn()

    monkeypatch.setattr(training_mod.db_manager, "get_engine", lambda _name: _Engine())
    tracker = MagicMock()
    tracker.log_model.return_value = "runs:/fake/model"
    registry = MagicMock()
    champion = MagicMock()
    registry.register.return_value = champion
    svc = TrainingService(
        mlflow_tracker=tracker,
        model_registry=registry,
        data_quality=__import__(
            "trendx.preprocessing.quality", fromlist=["DataQualityService"]
        ).DataQualityService(),
        resampler=__import__("trendx.preprocessing.resampling", fromlist=["Resampler"]).Resampler(),
        model_selector=MagicMock(),
    )
    result = svc.train_model(
        entity_id="dev-001",
        metric_key="temperature",
        algorithm="Prophet",
        params={
            "yearly_seasonality": False,
            "weekly_seasonality": False,
            "daily_seasonality": False,
        },
        frequency="1h",
        horizon=2,
        customer_id="11111111-1111-1111-1111-111111111111",
    )
    assert result is champion
    registry.register.assert_called_once()
