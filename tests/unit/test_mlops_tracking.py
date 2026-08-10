from __future__ import annotations

from unittest.mock import MagicMock, patch

import mlflow
import pytest
from mlflow.entities import Experiment, Metric, Run, RunData, RunInfo
from trendx.mlops.tracking import MLflowTracker


@pytest.fixture
def tracker():
    with patch("trendx.mlops.tracking.mlflow.set_tracking_uri"):
        tr = MLflowTracker(
            tracking_uri="http://test-mlflow:5000",
            client=MagicMock(),
        )
        yield tr
        # Ensure no leaked MLflow active run state between tests
        try:
            mlflow.end_run()
        except Exception:
            pass


@pytest.mark.unit
def test_create_experiment(tracker):
    mock_exp = MagicMock(spec=Experiment)
    mock_exp.experiment_id = "exp-123"
    tracker._client.get_experiment_by_name.return_value = None
    tracker._client.create_experiment.return_value = "exp-123"

    exp_id = tracker.create_experiment("test-experiment")
    assert exp_id == "exp-123"
    tracker._client.create_experiment.assert_called_once()


@pytest.mark.unit
def test_create_experiment_exists(tracker):
    mock_exp = MagicMock(spec=Experiment)
    mock_exp.experiment_id = "exp-123"
    tracker._client.get_experiment_by_name.return_value = mock_exp

    exp_id = tracker.create_experiment("existing-experiment")
    assert exp_id == "exp-123"
    tracker._client.create_experiment.assert_not_called()


@pytest.mark.unit
def test_start_end_run(tracker):
    mock_run = MagicMock(spec=Run)
    mock_run.info.run_id = "run-123"
    tracker._client.create_run.return_value = mock_run

    with patch("trendx.mlops.tracking.mlflow.start_run", return_value=mock_run):
        run_id = tracker.start_run("test-exp", run_name="test-run")

    assert run_id == "run-123"
    assert tracker.active_run is mock_run

    tracker.end_run(status="FINISHED")
    tracker._client.set_terminated.assert_called_with("run-123", status="FINISHED")
    assert tracker.active_run is None


@pytest.mark.unit
def test_log_params_metrics(tracker):
    mock_run = MagicMock(spec=Run)
    mock_run.info.run_id = "run-123"
    tracker._client.create_run.return_value = mock_run

    with patch("trendx.mlops.tracking.mlflow.start_run", return_value=mock_run):
        tracker.start_run("test-exp")

    tracker.log_params({"alpha": 0.5, "beta": 0.3})
    tracker._client.log_batch.assert_called_once()

    tracker.log_metrics({"mae": 0.5, "rmse": 0.7})
    assert tracker._client.log_metric.call_count == 2


@pytest.mark.unit
def test_log_model(tracker):
    mock_run = MagicMock(spec=Run)
    mock_run.info.run_id = "run-123"
    tracker._client.create_run.return_value = mock_run

    with patch("trendx.mlops.tracking.mlflow.start_run", return_value=mock_run):
        tracker.start_run("test-exp")

    cm = MagicMock()
    cm.__enter__.return_value = mock_run
    with (
        patch("trendx.mlops.tracking.mlflow.start_run", return_value=cm),
        patch("mlflow.pyfunc.log_model"),
        patch("mlflow.register_model"),
    ):
        model_uri = tracker.log_model(MagicMock(), "test-model", model_name="test-model-name")

    assert "runs:/run-123/test-model" in model_uri


@pytest.mark.unit
def test_search_runs(tracker):
    mock_exp = MagicMock(spec=Experiment)
    mock_exp.experiment_id = "exp-123"
    tracker._client.get_experiment_by_name.return_value = mock_exp
    tracker._client.search_runs.return_value = []

    runs = tracker.search_runs("test-exp")
    assert runs == []


@pytest.mark.unit
def test_search_runs_not_found(tracker):
    tracker._client.get_experiment_by_name.return_value = None
    runs = tracker.search_runs("nonexistent")
    assert runs == []


@pytest.mark.unit
def test_get_best_run(tracker):
    mock_exp = MagicMock(spec=Experiment)
    mock_exp.experiment_id = "exp-123"
    tracker._client.get_experiment_by_name.return_value = mock_exp

    run1_data = RunData(metrics=[Metric("sMAPE", 5.0, 0, 0)], params=[], tags=[])
    run2_data = RunData(metrics=[Metric("sMAPE", 3.0, 0, 0)], params=[], tags=[])
    run1 = Run(
        RunInfo("run-1", "exp-123", "user", "FINISHED", 1700000000, None, "active", "db"),
        run1_data,
    )
    run2 = Run(
        RunInfo("run-2", "exp-123", "user", "FINISHED", 1700000000, None, "active", "db"),
        run2_data,
    )
    tracker._client.search_runs.return_value = [run1, run2]

    best = tracker.get_best_run("test-exp", metric="sMAPE", mode="min")
    assert best is not None
    assert best.info.run_id == "run-2"


@pytest.mark.unit
def test_get_best_run_no_metric(tracker):
    mock_exp = MagicMock(spec=Experiment)
    mock_exp.experiment_id = "exp-123"
    tracker._client.get_experiment_by_name.return_value = mock_exp
    run_data = RunData(metrics=[], params=[], tags=[])
    run = Run(
        RunInfo("run-1", "exp-123", "user", "FINISHED", 1700000000, None, "active", "db"),
        run_data,
    )
    tracker._client.search_runs.return_value = [run]

    best = tracker.get_best_run("test-exp", metric="sMAPE")
    assert best is None


@pytest.mark.unit
def test_context_manager(tracker):
    mock_run = MagicMock(spec=Run)
    mock_run.info.run_id = "run-ctx"
    tracker._client.create_run.return_value = mock_run

    with patch("trendx.mlops.tracking.mlflow.start_run", return_value=mock_run):
        with tracker as tr:
            assert tr.active_run is not None

    tracker._client.set_terminated.assert_called_once()


@pytest.mark.unit
def test_register_model(tracker):
    with patch("trendx.mlops.tracking.mlflow.register_model") as mock_reg:
        mock_version = MagicMock()
        mock_version.version = "v1"
        mock_reg.return_value = mock_version

        version = tracker.register_model("run-123", "test-model", alias="challenger")

    assert version == "v1"
    tracker._client.set_registered_model_alias.assert_called_with("test-model", "challenger", "v1")


@pytest.mark.unit
def test_get_model_version(tracker):
    mock_mv = MagicMock()
    mock_mv.version = "v2"
    tracker._client.get_model_version_by_alias.return_value = mock_mv

    version = tracker.get_model_version("test-model", "champion")
    assert version == "v2"


@pytest.mark.unit
def test_get_model_version_not_found(tracker):
    tracker._client.get_model_version_by_alias.side_effect = Exception("Not found")

    version = tracker.get_model_version("test-model", "champion")
    assert version is None


@pytest.mark.unit
def test_log_artifact(tracker):
    mock_run = MagicMock(spec=Run)
    mock_run.info.run_id = "run-123"
    tracker._client.create_run.return_value = mock_run

    with patch("trendx.mlops.tracking.mlflow.start_run", return_value=mock_run):
        tracker.start_run("test-exp")

    tracker.log_artifact("/tmp/test.txt")
    tracker._client.log_artifact.assert_called_once()


@pytest.mark.unit
def test_no_active_run_raises(tracker):
    with pytest.raises(RuntimeError, match="No active MLflow run"):
        tracker._get_run_id()
