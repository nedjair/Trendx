from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from trendx.mlops.registry import ModelRegistry


@pytest.fixture
def registry():
    return ModelRegistry(repository=MagicMock())


@pytest.mark.unit
def test_register_model(registry):
    mock_session = MagicMock()
    mock_repo = MagicMock()
    mock_model = MagicMock()
    mock_model.id = 1
    mock_repo.create.return_value = mock_model

    with (
        patch("trendx.mlops.registry.next", return_value=mock_session),
        patch("trendx.mlops.registry.PredictionModelRepository", return_value=mock_repo),
    ):
        model = registry.register(
            entity_id="dev-001",
            metric_key="temperature",
            algorithm="Prophet",
            model_uri="runs:/run-123/model",
            metrics={"train": {"mae": 0.5}, "test": {"mae": 0.7}},
            hyperparameters={"seasonality": True},
        )

    assert model.id == 1
    mock_repo.create.assert_called_once()


@pytest.mark.unit
def test_get_champion(registry):
    mock_session = MagicMock()
    mock_repo = MagicMock()
    mock_champion = MagicMock()
    mock_champion.id = 1
    mock_champion.algorithm = "Prophet"
    mock_champion.status = "champion"
    mock_champion.tb_telemetry_key = "temperature"
    mock_champion.created_ts = 1
    mock_repo.find_by_business_entity.return_value = [mock_champion]

    with (
        patch("trendx.mlops.registry.next", return_value=mock_session),
        patch("trendx.mlops.registry.PredictionModelRepository", return_value=mock_repo),
    ):
        champion = registry.get_champion("dev-001", "temperature")

    assert champion is not None
    assert champion.id == 1


@pytest.mark.unit
def test_get_champion_none(registry):
    mock_session = MagicMock()
    mock_repo = MagicMock()
    mock_repo.find_by_business_entity.return_value = []

    with (
        patch("trendx.mlops.registry.next", return_value=mock_session),
        patch("trendx.mlops.registry.PredictionModelRepository", return_value=mock_repo),
    ):
        champion = registry.get_champion("dev-001", "temperature")

    assert champion is None


@pytest.mark.unit
def test_promote_champion(registry):
    mock_session = MagicMock()
    mock_repo = MagicMock()
    mock_model = MagicMock()
    mock_model.id = 1
    mock_model.status = "champion"
    mock_model.tb_telemetry_key = "temperature"
    mock_repo.find_by_business_entity.return_value = []
    mock_repo.set_champion.return_value = mock_model

    with (
        patch("trendx.mlops.registry.next", return_value=mock_session),
        patch("trendx.mlops.registry.PredictionModelRepository", return_value=mock_repo),
    ):
        result = registry.promote_to_champion("dev-001", "temperature", mock_model)

    assert result is not None
    assert result.status == "champion"


@pytest.mark.unit
def test_rollback(registry):
    mock_session = MagicMock()
    mock_repo = MagicMock()
    current_champion = MagicMock()
    current_champion.id = 2
    current_champion.status = "champion"
    previous_champion = MagicMock()
    previous_champion.id = 1
    previous_champion.status = "challenger"
    mock_repo.find_champion.return_value = current_champion
    mock_repo.find_previous_champion.return_value = previous_champion

    with (
        patch("trendx.mlops.registry.next", return_value=mock_session),
        patch("trendx.mlops.registry.PredictionModelRepository", return_value=mock_repo),
    ):
        result = registry.rollback("dev-001", "temperature")

    assert result is not None
    assert result.id == 1
    assert result.status == "champion"


@pytest.mark.unit
def test_rollback_no_champion(registry):
    mock_session = MagicMock()
    mock_repo = MagicMock()
    mock_repo.find_champion.return_value = None

    with (
        patch("trendx.mlops.registry.next", return_value=mock_session),
        patch("trendx.mlops.registry.PredictionModelRepository", return_value=mock_repo),
    ):
        result = registry.rollback("dev-001", "temperature")

    assert result is None


@pytest.mark.unit
def test_rollback_no_previous(registry):
    mock_session = MagicMock()
    mock_repo = MagicMock()
    current = MagicMock()
    current.id = 2
    current.status = "champion"
    mock_repo.find_champion.return_value = current
    mock_repo.find_previous_champion.return_value = None

    with (
        patch("trendx.mlops.registry.next", return_value=mock_session),
        patch("trendx.mlops.registry.PredictionModelRepository", return_value=mock_repo),
    ):
        result = registry.rollback("dev-001", "temperature")

    assert result is None


@pytest.mark.unit
def test_compare_models(registry):
    mock_session = MagicMock()
    mock_repo = MagicMock()
    champ = MagicMock()
    champ.id = 1
    champ.algorithm = "Prophet"
    champ.status = "champion"
    champ.tb_telemetry_key = "temperature"
    champ.created_ts = 1
    mock_repo.find_by_business_entity.return_value = [champ]

    with (
        patch("trendx.mlops.registry.next", return_value=mock_session),
        patch("trendx.mlops.registry.PredictionModelRepository", return_value=mock_repo),
    ):
        comparison = registry.compare_models("dev-001", "temperature")

    assert comparison["champion"] is not None
    assert comparison["champion"]["algorithm"] == "Prophet"
    assert comparison["challenger"] is None


@pytest.mark.unit
def test_store_selection_run(registry):
    mock_session = MagicMock()
    mock_session.add = MagicMock()
    mock_session.flush = MagicMock()

    with patch("trendx.mlops.registry.next", return_value=mock_session):
        run_id = registry.store_selection_run(
            entity_id="dev-001",
            metric_key="temperature",
            candidates=[{"algorithm": "Prophet", "smape": 5.0}],
            champion_id="1",
            selection_metric="sMAPE",
            champion_score=5.0,
            margin_gain=0.1,
            status="completed",
        )

    assert run_id is not None


@pytest.mark.unit
def test_get_challenger(registry):
    mock_session = MagicMock()
    mock_repo = MagicMock()
    challenger = MagicMock()
    challenger.id = 3
    challenger.status = "challenger"
    challenger.tb_telemetry_key = "temperature"
    challenger.created_ts = 1
    mock_repo.find_by_business_entity.return_value = [challenger]

    with (
        patch("trendx.mlops.registry.next", return_value=mock_session),
        patch("trendx.mlops.registry.PredictionModelRepository", return_value=mock_repo),
    ):
        result = registry.get_challenger("dev-001", "temperature")

    assert result is not None
    assert result.id == 3


@pytest.mark.unit
def test_get_challenger_none(registry):
    mock_session = MagicMock()
    mock_repo = MagicMock()
    mock_repo.find_by_business_entity.return_value = []

    with (
        patch("trendx.mlops.registry.next", return_value=mock_session),
        patch("trendx.mlops.registry.PredictionModelRepository", return_value=mock_repo),
    ):
        result = registry.get_challenger("dev-001", "temperature")

    assert result is None


@pytest.mark.unit
def test_list_versions(registry):
    mock_session = MagicMock()
    mock_repo = MagicMock()
    champ = MagicMock()
    champ.id = 1
    champ.status = "champion"
    champ.tb_telemetry_key = "temperature"
    champ.created_ts = 1
    mock_repo.find_by_business_entity.return_value = [champ]

    with (
        patch("trendx.mlops.registry.next", return_value=mock_session),
        patch("trendx.mlops.registry.PredictionModelRepository", return_value=mock_repo),
    ):
        versions = registry.list_versions("dev-001", "temperature")

    assert len(versions) == 1
