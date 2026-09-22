from __future__ import annotations

from unittest.mock import MagicMock, patch
from uuid import UUID, uuid4

import pytest
from sqlalchemy.exc import ProgrammingError
from trendx.database.models import PredictionModel
from trendx.database.repositories import PredictionModelRepository
from trendx.mlops.registry import ModelRegistry


@pytest.fixture
def registry():
    return ModelRegistry(repository=MagicMock())


def _session_cm(session: MagicMock) -> MagicMock:
    """Context-manager mock whose ``__enter__`` yields ``session``.

    Replaces the previous ``patch("trendx.mlops.registry.next", ...)`` hack,
    which patched the builtin ``next`` and silently accepted any argument —
    masking the misuse of ``get_session`` (a ``@contextmanager``) as an
    iterator. With this helper, ``with db_manager.get_session(...) as session``
    exercises the real control flow and can no longer hide the bug.
    """
    cm = MagicMock()
    cm.__enter__.return_value = session
    cm.__exit__.return_value = False
    return cm


@pytest.mark.unit
@pytest.mark.xfail(
    reason="teste la signature register()/compare_models() du WIP non stagé (entity_id, algorithm) — attend le future commit sur feat/detector-scoring-and-forecasting-fixes"
)
def test_register_model(registry):
    mock_session = MagicMock()
    mock_repo = MagicMock()
    mock_model = MagicMock()
    mock_model.id = 1
    mock_repo.create.return_value = mock_model

    with (
        patch(
            "trendx.mlops.registry.db_manager.get_session", return_value=_session_cm(mock_session)
        ),
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
def test_register_persists_model_uri(registry):
    mock_session = MagicMock()
    mock_repo = MagicMock()
    mock_model = MagicMock()
    mock_model.id = 1
    mock_repo.create.return_value = mock_model
    ent = str(uuid4())

    with (
        patch(
            "trendx.mlops.registry.db_manager.get_session",
            return_value=_session_cm(mock_session),
        ),
        patch("trendx.mlops.registry.PredictionModelRepository", return_value=mock_repo),
    ):
        registry.register(
            business_entity_id=ent,
            tb_telemetry_key="temperature",
            model_type="Prophet",
            model_uri="runs:/run-123/model",
            tenant_id=str(uuid4()),
            customer_id=str(uuid4()),
            promote=False,
        )

    mock_repo.create.assert_called_once()
    # model_uri must reach the persistence layer (was silently dropped before).
    assert mock_repo.create.call_args.kwargs.get("model_uri") == "runs:/run-123/model"
    # UUID contract: no empty string may reach PostgreSQL.
    for key, value in mock_repo.create.call_args.kwargs.items():
        if "entity" in key or "tenant" in key:
            UUID(str(value))


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
        patch(
            "trendx.mlops.registry.db_manager.get_session", return_value=_session_cm(mock_session)
        ),
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
        patch(
            "trendx.mlops.registry.db_manager.get_session", return_value=_session_cm(mock_session)
        ),
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
        patch(
            "trendx.mlops.registry.db_manager.get_session", return_value=_session_cm(mock_session)
        ),
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
        patch(
            "trendx.mlops.registry.db_manager.get_session", return_value=_session_cm(mock_session)
        ),
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
        patch(
            "trendx.mlops.registry.db_manager.get_session", return_value=_session_cm(mock_session)
        ),
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
        patch(
            "trendx.mlops.registry.db_manager.get_session", return_value=_session_cm(mock_session)
        ),
        patch("trendx.mlops.registry.PredictionModelRepository", return_value=mock_repo),
    ):
        result = registry.rollback("dev-001", "temperature")

    assert result is None


@pytest.mark.unit
@pytest.mark.xfail(
    reason="teste la signature register()/compare_models() du WIP non stagé (entity_id, algorithm) — attend le future commit sur feat/detector-scoring-and-forecasting-fixes"
)
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
        patch(
            "trendx.mlops.registry.db_manager.get_session", return_value=_session_cm(mock_session)
        ),
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

    with patch(
        "trendx.mlops.registry.db_manager.get_session", return_value=_session_cm(mock_session)
    ):
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
        patch(
            "trendx.mlops.registry.db_manager.get_session", return_value=_session_cm(mock_session)
        ),
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
        patch(
            "trendx.mlops.registry.db_manager.get_session", return_value=_session_cm(mock_session)
        ),
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
        patch(
            "trendx.mlops.registry.db_manager.get_session", return_value=_session_cm(mock_session)
        ),
        patch("trendx.mlops.registry.PredictionModelRepository", return_value=mock_repo),
    ):
        versions = registry.list_versions("dev-001", "temperature")

    assert len(versions) == 1


@pytest.mark.unit
def test_find_previous_champion_returns_none_when_history_table_is_missing():
    session = MagicMock()
    missing_table = MagicMock()
    missing_table.pgcode = "42P01"
    session.scalars.side_effect = ProgrammingError("SELECT", {}, missing_table)
    repo = PredictionModelRepository(session)

    result = repo.find_previous_champion("dev-001", "temperature", 2)

    assert result is None
    session.scalars.assert_called_once()


@pytest.mark.unit
def test_rollback_refuses_previous_model_with_unexpected_status(registry):
    mock_session = MagicMock()
    mock_repo = MagicMock()
    current = MagicMock(id=2, status="champion")
    previous = MagicMock(id=1, status="active")
    mock_repo.find_champion.return_value = current
    mock_repo.find_previous_champion.return_value = previous

    with (
        patch(
            "trendx.mlops.registry.db_manager.get_session", return_value=_session_cm(mock_session)
        ),
        patch("trendx.mlops.registry.PredictionModelRepository", return_value=mock_repo),
    ):
        result = registry.rollback("dev-001", "temperature")

    assert result is None
    assert current.status == "champion"
    assert previous.status == "active"
    mock_repo.record_status_transition.assert_not_called()
    mock_session.commit.assert_not_called()


@pytest.mark.unit
def test_promote_then_rollback_records_transitions(registry):
    mock_session = MagicMock()
    mock_repo = MagicMock()
    champion_a = MagicMock()
    champion_a.id = 1
    champion_a.status = "champion"
    champion_a.tb_telemetry_key = "temperature"
    challenger_b = MagicMock()
    challenger_b.id = 2
    challenger_b.status = "challenger"
    challenger_b.tb_telemetry_key = "temperature"

    def set_champion(model_id, entity_id, metric_key):
        assert model_id == challenger_b.id
        assert entity_id == "dev-001"
        assert metric_key == "temperature"
        champion_a.status = "challenger"
        challenger_b.status = "champion"
        return challenger_b

    mock_repo.find_by_business_entity.return_value = [champion_a, challenger_b]
    mock_repo.set_champion.side_effect = set_champion
    mock_repo.find_champion.return_value = challenger_b
    mock_repo.find_previous_champion.return_value = champion_a

    with (
        patch(
            "trendx.mlops.registry.db_manager.get_session", return_value=_session_cm(mock_session)
        ),
        patch("trendx.mlops.registry.PredictionModelRepository", return_value=mock_repo),
    ):
        promoted = registry.promote_to_champion("dev-001", "temperature", challenger_b)
        rolled_back = registry.rollback("dev-001", "temperature")

    assert promoted is challenger_b
    assert rolled_back is champion_a
    assert champion_a.status == "champion"
    assert challenger_b.status == "challenger"
    mock_repo.find_previous_champion.assert_called_once_with("dev-001", "temperature", 2)
    assert mock_repo.record_status_transition.call_count == 4
    transition_args = [record.args for record in mock_repo.record_status_transition.call_args_list]
    assert [args[:5] for args in transition_args] == [
        (1, "dev-001", "temperature", "champion", "challenger"),
        (2, "dev-001", "temperature", "challenger", "champion"),
        (2, "dev-001", "temperature", "champion", "challenger"),
        (1, "dev-001", "temperature", "challenger", "champion"),
    ]
    assert all(isinstance(args[5], int) for args in transition_args)


@pytest.mark.unit
def test_rollback_without_historical_champion_keeps_statuses_unchanged(registry):
    mock_session = MagicMock()
    mock_repo = MagicMock()
    current = MagicMock(id=2, status="champion")
    mock_repo.find_champion.return_value = current
    mock_repo.find_previous_champion.return_value = None

    with (
        patch(
            "trendx.mlops.registry.db_manager.get_session", return_value=_session_cm(mock_session)
        ),
        patch("trendx.mlops.registry.PredictionModelRepository", return_value=mock_repo),
    ):
        result = registry.rollback("dev-001", "temperature")

    assert result is None
    assert current.status == "champion"
    mock_repo.record_status_transition.assert_not_called()
    mock_session.commit.assert_not_called()


@pytest.mark.unit
def test_set_champion_demotes_previous_champion_status_column():
    session = MagicMock()
    repo = PredictionModelRepository(session)

    old_champion = PredictionModel(
        id=uuid4(),
        tenant_id=uuid4(),
        customer_id=uuid4(),
        created_ts=1,
        updated_ts=1,
        name="old",
        status="champion",
        type="prophet",
        associated_entity_field_id=uuid4(),
        tb_telemetry_key="temperature",
        model_parameters="",
        datasource_parameters="",
        method_parameters="",
        item_state_map="",
        trained_item_set="",
        business_entity_id=uuid4(),
        business_entity_field_id=uuid4(),
    )
    new_model = PredictionModel(
        id=uuid4(),
        tenant_id=uuid4(),
        customer_id=uuid4(),
        created_ts=2,
        updated_ts=2,
        name="new",
        status="challenger",
        type="prophet",
        associated_entity_field_id=uuid4(),
        tb_telemetry_key="temperature",
        model_parameters="",
        datasource_parameters="",
        method_parameters="",
        item_state_map="",
        trained_item_set="",
        business_entity_id=uuid4(),
        business_entity_field_id=uuid4(),
    )

    with patch.object(repo, "find_by_business_entity", return_value=[old_champion, new_model]):
        result = repo.set_champion(new_model.id, "dev-001", "temperature")

    assert result is new_model
    assert new_model.status == "champion"
    assert old_champion.status == "challenger"


@pytest.mark.unit
def test_register_rejects_empty_uuid_before_postgres(registry):
    """ "" dans une colonne UUID NOT NULL doit échouer avant tout accès PG."""
    with pytest.raises(ValueError, match="Invalid UUID"):
        registry.register(
            business_entity_id="",
            tb_telemetry_key="temperature",
            model_type="LinearRegression",
            model_uri="runs:/x/model",
            customer_id=str(uuid4()),
            promote=False,
        )


@pytest.mark.unit
def test_register_requires_customer_id_fail_closed(registry):
    """customer_id absent du contexte -> RuntimeError explicite, jamais de valeur fictive."""
    with pytest.raises(RuntimeError, match="customer_id is required"):
        registry.register(
            business_entity_id=str(uuid4()),
            tb_telemetry_key="temperature",
            model_type="LinearRegression",
            model_uri="runs:/x/model",
            promote=False,
        )
