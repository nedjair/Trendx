from __future__ import annotations

from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest
from trendx.services.alerting import AlertingService


@pytest.fixture
def alerting_service():
    svc = AlertingService(
        alarm_service=MagicMock(),
        tb_client=MagicMock(),
        cooldown_default=3600,
        min_duration_default=60,
    )
    return svc


@pytest.mark.unit
@pytest.mark.xfail(
    reason="AlertRuleRepository mock in evaluation tests — attend le future commit sur feat/detector-scoring-and-forecasting-fixes"
)
def test_evaluate_threshold(alerting_service):
    mock_session = MagicMock()
    mock_repo = MagicMock()
    rule = MagicMock()
    rule.id = 1
    rule.name = "high-temp"
    rule.rule_type = "threshold"
    rule.entity_id = None
    rule.metric_key = None
    rule.severity = "WARNING"
    rule.condition_json = {"operator": "gt", "threshold": 50.0}
    rule.open_threshold = 0.6
    rule.close_threshold = 0.3
    rule.cooldown_seconds = 3600
    rule.min_duration_seconds = 60
    mock_repo.find_active.return_value = [rule]
    mock_session.__enter__ = MagicMock(return_value=mock_session)
    mock_session.__exit__ = MagicMock(return_value=None)

    with (
        patch("trendx.services.alerting.next", return_value=mock_session),
        patch("trendx.services.alerting.AlertRuleRepository", return_value=mock_repo),
        patch.object(alerting_service, "_maybe_clear_alarm"),
        patch.object(alerting_service, "create_tb_alarm", return_value={"status": "created"}),
    ):
        results = alerting_service.evaluate_threshold_rules(
            entity_id="dev-001",
            metric_key="temperature",
            current_value=75.0,
        )

    assert len(results) == 1


@pytest.mark.unit
def test_apply_cooldown(alerting_service):
    incident = {
        "logical_key": "test-key",
        "cooldown_seconds": 3600,
    }
    mock_session = MagicMock()
    mock_repo = MagicMock()
    existing = MagicMock()
    existing.status = "ACTIVE"
    existing.opened_at = datetime.now(UTC) - timedelta(seconds=100)
    mock_repo.find_by_logical_key.return_value = existing
    mock_session.__enter__ = MagicMock(return_value=mock_session)
    mock_session.__exit__ = MagicMock(return_value=None)

    with (
        patch("trendx.services.alerting.next", return_value=mock_session),
        patch("trendx.services.alerting.AlertIncidentRepository", return_value=mock_repo),
    ):
        result = alerting_service.apply_cooldown(incident)

    assert result is False


@pytest.mark.unit
def test_apply_cooldown_no_existing(alerting_service):
    incident = {"logical_key": "test-key", "cooldown_seconds": 3600}
    mock_session = MagicMock()
    mock_repo = MagicMock()
    mock_repo.find_by_logical_key.return_value = None
    mock_session.__enter__ = MagicMock(return_value=mock_session)
    mock_session.__exit__ = MagicMock(return_value=None)

    with (
        patch("trendx.services.alerting.next", return_value=mock_session),
        patch("trendx.services.alerting.AlertIncidentRepository", return_value=mock_repo),
    ):
        result = alerting_service.apply_cooldown(incident)

    assert result is True


@pytest.mark.unit
def test_apply_hysteresis(alerting_service):
    incident = {
        "logical_key": "test-key",
        "open_threshold": 0.6,
        "close_threshold": 0.3,
    }
    mock_session = MagicMock()
    mock_repo = MagicMock()
    existing = MagicMock()
    existing.status = "ACTIVE"
    mock_repo.find_by_logical_key.return_value = existing
    mock_session.__enter__ = MagicMock(return_value=mock_session)
    mock_session.__exit__ = MagicMock(return_value=None)

    with (
        patch("trendx.services.alerting.next", return_value=mock_session),
        patch("trendx.services.alerting.AlertIncidentRepository", return_value=mock_repo),
    ):
        result = alerting_service.apply_hysteresis(incident, current_value=0.2)

    assert result is True


@pytest.mark.unit
@pytest.mark.xfail(
    reason="AlertRuleRepository mock in evaluation tests — attend le future commit sur feat/detector-scoring-and-forecasting-fixes"
)
def test_no_data_rule(alerting_service):
    mock_session = MagicMock()
    mock_repo = MagicMock()
    rule = MagicMock()
    rule.id = 2
    rule.name = "no-data-check"
    rule.rule_type = "no_data"
    rule.entity_id = None
    rule.metric_key = None
    rule.severity = "CRITICAL"
    rule.condition_json = {"max_gap_seconds": 7200}
    rule.open_threshold = 0.6
    rule.close_threshold = 0.3
    rule.cooldown_seconds = 3600
    rule.min_duration_seconds = 60
    mock_repo.find_active.return_value = [rule]
    mock_session.__enter__ = MagicMock(return_value=mock_session)
    mock_session.__exit__ = MagicMock(return_value=None)

    with (
        patch("trendx.services.alerting.next", return_value=mock_session),
        patch("trendx.services.alerting.AlertRuleRepository", return_value=mock_repo),
        patch.object(alerting_service, "_maybe_clear_alarm"),
        patch.object(alerting_service, "create_tb_alarm", return_value={"status": "created"}),
    ):
        old_ts = datetime.now(UTC) - timedelta(hours=24)
        results = alerting_service.evaluate_no_data_rules(
            entity_id="dev-001",
            metric_key="temperature",
            last_timestamp=old_ts,
        )

    assert len(results) == 1


@pytest.mark.unit
def test_no_data_rule_not_triggered(alerting_service):
    mock_session = MagicMock()
    mock_repo = MagicMock()
    rule = MagicMock()
    rule.id = 2
    rule.name = "no-data-check"
    rule.rule_type = "no_data"
    rule.entity_id = None
    rule.metric_key = None
    rule.severity = "CRITICAL"
    rule.condition_json = {"max_gap_seconds": 7200}
    rule.open_threshold = 0.6
    rule.close_threshold = 0.3
    rule.cooldown_seconds = 3600
    rule.min_duration_seconds = 60
    mock_repo.find_active.return_value = [rule]
    mock_session.__enter__ = MagicMock(return_value=mock_session)
    mock_session.__exit__ = MagicMock(return_value=None)

    with (
        patch("trendx.services.alerting.next", return_value=mock_session),
        patch.object(alerting_service, "_maybe_clear_alarm"),
        patch.object(alerting_service, "create_tb_alarm"),
    ):
        recent_ts = datetime.now(UTC) - timedelta(minutes=5)
        results = alerting_service.evaluate_no_data_rules(
            entity_id="dev-001",
            metric_key="temperature",
            last_timestamp=recent_ts,
        )

    assert len(results) == 0


@pytest.mark.unit
def test_compare_operator(alerting_service):
    assert alerting_service._compare(10.0, 5.0, "gt") is True
    assert alerting_service._compare(5.0, 10.0, "gt") is False
    assert alerting_service._compare(10.0, 10.0, "gte") is True
    assert alerting_service._compare(3.0, 5.0, "lt") is True
    assert alerting_service._compare(5.0, 5.0, "lte") is True
    assert alerting_service._compare(5.0, 5.0, "eq") is True
    assert alerting_service._compare(5.0, 3.0, "neq") is True
    assert alerting_service._compare(5.0, 5.0, "unknown") is False
