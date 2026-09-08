from __future__ import annotations

import hashlib
from datetime import UTC, datetime
from typing import Any, cast

from loguru import logger
from trendx.config import settings
from trendx.database.connection import manager as db_manager
from trendx.database.repositories import (
    AlertIncidentRepository,
    AlertRuleRepository,
)
from trendx.thingsboard.alarms import AlarmService
from trendx.thingsboard.client import ThingsBoardClient


class AlertingService:
    """Evaluates alert rules and manages alarm lifecycle.

    Supports threshold rules, anomaly rules, and no-data rules with
    idempotent alarm creation via logical keys, cooldowns, and hysteresis.
    """

    def __init__(
        self,
        alarm_service: AlarmService | None = None,
        tb_client: ThingsBoardClient | None = None,
        cooldown_default: int = 7200,
        min_duration_default: int = 60,
    ) -> None:
        self._alarm_service = alarm_service or AlarmService(
            client=tb_client or ThingsBoardClient(),
        )
        self._cooldown_default = cooldown_default
        self._min_duration_default = min_duration_default

    def _build_logical_key(
        self,
        entity_id: str,
        metric_key: str,
        rule_type: str,
        rule_name: str = "",
    ) -> str:
        raw = f"{entity_id}:{metric_key}:{rule_type}:{rule_name}"
        return hashlib.sha256(raw.encode()).hexdigest()[:32]

    def evaluate_threshold_rules(
        self,
        entity_id: str,
        metric_key: str,
        current_value: float,
    ) -> list[dict[str, Any]]:
        results: list[dict[str, Any]] = []
        with db_manager.get_session("catalog") as session:
            rule_repo = AlertRuleRepository(session)
            rules = rule_repo.find_active()
            relevant = [
                r
                for r in rules
                if r.rule_type == "threshold"
                and (r.entity_id is None or str(r.entity_id) == entity_id)
                and (r.metric_key is None or r.metric_key == metric_key)
            ]
            for rule in relevant:
                condition = rule.condition_json
                operator = condition.get("operator", "gt")
                threshold = condition.get("threshold", 0.0)
                triggered = self._compare(current_value, threshold, operator)
                incident_key = self._build_logical_key(
                    entity_id, metric_key, "threshold", rule.name
                )
                if triggered:
                    incident = {
                        "logical_key": incident_key,
                        "rule_id": str(rule.id),
                        "entity_id": entity_id,
                        "metric_key": metric_key,
                        "severity": rule.severity,
                        "current_value": current_value,
                        "threshold": threshold,
                        "open_threshold": rule.open_threshold,
                        "close_threshold": rule.close_threshold,
                        "cooldown_seconds": rule.cooldown_seconds,
                        "min_duration_seconds": rule.min_duration_seconds,
                        "rule_name": rule.name,
                        "rule_type": "threshold",
                    }
                    result = self.create_tb_alarm(entity_id, rule, incident)
                    results.append(result)
                else:
                    self._maybe_clear_alarm(incident_key, entity_id, rule)
        return results

    def evaluate_anomaly_rules(
        self,
        entity_id: str,
        metric_key: str,
        anomaly_event: dict[str, Any],
    ) -> list[dict[str, Any]]:
        results: list[dict[str, Any]] = []
        with db_manager.get_session("catalog") as session:
            rule_repo = AlertRuleRepository(session)
            rules = rule_repo.find_active()
            relevant = [
                r
                for r in rules
                if r.rule_type == "anomaly"
                and (r.entity_id is None or str(r.entity_id) == entity_id)
                and (r.metric_key is None or r.metric_key == metric_key)
            ]
            for rule in relevant:
                score = anomaly_event.get("score", 0.0)
                condition = rule.condition_json
                threshold = condition.get("threshold", rule.open_threshold or 0.7)
                triggered = score >= threshold
                incident_key = self._build_logical_key(entity_id, metric_key, "anomaly", rule.name)
                if triggered:
                    incident = {
                        "logical_key": incident_key,
                        "rule_id": str(rule.id),
                        "entity_id": entity_id,
                        "metric_key": metric_key,
                        "severity": rule.severity,
                        "current_value": score,
                        "threshold": threshold,
                        "anomaly_details": anomaly_event,
                        "open_threshold": rule.open_threshold,
                        "close_threshold": rule.close_threshold,
                        "cooldown_seconds": rule.cooldown_seconds,
                        "min_duration_seconds": rule.min_duration_seconds,
                        "rule_name": rule.name,
                        "rule_type": "anomaly",
                    }
                    result = self.create_tb_alarm(entity_id, rule, incident)
                    results.append(result)
                else:
                    self._maybe_clear_alarm(incident_key, entity_id, rule)
        return results

    def evaluate_no_data_rules(
        self,
        entity_id: str,
        metric_key: str,
        last_timestamp: datetime | None,
    ) -> list[dict[str, Any]]:
        results: list[dict[str, Any]] = []
        now = datetime.now(UTC)
        if last_timestamp is None:
            return results
        elapsed = (now - last_timestamp).total_seconds()
        with db_manager.get_session("catalog") as session:
            rule_repo = AlertRuleRepository(session)
            rules = rule_repo.find_active()
            relevant = [
                r
                for r in rules
                if r.rule_type == "no_data"
                and (r.entity_id is None or str(r.entity_id) == entity_id)
                and (r.metric_key is None or r.metric_key == metric_key)
            ]
            for rule in relevant:
                condition = rule.condition_json
                max_gap_seconds = condition.get("max_gap_seconds", 7200)
                triggered = elapsed > max_gap_seconds
                incident_key = self._build_logical_key(entity_id, metric_key, "no_data", rule.name)
                if triggered:
                    incident = {
                        "logical_key": incident_key,
                        "rule_id": str(rule.id),
                        "entity_id": entity_id,
                        "metric_key": metric_key,
                        "severity": rule.severity,
                        "current_value": float(elapsed),
                        "threshold": float(max_gap_seconds),
                        "last_timestamp": last_timestamp.isoformat(),
                        "open_threshold": rule.open_threshold,
                        "close_threshold": rule.close_threshold,
                        "cooldown_seconds": rule.cooldown_seconds,
                        "min_duration_seconds": rule.min_duration_seconds,
                        "rule_name": rule.name,
                        "rule_type": "no_data",
                    }
                    result = self.create_tb_alarm(entity_id, rule, incident)
                    results.append(result)
                else:
                    self._maybe_clear_alarm(incident_key, entity_id, rule)
        return results

    def create_tb_alarm(
        self,
        entity_id: str,
        rule: Any,
        incident: dict[str, Any],
    ) -> dict[str, Any]:
        if not self._check_cooldown(incident):
            return {"status": "cooldown", "logical_key": incident["logical_key"]}
        if not self._check_min_duration(incident):
            return {"status": "below_min_duration", "logical_key": incident["logical_key"]}
        if not self._check_hysteresis_open(incident):
            return {"status": "hysteresis_blocked", "logical_key": incident["logical_key"]}
        with db_manager.get_session("catalog") as session:
            incident_repo = AlertIncidentRepository(session)
            existing = incident_repo.find_by_logical_key(incident["logical_key"])
            if existing is not None and existing.status in ("ACTIVE", "ACK"):
                incident_repo.open_or_update(
                    logical_key=incident["logical_key"],
                    rule_id=incident["rule_id"],
                    entity_id=incident["entity_id"],
                    severity=incident["severity"],
                    metric_key=incident.get("metric_key"),
                    open_reason=f"{incident.get('rule_type', 'alert')}: {incident.get('rule_name', '')} triggered",
                    last_value=incident.get("current_value"),
                )
                session.commit()
                return {"status": "already_active", "logical_key": incident["logical_key"]}
            alarm_result = self._create_thingsboard_alarm(entity_id, rule, incident)
            incident_repo.open_or_update(
                logical_key=incident["logical_key"],
                rule_id=incident["rule_id"],
                entity_id=incident["entity_id"],
                severity=incident["severity"],
                metric_key=incident.get("metric_key"),
                open_reason=f"{incident.get('rule_type', 'alert')}: {incident.get('rule_name', '')} triggered",
                last_value=incident.get("current_value"),
            )
            session.commit()
            return {
                "status": "created",
                "logical_key": incident["logical_key"],
                "alarm": alarm_result,
            }

    def update_tb_alarm(self, incident: dict[str, Any]) -> dict[str, Any]:
        with db_manager.get_session("catalog") as session:
            incident_repo = AlertIncidentRepository(session)
            existing = incident_repo.find_by_logical_key(incident["logical_key"])
            if existing is None:
                return {"status": "not_found"}
            if existing.status in ("CLEARED", "CLOSED"):
                return {"status": "already_closed"}
            if incident.get("clear", False):
                cleared = incident_repo.clear(
                    incident["logical_key"], close_reason=incident.get("close_reason")
                )
                session.commit()
                if cleared and cleared.external_tb_alarm_id:
                    import asyncio

                    try:
                        loop = asyncio.get_event_loop()
                    except RuntimeError:
                        loop = asyncio.new_event_loop()
                        asyncio.set_event_loop(loop)
                    loop.run_until_complete(
                        self._alarm_service.clear_alarm(
                            str(cleared.external_tb_alarm_id),
                            incident_key=incident["logical_key"],
                        )
                    )
                return {"status": "cleared"}
            incident_repo.open_or_update(
                logical_key=incident["logical_key"],
                rule_id=incident["rule_id"],
                entity_id=incident["entity_id"],
                severity=incident["severity"],
                metric_key=incident.get("metric_key"),
                open_reason=f"{incident.get('rule_type', 'alert')}: {incident.get('rule_name', '')} triggered",
                last_value=incident.get("current_value"),
            )
            session.commit()
            return {"status": "updated"}

    def apply_cooldown(self, incident: dict[str, Any]) -> bool:
        return self._check_cooldown(incident)

    def apply_hysteresis(
        self,
        incident: dict[str, Any],
        current_value: float,
    ) -> bool:
        return self._check_hysteresis_value(incident, current_value)

    def _check_cooldown(self, incident: dict[str, Any]) -> bool:
        with db_manager.get_session("catalog") as session:
            incident_repo = AlertIncidentRepository(session)
            existing = incident_repo.find_by_logical_key(incident["logical_key"])
        if existing is None:
            return True
        cooldown = incident.get("cooldown_seconds", self._cooldown_default)
        if existing.status in ("ACTIVE", "ACK") and existing.opened_at is not None:
            elapsed = (datetime.now(UTC) - existing.opened_at).total_seconds()
            if elapsed < cooldown:
                logger.debug(
                    "Cooldown active for {}: {:.0f}s remaining",
                    incident["logical_key"],
                    cooldown - elapsed,
                )
                return False
        return True

    def _check_min_duration(self, incident: dict[str, Any]) -> bool:
        min_dur = incident.get("min_duration_seconds", self._min_duration_default)
        if min_dur <= 0:
            return True
        with db_manager.get_session("catalog") as session:
            incident_repo = AlertIncidentRepository(session)
            existing = incident_repo.find_by_logical_key(incident["logical_key"])
        if existing is not None and existing.opened_at is not None:
            elapsed = (datetime.now(UTC) - existing.opened_at).total_seconds()
            if elapsed < min_dur:
                logger.debug(
                    "Below min duration for {}: {:.0f}s < {}s",
                    incident["logical_key"],
                    elapsed,
                    min_dur,
                )
                return False
        return True

    def _check_hysteresis_open(self, incident: dict[str, Any]) -> bool:
        open_th = incident.get("open_threshold")
        if open_th is None:
            return True
        current = incident.get("current_value", 0.0)
        return cast(bool, current >= open_th)

    def _check_hysteresis_value(self, incident: dict[str, Any], current_value: float) -> bool:
        close_th = incident.get("close_threshold")
        if close_th is None:
            return True
        with db_manager.get_session("catalog") as session:
            incident_repo = AlertIncidentRepository(session)
            existing = incident_repo.find_by_logical_key(incident["logical_key"])
        if existing is None or existing.status not in ("ACTIVE", "ACK"):
            return cast(bool, current_value >= (incident.get("open_threshold", close_th)))
        return cast(bool, current_value < close_th)

    def _maybe_clear_alarm(
        self,
        incident_key: str,
        entity_id: str,
        rule: Any,
    ) -> None:
        with db_manager.get_session("catalog") as session:
            incident_repo = AlertIncidentRepository(session)
            existing = incident_repo.find_by_logical_key(incident_key)
            if existing is not None and existing.status in ("ACTIVE", "ACK"):
                incident_repo.clear(incident_key, close_reason="Condition no longer triggered")
                session.commit()
                if existing.external_tb_alarm_id:
                    import asyncio

                    try:
                        loop = asyncio.get_event_loop()
                    except RuntimeError:
                        loop = asyncio.new_event_loop()
                        asyncio.set_event_loop(loop)
                    loop.run_until_complete(
                        self._alarm_service.clear_alarm(
                            str(existing.external_tb_alarm_id),
                            incident_key=incident_key,
                        )
                    )

    def _create_thingsboard_alarm(
        self,
        entity_id: str,
        rule: Any,
        incident: dict[str, Any],
    ) -> dict[str, Any]:
        if not settings.tb_alarms_enabled:
            return {}
        import asyncio

        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        result = loop.run_until_complete(
            self._alarm_service.create_alarm(
                originator_type="DEVICE",
                originator_id=entity_id,
                alarm_type=f"trendx_{incident.get('rule_type', 'alert')}_{rule.name}",
                severity=incident.get("severity", "WARNING"),
                details={
                    "rule_name": rule.name,
                    "metric_key": incident.get("metric_key", ""),
                    "current_value": incident.get("current_value"),
                    "threshold": incident.get("threshold"),
                    "logical_key": incident["logical_key"],
                },
                incident_key=incident["logical_key"],
            )
        )
        return result

    @staticmethod
    def _compare(value: float, threshold: float, operator: str) -> bool:
        if operator == "gt":
            return value > threshold
        if operator == "gte":
            return value >= threshold
        if operator == "lt":
            return value < threshold
        if operator == "lte":
            return value <= threshold
        if operator == "eq":
            return abs(value - threshold) < 1e-9
        if operator == "neq":
            return abs(value - threshold) >= 1e-9
        return False
