from __future__ import annotations

import hashlib
import time
from dataclasses import dataclass
from typing import Any

from loguru import logger

from trendx.config import settings
from trendx.thingsboard.client import (
    AlarmData,
    EntityId,
    PageData,
    ThingsBoardClient,
    ThingsBoardError,
    ThingsBoardNotFoundError,
)


SEVERITY_MAP: dict[str, str] = {
    "critical": "CRITICAL",
    "major": "MAJOR",
    "minor": "MINOR",
    "warning": "WARNING",
    "info": "INFO",
}

SEVERITY_ORDER: dict[str, int] = {
    "CRITICAL": 5,
    "MAJOR": 4,
    "MINOR": 3,
    "WARNING": 2,
    "INFO": 1,
}


def _normalize_severity(severity: str) -> str:
    upper = severity.upper()
    if upper in SEVERITY_ORDER:
        return upper
    return SEVERITY_MAP.get(severity.lower(), "CRITICAL")


def _make_incident_key(alarm_type: str, originator_id: str) -> str:
    raw = f"{alarm_type}:{originator_id}"
    return hashlib.sha256(raw.encode()).hexdigest()[:32]


@dataclass
class SeverityConfig:
    open_threshold: float
    close_threshold: float | None = None
    cooldown_seconds: int = 7200
    min_duration_seconds: int = 0

    def __post_init__(self) -> None:
        if self.close_threshold is None:
            self.close_threshold = self.open_threshold * 0.75


SEVERITY_CONFIGS: dict[str, SeverityConfig] = {
    "CRITICAL": SeverityConfig(open_threshold=0.95, close_threshold=0.7, cooldown_seconds=3600),
    "MAJOR": SeverityConfig(open_threshold=0.85, close_threshold=0.6, cooldown_seconds=7200),
    "MINOR": SeverityConfig(open_threshold=0.75, close_threshold=0.5, cooldown_seconds=14400),
    "WARNING": SeverityConfig(open_threshold=0.6, close_threshold=0.4, cooldown_seconds=21600),
    "INFO": SeverityConfig(open_threshold=0.4, close_threshold=0.2, cooldown_seconds=43200),
}


@dataclass
class IncidentState:
    incident_key: str
    alarm_id: str | None = None
    created_ts: float = 0.0
    last_updated_ts: float = 0.0
    severity: str = "WARNING"
    active: bool = False
    acknowledged: bool = False
    cleared: bool = False
    score: float = 0.0


class AlarmService:
    def __init__(
        self,
        client: ThingsBoardClient | None = None,
        severity_configs: dict[str, SeverityConfig] | None = None,
    ) -> None:
        self._client = client or ThingsBoardClient()
        self._severity_configs = severity_configs or SEVERITY_CONFIGS
        self._incidents: dict[str, IncidentState] = {}
        self._cooldown_tracker: dict[str, float] = {}
        self._alarms_enabled = settings.tb_alarms_enabled

    async def _find_existing_alarm(
        self,
        originator_type: str,
        originator_id: str,
        alarm_type: str,
    ) -> AlarmData | None:
        try:
            result = await self._client.get_alarms(
                entity_type=originator_type,
                entity_id=originator_id,
                status="ACTIVE",
                limit=100,
            )
            for item in result.data:
                alarm = AlarmData.model_validate(item)
                if alarm.type == alarm_type:
                    return alarm
        except ThingsBoardError as exc:
            logger.warning("Failed to search existing alarms: {exc}", exc=exc)
        return None

    async def create_alarm(
        self,
        originator_type: str,
        originator_id: str,
        alarm_type: str,
        severity: str = "WARNING",
        details: dict[str, Any] | None = None,
        incident_key: str | None = None,
    ) -> dict[str, Any]:
        if not self._alarms_enabled:
            logger.debug("Alarms disabled, skipping create_alarm")
            return {}

        severity = _normalize_severity(severity)
        key = incident_key or _make_incident_key(alarm_type, originator_id)

        now_ms = int(time.time() * 1000)

        existing = await self._find_existing_alarm(originator_type, originator_id, alarm_type)
        if existing is not None:
            logger.info(
                "Active alarm already exists for {etype}/{eid} type={atype} (id={aid}), skipping creation",
                etype=originator_type, eid=originator_id, atype=alarm_type, aid=existing.id.id if existing.id else "?",
            )
            return existing.model_dump()

        alarm = AlarmData(
            type=alarm_type,
            originator=EntityId(entityType=originator_type, id=originator_id),
            severity=severity,
            status="ACTIVE_UNACK",
            startTs=now_ms,
            details=details or {},
            propagate=True,
        )
        try:
            result = await self._client.save_alarm(alarm)
            incident = IncidentState(
                incident_key=key,
                alarm_id=result.get("id", {}).get("id") if isinstance(result, dict) else None,
                created_ts=time.time(),
                last_updated_ts=time.time(),
                severity=severity,
                active=True,
                score=self._extract_score(details),
            )
            self._incidents[key] = incident
            logger.info(
                "Created alarm type={atype} severity={sev} for {etype}/{eid}",
                atype=alarm_type, sev=severity, etype=originator_type, eid=originator_id,
            )
            return result
        except ThingsBoardError as exc:
            logger.error("Failed to create alarm: {exc}", exc=exc)
            return {}

    async def clear_alarm(
        self,
        alarm_id: str,
        incident_key: str | None = None,
    ) -> dict[str, Any]:
        if not self._alarms_enabled:
            return {}
        try:
            result = await self._client.clear_alarm(alarm_id)
            if incident_key and incident_key in self._incidents:
                self._incidents[incident_key].cleared = True
                self._incidents[incident_key].active = False
            logger.info("Cleared alarm {aid}", aid=alarm_id)
            return result
        except ThingsBoardNotFoundError:
            logger.warning("Alarm {aid} not found for clearing", aid=alarm_id)
            return {}
        except ThingsBoardError as exc:
            logger.error("Failed to clear alarm {aid}: {exc}", aid=alarm_id, exc=exc)
            return {}

    async def ack_alarm(
        self,
        alarm_id: str,
        incident_key: str | None = None,
    ) -> dict[str, Any]:
        if not self._alarms_enabled:
            return {}
        try:
            result = await self._client.ack_alarm(alarm_id)
            if incident_key and incident_key in self._incidents:
                self._incidents[incident_key].acknowledged = True
            logger.info("Acknowledged alarm {aid}", aid=alarm_id)
            return result
        except ThingsBoardNotFoundError:
            logger.warning("Alarm {aid} not found for acknowledging", aid=alarm_id)
            return {}
        except ThingsBoardError as exc:
            logger.error("Failed to ack alarm {aid}: {exc}", aid=alarm_id, exc=exc)
            return {}

    async def get_active_alarms(
        self,
        originator_type: str,
        originator_id: str,
        limit: int = 100,
    ) -> list[AlarmData]:
        try:
            result = await self._client.get_alarms(
                entity_type=originator_type,
                entity_id=originator_id,
                status="ACTIVE",
                limit=limit,
            )
            return [AlarmData.model_validate(item) for item in result.data]
        except ThingsBoardError as exc:
            logger.error("Failed to get active alarms for {etype}/{eid}: {exc}",
                         etype=originator_type, eid=originator_id, exc=exc)
            return []

    async def evaluate_and_alert(
        self,
        originator_type: str,
        originator_id: str,
        alarm_type: str,
        score: float,
        details: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        if not self._alarms_enabled:
            return {}

        key = _make_incident_key(alarm_type, originator_id)
        incident = self._incidents.get(key)
        now = time.time()

        severity = self._score_to_severity(score)
        config = self._severity_configs.get(severity, self._severity_configs["WARNING"])

        if incident and incident.active:
            cooldown_remaining = config.cooldown_seconds - (now - incident.last_updated_ts)
            if cooldown_remaining > 0:
                logger.debug(
                    "Alarm cooldown active for {key}: {rem:.0f}s remaining",
                    key=key, rem=cooldown_remaining,
                )
                return {"status": "cooldown", "remaining_seconds": cooldown_remaining}

        if incident and incident.active:
            if score < config.close_threshold:
                if incident.alarm_id:
                    await self.clear_alarm(incident.alarm_id, incident_key=key)
                    logger.info(
                        "Alarm cleared for {key} (score {score:.3f} < close_threshold {thresh:.3f})",
                        key=key, score=score, thresh=config.close_threshold,
                    )
                return {"status": "cleared", "score": score}

        if incident and not incident.active:
            if now - incident.created_ts < config.min_duration_seconds:
                logger.debug("Alarm {key} below min duration, not recreating", key=key)
                return {"status": "below_min_duration"}

        if score >= config.open_threshold:
            if incident and incident.active:
                if incident.alarm_id:
                    await self.ack_alarm(incident.alarm_id)
                return {"status": "already_active", "severity": severity}
            return await self.create_alarm(
                originator_type=originator_type,
                originator_id=originator_id,
                alarm_type=alarm_type,
                severity=severity,
                details=details,
                incident_key=key,
            )

        return {"status": "below_threshold", "score": score}

    def _score_to_severity(self, score: float) -> str:
        if score >= 0.95:
            return "CRITICAL"
        if score >= 0.85:
            return "MAJOR"
        if score >= 0.75:
            return "MINOR"
        if score >= 0.6:
            return "WARNING"
        return "INFO"

    def _extract_score(self, details: dict[str, Any] | None) -> float:
        if details is None:
            return 0.0
        return float(details.get("score", details.get("anomaly_score", 0.0)))

    async def close(self) -> None:
        logger.info(
            "AlarmService closed ({active} active incidents tracked)",
            active=sum(1 for i in self._incidents.values() if i.active),
        )
