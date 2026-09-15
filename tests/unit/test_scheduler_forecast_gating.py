"""Unit tests for the forecast gating (suspension before rollout).

Covers: forecast-train/forecast-run excluded by default and when explicitly
disabled; ingestion/discovery unaffected; explicit true only ever lists the
specs (registration path — no forecast is executed here); anomaly_scan stays
excluded. No database, no threads, no execution.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

from trendx.scheduler.service import SchedulerService


def _settings(**overrides: Any) -> SimpleNamespace:
    base: dict[str, Any] = {
        "trendx_scheduler_enabled": False,
        "trendx_scheduler_instance_id": "unit-1",
    }
    base.update(overrides)
    return SimpleNamespace(**base)


def _service(**kwargs: Any) -> SchedulerService:
    settings_obj = kwargs.pop("settings_obj", _settings())
    return SchedulerService(settings_obj=settings_obj, **kwargs)


def _ids(svc: SchedulerService) -> set[str]:
    specs = svc._scheduled_specs()
    assert specs is not None
    return {s.job_id for s in specs}


def test_forecast_excluded_by_default() -> None:
    # No flag attribute at all (historical settings shape) -> fail-closed OFF.
    ids = _ids(_service())
    assert "forecast-train" not in ids
    assert "forecast-run" not in ids


def test_forecast_excluded_when_explicitly_false() -> None:
    ids = _ids(_service(settings_obj=_settings(trendx_scheduler_forecast_enabled=False)))
    assert "forecast-train" not in ids
    assert "forecast-run" not in ids


def test_ingestion_discovery_kept_when_forecast_off() -> None:
    ids = _ids(_service())
    assert "ingestion-run" in ids
    assert "discovery-sync-initial" in ids
    assert "discovery-sync-incremental" in ids
    assert "anomaly-scan" not in ids


def test_forecast_listed_when_explicitly_true() -> None:
    # Registration path only: specs are listed, nothing is executed here.
    svc = _service(settings_obj=_settings(trendx_scheduler_forecast_enabled=True))
    ids = _ids(svc)
    assert "forecast-train" in ids
    assert "forecast-run" in ids
    assert "ingestion-run" in ids


def test_explicit_specs_override_untouched() -> None:
    # An explicit specs override (tests/ops) bypasses filtering, as before.
    from trendx.scheduler.jobs import P0_JOBS

    svc = _service(specs=P0_JOBS)
    assert svc._scheduled_specs() == P0_JOBS


def test_config_default_is_false() -> None:
    from trendx.config import settings as _real_settings

    field = _real_settings.model_fields["trendx_scheduler_forecast_enabled"]
    assert field.default is False
    assert field.alias == "TRENDX_SCHEDULER_FORECAST_ENABLED"


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main([__file__, "-v"]))
