"""P0 job catalogue: stable IDs, declared intervals (documentation only)."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class JobSpec:
    job_id: str
    job_type: str
    interval: str
    description: str


P0_JOBS: tuple[JobSpec, ...] = (
    JobSpec("discovery-sync-initial", "topology_discovery", "once", "Initial topology discovery"),
    JobSpec("discovery-sync-incremental", "topology_sync", "15m", "Incremental topology sync"),
    JobSpec("ingestion-run", "ingestion", "1h", "Incremental ingestion with checkpoint"),
    JobSpec("forecast-train", "trendx_train", "1d", "Periodic model training"),
    JobSpec("forecast-run", "trendx_forecast", "1h", "Periodic forecasting"),
    JobSpec("anomaly-scan", "anomaly_scan", "1h", "Periodic anomaly scan"),
)
