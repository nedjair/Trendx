"""Trendx scheduler wiring (mock/ephemeral P0).

Orchestrates existing workers/services; duplicates no business logic.
No global side effects, no production scheduler, no TB writes.
"""

from trendx.scheduler.registry import JobDefinition, SchedulerRegistry

__all__ = ["JobDefinition", "SchedulerRegistry"]
