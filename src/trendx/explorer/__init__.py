"""Metric Explorer API (read-only, jetable P0).

Routes under /api/v1/explorer/*. Auth enforced by main middleware.
No TB writes, no alarms, no production side effects.
"""

from trendx.explorer.router import router

__all__ = ["router"]
