"""Trendx Business Query Engine (disposable P0, in-memory/pandas).

Storage UTC, display TZ separate. No DB, no TB, no writeback.
"""

from trendx.query.engine import BusinessQuery, BusinessQueryEngine, RelationGraph
from trendx.query.stats import describe_series

__all__ = ["BusinessQueryEngine", "BusinessQuery", "RelationGraph", "describe_series"]
