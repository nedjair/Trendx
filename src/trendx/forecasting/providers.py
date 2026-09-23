"""Generic external feature providers (W91): observations without obligation.

Layering (unchanged responsibilities):

    FeatureResolver  -> decides status (RESOLVED/UNAVAILABLE/...)
    ExternalFeatureProvider -> supplies observations for EXTERNAL_FORECAST
    DatasetBuilder   -> assembles (normalizes timezones, aligns, resamples)

``collect_external_frames`` bridges provider observations into the frames
mapping consumed by ``DatasetBuilder``. No network, no invented values:
missing provider -> UNAVAILABLE, malformed data -> INVALID (explicit).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol, runtime_checkable

import pandas as pd
from trendx.forecasting.resolution import ResolutionStatus, ResolvedFeatureSet


class ProviderUnavailableError(RuntimeError):
    """External source not configured or unreachable (controlled)."""


class ProviderInvalidDataError(ValueError):
    """External source returned malformed observations (controlled)."""


@dataclass(frozen=True)
class ExternalFeatureQuery:
    feature_name: str
    metric: str
    entity_id: str
    location: str = ""
    frequency: str = "1h"
    horizon: int = 24
    start: str = ""
    end: str = ""


@dataclass(frozen=True)
class ProviderFetch:
    feature_name: str
    status: str
    frame: pd.DataFrame | None = None
    detail: str = ""


@runtime_checkable
class ExternalFeatureProvider(Protocol):
    @property
    def name(self) -> str: ...

    def fetch(self, query: ExternalFeatureQuery) -> pd.DataFrame: ...


class InMemoryProvider:
    """Deterministic fixture provider keyed by (location, metric)."""

    def __init__(
        self,
        frames: dict[tuple[str, str], pd.DataFrame] | None = None,
        name: str = "in-memory",
    ) -> None:
        self._name = name
        self._frames = dict(frames or {})

    @property
    def name(self) -> str:
        return self._name

    def fetch(self, query: ExternalFeatureQuery) -> pd.DataFrame:
        key = (query.location or query.entity_id, query.metric)
        if key not in self._frames:
            msg = f"provider {self._name!r}: no data for {key!r}"
            raise ProviderUnavailableError(msg)
        frame = self._frames[key]
        if not isinstance(frame, pd.DataFrame) or (
            "ts" not in frame.columns or "value" not in frame.columns
        ):
            msg = f"provider {self._name!r}: malformed frame for {key!r}"
            raise ProviderInvalidDataError(msg)
        return frame.copy()


class WeatherAdapter:
    """Concrete weather adapter over a generic provider (optional layer).

    Maps entity scope to weather location explicitly; delegates fetching.
    Never used unless configured — weather stays optional.
    """

    def __init__(
        self,
        inner: ExternalFeatureProvider,
        location_by_entity: dict[str, str] | None = None,
        name: str = "weather",
    ) -> None:
        self._inner = inner
        self._locations = dict(location_by_entity or {})
        self._name = name

    @property
    def name(self) -> str:
        return self._name

    def fetch(self, query: ExternalFeatureQuery) -> pd.DataFrame:
        location = self._locations.get(query.entity_id, query.location)
        return self._inner.fetch(
            ExternalFeatureQuery(
                feature_name=query.feature_name,
                metric=query.metric,
                entity_id=query.entity_id,
                location=location,
                frequency=query.frequency,
                horizon=query.horizon,
                start=query.start,
                end=query.end,
            )
        )


def collect_external_frames(
    resolved: ResolvedFeatureSet,
    providers: dict[str, ExternalFeatureProvider] | None = None,
    frequency: str = "1h",
    horizon: int = 24,
) -> tuple[dict[tuple[str, str], pd.DataFrame], list[ProviderFetch]]:
    """Fetch external observations for EXTERNAL_FORECAST resolutions.

    Returns (frames keyed by (entity_id, metric), fetch records). Only
    RESOLVED-status external features with a configured provider yield
    frames; everything else is an explicit record, never invented data.
    """
    active = providers or {}
    frames: dict[tuple[str, str], pd.DataFrame] = {}
    records: list[ProviderFetch] = []
    for res in resolved.resolutions:
        if res.status is not ResolutionStatus.RESOLVED:
            continue
        provider = active.get(res.source, active.get("*"))
        if provider is None:
            records.append(
                ProviderFetch(
                    feature_name=res.name,
                    status="UNAVAILABLE",
                    detail="no provider configured",
                )
            )
            continue
        try:
            frame = provider.fetch(
                ExternalFeatureQuery(
                    feature_name=res.name,
                    metric=res.metric,
                    entity_id=res.entity_id,
                    frequency=frequency,
                    horizon=horizon,
                )
            )
        except ProviderUnavailableError as exc:
            records.append(
                ProviderFetch(feature_name=res.name, status="UNAVAILABLE", detail=str(exc))
            )
            continue
        except ProviderInvalidDataError as exc:
            records.append(ProviderFetch(feature_name=res.name, status="INVALID", detail=str(exc)))
            continue
        frames[(res.entity_id, res.metric or res.name)] = frame
        records.append(ProviderFetch(feature_name=res.name, status="RESOLVED"))
    return frames, records


__all__ = [
    "ExternalFeatureProvider",
    "ExternalFeatureQuery",
    "InMemoryProvider",
    "ProviderFetch",
    "ProviderInvalidDataError",
    "ProviderUnavailableError",
    "WeatherAdapter",
    "collect_external_frames",
]
