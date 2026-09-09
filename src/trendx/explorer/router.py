"""Explorer routes: /api/v1/explorer/* (auth via main middleware)."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query
from trendx.explorer import schemas, service

router = APIRouter(prefix="/api/v1/explorer", tags=["explorer"])


@router.get("/catalog", response_model=schemas.CatalogOut)
def get_catalog() -> schemas.CatalogOut:
    try:
        return service.catalog()
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


@router.get("/availability", response_model=schemas.AvailabilityOut)
def get_availability(
    entity_id: str = Query(...), metric: str = Query(...)
) -> schemas.AvailabilityOut:
    try:
        return service.availability(entity_id, metric)
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


@router.get("/stats", response_model=schemas.StatsOut)
def get_stats(entity_id: str = Query(...), metric: str = Query(...)) -> schemas.StatsOut:
    try:
        return service.stats(entity_id, metric)
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


@router.post("/query", response_model=schemas.QueryOut)
def post_query(req: schemas.QueryIn) -> schemas.QueryOut:
    try:
        return service.query(req)
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
