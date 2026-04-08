from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query, Response
from sqlalchemy.engine import Connection

from ..db import get_db_conn
from ..services.repository import DataRepository

router = APIRouter(prefix="/map", tags=["map"])


@router.get("/layers/{layer}.geojson")
def get_layer_geojson(
    layer: str,
    limit: int = Query(default=5000, ge=1, le=50000),
    conn: Connection = Depends(get_db_conn),
) -> dict:
    repo = DataRepository(conn)
    try:
        return repo.layer_geojson(layer=layer, limit=limit)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get("/tiles/{layer}/{z}/{x}/{y}.mvt")
def get_layer_tile(
    layer: str,
    z: int,
    x: int,
    y: int,
    conn: Connection = Depends(get_db_conn),
) -> Response:
    repo = DataRepository(conn)
    try:
        data = repo.mvt_tile(layer=layer, z=z, x=x, y=y)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return Response(content=data, media_type="application/vnd.mapbox-vector-tile")


@router.get("/tiles/dem/{z}/{x}/{y}.png")
def get_dem_tile(
    z: int,
    x: int,
    y: int,
    derivative: str = Query(default="hillshade", pattern="^(hillshade|slope|aspect|roughness)$"),
    conn: Connection = Depends(get_db_conn),
) -> Response:
    repo = DataRepository(conn)
    try:
        data = repo.dem_tile_png(derivative=derivative, z=z, x=x, y=y)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return Response(content=data, media_type="image/png")
