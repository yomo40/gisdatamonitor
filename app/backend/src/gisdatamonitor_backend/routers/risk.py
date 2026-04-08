from __future__ import annotations

from fastapi import APIRouter, Depends, Query
from sqlalchemy.engine import Connection

from ..db import get_db_conn
from ..services.repository import DataRepository

router = APIRouter(prefix="/risk", tags=["risk"])


@router.get("/snapshot")
def get_risk_snapshot(
    window: str = Query(default="24h", pattern="^(24h|7d|30d)$"),
    conn: Connection = Depends(get_db_conn),
) -> dict:
    repo = DataRepository(conn)
    return repo.risk_snapshot(window=window)


@router.get("/timeline")
def get_risk_timeline(
    window: str = Query(default="30d", pattern="^(24h|7d|30d)$"),
    region_level: str = Query(default="province", pattern="^(province|city)$"),
    region_name: str | None = Query(default=None),
    conn: Connection = Depends(get_db_conn),
) -> dict:
    repo = DataRepository(conn)
    return repo.risk_timeline(window=window, region_level=region_level, region_name=region_name)


@router.get("/explain")
def get_risk_explain(
    window: str = Query(default="24h", pattern="^(24h|7d|30d)$"),
    region_level: str = Query(default="province", pattern="^(province|city)$"),
    region_name: str | None = Query(default=None),
    conn: Connection = Depends(get_db_conn),
) -> dict:
    repo = DataRepository(conn)
    return repo.risk_explain(window=window, region_level=region_level, region_name=region_name)
