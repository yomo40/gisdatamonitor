from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.engine import Connection

from ..db import get_db_conn
from ..services.repository import DataRepository

router = APIRouter(prefix="", tags=["facilities"])


@router.get("/facilities")
def get_facilities(
    facility_type: str | None = Query(default=None),
    start_year_min: int | None = Query(default=None, ge=1800, le=2100),
    start_year_max: int | None = Query(default=None, ge=1800, le=2100),
    status: str | None = Query(default=None),
    admin_city: str | None = Query(default=None),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=500, ge=1, le=5000),
    conn: Connection = Depends(get_db_conn),
) -> dict:
    offset = (page - 1) * page_size
    repo = DataRepository(conn)
    data = repo.list_facilities(
        facility_type=facility_type,
        start_year_min=start_year_min,
        start_year_max=start_year_max,
        status=status,
        admin_city=admin_city,
        limit=page_size,
        offset=offset,
    )
    data["page"] = page
    data["page_size"] = page_size
    return data


@router.get("/facilities/{facility_id}")
def get_facility_detail(facility_id: str, conn: Connection = Depends(get_db_conn)) -> dict:
    repo = DataRepository(conn)
    detail = repo.get_facility_detail(facility_id)
    if detail is None:
        raise HTTPException(status_code=404, detail="未找到该设施")
    return detail
