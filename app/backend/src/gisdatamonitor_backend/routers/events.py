from __future__ import annotations

from datetime import datetime

from fastapi import APIRouter, Depends, Query
from sqlalchemy.engine import Connection

from ..db import get_db_conn
from ..services.repository import DataRepository

router = APIRouter(prefix="", tags=["events"])


@router.get("/events")
def get_events(
    source: str | None = Query(default=None),
    event_type: str | None = Query(default=None),
    severity: str | None = Query(default=None, pattern="^(low|medium|high)$"),
    hours: int = Query(default=24, ge=1, le=24 * 30),
    since: datetime | None = Query(default=None),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=500, ge=1, le=5000),
    conn: Connection = Depends(get_db_conn),
) -> dict:
    offset = (page - 1) * page_size
    repo = DataRepository(conn)
    data = repo.list_events(
        source=source,
        event_type=event_type,
        severity=severity,
        hours=hours,
        since=since,
        limit=page_size,
        offset=offset,
    )
    data["page"] = page
    data["page_size"] = page_size
    return data


@router.get("/events/enriched")
def get_events_enriched(
    source: str | None = Query(default=None),
    event_type: str | None = Query(default=None),
    severity: str | None = Query(default=None, pattern="^(low|medium|high)$"),
    risk_level: str | None = Query(default=None, pattern="^(low|medium|high|critical)$"),
    hours: int = Query(default=24, ge=1, le=24 * 30),
    since: datetime | None = Query(default=None),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=500, ge=1, le=5000),
    conn: Connection = Depends(get_db_conn),
) -> dict:
    offset = (page - 1) * page_size
    repo = DataRepository(conn)
    data = repo.list_events_enriched(
        source=source,
        event_type=event_type,
        severity=severity,
        risk_level=risk_level,
        hours=hours,
        since=since,
        limit=page_size,
        offset=offset,
    )
    data["page"] = page
    data["page_size"] = page_size
    return data
