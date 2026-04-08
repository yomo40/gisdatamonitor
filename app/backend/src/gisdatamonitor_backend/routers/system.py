from __future__ import annotations

from datetime import UTC, datetime

from fastapi import APIRouter, Depends, Query
from sqlalchemy.engine import Connection

from ..db import get_db_conn
from ..services.repository import DataRepository

router = APIRouter(prefix="/system", tags=["system"])


@router.get("/health")
def get_system_health(conn: Connection = Depends(get_db_conn)) -> dict:
    repo = DataRepository(conn)
    payload = repo.system_health()
    payload["timestamp"] = datetime.now(tz=UTC).isoformat()
    return payload


@router.get("/monitor")
def get_system_monitor(
    hours: int = Query(default=24, ge=1, le=24 * 30),
    conn: Connection = Depends(get_db_conn),
) -> dict:
    repo = DataRepository(conn)
    payload = repo.system_monitor(hours=hours)
    payload["timestamp"] = datetime.now(tz=UTC).isoformat()
    return payload
