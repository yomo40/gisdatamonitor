from __future__ import annotations

from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.engine import Connection

from ..db import get_db_conn
from ..services.repository import DataRepository

router = APIRouter(prefix="/timeline", tags=["timeline"])


@router.get("/playback")
def get_timeline_playback(
    scene_id: str = Query(default="world"),
    window: str = Query(default="24h", pattern="^(24h|7d|30d)$"),
    step_minutes: int = Query(default=30, ge=1, le=24 * 60),
    end_time: datetime | None = Query(default=None),
    frame_limit: int = Query(default=240, ge=1, le=720),
    conn: Connection = Depends(get_db_conn),
) -> dict:
    repo = DataRepository(conn)
    try:
        return repo.timeline_playback(
            scene_id=scene_id,
            window=window,
            step_minutes=step_minutes,
            end_time=end_time,
            frame_limit=frame_limit,
        )
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

