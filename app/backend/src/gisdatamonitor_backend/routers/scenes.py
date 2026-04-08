from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.engine import Connection

from ..db import get_db_conn
from ..services.repository import DataRepository

router = APIRouter(prefix="", tags=["scenes"])


@router.get("/scenes")
def get_scenes(conn: Connection = Depends(get_db_conn)) -> dict:
    repo = DataRepository(conn)
    return repo.list_scenes()


@router.get("/scenes/{scene_id}/state")
def get_scene_state(scene_id: str, conn: Connection = Depends(get_db_conn)) -> dict:
    repo = DataRepository(conn)
    payload = repo.scene_state(scene_id)
    if payload is None:
        raise HTTPException(status_code=404, detail="未找到该场景")
    return payload
