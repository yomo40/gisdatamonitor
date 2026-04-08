from __future__ import annotations

from fastapi import APIRouter, Depends
from sqlalchemy.engine import Connection

from ..db import get_db_conn
from ..services.repository import DataRepository

router = APIRouter(prefix="", tags=["layers"])


@router.get("/layers")
def get_layers(conn: Connection = Depends(get_db_conn)) -> dict:
    repo = DataRepository(conn)
    return repo.list_layers()

