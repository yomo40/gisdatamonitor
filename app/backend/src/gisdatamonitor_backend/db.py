from __future__ import annotations

from collections.abc import Generator
from pathlib import Path

from sqlalchemy import create_engine, event, text
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.pool import NullPool

from .config import get_settings


_engine: Engine | None = None


def _resolve_sqlite_database_url(database_url: str) -> str:
    prefix = "sqlite:///"
    if not database_url.startswith(prefix):
        return database_url
    sqlite_path = database_url[len(prefix) :]
    if not sqlite_path or sqlite_path == ":memory:":
        return database_url
    path_obj = Path(sqlite_path)
    if path_obj.is_absolute():
        return database_url
    backend_dir = Path(__file__).resolve().parents[2]
    resolved = (backend_dir / path_obj).resolve()
    return f"{prefix}{resolved.as_posix()}"


def _configure_sqlite_connection(dbapi_connection: object, _connection_record: object) -> None:
    cursor = dbapi_connection.cursor()
    try:
        cursor.execute("PRAGMA journal_mode=WAL")
        cursor.execute("PRAGMA synchronous=NORMAL")
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.execute("PRAGMA busy_timeout=60000")
    finally:
        cursor.close()


def get_engine() -> Engine:
    global _engine
    if _engine is None:
        settings = get_settings()
        if settings.database_backend == "sqlite":
            sqlite_url = _resolve_sqlite_database_url(settings.database_url)
            _engine = create_engine(
                sqlite_url,
                connect_args={
                    "check_same_thread": False,
                    "timeout": 60,
                },
                poolclass=NullPool,
                future=True,
            )
            event.listen(_engine, "connect", _configure_sqlite_connection)
        else:
            _engine = create_engine(settings.database_url, pool_pre_ping=True, future=True)
    return _engine


def get_db_conn() -> Generator[Connection, None, None]:
    engine = get_engine()
    with engine.connect() as conn:
        yield conn


def ping_database() -> dict[str, str]:
    engine = get_engine()
    settings = get_settings()
    with engine.connect() as conn:
        if settings.database_backend == "sqlite":
            version = conn.execute(text("SELECT sqlite_version()")).scalar_one()
            return {"sqlite": str(version)}
        version = conn.execute(text("SELECT version()")).scalar_one()
        postgis = conn.execute(text("SELECT PostGIS_Full_Version()")).scalar_one()
        return {"postgres": str(version), "postgis": str(postgis)}
