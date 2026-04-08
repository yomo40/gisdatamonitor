from __future__ import annotations

import logging
import os
import sys
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from .config import get_settings
from .db import get_engine, ping_database
from .routers import (
    events_router,
    facilities_router,
    layers_router,
    map_router,
    risk_router,
    scenes_router,
    system_router,
    timeline_router,
)
from .services.scheduler import SyncScheduler
from .services.sync import EventSyncService

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

settings = get_settings()
engine = get_engine()
sync_service = EventSyncService(engine=engine, settings=settings)
scheduler = SyncScheduler(sync_service=sync_service, settings=settings)

def _resolve_runtime_root() -> Path:
    runtime_root = os.environ.get("GISDATAMONITOR_RUNTIME_ROOT", "").strip()
    if runtime_root:
        return Path(runtime_root).resolve()
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parents[3]


APP_DIR = _resolve_runtime_root()
frontend_env = os.environ.get("GISDATAMONITOR_FRONTEND_DIR", "").strip()
if frontend_env:
    FRONTEND_DIR = Path(frontend_env).resolve()
else:
    candidate = APP_DIR / "app" / "frontend"
    FRONTEND_DIR = candidate if candidate.exists() else APP_DIR / "frontend"
ASSETS_DIR = FRONTEND_DIR / "assets"


@asynccontextmanager
async def lifespan(app: FastAPI):
    ping = ping_database()
    logger.info("database ready: %s", ping)
    if settings.scheduler_enabled:
        scheduler.start()
        if settings.sync_run_on_startup:
            logger.info("running startup sync cycle (blocking mode)")
            try:
                startup_summary = sync_service.run_cycle(startup_mode=True)
                logger.info(
                    "startup sync cycle completed: elapsed=%.2fs connectors=%s",
                    float(startup_summary.get("elapsed_sec") or 0.0),
                    len(startup_summary.get("connectors") or []),
                )
            except Exception:  # noqa: BLE001
                logger.exception("startup sync cycle failed, application will continue running")
    try:
        yield
    finally:
        scheduler.shutdown()


app = FastAPI(
    title=settings.app_name,
    version="0.1.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[settings.frontend_origin] if settings.frontend_origin != "*" else ["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

if ASSETS_DIR.exists():
    app.mount("/assets", StaticFiles(directory=ASSETS_DIR), name="assets")


@app.get("/", include_in_schema=False)
def index() -> FileResponse:
    return FileResponse(FRONTEND_DIR / "index.html")


@app.get("/leaflet", include_in_schema=False)
def leaflet() -> FileResponse:
    return FileResponse(FRONTEND_DIR / "leaflet.html")


@app.get("/monitor", include_in_schema=False)
def monitor() -> FileResponse:
    return FileResponse(FRONTEND_DIR / "monitor.html")


@app.get("/autoplay", include_in_schema=False)
def autoplay() -> FileResponse:
    return FileResponse(FRONTEND_DIR / "index.html")


@app.get("/ping", include_in_schema=False)
def ping() -> dict:
    return {"status": "ok"}


app.include_router(layers_router, prefix=settings.api_prefix)
app.include_router(facilities_router, prefix=settings.api_prefix)
app.include_router(events_router, prefix=settings.api_prefix)
app.include_router(risk_router, prefix=settings.api_prefix)
app.include_router(system_router, prefix=settings.api_prefix)
app.include_router(map_router, prefix=settings.api_prefix)
app.include_router(scenes_router, prefix=settings.api_prefix)
app.include_router(timeline_router, prefix=settings.api_prefix)
