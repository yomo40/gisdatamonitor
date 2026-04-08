from .events import router as events_router
from .facilities import router as facilities_router
from .layers import router as layers_router
from .map import router as map_router
from .risk import router as risk_router
from .scenes import router as scenes_router
from .system import router as system_router
from .timeline import router as timeline_router

__all__ = [
    "events_router",
    "facilities_router",
    "layers_router",
    "map_router",
    "risk_router",
    "scenes_router",
    "system_router",
    "timeline_router",
]
