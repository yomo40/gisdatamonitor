from __future__ import annotations

import logging

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger

from ..config import Settings
from .sync import EventSyncService

logger = logging.getLogger(__name__)


class SyncScheduler:
    def __init__(self, sync_service: EventSyncService, settings: Settings) -> None:
        self.sync_service = sync_service
        self.settings = settings
        self.scheduler = BackgroundScheduler(timezone=settings.timezone)
        self._started = False

    def start(self) -> None:
        if self._started:
            return
        self.scheduler.add_job(
            self.sync_service.run_cycle,
            trigger=IntervalTrigger(minutes=self.settings.sync_interval_minutes),
            id="event_sync",
            replace_existing=True,
            max_instances=1,
            coalesce=True,
        )
        self.scheduler.start()
        self._started = True
        logger.info("sync scheduler started: interval=%s min", self.settings.sync_interval_minutes)

    def shutdown(self) -> None:
        if not self._started:
            return
        self.scheduler.shutdown(wait=False)
        self._started = False
        logger.info("sync scheduler stopped")

