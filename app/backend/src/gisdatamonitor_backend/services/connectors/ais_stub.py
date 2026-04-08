from __future__ import annotations

import requests

from ...config import Settings
from .base import BaseConnector, ConnectorFetchResult, NormalizedEvent


class AisPortStubConnector(BaseConnector):
    name = "ais_port_stub"

    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    def fetch(self, session: requests.Session) -> ConnectorFetchResult:
        if not self.settings.ais_feed_url and not self.settings.port_feed_url:
            return ConnectorFetchResult(
                connector=self.name,
                records=[],
                skipped=True,
                skip_reason="未配置 AIS/港口连接地址",
            )

        # Keep API contract stable in MVP even when no credentialed feed is enabled.
        return ConnectorFetchResult(connector=self.name, records=[])
