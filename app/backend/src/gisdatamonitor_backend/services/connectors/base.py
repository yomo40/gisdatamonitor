from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

import requests


@dataclass(slots=True)
class NormalizedEvent:
    source: str
    external_id: str
    event_type: str
    severity: str
    title: str
    description: str
    event_time: datetime
    longitude: float | None
    latitude: float | None
    properties: dict[str, Any] = field(default_factory=dict)
    raw_payload: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class ConnectorFetchResult:
    connector: str
    records: list[NormalizedEvent]
    skipped: bool = False
    skip_reason: str | None = None


class BaseConnector:
    name: str = "base"

    def fetch(self, session: requests.Session) -> ConnectorFetchResult:
        raise NotImplementedError

