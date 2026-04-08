from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import requests
from dateutil import parser as dt_parser

from ...config import Settings
from .base import BaseConnector, ConnectorFetchResult, NormalizedEvent


def _severity_from_magnitude(magnitude: float) -> str:
    if magnitude >= 6:
        return "high"
    if magnitude >= 4:
        return "medium"
    return "low"


class UsgsEarthquakeConnector(BaseConnector):
    name = "usgs_earthquake"

    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    def fetch(self, session: requests.Session) -> ConnectorFetchResult:
        response = session.get(self.settings.usgs_feed_url, timeout=self.settings.connector_timeout_sec)
        response.raise_for_status()
        payload = response.json()

        records: list[NormalizedEvent] = []
        for feature in payload.get("features") or []:
            properties = feature.get("properties") or {}
            geometry = feature.get("geometry") or {}
            coordinates = geometry.get("coordinates") or []
            longitude = float(coordinates[0]) if len(coordinates) >= 2 else None
            latitude = float(coordinates[1]) if len(coordinates) >= 2 else None

            event_id = str(feature.get("id") or "")
            if not event_id:
                continue

            event_time: datetime
            if isinstance(properties.get("time"), (int, float)):
                event_time = datetime.fromtimestamp(float(properties["time"]) / 1000, tz=UTC)
            else:
                event_time = dt_parser.parse(str(properties.get("updated") or datetime.now(tz=UTC).isoformat()))

            magnitude = float(properties.get("mag") or 0.0)
            title = str(properties.get("title") or "Earthquake")
            description = str(properties.get("place") or "")

            records.append(
                NormalizedEvent(
                    source=self.name,
                    external_id=event_id,
                    event_type="earthquake",
                    severity=_severity_from_magnitude(magnitude),
                    title=title,
                    description=description,
                    event_time=event_time,
                    longitude=longitude,
                    latitude=latitude,
                    properties={
                        "magnitude": magnitude,
                        "place": properties.get("place"),
                        "url": properties.get("url"),
                    },
                    raw_payload=feature if isinstance(feature, dict) else {"value": feature},
                )
            )

        return ConnectorFetchResult(connector=self.name, records=records)

