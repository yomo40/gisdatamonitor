from __future__ import annotations

import csv
import io
from datetime import UTC, datetime

import requests
from dateutil import parser as dt_parser

from ...config import Settings
from .base import BaseConnector, ConnectorFetchResult, NormalizedEvent


def _severity_from_brightness(brightness: float) -> str:
    if brightness >= 400:
        return "high"
    if brightness >= 340:
        return "medium"
    return "low"


class NasaFirmsConnector(BaseConnector):
    name = "nasa_firms"

    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    def fetch(self, session: requests.Session) -> ConnectorFetchResult:
        if not self.settings.firms_api_key:
            return ConnectorFetchResult(
                connector=self.name,
                records=[],
                skipped=True,
                skip_reason="未配置 FIRMS API Key",
            )

        url = self.settings.firms_feed_template.format(
            api_key=self.settings.firms_api_key,
            bbox=self.settings.jiangxi_bbox_csv,
        )
        response = session.get(url, timeout=self.settings.connector_timeout_sec)
        response.raise_for_status()

        rows = list(csv.DictReader(io.StringIO(response.text)))
        records: list[NormalizedEvent] = []
        for row in rows:
            latitude = float(row.get("latitude") or 0.0)
            longitude = float(row.get("longitude") or 0.0)
            if latitude == 0.0 and longitude == 0.0:
                continue

            acq_date = str(row.get("acq_date") or "")
            acq_time = str(row.get("acq_time") or "0000").zfill(4)
            event_time = dt_parser.parse(f"{acq_date} {acq_time[:2]}:{acq_time[2:]}", default=datetime.now(tz=UTC))
            brightness = float(row.get("bright_ti4") or row.get("brightness") or 0.0)
            confidence = row.get("confidence")
            event_id = f"{row.get('satellite','sat')}-{acq_date}-{acq_time}-{latitude:.4f}-{longitude:.4f}"

            records.append(
                NormalizedEvent(
                    source=self.name,
                    external_id=event_id,
                    event_type="fire_hotspot",
                    severity=_severity_from_brightness(brightness),
                    title="活动火点",
                    description=f"FIRMS 火点置信度={confidence}",
                    event_time=event_time,
                    longitude=longitude,
                    latitude=latitude,
                    properties={
                        "instrument": row.get("instrument"),
                        "satellite": row.get("satellite"),
                        "confidence": confidence,
                        "brightness": brightness,
                        "frp": row.get("frp"),
                    },
                    raw_payload=dict(row),
                )
            )

        return ConnectorFetchResult(connector=self.name, records=records)
