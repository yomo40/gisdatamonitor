from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

import feedparser
import requests
from dateutil import parser as dt_parser

from ...config import Settings
from .base import BaseConnector, ConnectorFetchResult, NormalizedEvent

ANNOUNCEMENT_KEYWORDS = (
    "energy",
    "oil",
    "gas",
    "electric",
    "electricity",
    "power",
    "grid",
    "refinery",
    "lng",
    "coal",
    "nuclear",
    "renewable",
    "solar",
    "wind",
    "storage",
    "hydrogen",
    "pipeline",
    "outage",
    "pricing",
    "supply",
    "demand",
)
HIGH_RISK_KEYWORDS = ("blackout", "shutdown", "explosion", "emergency", "outage", "fire", "disruption")
MEDIUM_RISK_KEYWORDS = ("warning", "alert", "maintenance", "shortage", "volatility", "sanction")


def _severity_by_change(change_pct: float) -> str:
    change_abs = abs(change_pct)
    if change_abs >= 4.0:
        return "high"
    if change_abs >= 2.0:
        return "medium"
    return "low"


def _parse_feed_time(raw_value: str) -> datetime:
    tzinfos = {
        "UTC": 0,
        "GMT": 0,
        "EST": -5 * 3600,
        "EDT": -4 * 3600,
        "CST": 8 * 3600,
    }
    dt = dt_parser.parse(raw_value, tzinfos=tzinfos)
    return dt if dt.tzinfo else dt.replace(tzinfo=UTC)


def _is_relevant_announcement(title: str, summary: str) -> bool:
    haystack = f"{title} {summary}".lower()
    return any(keyword in haystack for keyword in ANNOUNCEMENT_KEYWORDS)


def _severity_from_announcement(title: str, summary: str) -> str:
    haystack = f"{title} {summary}".lower()
    if any(keyword in haystack for keyword in HIGH_RISK_KEYWORDS):
        return "high"
    if any(keyword in haystack for keyword in MEDIUM_RISK_KEYWORDS):
        return "medium"
    return "low"


class EnergyMarketConnector(BaseConnector):
    name = "energy_market"

    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    def _fetch_symbol_event(self, session: requests.Session, symbol: str, label: str) -> NormalizedEvent | None:
        response = session.get(
            f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}",
            params={"interval": "5m", "range": "1d"},
            timeout=self.settings.connector_timeout_sec,
        )
        if response.status_code >= 400:
            return None

        payload = response.json()
        chart = payload.get("chart") or {}
        result = (chart.get("result") or [None])[0] or {}
        timestamps = result.get("timestamp") or []
        quote = ((result.get("indicators") or {}).get("quote") or [None])[0] or {}
        closes = quote.get("close") or []
        opens = quote.get("open") or []
        if not timestamps or not closes:
            return None

        last_ts = int(timestamps[-1])
        close_val = float(closes[-1] or 0.0)
        open_val = float(opens[0] or close_val or 1.0)
        change_pct = ((close_val - open_val) / open_val) * 100 if open_val else 0.0
        event_time = datetime.fromtimestamp(last_ts, tz=UTC)

        return NormalizedEvent(
            source=self.name,
            external_id=f"{symbol}:{last_ts}",
            event_type="energy_price",
            severity=_severity_by_change(change_pct),
            title=f"{label} 价格更新",
            description=f"{label}={close_val:.2f}，日内变动 {change_pct:.2f}%",
            event_time=event_time,
            longitude=None,
            latitude=None,
            properties={
                "symbol": symbol,
                "price": close_val,
                "change_pct": change_pct,
            },
            raw_payload=payload if isinstance(payload, dict) else {"value": payload},
        )

    def fetch(self, session: requests.Session) -> ConnectorFetchResult:
        records: list[NormalizedEvent] = []
        for symbol, label in (("CL=F", "WTI"), ("BZ=F", "Brent"), ("NG=F", "Henry Hub"), ("RB=F", "RBOB")):
            event = self._fetch_symbol_event(session, symbol=symbol, label=label)
            if event is not None:
                records.append(event)
        return ConnectorFetchResult(connector=self.name, records=records)


class EnergyAnnouncementConnector(BaseConnector):
    name = "energy_announcement"

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.feeds = [
            "https://www.eia.gov/rss/press_rss.xml",
            "https://www.iea.org/news.rss",
        ]

    def fetch(self, session: requests.Session) -> ConnectorFetchResult:
        records: list[NormalizedEvent] = []
        dedupe_ids: set[str] = set()
        window_start = datetime.now(tz=UTC) - timedelta(days=30)
        for feed_url in self.feeds:
            parsed = feedparser.parse(feed_url)
            entries = parsed.entries if isinstance(parsed.entries, list) else []
            for entry in entries[:120]:
                title = str(getattr(entry, "title", "能源公告"))
                link = str(getattr(entry, "link", ""))
                published = str(
                    getattr(entry, "published", "")
                    or getattr(entry, "updated", "")
                    or datetime.now(tz=UTC).isoformat()
                )
                try:
                    event_time = _parse_feed_time(published)
                except (ValueError, TypeError, OverflowError):
                    event_time = datetime.now(tz=UTC)
                if event_time < window_start:
                    continue
                summary = str(getattr(entry, "summary", ""))[:400]
                if not _is_relevant_announcement(title, summary):
                    continue
                external_id = link or f"{feed_url}:{event_time.isoformat()}:{title}"
                if external_id in dedupe_ids:
                    continue
                dedupe_ids.add(external_id)
                severity = _severity_from_announcement(title, summary)
                records.append(
                    NormalizedEvent(
                        source=self.name,
                        external_id=external_id,
                        event_type="industry_announcement",
                        severity=severity,
                        title=title,
                        description=summary,
                        event_time=event_time,
                        longitude=None,
                        latitude=None,
                        properties={"feed_url": feed_url, "link": link},
                        raw_payload={"title": title, "link": link, "summary": summary},
                    )
                )

        return ConnectorFetchResult(connector=self.name, records=records)
