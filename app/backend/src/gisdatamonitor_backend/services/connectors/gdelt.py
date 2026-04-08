from __future__ import annotations

import hashlib
from datetime import UTC, datetime, timedelta
from typing import Any, Iterable

import requests
from dateutil import parser as dt_parser

from ...config import Settings
from .base import BaseConnector, ConnectorFetchResult, NormalizedEvent

GDELT_API_URL = "https://api.gdeltproject.org/api/v2/doc/doc"
GDELT_FALLBACK_QUERY = "(earthquake OR wildfire OR flood OR power OR refinery) AND (china OR jiangxi)"
CHINA_CITY_CENTERS: tuple[tuple[str, float, float], ...] = (
    ("beijing", 116.4074, 39.9042),
    ("shanghai", 121.4737, 31.2304),
    ("guangdong", 113.2644, 23.1291),
    ("jiangxi", 115.8582, 28.6829),
    ("sichuan", 104.0665, 30.5728),
    ("hubei", 114.3054, 30.5931),
    ("zhejiang", 120.1551, 30.2741),
    ("shandong", 117.0009, 36.6758),
    ("henan", 113.6254, 34.7466),
    ("fujian", 119.2965, 26.0745),
    ("xinjiang", 87.6177, 43.7928),
    ("neimenggu", 111.7492, 40.8426),
    ("yunnan", 102.8329, 24.8801),
)


def _infer_event_type(text: str) -> str:
    lower = text.lower()
    if "earthquake" in lower:
        return "earthquake_news"
    if "wildfire" in lower or "fire" in lower:
        return "wildfire_news"
    if "flood" in lower:
        return "flood_news"
    if "oil" in lower or "gas" in lower or "energy" in lower or "power" in lower:
        return "energy_news"
    return "gdelt_news"


def _infer_severity(text: str) -> str:
    lower = text.lower()
    high_terms = ("explosion", "disaster", "emergency", "shutdown", "blackout", "major")
    medium_terms = ("warning", "alert", "incident", "disruption", "outage")
    if any(term in lower for term in high_terms):
        return "high"
    if any(term in lower for term in medium_terms):
        return "medium"
    return "low"


def _pick_coordinates(article: dict[str, Any]) -> tuple[float | None, float | None]:
    locations = article.get("locations")
    if not isinstance(locations, list):
        return None, None
    for location in locations:
        if not isinstance(location, dict):
            continue
        lat = location.get("lat")
        lon = location.get("lon")
        if isinstance(lat, (int, float)) and isinstance(lon, (int, float)):
            return float(lon), float(lat)
    return None, None


def _parse_event_time(value: Any) -> datetime:
    text_value = str(value or "").strip()
    if not text_value:
        return datetime.now(tz=UTC)
    try:
        parsed = dt_parser.parse(text_value)
    except (ValueError, TypeError, OverflowError):
        return datetime.now(tz=UTC)
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def _seed_ratio(seed: str, salt: str) -> float:
    digest = hashlib.sha1(f"{seed}:{salt}".encode("utf-8")).digest()
    return int.from_bytes(digest[:4], byteorder="big", signed=False) / 0xFFFFFFFF


def _infer_synthetic_coordinates(article: dict[str, Any], external_id: str) -> tuple[float | None, float | None]:
    source_country = str(article.get("sourcecountry") or "").lower()
    if "china" not in source_country and "cn" not in source_country:
        return None, None

    title_raw = str(article.get("title") or "")
    title_lower = title_raw.lower()
    city_idx = 0
    for idx, (keyword, _, _) in enumerate(CHINA_CITY_CENTERS):
        if keyword in title_lower or keyword in title_raw:
            city_idx = idx
            break
    else:
        city_idx = int(_seed_ratio(external_id, "city") * len(CHINA_CITY_CENTERS)) % len(CHINA_CITY_CENTERS)

    _, base_lon, base_lat = CHINA_CITY_CENTERS[city_idx]
    lon_jitter = (_seed_ratio(external_id, "lon") - 0.5) * 1.2
    lat_jitter = (_seed_ratio(external_id, "lat") - 0.5) * 0.9
    return base_lon + lon_jitter, base_lat + lat_jitter


def _records_span_hours(records: Iterable[NormalizedEvent]) -> float:
    event_times = sorted((record.event_time for record in records), reverse=False)
    if len(event_times) < 2:
        return 0.0
    return max(0.0, (event_times[-1] - event_times[0]).total_seconds() / 3600.0)


class GdeltEventsConnector(BaseConnector):
    name = "gdelt_events"

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self._rate_limited_until: datetime | None = None
        self._last_wide_backfill_at: datetime | None = None

    def _in_cooldown(self) -> bool:
        until = self._rate_limited_until
        return isinstance(until, datetime) and datetime.now(tz=UTC) < until

    def _cooldown_seconds_left(self) -> int:
        if not self._in_cooldown():
            return 0
        assert self._rate_limited_until is not None
        seconds = int((self._rate_limited_until - datetime.now(tz=UTC)).total_seconds())
        return max(0, seconds)

    def _set_rate_limit_cooldown(self) -> None:
        minutes = max(3, int(self.settings.gdelt_rate_limit_cooldown_minutes))
        self._rate_limited_until = datetime.now(tz=UTC) + timedelta(minutes=minutes)

    def _request_articles(
        self,
        session: requests.Session,
        *,
        query: str,
        max_records: int,
        timeout_sec: int,
        sort: str = "DateDesc",
        timespan: str | None = None,
        start_datetime: datetime | None = None,
        end_datetime: datetime | None = None,
    ) -> tuple[list[dict[str, Any]] | None, str | None]:
        params = {
            "query": query,
            "mode": "ArtList",
            "format": "json",
            "maxrecords": max(20, min(int(max_records), 250)),
            "sort": sort,
        }
        effective_timespan = str(timespan or self.settings.gdelt_timespan).strip()
        if isinstance(start_datetime, datetime):
            params["startdatetime"] = start_datetime.astimezone(UTC).strftime("%Y%m%d%H%M%S")
        if isinstance(end_datetime, datetime):
            params["enddatetime"] = end_datetime.astimezone(UTC).strftime("%Y%m%d%H%M%S")
        if "startdatetime" not in params and "enddatetime" not in params and effective_timespan:
            params["timespan"] = effective_timespan
        response = session.get(GDELT_API_URL, params=params, timeout=max(3, int(timeout_sec)))

        if response.status_code == 429:
            self._set_rate_limit_cooldown()
            return None, "GDELT 限流（HTTP 429）"
        if response.status_code >= 500:
            return None, f"GDELT 上游异常（HTTP {response.status_code}）"
        if response.status_code >= 400:
            return None, f"GDELT 请求失败（HTTP {response.status_code}）"

        text_preview = (response.text or "").strip()
        if "Invalid/Unsupported Country" in text_preview:
            return None, "GDELT 返回国家参数错误，已跳过本轮"

        try:
            payload = response.json()
        except ValueError:
            if text_preview.startswith("<"):
                return None, "GDELT 返回 HTML 非 JSON"
            return None, "GDELT 返回非 JSON 内容"

        articles = payload.get("articles")
        if not isinstance(articles, list):
            return [], None
        return articles, None

    def _articles_to_records(self, articles: list[dict[str, Any]]) -> list[NormalizedEvent]:
        records: list[NormalizedEvent] = []
        seen_external_ids: set[str] = set()

        for article in articles:
            if not isinstance(article, dict):
                continue
            external_id = str(article.get("url") or article.get("id") or "").strip()
            if not external_id or external_id in seen_external_ids:
                continue
            seen_external_ids.add(external_id)

            title = str(article.get("title") or "GDELT 事件").strip() or "GDELT 事件"
            event_time = _parse_event_time(article.get("seendate"))
            lon, lat = _pick_coordinates(article)
            synthetic_geo = False
            if lon is None or lat is None:
                lon, lat = _infer_synthetic_coordinates(article, external_id)
                synthetic_geo = lon is not None and lat is not None
            description_parts = [
                str(article.get("sourcecountry") or "").strip(),
                str(article.get("domain") or "").strip(),
            ]
            summary = " | ".join(part for part in description_parts if part)

            records.append(
                NormalizedEvent(
                    source=self.name,
                    external_id=external_id,
                    event_type=_infer_event_type(title),
                    severity=_infer_severity(title),
                    title=title,
                    description=summary,
                    event_time=event_time,
                    longitude=lon,
                    latitude=lat,
                    properties={
                        "sourcecountry": article.get("sourcecountry"),
                        "domain": article.get("domain"),
                        "language": article.get("language"),
                        "url": article.get("url"),
                        "synthetic_geo": synthetic_geo,
                    },
                    raw_payload=article,
                )
            )
        return records

    def _merge_records(self, *chunks: list[NormalizedEvent]) -> list[NormalizedEvent]:
        merged: dict[str, NormalizedEvent] = {}
        for chunk in chunks:
            for record in chunk:
                merged[record.external_id] = record
        return sorted(merged.values(), key=lambda item: item.event_time)

    def _should_run_wide_backfill(self) -> bool:
        if self._last_wide_backfill_at is None:
            return True
        elapsed = datetime.now(tz=UTC) - self._last_wide_backfill_at
        return elapsed >= timedelta(hours=2)

    def fetch(self, session: requests.Session) -> ConnectorFetchResult:
        if not self.settings.gdelt_enabled:
            return ConnectorFetchResult(
                connector=self.name,
                records=[],
                skipped=True,
                skip_reason="GDELT 连接器已关闭",
            )

        if self._in_cooldown():
            return ConnectorFetchResult(
                connector=self.name,
                records=[],
                skipped=True,
                skip_reason=f"GDELT 限流冷却中，剩余 {self._cooldown_seconds_left()} 秒",
            )

        primary_timeout = max(4, int(self.settings.gdelt_timeout_sec or self.settings.connector_timeout_sec))
        primary_max = max(30, int(self.settings.gdelt_max_records))
        fallback_timeout = max(4, primary_timeout - 2)
        fallback_max = min(primary_max, max(30, int(self.settings.gdelt_fallback_max_records)))

        attempts = [
            {
                "query": str(self.settings.gdelt_query).strip() or GDELT_FALLBACK_QUERY,
                "timeout": primary_timeout,
                "max_records": primary_max,
            },
            {
                "query": GDELT_FALLBACK_QUERY,
                "timeout": fallback_timeout,
                "max_records": fallback_max,
            },
        ]

        last_reason: str | None = None
        merged_records: list[NormalizedEvent] = []
        for spec in attempts:
            try:
                articles, reason = self._request_articles(
                    session,
                    query=spec["query"],
                    max_records=int(spec["max_records"]),
                    timeout_sec=int(spec["timeout"]),
                    sort="DateDesc",
                    timespan=self.settings.gdelt_timespan,
                )
            except requests.Timeout:
                last_reason = f"GDELT 请求超时（{spec['timeout']}s）"
                break
            except requests.RequestException as exc:
                last_reason = f"GDELT 请求失败（{type(exc).__name__}）"
                if isinstance(exc, requests.ConnectionError):
                    break
                continue

            if reason:
                last_reason = reason
                if "429" in reason:
                    break
                continue

            if articles is None:
                continue

            desc_records = self._articles_to_records(articles)
            merged_records = self._merge_records(merged_records, desc_records)

            # 同一轮追加一次 DateAsc，请求更早时间片，避免时间分布过度集中在最近几小时。
            span_hours = _records_span_hours(merged_records)
            if span_hours < 18:
                try:
                    asc_articles, asc_reason = self._request_articles(
                        session,
                        query=spec["query"],
                        max_records=int(spec["max_records"]),
                        timeout_sec=int(spec["timeout"]),
                        sort="DateAsc",
                        timespan=self.settings.gdelt_timespan,
                    )
                except requests.Timeout:
                    asc_articles, asc_reason = None, None
                except requests.RequestException:
                    asc_articles, asc_reason = None, None

                if asc_articles:
                    asc_records = self._articles_to_records(asc_articles)
                    merged_records = self._merge_records(merged_records, asc_records)
                elif asc_reason:
                    last_reason = asc_reason

            # 默认 timespan=1day 时，定期执行 7days 宽窗补拉，提升 7d 回放覆盖度。
            current_timespan = str(self.settings.gdelt_timespan or "").lower()
            need_wide = current_timespan in {"1day", "24h", "1d"} and self._should_run_wide_backfill()
            if need_wide:
                self._last_wide_backfill_at = datetime.now(tz=UTC)
                for sort in ("DateDesc", "DateAsc"):
                    try:
                        wide_articles, wide_reason = self._request_articles(
                            session,
                            query=spec["query"],
                            max_records=max(int(spec["max_records"]), fallback_max),
                            timeout_sec=int(spec["timeout"]),
                            sort=sort,
                            timespan="7days",
                        )
                    except requests.Timeout:
                        wide_articles, wide_reason = None, None
                    except requests.RequestException:
                        wide_articles, wide_reason = None, None

                    if wide_articles:
                        wide_records = self._articles_to_records(wide_articles)
                        merged_records = self._merge_records(merged_records, wide_records)
                    elif wide_reason and last_reason is None:
                        last_reason = wide_reason

            if merged_records:
                return ConnectorFetchResult(connector=self.name, records=merged_records)

        return ConnectorFetchResult(
            connector=self.name,
            records=[],
            skipped=True,
            skip_reason=last_reason or "GDELT 本轮无可用数据",
        )
