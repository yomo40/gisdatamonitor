from __future__ import annotations

import argparse
import json
import sys
import time
from collections import Counter
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import requests
from sqlalchemy import text
from sqlalchemy.engine import Engine

sys.path.append(str(Path(__file__).resolve().parents[1] / "src"))

from gisdatamonitor_backend.config import get_settings  # noqa: E402
from gisdatamonitor_backend.db import get_engine  # noqa: E402
from gisdatamonitor_backend.services.connectors import (  # noqa: E402
    EnergyAnnouncementConnector,
    EnergyMarketConnector,
    GdeltEventsConnector,
    UsgsEarthquakeConnector,
)
from gisdatamonitor_backend.services.sync import EventSyncService  # noqa: E402


def _count_recent(engine: Engine, *, days: int) -> dict[str, int]:
    cutoff = datetime.now(tz=UTC) - timedelta(days=max(1, int(days)))
    with engine.connect() as conn:
        total = int(
            conn.execute(
                text(
                    """
                    SELECT COUNT(*)
                    FROM event_normalized
                    WHERE event_time >= :cutoff
                    """
                ),
                {"cutoff": cutoff.isoformat()},
            ).scalar_one()
        )
        geo = int(
            conn.execute(
                text(
                    """
                    SELECT COUNT(*)
                    FROM event_normalized
                    WHERE event_time >= :cutoff
                      AND geometry_json IS NOT NULL
                      AND TRIM(COALESCE(geometry_json, '')) <> ''
                    """
                ),
                {"cutoff": cutoff.isoformat()},
            ).scalar_one()
        )
    return {"total": total, "geo": geo}


def _daily_distribution(engine: Engine, *, days: int) -> list[dict[str, Any]]:
    cutoff = datetime.now(tz=UTC) - timedelta(days=max(1, int(days)))
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT substr(event_time, 1, 10) AS day_key, COUNT(*) AS total
                FROM event_normalized
                WHERE event_time >= :cutoff
                GROUP BY substr(event_time, 1, 10)
                ORDER BY day_key
                """
            ),
            {"cutoff": cutoff.isoformat()},
        ).mappings().all()
    return [{"day": row["day_key"], "count": int(row["total"])} for row in rows]


def _source_distribution(engine: Engine, *, days: int) -> list[dict[str, Any]]:
    cutoff = datetime.now(tz=UTC) - timedelta(days=max(1, int(days)))
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT source, COUNT(*) AS total
                FROM event_normalized
                WHERE event_time >= :cutoff
                GROUP BY source
                ORDER BY total DESC, source
                """
            ),
            {"cutoff": cutoff.isoformat()},
        ).mappings().all()
    return [{"source": str(row["source"]), "count": int(row["total"])} for row in rows]


def _persist_records(service: EventSyncService, records: list[Any], *, chunk_size: int = 2000) -> int:
    if not records:
        return 0
    inserted = 0
    start = 0
    while start < len(records):
        chunk = records[start : start + chunk_size]
        inserted += int(service._persist_events(chunk))  # noqa: SLF001
        start += chunk_size
    return inserted


def main() -> None:
    parser = argparse.ArgumentParser(description="回补过去 N 天历史事件，并刷新离线快照。")
    parser.add_argument("--days", type=int, default=30, help="回补天数，默认 30")
    parser.add_argument("--gdelt-bucket-hours", type=int, default=24, help="GDELT 分桶小时，默认 24")
    parser.add_argument("--gdelt-max-records", type=int, default=180, help="GDELT 单请求最大条数，默认 180")
    parser.add_argument("--gdelt-sleep-sec", type=float, default=0.25, help="GDELT 请求间隔秒，默认 0.25")
    parser.add_argument("--gdelt-date-asc", action="store_true", help="额外启用 GDELT DateAsc 二次补拉")
    args = parser.parse_args()

    days = max(1, int(args.days))
    bucket_hours = max(1, min(24, int(args.gdelt_bucket_hours)))
    gdelt_max_records = max(50, min(250, int(args.gdelt_max_records)))
    gdelt_sleep_sec = max(0.0, float(args.gdelt_sleep_sec))

    settings = get_settings()
    engine = get_engine()
    service = EventSyncService(engine=engine, settings=settings)
    now = datetime.now(tz=UTC)
    window_start = now - timedelta(days=days)

    session = service._build_session()  # noqa: SLF001
    summary: dict[str, Any] = {
        "status": "ok",
        "window_days": days,
        "started_at": now.isoformat(),
        "sources": {},
    }

    try:
        usgs_settings = settings.model_copy(
            update={
                "usgs_feed_url": "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_month.geojson",
            }
        )
        usgs_connector = UsgsEarthquakeConnector(usgs_settings)
        usgs_result = usgs_connector.fetch(session)
        usgs_records = [record for record in usgs_result.records if record.event_time >= window_start]
        summary["sources"]["usgs_earthquake"] = {
            "fetched": len(usgs_result.records),
            "window_filtered": len(usgs_records),
            "inserted": _persist_records(service, usgs_records),
        }

        market_connector = EnergyMarketConnector(settings)
        market_result = market_connector.fetch(session)
        summary["sources"]["energy_market"] = {
            "fetched": len(market_result.records),
            "inserted": _persist_records(service, market_result.records),
        }

        announcement_connector = EnergyAnnouncementConnector(settings)
        announcement_result = announcement_connector.fetch(session)
        announcement_records = [record for record in announcement_result.records if record.event_time >= window_start]
        summary["sources"]["energy_announcement"] = {
            "fetched": len(announcement_result.records),
            "window_filtered": len(announcement_records),
            "inserted": _persist_records(service, announcement_records),
        }

        gdelt_settings = settings.model_copy(
            update={
                "gdelt_timespan": "30days",
                "gdelt_max_records": gdelt_max_records,
                "gdelt_fallback_max_records": gdelt_max_records,
                "gdelt_timeout_sec": max(12, int(settings.gdelt_timeout_sec)),
            }
        )
        gdelt_connector = GdeltEventsConnector(gdelt_settings)
        gdelt_query = str(gdelt_settings.gdelt_query).strip()
        if not gdelt_query:
            gdelt_query = "(earthquake OR wildfire OR flood OR energy OR power OR refinery) AND (china OR jiangxi)"

        merged: dict[str, Any] = {}
        gdelt_stats = Counter()
        cursor = window_start
        gdelt_sorts = ("DateDesc", "DateAsc") if args.gdelt_date_asc else ("DateDesc",)
        while cursor < now:
            bucket_end = min(cursor + timedelta(hours=bucket_hours), now)
            for sort in gdelt_sorts:
                gdelt_stats["request_total"] += 1
                try:
                    articles, reason = gdelt_connector._request_articles(  # noqa: SLF001
                        session,
                        query=gdelt_query,
                        max_records=gdelt_max_records,
                        timeout_sec=gdelt_settings.gdelt_timeout_sec,
                        sort=sort,
                        timespan="30days",
                        start_datetime=cursor,
                        end_datetime=bucket_end,
                    )
                except requests.RequestException:
                    gdelt_stats["request_failed"] += 1
                    continue

                if reason:
                    gdelt_stats["request_skipped"] += 1
                    gdelt_stats[f"skip_reason::{reason}"] += 1
                    lower_reason = str(reason).lower()
                    if "429" in lower_reason or "限流" in lower_reason or "rate" in lower_reason:
                        time.sleep(max(2.0, gdelt_sleep_sec))
                    continue
                if not articles:
                    gdelt_stats["request_empty"] += 1
                    continue

                gdelt_stats["request_success"] += 1
                records = gdelt_connector._articles_to_records(articles)  # noqa: SLF001
                for record in records:
                    if record.event_time < window_start or record.event_time > now:
                        continue
                    merged[f"{record.source}|{record.external_id}"] = record
                gdelt_stats["records_accumulated"] = len(merged)

                if gdelt_sleep_sec > 0:
                    time.sleep(gdelt_sleep_sec)
            cursor = bucket_end

        gdelt_records = sorted(merged.values(), key=lambda item: item.event_time)
        summary["sources"]["gdelt_events"] = {
            "fetched": len(gdelt_records),
            "inserted": _persist_records(service, gdelt_records),
            "request_stats": dict(gdelt_stats),
        }

        summary["purged_non_mainland"] = service._purge_non_mainland_events()  # noqa: SLF001
        service._refresh_facility_links()  # noqa: SLF001
        service._refresh_risk_snapshot()  # noqa: SLF001
        service._refresh_risk_timeline()  # noqa: SLF001
        summary["analysis"] = service.analysis_service.run_cycle()
        summary["offline_snapshots"] = service._refresh_offline_event_snapshots()  # noqa: SLF001

    finally:
        session.close()

    summary["final_counts"] = {
        "24h": _count_recent(engine, days=1),
        "7d": _count_recent(engine, days=7),
        "30d": _count_recent(engine, days=30),
    }
    summary["distribution"] = {
        "daily_30d": _daily_distribution(engine, days=30),
        "source_30d": _source_distribution(engine, days=30),
    }
    summary["finished_at"] = datetime.now(tz=UTC).isoformat()

    print(json.dumps(summary, ensure_ascii=False, indent=2, default=str))


if __name__ == "__main__":
    main()
