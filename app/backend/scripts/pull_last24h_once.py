from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

from sqlalchemy import text
from sqlalchemy.engine import Engine

sys.path.append(str(Path(__file__).resolve().parents[1] / "src"))

from gisdatamonitor_backend.config import get_settings  # noqa: E402
from gisdatamonitor_backend.db import get_engine  # noqa: E402
from gisdatamonitor_backend.services.sync import EventSyncService  # noqa: E402


BACKEND_DIR = Path(__file__).resolve().parents[1]
OFFLINE_DIR = BACKEND_DIR / "cache" / "offline"


def _count_recent(engine: Engine, *, hours: int) -> dict[str, int]:
    cutoff = datetime.now(tz=UTC) - timedelta(hours=max(1, int(hours)))
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


def _count_active_days(engine: Engine, *, hours: int) -> int:
    cutoff = datetime.now(tz=UTC) - timedelta(hours=max(1, int(hours)))
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT substr(event_time, 1, 10) AS day_key
                FROM event_normalized
                WHERE event_time >= :cutoff
                GROUP BY substr(event_time, 1, 10)
                """
            ),
            {"cutoff": cutoff.isoformat()},
        ).fetchall()
    return len(rows)


def _read_snapshot_meta(file_name: str) -> dict[str, object]:
    target = OFFLINE_DIR / file_name
    if not target.exists():
        return {"status": "missing", "file": str(target), "event_count": 0}
    try:
        payload = json.loads(target.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {"status": "invalid", "file": str(target), "event_count": 0}
    return {
        "status": "ok",
        "file": str(target),
        "event_count": int(payload.get("event_count") or 0),
        "generated_at": payload.get("generated_at"),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="拉取最近24小时事件，并刷新离线回放快照。")
    parser.add_argument("--max-cycles", type=int, default=3, help="最多拉取轮次，默认 3")
    parser.add_argument("--sleep-sec", type=float, default=2.0, help="轮次间隔秒数，默认 2")
    parser.add_argument("--min-events-24h", type=int, default=120, help="24h事件目标数量，默认 120")
    parser.add_argument("--min-geo-events-24h", type=int, default=4, help="24h地理事件目标数量，默认 4")
    parser.add_argument("--min-events-7d", type=int, default=500, help="7d事件目标数量，默认 500")
    parser.add_argument("--min-geo-events-7d", type=int, default=15, help="7d地理事件目标数量，默认 15")
    parser.add_argument("--min-days-covered-7d", type=int, default=5, help="7d至少覆盖的活跃天数，默认 5")
    args = parser.parse_args()

    settings = get_settings()
    engine = get_engine()
    service = EventSyncService(engine=engine, settings=settings)

    rounds: list[dict[str, object]] = []
    reached_target = False

    max_cycles = max(1, int(args.max_cycles))
    sleep_sec = max(0.0, float(args.sleep_sec))
    min_events_24h = max(0, int(args.min_events_24h))
    min_geo_events_24h = max(0, int(args.min_geo_events_24h))
    min_events_7d = max(0, int(args.min_events_7d))
    min_geo_events_7d = max(0, int(args.min_geo_events_7d))
    min_days_7d = max(1, int(args.min_days_covered_7d))

    for cycle in range(1, max_cycles + 1):
        sync_result = service.run_cycle()
        count_24h = _count_recent(engine, hours=24)
        count_7d = _count_recent(engine, hours=24 * 7)
        days_7d = _count_active_days(engine, hours=24 * 7)

        rounds.append(
            {
                "cycle": cycle,
                "sync": sync_result,
                "count_24h": count_24h,
                "count_7d": count_7d,
                "active_days_7d": days_7d,
            }
        )

        reached_target = (
            count_24h["total"] >= min_events_24h
            and count_24h["geo"] >= min_geo_events_24h
            and count_7d["total"] >= min_events_7d
            and count_7d["geo"] >= min_geo_events_7d
            and days_7d >= min_days_7d
        )
        if reached_target:
            break
        if cycle < max_cycles and sleep_sec > 0:
            time.sleep(sleep_sec)

    # 确保离线快照最终一定刷新一次，供离线展示直接回退。
    offline_refresh = service._refresh_offline_event_snapshots()  # noqa: SLF001

    final_24h = _count_recent(engine, hours=24)
    final_7d = _count_recent(engine, hours=24 * 7)
    final_days_7d = _count_active_days(engine, hours=24 * 7)

    output = {
        "status": "ok",
        "target_reached": reached_target,
        "targets": {
            "min_events_24h": min_events_24h,
            "min_geo_events_24h": min_geo_events_24h,
            "min_events_7d": min_events_7d,
            "min_geo_events_7d": min_geo_events_7d,
            "min_days_covered_7d": min_days_7d,
        },
        "final_counts": {
            "24h": final_24h,
            "7d": final_7d,
            "active_days_7d": final_days_7d,
        },
        "offline_refresh": offline_refresh,
        "offline_files": {
            "events_last24h.json": _read_snapshot_meta("events_last24h.json"),
            "events_last7d.json": _read_snapshot_meta("events_last7d.json"),
            "events_last30d.json": _read_snapshot_meta("events_last30d.json"),
        },
        "rounds": rounds,
    }
    print(json.dumps(output, ensure_ascii=False, indent=2, default=str))


if __name__ == "__main__":
    main()
