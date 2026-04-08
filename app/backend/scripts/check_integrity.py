from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

from sqlalchemy import text

sys.path.append(str(Path(__file__).resolve().parents[1] / "src"))

from gisdatamonitor_backend.db import get_engine  # noqa: E402


PROJECT_ROOT = Path(__file__).resolve().parents[3]
DATA_DIR = PROJECT_ROOT / "data"


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def main() -> None:
    dem_manifest = load_json(DATA_DIR / "manifests" / "jiangxi_dem_manifest.json")
    baker_manifest = load_json(DATA_DIR / "manifests" / "jiangxi_baker_manifest.json")
    engine = get_engine()

    with engine.connect() as conn:
        dem_tile_count = int(conn.execute(text("SELECT COUNT(*) FROM dem_tiles")).scalar_one())
        dem_resolution = conn.execute(text("SELECT MIN(resolution_m) FROM dem_tiles")).scalar_one()
        baker_layer_count = int(conn.execute(text("SELECT COUNT(DISTINCT source_layer) FROM baker_facilities")).scalar_one())
        baker_feature_count = int(conn.execute(text("SELECT COUNT(*) FROM baker_facilities")).scalar_one())
        enriched_event_count = int(conn.execute(text("SELECT COUNT(*) FROM event_enriched")).scalar_one())
        scene_count = int(conn.execute(text("SELECT COUNT(*) FROM scene_preset")).scalar_one())
        playback_cache_count = int(conn.execute(text("SELECT COUNT(*) FROM playback_frame_cache")).scalar_one())
        field_completeness = conn.execute(
            text(
                """
                SELECT
                    AVG(CASE WHEN facility_type IS NOT NULL AND facility_type <> '' THEN 1 ELSE 0 END) AS facility_type_rate,
                    AVG(CASE WHEN start_year IS NOT NULL THEN 1 ELSE 0 END) AS start_year_rate,
                    AVG(CASE WHEN geom_json IS NOT NULL THEN 1 ELSE 0 END) AS geometry_rate
                FROM baker_facilities
                """
            )
        ).mappings().one()
        latest_sync = conn.execute(
            text(
                """
                SELECT connector, status, started_at, finished_at, records_fetched, records_inserted, error_message
                FROM (
                    SELECT
                        connector,
                        status,
                        started_at,
                        finished_at,
                        records_fetched,
                        records_inserted,
                        error_message,
                        ROW_NUMBER() OVER (PARTITION BY connector ORDER BY started_at DESC) AS rn
                    FROM sync_job_log
                ) ranked
                WHERE rn = 1
                ORDER BY connector
                """
            )
        ).mappings().all()

    report = {
        "dem": {
            "manifest_tile_count": dem_manifest["tile_count"],
            "db_tile_count": dem_tile_count,
            "manifest_resolution_m": dem_manifest["resolution_m"],
            "db_resolution_m": dem_resolution,
            "tile_count_match": dem_tile_count == dem_manifest["tile_count"],
            "resolution_match": abs(float(dem_resolution or 0) - float(dem_manifest["resolution_m"])) < 1e-6,
        },
        "baker": {
            "manifest_layer_count": baker_manifest["downloaded_layer_count"],
            "db_layer_count": baker_layer_count,
            "manifest_feature_count": sum(layer["jiangxi_feature_count"] for layer in baker_manifest.get("layers", [])),
            "db_feature_count": baker_feature_count,
            "layer_count_match": baker_layer_count == baker_manifest["downloaded_layer_count"],
        },
        "workbench": {
            "scene_count": scene_count,
            "enriched_event_count": enriched_event_count,
            "playback_cache_count": playback_cache_count,
        },
        "field_completeness": {
            "facility_type_rate": float(field_completeness["facility_type_rate"] or 0),
            "start_year_rate": float(field_completeness["start_year_rate"] or 0),
            "geometry_rate": float(field_completeness["geometry_rate"] or 0),
        },
        "realtime_connectors_latest": [dict(row) for row in latest_sync],
    }
    print(json.dumps(report, ensure_ascii=False, indent=2, default=str))


if __name__ == "__main__":
    main()
