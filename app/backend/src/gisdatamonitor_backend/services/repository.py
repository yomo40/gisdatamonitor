from __future__ import annotations

import hashlib
import json
import os
import sys
import warnings
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import mapbox_vector_tile
import mercantile
import numpy as np
import rasterio
from rasterio.io import MemoryFile
from rasterio.errors import NotGeoreferencedWarning
from rasterio.transform import from_bounds
from rasterio.warp import Resampling, reproject
from shapely.geometry import box, mapping, shape
from sqlalchemy import text
from sqlalchemy.engine import Connection


def _resolve_project_root() -> Path:
    runtime_root = os.environ.get("GISDATAMONITOR_RUNTIME_ROOT", "").strip()
    if runtime_root:
        return Path(runtime_root).resolve()
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parents[5]


PROJECT_ROOT = _resolve_project_root()
OFFLINE_CACHE_DIR = (PROJECT_ROOT / "app" / "backend" / "cache" / "offline").resolve()


def _safe_json(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (dict, list)):
        return value
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return value
    return value


def _as_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=UTC)
    text_value = str(value).strip()
    if not text_value:
        return None
    text_value = text_value.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(text_value)
        return dt if dt.tzinfo else dt.replace(tzinfo=UTC)
    except ValueError:
        return None


def _like_clause(column: str) -> str:
    return f"LOWER(COALESCE({column}, '')) LIKE LOWER(:{column})"


def _window_delta(window: str) -> timedelta:
    if window == "24h":
        return timedelta(hours=24)
    if window == "7d":
        return timedelta(days=7)
    if window == "30d":
        return timedelta(days=30)
    raise ValueError(f"unsupported window: {window}")


def _severity_component_fallback(severity: str | None) -> float:
    severity_map = {"high": 68.0, "medium": 46.0, "low": 22.0}
    return float(severity_map.get(str(severity or "low"), 22.0))


def _source_component_fallback(source: str | None) -> float:
    source_map = {
        "usgs_earthquake": 16.0,
        "nasa_firms": 14.0,
        "gdelt_events": 8.0,
        "energy_market": 12.0,
        "energy_announcement": 10.0,
        "ais_port_stub": 6.0,
    }
    return float(source_map.get(str(source or ""), 8.0))


def _risk_level_from_score(score: float) -> str:
    if score >= 85:
        return "critical"
    if score >= 65:
        return "high"
    if score >= 40:
        return "medium"
    return "low"


class DataRepository:
    def __init__(self, conn: Connection) -> None:
        self.conn = conn
        self.backend = self.conn.engine.url.get_backend_name()

    def list_layers(self) -> dict[str, Any]:
        layer_rows = self.conn.execute(
            text(
                """
                SELECT source_layer, facility_type, COUNT(*) AS feature_count
                FROM baker_facilities
                GROUP BY source_layer, facility_type
                ORDER BY source_layer
                """
            )
        ).mappings().all()
        versions = self.conn.execute(
            text(
                """
                SELECT dataset_key, dataset_version, metadata, updated_at
                FROM data_versions
                ORDER BY dataset_key
                """
            )
        ).mappings().all()
        return {
            "layers": [
                {
                    "layer": row["source_layer"],
                    "facility_type": row["facility_type"],
                    "feature_count": int(row["feature_count"]),
                }
                for row in layer_rows
            ],
            "versions": [
                {
                    "dataset_key": row["dataset_key"],
                    "dataset_version": row["dataset_version"],
                    "metadata": _safe_json(row["metadata"]),
                    "updated_at": row["updated_at"],
                }
                for row in versions
            ],
        }

    def list_scenes(self) -> dict[str, Any]:
        rows = self.conn.execute(
            text(
                """
                SELECT scene_id, scene_name, description, config_json, updated_at
                FROM scene_preset
                ORDER BY CASE scene_id
                    WHEN 'world' THEN 1
                    WHEN 'finance' THEN 2
                    WHEN 'tech' THEN 3
                    WHEN 'happy' THEN 4
                    ELSE 100
                END, scene_id
                """
            )
        ).mappings().all()
        items = []
        for row in rows:
            config = _safe_json(row["config_json"]) or {}
            items.append(
                {
                    "scene_id": row["scene_id"],
                    "scene_name": row["scene_name"],
                    "description": row["description"],
                    "config": config,
                    "updated_at": row["updated_at"],
                }
            )
        default_scene_id = "world" if any(item["scene_id"] == "world" for item in items) else (items[0]["scene_id"] if items else None)
        return {
            "default_scene_id": default_scene_id,
            "items": items,
        }

    def _load_scene(self, scene_id: str) -> dict[str, Any] | None:
        row = self.conn.execute(
            text(
                """
                SELECT scene_id, scene_name, description, config_json, updated_at
                FROM scene_preset
                WHERE scene_id = :scene_id
                LIMIT 1
                """
            ),
            {"scene_id": scene_id},
        ).mappings().first()
        if row is None:
            return None
        return {
            "scene_id": row["scene_id"],
            "scene_name": row["scene_name"],
            "description": row["description"],
            "config": _safe_json(row["config_json"]) or {},
            "updated_at": row["updated_at"],
        }

    def scene_state(self, scene_id: str) -> dict[str, Any] | None:
        scene = self._load_scene(scene_id)
        if scene is None:
            return None

        config = scene["config"] if isinstance(scene["config"], dict) else {}
        facility_type = str(config.get("facility_type") or "").strip() or None
        event_source = str(config.get("event_source") or "").strip() or None
        event_hours = int(config.get("event_hours") or 24)
        window_start = datetime.now(tz=UTC) - timedelta(hours=event_hours)

        facility_conditions = ["1=1"]
        facility_params: dict[str, Any] = {}
        if facility_type:
            facility_conditions.append("facility_type = :facility_type")
            facility_params["facility_type"] = facility_type
        facility_where = " AND ".join(facility_conditions)
        facility_count = int(
            self.conn.execute(
                text(f"SELECT COUNT(*) FROM baker_facilities WHERE {facility_where}"),
                facility_params,
            ).scalar_one()
        )

        event_conditions = ["event_time >= :window_start"]
        event_conditions_join = ["e.event_time >= :window_start"]
        event_params: dict[str, Any] = {"window_start": window_start.isoformat()}
        if event_source:
            event_conditions.append("source = :event_source")
            event_conditions_join.append("e.source = :event_source")
            event_params["event_source"] = event_source
        event_where = " AND ".join(event_conditions)
        event_where_join = " AND ".join(event_conditions_join)
        event_count = int(
            self.conn.execute(
                text(f"SELECT COUNT(*) FROM event_normalized WHERE {event_where}"),
                event_params,
            ).scalar_one()
        )
        high_risk_count = int(
            self.conn.execute(
                text(
                    f"""
                    SELECT COUNT(*)
                    FROM event_normalized e
                    JOIN event_enriched ee ON ee.event_id = e.id
                    WHERE {event_where_join}
                      AND ee.risk_level IN ('high', 'critical')
                    """
                ),
                event_params,
            ).scalar_one()
        )

        latest_sync = self.conn.execute(
            text("SELECT MAX(finished_at) FROM sync_job_log WHERE finished_at IS NOT NULL")
        ).scalar_one()
        anomaly_count_24h = int(
            self.conn.execute(
                text(
                    """
                    SELECT COUNT(*)
                    FROM connector_health_history
                    WHERE status IN ('failed', 'circuit_open')
                      AND recorded_at >= datetime('now', '-24 hour')
                    """
                )
            ).scalar_one()
        )

        layer_state = []
        for layer in config.get("layers", []):
            layer_state.append({"layer_id": str(layer), "enabled": True})

        return {
            "scene": scene,
            "runtime": {
                "loaded_layers": len(layer_state),
                "last_sync_time": latest_sync,
                "connector_anomaly_count_24h": anomaly_count_24h,
                "current_scene": scene_id,
            },
            "filters": {
                "facility_type": facility_type,
                "event_source": event_source,
                "event_hours": event_hours,
            },
            "layer_state": layer_state,
            "right_cards": config.get("right_cards", []),
            "timeline_window": config.get("timeline_window", "24h"),
            "stats": {
                "facility_count": facility_count,
                "event_count": event_count,
                "high_risk_event_count": high_risk_count,
            },
        }

    def list_facilities(
        self,
        *,
        facility_type: str | None,
        start_year_min: int | None,
        start_year_max: int | None,
        status: str | None,
        admin_city: str | None,
        limit: int,
        offset: int,
    ) -> dict[str, Any]:
        conditions = ["1=1"]
        params: dict[str, Any] = {"limit": limit, "offset": offset}
        if facility_type:
            conditions.append("facility_type = :facility_type")
            params["facility_type"] = facility_type
        if start_year_min is not None:
            conditions.append("start_year >= :start_year_min")
            params["start_year_min"] = start_year_min
        if start_year_max is not None:
            conditions.append("start_year <= :start_year_max")
            params["start_year_max"] = start_year_max
        if status:
            conditions.append(_like_clause("status"))
            params["status"] = f"%{status}%"
        if admin_city:
            conditions.append(_like_clause("admin_city"))
            params["admin_city"] = f"%{admin_city}%"
        where_sql = " AND ".join(conditions)
        total = int(
            self.conn.execute(text(f"SELECT COUNT(*) FROM baker_facilities WHERE {where_sql}"), params).scalar_one()
        )
        rows = self.conn.execute(
            text(
                f"""
                SELECT
                    id,
                    facility_id,
                    facility_type,
                    source_layer,
                    name,
                    start_year,
                    status,
                    admin_city,
                    geom_json AS geometry
                FROM baker_facilities
                WHERE {where_sql}
                ORDER BY facility_id
                LIMIT :limit OFFSET :offset
                """
            ),
            params,
        ).mappings()
        items = [
            {
                "id": str(row["id"]),
                "facility_id": row["facility_id"],
                "facility_type": row["facility_type"],
                "source_layer": row["source_layer"],
                "name": row["name"],
                "start_year": row["start_year"],
                "status": row["status"],
                "admin_city": row["admin_city"],
                "geometry": _safe_json(row["geometry"]),
            }
            for row in rows
        ]
        return {"total": total, "items": items}

    def get_facility_detail(self, facility_key: str) -> dict[str, Any] | None:
        row = self.conn.execute(
            text(
                """
                SELECT
                    id,
                    facility_id,
                    facility_type,
                    source_layer,
                    name,
                    start_year,
                    status,
                    admin_city,
                    properties,
                    geom_json AS geometry
                FROM baker_facilities
                WHERE facility_id = :facility_key OR id = :facility_key
                LIMIT 1
                """
            ),
            {"facility_key": facility_key},
        ).mappings().first()
        if row is None:
            return None
        terrain = self.conn.execute(
            text(
                """
                SELECT elevation_m, slope_deg, aspect_deg, hillshade, roughness, computed_at
                FROM facility_terrain_metrics
                WHERE facility_pk = :facility_pk
                """
            ),
            {"facility_pk": row["id"]},
        ).mappings().first()
        events = self.conn.execute(
            text(
                """
                SELECT
                    e.id,
                    e.source,
                    e.event_type,
                    e.severity,
                    e.title,
                    e.description,
                    e.event_time,
                    l.distance_km,
                    e.geometry_json AS geometry,
                    e.properties
                FROM facility_event_link l
                JOIN event_normalized e ON e.id = l.event_id
                WHERE l.facility_pk = :facility_pk
                ORDER BY e.event_time DESC
                LIMIT 50
                """
            ),
            {"facility_pk": row["id"]},
        ).mappings().all()
        return {
            "facility": {
                "id": str(row["id"]),
                "facility_id": row["facility_id"],
                "facility_type": row["facility_type"],
                "source_layer": row["source_layer"],
                "name": row["name"],
                "start_year": row["start_year"],
                "status": row["status"],
                "admin_city": row["admin_city"],
                "properties": _safe_json(row["properties"]),
                "geometry": _safe_json(row["geometry"]),
            },
            "terrain_metrics": dict(terrain) if terrain else None,
            "recent_events": [
                {
                    "id": event["id"],
                    "source": event["source"],
                    "event_type": event["event_type"],
                    "severity": event["severity"],
                    "title": event["title"],
                    "description": event["description"],
                    "event_time": event["event_time"],
                    "distance_km": float(event["distance_km"]),
                    "geometry": _safe_json(event["geometry"]),
                    "properties": _safe_json(event["properties"]),
                }
                for event in events
            ],
        }

    def list_events(
        self,
        *,
        source: str | None,
        event_type: str | None,
        severity: str | None,
        hours: int,
        since: datetime | None,
        limit: int,
        offset: int,
    ) -> dict[str, Any]:
        window_start = datetime.now(tz=UTC) - timedelta(hours=hours)
        conditions = ["event_time >= :window_start"]
        params: dict[str, Any] = {
            "window_start": window_start.isoformat(),
            "limit": limit,
            "offset": offset,
        }
        if since is not None:
            since_dt = since.astimezone(UTC) if since.tzinfo else since.replace(tzinfo=UTC)
            conditions.append("ingestion_time >= :since")
            params["since"] = since_dt.strftime("%Y-%m-%d %H:%M:%S")
        if source:
            conditions.append("source = :source")
            params["source"] = source
        if event_type:
            conditions.append("event_type = :event_type")
            params["event_type"] = event_type
        if severity:
            conditions.append("severity = :severity")
            params["severity"] = severity
        where_sql = " AND ".join(conditions)

        total = int(
            self.conn.execute(
                text(f"SELECT COUNT(*) FROM event_normalized WHERE {where_sql}"),
                params,
            ).scalar_one()
        )
        page_rows = self.conn.execute(
            text(
                f"""
                SELECT
                    id,
                    source,
                    external_id,
                    event_type,
                    severity,
                    title,
                    description,
                    event_time,
                    ingestion_time,
                    properties,
                    geometry_json AS geometry
                FROM event_normalized
                WHERE {where_sql}
                ORDER BY event_time DESC
                LIMIT :limit OFFSET :offset
                """
            ),
            params,
        ).mappings().all()
        return {
            "total": total,
            "items": [
                {
                    "id": row["id"],
                    "source": row["source"],
                    "external_id": row["external_id"],
                    "event_type": row["event_type"],
                    "severity": row["severity"],
                    "title": row["title"],
                    "description": row["description"],
                    "event_time": row["event_time"],
                    "ingestion_time": row["ingestion_time"],
                    "properties": _safe_json(row["properties"]),
                    "geometry": _safe_json(row["geometry"]),
                }
                for row in page_rows
            ],
        }

    def list_events_enriched(
        self,
        *,
        source: str | None,
        event_type: str | None,
        severity: str | None,
        risk_level: str | None,
        hours: int,
        since: datetime | None,
        limit: int,
        offset: int,
    ) -> dict[str, Any]:
        window_start = datetime.now(tz=UTC) - timedelta(hours=hours)
        conditions = ["e.event_time >= :window_start"]
        params: dict[str, Any] = {"window_start": window_start.isoformat()}
        if since is not None:
            since_dt = since.astimezone(UTC) if since.tzinfo else since.replace(tzinfo=UTC)
            conditions.append("e.ingestion_time >= :since")
            params["since"] = since_dt.strftime("%Y-%m-%d %H:%M:%S")
        if source:
            conditions.append("e.source = :source")
            params["source"] = source
        if event_type:
            conditions.append("e.event_type = :event_type")
            params["event_type"] = event_type
        if severity:
            conditions.append("e.severity = :severity")
            params["severity"] = severity
        where_sql = " AND ".join(conditions)

        select_sql = f"""
            SELECT
                e.id,
                e.source,
                e.external_id,
                e.event_type,
                e.severity,
                e.title,
                e.description,
                e.event_time,
                e.ingestion_time,
                e.properties,
                e.geometry_json AS geometry,
                ee.risk_score,
                ee.risk_level,
                ee.risk_reason,
                ee.summary_zh,
                ee.summary_en,
                ee.impact_tags,
                ee.severity_component,
                ee.proximity_component,
                ee.recency_component,
                ee.source_component,
                ee.confidence,
                ee.model_provider,
                ee.analysis_version,
                ee.updated_at AS enriched_updated_at
            FROM event_normalized e
            LEFT JOIN event_enriched ee ON ee.event_id = e.id
            WHERE {where_sql}
        """

        if risk_level:
            rows = self.conn.execute(
                text(
                    f"""
                    {select_sql}
                    ORDER BY e.event_time DESC
                    """
                ),
                params,
            ).mappings().all()
            filtered = []
            for row in rows:
                fallback_score = _severity_component_fallback(row["severity"]) + _source_component_fallback(row["source"]) + 8.0
                effective_risk_level = row["risk_level"] or _risk_level_from_score(fallback_score)
                if effective_risk_level == risk_level:
                    filtered.append(row)
            total = len(filtered)
            page_rows = filtered[offset : offset + limit]
        else:
            total = int(
                self.conn.execute(
                    text(
                        f"""
                        SELECT COUNT(*)
                        FROM event_normalized e
                        WHERE {where_sql}
                        """
                    ),
                    params,
                ).scalar_one()
            )
            page_rows = self.conn.execute(
                text(
                    f"""
                    {select_sql}
                    ORDER BY e.event_time DESC
                    LIMIT :limit OFFSET :offset
                    """
                ),
                {**params, "limit": limit, "offset": offset},
            ).mappings().all()
        items = []
        for row in page_rows:
            fallback_score = _severity_component_fallback(row["severity"]) + _source_component_fallback(row["source"]) + 8.0
            fallback_level = _risk_level_from_score(fallback_score)
            items.append(
                {
                    "id": row["id"],
                    "source": row["source"],
                    "external_id": row["external_id"],
                    "event_type": row["event_type"],
                    "severity": row["severity"],
                    "title": row["title"],
                    "description": row["description"],
                    "event_time": row["event_time"],
                    "ingestion_time": row["ingestion_time"],
                    "properties": _safe_json(row["properties"]),
                    "geometry": _safe_json(row["geometry"]),
                    "risk_score": float(row["risk_score"] if row["risk_score"] is not None else fallback_score),
                    "risk_level": row["risk_level"] or fallback_level,
                    "risk_reason": row["risk_reason"] or "规则回退：严重度与来源基线",
                    "summary_zh": row["summary_zh"] or (row["title"] or ""),
                    "summary_en": row["summary_en"] or (row["title"] or ""),
                    "impact_tags": _safe_json(row["impact_tags"]) or [],
                    "score_components": {
                        "severity_component": float(
                            row["severity_component"]
                            if row["severity_component"] is not None
                            else _severity_component_fallback(row["severity"])
                        ),
                        "proximity_component": float(row["proximity_component"] if row["proximity_component"] is not None else 4.0),
                        "recency_component": float(row["recency_component"] if row["recency_component"] is not None else 6.0),
                        "source_component": float(
                            row["source_component"] if row["source_component"] is not None else _source_component_fallback(row["source"])
                        ),
                    },
                    "confidence": float(row["confidence"] if row["confidence"] is not None else 0.55),
                    "model_provider": row["model_provider"] or "rule_fallback",
                    "analysis_version": row["analysis_version"] or "fallback",
                    "enriched_updated_at": row["enriched_updated_at"],
                }
            )
        return {"total": total, "items": items}

    def risk_explain(self, *, window: str, region_level: str, region_name: str | None) -> dict[str, Any]:
        delta = _window_delta(window)
        now = datetime.now(tz=UTC)
        window_start = now - delta

        rows = self.conn.execute(
            text(
                """
                SELECT
                    e.id,
                    e.source,
                    e.event_type,
                    e.severity,
                    e.title,
                    e.event_time,
                    ee.risk_score,
                    ee.risk_level,
                    ee.risk_reason,
                    ee.summary_zh,
                    ee.summary_en,
                    ee.impact_tags,
                    ee.severity_component,
                    ee.proximity_component,
                    ee.recency_component,
                    ee.source_component
                FROM event_normalized e
                LEFT JOIN event_enriched ee ON ee.event_id = e.id
                ORDER BY e.event_time DESC
                """
            )
        ).mappings().all()

        city_event_allowlist: set[str] | None = None
        effective_region_name = region_name or "Jiangxi"
        if region_level == "city":
            target_city = (region_name or "").strip()
            if target_city:
                link_rows = self.conn.execute(
                    text(
                        """
                        SELECT DISTINCT l.event_id
                        FROM facility_event_link l
                        JOIN baker_facilities f ON f.id = l.facility_pk
                        WHERE LOWER(COALESCE(f.admin_city, '')) = LOWER(:target_city)
                        """
                    ),
                    {"target_city": target_city},
                ).mappings().all()
                city_event_allowlist = {str(row["event_id"]) for row in link_rows}
            else:
                city_event_allowlist = set()
            effective_region_name = target_city or "UNKNOWN"

        events: list[dict[str, Any]] = []
        for row in rows:
            event_id = str(row["id"])
            if city_event_allowlist is not None and event_id not in city_event_allowlist:
                continue
            event_time = _as_datetime(row["event_time"])
            if event_time is None or event_time < window_start:
                continue
            severity_component = float(
                row["severity_component"]
                if row["severity_component"] is not None
                else _severity_component_fallback(row["severity"])
            )
            proximity_component = float(row["proximity_component"] if row["proximity_component"] is not None else 4.0)
            recency_component = float(row["recency_component"] if row["recency_component"] is not None else 6.0)
            source_component = float(
                row["source_component"] if row["source_component"] is not None else _source_component_fallback(row["source"])
            )
            risk_score = float(
                row["risk_score"]
                if row["risk_score"] is not None
                else max(0.0, min(100.0, severity_component + proximity_component + recency_component + source_component))
            )
            risk_level = row["risk_level"] or _risk_level_from_score(risk_score)
            events.append(
                {
                    "id": event_id,
                    "source": row["source"],
                    "event_type": row["event_type"],
                    "severity": row["severity"],
                    "title": row["title"],
                    "event_time": row["event_time"],
                    "risk_score": risk_score,
                    "risk_level": risk_level,
                    "risk_reason": row["risk_reason"],
                    "summary_zh": row["summary_zh"],
                    "summary_en": row["summary_en"],
                    "impact_tags": _safe_json(row["impact_tags"]) or [],
                    "severity_component": severity_component,
                    "proximity_component": proximity_component,
                    "recency_component": recency_component,
                    "source_component": source_component,
                }
            )

        total_events = len(events)
        high_events = sum(1 for item in events if item["risk_level"] in {"high", "critical"})
        medium_events = sum(1 for item in events if item["risk_level"] == "medium")
        low_events = total_events - high_events - medium_events
        weighted_score = sum(item["risk_score"] for item in events) / total_events if total_events else 0.0

        severity_sum = sum(item["severity_component"] for item in events)
        proximity_sum = sum(item["proximity_component"] for item in events)
        recency_sum = sum(item["recency_component"] for item in events)
        source_sum = sum(item["source_component"] for item in events)
        component_total = severity_sum + proximity_sum + recency_sum + source_sum

        if component_total > 0:
            breakdown = {
                "severity_weight": round(severity_sum / component_total, 4),
                "proximity_weight": round(proximity_sum / component_total, 4),
                "recency_weight": round(recency_sum / component_total, 4),
                "source_weight": round(source_sum / component_total, 4),
            }
        else:
            breakdown = {
                "severity_weight": 0.0,
                "proximity_weight": 0.0,
                "recency_weight": 0.0,
                "source_weight": 0.0,
            }

        top_events = sorted(events, key=lambda item: (item["risk_score"], str(item["event_time"])), reverse=True)[:10]
        dominant = max(breakdown.items(), key=lambda pair: pair[1])[0] if events else "severity_weight"
        explanation_zh = (
            f"{effective_region_name} 在 {window} 时间窗内监测到 {total_events} 条事件，"
            f"高风险 {high_events} 条，综合风险分 {weighted_score:.1f}。"
            f"当前主导因子为 {dominant}。"
        )
        explanation_en = (
            f"{effective_region_name} has {total_events} events in {window}, with {high_events} high/critical cases. "
            f"Composite score is {weighted_score:.1f}, dominated by {dominant}."
        )

        return {
            "window": window,
            "region_level": region_level,
            "region_name": effective_region_name,
            "metrics": {
                "total_events": total_events,
                "high_events": high_events,
                "medium_events": medium_events,
                "low_events": low_events,
                "composite_risk_score": round(weighted_score, 2),
            },
            "weights": {
                "formula": "severity_baseline + distance_weight + recency_decay + source_credibility",
                "components": {
                    "severity_component_sum": round(severity_sum, 3),
                    "proximity_component_sum": round(proximity_sum, 3),
                    "recency_component_sum": round(recency_sum, 3),
                    "source_component_sum": round(source_sum, 3),
                },
            },
            "score_breakdown": breakdown,
            "explanation_zh": explanation_zh,
            "explanation_en": explanation_en,
            "event_trace": {
                "event_count": total_events,
                "event_ids": [item["id"] for item in top_events],
            },
            "top_events": top_events,
            "generated_at": now.isoformat(),
        }

    def timeline_playback(
        self,
        *,
        scene_id: str,
        window: str,
        step_minutes: int,
        end_time: datetime | None,
        frame_limit: int,
    ) -> dict[str, Any]:
        scene = self._load_scene(scene_id)
        if scene is None:
            raise ValueError(f"未找到场景: {scene_id}")
        config = scene["config"] if isinstance(scene["config"], dict) else {}
        scene_event_source = str(config.get("event_source") or "").strip() or None
        scene_facility_type = str(config.get("facility_type") or "").strip() or None

        if step_minutes < 1:
            step_minutes = 5
        step = timedelta(minutes=step_minutes)
        window_delta = _window_delta(window)

        def _floor_to_step(dt: datetime, step_minutes_value: int) -> datetime:
            step_seconds = max(60, int(step_minutes_value) * 60)
            dt_utc = dt.astimezone(UTC)
            floored = int(dt_utc.timestamp()) // step_seconds * step_seconds
            return datetime.fromtimestamp(floored, tz=UTC)

        events_raw = self.conn.execute(
            text(
                """
                SELECT
                    e.id,
                    e.source,
                    e.event_type,
                    e.severity,
                    e.title,
                    e.event_time,
                    e.geometry_json,
                    ee.risk_score,
                    ee.risk_level
                FROM event_normalized e
                LEFT JOIN event_enriched ee ON ee.event_id = e.id
                ORDER BY e.event_time ASC
                """
            )
        ).mappings().all()

        all_events_full: list[dict[str, Any]] = []
        for row in events_raw:
            event_time = _as_datetime(row["event_time"])
            if event_time is None:
                continue
            fallback_score = _severity_component_fallback(row["severity"]) + _source_component_fallback(row["source"]) + 8.0
            all_events_full.append(
                {
                    "id": str(row["id"]),
                    "source": row["source"],
                    "event_type": row["event_type"],
                    "severity": row["severity"],
                    "title": row["title"],
                    "event_time": event_time,
                    "event_time_raw": row["event_time"],
                    "geometry": _safe_json(row["geometry_json"]),
                    "risk_score": float(row["risk_score"] if row["risk_score"] is not None else fallback_score),
                    "risk_level": row["risk_level"] or _risk_level_from_score(fallback_score),
                }
            )

        requested_end_dt = end_time.astimezone(UTC) if end_time else datetime.now(tz=UTC)
        anchor_mode = "requested_end"
        anchor_pool = all_events_full
        if scene_event_source:
            scoped = [event for event in all_events_full if str(event["source"] or "") == scene_event_source]
            recent_cutoff = requested_end_dt - timedelta(hours=48)
            scoped_recent = [event for event in scoped if event["event_time"] >= recent_cutoff]
            if scoped_recent:
                anchor_pool = scoped_recent
        anchor_candidates = [event["event_time"] for event in anchor_pool if event["event_time"] <= requested_end_dt]
        if end_time is None and anchor_candidates:
            anchor_mode = "latest_event"
            end_dt = _floor_to_step(max(anchor_candidates), step_minutes)
        else:
            end_dt = _floor_to_step(requested_end_dt, step_minutes)
        start_dt = end_dt - window_delta

        all_events = [
            event
            for event in all_events_full
            if start_dt <= event["event_time"] <= end_dt
        ]

        frame_times: list[datetime] = []
        frame_limit = max(1, int(frame_limit))
        total_steps = max(1, int((end_dt - start_dt) / step))
        if total_steps > frame_limit:
            cursor = end_dt - step * (frame_limit - 1)
        else:
            cursor = start_dt + step

        while cursor <= end_dt:
            frame_times.append(cursor)
            cursor = cursor + step

        if len(frame_times) > frame_limit:
            frame_times = frame_times[-frame_limit:]
        if not frame_times:
            frame_times = [end_dt]

        filtered_events = [
            event
            for event in all_events
            if (not scene_event_source or str(event["source"] or "") == scene_event_source)
        ]

        min_events_map = {"24h": 30, "7d": 140, "30d": 260}
        min_events_required = int(min_events_map.get(window, 30))
        source_mode = "scene_source" if scene_event_source else "all_sources"
        fallback_reason: str | None = None
        if scene_event_source and len(filtered_events) < min_events_required:
            filtered_events = list(all_events)
            source_mode = "all_sources_fallback"
            fallback_reason = "scene_source_events_insufficient"

        def _coverage_day_count(events: list[dict[str, Any]]) -> int:
            day_keys = {
                event["event_time"].astimezone(UTC).strftime("%Y-%m-%d")
                for event in events
                if isinstance(event.get("event_time"), datetime)
            }
            return len(day_keys)

        min_days_map = {"24h": 1, "7d": 3, "30d": 7}
        min_days_required = int(min_days_map.get(window, 1))
        coverage_days = _coverage_day_count(filtered_events)
        if coverage_days < min_days_required:
            offline_events = self._load_offline_events_for_playback(
                start_dt=start_dt,
                end_dt=end_dt,
                source=None if source_mode != "scene_source" else scene_event_source,
            )
            if offline_events:
                merged: dict[str, dict[str, Any]] = {str(event["id"]): event for event in filtered_events}
                for event in offline_events:
                    merged[str(event["id"])] = event
                filtered_events = list(merged.values())
                if fallback_reason is None:
                    fallback_reason = "temporal_coverage_insufficient"

        offline_used = False
        if len(filtered_events) < max(12, min_events_required // 3):
            offline_events = self._load_offline_events_for_playback(
                start_dt=start_dt,
                end_dt=end_dt,
                source=None if source_mode != "scene_source" else scene_event_source,
            )
            if offline_events:
                offline_used = True
                merged: dict[str, dict[str, Any]] = {str(event["id"]): event for event in filtered_events}
                for event in offline_events:
                    merged[str(event["id"])] = event
                filtered_events = list(merged.values())
                if fallback_reason is None:
                    fallback_reason = "offline_snapshot_fallback"

        filtered_events.sort(key=lambda item: item["event_time"])

        event_watermark_row = self.conn.execute(
            text("SELECT COALESCE(MAX(ingestion_time), '') AS max_ingestion_time, COUNT(*) AS total FROM event_normalized")
        ).mappings().first()
        event_watermark = ""
        if event_watermark_row is not None:
            event_watermark = f"{event_watermark_row['max_ingestion_time']}|{event_watermark_row['total']}"

        offline_watermark = "none"
        try:
            mtimes = []
            for file_name in ("events_last30d.json", "events_last7d.json", "events_last24h.json"):
                target = OFFLINE_CACHE_DIR / file_name
                if target.exists():
                    mtimes.append(int(target.stat().st_mtime))
            if mtimes:
                offline_watermark = str(max(mtimes))
        except OSError:
            offline_watermark = "none"

        link_rows = self.conn.execute(
            text(
                """
                SELECT
                    l.event_id,
                    f.facility_id,
                    f.name,
                    f.facility_type,
                    f.admin_city
                FROM facility_event_link l
                JOIN baker_facilities f ON f.id = l.facility_pk
                """
            )
        ).mappings().all()
        event_facilities: dict[str, list[dict[str, Any]]] = {}
        for row in link_rows:
            if scene_facility_type and str(row["facility_type"] or "") != scene_facility_type:
                continue
            key = str(row["event_id"])
            event_facilities.setdefault(key, []).append(
                {
                    "facility_id": row["facility_id"],
                    "name": row["name"],
                    "facility_type": row["facility_type"],
                    "admin_city": row["admin_city"],
                }
            )

        cached_hits = 0
        cache_misses = 0
        frames: list[dict[str, Any]] = []
        cache_upserts: list[dict[str, Any]] = []
        for frame_time in frame_times:
            frame_key = hashlib.sha1(
                f"{scene_id}|{window}|{step_minutes}|{frame_time.isoformat()}|{scene_event_source or ''}|{scene_facility_type or ''}|{source_mode}|{event_watermark}|{offline_watermark}".encode(
                    "utf-8"
                )
            ).hexdigest()
            cache_row = self.conn.execute(
                text("SELECT payload FROM playback_frame_cache WHERE frame_key = :frame_key LIMIT 1"),
                {"frame_key": frame_key},
            ).mappings().first()
            if cache_row is not None:
                cached_hits += 1
                payload = _safe_json(cache_row["payload"]) or {}
                frames.append(payload)
                continue

            cache_misses += 1
            frame_start = frame_time - step
            frame_events = [
                event
                for event in filtered_events
                if frame_start < event["event_time"] <= frame_time
            ]
            frame_events_sorted = sorted(frame_events, key=lambda item: (item["risk_score"], item["event_time"]), reverse=True)[:200]

            high_risk_facilities: dict[str, dict[str, Any]] = {}
            for event in frame_events_sorted:
                if event["risk_level"] not in {"high", "critical"}:
                    continue
                for facility in event_facilities.get(event["id"], []):
                    high_risk_facilities[facility["facility_id"]] = facility

            payload = {
                "frame_time": frame_time.isoformat(),
                "window_start": frame_start.isoformat(),
                "event_count": len(frame_events),
                "high_risk_facility_count": len(high_risk_facilities),
                "events": [
                    {
                        "id": event["id"],
                        "source": event["source"],
                        "event_type": event["event_type"],
                        "severity": event["severity"],
                        "title": event["title"],
                        "event_time": event["event_time_raw"],
                        "risk_score": event["risk_score"],
                        "risk_level": event["risk_level"],
                        "geometry": event["geometry"],
                    }
                    for event in frame_events_sorted
                ],
                "high_risk_facilities": list(high_risk_facilities.values())[:120],
            }

            cache_upserts.append(
                {
                    "frame_key": frame_key,
                    "scene_id": scene_id,
                    "window": window,
                    "step_minutes": step_minutes,
                    "frame_time": frame_time.isoformat(),
                    "payload": json.dumps(payload, ensure_ascii=False),
                }
            )
            frames.append(payload)

        if cache_upserts:
            with self.conn.engine.begin() as write_conn:
                for row in cache_upserts:
                    write_conn.execute(
                        text(
                            """
                            INSERT INTO playback_frame_cache (
                                frame_key, scene_id, window, step_minutes, frame_time, payload, generated_at
                            )
                            VALUES (
                                :frame_key, :scene_id, :window, :step_minutes, :frame_time, :payload, CURRENT_TIMESTAMP
                            )
                            ON CONFLICT(frame_key)
                            DO UPDATE SET
                                payload = excluded.payload,
                                generated_at = CURRENT_TIMESTAMP
                            """
                        ),
                        row,
                    )

        return {
            "scene_id": scene_id,
            "window": window,
            "step_minutes": step_minutes,
            "start_time": start_dt.isoformat(),
            "end_time": end_dt.isoformat(),
            "frames": frames,
            "cache": {"hits": cached_hits, "misses": cache_misses},
            "data_quality": {
                "source_mode": source_mode,
                "fallback_reason": fallback_reason,
                "offline_used": offline_used,
                "event_count_used": len(filtered_events),
                "timeline_end_mode": anchor_mode,
            },
        }

    def _load_offline_events_for_playback(
        self,
        *,
        start_dt: datetime,
        end_dt: datetime,
        source: str | None,
    ) -> list[dict[str, Any]]:
        events: dict[str, dict[str, Any]] = {}
        file_names = ("events_last30d.json", "events_last7d.json", "events_last24h.json")
        for file_name in file_names:
            target = OFFLINE_CACHE_DIR / file_name
            if not target.exists():
                continue
            try:
                payload = json.loads(target.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                continue
            rows = payload.get("items")
            if not isinstance(rows, list):
                continue
            for row in rows:
                if not isinstance(row, dict):
                    continue
                event_time = _as_datetime(row.get("event_time"))
                if event_time is None or event_time < start_dt or event_time > end_dt:
                    continue
                row_source = str(row.get("source") or "")
                if source and row_source != source:
                    continue
                event_id = str(row.get("id") or "")
                external_id = str(row.get("external_id") or "")
                event_key = event_id or external_id
                if not event_key:
                    event_key = hashlib.sha1(
                        f"{row_source}|{row.get('title') or ''}|{row.get('event_time') or ''}".encode("utf-8")
                    ).hexdigest()
                severity = str(row.get("severity") or "low")
                fallback_score = _severity_component_fallback(severity) + _source_component_fallback(row_source) + 8.0
                geometry = _safe_json(row.get("geometry"))
                if geometry is None:
                    geometry = _safe_json(row.get("geometry_json"))
                events[event_key] = {
                    "id": event_key,
                    "source": row_source,
                    "event_type": row.get("event_type"),
                    "severity": severity,
                    "title": str(row.get("title") or "离线历史事件"),
                    "event_time": event_time,
                    "event_time_raw": row.get("event_time"),
                    "geometry": geometry,
                    "risk_score": float(row.get("risk_score") if row.get("risk_score") is not None else fallback_score),
                    "risk_level": row.get("risk_level") or _risk_level_from_score(fallback_score),
                }
        return list(events.values())

    def risk_snapshot(self, window: str) -> dict[str, Any]:
        rows = self.conn.execute(
            text(
                """
                SELECT region_level, region_name, window, snapshot_time, total_events, high_events, medium_events, low_events, weighted_score
                FROM risk_snapshot
                WHERE window = :window
                  AND snapshot_time = (SELECT MAX(snapshot_time) FROM risk_snapshot WHERE window = :window)
                ORDER BY region_level, weighted_score DESC
                """
            ),
            {"window": window},
        ).mappings().all()
        return {"window": window, "items": [dict(row) for row in rows]}

    def risk_timeline(self, window: str, region_level: str, region_name: str | None) -> dict[str, Any]:
        conditions = ["window = :window", "region_level = :region_level"]
        params: dict[str, Any] = {"window": window, "region_level": region_level}
        if region_name:
            conditions.append("region_name = :region_name")
            params["region_name"] = region_name
        else:
            conditions.append("region_name = 'Jiangxi'")
        where_sql = " AND ".join(conditions)
        rows = self.conn.execute(
            text(
                f"""
                SELECT region_name, bucket_start, event_count, weighted_score
                FROM risk_timeline
                WHERE {where_sql}
                ORDER BY bucket_start
                """
            ),
            params,
        ).mappings().all()
        return {"window": window, "region_level": region_level, "series": [dict(row) for row in rows]}

    def system_health(self) -> dict[str, Any]:
        if self.backend == "sqlite":
            db_version = self.conn.execute(text("SELECT sqlite_version()")).scalar_one()
            db_info = {"sqlite": str(db_version)}
        else:
            db_version = self.conn.execute(text("SELECT version()")).scalar_one()
            postgis_version = self.conn.execute(text("SELECT PostGIS_Full_Version()")).scalar_one()
            db_info = {"postgres": str(db_version), "postgis": str(postgis_version)}
        dataset_rows = self.conn.execute(
            text(
                """
                SELECT dataset_key, dataset_version, metadata, updated_at
                FROM data_versions
                ORDER BY dataset_key
                """
            )
        ).mappings().all()
        table_counts = self.conn.execute(
            text(
                """
                SELECT 'boundary_jx' AS table_name, COUNT(*) AS count FROM boundary_jx
                UNION ALL SELECT 'dem_tiles', COUNT(*) FROM dem_tiles
                UNION ALL SELECT 'dem_derivatives', COUNT(*) FROM dem_derivatives
                UNION ALL SELECT 'baker_facilities', COUNT(*) FROM baker_facilities
                UNION ALL SELECT 'event_normalized', COUNT(*) FROM event_normalized
                UNION ALL SELECT 'event_enriched', COUNT(*) FROM event_enriched
                UNION ALL SELECT 'playback_frame_cache', COUNT(*) FROM playback_frame_cache
                UNION ALL SELECT 'analysis_job_log', COUNT(*) FROM analysis_job_log
                UNION ALL SELECT 'connector_health_history', COUNT(*) FROM connector_health_history
                """
            )
        ).mappings().all()
        connectors = self.conn.execute(
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
        runtime = {
            "last_sync_time": self.conn.execute(
                text("SELECT MAX(finished_at) FROM sync_job_log WHERE finished_at IS NOT NULL")
            ).scalar_one(),
            "connector_anomaly_count_24h": int(
                self.conn.execute(
                    text(
                        """
                        SELECT COUNT(*)
                        FROM connector_health_history
                        WHERE status IN ('failed', 'circuit_open')
                          AND recorded_at >= datetime('now', '-24 hour')
                        """
                    )
                ).scalar_one()
            ),
            "last_analysis_time": self.conn.execute(
                text("SELECT MAX(finished_at) FROM analysis_job_log WHERE finished_at IS NOT NULL")
            ).scalar_one(),
        }
        return {
            "status": "ok",
            "database": db_info,
            "datasets": [
                {
                    "dataset_key": row["dataset_key"],
                    "dataset_version": row["dataset_version"],
                    "metadata": _safe_json(row["metadata"]),
                    "updated_at": row["updated_at"],
                }
                for row in dataset_rows
            ],
            "table_counts": [{"table_name": row["table_name"], "count": int(row["count"])} for row in table_counts],
            "connectors": [dict(row) for row in connectors],
            "runtime": runtime,
        }

    def system_monitor(self, *, hours: int) -> dict[str, Any]:
        history_window = max(1, min(hours, 24 * 30))
        sync_rows = self.conn.execute(
            text(
                """
                SELECT connector, status, started_at, finished_at, records_fetched, records_inserted, error_message
                FROM sync_job_log
                WHERE started_at >= datetime('now', :window_expr)
                ORDER BY started_at DESC
                LIMIT 2000
                """
            ),
            {"window_expr": f"-{history_window} hour"},
        ).mappings().all()
        connector_health_rows = self.conn.execute(
            text(
                """
                SELECT connector, status, attempt, latency_ms, circuit_open, message, recorded_at
                FROM connector_health_history
                WHERE recorded_at >= datetime('now', :window_expr)
                ORDER BY recorded_at DESC
                LIMIT 4000
                """
            ),
            {"window_expr": f"-{history_window} hour"},
        ).mappings().all()
        analysis_rows = self.conn.execute(
            text(
                """
                SELECT job_name, started_at, finished_at, status, analyzed_count, failed_count, model_used, error_message
                FROM analysis_job_log
                WHERE started_at >= datetime('now', :window_expr)
                ORDER BY started_at DESC
                LIMIT 1000
                """
            ),
            {"window_expr": f"-{history_window} hour"},
        ).mappings().all()
        throughput_rows = self.conn.execute(
            text(
                """
                SELECT
                    strftime('%Y-%m-%dT%H:00:00Z', ingestion_time) AS bucket,
                    COUNT(*) AS event_count
                FROM event_normalized
                WHERE ingestion_time >= datetime('now', :window_expr)
                GROUP BY bucket
                ORDER BY bucket DESC
                LIMIT 720
                """
            ),
            {"window_expr": f"-{history_window} hour"},
        ).mappings().all()

        connector_summary: dict[str, dict[str, Any]] = {}
        for row in connector_health_rows:
            connector = str(row["connector"])
            current = connector_summary.setdefault(
                connector,
                {"success": 0, "failed": 0, "skipped": 0, "circuit_open": 0, "total": 0, "latency_ms_sum": 0.0},
            )
            status = str(row["status"])
            current[status] = int(current.get(status, 0)) + 1
            current["total"] += 1
            current["latency_ms_sum"] += float(row["latency_ms"] or 0.0)

        connector_availability = []
        for connector, stats in sorted(connector_summary.items()):
            total = int(stats["total"])
            ok = int(stats.get("success", 0) + stats.get("skipped", 0))
            availability = (ok / total) if total else 0.0
            avg_latency = (stats["latency_ms_sum"] / total) if total else 0.0
            connector_availability.append(
                {
                    "connector": connector,
                    "availability": round(availability, 4),
                    "avg_latency_ms": round(avg_latency, 2),
                    "counts": {
                        "success": int(stats.get("success", 0)),
                        "failed": int(stats.get("failed", 0)),
                        "skipped": int(stats.get("skipped", 0)),
                        "circuit_open": int(stats.get("circuit_open", 0)),
                        "total": total,
                    },
                }
            )

        return {
            "window_hours": history_window,
            "sync_jobs": [dict(row) for row in sync_rows[:200]],
            "analysis_jobs": [dict(row) for row in analysis_rows[:200]],
            "connector_health": [dict(row) for row in connector_health_rows[:400]],
            "connector_availability": connector_availability,
            "event_throughput": [dict(row) for row in throughput_rows],
        }

    def _dem_derivative_path(self, derivative: str) -> Path:
        row = self.conn.execute(
            text(
                """
                SELECT raster_path
                FROM dem_derivatives
                WHERE derivative_type = :derivative
                ORDER BY loaded_at DESC
                LIMIT 1
                """
            ),
            {"derivative": derivative},
        ).mappings().first()
        if row is None or not row.get("raster_path"):
            raise ValueError(f"未找到 DEM 派生图层: {derivative}")
        raster_rel = str(row["raster_path"]).replace("\\", "/")
        raster_abs = (PROJECT_ROOT / raster_rel).resolve()
        if not raster_abs.exists():
            raise ValueError(f"DEM 栅格文件不存在: {raster_rel}")
        return raster_abs

    def dem_tile_png(self, *, derivative: str, z: int, x: int, y: int, tile_size: int = 256) -> bytes:
        if derivative not in {"hillshade", "slope", "aspect", "roughness"}:
            raise ValueError(f"不支持的 DEM 派生图层: {derivative}")

        raster_path = self._dem_derivative_path(derivative)
        bounds = mercantile.bounds(x, y, z)
        dst_transform = from_bounds(bounds.west, bounds.south, bounds.east, bounds.north, tile_size, tile_size)
        dst = np.full((tile_size, tile_size), np.nan, dtype="float32")

        with rasterio.open(raster_path) as src:
            src_nodata = src.nodata
            reproject(
                source=rasterio.band(src, 1),
                destination=dst,
                src_transform=src.transform,
                src_crs=src.crs,
                src_nodata=src_nodata,
                dst_transform=dst_transform,
                dst_crs="EPSG:4326",
                dst_nodata=np.nan,
                resampling=Resampling.bilinear,
            )

        mask = np.isfinite(dst)
        if src_nodata is not None:
            mask &= ~np.isclose(dst, float(src_nodata))
        if not np.any(mask):
            return b""

        values = np.zeros_like(dst, dtype="float32")
        if derivative == "hillshade":
            values[mask] = np.clip(dst[mask], 0.0, 255.0)
        elif derivative == "aspect":
            values[mask] = (np.mod(dst[mask], 360.0) / 360.0) * 255.0
        else:
            raw = dst[mask]
            low = float(np.nanpercentile(raw, 5))
            high = float(np.nanpercentile(raw, 95))
            if high <= low:
                values[mask] = 0.0
            else:
                values[mask] = np.clip((raw - low) / (high - low) * 255.0, 0.0, 255.0)

        norm = np.clip(values / 255.0, 0.0, 1.0)
        red = np.zeros_like(values, dtype="uint8")
        green = np.zeros_like(values, dtype="uint8")
        blue = np.zeros_like(values, dtype="uint8")

        if derivative == "hillshade":
            red_src = np.interp(norm, [0.0, 0.22, 0.45, 0.7, 1.0], [12, 28, 72, 180, 244])
            green_src = np.interp(norm, [0.0, 0.22, 0.45, 0.7, 1.0], [36, 63, 116, 198, 248])
            blue_src = np.interp(norm, [0.0, 0.22, 0.45, 0.7, 1.0], [78, 112, 164, 214, 252])
        elif derivative == "aspect":
            red_src = np.interp(norm, [0.0, 0.33, 0.66, 1.0], [220, 255, 90, 220])
            green_src = np.interp(norm, [0.0, 0.33, 0.66, 1.0], [95, 170, 255, 95])
            blue_src = np.interp(norm, [0.0, 0.33, 0.66, 1.0], [80, 90, 210, 80])
        elif derivative == "slope":
            red_src = np.interp(norm, [0.0, 0.5, 1.0], [32, 255, 245])
            green_src = np.interp(norm, [0.0, 0.5, 1.0], [88, 178, 95])
            blue_src = np.interp(norm, [0.0, 0.5, 1.0], [120, 65, 66])
        else:
            red_src = np.interp(norm, [0.0, 1.0], [34, 248])
            green_src = np.interp(norm, [0.0, 1.0], [72, 226])
            blue_src = np.interp(norm, [0.0, 1.0], [94, 124])

        red[mask] = np.clip(red_src[mask], 0, 255).astype("uint8")
        green[mask] = np.clip(green_src[mask], 0, 255).astype("uint8")
        blue[mask] = np.clip(blue_src[mask], 0, 255).astype("uint8")
        alpha = np.where(mask, np.uint8(216), np.uint8(0))
        rgba = np.stack([red, green, blue, alpha], axis=0)

        with MemoryFile() as mem:
            with warnings.catch_warnings():
                warnings.filterwarnings("ignore", category=NotGeoreferencedWarning)
                with mem.open(driver="PNG", width=tile_size, height=tile_size, count=4, dtype="uint8") as dst_ds:
                    dst_ds.write(rgba)
            return mem.read()

    def layer_geojson(self, layer: str, limit: int = 5000) -> dict[str, Any]:
        if layer == "boundary":
            rows = self.conn.execute(text("SELECT name, iso3, geom_json FROM boundary_jx")).mappings().all()
            return {
                "type": "FeatureCollection",
                "features": [
                    {
                        "type": "Feature",
                        "properties": {"name": row["name"], "iso3": row["iso3"]},
                        "geometry": _safe_json(row["geom_json"]),
                    }
                    for row in rows
                ],
            }
        if layer == "facilities":
            rows = self.conn.execute(
                text(
                    """
                    SELECT id, facility_id, facility_type, source_layer, name, start_year, status, admin_city, geom_json
                    FROM baker_facilities
                    ORDER BY facility_id
                    LIMIT :limit
                    """
                ),
                {"limit": limit},
            ).mappings().all()
            return {
                "type": "FeatureCollection",
                "features": [
                    {
                        "type": "Feature",
                        "properties": {
                            "id": row["id"],
                            "facility_id": row["facility_id"],
                            "facility_type": row["facility_type"],
                            "source_layer": row["source_layer"],
                            "name": row["name"],
                            "start_year": row["start_year"],
                            "status": row["status"],
                            "admin_city": row["admin_city"],
                        },
                        "geometry": _safe_json(row["geom_json"]),
                    }
                    for row in rows
                ],
            }
        if layer == "events":
            rows = self.conn.execute(
                text(
                    """
                    SELECT id, source, event_type, severity, title, event_time, geometry_json
                    FROM event_normalized
                    WHERE geometry_json IS NOT NULL
                    ORDER BY event_time DESC
                    LIMIT :limit
                    """
                ),
                {"limit": limit},
            ).mappings().all()
            return {
                "type": "FeatureCollection",
                "features": [
                    {
                        "type": "Feature",
                        "properties": {
                            "id": row["id"],
                            "source": row["source"],
                            "event_type": row["event_type"],
                            "severity": row["severity"],
                            "title": row["title"],
                            "event_time": row["event_time"],
                        },
                        "geometry": _safe_json(row["geometry_json"]),
                    }
                    for row in rows
                ],
            }
        raise ValueError(f"不支持的图层: {layer}")

    def mvt_tile(self, *, layer: str, z: int, x: int, y: int) -> bytes:
        bounds = mercantile.bounds(x, y, z)
        tile_bbox = box(bounds.west, bounds.south, bounds.east, bounds.north)

        if layer == "boundary":
            rows = self.conn.execute(text("SELECT name, iso3, geom_json FROM boundary_jx")).mappings().all()
            features = []
            for row in rows:
                geom = shape(_safe_json(row["geom_json"]))
                if geom.is_empty or not geom.intersects(tile_bbox):
                    continue
                clipped = geom.intersection(tile_bbox)
                if clipped.is_empty:
                    continue
                features.append(
                    {
                        "geometry": mapping(clipped),
                        "properties": {"name": row["name"], "iso3": row["iso3"]},
                        "id": row["name"],
                    }
                )
        elif layer == "facilities":
            rows = self.conn.execute(
                text(
                    """
                    SELECT id, facility_id, facility_type, status, geom_json
                    FROM baker_facilities
                    """
                )
            ).mappings().all()
            features = []
            for row in rows:
                geom = shape(_safe_json(row["geom_json"]))
                if geom.is_empty or not geom.intersects(tile_bbox):
                    continue
                clipped = geom.intersection(tile_bbox)
                if clipped.is_empty:
                    continue
                features.append(
                    {
                        "geometry": mapping(clipped),
                        "properties": {
                            "facility_id": row["facility_id"],
                            "facility_type": row["facility_type"],
                            "status": row["status"],
                        },
                        "id": row["id"],
                    }
                )
        elif layer == "events":
            rows = self.conn.execute(
                text(
                    """
                    SELECT id, source, event_type, severity, event_time, geometry_json
                    FROM event_normalized
                    WHERE geometry_json IS NOT NULL
                    """
                )
            ).mappings().all()
            features = []
            for row in rows:
                geom = shape(_safe_json(row["geometry_json"]))
                if geom.is_empty or not geom.intersects(tile_bbox):
                    continue
                clipped = geom.intersection(tile_bbox)
                if clipped.is_empty:
                    continue
                features.append(
                    {
                        "geometry": mapping(clipped),
                        "properties": {
                            "source": row["source"],
                            "event_type": row["event_type"],
                            "severity": row["severity"],
                            "event_time": row["event_time"],
                        },
                        "id": row["id"],
                    }
                )
        else:
            raise ValueError(f"不支持的瓦片图层: {layer}")

        if not features:
            return b""

        return mapbox_vector_tile.encode(
            [
                {
                    "name": layer,
                    "features": features,
                }
            ],
            default_options={
                "quantize_bounds": (bounds.west, bounds.south, bounds.east, bounds.north),
                "extents": 4096,
                "y_coord_down": False,
            },
        )
