from __future__ import annotations

import hashlib
import json
import logging
import math
import os
import socket
import sys
import time
from collections import defaultdict
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import requests
from shapely.geometry import Point, shape
from sqlalchemy import text
from sqlalchemy.engine import Engine

from ..config import Settings
from .analysis import EventAnalysisService
from .connectors import (
    AisPortStubConnector,
    BaseConnector,
    EnergyAnnouncementConnector,
    EnergyMarketConnector,
    GdeltEventsConnector,
    NasaFirmsConnector,
    NormalizedEvent,
    UsgsEarthquakeConnector,
)

logger = logging.getLogger(__name__)

CHINA_MAINLAND_BOUNDS = (73.0, 18.0, 135.5, 54.5)
CHINA_TEXT_KEYWORDS = (
    "china",
    "chinese",
    "mainland",
    "beijing",
    "shanghai",
    "guangdong",
    "jiangxi",
    "中国",
    "大陆",
    "北京",
    "上海",
    "广东",
    "江西",
)
NON_GEO_ALWAYS_ALLOW_SOURCES = {"energy_market", "energy_announcement"}
STARTUP_OFFLINE_PROBES: tuple[tuple[str, int], ...] = (("1.1.1.1", 53), ("223.5.5.5", 53))
STARTUP_NETWORK_CONNECTORS = {
    "usgs_earthquake",
    "gdelt_events",
    "energy_market",
    "energy_announcement",
}


def _resolve_backend_dir() -> Path:
    runtime_root = os.environ.get("GISDATAMONITOR_RUNTIME_ROOT", "").strip()
    if runtime_root:
        return (Path(runtime_root).resolve() / "app" / "backend").resolve()
    if getattr(sys, "frozen", False):
        return (Path(sys.executable).resolve().parent / "app" / "backend").resolve()
    return Path(__file__).resolve().parents[3]


BACKEND_DIR = _resolve_backend_dir()
CONNECTOR_CACHE_DIR = BACKEND_DIR / "cache" / "connectors"
OFFLINE_CACHE_DIR = BACKEND_DIR / "cache" / "offline"


def _as_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=UTC)
    text_value = str(value).strip().replace("Z", "+00:00")
    if not text_value:
        return None
    try:
        dt = datetime.fromisoformat(text_value)
        return dt if dt.tzinfo else dt.replace(tzinfo=UTC)
    except ValueError:
        return None


def _haversine_km(lon1: float, lat1: float, lon2: float, lat2: float) -> float:
    r = 6371.0
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return r * c


def _weighted_score(high: int, medium: int, low: int) -> float:
    return float(high * 3 + medium * 2 + low)


def _seed_ratio(seed: str, salt: str) -> float:
    digest = hashlib.sha1(f"{seed}:{salt}".encode("utf-8")).digest()
    return int.from_bytes(digest[:4], byteorder="big", signed=False) / 0xFFFFFFFF


def _synthetic_mainland_point(seed: str) -> tuple[float, float]:
    lon_min, lat_min, lon_max, lat_max = CHINA_MAINLAND_BOUNDS
    lon = (lon_min + 1.0) + _seed_ratio(seed, "lon") * ((lon_max - lon_min) - 2.0)
    lat = (lat_min + 1.0) + _seed_ratio(seed, "lat") * ((lat_max - lat_min) - 2.0)
    return lon, lat


class EventSyncService:
    def __init__(self, engine: Engine, settings: Settings) -> None:
        self.engine = engine
        self.settings = settings
        self.backend = engine.url.get_backend_name()
        self.analysis_service = EventAnalysisService(engine=engine, settings=settings)
        self.connectors: list[BaseConnector] = [
            UsgsEarthquakeConnector(settings),
            NasaFirmsConnector(settings),
            GdeltEventsConnector(settings),
            EnergyMarketConnector(settings),
            EnergyAnnouncementConnector(settings),
            AisPortStubConnector(settings),
        ]
        self.connector_state: dict[str, dict[str, Any]] = {
            connector.name: {"consecutive_failures": 0, "circuit_until": None}
            for connector in self.connectors
        }

    def _build_session(self) -> requests.Session:
        session = requests.Session()
        session.headers.update({"User-Agent": "gisdatamonitor-sync/0.2"})
        proxies = self.settings.request_proxies
        if proxies:
            session.proxies.update(proxies)
        return session

    def _connector_cache_path(self, connector_name: str) -> Path:
        safe_name = "".join(ch for ch in connector_name if ch.isalnum() or ch in {"_", "-"}).strip("_-")
        safe_name = safe_name or "connector"
        CONNECTOR_CACHE_DIR.mkdir(parents=True, exist_ok=True)
        return CONNECTOR_CACHE_DIR / f"{safe_name}.json"

    def _serialize_record(self, record: NormalizedEvent) -> dict[str, Any]:
        return {
            "source": record.source,
            "external_id": record.external_id,
            "event_type": record.event_type,
            "severity": record.severity,
            "title": record.title,
            "description": record.description,
            "event_time": record.event_time.astimezone(UTC).isoformat(),
            "longitude": record.longitude,
            "latitude": record.latitude,
            "properties": record.properties if isinstance(record.properties, dict) else {},
            "raw_payload": record.raw_payload if isinstance(record.raw_payload, dict) else {"value": record.raw_payload},
        }

    def _deserialize_record(self, payload: dict[str, Any]) -> NormalizedEvent | None:
        if not isinstance(payload, dict):
            return None
        event_time = _as_datetime(payload.get("event_time")) or datetime.now(tz=UTC)
        source = str(payload.get("source") or "")
        external_id = str(payload.get("external_id") or "")
        longitude = float(payload["longitude"]) if payload.get("longitude") is not None else None
        latitude = float(payload["latitude"]) if payload.get("latitude") is not None else None
        properties = payload.get("properties") if isinstance(payload.get("properties"), dict) else {}
        if source == "gdelt_events" and (longitude is None or latitude is None):
            seed = external_id or str(payload.get("title") or "")
            if seed:
                longitude, latitude = _synthetic_mainland_point(seed)
                properties = dict(properties)
                properties["synthetic_geo"] = True
        return NormalizedEvent(
            source=source,
            external_id=external_id,
            event_type=str(payload.get("event_type") or "cached_event"),
            severity=str(payload.get("severity") or "low"),
            title=str(payload.get("title") or "离线历史事件"),
            description=str(payload.get("description") or ""),
            event_time=event_time,
            longitude=longitude,
            latitude=latitude,
            properties=properties,
            raw_payload=payload.get("raw_payload") if isinstance(payload.get("raw_payload"), dict) else {},
        )

    def _save_connector_cache(self, connector_name: str, records: list[NormalizedEvent]) -> None:
        if not records:
            return
        cache_path = self._connector_cache_path(connector_name)
        try:
            serialized = [self._serialize_record(record) for record in records[:5000]]
            cache_payload = {
                "connector": connector_name,
                "cached_at": datetime.now(tz=UTC).isoformat(),
                "records": serialized,
            }
            cache_path.write_text(json.dumps(cache_payload, ensure_ascii=False), encoding="utf-8")
        except Exception as exc:  # noqa: BLE001
            logger.warning("save connector cache failed: %s (%s)", connector_name, exc)

    def _load_connector_cache(self, connector_name: str) -> list[NormalizedEvent]:
        cache_path = self._connector_cache_path(connector_name)
        if not cache_path.exists():
            return self._load_connector_cache_from_db(connector_name)
        try:
            payload = json.loads(cache_path.read_text(encoding="utf-8"))
        except Exception as exc:  # noqa: BLE001
            logger.warning("read connector cache failed: %s (%s)", connector_name, exc)
            return self._load_connector_cache_from_db(connector_name)
        rows = payload.get("records") if isinstance(payload, dict) else None
        if not isinstance(rows, list):
            return self._load_connector_cache_from_db(connector_name)
        records: list[NormalizedEvent] = []
        for row in rows:
            item = self._deserialize_record(row)
            if item is None:
                continue
            if not item.external_id:
                continue
            records.append(item)
        return records or self._load_connector_cache_from_db(connector_name)

    def _load_connector_cache_from_db(self, connector_name: str, limit: int = 1200) -> list[NormalizedEvent]:
        with self.engine.connect() as conn:
            rows = conn.execute(
                text(
                    """
                    SELECT
                        source,
                        external_id,
                        event_type,
                        severity,
                        title,
                        description,
                        event_time,
                        longitude,
                        latitude,
                        properties
                    FROM event_normalized
                    WHERE source = :source
                    ORDER BY event_time DESC
                    LIMIT :limit
                    """
                ),
                {"source": connector_name, "limit": max(50, int(limit))},
            ).mappings().all()
        records: list[NormalizedEvent] = []
        for row in rows:
            event_time = _as_datetime(row.get("event_time")) or datetime.now(tz=UTC)
            properties_raw = row.get("properties")
            properties: dict[str, Any] = {}
            if isinstance(properties_raw, dict):
                properties = properties_raw
            elif isinstance(properties_raw, str):
                try:
                    parsed = json.loads(properties_raw)
                    if isinstance(parsed, dict):
                        properties = parsed
                except json.JSONDecodeError:
                    properties = {}
            external_id = str(row.get("external_id") or "").strip()
            if not external_id:
                continue
            source = str(row.get("source") or connector_name)
            longitude = float(row["longitude"]) if row.get("longitude") is not None else None
            latitude = float(row["latitude"]) if row.get("latitude") is not None else None
            if source == "gdelt_events" and (longitude is None or latitude is None):
                longitude, latitude = _synthetic_mainland_point(external_id)
                properties = dict(properties)
                properties["synthetic_geo"] = True
            records.append(
                NormalizedEvent(
                    source=source,
                    external_id=external_id,
                    event_type=str(row.get("event_type") or "cached_event"),
                    severity=str(row.get("severity") or "low"),
                    title=str(row.get("title") or "离线历史事件"),
                    description=str(row.get("description") or ""),
                    event_time=event_time,
                    longitude=longitude,
                    latitude=latitude,
                    properties=properties,
                    raw_payload={"fallback": "event_normalized_cache"},
                )
            )
        return records

    def _should_use_offline_cache(self, reason: str | None) -> bool:
        text_value = str(reason or "").lower()
        if not text_value:
            return False
        markers = (
            "超时",
            "timeout",
            "timed out",
            "请求失败",
            "connection",
            "network",
            "ssl",
            "temporary",
            "offline",
            "429",
            "rate limit",
            "too many requests",
            "限流",
        )
        return any(marker in text_value for marker in markers)

    def _try_offline_cache_fallback(
        self,
        *,
        connector_name: str,
        log_id: int,
        attempt: int,
        latency_ms: float,
        reason: str,
    ) -> dict[str, Any] | None:
        cached_records = self._load_connector_cache(connector_name)
        if not cached_records:
            return None
        inserted = self._persist_events(cached_records)
        message = f"{reason}; 离线回退命中 {len(cached_records)} 条历史记录"
        self._log_finish(
            log_id=log_id,
            status="success",
            records_fetched=len(cached_records),
            records_inserted=inserted,
            error_message=message,
        )
        self._record_connector_health(
            connector=connector_name,
            status="success",
            attempt=attempt,
            latency_ms=latency_ms,
            circuit_open=False,
            message="offline_cache_fallback",
        )
        return {
            "connector": connector_name,
            "status": "offline_cache",
            "fetched": len(cached_records),
            "inserted": inserted,
            "reason": reason,
        }

    def _is_network_reachable(self, timeout_sec: int) -> bool:
        timeout = max(1.0, float(timeout_sec))
        for host, port in STARTUP_OFFLINE_PROBES:
            try:
                with socket.create_connection((host, port), timeout=timeout):
                    return True
            except OSError:
                continue
        return False

    def _run_startup_offline_fallback(self, connector: BaseConnector) -> dict[str, Any]:
        log_id = self._log_start(connector=connector.name, attempt=1)
        fallback = self._try_offline_cache_fallback(
            connector_name=connector.name,
            log_id=log_id,
            attempt=1,
            latency_ms=0.0,
            reason="startup_offline_fast_skip",
        )
        if fallback is not None:
            return fallback
        message = "startup_offline_fast_skip_no_cache"
        self._log_finish(
            log_id=log_id,
            status="skipped",
            records_fetched=0,
            records_inserted=0,
            error_message=message,
        )
        self._record_connector_health(
            connector=connector.name,
            status="skipped",
            attempt=1,
            latency_ms=0.0,
            circuit_open=False,
            message=message,
        )
        return {"connector": connector.name, "status": "skipped", "reason": message}

    def run_cycle(self, *, startup_mode: bool = False) -> dict[str, Any]:
        started_at = datetime.now(tz=UTC)
        started_perf = time.perf_counter()
        summary: dict[str, Any] = {
            "started_at": started_at.isoformat(),
            "startup_mode": startup_mode,
            "connectors": [],
        }
        summary["stale_running_fixed"] = self._finalize_stale_running_jobs(max_age_minutes=20)

        original_timeout = self.settings.connector_timeout_sec
        original_max_retry = self.settings.sync_max_retry
        startup_offline_fast_skip = False
        if startup_mode:
            self.settings.connector_timeout_sec = max(1, int(self.settings.startup_connector_timeout_sec))
            self.settings.sync_max_retry = max(0, int(self.settings.startup_max_retry))
            if self.settings.startup_offline_fast_skip:
                probe_timeout = max(1, int(self.settings.startup_probe_timeout_sec))
                startup_offline_fast_skip = not self._is_network_reachable(timeout_sec=probe_timeout)
                if startup_offline_fast_skip:
                    logger.info(
                        "[启动同步] 网络探测离线，启用离线快速跳过；连接器超时=%ss，重试=%s",
                        self.settings.connector_timeout_sec,
                        self.settings.sync_max_retry,
                    )
                else:
                    logger.info(
                        "[启动同步] 网络探测在线，按在线模式同步；连接器超时=%ss，重试=%s",
                        self.settings.connector_timeout_sec,
                        self.settings.sync_max_retry,
                    )
            logger.info("[启动同步] 连接器总数=%s", len(self.connectors))
        summary["startup_offline_fast_skip"] = startup_offline_fast_skip

        session = self._build_session()
        try:
            total_connectors = len(self.connectors)
            for index, connector in enumerate(self.connectors, start=1):
                connector_started = time.perf_counter()
                if startup_mode:
                    logger.info("[启动同步] 连接器 %s/%s 开始: %s", index, total_connectors, connector.name)
                if startup_mode and startup_offline_fast_skip and connector.name in STARTUP_NETWORK_CONNECTORS:
                    connector_summary = self._run_startup_offline_fallback(connector)
                else:
                    connector_summary = self._run_connector_with_retry(session=session, connector=connector)
                summary["connectors"].append(connector_summary)
                if startup_mode:
                    status = str(connector_summary.get("status") or "unknown")
                    fetched = int(connector_summary.get("fetched") or 0)
                    inserted = int(connector_summary.get("inserted") or 0)
                    reason = str(connector_summary.get("reason") or connector_summary.get("error") or "")
                    reason_msg = f" | 原因={reason}" if reason else ""
                    logger.info(
                        "[启动同步] 连接器 %s/%s 结束: %s | 状态=%s | 抓取=%s | 入库=%s | 耗时=%.1fms%s",
                        index,
                        total_connectors,
                        connector.name,
                        status,
                        fetched,
                        inserted,
                        (time.perf_counter() - connector_started) * 1000.0,
                        reason_msg,
                    )

            skip_heavy_refresh = bool(startup_mode and self.settings.startup_skip_heavy_refresh)
            summary["skip_heavy_refresh"] = skip_heavy_refresh
            if skip_heavy_refresh:
                summary["purged_non_mainland"] = "skipped_on_startup"
                summary["analysis"] = {"status": "skipped", "reason": "startup_skip_heavy_refresh"}
                logger.info("[启动同步] 轻量模式：跳过重计算（purge/link/risk/analysis），优先保证服务就绪")
            else:
                summary["purged_non_mainland"] = self._purge_non_mainland_events()
                self._refresh_facility_links()
                self._refresh_risk_snapshot()
                self._refresh_risk_timeline()
                summary["analysis"] = self.analysis_service.run_cycle()
            summary["offline_snapshots"] = self._refresh_offline_event_snapshots()
            summary["maintenance"] = self._run_storage_maintenance()
        finally:
            session.close()
            if startup_mode:
                self.settings.connector_timeout_sec = original_timeout
                self.settings.sync_max_retry = original_max_retry

        summary["finished_at"] = datetime.now(tz=UTC).isoformat()
        summary["elapsed_sec"] = time.perf_counter() - started_perf
        if startup_mode:
            status_bins = defaultdict(int)
            for item in summary["connectors"]:
                status_bins[str(item.get("status") or "unknown")] += 1
            logger.info(
                "[启动同步] 完成: 总耗时=%.2fs | success=%s | skipped=%s | offline_cache=%s | failed=%s",
                summary["elapsed_sec"],
                status_bins.get("success", 0),
                status_bins.get("skipped", 0),
                status_bins.get("offline_cache", 0),
                status_bins.get("failed", 0),
            )
        return summary

    def _refresh_offline_event_snapshots(self) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for hours, file_name in (
            (24, "events_last24h.json"),
            (24 * 7, "events_last7d.json"),
            (24 * 30, "events_last30d.json"),
        ):
            try:
                result[file_name] = self._write_offline_event_snapshot(hours=hours, file_name=file_name)
            except Exception as exc:  # noqa: BLE001
                logger.warning("offline snapshot refresh failed: %s (%s)", file_name, exc)
                result[file_name] = {"status": "failed", "error": str(exc)}
        return result

    def _write_offline_event_snapshot(self, *, hours: int, file_name: str, limit: int = 6000) -> dict[str, Any]:
        cutoff = datetime.now(tz=UTC) - timedelta(hours=max(1, int(hours)))
        with self.engine.connect() as conn:
            rows = conn.execute(
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
                        e.ingestion_time,
                        e.properties,
                        e.geometry_json,
                        ee.risk_score,
                        ee.risk_level,
                        ee.summary_zh,
                        ee.summary_en
                    FROM event_normalized e
                    LEFT JOIN event_enriched ee ON ee.event_id = e.id
                    WHERE e.event_time >= :cutoff
                    ORDER BY e.event_time DESC
                    LIMIT :limit
                    """
                ),
                {"cutoff": cutoff.isoformat(), "limit": max(100, int(limit))},
            ).mappings().all()

        payload = {
            "generated_at": datetime.now(tz=UTC).isoformat(),
            "window_hours": int(hours),
            "event_count": len(rows),
            "items": [dict(row) for row in rows],
        }
        OFFLINE_CACHE_DIR.mkdir(parents=True, exist_ok=True)
        target = OFFLINE_CACHE_DIR / file_name
        target.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
        return {"status": "ok", "event_count": len(rows), "file": str(target)}

    def _run_storage_maintenance(self) -> dict[str, Any]:
        if not self.settings.maintenance_enabled:
            return {"status": "skipped", "reason": "maintenance_disabled"}
        if self.backend != "sqlite":
            return {"status": "skipped", "reason": "non_sqlite_backend"}

        deleted: dict[str, int] = {}
        retention_rules = [
            ("sync_job_log", "started_at", max(1, int(self.settings.sync_log_retention_days))),
            ("connector_health_history", "recorded_at", max(1, int(self.settings.connector_health_retention_days))),
            ("analysis_job_log", "started_at", max(1, int(self.settings.analysis_log_retention_days))),
            ("playback_frame_cache", "generated_at", max(1, int(self.settings.playback_cache_retention_days))),
            ("event_raw", "fetched_at", max(1, int(self.settings.event_raw_retention_days))),
        ]

        row_caps = [
            ("sync_job_log", 20000),
            ("connector_health_history", 50000),
            ("analysis_job_log", 20000),
            ("playback_frame_cache", 60000),
        ]

        with self.engine.begin() as conn:
            for table, column, days in retention_rules:
                result = conn.execute(
                    text(
                        f"""
                        DELETE FROM {table}
                        WHERE {column} < datetime('now', :retention_expr)
                        """
                    ),
                    {"retention_expr": f"-{days} day"},
                )
                deleted[f"{table}_retention"] = int(result.rowcount or 0)

            for table, max_rows in row_caps:
                total_rows = int(conn.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar_one())
                overflow = max(total_rows - max_rows, 0)
                if overflow <= 0:
                    deleted[f"{table}_cap"] = 0
                    continue
                result = conn.execute(
                    text(
                        f"""
                        DELETE FROM {table}
                        WHERE id IN (
                            SELECT id
                            FROM {table}
                            ORDER BY id ASC
                            LIMIT :overflow
                        )
                        """
                    ),
                    {"overflow": overflow},
                )
                deleted[f"{table}_cap"] = int(result.rowcount or 0)

        pragma_info: dict[str, Any] = {}
        if self.backend == "sqlite":
            with self.engine.connect() as conn:
                conn.execute(text("PRAGMA optimize"))
                checkpoint = conn.execute(text("PRAGMA wal_checkpoint(PASSIVE)")).fetchone()
                if checkpoint is not None:
                    pragma_info["wal_checkpoint"] = list(checkpoint)

        return {"status": "ok", "deleted": deleted, "pragma": pragma_info}

    def _finalize_stale_running_jobs(self, *, max_age_minutes: int) -> int:
        cutoff = (datetime.now(tz=UTC) - timedelta(minutes=max_age_minutes)).strftime("%Y-%m-%d %H:%M:%S")
        with self.engine.begin() as conn:
            result = conn.execute(
                text(
                    """
                    UPDATE sync_job_log
                    SET
                        status = 'failed',
                        finished_at = COALESCE(finished_at, CURRENT_TIMESTAMP),
                        error_message = COALESCE(NULLIF(error_message, ''), 'interrupted_or_aborted')
                    WHERE status = 'running'
                      AND started_at < :cutoff
                    """
                ),
                {"cutoff": cutoff},
            )
        return int(result.rowcount or 0)

    def _run_connector_with_retry(self, session: requests.Session, connector: BaseConnector) -> dict[str, Any]:
        now = datetime.now(tz=UTC)
        state = self.connector_state.setdefault(connector.name, {"consecutive_failures": 0, "circuit_until": None})
        circuit_until = state.get("circuit_until")
        if isinstance(circuit_until, datetime) and now < circuit_until:
            message = f"circuit open until {circuit_until.isoformat()}"
            log_id = self._log_start(connector=connector.name, attempt=1)
            self._log_finish(
                log_id=log_id,
                status="skipped",
                records_fetched=0,
                records_inserted=0,
                error_message=message,
            )
            self._record_connector_health(
                connector=connector.name,
                status="circuit_open",
                attempt=1,
                latency_ms=0.0,
                circuit_open=True,
                message=message,
            )
            return {"connector": connector.name, "status": "circuit_open", "reason": message}

        attempts = max(1, self.settings.sync_max_retry + 1)
        last_error: str | None = None
        recovered = bool(state.get("consecutive_failures", 0))
        for attempt in range(1, attempts + 1):
            log_id = self._log_start(connector=connector.name, attempt=attempt)
            started = time.perf_counter()
            try:
                result = connector.fetch(session)
                latency_ms = (time.perf_counter() - started) * 1000.0
                if result.skipped:
                    if self._should_use_offline_cache(result.skip_reason):
                        fallback = self._try_offline_cache_fallback(
                            connector_name=connector.name,
                            log_id=log_id,
                            attempt=attempt,
                            latency_ms=latency_ms,
                            reason=str(result.skip_reason or "connector skipped"),
                        )
                        if fallback is not None:
                            state["consecutive_failures"] = 0
                            state["circuit_until"] = None
                            return fallback
                    self._log_finish(
                        log_id=log_id,
                        status="skipped",
                        records_fetched=0,
                        records_inserted=0,
                        error_message=result.skip_reason,
                    )
                    self._record_connector_health(
                        connector=connector.name,
                        status="skipped",
                        attempt=attempt,
                        latency_ms=latency_ms,
                        circuit_open=False,
                        message=result.skip_reason,
                    )
                    return {"connector": connector.name, "status": "skipped", "reason": result.skip_reason}

                inserted = self._persist_events(records=result.records)
                self._save_connector_cache(connector.name, result.records)
                self._log_finish(
                    log_id=log_id,
                    status="success",
                    records_fetched=len(result.records),
                    records_inserted=inserted,
                    error_message=None,
                )
                self._record_connector_health(
                    connector=connector.name,
                    status="success",
                    attempt=attempt,
                    latency_ms=latency_ms,
                    circuit_open=False,
                    message="recovered_with_backfill_window" if recovered else None,
                )
                state["consecutive_failures"] = 0
                state["circuit_until"] = None
                return {
                    "connector": connector.name,
                    "status": "success",
                    "fetched": len(result.records),
                    "inserted": inserted,
                    "recovered": recovered,
                }
            except Exception as exc:  # noqa: BLE001
                latency_ms = (time.perf_counter() - started) * 1000.0
                last_error = str(exc)
                if isinstance(exc, requests.RequestException):
                    logger.warning(
                        "connector sync failed: %s (%s: %s)",
                        connector.name,
                        type(exc).__name__,
                        exc,
                    )
                else:
                    logger.exception("connector sync failed: %s", connector.name)
                if attempt >= attempts:
                    fallback = self._try_offline_cache_fallback(
                        connector_name=connector.name,
                        log_id=log_id,
                        attempt=attempt,
                        latency_ms=latency_ms,
                        reason=last_error or "connector exception",
                    )
                    if fallback is not None:
                        state["consecutive_failures"] = 0
                        state["circuit_until"] = None
                        return fallback
                self._log_finish(
                    log_id=log_id,
                    status="failed",
                    records_fetched=0,
                    records_inserted=0,
                    error_message=last_error,
                )
                if attempt >= attempts:
                    self._record_connector_health(
                        connector=connector.name,
                        status="failed",
                        attempt=attempt,
                        latency_ms=latency_ms,
                        circuit_open=False,
                        message=last_error,
                    )
                    break

        state["consecutive_failures"] = int(state.get("consecutive_failures", 0)) + 1
        if state["consecutive_failures"] >= self.settings.sync_failure_open_circuit_threshold:
            open_until = datetime.now(tz=UTC) + timedelta(minutes=self.settings.sync_circuit_open_minutes)
            state["circuit_until"] = open_until
            self._record_connector_health(
                connector=connector.name,
                status="circuit_open",
                attempt=attempts,
                latency_ms=0.0,
                circuit_open=True,
                message=f"opened until {open_until.isoformat()}",
            )

        return {
            "connector": connector.name,
            "status": "failed",
            "error": last_error,
            "attempts": attempts,
            "consecutive_failures": state["consecutive_failures"],
        }

    def _validate_event(self, record: NormalizedEvent) -> tuple[bool, str | None]:
        if not record.external_id.strip():
            return False, "missing_external_id"
        if record.severity not in {"low", "medium", "high"}:
            return False, "invalid_severity"
        if record.longitude is not None and (record.longitude < -180 or record.longitude > 180):
            return False, "invalid_longitude"
        if record.latitude is not None and (record.latitude < -90 or record.latitude > 90):
            return False, "invalid_latitude"
        return True, None

    def _is_point_in_mainland_china(self, lon: float | None, lat: float | None) -> bool:
        if lon is None or lat is None:
            return False
        min_lon, min_lat, max_lon, max_lat = CHINA_MAINLAND_BOUNDS
        return min_lon <= float(lon) <= max_lon and min_lat <= float(lat) <= max_lat

    def _contains_china_keyword(self, text_value: str) -> bool:
        lower = text_value.lower()
        return any(keyword in lower for keyword in CHINA_TEXT_KEYWORDS)

    def _ensure_renderable_coordinates(self, record: NormalizedEvent) -> None:
        if record.longitude is not None and record.latitude is not None:
            return
        if record.source not in NON_GEO_ALWAYS_ALLOW_SOURCES:
            return
        seed = (record.external_id or record.title or "").strip()
        if not seed:
            return
        lon, lat = _synthetic_mainland_point(seed)
        record.longitude = lon
        record.latitude = lat
        if isinstance(record.properties, dict):
            record.properties.setdefault("synthetic_geo", True)

    def _is_mainland_china_event(self, record: NormalizedEvent) -> bool:
        if record.source in NON_GEO_ALWAYS_ALLOW_SOURCES:
            return True
        if self._is_point_in_mainland_china(record.longitude, record.latitude):
            return True

        candidate_parts = [
            record.title or "",
            record.description or "",
            record.event_type or "",
            str(record.properties.get("sourcecountry") or ""),
            str(record.properties.get("country") or ""),
            str(record.properties.get("location") or ""),
        ]
        joined = " ".join(candidate_parts)
        return self._contains_china_keyword(joined)

    def _is_mainland_china_row(self, row: dict[str, Any]) -> bool:
        source = str(row.get("source") or "")
        if source in NON_GEO_ALWAYS_ALLOW_SOURCES:
            return True
        if self._is_point_in_mainland_china(row.get("longitude"), row.get("latitude")):
            return True
        properties = row.get("properties")
        properties_obj = properties if isinstance(properties, dict) else {}
        if isinstance(properties, str):
            try:
                parsed = json.loads(properties)
                if isinstance(parsed, dict):
                    properties_obj = parsed
            except json.JSONDecodeError:
                properties_obj = {}
        candidate_parts = [
            str(row.get("title") or ""),
            str(row.get("description") or ""),
            str(row.get("event_type") or ""),
            str(properties_obj.get("sourcecountry") or ""),
            str(properties_obj.get("country") or ""),
            str(properties_obj.get("location") or ""),
        ]
        return self._contains_china_keyword(" ".join(candidate_parts))

    def _event_id(self, record: NormalizedEvent) -> str:
        value = f"{record.source}|{record.external_id}"
        return hashlib.sha1(value.encode("utf-8")).hexdigest()

    def _event_geometry_json(self, record: NormalizedEvent) -> str | None:
        if record.longitude is None or record.latitude is None:
            return None
        return json.dumps({"type": "Point", "coordinates": [record.longitude, record.latitude]}, ensure_ascii=False)

    def _persist_events(self, records: list[NormalizedEvent]) -> int:
        if not records:
            return 0
        inserted = 0
        with self.engine.begin() as conn:
            for record in records:
                self._ensure_renderable_coordinates(record)
                valid, reason = self._validate_event(record)
                event_time = record.event_time.astimezone(UTC).isoformat()
                geometry_json = self._event_geometry_json(record)
                raw_payload = record.raw_payload if isinstance(record.raw_payload, dict) else {"value": record.raw_payload}
                if not self._is_mainland_china_event(record):
                    conn.execute(
                        text(
                            """
                            INSERT INTO event_raw (
                                source, external_id, fetched_at, event_time, payload, longitude, latitude, geometry_json, status
                            )
                            VALUES (
                                :source, :external_id, CURRENT_TIMESTAMP, :event_time, :payload, :longitude, :latitude, :geometry_json, 'filtered_non_mainland'
                            )
                            """
                        ),
                        {
                            "source": record.source,
                            "external_id": record.external_id,
                            "event_time": event_time,
                            "payload": json.dumps(raw_payload, ensure_ascii=False),
                            "longitude": record.longitude,
                            "latitude": record.latitude,
                            "geometry_json": geometry_json,
                        },
                    )
                    continue

                if not valid:
                    quarantined_payload = {"quarantine_reason": reason, "raw": raw_payload}
                    conn.execute(
                        text(
                            """
                            INSERT INTO event_raw (
                                source, external_id, fetched_at, event_time, payload, longitude, latitude, geometry_json, status
                            )
                            VALUES (
                                :source, :external_id, CURRENT_TIMESTAMP, :event_time, :payload, :longitude, :latitude, :geometry_json, 'quarantined'
                            )
                            """
                        ),
                        {
                            "source": record.source,
                            "external_id": record.external_id or f"missing:{hashlib.sha1(event_time.encode('utf-8')).hexdigest()}",
                            "event_time": event_time,
                            "payload": json.dumps(quarantined_payload, ensure_ascii=False),
                            "longitude": record.longitude,
                            "latitude": record.latitude,
                            "geometry_json": geometry_json,
                        },
                    )
                    continue

                event_id = self._event_id(record)
                conn.execute(
                    text(
                        """
                        INSERT INTO event_raw (
                            source, external_id, fetched_at, event_time, payload, longitude, latitude, geometry_json, status
                        )
                        VALUES (
                            :source, :external_id, CURRENT_TIMESTAMP, :event_time, :payload, :longitude, :latitude, :geometry_json, 'ok'
                        )
                        """
                    ),
                    {
                        "source": record.source,
                        "external_id": record.external_id,
                        "event_time": event_time,
                        "payload": json.dumps(raw_payload, ensure_ascii=False),
                        "longitude": record.longitude,
                        "latitude": record.latitude,
                        "geometry_json": geometry_json,
                    },
                )
                result = conn.execute(
                    text(
                        """
                        INSERT INTO event_normalized (
                            id, source, external_id, event_type, severity, title, description, event_time, ingestion_time,
                            properties, longitude, latitude, geometry_json
                        )
                        VALUES (
                            :id, :source, :external_id, :event_type, :severity, :title, :description, :event_time, CURRENT_TIMESTAMP,
                            :properties, :longitude, :latitude, :geometry_json
                        )
                        ON CONFLICT(source, external_id)
                        DO UPDATE SET
                            event_type = excluded.event_type,
                            severity = excluded.severity,
                            title = excluded.title,
                            description = excluded.description,
                            event_time = excluded.event_time,
                            ingestion_time = CURRENT_TIMESTAMP,
                            properties = excluded.properties,
                            longitude = excluded.longitude,
                            latitude = excluded.latitude,
                            geometry_json = excluded.geometry_json
                        WHERE
                            event_normalized.event_type IS NOT excluded.event_type
                            OR event_normalized.severity IS NOT excluded.severity
                            OR event_normalized.title IS NOT excluded.title
                            OR event_normalized.description IS NOT excluded.description
                            OR event_normalized.event_time IS NOT excluded.event_time
                            OR event_normalized.properties IS NOT excluded.properties
                            OR event_normalized.longitude IS NOT excluded.longitude
                            OR event_normalized.latitude IS NOT excluded.latitude
                            OR event_normalized.geometry_json IS NOT excluded.geometry_json
                        """
                    ),
                    {
                        "id": event_id,
                        "source": record.source,
                        "external_id": record.external_id,
                        "event_type": record.event_type,
                        "severity": record.severity,
                        "title": record.title,
                        "description": record.description,
                        "event_time": event_time,
                        "properties": json.dumps(record.properties, ensure_ascii=False),
                        "longitude": record.longitude,
                        "latitude": record.latitude,
                        "geometry_json": geometry_json,
                    },
                )
                inserted += int(result.rowcount or 0)
        return inserted

    def _purge_non_mainland_events(self) -> int:
        with self.engine.begin() as conn:
            rows = conn.execute(
                text(
                    """
                    SELECT id, title, description, event_type, properties, longitude, latitude
                    FROM event_normalized
                    """
                )
            ).mappings().all()
            drop_ids = [str(row["id"]) for row in rows if not self._is_mainland_china_row(dict(row))]
            if not drop_ids:
                return 0
            for event_id in drop_ids:
                conn.execute(text("DELETE FROM facility_event_link WHERE event_id = :event_id"), {"event_id": event_id})
                conn.execute(text("DELETE FROM event_enriched WHERE event_id = :event_id"), {"event_id": event_id})
                conn.execute(text("DELETE FROM event_normalized WHERE id = :event_id"), {"event_id": event_id})
            return len(drop_ids)

    def _refresh_facility_links(self) -> None:
        now = datetime.now(tz=UTC)
        min_time = now - timedelta(days=30)
        radius_km = self.settings.event_link_radius_km
        with self.engine.begin() as conn:
            facilities = conn.execute(
                text("SELECT id, geom_json FROM baker_facilities WHERE geom_json IS NOT NULL")
            ).mappings().all()
            events = conn.execute(
                text(
                    """
                    SELECT id, event_time, geometry_json
                    FROM event_normalized
                    WHERE geometry_json IS NOT NULL
                    """
                )
            ).mappings().all()
            conn.execute(text("DELETE FROM facility_event_link"))

            event_points: list[dict[str, Any]] = []
            for event in events:
                event_time = _as_datetime(event["event_time"])
                if event_time is None or event_time < min_time:
                    continue
                point = shape(json.loads(event["geometry_json"]))
                if not isinstance(point, Point):
                    point = point.representative_point()
                event_points.append({"id": event["id"], "lon": float(point.x), "lat": float(point.y)})

            for facility in facilities:
                geom = shape(json.loads(facility["geom_json"]))
                f_point = geom.representative_point()
                flon, flat = float(f_point.x), float(f_point.y)
                for event in event_points:
                    distance_km = _haversine_km(flon, flat, event["lon"], event["lat"])
                    if distance_km <= radius_km:
                        conn.execute(
                            text(
                                """
                                INSERT OR IGNORE INTO facility_event_link (facility_pk, event_id, distance_km, linked_at)
                                VALUES (:facility_pk, :event_id, :distance_km, CURRENT_TIMESTAMP)
                                """
                            ),
                            {
                                "facility_pk": facility["id"],
                                "event_id": event["id"],
                                "distance_km": distance_km,
                            },
                        )

    def _refresh_risk_snapshot(self) -> None:
        now = datetime.now(tz=UTC)
        windows = {"24h": timedelta(hours=24), "7d": timedelta(days=7), "30d": timedelta(days=30)}
        with self.engine.begin() as conn:
            events = conn.execute(
                text("SELECT id, severity, event_time FROM event_normalized")
            ).mappings().all()
            links = conn.execute(
                text(
                    """
                    SELECT l.event_id, COALESCE(NULLIF(f.admin_city, ''), 'UNKNOWN') AS admin_city
                    FROM facility_event_link l
                    JOIN baker_facilities f ON f.id = l.facility_pk
                    """
                )
            ).mappings().all()

            event_map: dict[str, dict[str, Any]] = {}
            for event in events:
                event_time = _as_datetime(event["event_time"])
                if event_time is None:
                    continue
                event_map[str(event["id"])] = {"severity": str(event["severity"]), "event_time": event_time}

            conn.execute(text("DELETE FROM risk_snapshot"))
            for window_name, delta in windows.items():
                cutoff = now - delta
                filtered = [e for e in event_map.values() if e["event_time"] >= cutoff]
                high = sum(1 for e in filtered if e["severity"] == "high")
                medium = sum(1 for e in filtered if e["severity"] == "medium")
                low = sum(1 for e in filtered if e["severity"] == "low")
                conn.execute(
                    text(
                        """
                        INSERT INTO risk_snapshot (
                            region_level, region_name, window, snapshot_time,
                            total_events, high_events, medium_events, low_events, weighted_score
                        )
                        VALUES (
                            'province', 'Jiangxi', :window, CURRENT_TIMESTAMP,
                            :total_events, :high_events, :medium_events, :low_events, :weighted_score
                        )
                        """
                    ),
                    {
                        "window": window_name,
                        "total_events": len(filtered),
                        "high_events": high,
                        "medium_events": medium,
                        "low_events": low,
                        "weighted_score": _weighted_score(high, medium, low),
                    },
                )

                city_bins: dict[str, dict[str, int]] = defaultdict(lambda: {"high": 0, "medium": 0, "low": 0})
                for link in links:
                    event_data = event_map.get(str(link["event_id"]))
                    if event_data is None or event_data["event_time"] < cutoff:
                        continue
                    city = str(link["admin_city"])
                    sev = event_data["severity"]
                    city_bins[city][sev] = city_bins[city].get(sev, 0) + 1
                for city, counts in city_bins.items():
                    high = counts.get("high", 0)
                    medium = counts.get("medium", 0)
                    low = counts.get("low", 0)
                    total = high + medium + low
                    conn.execute(
                        text(
                            """
                            INSERT INTO risk_snapshot (
                                region_level, region_name, window, snapshot_time,
                                total_events, high_events, medium_events, low_events, weighted_score
                            )
                            VALUES (
                                'city', :city, :window, CURRENT_TIMESTAMP,
                                :total_events, :high_events, :medium_events, :low_events, :weighted_score
                            )
                            """
                        ),
                        {
                            "city": city,
                            "window": window_name,
                            "total_events": total,
                            "high_events": high,
                            "medium_events": medium,
                            "low_events": low,
                            "weighted_score": _weighted_score(high, medium, low),
                        },
                    )

    def _refresh_risk_timeline(self) -> None:
        now = datetime.now(tz=UTC)
        min_time = now - timedelta(days=30)
        with self.engine.begin() as conn:
            rows = conn.execute(
                text("SELECT event_time, severity FROM event_normalized")
            ).mappings().all()
            bucket: dict[str, dict[str, int]] = defaultdict(lambda: {"high": 0, "medium": 0, "low": 0})
            for row in rows:
                event_time = _as_datetime(row["event_time"])
                if event_time is None or event_time < min_time:
                    continue
                day_key = event_time.astimezone(UTC).strftime("%Y-%m-%dT00:00:00+00:00")
                severity = str(row["severity"])
                if severity not in ("high", "medium", "low"):
                    severity = "low"
                bucket[day_key][severity] += 1

            conn.execute(
                text(
                    """
                    DELETE FROM risk_timeline
                    WHERE window = '30d' AND region_level = 'province' AND region_name = 'Jiangxi'
                    """
                )
            )
            for day in sorted(bucket.keys()):
                high = bucket[day]["high"]
                medium = bucket[day]["medium"]
                low = bucket[day]["low"]
                conn.execute(
                    text(
                        """
                        INSERT INTO risk_timeline (
                            region_level, region_name, bucket_start, window, event_count, weighted_score
                        )
                        VALUES (
                            'province', 'Jiangxi', :bucket_start, '30d', :event_count, :weighted_score
                        )
                        """
                    ),
                    {
                        "bucket_start": day,
                        "event_count": high + medium + low,
                        "weighted_score": _weighted_score(high, medium, low),
                    },
                )

    def _log_start(self, connector: str, attempt: int) -> int:
        with self.engine.begin() as conn:
            conn.execute(
                text(
                    """
                    INSERT INTO sync_job_log (job_name, connector, status, attempt, started_at)
                    VALUES ('event_sync', :connector, 'running', :attempt, CURRENT_TIMESTAMP)
                    """
                ),
                {"connector": connector, "attempt": attempt},
            )
            return int(conn.execute(text("SELECT last_insert_rowid()")).scalar_one())

    def _log_finish(
        self,
        *,
        log_id: int,
        status: str,
        records_fetched: int,
        records_inserted: int,
        error_message: str | None,
    ) -> None:
        with self.engine.begin() as conn:
            conn.execute(
                text(
                    """
                    UPDATE sync_job_log
                    SET
                        status = :status,
                        records_fetched = :records_fetched,
                        records_inserted = :records_inserted,
                        finished_at = CURRENT_TIMESTAMP,
                        error_message = :error_message
                    WHERE id = :log_id
                    """
                ),
                {
                    "status": status,
                    "records_fetched": records_fetched,
                    "records_inserted": records_inserted,
                    "error_message": error_message,
                    "log_id": log_id,
                },
            )

    def _record_connector_health(
        self,
        *,
        connector: str,
        status: str,
        attempt: int,
        latency_ms: float,
        circuit_open: bool,
        message: str | None,
    ) -> None:
        with self.engine.begin() as conn:
            conn.execute(
                text(
                    """
                    INSERT INTO connector_health_history (
                        connector, status, attempt, latency_ms, circuit_open, message, recorded_at
                    )
                    VALUES (
                        :connector, :status, :attempt, :latency_ms, :circuit_open, :message, CURRENT_TIMESTAMP
                    )
                    """
                ),
                {
                    "connector": connector,
                    "status": status,
                    "attempt": attempt,
                    "latency_ms": latency_ms,
                    "circuit_open": 1 if circuit_open else 0,
                    "message": message,
                },
            )
