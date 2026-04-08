from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

import requests
from sqlalchemy import text
from sqlalchemy.engine import Engine

from ..config import Settings

logger = logging.getLogger(__name__)


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


def _severity_base(severity: str) -> float:
    return {
        "high": 68.0,
        "medium": 46.0,
        "low": 22.0,
    }.get(severity, 22.0)


def _source_weight(source: str) -> float:
    source_map = {
        "usgs_earthquake": 16.0,
        "nasa_firms": 14.0,
        "gdelt_events": 8.0,
        "energy_market": 12.0,
        "energy_announcement": 10.0,
        "ais_port_stub": 6.0,
    }
    return source_map.get(source, 8.0)


def _proximity_component(min_distance_km: float | None) -> float:
    if min_distance_km is None:
        return 4.0
    if min_distance_km <= 20:
        return 16.0
    if min_distance_km <= 50:
        return 11.0
    if min_distance_km <= 80:
        return 7.0
    return 3.0


def _recency_component(event_time: datetime, now: datetime) -> float:
    age = now - event_time
    if age <= timedelta(hours=6):
        return 14.0
    if age <= timedelta(hours=24):
        return 10.0
    if age <= timedelta(days=3):
        return 7.0
    if age <= timedelta(days=7):
        return 4.0
    return 2.0


def _risk_level(risk_score: float) -> str:
    if risk_score >= 85:
        return "critical"
    if risk_score >= 65:
        return "high"
    if risk_score >= 40:
        return "medium"
    return "low"


@dataclass(slots=True)
class RuleAnalysisResult:
    risk_score: float
    risk_level: str
    risk_reason: str
    summary_zh: str
    summary_en: str
    impact_tags: list[str]
    severity_component: float
    proximity_component: float
    recency_component: float
    source_component: float
    confidence: float
    model_provider: str = "rule"
    analysis_version: str = "v1"


class EventAnalysisService:
    def __init__(self, engine: Engine, settings: Settings) -> None:
        self.engine = engine
        self.settings = settings

    def run_cycle(self) -> dict[str, Any]:
        if not self.settings.analysis_enabled:
            return {"status": "skipped", "reason": "分析功能已禁用"}

        log_id = self._log_start()
        analyzed_count = 0
        failed_count = 0
        model_used = "rule"
        try:
            rows = self._fetch_candidate_events(limit=self.settings.analysis_max_events_per_cycle)
            if self._llm_enabled():
                model_used = f"rule+{self.settings.analysis_llm_model}"
            with self.engine.begin() as conn:
                now = datetime.now(tz=UTC)
                llm_budget = 24
                for row in rows:
                    try:
                        analysis = self._analyze_rule(row=row, now=now)
                        if self._llm_enabled() and llm_budget > 0 and analysis.risk_level in {"high", "critical"}:
                            llm_result = self._analyze_with_llm(row=row, rule=analysis)
                            if llm_result is not None:
                                analysis = llm_result
                                llm_budget -= 1
                        self._upsert_enriched(conn=conn, row=row, analysis=analysis)
                        analyzed_count += 1
                    except Exception:  # noqa: BLE001
                        failed_count += 1
                        logger.exception("event analysis failed for event_id=%s", row.get("id"))
            self._log_finish(
                log_id=log_id,
                status="success",
                analyzed_count=analyzed_count,
                failed_count=failed_count,
                model_used=model_used,
                error_message=None,
            )
            return {
                "status": "success",
                "analyzed_count": analyzed_count,
                "failed_count": failed_count,
                "model_used": model_used,
            }
        except Exception as exc:  # noqa: BLE001
            self._log_finish(
                log_id=log_id,
                status="failed",
                analyzed_count=analyzed_count,
                failed_count=failed_count + 1,
                model_used=model_used,
                error_message=str(exc),
            )
            logger.exception("analysis cycle failed")
            return {
                "status": "failed",
                "error": str(exc),
                "analyzed_count": analyzed_count,
                "failed_count": failed_count + 1,
            }

    def _fetch_candidate_events(self, limit: int) -> list[dict[str, Any]]:
        refresh_hours = max(1, int(self.settings.analysis_recheck_hours))
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
                        e.properties,
                        MIN(l.distance_km) AS min_distance_km
                    FROM event_normalized e
                    LEFT JOIN facility_event_link l ON l.event_id = e.id
                    LEFT JOIN event_enriched ee ON ee.event_id = e.id
                    WHERE e.event_time >= datetime('now', '-30 day')
                      AND (
                          ee.event_id IS NULL
                          OR e.ingestion_time > ee.updated_at
                          OR ee.updated_at <= datetime('now', :refresh_expr)
                      )
                    GROUP BY e.id, e.source, e.event_type, e.severity, e.title, e.description, e.event_time, e.properties
                    ORDER BY e.event_time DESC
                    LIMIT :limit
                    """
                ),
                {"limit": limit, "refresh_expr": f"-{refresh_hours} hour"},
            ).mappings().all()
        return [dict(row) for row in rows]

    def _analyze_rule(self, row: dict[str, Any], now: datetime) -> RuleAnalysisResult:
        event_time = _as_datetime(row.get("event_time")) or now
        severity = str(row.get("severity") or "low")
        source = str(row.get("source") or "unknown")
        event_type = str(row.get("event_type") or "unknown_event")
        title = str(row.get("title") or event_type)
        description = str(row.get("description") or "")
        min_distance = row.get("min_distance_km")
        min_distance_float = float(min_distance) if min_distance is not None else None

        severity_component = _severity_base(severity)
        proximity_component = _proximity_component(min_distance_float)
        recency_component = _recency_component(event_time, now)
        source_component = _source_weight(source)
        risk_score = max(0.0, min(100.0, severity_component + proximity_component + recency_component + source_component))
        risk_level = _risk_level(risk_score)

        tags = [event_type, source, severity]
        if min_distance_float is not None and min_distance_float <= self.settings.event_link_radius_km:
            tags.append("near_facility")
        if "earthquake" in event_type:
            tags.append("seismic")
        if "fire" in event_type:
            tags.append("fire")
        if "energy" in event_type or source.startswith("energy_"):
            tags.append("energy_market")

        reason = (
            f"{severity.upper()} baseline {severity_component:.1f}, "
            f"proximity {proximity_component:.1f}, recency {recency_component:.1f}, source {source_component:.1f}."
        )
        summary_en = f"{title}. {description}".strip()[:360]
        summary_zh = f"事件：{title}；影响描述：{description or '暂无详细描述'}".strip()[:220]
        confidence = 0.68 if risk_level in {"high", "critical"} else 0.62

        return RuleAnalysisResult(
            risk_score=risk_score,
            risk_level=risk_level,
            risk_reason=reason,
            summary_zh=summary_zh,
            summary_en=summary_en,
            impact_tags=sorted(set(tags)),
            severity_component=severity_component,
            proximity_component=proximity_component,
            recency_component=recency_component,
            source_component=source_component,
            confidence=confidence,
        )

    def _llm_enabled(self) -> bool:
        return bool(
            self.settings.analysis_use_llm
            and self.settings.analysis_llm_base_url.strip()
            and self.settings.analysis_llm_api_key.strip()
        )

    def _analyze_with_llm(self, row: dict[str, Any], rule: RuleAnalysisResult) -> RuleAnalysisResult | None:
        base_url = self.settings.analysis_llm_base_url.rstrip("/")
        if not base_url:
            return None

        event_id = str(row.get("id"))
        prompt = {
            "event_id": event_id,
            "source": row.get("source"),
            "event_type": row.get("event_type"),
            "severity": row.get("severity"),
            "title": row.get("title"),
            "description": row.get("description"),
            "event_time": row.get("event_time"),
            "min_distance_km": row.get("min_distance_km"),
            "rule_risk_score": rule.risk_score,
            "rule_risk_level": rule.risk_level,
            "rule_reason": rule.risk_reason,
        }

        messages = [
            {
                "role": "system",
                "content": (
                    "You are an energy risk analyst. "
                    "Return compact JSON only with keys: summary_zh, summary_en, risk_reason, impact_tags, confidence."
                ),
            },
            {
                "role": "user",
                "content": json.dumps(prompt, ensure_ascii=False),
            },
        ]
        headers = {
            "Authorization": f"Bearer {self.settings.analysis_llm_api_key}",
            "Content-Type": "application/json",
        }
        response = requests.post(
            f"{base_url}/v1/chat/completions",
            headers=headers,
            json={
                "model": self.settings.analysis_llm_model,
                "temperature": 0.1,
                "response_format": {"type": "json_object"},
                "messages": messages,
            },
            timeout=self.settings.analysis_llm_timeout_sec,
        )
        if response.status_code >= 400:
            logger.warning("llm enhancement failed: status=%s body=%s", response.status_code, response.text[:320])
            return None

        payload = response.json()
        content = (
            ((payload.get("choices") or [{}])[0].get("message") or {}).get("content")
            if isinstance(payload, dict)
            else None
        )
        if not content:
            return None
        parsed = json.loads(content)
        impact_tags = parsed.get("impact_tags")
        if not isinstance(impact_tags, list):
            impact_tags = rule.impact_tags

        summary_zh = str(parsed.get("summary_zh") or rule.summary_zh)[:260]
        summary_en = str(parsed.get("summary_en") or rule.summary_en)[:420]
        risk_reason = str(parsed.get("risk_reason") or rule.risk_reason)[:360]
        confidence = float(parsed.get("confidence") or rule.confidence)
        confidence = max(0.3, min(0.99, confidence))

        return RuleAnalysisResult(
            risk_score=rule.risk_score,
            risk_level=rule.risk_level,
            risk_reason=risk_reason,
            summary_zh=summary_zh,
            summary_en=summary_en,
            impact_tags=[str(x) for x in impact_tags][:10],
            severity_component=rule.severity_component,
            proximity_component=rule.proximity_component,
            recency_component=rule.recency_component,
            source_component=rule.source_component,
            confidence=confidence,
            model_provider=self.settings.analysis_llm_model,
            analysis_version="v1+llm",
        )

    def _upsert_enriched(self, conn: Any, row: dict[str, Any], analysis: RuleAnalysisResult) -> None:
        conn.execute(
            text(
                """
                INSERT INTO event_enriched (
                    event_id,
                    source,
                    event_type,
                    severity,
                    event_time,
                    risk_score,
                    risk_level,
                    risk_reason,
                    summary_zh,
                    summary_en,
                    impact_tags,
                    severity_component,
                    proximity_component,
                    recency_component,
                    source_component,
                    confidence,
                    model_provider,
                    analysis_version,
                    updated_at
                )
                VALUES (
                    :event_id,
                    :source,
                    :event_type,
                    :severity,
                    :event_time,
                    :risk_score,
                    :risk_level,
                    :risk_reason,
                    :summary_zh,
                    :summary_en,
                    :impact_tags,
                    :severity_component,
                    :proximity_component,
                    :recency_component,
                    :source_component,
                    :confidence,
                    :model_provider,
                    :analysis_version,
                    CURRENT_TIMESTAMP
                )
                ON CONFLICT(event_id)
                DO UPDATE SET
                    source = excluded.source,
                    event_type = excluded.event_type,
                    severity = excluded.severity,
                    event_time = excluded.event_time,
                    risk_score = excluded.risk_score,
                    risk_level = excluded.risk_level,
                    risk_reason = excluded.risk_reason,
                    summary_zh = excluded.summary_zh,
                    summary_en = excluded.summary_en,
                    impact_tags = excluded.impact_tags,
                    severity_component = excluded.severity_component,
                    proximity_component = excluded.proximity_component,
                    recency_component = excluded.recency_component,
                    source_component = excluded.source_component,
                    confidence = excluded.confidence,
                    model_provider = excluded.model_provider,
                    analysis_version = excluded.analysis_version,
                    updated_at = CURRENT_TIMESTAMP
                """
            ),
            {
                "event_id": row.get("id"),
                "source": row.get("source"),
                "event_type": row.get("event_type"),
                "severity": row.get("severity"),
                "event_time": row.get("event_time"),
                "risk_score": analysis.risk_score,
                "risk_level": analysis.risk_level,
                "risk_reason": analysis.risk_reason,
                "summary_zh": analysis.summary_zh,
                "summary_en": analysis.summary_en,
                "impact_tags": json.dumps(analysis.impact_tags, ensure_ascii=False),
                "severity_component": analysis.severity_component,
                "proximity_component": analysis.proximity_component,
                "recency_component": analysis.recency_component,
                "source_component": analysis.source_component,
                "confidence": analysis.confidence,
                "model_provider": analysis.model_provider,
                "analysis_version": analysis.analysis_version,
            },
        )

    def _log_start(self) -> int:
        with self.engine.begin() as conn:
            conn.execute(
                text(
                    """
                    INSERT INTO analysis_job_log (job_name, started_at, status)
                    VALUES ('event_enrichment', CURRENT_TIMESTAMP, 'running')
                    """
                )
            )
            return int(conn.execute(text("SELECT last_insert_rowid()")).scalar_one())

    def _log_finish(
        self,
        *,
        log_id: int,
        status: str,
        analyzed_count: int,
        failed_count: int,
        model_used: str,
        error_message: str | None,
    ) -> None:
        with self.engine.begin() as conn:
            conn.execute(
                text(
                    """
                    UPDATE analysis_job_log
                    SET
                        finished_at = CURRENT_TIMESTAMP,
                        status = :status,
                        analyzed_count = :analyzed_count,
                        failed_count = :failed_count,
                        model_used = :model_used,
                        error_message = :error_message
                    WHERE id = :log_id
                    """
                ),
                {
                    "status": status,
                    "analyzed_count": analyzed_count,
                    "failed_count": failed_count,
                    "model_used": model_used,
                    "error_message": error_message,
                    "log_id": log_id,
                },
            )
