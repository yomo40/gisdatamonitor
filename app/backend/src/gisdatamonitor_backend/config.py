from __future__ import annotations

from functools import lru_cache
from typing import Literal

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="GISDATAMONITOR_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    app_name: str = "GISDataMonitor 后端服务"
    env: Literal["dev", "test", "prod"] = "dev"
    api_prefix: str = "/api/v1"
    timezone: str = "Asia/Shanghai"
    frontend_origin: str = "*"

    database_url: str = "sqlite:///./gisdatamonitor.sqlite3"

    http_proxy: str = ""
    https_proxy: str = ""
    connector_timeout_sec: int = 30

    scheduler_enabled: bool = True
    sync_interval_minutes: int = 10
    sync_run_on_startup: bool = True
    sync_max_retry: int = 2
    startup_offline_fast_skip: bool = True
    startup_probe_timeout_sec: int = 1
    startup_connector_timeout_sec: int = 6
    startup_max_retry: int = 0
    startup_skip_heavy_refresh: bool = True
    browser_wait_ready_timeout_sec: int = 900
    event_link_radius_km: float = 80.0
    sync_failure_open_circuit_threshold: int = 3
    sync_circuit_open_minutes: int = 10

    jiangxi_bbox: str = "113.67845,24.58754,118.59998,30.14784"

    usgs_feed_url: str = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_week.geojson"
    gdelt_enabled: bool = True
    gdelt_query: str = "(earthquake OR wildfire OR flood OR energy OR power OR refinery) AND (china OR jiangxi)"
    gdelt_max_records: int = 200
    gdelt_fallback_max_records: int = 120
    gdelt_timeout_sec: int = 12
    gdelt_timespan: str = "7days"
    gdelt_rate_limit_cooldown_minutes: int = 20
    firms_api_key: str = ""
    firms_feed_template: str = (
        "https://firms.modaps.eosdis.nasa.gov/api/area/csv/{api_key}/VIIRS_NOAA20_NRT/{bbox}/1"
    )

    ais_feed_url: str = ""
    port_feed_url: str = ""

    analysis_enabled: bool = True
    analysis_max_events_per_cycle: int = 2000
    analysis_recheck_hours: int = 6
    analysis_use_llm: bool = True
    analysis_llm_base_url: str = ""
    analysis_llm_api_key: str = ""
    analysis_llm_model: str = "gpt-4o-mini"
    analysis_llm_timeout_sec: int = 25

    maintenance_enabled: bool = True
    sync_log_retention_days: int = 14
    connector_health_retention_days: int = 14
    analysis_log_retention_days: int = 30
    playback_cache_retention_days: int = 14
    event_raw_retention_days: int = 14

    @property
    def database_dsn(self) -> str:
        return self.database_url.replace("+psycopg", "")

    @property
    def database_backend(self) -> str:
        return "sqlite" if self.database_url.startswith("sqlite") else "postgres"

    @property
    def jiangxi_bbox_csv(self) -> str:
        return ",".join(str(x) for x in self.jiangxi_bbox_values)

    @property
    def jiangxi_bbox_values(self) -> tuple[float, float, float, float]:
        parts = [segment.strip() for segment in self.jiangxi_bbox.split(",")]
        if len(parts) != 4:
            raise ValueError("GISDATAMONITOR_JIANGXI_BBOX must contain 4 comma-separated values.")
        return (float(parts[0]), float(parts[1]), float(parts[2]), float(parts[3]))

    @property
    def request_proxies(self) -> dict[str, str]:
        proxies: dict[str, str] = {}
        if self.http_proxy:
            proxies["http"] = self.http_proxy
        if self.https_proxy:
            proxies["https"] = self.https_proxy
        return proxies


@lru_cache
def get_settings() -> Settings:
    return Settings()
