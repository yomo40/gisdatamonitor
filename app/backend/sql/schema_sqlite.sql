PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS boundary_jx (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    iso3 TEXT,
    source_name TEXT,
    geom_json TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS dem_tiles (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_collection TEXT NOT NULL,
    source_title TEXT,
    tile_name TEXT NOT NULL,
    tile_path TEXT NOT NULL,
    resolution_m REAL NOT NULL,
    width INTEGER,
    height INTEGER,
    bbox_json TEXT,
    loaded_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (source_collection, tile_name)
);

CREATE TABLE IF NOT EXISTS dem_derivatives (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    derivative_type TEXT NOT NULL CHECK (derivative_type IN ('slope', 'aspect', 'hillshade', 'roughness')),
    raster_path TEXT NOT NULL,
    resolution_m REAL NOT NULL,
    width INTEGER,
    height INTEGER,
    bbox_json TEXT,
    metadata TEXT NOT NULL DEFAULT '{}',
    loaded_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (derivative_type, raster_path)
);

CREATE TABLE IF NOT EXISTS baker_facilities (
    id TEXT PRIMARY KEY,
    facility_id TEXT NOT NULL UNIQUE,
    facility_type TEXT NOT NULL,
    source_layer TEXT NOT NULL,
    name TEXT,
    start_year INTEGER,
    status TEXT,
    admin_city TEXT,
    properties TEXT NOT NULL DEFAULT '{}',
    geom_json TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS baker_facilities_type_idx ON baker_facilities (facility_type);
CREATE INDEX IF NOT EXISTS baker_facilities_year_idx ON baker_facilities (start_year);
CREATE INDEX IF NOT EXISTS baker_facilities_status_idx ON baker_facilities (status);
CREATE INDEX IF NOT EXISTS baker_facilities_city_idx ON baker_facilities (admin_city);
CREATE INDEX IF NOT EXISTS baker_facilities_source_layer_idx ON baker_facilities (source_layer);

CREATE TABLE IF NOT EXISTS facility_terrain_metrics (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    facility_pk TEXT NOT NULL REFERENCES baker_facilities(id) ON DELETE CASCADE,
    elevation_m REAL,
    slope_deg REAL,
    aspect_deg REAL,
    hillshade REAL,
    roughness REAL,
    computed_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (facility_pk)
);

CREATE TABLE IF NOT EXISTS event_raw (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source TEXT NOT NULL,
    external_id TEXT NOT NULL,
    fetched_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    event_time TEXT,
    payload TEXT NOT NULL,
    longitude REAL,
    latitude REAL,
    geometry_json TEXT,
    status TEXT NOT NULL DEFAULT 'ok'
);

CREATE INDEX IF NOT EXISTS event_raw_time_idx ON event_raw (event_time);

CREATE TABLE IF NOT EXISTS event_normalized (
    id TEXT PRIMARY KEY,
    source TEXT NOT NULL,
    external_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    severity TEXT NOT NULL CHECK (severity IN ('low', 'medium', 'high')),
    title TEXT NOT NULL,
    description TEXT,
    event_time TEXT NOT NULL,
    ingestion_time TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    properties TEXT NOT NULL DEFAULT '{}',
    longitude REAL,
    latitude REAL,
    geometry_json TEXT,
    UNIQUE (source, external_id)
);

CREATE INDEX IF NOT EXISTS event_normalized_time_idx ON event_normalized (event_time);
CREATE INDEX IF NOT EXISTS event_normalized_ingestion_idx ON event_normalized (ingestion_time);
CREATE INDEX IF NOT EXISTS event_normalized_source_idx ON event_normalized (source);
CREATE INDEX IF NOT EXISTS event_normalized_source_time_idx ON event_normalized (source, event_time);
CREATE INDEX IF NOT EXISTS event_normalized_type_idx ON event_normalized (event_type);
CREATE INDEX IF NOT EXISTS event_normalized_severity_idx ON event_normalized (severity);

CREATE TABLE IF NOT EXISTS facility_event_link (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    facility_pk TEXT NOT NULL REFERENCES baker_facilities(id) ON DELETE CASCADE,
    event_id TEXT NOT NULL REFERENCES event_normalized(id) ON DELETE CASCADE,
    distance_km REAL NOT NULL,
    linked_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (facility_pk, event_id)
);

CREATE INDEX IF NOT EXISTS facility_event_link_facility_idx ON facility_event_link (facility_pk);
CREATE INDEX IF NOT EXISTS facility_event_link_event_idx ON facility_event_link (event_id);

CREATE TABLE IF NOT EXISTS risk_snapshot (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    region_level TEXT NOT NULL CHECK (region_level IN ('province', 'city')),
    region_name TEXT NOT NULL,
    window TEXT NOT NULL CHECK (window IN ('24h', '7d', '30d')),
    snapshot_time TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    total_events INTEGER NOT NULL DEFAULT 0,
    high_events INTEGER NOT NULL DEFAULT 0,
    medium_events INTEGER NOT NULL DEFAULT 0,
    low_events INTEGER NOT NULL DEFAULT 0,
    weighted_score REAL NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS risk_snapshot_idx ON risk_snapshot (region_level, region_name, window, snapshot_time);

CREATE TABLE IF NOT EXISTS risk_timeline (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    region_level TEXT NOT NULL CHECK (region_level IN ('province', 'city')),
    region_name TEXT NOT NULL,
    bucket_start TEXT NOT NULL,
    window TEXT NOT NULL CHECK (window IN ('24h', '7d', '30d')),
    event_count INTEGER NOT NULL DEFAULT 0,
    weighted_score REAL NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS risk_timeline_idx ON risk_timeline (region_level, region_name, window, bucket_start);

CREATE TABLE IF NOT EXISTS sync_job_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_name TEXT NOT NULL,
    connector TEXT NOT NULL,
    started_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    finished_at TEXT,
    status TEXT NOT NULL CHECK (status IN ('running', 'success', 'failed', 'skipped')),
    attempt INTEGER NOT NULL DEFAULT 1,
    records_fetched INTEGER NOT NULL DEFAULT 0,
    records_inserted INTEGER NOT NULL DEFAULT 0,
    error_message TEXT
);

CREATE INDEX IF NOT EXISTS sync_job_log_idx ON sync_job_log (job_name, connector, started_at);
CREATE INDEX IF NOT EXISTS sync_job_log_started_idx ON sync_job_log (started_at);

CREATE TABLE IF NOT EXISTS data_versions (
    dataset_key TEXT PRIMARY KEY,
    dataset_version TEXT NOT NULL,
    metadata TEXT NOT NULL DEFAULT '{}',
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS event_enriched (
    event_id TEXT PRIMARY KEY REFERENCES event_normalized(id) ON DELETE CASCADE,
    source TEXT NOT NULL,
    event_type TEXT NOT NULL,
    severity TEXT NOT NULL CHECK (severity IN ('low', 'medium', 'high')),
    event_time TEXT NOT NULL,
    risk_score REAL NOT NULL,
    risk_level TEXT NOT NULL CHECK (risk_level IN ('low', 'medium', 'high', 'critical')),
    risk_reason TEXT NOT NULL,
    summary_zh TEXT NOT NULL,
    summary_en TEXT NOT NULL,
    impact_tags TEXT NOT NULL DEFAULT '[]',
    severity_component REAL NOT NULL DEFAULT 0,
    proximity_component REAL NOT NULL DEFAULT 0,
    recency_component REAL NOT NULL DEFAULT 0,
    source_component REAL NOT NULL DEFAULT 0,
    confidence REAL NOT NULL DEFAULT 0.5,
    model_provider TEXT NOT NULL DEFAULT 'rule',
    analysis_version TEXT NOT NULL DEFAULT 'v1',
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS event_enriched_level_idx ON event_enriched (risk_level);
CREATE INDEX IF NOT EXISTS event_enriched_time_idx ON event_enriched (event_time);
CREATE INDEX IF NOT EXISTS event_enriched_source_idx ON event_enriched (source);
CREATE INDEX IF NOT EXISTS event_enriched_updated_idx ON event_enriched (updated_at);

CREATE TABLE IF NOT EXISTS scene_preset (
    scene_id TEXT PRIMARY KEY,
    scene_name TEXT NOT NULL,
    description TEXT,
    config_json TEXT NOT NULL,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS playback_frame_cache (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    frame_key TEXT NOT NULL UNIQUE,
    scene_id TEXT NOT NULL REFERENCES scene_preset(scene_id) ON DELETE CASCADE,
    window TEXT NOT NULL CHECK (window IN ('24h', '7d', '30d')),
    step_minutes INTEGER NOT NULL,
    frame_time TEXT NOT NULL,
    payload TEXT NOT NULL,
    generated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS playback_frame_cache_lookup_idx
    ON playback_frame_cache (scene_id, window, step_minutes, frame_time);

CREATE TABLE IF NOT EXISTS analysis_job_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_name TEXT NOT NULL,
    started_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    finished_at TEXT,
    status TEXT NOT NULL CHECK (status IN ('running', 'success', 'failed')),
    analyzed_count INTEGER NOT NULL DEFAULT 0,
    failed_count INTEGER NOT NULL DEFAULT 0,
    model_used TEXT,
    error_message TEXT
);

CREATE INDEX IF NOT EXISTS analysis_job_log_idx ON analysis_job_log (job_name, started_at);
CREATE INDEX IF NOT EXISTS analysis_job_log_started_idx ON analysis_job_log (started_at);

CREATE TABLE IF NOT EXISTS connector_health_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    connector TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('success', 'failed', 'skipped', 'circuit_open')),
    attempt INTEGER NOT NULL DEFAULT 1,
    latency_ms REAL NOT NULL DEFAULT 0,
    circuit_open INTEGER NOT NULL DEFAULT 0,
    message TEXT,
    recorded_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS connector_health_history_idx ON connector_health_history (connector, recorded_at);
CREATE INDEX IF NOT EXISTS connector_health_history_recorded_idx ON connector_health_history (recorded_at);

INSERT OR IGNORE INTO scene_preset (scene_id, scene_name, description, config_json, updated_at)
VALUES (
    'world',
    '全球视角',
    '全球能源安全基线场景。',
    '{"layers":["boundary","facilities","events"],"event_source":"","event_hours":24,"facility_type":"","right_cards":["risk_snapshot","risk_explain","event_brief"],"timeline_window":"24h"}',
    CURRENT_TIMESTAMP
);

INSERT OR IGNORE INTO scene_preset (scene_id, scene_name, description, config_json, updated_at)
VALUES (
    'finance',
    '金融视角',
    '聚焦能源价格与市场敏感扰动。',
    '{"layers":["boundary","facilities","events"],"event_source":"energy_market","event_hours":72,"facility_type":"","right_cards":["risk_snapshot","event_brief","system_health"],"timeline_window":"7d"}',
    CURRENT_TIMESTAMP
);

INSERT OR IGNORE INTO scene_preset (scene_id, scene_name, description, config_json, updated_at)
VALUES (
    'tech',
    '技术视角',
    '聚焦产业与基础设施压力场景。',
    '{"layers":["boundary","facilities","events"],"event_source":"energy_announcement","event_hours":168,"facility_type":"battery_factory","right_cards":["risk_snapshot","risk_explain","facility_detail"],"timeline_window":"7d"}',
    CURRENT_TIMESTAMP
);

INSERT OR IGNORE INTO scene_preset (scene_id, scene_name, description, config_json, updated_at)
VALUES (
    'happy',
    '稳定视角',
    '低风险稳定态势监测场景。',
    '{"layers":["boundary","facilities"],"event_source":"","event_hours":24,"facility_type":"","right_cards":["risk_snapshot","system_health"],"timeline_window":"24h"}',
    CURRENT_TIMESTAMP
);
