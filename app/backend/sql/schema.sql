CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS boundary_jx (
    id BIGSERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    iso3 TEXT,
    source_name TEXT,
    geom GEOMETRY(MULTIPOLYGON, 4326) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS boundary_jx_geom_gix ON boundary_jx USING GIST (geom);

CREATE TABLE IF NOT EXISTS dem_tiles (
    id BIGSERIAL PRIMARY KEY,
    source_collection TEXT NOT NULL,
    source_title TEXT,
    tile_name TEXT NOT NULL,
    tile_path TEXT NOT NULL,
    resolution_m DOUBLE PRECISION NOT NULL,
    width INTEGER,
    height INTEGER,
    bbox GEOMETRY(POLYGON, 4326),
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (source_collection, tile_name)
);

CREATE INDEX IF NOT EXISTS dem_tiles_bbox_gix ON dem_tiles USING GIST (bbox);

CREATE TABLE IF NOT EXISTS dem_derivatives (
    id BIGSERIAL PRIMARY KEY,
    derivative_type TEXT NOT NULL CHECK (derivative_type IN ('slope', 'aspect', 'hillshade', 'roughness')),
    raster_path TEXT NOT NULL,
    resolution_m DOUBLE PRECISION NOT NULL,
    width INTEGER,
    height INTEGER,
    bbox GEOMETRY(POLYGON, 4326),
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (derivative_type, raster_path)
);

CREATE INDEX IF NOT EXISTS dem_derivatives_bbox_gix ON dem_derivatives USING GIST (bbox);

CREATE TABLE IF NOT EXISTS baker_facilities (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    facility_id TEXT NOT NULL UNIQUE,
    facility_type TEXT NOT NULL,
    source_layer TEXT NOT NULL,
    name TEXT,
    start_year INTEGER,
    status TEXT,
    admin_city TEXT,
    properties JSONB NOT NULL DEFAULT '{}'::jsonb,
    geom GEOMETRY(GEOMETRY, 4326) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS baker_facilities_geom_gix ON baker_facilities USING GIST (geom);
CREATE INDEX IF NOT EXISTS baker_facilities_type_idx ON baker_facilities (facility_type);
CREATE INDEX IF NOT EXISTS baker_facilities_year_idx ON baker_facilities (start_year);
CREATE INDEX IF NOT EXISTS baker_facilities_status_idx ON baker_facilities (status);
CREATE INDEX IF NOT EXISTS baker_facilities_city_idx ON baker_facilities (admin_city);
CREATE INDEX IF NOT EXISTS baker_facilities_source_layer_idx ON baker_facilities (source_layer);

CREATE TABLE IF NOT EXISTS facility_terrain_metrics (
    id BIGSERIAL PRIMARY KEY,
    facility_pk UUID NOT NULL REFERENCES baker_facilities(id) ON DELETE CASCADE,
    elevation_m DOUBLE PRECISION,
    slope_deg DOUBLE PRECISION,
    aspect_deg DOUBLE PRECISION,
    hillshade DOUBLE PRECISION,
    roughness DOUBLE PRECISION,
    computed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (facility_pk)
);

CREATE TABLE IF NOT EXISTS event_raw (
    id BIGSERIAL PRIMARY KEY,
    source TEXT NOT NULL,
    external_id TEXT NOT NULL,
    fetched_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    event_time TIMESTAMPTZ,
    payload JSONB NOT NULL,
    geom GEOMETRY(POINT, 4326),
    status TEXT NOT NULL DEFAULT 'ok',
    UNIQUE (source, external_id, fetched_at)
);

CREATE INDEX IF NOT EXISTS event_raw_time_idx ON event_raw (event_time DESC);
CREATE INDEX IF NOT EXISTS event_raw_geom_gix ON event_raw USING GIST (geom);

CREATE TABLE IF NOT EXISTS event_normalized (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source TEXT NOT NULL,
    external_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    severity TEXT NOT NULL CHECK (severity IN ('low', 'medium', 'high')),
    title TEXT NOT NULL,
    description TEXT,
    event_time TIMESTAMPTZ NOT NULL,
    ingestion_time TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    properties JSONB NOT NULL DEFAULT '{}'::jsonb,
    geom GEOMETRY(POINT, 4326),
    UNIQUE (source, external_id)
);

CREATE INDEX IF NOT EXISTS event_normalized_time_idx ON event_normalized (event_time DESC);
CREATE INDEX IF NOT EXISTS event_normalized_source_idx ON event_normalized (source);
CREATE INDEX IF NOT EXISTS event_normalized_type_idx ON event_normalized (event_type);
CREATE INDEX IF NOT EXISTS event_normalized_severity_idx ON event_normalized (severity);
CREATE INDEX IF NOT EXISTS event_normalized_geom_gix ON event_normalized USING GIST (geom);

CREATE TABLE IF NOT EXISTS facility_event_link (
    id BIGSERIAL PRIMARY KEY,
    facility_pk UUID NOT NULL REFERENCES baker_facilities(id) ON DELETE CASCADE,
    event_id UUID NOT NULL REFERENCES event_normalized(id) ON DELETE CASCADE,
    distance_km DOUBLE PRECISION NOT NULL,
    linked_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (facility_pk, event_id)
);

CREATE INDEX IF NOT EXISTS facility_event_link_facility_idx ON facility_event_link (facility_pk);
CREATE INDEX IF NOT EXISTS facility_event_link_event_idx ON facility_event_link (event_id);

CREATE TABLE IF NOT EXISTS risk_snapshot (
    id BIGSERIAL PRIMARY KEY,
    region_level TEXT NOT NULL CHECK (region_level IN ('province', 'city')),
    region_name TEXT NOT NULL,
    window TEXT NOT NULL CHECK (window IN ('24h', '7d', '30d')),
    snapshot_time TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    total_events INTEGER NOT NULL DEFAULT 0,
    high_events INTEGER NOT NULL DEFAULT 0,
    medium_events INTEGER NOT NULL DEFAULT 0,
    low_events INTEGER NOT NULL DEFAULT 0,
    weighted_score DOUBLE PRECISION NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS risk_snapshot_idx ON risk_snapshot (region_level, region_name, window, snapshot_time DESC);

CREATE TABLE IF NOT EXISTS risk_timeline (
    id BIGSERIAL PRIMARY KEY,
    region_level TEXT NOT NULL CHECK (region_level IN ('province', 'city')),
    region_name TEXT NOT NULL,
    bucket_start TIMESTAMPTZ NOT NULL,
    window TEXT NOT NULL CHECK (window IN ('24h', '7d', '30d')),
    event_count INTEGER NOT NULL DEFAULT 0,
    weighted_score DOUBLE PRECISION NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS risk_timeline_idx ON risk_timeline (region_level, region_name, window, bucket_start DESC);

CREATE TABLE IF NOT EXISTS sync_job_log (
    id BIGSERIAL PRIMARY KEY,
    job_name TEXT NOT NULL,
    connector TEXT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at TIMESTAMPTZ,
    status TEXT NOT NULL CHECK (status IN ('running', 'success', 'failed', 'skipped')),
    attempt INTEGER NOT NULL DEFAULT 1,
    records_fetched INTEGER NOT NULL DEFAULT 0,
    records_inserted INTEGER NOT NULL DEFAULT 0,
    error_message TEXT
);

CREATE INDEX IF NOT EXISTS sync_job_log_idx ON sync_job_log (job_name, connector, started_at DESC);

CREATE TABLE IF NOT EXISTS data_versions (
    dataset_key TEXT PRIMARY KEY,
    dataset_version TEXT NOT NULL,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

