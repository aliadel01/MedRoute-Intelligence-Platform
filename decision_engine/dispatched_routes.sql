-- ────────────────────────────────────────────────────────────────
-- DDL Script for creating the dispatched_routes table in PostgreSQL
-- ────────────────────────────────────────────────────────────────

-- Create the main table to hold streaming dispatch decisions from PySpark
CREATE TABLE IF NOT EXISTS dispatched_routes (
    incident_id VARCHAR(50) PRIMARY KEY,
    severity INT NOT NULL,
    incident_start_time TIMESTAMPTZ NOT NULL,
    incident_lat DOUBLE PRECISION NOT NULL,
    incident_lon DOUBLE PRECISION NOT NULL,
    incident_description TEXT,
    kafka_timestamp TIMESTAMPTZ NOT NULL,
    target_hospital_name VARCHAR(255) NOT NULL,
    target_hospital_lat DOUBLE PRECISION NOT NULL,
    target_hospital_lon DOUBLE PRECISION NOT NULL,
    available_icu_beds INT NOT NULL,
    travel_duration_seconds DOUBLE PRECISION NOT NULL,
    travel_distance_meters DOUBLE PRECISION NOT NULL,
    route_geometry TEXT NOT NULL,
    processed_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- Indexing on timestamps for fast time-series filtering in Grafana dashboards
CREATE INDEX IF NOT EXISTS idx_dispatched_routes_kafka_time 
ON dispatched_routes (kafka_timestamp DESC);

-- ────────────────────────────────────────────────────────────────
-- Optional PostGIS Integration (Highly recommended for Geomap)
-- ────────────────────────────────────────────────────────────────
/*
-- 1. Enable PostGIS extension if it hasn't been enabled yet
CREATE EXTENSION IF NOT EXISTS postgis;

-- 2. Add structural geometry columns using SRID 4326 (WGS 84 coordinate system)
ALTER TABLE dispatched_routes ADD COLUMN IF NOT EXISTS geom_accident geometry(Point, 4326);
ALTER TABLE dispatched_routes ADD COLUMN IF NOT EXISTS geom_hospital geometry(Point, 4326);

-- 3. Create spatial indexes for lightning-fast bounding-box queries
CREATE INDEX IF NOT EXISTS idx_dispatched_routes_geom_accident 
ON dispatched_routes USING GIST (geom_accident);

CREATE INDEX IF NOT EXISTS idx_dispatched_routes_geom_hospital 
ON dispatched_routes USING GIST (geom_hospital);

-- 4. Create a database trigger to automatically convert lat/lon to geometries on INSERT
CREATE OR REPLACE FUNCTION update_spatial_geometries()
RETURNS TRIGGER AS $$
BEGIN
    NEW.geom_accident := ST_SetSRID(ST_MakePoint(NEW.incident_lon, NEW.incident_lat), 4326);
    NEW.geom_hospital := ST_SetSRID(ST_MakePoint(NEW.target_hospital_lon, NEW.target_hospital_lat), 4326);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER trigger_auto_spatial_geometries
BEFORE INSERT ON dispatched_routes
FOR EACH ROW
EXECUTE FUNCTION update_spatial_geometries();
*/