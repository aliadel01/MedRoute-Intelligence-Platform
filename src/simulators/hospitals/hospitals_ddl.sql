CREATE TABLE IF NOT EXISTS hospitals (
    name VARCHAR(255),
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
	state VARCHAR(50),
	STATUS VARCHAR(50) ,
	beds DOUBLE PRECISION,
    icu_beds DOUBLE PRECISION
);

-- 1. Add the geometry column
SELECT AddGeometryColumn('hospitals', 'geom', 4326, 'POINT', 2);

-- 2. Convert Lat/Lng numbers into real spatial points
UPDATE hospitals 
SET geom = ST_SetSRID(ST_MakePoint(longitude, latitude), 4326);

-- 3. Create GIST Spatial Index for high performance
CREATE INDEX IF NOT EXISTS idx_hospitals_geom ON hospitals USING gist(geom);