-- ────────────────────────────────────────────────────────────────
-- DDL Script for hospitals CDC → OLAP replica table
-- ────────────────────────────────────────────────────────────────

CREATE TABLE
IF NOT EXISTS hospitals_olap
(
    hospital_id TEXT PRIMARY KEY,
    name TEXT,
    status TEXT,
    beds INTEGER,
    icu_beds INTEGER,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    source_cdc_op CHAR
(1) NOT NULL,
    source_ts TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX
IF NOT EXISTS idx_hospitals_olap_status
ON hospitals_olap
(status);

CREATE INDEX
IF NOT EXISTS idx_hospitals_olap_updated_at
ON hospitals_olap
(updated_at DESC);
