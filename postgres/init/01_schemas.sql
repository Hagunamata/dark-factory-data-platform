-- =============================================================================
-- Dark Factory Data Platform — Postgres schema initialisation
-- =============================================================================
-- This script runs once when the postgres container is first created.
-- It establishes the two-schema design described in docs/01-conception.md §3.
-- Actual column definitions are TODOs that depend on the Kaggle dataset
-- columns and will be finalised during Phase 2 development.
-- =============================================================================

-- ---------------------------------------------------------------------------
-- Schemas
-- ---------------------------------------------------------------------------

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS analytics;

COMMENT ON SCHEMA raw IS
  'Append-only event tables. Source of truth for all downstream processing. '
  'Rows in this schema are never updated or deleted by the pipeline.';

COMMENT ON SCHEMA analytics IS
  'ML-ready aggregated feature tables. Rebuilt quarterly by the Spark batch job. '
  'Downstream ML application reads from this schema only.';

-- ---------------------------------------------------------------------------
-- Raw tables — schemas TBD from Kaggle datasets
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS raw.logistics_events (
    event_id         BIGSERIAL PRIMARY KEY,
    event_timestamp  TIMESTAMPTZ NOT NULL,
    -- TODO: add columns matching the Smart Logistics Supply Chain dataset
    -- e.g. asset_id, location, status, weight, destination, ...
    ingested_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    payload          JSONB
);

CREATE INDEX IF NOT EXISTS idx_logistics_events_timestamp
    ON raw.logistics_events (event_timestamp);

CREATE TABLE IF NOT EXISTS raw.hrss_telemetry (
    reading_id       BIGSERIAL PRIMARY KEY,
    reading_timestamp TIMESTAMPTZ NOT NULL,
    -- TODO: add columns matching the HRSS dataset
    -- e.g. asset_id, motor_power, position, cycle_time, ...
    is_anomalous     BOOLEAN,
    is_optimised     BOOLEAN,
    ingested_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    payload          JSONB
);

CREATE INDEX IF NOT EXISTS idx_hrss_telemetry_timestamp
    ON raw.hrss_telemetry (reading_timestamp);

-- ---------------------------------------------------------------------------
-- Analytics tables — feature tables consumed by the ML application
-- ---------------------------------------------------------------------------

-- TODO: define feature tables in Phase 2. Likely candidates:
--   analytics.logistics_features_quarterly    (per-asset aggregates)
--   analytics.hrss_features_quarterly         (per-asset aggregates)
--   analytics.anomaly_labels                  (joined view for ML training)
