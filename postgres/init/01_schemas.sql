-- =============================================================================
-- Dark Factory Data Platform — Postgres schema initialisation
-- =============================================================================
-- Runs once on first container start. Establishes the two-schema design from
-- docs/01-conception.md §3. Column definitions are derived from the two Kaggle
-- reference datasets: Smart Logistics Supply Chain and HRSS Sensor Data.
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
-- raw.logistics_events
-- Schema derived from: Smart Logistics Supply Chain Dataset (~1 000 rows)
-- Columns: Timestamp, Asset_ID, Latitude, Longitude, Inventory_Level,
--   Shipment_Status, Temperature, Humidity, Traffic_Status, Waiting_Time,
--   User_Transaction_Amount, User_Purchase_Frequency, Logistics_Delay_Reason,
--   Asset_Utilization, Demand_Forecast, Logistics_Delay
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS raw.logistics_events (
    event_id                  BIGSERIAL PRIMARY KEY,
    event_timestamp           TIMESTAMPTZ     NOT NULL,
    asset_id                  VARCHAR(50)     NOT NULL,
    latitude                  NUMERIC(9, 4),
    longitude                 NUMERIC(9, 4),
    inventory_level           INTEGER,
    shipment_status           VARCHAR(50),
    temperature_c             NUMERIC(5, 2),
    humidity_pct              NUMERIC(5, 2),
    traffic_status            VARCHAR(50),
    waiting_time_min          INTEGER,
    user_transaction_amount   INTEGER,
    user_purchase_frequency   INTEGER,
    logistics_delay_reason    VARCHAR(100),
    asset_utilization_pct     NUMERIC(5, 2),
    demand_forecast           INTEGER,
    logistics_delay           BOOLEAN,
    ingested_at               TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_logistics_events_timestamp
    ON raw.logistics_events (event_timestamp);

CREATE INDEX IF NOT EXISTS idx_logistics_events_asset_id
    ON raw.logistics_events (asset_id);

-- ---------------------------------------------------------------------------
-- raw.hrss_telemetry
-- Schema derived from: HRSS Sensor Data for Energy Optimisation (4 files,
--   ~90 500 rows total). Each file represents one cell of the 2×2 matrix:
--   {normal, anomalous} × {standard, energy-optimised control}.
--   is_anomalous and is_optimised encode which file the row came from.
--
-- Original columns: Timestamp (cycle-elapsed seconds, float), Labels (0/1),
--   then 6 axes × 3 signals = 18 sensor columns.
--   Axes: BLO, BHL, BHR, BRU, HR, HL
--   Signals per axis: I_w_*_Weg (position mm), O_w_*_power (W), O_w_*_voltage (V)
--
-- reading_timestamp is assigned by the generator (mapped to a real calendar
--   window). cycle_elapsed_sec preserves the original float for fidelity.
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS raw.hrss_telemetry (
    reading_id            BIGSERIAL   PRIMARY KEY,
    reading_timestamp     TIMESTAMPTZ NOT NULL,
    cycle_elapsed_sec     NUMERIC(14, 7),         -- original float Timestamp from source

    -- Belt Lower (BLO)
    blo_position_mm       INTEGER,
    blo_power_w           INTEGER,
    blo_voltage_v         INTEGER,

    -- Belt Horizontal Left (BHL)
    bhl_position_mm       INTEGER,
    bhl_power_w           INTEGER,
    bhl_voltage_v         INTEGER,

    -- Belt Horizontal Right (BHR)
    bhr_position_mm       INTEGER,
    bhr_power_w           INTEGER,
    bhr_voltage_v         INTEGER,

    -- Belt Rotation Unit (BRU)
    bru_position_mm       INTEGER,
    bru_power_w           INTEGER,
    bru_voltage_v         INTEGER,

    -- Horizontal Rail Right (HR)
    hr_position_mm        INTEGER,
    hr_power_w            INTEGER,
    hr_voltage_v          INTEGER,

    -- Horizontal Rail Left (HL)
    hl_position_mm        INTEGER,
    hl_power_w            INTEGER,
    hl_voltage_v          INTEGER,

    -- Source-file labels (encode which of the 4 HRSS files this row came from)
    label                 SMALLINT,               -- original Labels column (0 or 1)
    is_anomalous          BOOLEAN NOT NULL,
    is_optimised          BOOLEAN NOT NULL,

    ingested_at           TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_hrss_telemetry_timestamp
    ON raw.hrss_telemetry (reading_timestamp);

CREATE INDEX IF NOT EXISTS idx_hrss_telemetry_flags
    ON raw.hrss_telemetry (is_anomalous, is_optimised);

-- ---------------------------------------------------------------------------
-- analytics.logistics_features_quarterly
-- Per-asset aggregate features over a quarter. Written by the Spark job.
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS analytics.logistics_features_quarterly (
    feature_id                  BIGSERIAL   PRIMARY KEY,
    quarter_start               DATE        NOT NULL,   -- first day of the quarter
    asset_id                    VARCHAR(50) NOT NULL,
    event_count                 INTEGER,
    delay_rate                  NUMERIC(5, 4),          -- fraction of events where logistics_delay=true
    avg_waiting_time_min        NUMERIC(8, 2),
    avg_inventory_level         NUMERIC(10, 2),
    avg_asset_utilization_pct   NUMERIC(5, 2),
    avg_temperature_c           NUMERIC(6, 2),
    avg_humidity_pct            NUMERIC(6, 2),
    total_transaction_amount    BIGINT,
    computed_at                 TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (quarter_start, asset_id)
);

-- ---------------------------------------------------------------------------
-- analytics.hrss_features_quarterly
-- Per-anomaly-class aggregate sensor features over a quarter. Written by Spark.
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS analytics.hrss_features_quarterly (
    feature_id              BIGSERIAL   PRIMARY KEY,
    quarter_start           DATE        NOT NULL,
    is_anomalous            BOOLEAN     NOT NULL,
    is_optimised            BOOLEAN     NOT NULL,
    reading_count           INTEGER,
    avg_blo_power_w         NUMERIC(10, 2),
    avg_bhl_power_w         NUMERIC(10, 2),
    avg_bhr_power_w         NUMERIC(10, 2),
    avg_bru_power_w         NUMERIC(10, 2),
    avg_hr_power_w          NUMERIC(10, 2),
    avg_hl_power_w          NUMERIC(10, 2),
    total_power_w           NUMERIC(14, 2),             -- sum across all axes
    anomaly_label_rate      NUMERIC(5, 4),              -- fraction of rows where label=1
    computed_at             TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (quarter_start, is_anomalous, is_optimised)
);
