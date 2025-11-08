-- Initialize Data Warehouse
-- This script creates schemas, tables, dimensions, facts, and materialized views

-- ============================================================================
-- STEP 1: Create Schemas
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS core;
CREATE SCHEMA IF NOT EXISTS mart;

-- ============================================================================
-- STEP 2: Create Raw Tables
-- ============================================================================

-- Raw weather observations table
CREATE TABLE IF NOT EXISTS raw.weather_observations (
    id SERIAL PRIMARY KEY,
    location_name VARCHAR(255),
    latitude DECIMAL(10, 8),
    longitude DECIMAL(11, 8),
    observed_at TIMESTAMP,
    temperature_celsius DECIMAL(5, 2),
    humidity_percent DECIMAL(5, 2),
    pressure_hpa DECIMAL(7, 2),
    wind_speed_kmh DECIMAL(5, 2),
    wind_direction_degrees INTEGER,
    conditions VARCHAR(100),
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source VARCHAR(100)
);

-- Raw Wikipedia pages table
CREATE TABLE IF NOT EXISTS raw.wikipedia_pages (
    id SERIAL PRIMARY KEY,
    page_id BIGINT,
    page_title VARCHAR(500),
    namespace INTEGER,
    revision_id BIGINT,
    revision_timestamp TIMESTAMP,
    revision_user VARCHAR(255),
    revision_size_bytes INTEGER,
    page_language VARCHAR(10),
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source VARCHAR(100)
);

-- ============================================================================
-- STEP 3: Create Core Dimension Tables
-- ============================================================================

-- Location dimension
CREATE TABLE IF NOT EXISTS core.location (
    location_id SERIAL PRIMARY KEY,
    location_name VARCHAR(255) NOT NULL,
    latitude DECIMAL(10, 8),
    longitude DECIMAL(11, 8),
    country VARCHAR(100),
    region VARCHAR(100),
    city VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(location_name, latitude, longitude)
);

-- Wikipedia page dimension
CREATE TABLE IF NOT EXISTS core.wikipedia_page (
    page_id SERIAL PRIMARY KEY,
    wikipedia_page_id BIGINT NOT NULL,
    page_title VARCHAR(500) NOT NULL,
    namespace INTEGER,
    page_language VARCHAR(10),
    first_seen_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(wikipedia_page_id, page_language)
);

-- ============================================================================
-- STEP 4: Create Core Fact Tables
-- ============================================================================

-- Weather fact table (will be partitioned by month)
CREATE TABLE IF NOT EXISTS core.weather (
    weather_id BIGSERIAL,
    location_id INTEGER NOT NULL REFERENCES core.location(location_id),
    observed_at TIMESTAMP NOT NULL,
    temperature_celsius DECIMAL(5, 2),
    humidity_percent DECIMAL(5, 2),
    pressure_hpa DECIMAL(7, 2),
    wind_speed_kmh DECIMAL(5, 2),
    wind_direction_degrees INTEGER,
    conditions VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (weather_id, observed_at)
) PARTITION BY RANGE (observed_at);

-- Create initial partition for current month
CREATE TABLE IF NOT EXISTS core.weather_2024_11 PARTITION OF core.weather
    FOR VALUES FROM ('2024-11-01') TO ('2024-12-01');

-- Revision fact table
CREATE TABLE IF NOT EXISTS core.revision (
    revision_id SERIAL PRIMARY KEY,
    page_id INTEGER NOT NULL REFERENCES core.wikipedia_page(page_id),
    revision_timestamp TIMESTAMP NOT NULL,
    revision_user VARCHAR(255),
    revision_size_bytes INTEGER,
    revision_number BIGINT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================================
-- STEP 5: Create Indexes
-- ============================================================================

-- Index on weather fact by location_id and observed_at
CREATE INDEX IF NOT EXISTS idx_weather_location_observed 
    ON core.weather (location_id, observed_at);

-- Additional useful indexes
CREATE INDEX IF NOT EXISTS idx_weather_observed_at 
    ON core.weather (observed_at);

CREATE INDEX IF NOT EXISTS idx_revision_page_timestamp 
    ON core.revision (page_id, revision_timestamp);

CREATE INDEX IF NOT EXISTS idx_revision_timestamp 
    ON core.revision (revision_timestamp);

-- ============================================================================
-- STEP 6: Create Materialized Views in Mart Schema
-- ============================================================================

-- Daily weather aggregates
CREATE MATERIALIZED VIEW IF NOT EXISTS mart.daily_weather_aggregates AS
SELECT 
    l.location_name,
    l.city,
    l.country,
    DATE(w.observed_at) AS observation_date,
    COUNT(*) AS observation_count,
    AVG(w.temperature_celsius) AS avg_temperature_celsius,
    MIN(w.temperature_celsius) AS min_temperature_celsius,
    MAX(w.temperature_celsius) AS max_temperature_celsius,
    AVG(w.humidity_percent) AS avg_humidity_percent,
    AVG(w.pressure_hpa) AS avg_pressure_hpa,
    AVG(w.wind_speed_kmh) AS avg_wind_speed_kmh,
    MODE() WITHIN GROUP (ORDER BY w.conditions) AS most_common_conditions
FROM core.weather w
JOIN core.location l ON w.location_id = l.location_id
GROUP BY l.location_name, l.city, l.country, DATE(w.observed_at);

-- Create index on materialized view
CREATE UNIQUE INDEX IF NOT EXISTS idx_daily_weather_agg_unique 
    ON mart.daily_weather_aggregates (location_name, observation_date);

-- Daily Wikipedia page stats
CREATE MATERIALIZED VIEW IF NOT EXISTS mart.daily_wikipedia_page_stats AS
SELECT 
    wp.page_title,
    wp.page_language,
    DATE(r.revision_timestamp) AS revision_date,
    COUNT(*) AS revision_count,
    COUNT(DISTINCT r.revision_user) AS unique_editors,
    SUM(r.revision_size_bytes) AS total_bytes_changed,
    AVG(r.revision_size_bytes) AS avg_bytes_per_revision,
    MIN(r.revision_size_bytes) AS min_revision_size,
    MAX(r.revision_size_bytes) AS max_revision_size
FROM core.revision r
JOIN core.wikipedia_page wp ON r.page_id = wp.page_id
GROUP BY wp.page_title, wp.page_language, DATE(r.revision_timestamp);

-- Create index on materialized view
CREATE UNIQUE INDEX IF NOT EXISTS idx_daily_wiki_stats_unique 
    ON mart.daily_wikipedia_page_stats (page_title, page_language, revision_date);

-- ============================================================================
-- STEP 7: Partitioning Plan for Weather Fact
-- ============================================================================

-- Note: The weather table is already set up for monthly partitioning
-- Future partitions can be created with:
-- CREATE TABLE core.weather_YYYY_MM PARTITION OF core.weather
--     FOR VALUES FROM ('YYYY-MM-01') TO ('YYYY-MM+1-01');
--
-- Example for December 2024:
-- CREATE TABLE core.weather_2024_12 PARTITION OF core.weather
--     FOR VALUES FROM ('2024-12-01') TO ('2025-01-01');

COMMENT ON TABLE core.weather IS 'Weather fact table partitioned by month. Create new partitions monthly.';

