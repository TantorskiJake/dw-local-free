-- Update Core Table Schemas for Transform Tasks
-- This script adds columns for type-2 SCD, unit conversions, and lineage tracking

-- ============================================================================
-- Update core.weather fact table
-- ============================================================================

-- Add wind_speed_mps column for standardized unit (meters per second)
ALTER TABLE core.weather 
ADD COLUMN IF NOT EXISTS wind_speed_mps DECIMAL(5, 2);

-- Add raw_ref column for lineage tracking
ALTER TABLE core.weather 
ADD COLUMN IF NOT EXISTS raw_ref JSONB;

-- Add unique constraint on (location_id, observed_at) for upsert operations
-- First, drop existing primary key if it conflicts
-- Note: The existing PRIMARY KEY is on (weather_id, observed_at)
-- We'll add a unique constraint instead
CREATE UNIQUE INDEX IF NOT EXISTS idx_weather_location_observed_unique 
    ON core.weather (location_id, observed_at);

-- Add comments
COMMENT ON COLUMN core.weather.wind_speed_mps IS 'Wind speed in meters per second (converted from km/h)';
COMMENT ON COLUMN core.weather.raw_ref IS 'Reference to source raw record for lineage tracking';

-- ============================================================================
-- Update core.wikipedia_page dimension table (Type-2 SCD)
-- ============================================================================

-- Add type-2 SCD columns
ALTER TABLE core.wikipedia_page 
ADD COLUMN IF NOT EXISTS valid_from TIMESTAMP DEFAULT CURRENT_TIMESTAMP;

ALTER TABLE core.wikipedia_page 
ADD COLUMN IF NOT EXISTS valid_to TIMESTAMP;

ALTER TABLE core.wikipedia_page 
ADD COLUMN IF NOT EXISTS is_current BOOLEAN DEFAULT true;

-- Set initial values for existing records
UPDATE core.wikipedia_page 
SET valid_from = COALESCE(first_seen_at, CURRENT_TIMESTAMP),
    valid_to = NULL,
    is_current = true
WHERE valid_from IS NULL;

-- Drop old unique constraint
ALTER TABLE core.wikipedia_page 
DROP CONSTRAINT IF EXISTS wikipedia_page_wikipedia_page_id_page_language_key;

-- Add new unique constraint that allows multiple rows per page (for type-2)
-- But ensures only one current row per (wikipedia_page_id, page_language)
CREATE UNIQUE INDEX IF NOT EXISTS idx_wikipedia_page_current_unique 
    ON core.wikipedia_page (wikipedia_page_id, page_language) 
    WHERE is_current = true;

-- Add comments
COMMENT ON COLUMN core.wikipedia_page.valid_from IS 'Timestamp when this version of the page became active (type-2 SCD)';
COMMENT ON COLUMN core.wikipedia_page.valid_to IS 'Timestamp when this version became inactive (NULL for current version)';
COMMENT ON COLUMN core.wikipedia_page.is_current IS 'True if this is the current version of the page (type-2 SCD)';

-- ============================================================================
-- Update core.revision fact table
-- ============================================================================

-- Add a new surrogate key column first (for primary key)
ALTER TABLE core.revision 
ADD COLUMN IF NOT EXISTS revision_key SERIAL;

-- Drop the existing primary key constraint on revision_id
ALTER TABLE core.revision 
DROP CONSTRAINT IF EXISTS revision_pkey;

-- Add new primary key on revision_key
ALTER TABLE core.revision 
ADD PRIMARY KEY (revision_key);

-- Change revision_id from SERIAL/INTEGER to VARCHAR to match raw table
-- Since table is empty, we can drop and recreate the column
ALTER TABLE core.revision 
DROP COLUMN IF EXISTS revision_id;

ALTER TABLE core.revision 
ADD COLUMN revision_id VARCHAR(255) NOT NULL;

-- Rename revision_size_bytes to content_len
ALTER TABLE core.revision 
RENAME COLUMN revision_size_bytes TO content_len;

-- Add fetched_at timestamp
ALTER TABLE core.revision 
ADD COLUMN IF NOT EXISTS fetched_at TIMESTAMP;

-- Add raw_ref column for lineage tracking
ALTER TABLE core.revision 
ADD COLUMN IF NOT EXISTS raw_ref JSONB;

-- Add unique constraint on (page_id, revision_id) for upsert operations
CREATE UNIQUE INDEX IF NOT EXISTS idx_revision_page_revision_unique 
    ON core.revision (page_id, revision_id);

-- Add comments
COMMENT ON COLUMN core.revision.revision_id IS 'Wikipedia revision ID (string)';
COMMENT ON COLUMN core.revision.content_len IS 'Size of rendered content in bytes';
COMMENT ON COLUMN core.revision.fetched_at IS 'Timestamp when revision data was fetched';
COMMENT ON COLUMN core.revision.raw_ref IS 'Reference to source raw record for lineage tracking';

-- ============================================================================
-- Verify changes
-- ============================================================================

-- Check weather table
SELECT 
    column_name,
    data_type,
    is_nullable
FROM information_schema.columns
WHERE table_schema = 'core' 
  AND table_name = 'weather'
  AND column_name IN ('wind_speed_mps', 'raw_ref')
ORDER BY ordinal_position;

-- Check wikipedia_page table
SELECT 
    column_name,
    data_type,
    is_nullable
FROM information_schema.columns
WHERE table_schema = 'core' 
  AND table_name = 'wikipedia_page'
  AND column_name IN ('valid_from', 'valid_to', 'is_current')
ORDER BY ordinal_position;

-- Check revision table
SELECT 
    column_name,
    data_type,
    is_nullable
FROM information_schema.columns
WHERE table_schema = 'core' 
  AND table_name = 'revision'
  AND column_name IN ('revision_id', 'content_len', 'fetched_at', 'raw_ref')
ORDER BY ordinal_position;

