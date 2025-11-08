-- Update Raw Table Schemas for Extract Tasks
-- This script adds JSONB payload columns and adjusts data types to support
-- storing full API responses and revision IDs as strings

-- ============================================================================
-- Update raw.weather_observations
-- ============================================================================

-- Add JSONB column to store full Open-Meteo API response
ALTER TABLE raw.weather_observations 
ADD COLUMN IF NOT EXISTS payload JSONB;

-- Add comment
COMMENT ON COLUMN raw.weather_observations.payload IS 'Full JSON payload from Open-Meteo API response';

-- ============================================================================
-- Update raw.wikipedia_pages
-- ============================================================================

-- Add JSONB column to store full MediaWiki REST API response
ALTER TABLE raw.wikipedia_pages 
ADD COLUMN IF NOT EXISTS payload JSONB;

-- Change revision_id from BIGINT to VARCHAR to accommodate string revision IDs
-- Note: This may require data migration if there's existing data
ALTER TABLE raw.wikipedia_pages 
ALTER COLUMN revision_id TYPE VARCHAR(255);

-- Add comment
COMMENT ON COLUMN raw.wikipedia_pages.payload IS 'Full JSON payload from MediaWiki REST API summary response';

-- Verify changes
SELECT 
    table_name,
    column_name,
    data_type,
    is_nullable
FROM information_schema.columns
WHERE table_schema = 'raw'
  AND table_name IN ('weather_observations', 'wikipedia_pages')
ORDER BY table_name, ordinal_position;

