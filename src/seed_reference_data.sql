-- Seed Reference Data
-- This script inserts initial reference data into the warehouse
-- Source: src/seed_data.yaml

-- ============================================================================
-- Insert Locations
-- ============================================================================

INSERT INTO core.location (location_name, latitude, longitude, country, region, city)
VALUES 
    ('Boston', 42.3601, -71.0589, 'US', 'Massachusetts', 'Boston'),
    ('St Louis', 38.6270, -90.1994, 'US', 'Missouri', 'St. Louis')
ON CONFLICT (location_name, latitude, longitude) DO NOTHING;

-- ============================================================================
-- Insert Wikipedia Pages
-- ============================================================================
-- Note: wikipedia_page_id will be set when we fetch actual page IDs from Wikipedia API
-- For now, using temporary negative placeholder IDs that will be updated during data ingestion

INSERT INTO core.wikipedia_page (wikipedia_page_id, page_title, namespace, page_language)
VALUES 
    (-1, 'Boston', 0, 'en'),
    (-2, 'St. Louis', 0, 'en'),
    (-3, 'New England', 0, 'en'),
    (-4, 'Cardinals', 0, 'en')
ON CONFLICT (wikipedia_page_id, page_language) DO NOTHING;

-- Verify inserts
SELECT 'Locations inserted:' as info, COUNT(*) as count FROM core.location;
SELECT 'Wikipedia pages inserted:' as info, COUNT(*) as count FROM core.wikipedia_page;

