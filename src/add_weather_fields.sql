-- Add precipitation and cloud cover fields to core.weather table
-- This migration adds the missing weather data points from the API

-- Add precipitation_mm column (precipitation in millimeters)
ALTER TABLE core.weather 
ADD COLUMN IF NOT EXISTS precipitation_mm DECIMAL(5, 2);

-- Add cloud_cover_percent column (cloud cover percentage 0-100)
ALTER TABLE core.weather 
ADD COLUMN IF NOT EXISTS cloud_cover_percent DECIMAL(5, 2);

-- Add index for better query performance
CREATE INDEX IF NOT EXISTS idx_weather_precipitation 
ON core.weather(location_id, observed_at) 
WHERE precipitation_mm IS NOT NULL;

-- Add comment for documentation
COMMENT ON COLUMN core.weather.precipitation_mm IS 'Precipitation amount in millimeters';
COMMENT ON COLUMN core.weather.cloud_cover_percent IS 'Cloud cover percentage (0-100)';

