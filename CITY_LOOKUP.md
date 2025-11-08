# City Lookup Feature

## Overview

The `lookup` command allows you to input any city name and automatically:
1. **Geocode** the city to get coordinates (using OpenStreetMap Nominatim)
2. **Fetch weather data** from Open-Meteo API
3. **Fetch Wikipedia data** for the city
4. **Store everything** in the database
5. **Display results** in a formatted table

## Usage

### Basic Usage

**Make sure your virtual environment is activated:**

```bash
# Activate virtual environment (if using one)
source .venv/bin/activate

# Look up any city
python src/cli.py lookup "New York"

# Narrow search with country
python src/cli.py lookup "Paris" --country "France"

# Or use the helper script
./scripts/dw-cli lookup "Tokyo"
```

**Or use Python from the venv directly:**
```bash
.venv/bin/python src/cli.py lookup "Boston"
```

### Examples

```bash
# US Cities
python src/cli.py lookup "San Francisco"
python src/cli.py lookup "Chicago"
python src/cli.py lookup "Miami"

# International Cities
python src/cli.py lookup "London" --country "United Kingdom"
python src/cli.py lookup "Sydney" --country "Australia"
python src/cli.py lookup "Berlin" --country "Germany"
```

## What It Does

1. **Geocoding**: Uses OpenStreetMap's free Nominatim API to convert city names to coordinates
2. **Location Storage**: Adds the location to `core.location` table (or updates if exists)
3. **Weather Fetch**: Gets hourly weather data for the past 24 hours + 7-day forecast
4. **Wikipedia Fetch**: Retrieves the Wikipedia page summary for the city
5. **Data Storage**: Stores raw data in `raw.*` tables and transforms to `core.*` tables
6. **Results Display**: Shows formatted weather observations and Wikipedia summary

## Output Format

The command displays:
- **Location Info**: City, region, country, coordinates
- **Weather Data**: Latest 24 hours of temperature, humidity, wind speed
- **Wikipedia Summary**: Page title, URL, and first 500 characters of extract

## Viewing Data in Database

After running the lookup, you can view the data in Adminer:

1. Open http://localhost:8080
2. Connect to database `dw`
3. Browse tables:
   - `raw.weather_observations` - Raw weather API responses
   - `raw.wikipedia_pages` - Raw Wikipedia API responses
   - `core.location` - Location dimension
   - `core.weather` - Weather fact table
   - `core.wikipedia_page` - Wikipedia page dimension
   - `core.revision` - Wikipedia revision facts

## Requirements

- Docker containers running (`docker-compose up -d`)
- Database initialized (run `src/init_warehouse.sql` and related scripts)
- Internet connection (for geocoding and API calls)

## Notes

- The geocoding API (Nominatim) has rate limits - be respectful with requests
- Weather data includes historical (last 24h) and forecast (next 7 days)
- Wikipedia data is fetched from the English Wikipedia by default
- All data is stored in UTC timezone

