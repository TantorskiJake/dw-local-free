# Weather Data Accuracy

## Overview

This document explains how weather data accuracy is ensured in the data warehouse.

## Data Source

We use **Open-Meteo** (https://open-meteo.com), a free, open-source weather API that provides:
- High-resolution weather forecasts
- Historical weather data
- Global coverage
- No API key required

Open-Meteo uses multiple weather models including:
- ECMWF (European Centre for Medium-Range Weather Forecasts)
- GFS (Global Forecast System)
- ICON (Icosahedral Nonhydrostatic)

## Accuracy Verification

### 1. Coordinate Accuracy

**Source**: OpenStreetMap Nominatim Geocoding API
- Provides accurate latitude/longitude coordinates for cities
- Used by many mapping services worldwide
- Coordinates are stored and used directly for weather API calls

**Verification**: You can verify coordinates using:
```bash
python scripts/verify_weather.py <city_name>
```

### 2. API Parameters

The weather API is called with:
- **Latitude/Longitude**: From geocoding (accurate to ~10 meters for cities)
- **Timezone**: UTC (standard for data storage)
- **Time Range**: Last 24 hours (historical) + Next 7 days (forecast)
- **Variables**: temperature_2m, relativehumidity_2m, windspeed_10m

### 3. Data Transformation

**Temperature**: 
- Stored as-is from API (Celsius)
- Converted to Fahrenheit in frontend if needed
- ✅ No transformation errors

**Humidity**:
- Stored as percentage (0-100)
- ✅ Direct from API, no conversion needed

**Wind Speed**:
- API provides: km/h
- Stored as: m/s (converted by dividing by 3.6)
- ✅ Conversion: `wind_mps = wind_kmh / 3.6`

### 4. Timestamp Handling

- **API Returns**: ISO8601 timestamps in UTC
- **Database Stores**: UTC timestamps
- **Frontend Displays**: Converts to user's local timezone
- ✅ Timezone handling is correct

## Potential Accuracy Considerations

### 1. Forecast vs Historical Data

- **Historical data** (last 24 hours): Based on actual observations, highly accurate
- **Forecast data** (next 7 days): Model predictions, accuracy decreases with time
  - Day 1-2: Very accurate (~95%+)
  - Day 3-5: Good accuracy (~85-90%)
  - Day 6-7: Moderate accuracy (~75-80%)

### 2. Location Precision

- For large cities: Coordinates are very accurate
- For smaller towns: May use city center coordinates
- Weather can vary within a city (especially large ones like NYC, LA)

### 3. Elevation

- Open-Meteo automatically accounts for elevation
- Elevation is included in API response
- No manual adjustment needed

## Verification Steps

### Run Verification Script

```bash
# Verify weather data for a city
python scripts/verify_weather.py Boston
```

This will:
1. Show stored data from database
2. Fetch current data from Open-Meteo API
3. Compare values and show differences
4. Flag any significant discrepancies

### Expected Results

- **Temperature difference**: < 1.0°C (normal variation)
- **Humidity difference**: < 5% (normal variation)
- **Wind speed difference**: < 0.5 m/s (normal variation)
- **Time difference**: < 1 hour (data refresh interval)

## Data Quality Checks

The pipeline includes data quality checks via Great Expectations:
- Temperature values are within reasonable ranges (-50°C to 60°C)
- Humidity values are between 0-100%
- Wind speed values are non-negative
- Timestamps are valid and in correct format

## Known Limitations

1. **Free Tier**: Open-Meteo free tier has rate limits but no accuracy limits
2. **Update Frequency**: Data is updated when pipeline runs (not real-time)
3. **Spatial Resolution**: ~11km grid resolution (sufficient for city-level forecasts)
4. **Model Updates**: Forecast models update every 6-12 hours

## Conclusion

The weather data is **accurate** for its intended use case (city-level weather forecasts). The data:
- ✅ Uses reliable sources (Open-Meteo, OpenStreetMap)
- ✅ Handles coordinates correctly
- ✅ Converts units accurately
- ✅ Stores timestamps properly
- ✅ Includes data quality checks

For production use, consider:
- Adding more frequent data refreshes for real-time applications
- Using higher-resolution models for specific locations
- Implementing data validation alerts for anomalies

