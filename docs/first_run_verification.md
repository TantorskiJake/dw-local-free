# First Manual Run - End-to-End Verification

This document describes how to verify the first manual run of the pipeline end-to-end.

## Prerequisites

1. **Containers Running**
   ```bash
   docker-compose up -d
   docker-compose ps  # Verify both containers are healthy
   ```

2. **Prefect Server (Optional, for UI)**
   ```bash
   prefect server start
   ```
   UI available at: http://127.0.0.1:4200

3. **Environment Variables**
   - `.env` file should exist with `DATABASE_URL` and `TZ=UTC`

## Running the Flow

### Option 1: Direct Python Execution
```bash
python3 -m prefect.daily_pipeline
```

### Option 2: Prefect CLI (Recommended)
```bash
# Start Prefect server first
prefect server start

# In another terminal, run the flow
prefect deployment run daily_pipeline/weather-schedule
# or
prefect deployment run daily_pipeline/wikipedia-schedule
```

### Option 3: Prefect UI
1. Open http://127.0.0.1:4200
2. Navigate to Deployments
3. Select a deployment
4. Click "Run"

## Verification Steps

### 1. Verify Raw Tables Received Data

**Check `raw.weather_observations`:**
```sql
SELECT 
    location_name,
    ingested_at,
    payload IS NOT NULL as has_payload
FROM raw.weather_observations
ORDER BY ingested_at DESC
LIMIT 10;
```

**Expected:**
- Rows with recent `ingested_at` timestamps (within last hour)
- `payload` column contains JSONB with Open-Meteo API response
- At least 2 rows (one per location: Boston, St Louis)

**Check `raw.wikipedia_pages`:**
```sql
SELECT 
    page_title,
    ingested_at,
    revision_id,
    payload IS NOT NULL as has_payload
FROM raw.wikipedia_pages
ORDER BY ingested_at DESC
LIMIT 10;
```

**Expected:**
- Rows with recent `ingested_at` timestamps (within last hour)
- `payload` column contains JSONB with MediaWiki API response
- At least 4 rows (one per page: Boston, St. Louis, New England, Cardinals)

### 2. Verify Core Fact Tables

**Check `core.weather`:**
```sql
SELECT 
    l.location_name,
    DATE(w.observed_at) as observation_date,
    COUNT(*) as observation_count,
    MIN(w.observed_at) as first_observation,
    MAX(w.observed_at) as last_observation
FROM core.weather w
JOIN core.location l ON w.location_id = l.location_id
WHERE DATE(w.observed_at) = CURRENT_DATE
GROUP BY l.location_name, DATE(w.observed_at)
ORDER BY l.location_name;
```

**Expected:**
- Rows for today's date (`CURRENT_DATE`)
- At least 2 rows (one per location: Boston, St Louis)
- Multiple observations per location (hourly data)
- `observed_at` timestamps within the last 24 hours

**Check `core.revision`:**
```sql
SELECT 
    wp.page_title,
    COUNT(*) as revision_count,
    MIN(r.fetched_at) as first_fetch,
    MAX(r.fetched_at) as last_fetch
FROM core.revision r
JOIN core.wikipedia_page wp ON r.page_id = wp.page_id
WHERE wp.is_current = true
  AND DATE(r.fetched_at) = CURRENT_DATE
GROUP BY wp.page_title
ORDER BY wp.page_title;
```

**Expected:**
- Rows for today's date
- At least 4 rows (one per page)
- `fetched_at` timestamps from today
- `revision_id` values are unique per page

### 3. Refresh Materialized Views

**Refresh views manually:**
```sql
REFRESH MATERIALIZED VIEW CONCURRENTLY mart.daily_weather_aggregates;
REFRESH MATERIALIZED VIEW CONCURRENTLY mart.daily_wikipedia_page_stats;
```

Or via Prefect flow (should happen automatically if checkpoints pass).

### 4. Verify Mart Views

**Check `mart.daily_weather_aggregates`:**
```sql
SELECT 
    location_name,
    city,
    country,
    observation_date,
    observation_count,
    avg_temperature_celsius,
    avg_humidity_percent,
    avg_wind_speed_kmh
FROM mart.daily_weather_aggregates
WHERE observation_date = CURRENT_DATE
ORDER BY location_name;
```

**Expected:**
- At least 2 rows (one per location: Boston, St Louis)
- `observation_date` = today's date
- Aggregated metrics (avg temperature, humidity, wind speed)
- `observation_count` >= 1 (ideally 24 for full day)

**Check `mart.daily_wikipedia_page_stats`:**
```sql
SELECT 
    page_title,
    page_language,
    revision_date,
    revision_count,
    unique_editors,
    total_bytes_changed
FROM mart.daily_wikipedia_page_stats
WHERE revision_date = CURRENT_DATE
ORDER BY page_title;
```

**Expected:**
- At least 4 rows (one per page)
- `revision_date` = today's date
- `revision_count` >= 1
- `total_bytes_changed` > 0

### 5. Verify Prefect UI

1. Open Prefect UI: http://127.0.0.1:4200
2. Navigate to **Flow Runs**
3. Find the latest run of `daily_pipeline`
4. Verify:
   - **Status**: Green/Completed (not Failed)
   - **All tasks completed**: Check task list
   - **Checkpoints passed**: 
     - `run_weather_data_quality_checkpoint` - Success
     - `run_wikipedia_data_quality_checkpoint` - Success
   - **No errors**: Check logs for any warnings or errors

### 6. Verify Data Quality Checkpoints

**In Prefect UI:**
- Navigate to the flow run
- Click on `run_weather_data_quality_checkpoint` task
- Verify: Status = Success, no exceptions
- Click on `run_wikipedia_data_quality_checkpoint` task
- Verify: Status = Success, no exceptions

**Via Great Expectations:**
```bash
python3 src/data_quality.py
```

This will run both checkpoints and show results.

## Common Issues

### No Data in Raw Tables

**Possible causes:**
- Extract tasks not implemented (still placeholders)
- API calls failing
- Network connectivity issues

**Solution:**
- Check task logs in Prefect UI
- Verify API endpoints are accessible
- Check `raw.weather_observations` and `raw.wikipedia_pages` tables exist

### No Data in Core Fact Tables

**Possible causes:**
- Transform tasks not implemented (still placeholders)
- Data quality checkpoints failing
- Foreign key constraints failing

**Solution:**
- Check transform task logs
- Verify location and page dimensions have data
- Check data quality checkpoint results

### Materialized Views Empty

**Possible causes:**
- Views not refreshed after data load
- No data in underlying fact tables
- View definition issues

**Solution:**
- Manually refresh views (see step 3)
- Verify fact tables have data
- Check view definitions in `src/init_warehouse.sql`

### Checkpoints Failing

**Possible causes:**
- Data doesn't meet expectations
- Great Expectations not initialized
- Database connection issues

**Solution:**
- Check checkpoint logs for specific failures
- Review expectation suite definitions
- Verify database connection string
- Run `python3 src/data_quality.py` to initialize GE

## Current Implementation Status

**Note:** As of the initial setup, the extract and transform tasks contain placeholder logic. To get real data:

1. **Implement Extract Tasks:**
   - `fetch_raw_weather`: Call Open-Meteo API and insert into `raw.weather_observations`
   - `fetch_raw_wikipedia_page`: Call MediaWiki REST API and insert into `raw.wikipedia_pages`

2. **Implement Transform Tasks:**
   - `transform_weather_to_fact`: Explode arrays and upsert into `core.weather`
   - `upsert_wikipedia_dimension_and_facts`: Type-2 SCD and insert revision facts

3. **Test End-to-End:**
   - Run flow manually
   - Verify data in each layer
   - Confirm checkpoints pass
   - Verify mart views refresh

## Quick Verification Queries

**All-in-one verification:**
```sql
-- Raw layer
SELECT 'raw.weather_observations' as table_name, COUNT(*) as row_count, MAX(ingested_at) as latest_ingest
FROM raw.weather_observations
UNION ALL
SELECT 'raw.wikipedia_pages', COUNT(*), MAX(ingested_at)
FROM raw.wikipedia_pages
UNION ALL
-- Core layer
SELECT 'core.weather', COUNT(*), MAX(created_at)
FROM core.weather
UNION ALL
SELECT 'core.revision', COUNT(*), MAX(fetched_at)
FROM core.revision
UNION ALL
-- Mart layer
SELECT 'mart.daily_weather_aggregates', COUNT(*), MAX(observation_date::text)
FROM mart.daily_weather_aggregates
UNION ALL
SELECT 'mart.daily_wikipedia_page_stats', COUNT(*), MAX(revision_date::text)
FROM mart.daily_wikipedia_page_stats;
```

This query shows row counts and latest timestamps for all key tables.

