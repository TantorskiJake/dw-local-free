# Data Warehouse Local Free - Runbook

## Overview

This project provides a local, free data warehouse solution using modern data engineering tools. It demonstrates a complete ETL pipeline with:
- **Raw Layer**: Stores full API responses from Open-Meteo (weather) and Wikipedia
- **Core Layer**: Dimensional model with facts and dimensions
- **Mart Layer**: Materialized views for analytics
- **Orchestration**: Prefect flows with scheduled and manual execution
- **Data Quality**: Great Expectations checkpoints

## Project Structure

- `src/` - Source code, SQL scripts, data quality checks, and CLI tool
- `prefect/` - Prefect workflows and orchestration
- `docs/` - Documentation and architecture diagrams
- `tests/` - Test files
- `dbt_project/` - dbt Core models for documentation and lineage
- `scripts/` - Convenience scripts

## Quick Start

### 1. Start Services

```bash
# Start PostgreSQL and Adminer
docker-compose up -d

# Verify containers are running
docker-compose ps
```

### 2. Initialize Database

```bash
# Run initialization scripts
docker-compose exec -T postgres psql -U postgres -d dw < src/init_warehouse.sql
docker-compose exec -T postgres psql -U postgres -d dw < src/update_raw_schemas.sql
docker-compose exec -T postgres psql -U postgres -d dw < src/update_core_schemas.sql
docker-compose exec -T postgres psql -U postgres -d dw < src/seed_reference_data.sql
```

### 3. Start Prefect Server (Optional)

```bash
prefect server start
```

Prefect UI will be available at http://127.0.0.1:4200

### 4. View Architecture Documentation

Open `docs/architecture.html` in your browser for a visual overview of the system architecture.

### 5. Use CLI Tool

```bash
# Look up a city by name (automatically geocodes, fetches weather & Wikipedia)
python src/cli.py lookup "New York"
python src/cli.py lookup "Paris" --country "France"

# Or manually add a location with coordinates
python src/cli.py add-location \
    --name "San Francisco" \
    --lat 37.7749 \
    --lon -122.4194 \
    --city "San Francisco" \
    --region "California" \
    --run

# Add a new Wikipedia page
python src/cli.py add-page --title "San Francisco" --run

# See all commands
python src/cli.py --help
```

### 6. Generate dbt Documentation (Optional)

```bash
cd dbt_project
dbt docs generate
dbt docs serve
```

dbt docs will be available at http://localhost:8080 with lineage graphs and model documentation.

---

## Live Demo Script

Follow this script to demonstrate the end-to-end pipeline execution.

### Step 1: Verify Initial State

**In Adminer (http://localhost:8080):**

1. Connect to database:
   - System: `PostgreSQL`
   - Server: `postgres`
   - Username: `postgres`
   - Password: `postgres`
   - Database: `dw`

2. Check seed data exists:
   ```sql
   -- Verify locations are seeded
   SELECT location_name, city, country FROM core.location;
   
   -- Verify Wikipedia pages are seeded
   SELECT page_title, page_language FROM core.wikipedia_page WHERE is_current = true;
   ```

3. Check raw tables are empty (before run):
   ```sql
   SELECT COUNT(*) as weather_count FROM raw.weather_observations;
   SELECT COUNT(*) as wiki_count FROM raw.wikipedia_pages;
   ```

**Expected:** 2 locations, 4 Wikipedia pages, 0 rows in raw tables

---

### Step 2: Run the Pipeline

**Option A: Via Prefect UI (Recommended for Demo)**

1. Open Prefect UI: http://127.0.0.1:4200
2. Navigate to **Deployments**
3. Find `daily_pipeline` with name `weather-schedule` or `wikipedia-schedule`
4. Click **"Run"** button
5. Navigate to **Flow Runs** to watch execution

**Option B: Via CLI**

```bash
# Run weather schedule manually
prefect deployment run daily_pipeline/weather-schedule

# Or run Wikipedia schedule
prefect deployment run daily_pipeline/wikipedia-schedule
```

**Option C: Direct Python**

```bash
python3 -m prefect.daily_pipeline
```

---

### Step 3: Monitor Flow Execution in Prefect UI

**In Prefect UI (http://127.0.0.1:4200):**

1. Navigate to **Flow Runs**
2. Click on the latest `daily_pipeline` run
3. Watch tasks execute in order:
   - ✅ `ensure_location_dimension` - Updates location dimension
   - ✅ `fetch_raw_weather` (2 parallel tasks) - Fetches weather for Boston and St Louis
   - ✅ `transform_weather_to_fact` - Transforms weather data
   - ✅ `fetch_raw_wikipedia_page` (4 parallel tasks) - Fetches Wikipedia pages
   - ✅ `upsert_wikipedia_dimension_and_facts` - Updates dimension and facts
   - ✅ `run_weather_data_quality_checkpoint` - **Verify this passes (green)**
   - ✅ `run_wikipedia_data_quality_checkpoint` - **Verify this passes (green)**
   - ✅ `refresh_materialized_view` (2 concurrent) - Refreshes mart views

4. **Verify Flow Status**: Should show **"Completed"** (green) with all tasks successful

---

### Step 4: Verify Raw Tables Received Data

**In Adminer, run these queries:**

**Query 1: Check weather raw data was loaded**
```sql
-- Show me all weather observations with their load timestamps
SELECT 
    location_name,
    ingested_at,
    jsonb_typeof(payload) as has_payload,
    (payload->'hourly'->'time')::jsonb->0 as first_timestamp
FROM raw.weather_observations
ORDER BY ingested_at DESC;
```

**Expected Results:**
- 2 rows (one per location: Boston, St Louis)
- Recent `ingested_at` timestamps (within last few minutes)
- `has_payload` = "array" (JSONB contains hourly data)
- `first_timestamp` shows first observation time

**Query 2: Check Wikipedia raw data was loaded**
```sql
-- Show me all Wikipedia pages with their fetch timestamps
SELECT 
    page_title,
    revision_id,
    ingested_at,
    revision_size_bytes,
    jsonb_typeof(payload) as has_payload
FROM raw.wikipedia_pages
ORDER BY ingested_at DESC;
```

**Expected Results:**
- 4 rows (one per page: Boston, St. Louis, New England, Cardinals)
- Recent `ingested_at` timestamps
- `revision_id` values (string format)
- `revision_size_bytes` > 0
- `has_payload` = "object" (JSONB contains page data)

**Query 3: Count rows before and after (demonstrate growth)**
```sql
-- Count total rows in raw tables
SELECT 
    'weather_observations' as table_name,
    COUNT(*) as total_rows,
    MAX(ingested_at) as latest_load
FROM raw.weather_observations
UNION ALL
SELECT 
    'wikipedia_pages',
    COUNT(*),
    MAX(ingested_at)
FROM raw.wikipedia_pages;
```

**Expected Results:**
- Row counts match number of locations/pages processed
- `latest_load` shows recent timestamp

---

### Step 5: Verify Core Fact Tables

**In Adminer, run these queries:**

**Query 4: Show weather facts for today by location**
```sql
-- Show me today's weather observations grouped by location
SELECT 
    l.location_name,
    l.city,
    COUNT(*) as observation_count,
    MIN(w.observed_at) as first_observation,
    MAX(w.observed_at) as last_observation,
    ROUND(AVG(w.temperature_celsius)::numeric, 2) as avg_temp_c,
    ROUND(AVG(w.humidity_percent)::numeric, 2) as avg_humidity_pct,
    ROUND(AVG(w.wind_speed_mps)::numeric, 2) as avg_wind_mps
FROM core.weather w
JOIN core.location l ON w.location_id = l.location_id
WHERE DATE(w.observed_at) = CURRENT_DATE
GROUP BY l.location_name, l.city
ORDER BY l.location_name;
```

**Expected Results:**
- 2 rows (Boston and St Louis)
- `observation_count` >= 1 (ideally 24+ for full day of hourly data)
- `first_observation` and `last_observation` show today's timestamps
- Temperature, humidity, and wind speed averages are reasonable

**Query 5: Show Wikipedia revision facts for today by page**
```sql
-- Show me today's Wikipedia revisions grouped by page
SELECT 
    wp.page_title,
    COUNT(*) as revision_count,
    MIN(r.fetched_at) as first_fetch,
    MAX(r.fetched_at) as last_fetch,
    SUM(r.content_len) as total_bytes,
    ROUND(AVG(r.content_len)::numeric, 0) as avg_content_bytes
FROM core.revision r
JOIN core.wikipedia_page wp ON r.page_id = wp.page_id
WHERE wp.is_current = true
  AND DATE(r.fetched_at) = CURRENT_DATE
GROUP BY wp.page_title
ORDER BY wp.page_title;
```

**Expected Results:**
- 4 rows (one per Wikipedia page)
- `revision_count` >= 1
- `first_fetch` and `last_fetch` show today's timestamps
- `total_bytes` and `avg_content_bytes` are positive numbers

**Query 6: Verify data quality - check for duplicates**
```sql
-- Check for duplicate weather observations (should return 0 rows)
SELECT 
    location_id,
    observed_at,
    COUNT(*) as duplicate_count
FROM core.weather
WHERE DATE(observed_at) = CURRENT_DATE
GROUP BY location_id, observed_at
HAVING COUNT(*) > 1;
```

**Expected Results:**
- 0 rows (no duplicates - uniqueness constraint working)

---

### Step 6: Verify Data Quality Checkpoints Passed

**In Prefect UI:**

1. Navigate to the flow run
2. Click on `run_weather_data_quality_checkpoint` task
3. **Verify:**
   - Status: ✅ **Success** (green)
   - No exceptions or errors
   - Check logs for "Weather data quality checkpoint PASSED"

4. Click on `run_wikipedia_data_quality_checkpoint` task
5. **Verify:**
   - Status: ✅ **Success** (green)
   - No exceptions or errors
   - Check logs for "Wikipedia data quality checkpoint PASSED"

**In Adminer (verify expectations manually):**

```sql
-- Verify weather data meets quality expectations
SELECT 
    'Temperature range check' as check_name,
    COUNT(*) as violations,
    COUNT(*) FILTER (WHERE temperature_celsius < -50 OR temperature_celsius > 60) as out_of_range
FROM core.weather
WHERE DATE(observed_at) = CURRENT_DATE
UNION ALL
SELECT 
    'Humidity range check',
    COUNT(*),
    COUNT(*) FILTER (WHERE humidity_percent < 0 OR humidity_percent > 100)
FROM core.weather
WHERE DATE(observed_at) = CURRENT_DATE;
```

**Expected Results:**
- `violations` shows total rows checked
- `out_of_range` should be 0 or very small (within 5% tolerance)

---

### Step 7: Verify Mart Layer (Analytics Ready)

**In Adminer, run these queries:**

**Query 7: Show daily weather aggregates by location**
```sql
-- Show me today's weather summary for each location
SELECT 
    location_name,
    city,
    country,
    observation_date,
    observation_count,
    ROUND(avg_temperature_celsius::numeric, 2) as avg_temp_c,
    ROUND(min_temperature_celsius::numeric, 2) as min_temp_c,
    ROUND(max_temperature_celsius::numeric, 2) as max_temp_c,
    ROUND(avg_humidity_percent::numeric, 2) as avg_humidity_pct,
    most_common_conditions
FROM mart.daily_weather_aggregates
WHERE observation_date = CURRENT_DATE
ORDER BY location_name;
```

**Expected Results:**
- 2 rows (Boston and St Louis)
- `observation_date` = today's date
- `observation_count` >= 1
- Temperature, humidity metrics are populated
- `most_common_conditions` shows weather description

**Query 8: Show daily Wikipedia page statistics by page**
```sql
-- Show me today's Wikipedia page activity summary
SELECT 
    page_title,
    page_language,
    revision_date,
    revision_count,
    unique_editors,
    total_bytes_changed,
    ROUND(avg_bytes_per_revision::numeric, 0) as avg_bytes_per_rev
FROM mart.daily_wikipedia_page_stats
WHERE revision_date = CURRENT_DATE
ORDER BY page_title;
```

**Expected Results:**
- 4 rows (one per Wikipedia page)
- `revision_date` = today's date
- `revision_count` >= 1
- `total_bytes_changed` > 0
- `unique_editors` >= 1

**Query 9: Compare locations side-by-side**
```sql
-- Compare weather conditions between Boston and St Louis today
SELECT 
    location_name,
    observation_count,
    avg_temperature_celsius,
    avg_humidity_percent,
    avg_wind_speed_kmh,
    most_common_conditions
FROM mart.daily_weather_aggregates
WHERE observation_date = CURRENT_DATE
ORDER BY avg_temperature_celsius DESC;
```

**Expected Results:**
- Side-by-side comparison of both locations
- Shows which location is warmer/cooler
- Weather conditions for each location

---

### Step 8: Verify End-to-End Data Flow

**Final verification query:**

```sql
-- Complete data lineage check: raw -> core -> mart
SELECT 
    'Raw Weather' as layer,
    COUNT(*) as row_count,
    MAX(ingested_at) as latest_timestamp
FROM raw.weather_observations
UNION ALL
SELECT 
    'Raw Wikipedia',
    COUNT(*),
    MAX(ingested_at)
FROM raw.wikipedia_pages
UNION ALL
SELECT 
    'Core Weather Facts',
    COUNT(*),
    MAX(created_at)
FROM core.weather
WHERE DATE(observed_at) = CURRENT_DATE
UNION ALL
SELECT 
    'Core Revision Facts',
    COUNT(*),
    MAX(fetched_at)
FROM core.revision
WHERE DATE(fetched_at) = CURRENT_DATE
UNION ALL
SELECT 
    'Mart Weather Aggregates',
    COUNT(*),
    MAX(observation_date::text)
FROM mart.daily_weather_aggregates
WHERE observation_date = CURRENT_DATE
UNION ALL
SELECT 
    'Mart Wikipedia Stats',
    COUNT(*),
    MAX(revision_date::text)
FROM mart.daily_wikipedia_page_stats
WHERE revision_date = CURRENT_DATE;
```

**Expected Results:**
- All layers show row counts > 0
- Timestamps are recent (within last hour)
- Data flows: Raw → Core → Mart

---

## Sample SQL Queries for Adminer

Here are some English descriptions of useful queries you can try in Adminer:

### Query A: "Show me the latest weather observations for each location"
**What it does:** Returns the most recent weather observation for Boston and St Louis, showing temperature, humidity, and wind conditions.

**SQL:**
```sql
SELECT 
    l.location_name,
    w.observed_at,
    w.temperature_celsius,
    w.humidity_percent,
    w.wind_speed_mps
FROM core.weather w
JOIN core.location l ON w.location_id = l.location_id
WHERE w.observed_at = (
    SELECT MAX(observed_at) 
    FROM core.weather w2 
    WHERE w2.location_id = w.location_id
)
ORDER BY l.location_name;
```

### Query B: "Show me which Wikipedia pages had the most activity today"
**What it does:** Ranks Wikipedia pages by number of revisions and total bytes changed, showing the most active pages first.

**SQL:**
```sql
SELECT 
    page_title,
    revision_count,
    unique_editors,
    total_bytes_changed,
    ROUND(total_bytes_changed::numeric / NULLIF(revision_count, 0), 0) as avg_bytes_per_revision
FROM mart.daily_wikipedia_page_stats
WHERE revision_date = CURRENT_DATE
ORDER BY revision_count DESC, total_bytes_changed DESC;
```

### Query C: "Show me the temperature range for each location today"
**What it does:** Compares the minimum and maximum temperatures between Boston and St Louis, showing the temperature spread for the day.

**SQL:**
```sql
SELECT 
    location_name,
    min_temperature_celsius,
    max_temperature_celsius,
    ROUND((max_temperature_celsius - min_temperature_celsius)::numeric, 2) as temperature_range,
    avg_temperature_celsius
FROM mart.daily_weather_aggregates
WHERE observation_date = CURRENT_DATE
ORDER BY temperature_range DESC;
```

---

## Troubleshooting

### Flow Fails in Prefect UI

- **Check task logs**: Click on failed task to see error details
- **Verify database connection**: Ensure `DATABASE_URL` in `.env` is correct
- **Check API connectivity**: Ensure internet connection for Open-Meteo and Wikipedia APIs

### No Data in Raw Tables

- **Check extract tasks**: Verify `fetch_raw_weather` and `fetch_raw_wikipedia_page` completed
- **Check API responses**: Review task logs for API errors
- **Verify seed data**: Ensure locations and Wikipedia pages exist in dimensions

### Data Quality Checkpoints Fail

- **Review expectations**: Check which expectation failed in Prefect logs
- **Verify data ranges**: Ensure temperature, humidity values are within expected ranges
- **Check for duplicates**: Verify uniqueness constraints are met

### Mart Views Empty

- **Refresh views manually**: Run `REFRESH MATERIALIZED VIEW CONCURRENTLY mart.daily_weather_aggregates;`
- **Verify fact tables**: Ensure `core.weather` and `core.revision` have data
- **Check view definitions**: Verify views are defined correctly in `src/init_warehouse.sql`

---

## Next Steps

- Review detailed documentation in `docs/` directory
- Customize schedules in `prefect/deployments.py`
- Adjust data quality expectations in `src/data_quality.py`
- Add more locations or Wikipedia pages in `src/seed_data.yaml`

## Documentation

- **[Operations Runbook](docs/runbook.md)** - Day-to-day operations, failure modes, and recovery procedures
- **[Performance Guide](docs/performance.md)** - Partitioning, concurrency, and scaling strategies
- **[Architecture Diagram](docs/architecture.html)** - Visual overview of the system architecture
- **[Data Contracts](docs/data_contracts.md)** - API response structures and field definitions
- **[Acceptance Checklist](docs/ACCEPTANCE_CHECKLIST.md)** - Verification checklist for all components

---

## Contributing

_Contributing guidelines to be added..._

