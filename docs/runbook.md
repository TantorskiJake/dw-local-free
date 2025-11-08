# Operations Runbook

This runbook provides operational procedures for running, monitoring, and troubleshooting the data warehouse pipeline.

## Table of Contents

1. [Day-to-Day Operations](#day-to-day-operations)
2. [Failure Modes](#failure-modes)
3. [Recovery Procedures](#recovery-procedures)

---

## Day-to-Day Operations

### Starting the Stack

**1. Start Docker Containers**

```bash
# Navigate to project root
cd /path/to/dw-local-free

# Start PostgreSQL and Adminer
docker-compose up -d

# Verify containers are running
docker-compose ps
```

**Expected Output:**
```
NAME          IMAGE            STATUS
dw-postgres   postgres:15      Up (healthy)
dw-adminer    adminer:latest   Up
```

**2. Verify Database Connectivity**

```bash
# Test PostgreSQL connection
docker-compose exec postgres psql -U postgres -d dw -c "SELECT version();"

# Or via Adminer UI
# Open http://localhost:8080
# System: PostgreSQL
# Server: postgres
# Username: postgres
# Password: postgres
# Database: dw
```

**3. Start Prefect Server (Optional, for UI)**

```bash
# Start Prefect server
prefect server start

# Server will be available at http://127.0.0.1:4200
```

**4. Start Prefect Worker (If using work queues)**

```bash
# Start worker to execute flows
prefect worker start --pool default
```

### Viewing Logs

**Docker Container Logs**

```bash
# View all container logs
docker-compose logs

# View specific service logs
docker-compose logs postgres
docker-compose logs adminer

# Follow logs in real-time
docker-compose logs -f postgres

# View last 100 lines
docker-compose logs --tail=100 postgres
```

**Prefect Flow Logs**

**Via Prefect UI:**
1. Open http://127.0.0.1:4200
2. Navigate to **Flow Runs**
3. Click on a flow run
4. Click on individual tasks to view logs
5. Use the **Logs** tab for detailed output

**Via CLI:**
```bash
# List recent flow runs
prefect flow-run ls --limit 10

# View logs for a specific flow run
prefect flow-run logs <flow-run-id>

# Stream logs in real-time
prefect flow-run logs <flow-run-id> --follow
```

**Database Logs**

```bash
# View PostgreSQL logs
docker-compose exec postgres tail -f /var/log/postgresql/postgresql-*.log

# Or view via Docker
docker-compose logs postgres | grep -i error
```

**Application Logs**

If running flows directly (not via Prefect server):
```bash
# Logs are printed to stdout/stderr
python3 -m workflows.daily_pipeline

# Redirect to file
python3 -m workflows.daily_pipeline > pipeline.log 2>&1
```

### Rerunning a Failed Flow

**Via Prefect UI (Recommended)**

1. Open Prefect UI: http://127.0.0.1:4200
2. Navigate to **Flow Runs**
3. Find the failed flow run
4. Click **"Rerun"** button
   - Option 1: **Rerun from failed task** - Continues from where it failed
   - Option 2: **Rerun from start** - Restarts entire flow

**Via Prefect CLI**

```bash
# Rerun a specific flow run
prefect flow-run rerun <flow-run-id>

# Rerun from a specific task
prefect flow-run rerun <flow-run-id> --task-run-id <task-run-id>

# Rerun latest failed run
prefect flow-run rerun $(prefect flow-run ls --state Failed --limit 1 -o json | jq -r '.[0].id')
```

**Manual Rerun**

```bash
# Run flow directly
python3 -m workflows.daily_pipeline

# Or trigger deployment
prefect deployment run daily_pipeline/weather-schedule
prefect deployment run daily_pipeline/wikipedia-schedule
```

**Checkpoint Recovery**

If a flow fails after transforms but before mart refresh:
- Data quality checkpoints prevent bad data from reaching marts
- Simply rerun the flow - idempotent upserts will handle duplicates
- Mart refresh will happen automatically on successful rerun

---

## Failure Modes

### HTTP Timeout

**Symptoms:**
- Task logs show: `TimeoutError` or `ReadTimeout`
- Prefect UI shows task as "Failed" with timeout error
- No data in raw tables for affected locations/pages

**Root Cause:**
- External API (Open-Meteo or Wikipedia) is slow or unresponsive
- Network connectivity issues
- API rate limiting causing delays

**Automatic Recovery:**

The pipeline has built-in retry logic:
```python
@task(
    retries=3,
    retry_delay_seconds=2,  # Exponential backoff
    timeout_seconds=120
)
```

**How It Works:**
1. Task fails with timeout
2. Prefect automatically retries (up to 3 times)
3. Exponential backoff: 2s, 4s, 8s delays
4. If all retries fail, task is marked as failed

**Manual Recovery:**

```bash
# Check which locations/pages failed
# In Prefect UI, check task logs for specific failures

# Rerun just the failed task
prefect flow-run rerun <flow-run-id> --task-run-id <failed-task-run-id>

# Or rerun entire flow (idempotent, won't duplicate data)
prefect deployment run daily_pipeline/weather-schedule
```

**Prevention:**
- Monitor API response times
- Adjust `timeout_seconds` if APIs are consistently slow
- Consider increasing retry count for unreliable APIs

### Malformed JSON

**Symptoms:**
- Task logs show: `JSONDecodeError` or `ValueError`
- Transform tasks fail with parsing errors
- Data in raw tables but not in core facts

**Root Cause:**
- API returned invalid JSON
- Response truncated or corrupted
- API changed response format

**Automatic Recovery:**

**Idempotent Upserts Handle This:**
```sql
INSERT INTO core.weather (...)
VALUES (...)
ON CONFLICT (location_id, observed_at) 
DO UPDATE SET ...
```

**How It Works:**
1. Raw data is stored as-is (even if malformed)
2. Transform task fails on JSON parsing
3. Rerun flow with fixed API response
4. Upsert logic prevents duplicates
5. New valid data overwrites old attempts

**Manual Recovery:**

```bash
# 1. Check raw payload for issues
docker-compose exec postgres psql -U postgres -d dw -c "
    SELECT location_name, ingested_at, payload::text
    FROM raw.weather_observations
    ORDER BY ingested_at DESC
    LIMIT 5;
"

# 2. If payload is invalid, delete the bad record
DELETE FROM raw.weather_observations 
WHERE id = <bad_record_id>;

# 3. Rerun the flow to fetch fresh data
prefect deployment run daily_pipeline/weather-schedule
```

**Prevention:**
- Validate JSON structure in extract tasks
- Log API responses before parsing
- Add schema validation using Great Expectations

### Database Lock Contention

**Symptoms:**
- Tasks hang or timeout
- PostgreSQL logs show: `deadlock detected` or `lock timeout`
- Multiple flows running simultaneously
- High concurrent write operations

**Root Cause:**
- Multiple flows trying to write to same partitions
- Long-running transactions blocking others
- Index creation/maintenance blocking writes

**Automatic Recovery:**

**Partitioning Reduces Contention:**
- Different months = different partitions = different locks
- Concurrent writes to different partitions don't block
- Indexes are per-partition (smaller, faster)

**Idempotent Upserts Handle Conflicts:**
```sql
ON CONFLICT (location_id, observed_at) DO UPDATE
```
- If two flows write same record, one succeeds, one updates
- No data loss, just overwrites with latest

**Manual Recovery:**

```bash
# 1. Check for blocking queries
docker-compose exec postgres psql -U postgres -d dw -c "
    SELECT 
        pid,
        usename,
        application_name,
        state,
        query,
        query_start,
        now() - query_start as duration
    FROM pg_stat_activity
    WHERE state != 'idle'
    ORDER BY query_start;
"

# 2. Kill blocking queries if needed (use with caution)
SELECT pg_terminate_backend(<pid>);

# 3. Check for locks
SELECT 
    locktype,
    relation::regclass,
    mode,
    granted
FROM pg_locks
WHERE NOT granted;

# 4. Rerun failed flows after locks clear
prefect flow-run rerun <flow-run-id>
```

**Prevention:**
- Use `CONCURRENTLY` for index creation
- Keep transactions short
- Use connection pooling
- Schedule flows to avoid overlap
- Monitor for long-running queries

### Data Quality Checkpoint Failures

**Symptoms:**
- Flow fails at data quality checkpoint step
- Prefect UI shows checkpoint task as "Failed"
- Error message indicates which expectation failed
- Mart views not refreshed (by design)

**Root Cause:**
- Data doesn't meet quality expectations
- Temperature/humidity out of range
- Duplicate records
- Referential integrity violations

**Automatic Recovery:**

**Checkpoints Fail Fast:**
- Flow stops immediately on checkpoint failure
- Prevents bad data from reaching marts
- No automatic retry (data quality issues need investigation)

**Manual Recovery:**

```bash
# 1. Check which expectation failed
# In Prefect UI, view checkpoint task logs

# 2. Query data to find violations
docker-compose exec postgres psql -U postgres -d dw -c "
    -- Example: Find temperature outliers
    SELECT location_id, observed_at, temperature_celsius
    FROM core.weather
    WHERE temperature_celsius < -50 OR temperature_celsius > 60
    ORDER BY observed_at DESC;
"

# 3. Fix data issues
# Option A: Delete bad records and rerun
DELETE FROM core.weather WHERE temperature_celsius < -50;

# Option B: Adjust expectation thresholds if data is valid
# Edit src/data_quality.py and update ranges

# 4. Rerun flow
prefect deployment run daily_pipeline/weather-schedule
```

**Prevention:**
- Review data quality expectations regularly
- Adjust `mostly` parameter for known edge cases
- Monitor checkpoint failure rates
- Investigate root causes of data quality issues

---

## Recovery Procedures

### Rerun a Specific Window by Date

**Scenario:** Need to backfill or reprocess data for a specific date range.

**Procedure:**

**1. Identify Missing or Incorrect Data**

```sql
-- Check what data exists for a date range
SELECT 
    DATE(observed_at) as date,
    COUNT(*) as observation_count,
    COUNT(DISTINCT location_id) as location_count
FROM core.weather
WHERE observed_at >= '2024-11-01' 
  AND observed_at < '2024-11-08'
GROUP BY DATE(observed_at)
ORDER BY date;
```

**2. Delete Data for Date Range (if reprocessing)**

```sql
-- Delete weather facts for date range
DELETE FROM core.weather
WHERE observed_at >= '2024-11-01' 
  AND observed_at < '2024-11-08';

-- Delete corresponding raw data
DELETE FROM raw.weather_observations
WHERE ingested_at >= '2024-11-01' 
  AND ingested_at < '2024-11-08';
```

**3. Modify Extract Task Temporarily**

Create a custom script or modify the flow to:
- Override date range parameters
- Fetch data for specific window
- Process and load into warehouse

**Example Script:**
```python
# scripts/backfill_date_range.py
from prefect import flow
from workflows.daily_pipeline import fetch_raw_weather, transform_weather_to_fact
from datetime import datetime, timedelta

@flow
def backfill_weather(start_date: str, end_date: str):
    """Backfill weather data for specific date range."""
    locations = [...]  # Your locations
    
    # Fetch with custom date range
    results = []
    for location in locations:
        # Modify API call to use start_date and end_date
        result = fetch_raw_weather.with_options(
            parameters={"start_date": start_date, "end_date": end_date}
        )(location)
        results.append(result)
    
    # Transform
    transform_weather_to_fact(results)

# Run backfill
backfill_weather("2024-11-01", "2024-11-08")
```

**4. Rerun Flow with Date Parameters**

```bash
# If flow supports date parameters
prefect deployment run daily_pipeline/weather-schedule \
    --param start_date=2024-11-01 \
    --param end_date=2024-11-08
```

**5. Verify Data Loaded**

```sql
-- Verify data exists for date range
SELECT 
    DATE(observed_at) as date,
    COUNT(*) as count
FROM core.weather
WHERE observed_at >= '2024-11-01' 
  AND observed_at < '2024-11-08'
GROUP BY DATE(observed_at);
```

### Rehydrate a Mart by Refreshing Materialized Views

**Scenario:** Mart views are stale or need to be rebuilt from existing fact data.

**Procedure:**

**1. Check Current State of Mart Views**

```sql
-- Check when views were last refreshed
SELECT 
    schemaname,
    matviewname,
    hasindexes,
    ispopulated
FROM pg_matviews
WHERE schemaname = 'mart';

-- Check data in views
SELECT COUNT(*) FROM mart.daily_weather_aggregates;
SELECT COUNT(*) FROM mart.daily_wikipedia_page_stats;
```

**2. Refresh Materialized Views**

**Option A: Via SQL (Manual)**

```sql
-- Refresh weather aggregates
REFRESH MATERIALIZED VIEW CONCURRENTLY mart.daily_weather_aggregates;

-- Refresh Wikipedia stats
REFRESH MATERIALIZED VIEW CONCURRENTLY mart.daily_wikipedia_page_stats;
```

**Option B: Via Prefect Flow**

```bash
# Rerun just the refresh task
# In Prefect UI, find a successful flow run
# Click on refresh_materialized_view task
# Click "Rerun" to execute just that task
```

**Option C: Via Custom Script**

```python
# scripts/refresh_marts.py
import psycopg2
import os

database_url = os.getenv("DATABASE_URL")
conn = psycopg2.connect(database_url)
cursor = conn.cursor()

views = [
    "mart.daily_weather_aggregates",
    "mart.daily_wikipedia_page_stats"
]

for view in views:
    print(f"Refreshing {view}...")
    cursor.execute(f"REFRESH MATERIALIZED VIEW CONCURRENTLY {view}")
    conn.commit()
    print(f"✓ {view} refreshed")

cursor.close()
conn.close()
```

**3. Verify Refresh Success**

```sql
-- Check row counts
SELECT 
    'daily_weather_aggregates' as view_name,
    COUNT(*) as row_count,
    MIN(observation_date) as min_date,
    MAX(observation_date) as max_date
FROM mart.daily_weather_aggregates
UNION ALL
SELECT 
    'daily_wikipedia_page_stats',
    COUNT(*),
    MIN(revision_date),
    MAX(revision_date)
FROM mart.daily_wikipedia_page_stats;

-- Verify data freshness
SELECT 
    observation_date,
    COUNT(*) as location_count
FROM mart.daily_weather_aggregates
WHERE observation_date >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY observation_date
ORDER BY observation_date DESC;
```

**4. Monitor Refresh Performance**

```sql
-- Check if refresh is in progress
SELECT 
    pid,
    usename,
    state,
    query,
    now() - query_start as duration
FROM pg_stat_activity
WHERE query LIKE '%REFRESH MATERIALIZED VIEW%';
```

### Add a New Location or Page

**Scenario:** Need to track weather for a new city or monitor a new Wikipedia page.

**Procedure:**

**1. Edit Seed Data File**

```yaml
# Edit src/seed_data.yaml

locations:
  # ... existing locations ...
  - name: San Francisco
    latitude: 37.7749
    longitude: -122.4194
    country: US
    region: California
    city: San Francisco

wikipedia_pages:
  # ... existing pages ...
  - title: San Francisco
    language: en
    namespace: 0
```

**2. Update Seed SQL Script (Optional)**

```sql
-- Edit src/seed_reference_data.sql
-- Add new location
INSERT INTO core.location (location_name, latitude, longitude, country, region, city)
VALUES 
    ('San Francisco', 37.7749, -122.4194, 'US', 'California', 'San Francisco')
ON CONFLICT (location_name, latitude, longitude) DO NOTHING;

-- Add new Wikipedia page
INSERT INTO core.wikipedia_page (wikipedia_page_id, page_title, namespace, page_language)
VALUES 
    (-5, 'San Francisco', 0, 'en')
ON CONFLICT (wikipedia_page_id, page_language) DO NOTHING;
```

**3. Load Seed Data**

```bash
# Run seed script
docker-compose exec -T postgres psql -U postgres -d dw < src/seed_reference_data.sql

# Or manually insert via Adminer
# Connect to http://localhost:8080
# Insert into core.location and core.wikipedia_page tables
```

**4. Verify Seed Data**

```sql
-- Check locations
SELECT location_name, city, country 
FROM core.location 
ORDER BY location_name;

-- Check Wikipedia pages
SELECT page_title, page_language 
FROM core.wikipedia_page 
WHERE is_current = true
ORDER BY page_title;
```

**5. Run Pipeline**

```bash
# Run flow - it will automatically pick up new locations/pages
prefect deployment run daily_pipeline/weather-schedule
prefect deployment run daily_pipeline/wikipedia-schedule

# Or run directly
python3 -m workflows.daily_pipeline
```

**6. Verify Data Collection**

```sql
-- Check raw data for new location
SELECT 
    location_name,
    COUNT(*) as raw_count,
    MAX(ingested_at) as latest_ingest
FROM raw.weather_observations
WHERE location_name = 'San Francisco'
GROUP BY location_name;

-- Check facts for new location
SELECT 
    l.location_name,
    COUNT(*) as fact_count,
    MIN(w.observed_at) as first_obs,
    MAX(w.observed_at) as last_obs
FROM core.weather w
JOIN core.location l ON w.location_id = l.location_id
WHERE l.location_name = 'San Francisco'
GROUP BY l.location_name;

-- Check mart aggregates
SELECT * 
FROM mart.daily_weather_aggregates
WHERE location_name = 'San Francisco'
ORDER BY observation_date DESC;
```

**7. Update Flow Configuration (If Hardcoded)**

If locations are hardcoded in the flow, update `workflows/daily_pipeline.py`:

```python
# Change from hardcoded list to query from database
# TODO: Query core.location to get actual locations
locations = query_locations_from_db()  # Implement this
```

**Best Practice:** Always query from `core.location` table rather than hardcoding locations.

---

## Quick Reference

### Common Commands

```bash
# Start stack
docker-compose up -d

# View logs
docker-compose logs -f postgres

# Rerun flow
prefect deployment run daily_pipeline/weather-schedule

# Refresh marts
docker-compose exec postgres psql -U postgres -d dw -c \
    "REFRESH MATERIALIZED VIEW CONCURRENTLY mart.daily_weather_aggregates;"

# Check data quality
docker-compose exec postgres psql -U postgres -d dw -c \
    "SELECT COUNT(*) FROM core.weather WHERE DATE(observed_at) = CURRENT_DATE;"
```

### Emergency Contacts

- **Prefect UI**: http://127.0.0.1:4200
- **Adminer**: http://localhost:8080
- **PostgreSQL**: localhost:5432
- **Documentation**: `docs/` directory

### Health Checks

```bash
# Database health
docker-compose exec postgres pg_isready -U postgres

# Container health
docker-compose ps

# Flow run status
prefect flow-run ls --limit 5
```

---

## Version History

- **2024-11-08**: Initial runbook creation
- **2024-12-19**: Updated commands and location counts for accuracy

