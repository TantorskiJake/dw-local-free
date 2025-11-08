# Quick Start Guide - Running the Pipeline

## Step 1: Start the Stack

```bash
# Start PostgreSQL and Adminer
docker-compose up -d

# Verify containers are running
docker-compose ps
```

You should see:
- `dw-postgres` - Up and healthy
- `dw-adminer` - Up

## Step 2: Verify Database is Ready

```bash
# Check that schemas exist
docker-compose exec postgres psql -U postgres -d dw -c "
SELECT schema_name FROM information_schema.schemata 
WHERE schema_name IN ('raw', 'core', 'mart');
"

# Check seed data
docker-compose exec postgres psql -U postgres -d dw -c "
SELECT COUNT(*) as locations FROM core.location;
SELECT COUNT(*) as pages FROM core.wikipedia_page WHERE is_current = true;
"
```

Expected: 3 schemas, 10 locations, 4 pages

## Step 3: Install Dependencies (if not already done)

```bash
pip install -r requirements.txt
```

## Step 4: Run the Pipeline

### Option A: Direct Python Execution (Simplest)

```bash
python3 -m prefect.daily_pipeline
```

### Option B: Via Prefect CLI (Recommended for monitoring)

```bash
# Start Prefect server (in one terminal)
prefect server start

# In another terminal, run the flow
prefect deployment run daily_pipeline/weather-schedule

# Or run directly
python3 -m prefect.daily_pipeline
```

## Step 5: Monitor Progress

### Via Console Output

You'll see logs like:
```
Starting daily pipeline
Ensuring location dimension is up to date from seeds
Found 10 locations to process
Fetching weather data for Boston
Fetching weather data for St Louis
...
Weather transform completed: {'status': 'success', 'locations_processed': 10, 'rows_inserted': 240}
...
```

### Via Prefect UI (if server is running)

1. Open http://127.0.0.1:4200
2. Click "Flow Runs"
3. Click on the latest run
4. See task execution details and logs

## Step 6: Verify Results

### Check Raw Tables

```bash
docker-compose exec postgres psql -U postgres -d dw -c "
SELECT 
    location_name,
    COUNT(*) as raw_count,
    MAX(ingested_at) as latest_ingest
FROM raw.weather_observations
GROUP BY location_name
ORDER BY location_name;
"
```

### Check Core Facts

```bash
docker-compose exec postgres psql -U postgres -d dw -c "
SELECT 
    l.location_name,
    COUNT(*) as fact_count,
    MIN(w.observed_at) as first_obs,
    MAX(w.observed_at) as last_obs
FROM core.weather w
JOIN core.location l ON w.location_id = l.location_id
GROUP BY l.location_name
ORDER BY l.location_name;
"
```

### Check Mart Views

```bash
docker-compose exec postgres psql -U postgres -d dw -c "
SELECT 
    location_name,
    observation_date,
    observation_count,
    ROUND(avg_temperature_celsius::numeric, 2) as avg_temp
FROM mart.daily_weather_aggregates
ORDER BY observation_date DESC, location_name
LIMIT 10;
"
```

### Via Adminer (Visual)

1. Open http://localhost:8080
2. Login:
   - System: PostgreSQL
   - Server: postgres
   - Username: postgres
   - Password: postgres
   - Database: dw
3. Browse tables:
   - `raw.weather_observations` - Should have 10 rows (one per location)
   - `raw.wikipedia_pages` - Should have 4 rows (one per page)
   - `core.weather` - Should have many rows (hourly data exploded)
   - `core.revision` - Should have 4 rows (one per page)
   - `mart.daily_weather_aggregates` - Should have aggregated daily data

## Step 7: Check Data Quality

The pipeline automatically runs Great Expectations checkpoints. Check Prefect UI or logs for:
- ✅ Weather data quality checkpoint PASSED
- ✅ Wikipedia data quality checkpoint PASSED

## Troubleshooting

### Pipeline Fails with Import Error

```bash
# Make sure you're in the project root
cd /path/to/dw-local-free

# Install dependencies
pip install -r requirements.txt
```

### Database Connection Error

```bash
# Check database is running
docker-compose ps

# Check connection string
echo $DATABASE_URL
# Should be: postgresql://postgres:postgres@localhost:5432/dw

# Or set it explicitly
export DATABASE_URL="postgresql://postgres:postgres@localhost:5432/dw"
```

### API Errors

- Check internet connection
- Open-Meteo and Wikipedia APIs should be accessible
- Check task logs in Prefect UI for specific errors

### No Data in Tables

- Check raw tables first - if empty, extract failed
- Check task logs for errors
- Verify seed data exists in dimensions

## Next Steps

Once the pipeline runs successfully:

1. **Enable Scheduled Runs:**
   ```bash
   python prefect/create_deployments.py
   python prefect/serve_deployments.py
   ```

2. **Add More Locations:**
   ```bash
   python src/cli.py add-location \
       --name "Portland" \
       --lat 45.5152 \
       --lon -122.6784 \
       --city "Portland" \
       --region "Oregon" \
       --run
   ```

3. **View dbt Documentation:**
   ```bash
   cd dbt_project
   dbt docs generate
   dbt docs serve
   ```

## Expected Runtime

- First run: ~30-60 seconds
  - API calls: ~10-20 seconds (10 locations + 4 pages)
  - Transforms: ~5-10 seconds
  - Data quality: ~5-10 seconds
  - Mart refresh: ~5-10 seconds

- Subsequent runs: Faster (idempotent upserts)

## Success Indicators

✅ Pipeline completes without errors  
✅ Raw tables have data  
✅ Core facts have data  
✅ Mart views have aggregated data  
✅ Data quality checkpoints pass  
✅ Prefect UI shows green "Completed" status  

You're all set! 🎉

