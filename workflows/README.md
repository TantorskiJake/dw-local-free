# Prefect Orchestration

This directory contains Prefect flows and tasks for orchestrating the data warehouse ETL pipeline.

## Setup

1. Install Prefect:
```bash
pip install -r requirements.txt
```

2. Start Prefect server (optional, for UI):
```bash
prefect server start
```

The Prefect UI will be available at http://127.0.0.1:4200

## Flows

### daily_pipeline

The main daily ETL pipeline that orchestrates all data warehouse tasks.

**Execution Order:**
1. Ensure location dimension is up to date from seeds
2. Fetch and store raw weather data for each location (parallel)
3. Transform weather data into fact table
4. Fetch and store raw Wikipedia pages (parallel)
5. Upsert Wikipedia dimension and insert revision facts
6. Run data quality checkpoints (Great Expectations)
   - Weather fact validation
   - Wikipedia revision validation
   - **Pipeline fails if checkpoints fail**
7. Refresh materialized views (concurrent, only if checkpoints pass)

**Features:**
- Parallel execution for weather and Wikipedia fetches
- Concurrent materialized view refreshes
- Retries with exponential backoff for network tasks
- Conservative timeouts for all tasks
- Tagged with `env=local` for environment tracking

## Deployments and Schedules

Two deployments are configured for the `daily_pipeline` flow:

### Weather Schedule
- **Name**: `weather-schedule`
- **Schedule**: Hourly at :30 past each hour (UTC)
- **Cron**: `30 * * * *`
- **Frequency**: Every hour
- **Tags**: `schedule=weather`, `frequency=hourly`

### Wikipedia Schedule
- **Name**: `wikipedia-schedule`
- **Schedule**: Twice daily at 01:00 and 13:00 UTC
- **Cron**: `0 1,13 * * *`
- **Frequency**: Twice per day
- **Tags**: `schedule=wikipedia`, `frequency=twice-daily`

Both deployments support:
- ✅ Scheduled runs (automatic based on cron)
- ✅ Manual "run now" via Prefect UI or CLI

## Running the Flow

### Create Deployments

**Option 1: Using the deployment script**
```bash
python workflows/create_deployments.py
```

**Option 2: Using Prefect CLI**
```bash
# Create weather deployment
prefect deployment build workflows/daily_pipeline.py:daily_pipeline \
    -n weather-schedule \
    --schedule "30 * * * *" \
    --timezone UTC \
    --tag schedule=weather

# Create Wikipedia deployment
prefect deployment build workflows/daily_pipeline.py:daily_pipeline \
    -n wikipedia-schedule \
    --schedule "0 1,13 * * *" \
    --timezone UTC \
    --tag schedule=wikipedia

# Apply deployments
prefect deployment apply daily_pipeline-deployment.yaml
```

### Serve Deployments (Enable Scheduled Runs)

To enable scheduled runs and manual execution:

```bash
python workflows/serve_deployments.py
```

Or using Prefect serve:
```bash
prefect serve workflows/serve_deployments.py
```

### Manual "Run Now"

**Via Prefect CLI:**
```bash
# Run weather schedule manually
prefect deployment run daily_pipeline/weather-schedule

# Run Wikipedia schedule manually
prefect deployment run daily_pipeline/wikipedia-schedule
```

**Via Prefect UI:**
1. Start Prefect server: `prefect server start`
2. Open http://127.0.0.1:4200
3. Navigate to Deployments
4. Click on a deployment and select "Run"

### Direct Flow Execution (No Schedule)

For testing without deployments:
```bash
python3 -m workflows.daily_pipeline
```

## Task Configuration

| Task | Retries | Timeout | Notes |
|-----|---------|---------|-------|
| ensure_location_dimension | 2 | 60s | Database operation |
| fetch_raw_weather | 3 | 120s | Network call, exponential backoff |
| transform_weather_to_fact | 2 | 300s | Database transform |
| fetch_raw_wikipedia_page | 3 | 120s | Network call, exponential backoff |
| upsert_wikipedia_dimension_and_facts | 2 | 300s | Database transform |
| run_weather_data_quality_checkpoint | 0 | 300s | Great Expectations, no retries |
| run_wikipedia_data_quality_checkpoint | 0 | 300s | Great Expectations, no retries |
| refresh_materialized_view | 2 | 300s | Database operation |

## Environment Variables

The flow uses the following environment variables (from `.env`):
- `DATABASE_URL`: PostgreSQL connection string
- `TZ`: Timezone (should be UTC)

## Tags

The flow is tagged with:
- `env=local`: Environment identifier
- `pipeline=daily`: Pipeline type

These tags can be used for filtering and organization in the Prefect UI.

