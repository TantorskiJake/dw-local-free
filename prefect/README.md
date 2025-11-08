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
6. Run data quality checkpoints
7. Refresh materialized views (concurrent)

**Features:**
- Parallel execution for weather and Wikipedia fetches
- Concurrent materialized view refreshes
- Retries with exponential backoff for network tasks
- Conservative timeouts for all tasks
- Tagged with `env=local` for environment tracking

## Running the Flow

### Option 1: Python script
```bash
python -m prefect.daily_pipeline
```

### Option 2: Prefect CLI
```bash
prefect deployment build prefect/daily_pipeline.py:daily_pipeline -n daily-pipeline-local
prefect deployment apply daily_pipeline-deployment.yaml
prefect deployment run daily_pipeline/daily-pipeline-local
```

### Option 3: Prefect UI
1. Start Prefect server: `prefect server start`
2. Open http://127.0.0.1:4200
3. Navigate to Flows
4. Run `daily_pipeline` flow

## Task Configuration

| Task | Retries | Timeout | Notes |
|-----|---------|---------|-------|
| ensure_location_dimension | 2 | 60s | Database operation |
| fetch_raw_weather | 3 | 120s | Network call, exponential backoff |
| transform_weather_to_fact | 2 | 300s | Database transform |
| fetch_raw_wikipedia_page | 3 | 120s | Network call, exponential backoff |
| upsert_wikipedia_dimension_and_facts | 2 | 300s | Database transform |
| run_data_quality_checkpoints | 1 | 180s | Validation |
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

