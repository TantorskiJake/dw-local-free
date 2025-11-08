# How to Run the Pipeline

## The Problem

The local `prefect/` directory was conflicting with the Prefect library. It's been renamed to `workflows/` to avoid the conflict.

## How to Run

### Method 1: Direct Python Execution (Recommended)

```bash
python3 -m workflows.daily_pipeline
```

### Method 2: Run as Script

```bash
python3 workflows/daily_pipeline.py
```

### Method 3: Via Prefect CLI

```bash
# Start Prefect server (optional, for UI)
prefect server start

# Run the flow
prefect deployment run daily_pipeline/weather-schedule
```

## Quick Test

```bash
# 1. Ensure stack is running
docker-compose up -d

# 2. Run the pipeline
python3 -m workflows.daily_pipeline

# 3. Check results
docker-compose exec postgres psql -U postgres -d dw -c "
SELECT COUNT(*) FROM raw.weather_observations;
SELECT COUNT(*) FROM core.weather;
"
```

## What Changed

- `prefect/` directory → `workflows/` directory
- All imports updated
- Run command: `python3 -m workflows.daily_pipeline`

The functionality is exactly the same, just a different directory name to avoid conflicts!

