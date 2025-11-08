# Data Quality Checks

This document describes the data quality framework using Great Expectations.

## Overview

Data quality checkpoints run after data transforms but before materialized view refreshes. If any checkpoint fails, the pipeline stops and marks the flow as failed, preventing bad data from reaching the mart layer.

## Expectation Suites

### Weather Fact Suite (`weather_fact_suite`)

Validates weather fact table data quality:

1. **Uniqueness Check**
   - Expects `(location_id, observed_at)` to be unique
   - Ensures no duplicate observations for the same location and time

2. **Temperature Range**
   - Expects temperature between -50°C and 60°C
   - Allows 5% outliers (`mostly=0.95`)
   - Catches invalid or corrupted temperature data

3. **Humidity Range**
   - Expects humidity between 0% and 100%
   - Allows 5% outliers
   - Validates percentage values are within bounds

4. **Wind Speed Range**
   - Expects wind speed between 0 m/s and 200 m/s
   - Allows 5% outliers
   - Validates non-negative and reasonable values

5. **Minimum Rows**
   - Expects at least 1 row per location
   - Validates that data was loaded for each location
   - Ensures we have at least the hours returned per location

6. **Required Fields**
   - Validates that `location_id`, `observed_at`, `temperature_celsius`, `humidity_percent`, and `wind_speed_mps` exist

### Wikipedia Revision Suite (`wikipedia_revision_suite`)

Validates Wikipedia revision fact table data quality:

1. **Uniqueness Check**
   - Expects `(page_id, revision_id)` to be unique
   - Ensures no duplicate revisions for the same page

2. **Content Length**
   - Expects `content_len` to be greater than 0
   - Validates that pages have content

3. **Referential Integrity**
   - Validates that all `page_id` values in facts exist in the `wikipedia_page` dimension
   - Implemented via SQL JOIN in checkpoint query
   - Ensures data integrity between fact and dimension tables

4. **Required Fields**
   - Validates that `page_id`, `revision_id`, and `content_len` exist

## Checkpoints

### Weather Fact Checkpoint

- **Name**: `weather_fact_checkpoint`
- **Suite**: `weather_fact_suite`
- **Query**: Selects recent weather data (last 1000 rows by `created_at`)
- **Failure Action**: Raises exception, stops pipeline

### Wikipedia Revision Checkpoint

- **Name**: `wikipedia_revision_checkpoint`
- **Suite**: `wikipedia_revision_suite`
- **Query**: Selects recent revision data with referential integrity check (JOIN with dimension)
- **Failure Action**: Raises exception, stops pipeline

## Integration with Pipeline

### Execution Order

1. Weather transform completes
2. Wikipedia transform completes
3. **Weather checkpoint runs** ← Stops here if fails
4. **Wikipedia checkpoint runs** ← Stops here if fails
5. Materialized views refresh (only if checkpoints pass)

### Failure Handling

- **No Retries**: Checkpoints have `retries=0` - data quality failures should not be retried
- **Flow Failure**: If checkpoint fails, exception is raised and flow is marked as failed
- **Mart Refresh Skipped**: Materialized views are NOT refreshed if checkpoints fail
- **Error Logging**: Detailed error messages logged for debugging

## Configuration

### Great Expectations Setup

Great Expectations is configured to use:
- **Datasource**: PostgreSQL database (from `DATABASE_URL` environment variable)
- **Store Backend**: Filesystem (`./great_expectations/` directory)
- **Data Connector**: Runtime data connector (queries executed at runtime)

### Directory Structure

```
great_expectations/
├── expectations/
│   ├── weather_fact_suite.json
│   └── wikipedia_revision_suite.json
├── checkpoints/
│   ├── weather_fact_checkpoint.yml
│   └── wikipedia_revision_checkpoint.yml
└── great_expectations.yml
```

## Usage

### Initialize Great Expectations

```python
from src.data_quality import initialize_great_expectations

context = initialize_great_expectations()
```

### Run Checkpoints

```python
from src.data_quality import run_weather_checkpoint, run_wikipedia_checkpoint

# Run weather checkpoint
weather_result = run_weather_checkpoint(context)

# Run Wikipedia checkpoint
wikipedia_result = run_wikipedia_checkpoint(context)
```

### Custom Queries

You can pass custom SQL queries to filter the batch:

```python
# Check only recent data
query = """
    SELECT * FROM core.weather 
    WHERE created_at > NOW() - INTERVAL '1 hour'
"""
result = run_weather_checkpoint(context, batch_query=query)
```

## Monitoring

### Checkpoint Results

Each checkpoint returns:
- `success`: Boolean indicating if all expectations passed
- `statistics`: Dictionary with validation statistics
- `result`: Full Great Expectations result object

### Logging

Checkpoints log:
- Start of checkpoint execution
- Pass/fail status
- Detailed error messages on failure
- Statistics and metrics

## Best Practices

1. **Run After Transforms**: Checkpoints run after data is loaded into core tables
2. **Fail Fast**: Don't retry on data quality failures - investigate the root cause
3. **Custom Queries**: Use batch queries to validate specific time ranges or data subsets
4. **Update Expectations**: Adjust expectation thresholds as you learn about your data
5. **Monitor Trends**: Track checkpoint results over time to identify data quality trends

## Troubleshooting

### Checkpoint Fails

1. Check logs for specific expectation that failed
2. Query the data to see what values caused the failure
3. Adjust expectation thresholds if needed (e.g., `mostly` parameter)
4. Investigate source data if failures are unexpected

### Great Expectations Not Initialized

If you see errors about missing context:
```bash
python src/data_quality.py
```

This will initialize Great Expectations and create expectation suites.

### Database Connection Issues

Ensure `DATABASE_URL` environment variable is set correctly:
```bash
export DATABASE_URL=postgresql://postgres:postgres@localhost:5432/dw
```

## Version History

- **2024-11-08**: Initial data quality framework with Great Expectations
- **2024-12-19**: Updated documentation for consistency

