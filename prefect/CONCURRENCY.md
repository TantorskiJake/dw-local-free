# Concurrency Control

## Weather API Rate Limiting

The pipeline processes 10 locations but limits concurrent API calls to 3 to respect rate limits and prevent overwhelming the Open-Meteo API.

### Implementation

The concurrency limit is enforced at the Prefect work queue level. To set this up:

1. **Create a work queue with concurrency limit:**
   ```bash
   prefect work-queue create weather-api-queue --concurrency-limit 3
   ```

2. **Start a worker with the queue:**
   ```bash
   prefect worker start --pool default --work-queue weather-api-queue
   ```

3. **Update deployment to use the queue:**
   ```python
   deployment = daily_pipeline.to_deployment(
       name="weather-schedule",
       work_queue_name="weather-api-queue",
       ...
   )
   ```

### Alternative: Task-Level Concurrency

For simpler local testing, you can use Prefect's concurrency blocks:

```python
from prefect.concurrency import concurrency

@task
def fetch_raw_weather(location):
    with concurrency("weather_api", occupy=1, limit=3):
        # API call here
        pass
```

This ensures only 3 tasks execute concurrently across all flow runs.

### Current Setup

The flow is configured to process 10 locations:
- Boston, St Louis, New York, Chicago, Los Angeles
- Miami, Seattle, Denver, Phoenix, Atlanta

With a concurrency limit of 3, the execution pattern is:
- First 3 locations execute concurrently
- Next 3 locations execute when slots available
- Continues until all 10 are processed

This provides a good balance between:
- **Speed**: Parallel processing (3x faster than sequential)
- **API Safety**: Respects rate limits (won't overwhelm API)
- **Efficiency**: Better resource utilization

