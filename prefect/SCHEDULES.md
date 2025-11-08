# Pipeline Schedules

This document describes the scheduled deployments for the data warehouse pipeline.

## Overview

The `daily_pipeline` flow has two deployments with different schedules:

1. **Weather Schedule**: Runs hourly to fetch fresh weather data
2. **Wikipedia Schedule**: Runs twice daily to capture Wikipedia page changes

Both deployments can also be triggered manually for testing and demos.

## Weather Schedule

- **Deployment Name**: `weather-schedule`
- **Schedule**: Hourly at :30 past each hour (UTC)
- **Cron Expression**: `30 * * * *`
- **Examples**:
  - 00:30 UTC
  - 01:30 UTC
  - 02:30 UTC
  - ... (every hour)

**Why hourly?**
- Weather data updates frequently
- Forecasts are most accurate when recent
- Hourly runs ensure fresh data for analysis

## Wikipedia Schedule

- **Deployment Name**: `wikipedia-schedule`
- **Schedule**: Twice daily at 01:00 and 13:00 UTC
- **Cron Expression**: `0 1,13 * * *`
- **Examples**:
  - 01:00 UTC (1:00 AM UTC)
  - 13:00 UTC (1:00 PM UTC)

**Why twice daily?**
- Wikipedia pages change less frequently than weather
- Twice daily captures most significant changes
- Balances data freshness with API usage

## Manual Execution

Both deployments support manual "run now" functionality:

### Via CLI
```bash
# Run weather schedule
prefect deployment run daily_pipeline/weather-schedule

# Run Wikipedia schedule
prefect deployment run daily_pipeline/wikipedia-schedule
```

### Via Prefect UI
1. Navigate to http://127.0.0.1:4200
2. Go to Deployments
3. Select a deployment
4. Click "Run" button

## Timezone

All schedules use **UTC** timezone to ensure consistency across environments and avoid daylight saving time issues.

## Schedule Management

### View Schedules
```bash
prefect deployment ls
```

### Update Schedules
Edit the cron expressions in:
- `prefect/create_deployments.py`
- `prefect/serve_deployments.py`

Then recreate the deployments:
```bash
python prefect/create_deployments.py
```

### Disable Schedules
To temporarily disable scheduled runs, pause the deployment:
```bash
prefect deployment pause daily_pipeline/weather-schedule
prefect deployment pause daily_pipeline/wikipedia-schedule
```

To resume:
```bash
prefect deployment resume daily_pipeline/weather-schedule
prefect deployment resume daily_pipeline/wikipedia-schedule
```

## Cron Expression Reference

| Expression | Meaning |
|------------|---------|
| `30 * * * *` | Every hour at :30 |
| `0 1,13 * * *` | At 01:00 and 13:00 daily |
| `0 */6 * * *` | Every 6 hours |
| `0 0 * * *` | Daily at midnight |
| `0 0 * * 1` | Weekly on Monday at midnight |

Format: `minute hour day month weekday`

## Testing Schedules

To test that schedules are working:

1. **Check deployment status**:
   ```bash
   prefect deployment ls
   ```

2. **View next scheduled run**:
   ```bash
   prefect deployment inspect daily_pipeline/weather-schedule
   ```

3. **Manually trigger a run**:
   ```bash
   prefect deployment run daily_pipeline/weather-schedule
   ```

4. **Monitor in UI**:
   - Open Prefect UI
   - Navigate to Deployments
   - View run history and schedule

