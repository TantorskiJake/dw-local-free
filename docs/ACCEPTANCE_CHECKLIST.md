# Acceptance Checklist

This document verifies that all components of the data warehouse are working correctly.

## ✅ Stack Startup

- [ ] **One command starts all services**
  ```bash
  docker-compose up -d
  ```
  - PostgreSQL container starts and becomes healthy
  - Adminer container starts and is accessible on port 8080
  - Both containers show as "Up" in `docker-compose ps`

**Verification:**
```bash
docker-compose ps
# Should show both containers as "Up" and postgres as "healthy"
```

---

## ✅ DDL Execution

- [ ] **All DDL scripts run cleanly**
  ```bash
  docker-compose exec -T postgres psql -U postgres -d dw < src/init_warehouse.sql
  docker-compose exec -T postgres psql -U postgres -d dw < src/update_raw_schemas.sql
  docker-compose exec -T postgres psql -U postgres -d dw < src/update_core_schemas.sql
  docker-compose exec -T postgres psql -U postgres -d dw < src/seed_reference_data.sql
  ```
  - No errors in output
  - All schemas created (raw, core, mart)
  - All tables created with correct structure
  - Indexes and constraints created
  - Materialized views created

**Verification:**
```sql
-- Check schemas
SELECT schema_name FROM information_schema.schemata 
WHERE schema_name IN ('raw', 'core', 'mart');

-- Check tables
SELECT table_schema, table_name 
FROM information_schema.tables 
WHERE table_schema IN ('raw', 'core', 'mart')
ORDER BY table_schema, table_name;

-- Check materialized views
SELECT schemaname, matviewname 
FROM pg_matviews 
WHERE schemaname = 'mart';
```

---

## ✅ Manual Flow Run

- [ ] **Flow runs successfully**
  ```bash
  python3 -m prefect.daily_pipeline
  # OR
  prefect deployment run daily_pipeline/weather-schedule
  ```
  - Flow completes without errors
  - All tasks execute in correct order
  - Tasks show as "Completed" in Prefect UI

- [ ] **Raw tables receive data**
  - `raw.weather_observations` has rows with recent `ingested_at` timestamps
  - `raw.wikipedia_pages` has rows with recent `ingested_at` timestamps
  - `payload` columns contain JSONB data

**Verification:**
```sql
SELECT COUNT(*) as count, MAX(ingested_at) as latest 
FROM raw.weather_observations;

SELECT COUNT(*) as count, MAX(ingested_at) as latest 
FROM raw.wikipedia_pages;
```

- [ ] **Core facts populated**
  - `core.weather` has rows for today's date
  - `core.revision` has rows for today's date
  - Data linked correctly to dimensions

**Verification:**
```sql
SELECT COUNT(*) FROM core.weather 
WHERE DATE(observed_at) = CURRENT_DATE;

SELECT COUNT(*) FROM core.revision 
WHERE DATE(fetched_at) = CURRENT_DATE;
```

- [ ] **Mart views populated**
  - `mart.daily_weather_aggregates` has rows for today
  - `mart.daily_wikipedia_page_stats` has rows for today

**Verification:**
```sql
SELECT COUNT(*) FROM mart.daily_weather_aggregates 
WHERE observation_date = CURRENT_DATE;

SELECT COUNT(*) FROM mart.daily_wikipedia_page_stats 
WHERE revision_date = CURRENT_DATE;
```

---

## ✅ Data Quality Checkpoints

- [ ] **Checkpoints pass**
  - `run_weather_data_quality_checkpoint` shows as "Success" in Prefect UI
  - `run_wikipedia_data_quality_checkpoint` shows as "Success" in Prefect UI
  - No exceptions or errors in checkpoint logs
  - Flow continues to mart refresh (checkpoints passed)

**Verification:**
- In Prefect UI, check task logs for both checkpoint tasks
- Should see "Weather data quality checkpoint PASSED"
- Should see "Wikipedia data quality checkpoint PASSED"

---

## ✅ Scheduled Runs

- [ ] **Weather schedule triggers automatically**
  - Schedule: Hourly at :30 past each hour (UTC)
  - Flow runs automatically at scheduled times
  - New rows appear in raw tables after scheduled runs

- [ ] **Wikipedia schedule triggers automatically**
  - Schedule: Twice daily at 01:00 and 13:00 UTC
  - Flow runs automatically at scheduled times
  - New rows appear in raw tables after scheduled runs

**Verification:**
```sql
-- Check for multiple runs (should see increasing counts)
SELECT 
    DATE(ingested_at) as date,
    COUNT(*) as run_count,
    COUNT(DISTINCT DATE_TRUNC('hour', ingested_at)) as hourly_runs
FROM raw.weather_observations
GROUP BY DATE(ingested_at)
ORDER BY date DESC;
```

---

## ✅ Prefect UI History

- [ ] **Flow run history visible**
  - Navigate to Prefect UI: http://127.0.0.1:4200
  - Go to "Flow Runs"
  - See history of `daily_pipeline` runs
  - Runs show as "Completed" (green) with successful status

- [ ] **Task details available**
  - Click on any flow run
  - See all tasks with their status
  - Can view logs for each task
  - Checkpoints show as "Success"

**Verification:**
- Open Prefect UI
- Navigate to Flow Runs
- Verify multiple runs exist
- Verify all runs show "Completed" status
- Click on a run and verify all tasks are green

---

## ✅ Documentation

- [ ] **README contains demo script**
  - README.md has "Live Demo Script" section
  - Step-by-step instructions for running pipeline
  - Verification queries included
  - Sample SQL queries with English descriptions

- [ ] **Links to runbook**
  - README links to `docs/runbook.md`
  - Runbook contains day-to-day operations
  - Runbook contains failure modes
  - Runbook contains recovery procedures

- [ ] **Links to performance notes**
  - README or architecture page links to `docs/performance.md`
  - Performance doc explains partitioning
  - Performance doc explains concurrency
  - Performance doc explains Citus sharding strategy

**Verification:**
- Open README.md
- Verify "Live Demo Script" section exists
- Check for links to runbook.md
- Check for links to performance.md
- Open architecture.html and verify links work

---

## ✅ CI Pipeline

- [ ] **CI runs on main branch**
  - GitHub Actions workflow exists: `.github/workflows/ci.yml`
  - Workflow triggers on push to main
  - Workflow triggers on pull requests

- [ ] **CI jobs pass**
  - Lint and format check passes
  - Unit tests pass
  - Smoke tests pass (if database available)
  - All tests complete in < 2 minutes

**Verification:**
- Check `.github/workflows/ci.yml` exists
- Verify workflow triggers on push/PR
- Check GitHub Actions tab shows green checkmarks
- Verify all jobs complete successfully

---

## Quick Verification Commands

Run these commands to verify the entire stack:

```bash
# 1. Start stack
docker-compose up -d

# 2. Verify containers
docker-compose ps

# 3. Check schemas
docker-compose exec postgres psql -U postgres -d dw -c "
SELECT schema_name FROM information_schema.schemata 
WHERE schema_name IN ('raw', 'core', 'mart');
"

# 4. Check tables
docker-compose exec postgres psql -U postgres -d dw -c "
SELECT COUNT(*) as table_count 
FROM information_schema.tables 
WHERE table_schema IN ('raw', 'core', 'mart');
"

# 5. Check seed data
docker-compose exec postgres psql -U postgres -d dw -c "
SELECT COUNT(*) as location_count FROM core.location;
SELECT COUNT(*) as page_count FROM core.wikipedia_page WHERE is_current = true;
"

# 6. Run flow (if Prefect installed)
python3 -m prefect.daily_pipeline

# 7. Check Prefect UI
# Open http://127.0.0.1:4200 and verify flow runs
```

---

## Expected Results

### After Initial Setup:
- ✅ 3 schemas (raw, core, mart)
- ✅ 7+ tables across schemas
- ✅ 2 materialized views
- ✅ 10 locations seeded
- ✅ 4 Wikipedia pages seeded

### After First Flow Run:
- ✅ Raw tables have data
- ✅ Core facts have data
- ✅ Mart views have data
- ✅ Checkpoints passed
- ✅ Flow marked as "Completed"

### After Scheduled Runs:
- ✅ Multiple flow runs in history
- ✅ Increasing row counts in raw tables
- ✅ New data in core and mart layers

---

## Troubleshooting

If any item fails:

1. **Stack won't start**: Check Docker is running, ports 5432 and 8080 are available
2. **DDL errors**: Check PostgreSQL logs, verify database exists
3. **Flow fails**: Check Prefect logs, verify dependencies installed
4. **No data**: Verify extract/transform tasks are implemented (currently placeholders)
5. **Checkpoints fail**: Check Great Expectations setup, verify data exists
6. **CI fails**: Check GitHub Actions logs, verify test dependencies

See `docs/runbook.md` for detailed troubleshooting procedures.

---

## Version

- **Created**: 2024-11-08
- **Last Updated**: 2024-11-08

