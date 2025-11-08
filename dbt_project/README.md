# dbt Project

This dbt project provides documentation, lineage graphs, and materialized mart models.

## Setup

1. **Install dbt:**
   ```bash
   pip install dbt-core dbt-postgres
   ```

2. **Configure connection:**
   The `profiles.yml` is already configured for local PostgreSQL.

3. **Test connection:**
   ```bash
   cd dbt_project
   dbt debug
   ```

## Usage

### Run Models

```bash
# Run all models
dbt run

# Run specific model
dbt run --select daily_weather_aggregates

# Run models in mart schema
dbt run --select mart.*
```

### Generate Documentation

```bash
# Generate docs
dbt docs generate

# Serve docs locally
dbt docs serve
```

Docs will be available at http://localhost:8080

### View Lineage

1. Run `dbt docs generate`
2. Run `dbt docs serve`
3. Open browser to http://localhost:8080
4. Click on "Lineage Graph" to see data flow

## Models

### Core Models (References)

- `weather` - References `core.weather` table
- `location` - References `core.location` table
- `revision` - References `core.revision` table
- `wikipedia_page` - References `core.wikipedia_page` table

### Mart Models (Materialized Views)

- `daily_weather_aggregates` - Daily weather stats by location
- `daily_wikipedia_page_stats` - Daily Wikipedia page stats

## Integration with Pipeline

The dbt models reference the same tables created by the Prefect pipeline. You can:

1. **Run pipeline first** (loads data into core tables)
2. **Run dbt** (creates/refreshes mart views)
3. **View docs** (see lineage and documentation)

Or use dbt to materialize mart views instead of SQL materialized views - both approaches work!

