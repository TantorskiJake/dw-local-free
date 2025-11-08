# SQL Scripts Directory

This directory contains all SQL scripts organized by purpose.

## Structure

```
sql/
├── schemas/          # Database schema definitions
│   ├── init_warehouse.sql          # Main warehouse initialization (schemas, tables, views)
│   ├── update_raw_schemas.sql     # Raw layer schema updates
│   └── update_core_schemas.sql    # Core layer schema updates
├── migrations/       # Database migrations
│   └── add_weather_fields.sql     # Migration to add precipitation and cloud cover fields
└── seeds/            # Seed/reference data
    └── seed_reference_data.sql     # Initial reference data (locations, Wikipedia pages)
```

## Usage

### Initial Setup

Run scripts in order:

```bash
# 1. Initialize warehouse structure
docker-compose exec -T postgres psql -U postgres -d dw < sql/schemas/init_warehouse.sql

# 2. Update raw schemas
docker-compose exec -T postgres psql -U postgres -d dw < sql/schemas/update_raw_schemas.sql

# 3. Update core schemas
docker-compose exec -T postgres psql -U postgres -d dw < sql/schemas/update_core_schemas.sql

# 4. Load seed data
docker-compose exec -T postgres psql -U postgres -d dw < sql/seeds/seed_reference_data.sql
```

### Running Migrations

```bash
# Apply a migration
docker-compose exec -T postgres psql -U postgres -d dw < sql/migrations/add_weather_fields.sql
```

## File Descriptions

### Schemas

- **init_warehouse.sql**: Creates the main database structure including:
  - Schemas: `raw`, `core`, `mart`
  - Core tables: `location`, `weather`, `wikipedia_page`, `revision`
  - Materialized views: `daily_weather_aggregates`, `daily_wikipedia_page_stats`
  - Indexes and constraints

- **update_raw_schemas.sql**: Defines raw layer tables for storing API responses:
  - `raw.weather_observations`
  - `raw.wikipedia_pages`

- **update_core_schemas.sql**: Defines core layer tables and relationships

### Migrations

- **add_weather_fields.sql**: Adds `precipitation_mm` and `cloud_cover_percent` columns to `core.weather` table

### Seeds

- **seed_reference_data.sql**: Inserts initial reference data:
  - Location dimensions (cities)
  - Wikipedia page dimensions

## Notes

- All scripts use `IF NOT EXISTS` and `ON CONFLICT` clauses for idempotency
- Scripts can be run multiple times safely
- The CLI tool (`src/cli.py`) automatically updates `seed_reference_data.sql` when adding new locations/pages

