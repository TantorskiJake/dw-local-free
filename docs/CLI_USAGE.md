# CLI Usage Guide

The data warehouse includes a CLI tool for managing seeds and triggering pipeline runs.

## Installation

The CLI is included in the project. Ensure dependencies are installed:

```bash
pip install -r requirements.txt
```

## Usage

### Add a New Location

```bash
# Basic usage
python src/cli.py add-location \
    --name "San Francisco" \
    --lat 37.7749 \
    --lon -122.4194 \
    --city "San Francisco" \
    --region "California"

# With automatic pipeline run
python src/cli.py add-location \
    --name "San Francisco" \
    --lat 37.7749 \
    --lon -122.4194 \
    --city "San Francisco" \
    --region "California" \
    --run

# Specify country (default: US)
python src/cli.py add-location \
    --name "Toronto" \
    --lat 43.6532 \
    --lon -79.3832 \
    --city "Toronto" \
    --region "Ontario" \
    --country "CA"
```

### Add a New Wikipedia Page

```bash
# Basic usage
python src/cli.py add-page --title "San Francisco"

# With language and namespace
python src/cli.py add-page \
    --title "San Francisco" \
    --language en \
    --namespace 0

# With automatic pipeline run
python src/cli.py add-page \
    --title "San Francisco" \
    --run
```

### Trigger Pipeline Run

```bash
# Run weather schedule
python src/cli.py run-pipeline --deployment weather-schedule

# Run Wikipedia schedule
python src/cli.py run-pipeline --deployment wikipedia-schedule
```

## Convenience Wrapper

Use the wrapper script for easier access:

```bash
# Make executable (first time only)
chmod +x scripts/dw-cli

# Use wrapper
./scripts/dw-cli add-location --name "Portland" --lat 45.5152 --lon -122.6784 --city "Portland" --region "Oregon"
```

## What It Does

### Adding Locations/Pages

1. **Validates** that location/page doesn't already exist
2. **Updates** `src/seed_data.yaml` with new entry
3. **Updates** `src/seed_reference_data.sql` with SQL INSERT
4. **Optionally triggers** pipeline run if `--run` flag is used

### Pipeline Triggering

1. **Attempts** to use Prefect CLI (`prefect deployment run`)
2. **Falls back** to direct Python execution if Prefect CLI unavailable
3. **Reports** success/failure status

## Examples

### Complete Workflow: Add City and Run Pipeline

```bash
# 1. Add location
python src/cli.py add-location \
    --name "Portland" \
    --lat 45.5152 \
    --lon -122.6784 \
    --city "Portland" \
    --region "Oregon" \
    --run

# Output:
# Adding location: Portland
# ✅ Added location: Portland (Portland, Oregon)
#    Coordinates: 45.5152, -122.6784
#    Updated seed_reference_data.sql
# 
# 🚀 Triggering pipeline run...
# ✅ Pipeline run triggered: weather-schedule
```

### Add Multiple Locations

```bash
# Add several cities
python src/cli.py add-location --name "Austin" --lat 30.2672 --lon -97.7431 --city "Austin" --region "Texas"
python src/cli.py add-location --name "Portland" --lat 45.5152 --lon -122.6784 --city "Portland" --region "Oregon"
python src/cli.py add-location --name "Nashville" --lat 36.1627 --lon -86.7816 --city "Nashville" --region "Tennessee"

# Then run pipeline once
python src/cli.py run-pipeline
```

## Troubleshooting

### Location Already Exists

```bash
# Error: ⚠️  Location 'Boston' already exists. Skipping.
# Solution: Use different name or check existing locations
python src/cli.py add-location --name "Boston North" ...
```

### Prefect CLI Not Found

```bash
# CLI will automatically fall back to direct execution
# Ensure Prefect is installed: pip install prefect
```

### SQL File Update Issues

If SQL file update fails:
1. Manually edit `src/seed_reference_data.sql`
2. Run seed script: `docker-compose exec -T postgres psql -U postgres -d dw < src/seed_reference_data.sql`

## Integration with Runbook

See `docs/runbook.md` for:
- How to manually add locations/pages
- Recovery procedures
- Verification steps

