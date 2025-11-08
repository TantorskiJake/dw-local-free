# Tests

This directory contains unit tests and smoke tests for the data warehouse project.

## Test Structure

- `test_transforms.py` - Unit tests for transform logic using JSON fixtures
- `test_smoke.py` - Smoke tests that simulate full pipeline runs against temporary schemas
- `fixtures/` - JSON fixture files with sample API responses

## Running Tests Locally

### Install Dependencies

```bash
pip install -r requirements.txt
```

### Run All Tests

```bash
pytest tests/ -v
```

### Run Unit Tests Only

```bash
pytest tests/test_transforms.py -v
```

### Run Smoke Tests Only

```bash
# Requires database connection
pytest tests/test_smoke.py -v -m smoke
```

### Skip Database Tests

```bash
SKIP_DB_TESTS=true pytest tests/ -v
```

## Test Fixtures

### Weather API Response (`fixtures/weather_api_response.json`)

Sample Open-Meteo API response with:
- 4 hourly observations
- Temperature, humidity, precipitation, cloud cover, wind speed
- ISO8601 timestamps

### Wikipedia API Response (`fixtures/wikipedia_api_response.json`)

Sample MediaWiki REST API response with:
- Page metadata (pageid, title, revision)
- Timestamp and namespace information
- Content URLs and extracts

## Test Coverage

### Transform Tests (`test_transforms.py`)

- **Weather Transform**:
  - Array explosion logic
  - Wind speed conversion (km/h to m/s)
  - Temperature range validation
  - Humidity range validation
  - Timestamp parsing

- **Wikipedia Transform**:
  - Page metadata extraction
  - Revision ID format validation
  - Timestamp parsing
  - Namespace extraction
  - Content size calculation
  - Type-2 SCD logic

- **Data Quality**:
  - Uniqueness constraints
  - Data range validation

### Smoke Tests (`test_smoke.py`)

- Schema creation in temporary schema
- Table creation and data insertion
- Data flow simulation (raw → core → mart)
- Database connection validation

**Note:** Smoke tests use temporary schemas to avoid polluting production data.

## CI/CD Integration

Tests run automatically on:
- Pull requests to `main` or `develop`
- Pushes to `main` or `develop`

The CI workflow includes:
1. **Lint and Format Check** - Black, isort, flake8
2. **Unit Tests** - Fast tests without database
3. **Smoke Tests** - Full pipeline simulation with PostgreSQL
4. **All Tests** - Combined test run

Total CI time: < 2 minutes

## Writing New Tests

### Unit Test Example

```python
def test_my_transform():
    """Test my transform logic."""
    fixture = load_fixture("my_fixture.json")
    # Test logic here
    assert result == expected
```

### Smoke Test Example

```python
@pytest.mark.smoke
def test_my_smoke_test(temp_schema):
    """Test against temporary schema."""
    # Use temp_schema fixture
    # Test database operations
    pass
```

## Test Markers

- `@pytest.mark.unit` - Unit tests (no database)
- `@pytest.mark.integration` - Integration tests (require database)
- `@pytest.mark.smoke` - Smoke tests (full pipeline simulation)

Run tests by marker:
```bash
pytest -m unit
pytest -m smoke
```

