# Extract Tasks Plan

This document outlines the extract tasks for ingesting data from external APIs into the raw layer of the data warehouse.

## Overview

Two extract tasks will be implemented:
1. **Weather Extract**: Fetches hourly weather data from Open-Meteo API
2. **Wikipedia Extract**: Fetches page metadata from MediaWiki REST API

Both tasks follow an append-only pattern, ensuring historical data is preserved.

---

## Weather Extract Task

### Purpose
Extract hourly weather observations for seeded locations and store the full API response in the raw layer.

### Logic Flow

1. **Read Seeded Locations**
   - Query `core.location` table to get all locations
   - For each location, extract: `location_id`, `location_name`, `latitude`, `longitude`

2. **Calculate Time Range**
   - **Start Date**: 24 hours ago from current time (last day)
   - **End Date**: Current time + available forecast hours (typically 7 days ahead)
   - Format dates as `YYYY-MM-DD` for API request
   - All times in UTC

3. **API Request**
   - For each location:
     - Endpoint: `https://api.open-meteo.com/v1/forecast`
     - Parameters:
       - `latitude`: location latitude
       - `longitude`: location longitude
       - `hourly`: `temperature_2m,relativehumidity_2m,precipitation,cloudcover,windspeed_10m`
       - `timezone`: `UTC`
       - `start_date`: calculated start date
       - `end_date`: calculated end date
     - Method: GET
     - Response: JSON object with full API response

4. **Data Persistence**
   - Insert one record per location per run into `raw.weather_observations`
   - Fields:
     - `location_name`: from location dimension
     - `latitude`: from location dimension
     - `longitude`: from location dimension
     - `observed_at`: NULL (not applicable for raw JSON payload)
     - `temperature_celsius`: NULL (extracted in transform)
     - `humidity_percent`: NULL (extracted in transform)
     - `pressure_hpa`: NULL (not in API response)
     - `wind_speed_kmh`: NULL (extracted in transform)
     - `wind_direction_degrees`: NULL (not in API response)
     - `conditions`: NULL (not in API response)
     - `ingested_at`: CURRENT_TIMESTAMP (load timestamp)
     - `source`: JSON string of full API response

   **Note**: Since we're storing the full JSON payload, most fields will be NULL in the raw table. The JSON is stored in the `source` field (or we may need to adjust the schema to have a JSON/JSONB column).

### Schema Consideration

The current `raw.weather_observations` table may need adjustment to store the full JSON payload. Options:
- Add a `payload` JSONB column to store the full response
- Store JSON as text in an existing field
- Modify schema to have a dedicated JSON column

**Recommended**: Add `payload JSONB` column to `raw.weather_observations` table.

### Example Record

```json
{
  "location_name": "Boston",
  "latitude": 42.3601,
  "longitude": -71.0589,
  "observed_at": null,
  "temperature_celsius": null,
  "humidity_percent": null,
  "pressure_hpa": null,
  "wind_speed_kmh": null,
  "wind_direction_degrees": null,
  "conditions": null,
  "ingested_at": "2024-11-08T13:00:00Z",
  "source": "open-meteo",
  "payload": {
    "latitude": 42.3601,
    "longitude": -71.0589,
    "generationtime_ms": 0.123,
    "utc_offset_seconds": 0,
    "timezone": "UTC",
    "hourly_units": {...},
    "hourly": {
      "time": ["2024-11-07T13:00", ...],
      "temperature_2m": [15.2, ...],
      "relativehumidity_2m": [65.0, ...],
      "precipitation": [0.0, ...],
      "cloudcover": [45.0, ...],
      "windspeed_10m": [12.5, ...]
    }
  }
}
```

### Error Handling

- If API request fails, log error and skip location (don't fail entire job)
- If location has no data, still insert record with NULL payload
- Handle rate limiting (if applicable)
- Validate JSON response before insertion

### Append-Only Guarantee

- No UPDATE or DELETE operations on `raw.weather_observations`
- Each run creates new records
- Historical data preserved indefinitely

---

## Wikipedia Extract Task

### Purpose
Extract Wikipedia page metadata for seeded titles and store page information in the raw layer.

### Logic Flow

1. **Read Seeded Wikipedia Pages**
   - Query `core.wikipedia_page` table to get all pages
   - For each page, extract: `page_id`, `page_title`, `page_language`

2. **API Request**
   - For each page:
     - Endpoint: `https://en.wikipedia.org/api/rest_v1/page/summary/{title}`
     - URL encode the page title (replace spaces with underscores)
     - Method: GET
     - Response: JSON object with page summary
   
   - For content size:
     - Endpoint: `https://en.wikipedia.org/api/rest_v1/page/html/{title}`
     - Method: GET
     - Response: HTML string
     - Calculate: `content_size_bytes = len(response_body.encode('utf-8'))`

3. **Data Persistence**
   - Insert one record per page per revision into `raw.wikipedia_pages`
   - Fields:
     - `page_id`: from summary response (`pageid`)
     - `page_title`: from summary response (`title`)
     - `namespace`: from summary response (`namespace.id`)
     - `revision_id`: from summary response (`revision`)
     - `revision_timestamp`: from summary response (`timestamp`)
     - `revision_user`: NULL (not in summary endpoint)
     - `revision_size_bytes`: calculated from HTML endpoint
     - `page_language`: from page dimension or summary response (`lang`)
     - `ingested_at`: CURRENT_TIMESTAMP (fetched timestamp)
     - `source`: JSON string of summary response (or store in separate JSON column)

### Schema Consideration

The current `raw.wikipedia_pages` table may need adjustment:
- Add `payload JSONB` column to store full summary response
- Ensure `revision_id` can store string values (currently BIGINT, may need VARCHAR)
- Consider storing HTML content separately if needed

**Recommended**: 
- Add `payload JSONB` column
- Change `revision_id` to VARCHAR to accommodate string revision IDs
- Add `content_size_bytes` calculation to existing field

### Example Record

```json
{
  "page_id": 12345,
  "page_title": "Boston",
  "namespace": 0,
  "revision_id": "123456789",
  "revision_timestamp": "2024-11-08T12:00:00Z",
  "revision_user": null,
  "revision_size_bytes": 245678,
  "page_language": "en",
  "ingested_at": "2024-11-08T13:00:00Z",
  "source": "mediawiki-rest-api",
  "payload": {
    "type": "standard",
    "title": "Boston",
    "pageid": 12345,
    "revision": "123456789",
    "timestamp": "2024-11-08T12:00:00Z",
    ...
  }
}
```

### Revision Tracking

- Each API call captures the current revision
- If the same revision is fetched multiple times, multiple records are created (append-only)
- The `revision_id` field identifies the specific revision
- Duplicate revisions can be deduplicated in the transform layer if needed

### Error Handling

- If page doesn't exist (404), log error and skip page
- If API request fails, log error and skip page (don't fail entire job)
- Handle rate limiting
- Validate JSON response before insertion
- Handle missing fields gracefully

### Append-Only Guarantee

- No UPDATE or DELETE operations on `raw.wikipedia_pages`
- Each run creates new records
- Historical revisions preserved indefinitely
- Same revision fetched multiple times = multiple records (idempotent by design)

---

## Implementation Notes

### Common Patterns

1. **Idempotency**: Both tasks are idempotent - running multiple times creates new records without side effects
2. **Error Isolation**: Failures for one location/page don't stop processing of others
3. **Full Payload Storage**: Store complete API responses for audit and reprocessing
4. **Timestamp Tracking**: Always record when data was fetched (`ingested_at`)

### Scheduling

- Weather extract: Run hourly (or as needed for forecast updates)
- Wikipedia extract: Run on a schedule (e.g., daily or hourly) to capture revisions

### Data Volume Estimates

- **Weather**: ~1 record per location per run (small payload, ~1-5 KB per record)
- **Wikipedia**: ~1 record per page per run (medium payload, ~10-50 KB per record)

### Next Steps

1. Update raw table schemas to include JSONB payload columns
2. Implement weather extract task
3. Implement Wikipedia extract task
4. Add error handling and logging
5. Set up scheduling (Prefect flows)

---

## Schema Updates Required

### raw.weather_observations
```sql
ALTER TABLE raw.weather_observations 
ADD COLUMN payload JSONB;
```

### raw.wikipedia_pages
```sql
ALTER TABLE raw.wikipedia_pages 
ADD COLUMN payload JSONB,
ALTER COLUMN revision_id TYPE VARCHAR(255);
```

---

## Version History

- **2024-11-08**: Initial extract tasks plan

