# Core Transforms Plan

This document outlines the transform tasks that move data from the raw layer to the core dimensional model, implementing business logic and data quality rules.

## Overview

Two transform tasks will be implemented:
1. **Weather Transform**: Explodes hourly arrays and upserts into weather fact table
2. **Wikipedia Transform**: Implements type-2 SCD for page dimension and inserts revision facts

Both transforms maintain data lineage through `raw_ref` columns.

---

## Weather Transform

### Purpose
Transform raw Open-Meteo API responses into normalized weather fact records, one per hour per location.

### Logic Flow

1. **Read Latest Raw Payload Per Location**
   - Query `raw.weather_observations`
   - For each location (identified by `location_name`, `latitude`, `longitude`):
     - Get the record with the latest `ingested_at` timestamp
     - Extract the `payload` JSONB column
   - If no payload exists, skip location

2. **Parse JSON Payload**
   - Extract `hourly` object from payload
   - Extract arrays:
     - `time`: Array of ISO8601 timestamps
     - `temperature_2m`: Array of temperatures in Celsius
     - `relativehumidity_2m`: Array of humidity percentages
     - `precipitation`: Array of precipitation in mm
     - `cloudcover`: Array of cloud cover percentages
     - `windspeed_10m`: Array of wind speeds in km/h

3. **Explode Arrays into Rows**
   - For each index in the arrays (arrays are aligned by index):
     - Create one row with:
       - `time[index]` → `observed_at`
       - `temperature_2m[index]` → `temperature_celsius`
       - `relativehumidity_2m[index]` → `humidity_percent`
       - `precipitation[index]` → (not stored in fact, but available)
       - `cloudcover[index]` → (not stored in fact, but available)
       - `windspeed_10m[index]` → convert to m/s → `wind_speed_mps`

4. **Unit Conversions**
   - **Temperature**: Already in Celsius, no conversion needed
   - **Wind Speed**: Convert from km/h to m/s
     - Formula: `wind_speed_mps = windspeed_10m * (1000 / 3600) = windspeed_10m / 3.6`
   - **Humidity**: Already in percentage, no conversion needed
   - **Pressure**: Not in API response, leave as NULL

5. **Lookup Location ID**
   - Match `location_name`, `latitude`, `longitude` from raw record to `core.location`
   - Get `location_id` for foreign key relationship
   - If location not found, log error and skip

6. **Upsert into Weather Fact**
   - Key: `(location_id, observed_at)`
   - Use PostgreSQL `ON CONFLICT` with `DO UPDATE`:
     ```sql
     INSERT INTO core.weather (
         location_id, observed_at, temperature_celsius, 
         humidity_percent, wind_speed_mps, raw_ref
     )
     VALUES (...)
     ON CONFLICT (location_id, observed_at) 
     DO UPDATE SET
         temperature_celsius = EXCLUDED.temperature_celsius,
         humidity_percent = EXCLUDED.humidity_percent,
         wind_speed_mps = EXCLUDED.wind_speed_mps,
         raw_ref = EXCLUDED.raw_ref,
         created_at = EXCLUDED.created_at
     ```
   - Store `raw_ref` as reference to the source raw record (e.g., raw record ID or JSON snippet)

### Data Quality Rules

- Skip rows where `time` is NULL or invalid
- Skip rows where `location_id` cannot be resolved
- Handle missing values in arrays (NULL values)
- Validate timestamp format before insertion
- Ensure wind speed conversion is accurate (km/h → m/s)

### Example Transformation

**Input (Raw Payload)**:
```json
{
  "hourly": {
    "time": ["2024-11-08T00:00", "2024-11-08T01:00"],
    "temperature_2m": [15.2, 14.8],
    "relativehumidity_2m": [65.0, 67.0],
    "windspeed_10m": [12.5, 13.2]
  }
}
```

**Output (Fact Rows)**:
```
location_id=1, observed_at='2024-11-08T00:00', temperature_celsius=15.2, humidity_percent=65.0, wind_speed_mps=3.47, raw_ref='{"raw_id": 123}'
location_id=1, observed_at='2024-11-08T01:00', temperature_celsius=14.8, humidity_percent=67.0, wind_speed_mps=3.67, raw_ref='{"raw_id": 123}'
```

### Schema Requirements

- `core.weather` needs:
  - `wind_speed_mps` column (DECIMAL) for meters per second
  - `raw_ref` column (JSONB or VARCHAR) for lineage
  - Unique constraint on `(location_id, observed_at)` for upsert

---

## Wikipedia Transform

### Purpose
Transform raw Wikipedia API responses into type-2 slowly changing dimension (SCD) records and revision fact records.

### Logic Flow

#### Part 1: Update Page Dimension (Type-2 SCD)

1. **Read Latest Raw Payload Per Page**
   - Query `raw.wikipedia_pages`
   - For each unique `page_title` and `page_language`:
     - Get the record with the latest `ingested_at` timestamp
     - Extract the `payload` JSONB column
   - If no payload exists, skip page

2. **Extract Page Metadata**
   - From payload, extract:
     - `pageid` → `wikipedia_page_id`
     - `title` → `page_title`
     - `namespace.id` → `namespace`
     - `lang` → `page_language`
     - `revision` → current revision ID
     - `timestamp` → revision timestamp

3. **Check for Changes**
   - Lookup existing record in `core.wikipedia_page`:
     - Match on `wikipedia_page_id` and `page_language`
     - Find the current row (`is_current = true`)
   - Compare:
     - If `page_title` changed → Type-2 change (title change)
     - If `revision` changed but title same → No dimension change, only fact change
     - If no existing record → New page

4. **Handle Type-2 SCD Logic**
   
   **Case A: New Page (No Existing Record)**
   - Insert new row:
     - `wikipedia_page_id`, `page_title`, `namespace`, `page_language`
     - `valid_from` = CURRENT_TIMESTAMP
     - `valid_to` = NULL
     - `is_current` = true

   **Case B: Title Changed (Type-2 Change)**
   - Update existing current row:
     - Set `valid_to` = CURRENT_TIMESTAMP
     - Set `is_current` = false
   - Insert new current row:
     - New `page_title` value
     - `valid_from` = CURRENT_TIMESTAMP
     - `valid_to` = NULL
     - `is_current` = true
     - Same `wikipedia_page_id` and `page_language`

   **Case C: No Title Change (Only Revision Change)**
   - No dimension update needed
   - Proceed to fact insert only

5. **Get Page ID for Fact**
   - After dimension update, get the `page_id` (surrogate key) for the current row
   - This will be used in the revision fact

#### Part 2: Insert Revision Fact

1. **Extract Revision Data**
   - From raw payload:
     - `revision` → `revision_id` (string)
     - `timestamp` → `revision_timestamp`
     - From raw record:
     - `revision_size_bytes` → `content_len`
     - `ingested_at` → `fetched_at`

2. **Upsert Revision Fact**
   - Key: `(page_id, revision_id)`
   - Use PostgreSQL `ON CONFLICT` with `DO NOTHING` (revisions are immutable):
     ```sql
     INSERT INTO core.revision (
         page_id, revision_id, revision_timestamp,
         content_len, fetched_at, raw_ref
     )
     VALUES (...)
     ON CONFLICT (page_id, revision_id) DO NOTHING
     ```
   - Store `raw_ref` as reference to source raw record

### Type-2 SCD Example

**Initial State**:
```
page_id=1, wikipedia_page_id=12345, page_title='Boston', is_current=true, valid_from='2024-11-01', valid_to=NULL
```

**After Title Change to 'Boston, Massachusetts'**:
```
page_id=1, wikipedia_page_id=12345, page_title='Boston', is_current=false, valid_from='2024-11-01', valid_to='2024-11-08'
page_id=2, wikipedia_page_id=12345, page_title='Boston, Massachusetts', is_current=true, valid_from='2024-11-08', valid_to=NULL
```

### Data Quality Rules

- Skip rows where `pageid` is NULL
- Skip rows where `page_title` is NULL
- Validate revision timestamp format
- Ensure `is_current` flag is properly maintained (only one current row per `wikipedia_page_id` + `page_language`)
- Handle missing `revision_size_bytes` gracefully

### Schema Requirements

- `core.wikipedia_page` needs:
  - `valid_from` TIMESTAMP (when this version became active)
  - `valid_to` TIMESTAMP (when this version became inactive, NULL for current)
  - `is_current` BOOLEAN (true for current version)
  - Unique constraint on `(wikipedia_page_id, page_language, is_current)` where `is_current = true`

- `core.revision` needs:
  - `revision_id` VARCHAR (to match raw table)
  - `content_len` INTEGER (renamed from `revision_size_bytes`)
  - `fetched_at` TIMESTAMP (when data was fetched)
  - `raw_ref` JSONB or VARCHAR (for lineage)
  - Unique constraint on `(page_id, revision_id)` for upsert

---

## Common Patterns

### Lineage Tracking

Both transforms store `raw_ref` in fact tables:
- **Purpose**: Trace back to source raw record
- **Format**: JSONB containing:
  - `raw_table`: table name (e.g., "weather_observations")
  - `raw_id`: primary key of raw record
  - `ingested_at`: timestamp from raw record
- **Usage**: Enables reprocessing, debugging, and audit trails

### Upsert Strategy

- **Weather**: Upsert on conflict (data can be updated if API provides corrections)
- **Revision**: Insert only on conflict (revisions are immutable historical records)

### Error Handling

- Log all errors but continue processing other records
- Skip records that fail validation
- Maintain audit log of transform runs
- Track success/failure counts per run

---

## Implementation Notes

### Performance Considerations

- **Weather Transform**: Process one location at a time to manage memory (large arrays)
- **Wikipedia Transform**: Batch process pages for efficiency
- Use transactions to ensure atomicity of type-2 SCD updates
- Consider using CTEs for complex type-2 logic

### Testing Strategy

- Test with sample payloads from data contracts
- Verify unit conversions (especially wind speed)
- Test type-2 SCD scenarios (new page, title change, no change)
- Verify upsert logic handles conflicts correctly
- Test with missing/null values

### Next Steps

1. Update core table schemas (add columns, constraints)
2. Implement weather transform logic
3. Implement Wikipedia transform logic
4. Add error handling and logging
5. Create Prefect tasks for transforms

---

## Schema Updates Required

See `src/update_core_schemas.sql` for detailed migration script.

### core.weather
- Add `wind_speed_mps DECIMAL(5, 2)`
- Add `raw_ref JSONB`
- Add unique constraint on `(location_id, observed_at)`

### core.wikipedia_page
- Add `valid_from TIMESTAMP`
- Add `valid_to TIMESTAMP`
- Add `is_current BOOLEAN DEFAULT true`
- Update unique constraint

### core.revision
- Change `revision_id` from SERIAL to VARCHAR(255)
- Rename `revision_size_bytes` to `content_len`
- Add `fetched_at TIMESTAMP`
- Add `raw_ref JSONB`
- Add unique constraint on `(page_id, revision_id)`

---

## Version History

- **2024-11-08**: Initial core transforms plan

