# Next Steps - Implementation Guide

## ✅ What's Complete

All infrastructure and orchestration is set up:
- ✅ Database schemas, tables, and materialized views
- ✅ Prefect flow orchestration with schedules
- ✅ Great Expectations data quality framework
- ✅ Docker Compose setup
- ✅ Documentation (runbook, performance, architecture)
- ✅ CI/CD pipeline
- ✅ CLI tool for managing seeds
- ✅ dbt Core integration

## 🚧 What Needs Implementation

The extract and transform tasks currently contain placeholder logic. To make the pipeline fully operational, you need to implement:

### 1. Extract Tasks (Priority: High)

**File:** `prefect/daily_pipeline.py`

#### `fetch_raw_weather` Task
**Current:** Returns mock data  
**Needs:** Actual Open-Meteo API integration

**Implementation Steps:**
1. Import `requests` library
2. Calculate date range (last 24 hours + forecast)
3. Call Open-Meteo API:
   ```python
   url = "https://api.open-meteo.com/v1/forecast"
   params = {
       "latitude": location["latitude"],
       "longitude": location["longitude"],
       "hourly": "temperature_2m,relativehumidity_2m,precipitation,cloudcover,windspeed_10m",
       "timezone": "UTC",
       "start_date": start_date,
       "end_date": end_date
   }
   response = requests.get(url, params=params)
   data = response.json()
   ```
4. Insert into `raw.weather_observations`:
   ```python
   # Use psycopg2 or SQLAlchemy to insert
   # Store full JSON in payload column
   ```

#### `fetch_raw_wikipedia_page` Task
**Current:** Returns mock data  
**Needs:** Actual MediaWiki REST API integration

**Implementation Steps:**
1. Call `/api/rest_v1/page/summary/{title}` for metadata
2. Call `/api/rest_v1/page/html/{title}` for content size
3. Insert into `raw.wikipedia_pages` with full JSON payload

### 2. Transform Tasks (Priority: High)

#### `transform_weather_to_fact` Task
**Current:** Returns mock data  
**Needs:** Actual transformation logic

**Implementation Steps:**
1. Query latest raw payload per location from `raw.weather_observations`
2. Parse JSONB payload, extract `hourly` arrays
3. Explode arrays: for each index, create one row
4. Convert units:
   - Temperature: already Celsius ✓
   - Wind speed: km/h → m/s (divide by 3.6)
5. Lookup `location_id` from `core.location`
6. Upsert into `core.weather`:
   ```sql
   INSERT INTO core.weather (location_id, observed_at, temperature_celsius, ...)
   VALUES (...)
   ON CONFLICT (location_id, observed_at) 
   DO UPDATE SET ...
   ```

#### `upsert_wikipedia_dimension_and_facts` Task
**Current:** Returns mock data  
**Needs:** Type-2 SCD and revision fact logic

**Implementation Steps:**
1. Query latest raw payload per page from `raw.wikipedia_pages`
2. Extract page metadata (pageid, title, revision, etc.)
3. Check for changes in `core.wikipedia_page`:
   - If title changed: Close old row, insert new current row
   - If no change: Skip dimension update
4. Insert revision fact into `core.revision`

### 3. Seed Data Loading (Priority: Medium)

#### `ensure_location_dimension` Task
**Current:** Returns mock data  
**Needs:** Load from `src/seed_data.yaml`

**Implementation Steps:**
1. Read `src/seed_data.yaml` using PyYAML
2. Upsert locations into `core.location`
3. Upsert Wikipedia pages into `core.wikipedia_page`

## 📋 Implementation Checklist

### Phase 1: Extract Tasks
- [ ] Implement `fetch_raw_weather` with Open-Meteo API
- [ ] Implement `fetch_raw_wikipedia_page` with MediaWiki API
- [ ] Test extract tasks independently
- [ ] Verify data in `raw.weather_observations` and `raw.wikipedia_pages`

### Phase 2: Transform Tasks
- [ ] Implement `transform_weather_to_fact` with array explosion
- [ ] Implement `upsert_wikipedia_dimension_and_facts` with type-2 SCD
- [ ] Test transform tasks independently
- [ ] Verify data in `core.weather` and `core.revision`

### Phase 3: Seed Loading
- [ ] Implement `ensure_location_dimension` to load from YAML
- [ ] Test seed loading
- [ ] Verify locations and pages in dimensions

### Phase 4: End-to-End Testing
- [ ] Run full pipeline manually
- [ ] Verify data flows: raw → core → mart
- [ ] Verify data quality checkpoints pass
- [ ] Verify materialized views refresh
- [ ] Test scheduled runs

## 🛠️ Quick Start Implementation

### Step 1: Implement Weather Extract

Create `src/extract_weather.py`:
```python
import requests
import psycopg2
import os
from datetime import datetime, timedelta
import json

def fetch_weather_data(location, database_url):
    """Fetch weather data from Open-Meteo and store in raw table."""
    # Calculate date range
    end_date = datetime.utcnow()
    start_date = end_date - timedelta(days=1)
    
    # Call API
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": location["latitude"],
        "longitude": location["longitude"],
        "hourly": "temperature_2m,relativehumidity_2m,precipitation,cloudcover,windspeed_10m",
        "timezone": "UTC",
        "start_date": start_date.strftime("%Y-%m-%d"),
        "end_date": end_date.strftime("%Y-%m-%d")
    }
    
    response = requests.get(url, params=params)
    response.raise_for_status()
    data = response.json()
    
    # Insert into raw table
    conn = psycopg2.connect(database_url)
    cursor = conn.cursor()
    
    cursor.execute("""
        INSERT INTO raw.weather_observations 
        (location_name, latitude, longitude, payload, ingested_at, source)
        VALUES (%s, %s, %s, %s, %s, %s)
    """, (
        location["location_name"],
        location["latitude"],
        location["longitude"],
        json.dumps(data),
        datetime.utcnow(),
        "open-meteo"
    ))
    
    conn.commit()
    cursor.close()
    conn.close()
    
    return {"status": "success", "records_inserted": 1}
```

Then update `prefect/daily_pipeline.py` to call this function.

### Step 2: Implement Weather Transform

Create `src/transform_weather.py`:
```python
import psycopg2
import json
from datetime import datetime

def transform_weather_to_fact(database_url):
    """Transform raw weather data to fact table."""
    conn = psycopg2.connect(database_url)
    cursor = conn.cursor()
    
    # Get latest raw payload per location
    cursor.execute("""
        SELECT DISTINCT ON (location_name) 
            location_name, latitude, longitude, payload
        FROM raw.weather_observations
        ORDER BY location_name, ingested_at DESC
    """)
    
    raw_records = cursor.fetchall()
    
    for location_name, lat, lon, payload_json in raw_records:
        payload = json.loads(payload_json)
        hourly = payload.get("hourly", {})
        
        # Get location_id
        cursor.execute("""
            SELECT location_id FROM core.location
            WHERE location_name = %s AND latitude = %s AND longitude = %s
        """, (location_name, lat, lon))
        
        location_result = cursor.fetchone()
        if not location_result:
            continue
        
        location_id = location_result[0]
        
        # Explode arrays
        times = hourly.get("time", [])
        temps = hourly.get("temperature_2m", [])
        humidities = hourly.get("relativehumidity_2m", [])
        wind_speeds = hourly.get("windspeed_10m", [])
        
        for i in range(len(times)):
            observed_at = datetime.fromisoformat(times[i].replace("Z", "+00:00"))
            temp = temps[i] if i < len(temps) else None
            humidity = humidities[i] if i < len(humidities) else None
            wind_kmh = wind_speeds[i] if i < len(wind_speeds) else None
            wind_mps = wind_kmh / 3.6 if wind_kmh is not None else None
            
            # Upsert
            cursor.execute("""
                INSERT INTO core.weather 
                (location_id, observed_at, temperature_celsius, humidity_percent, wind_speed_mps, raw_ref)
                VALUES (%s, %s, %s, %s, %s, %s::jsonb)
                ON CONFLICT (location_id, observed_at)
                DO UPDATE SET
                    temperature_celsius = EXCLUDED.temperature_celsius,
                    humidity_percent = EXCLUDED.humidity_percent,
                    wind_speed_mps = EXCLUDED.wind_speed_mps,
                    raw_ref = EXCLUDED.raw_ref
            """, (
                location_id,
                observed_at,
                temp,
                humidity,
                wind_mps,
                json.dumps({"source": "open-meteo", "location": location_name})
            ))
    
    conn.commit()
    cursor.close()
    conn.close()
    
    return {"status": "success"}
```

## 🎯 Immediate Next Steps

1. **Test the Stack:**
   ```bash
   # Verify everything is running
   docker-compose ps
   
   # Check database
   docker-compose exec postgres psql -U postgres -d dw -c "SELECT COUNT(*) FROM core.location;"
   ```

2. **Start with One Extract Task:**
   - Pick `fetch_raw_weather` or `fetch_raw_wikipedia_page`
   - Implement the API call
   - Test it independently
   - Verify data appears in raw table

3. **Implement Corresponding Transform:**
   - Once extract works, implement the transform
   - Test with the data you just extracted
   - Verify data appears in core fact table

4. **Iterate:**
   - Repeat for the other extract/transform pair
   - Test end-to-end
   - Fix any issues

5. **Enable Schedules:**
   ```bash
   # Create deployments
   python prefect/create_deployments.py
   
   # Serve deployments (enables scheduled runs)
   python prefect/serve_deployments.py
   ```

## 📚 Resources

- **Data Contracts:** `docs/data_contracts.md` - API response structures
- **Extract Plan:** `docs/extract_tasks_plan.md` - Detailed extract logic
- **Transform Plan:** `docs/core_transforms_plan.md` - Detailed transform logic
- **Runbook:** `docs/runbook.md` - Operations and troubleshooting
- **API Docs:**
  - Open-Meteo: https://open-meteo.com/en/docs
  - MediaWiki REST: https://www.mediawiki.org/wiki/API:REST_API

## 💡 Tips

1. **Start Small:** Implement one location/page first, then scale
2. **Test Incrementally:** Test each task independently before running full flow
3. **Use Adminer:** Visual inspection of data helps debug
4. **Check Logs:** Prefect UI shows detailed task logs
5. **Idempotent Design:** Your upserts are already idempotent, so safe to rerun

## 🎉 You're Ready!

The infrastructure is complete. Now it's time to implement the data pipeline logic. Start with one extract task, get it working, then move to the transform. You'll have a fully operational data warehouse soon!

