"""
Transform functions for converting raw data to core fact tables.
"""

import psycopg2
from psycopg2 import sql
import json
import os
from datetime import datetime, timezone
from typing import Dict, Any
import logging

logger = logging.getLogger(__name__)

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/dw")


def transform_weather_to_fact() -> Dict[str, Any]:
    """
    Transform raw weather data into weather fact table.
    
    Reads latest raw payload per location, explodes hourly arrays,
    converts units, and upserts into core.weather.
    
    Returns:
        Dictionary with transform summary
    """
    conn = psycopg2.connect(DATABASE_URL)
    cursor = conn.cursor()
    
    # Helper function to ensure partition exists
    def ensure_partition(cursor, observed_at):
        """Ensure the partition for the given timestamp exists."""
        partition_name = f"weather_{observed_at.year}_{observed_at.month:02d}"
        start_date = observed_at.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        if observed_at.month == 12:
            end_date = start_date.replace(year=observed_at.year + 1, month=1)
        else:
            end_date = start_date.replace(month=observed_at.month + 1)
        
        # Check if partition exists
        cursor.execute("""
            SELECT EXISTS (
                SELECT 1 FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = 'core' AND c.relname = %s
            )
        """, (partition_name,))
        
        if not cursor.fetchone()[0]:
            # Create partition using proper SQL escaping
            # Use format identifier for table name (safe since we control the format)
            create_partition_sql = sql.SQL("""
                CREATE TABLE IF NOT EXISTS core.{partition} PARTITION OF core.weather
                FOR VALUES FROM (%s) TO (%s)
            """).format(partition=sql.Identifier(partition_name))
            cursor.execute(create_partition_sql, (start_date, end_date))
            logger.info(f"Created partition: {partition_name} for {start_date} to {end_date}")
    
    # Get latest raw payload per location
    cursor.execute("""
        SELECT DISTINCT ON (location_name, latitude, longitude)
            location_name, latitude, longitude, payload, id as raw_id
        FROM raw.weather_observations
        ORDER BY location_name, latitude, longitude, ingested_at DESC
    """)
    
    raw_records = cursor.fetchall()
    total_rows_inserted = 0
    locations_processed = 0
    
    for location_name, lat, lon, payload_json, raw_id in raw_records:
        try:
            if not payload_json:
                logger.warning(f"No payload for {location_name}, skipping")
                continue
            
            payload = json.loads(payload_json) if isinstance(payload_json, str) else payload_json
            hourly = payload.get("hourly", {})
            
            if not hourly:
                logger.warning(f"No hourly data for {location_name}, skipping")
                continue
            
            # Get location_id
            cursor.execute("""
                SELECT location_id FROM core.location
                WHERE location_name = %s AND latitude = %s AND longitude = %s
            """, (location_name, float(lat), float(lon)))
            
            location_result = cursor.fetchone()
            if not location_result:
                logger.warning(f"Location not found: {location_name}, skipping")
                continue
            
            location_id = location_result[0]
            
            # Extract arrays
            times = hourly.get("time", [])
            temps = hourly.get("temperature_2m", [])
            humidities = hourly.get("relativehumidity_2m", [])
            wind_speeds_kmh = hourly.get("windspeed_10m", [])
            precipitations = hourly.get("precipitation", [])
            cloud_covers = hourly.get("cloudcover", [])
            
            if not times:
                logger.warning(f"No time data for {location_name}, skipping")
                continue
            
            # Explode arrays into rows
            rows_inserted = 0
            processed_timestamps = set()  # Track which partitions we've ensured
            
            for i in range(len(times)):
                try:
                    # Parse timestamp
                    time_str = times[i]
                    if time_str.endswith("Z"):
                        time_str = time_str.replace("Z", "+00:00")
                    observed_at = datetime.fromisoformat(time_str)
                    
                    # Ensure partition exists (only check once per month)
                    partition_key = (observed_at.year, observed_at.month)
                    if partition_key not in processed_timestamps:
                        ensure_partition(cursor, observed_at)
                        processed_timestamps.add(partition_key)
                    
                    # Get values (handle missing/null)
                    temp = temps[i] if i < len(temps) and temps[i] is not None else None
                    humidity = humidities[i] if i < len(humidities) and humidities[i] is not None else None
                    wind_kmh = wind_speeds_kmh[i] if i < len(wind_speeds_kmh) and wind_speeds_kmh[i] is not None else None
                    precipitation = precipitations[i] if i < len(precipitations) and precipitations[i] is not None else None
                    cloud_cover = cloud_covers[i] if i < len(cloud_covers) and cloud_covers[i] is not None else None
                    
                    # Convert wind speed: km/h to m/s
                    wind_mps = (wind_kmh / 3.6) if wind_kmh is not None else None
                    
                    # Create raw_ref for lineage
                    raw_ref = json.dumps({
                        "raw_table": "weather_observations",
                        "raw_id": raw_id,
                        "location_name": location_name
                    })
                    
                    # Upsert into fact table
                    cursor.execute("""
                        INSERT INTO core.weather 
                        (location_id, observed_at, temperature_celsius, humidity_percent, 
                         wind_speed_mps, precipitation_mm, cloud_cover_percent, raw_ref, created_at)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s)
                        ON CONFLICT (location_id, observed_at)
                        DO UPDATE SET
                            temperature_celsius = EXCLUDED.temperature_celsius,
                            humidity_percent = EXCLUDED.humidity_percent,
                            wind_speed_mps = EXCLUDED.wind_speed_mps,
                            precipitation_mm = EXCLUDED.precipitation_mm,
                            cloud_cover_percent = EXCLUDED.cloud_cover_percent,
                            raw_ref = EXCLUDED.raw_ref
                    """, (
                        location_id,
                        observed_at,
                        temp,
                        humidity,
                        wind_mps,
                        precipitation,
                        cloud_cover,
                        raw_ref,
                        datetime.now(timezone.utc)
                    ))
                    
                    rows_inserted += 1
                    
                except Exception as e:
                    logger.warning(f"Error processing row {i} for {location_name}: {e}")
                    # Rollback this location's transaction and continue with next location
                    conn.rollback()
                    break  # Break out of inner loop, will continue to next location
            
            conn.commit()
            total_rows_inserted += rows_inserted
            locations_processed += 1
            logger.info(f"Processed {location_name}: {rows_inserted} rows inserted")
            
        except Exception as e:
            logger.error(f"Error processing {location_name}: {e}")
            try:
                conn.rollback()
            except:
                pass
            # Create new connection for next location
            conn.close()
            conn = psycopg2.connect(DATABASE_URL)
            cursor = conn.cursor()
            continue
    
    cursor.close()
    conn.close()
    
    return {
        "status": "success",
        "locations_processed": locations_processed,
        "rows_inserted": total_rows_inserted
    }


def transform_wikipedia_to_fact() -> Dict[str, Any]:
    """
    Transform raw Wikipedia data into dimension and fact tables.
    
    Implements type-2 SCD for page dimension and inserts revision facts.
    
    Returns:
        Dictionary with transform summary
    """
    conn = psycopg2.connect(DATABASE_URL)
    cursor = conn.cursor()
    
    # Get latest raw payload per page
    cursor.execute("""
        SELECT DISTINCT ON (page_title, page_language)
            page_title, page_language, payload, id as raw_id,
            page_id, revision_id, revision_timestamp, revision_size_bytes
        FROM raw.wikipedia_pages
        ORDER BY page_title, page_language, ingested_at DESC
    """)
    
    raw_records = cursor.fetchall()
    pages_processed = 0
    revisions_inserted = 0
    
    for (page_title, page_language, payload_json, raw_id, 
         page_id, revision_id, revision_timestamp, revision_size_bytes) in raw_records:
        
        try:
            if not payload_json:
                logger.warning(f"No payload for {page_title}, skipping")
                continue
            
            payload = json.loads(payload_json) if isinstance(payload_json, str) else payload_json
            
            # Extract metadata
            wikipedia_page_id = payload.get("pageid", page_id)
            current_title = payload.get("title", page_title)
            namespace_id = payload.get("namespace", {})
            if isinstance(namespace_id, dict):
                namespace_id = namespace_id.get("id", 0)
            else:
                namespace_id = namespace_id or 0
            
            current_revision_id = str(payload.get("revision", revision_id))
            
            # Parse timestamp
            timestamp_str = payload.get("timestamp", revision_timestamp)
            if timestamp_str:
                try:
                    if timestamp_str.endswith("Z"):
                        timestamp_str = timestamp_str.replace("Z", "+00:00")
                    revision_ts = datetime.fromisoformat(timestamp_str)
                except:
                    revision_ts = datetime.now(timezone.utc)
            else:
                revision_ts = datetime.now(timezone.utc)
            
            # Check if page exists in dimension
            cursor.execute("""
                SELECT page_id, page_title, is_current
                FROM core.wikipedia_page
                WHERE wikipedia_page_id = %s AND page_language = %s AND is_current = true
            """, (wikipedia_page_id, page_language))
            
            existing_page = cursor.fetchone()
            
            if existing_page:
                existing_page_id, existing_title, _ = existing_page
                
                # Check if title changed (type-2 SCD)
                if existing_title != current_title:
                    logger.info(f"Title change detected for page {wikipedia_page_id}: '{existing_title}' -> '{current_title}'")
                    
                    # Close old row
                    cursor.execute("""
                        UPDATE core.wikipedia_page
                        SET valid_to = %s, is_current = false
                        WHERE page_id = %s
                    """, (datetime.now(timezone.utc), existing_page_id))
                    
                    # Insert new current row
                    cursor.execute("""
                        INSERT INTO core.wikipedia_page
                        (wikipedia_page_id, page_title, namespace, page_language,
                         valid_from, valid_to, is_current)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        RETURNING page_id
                    """, (
                        wikipedia_page_id,
                        current_title,
                        namespace_id,
                        page_language,
                        datetime.now(timezone.utc),
                        None,
                        True
                    ))
                    
                    current_page_id = cursor.fetchone()[0]
                else:
                    # No change, use existing page_id
                    current_page_id = existing_page_id
            else:
                # New page
                cursor.execute("""
                    INSERT INTO core.wikipedia_page
                    (wikipedia_page_id, page_title, namespace, page_language,
                     valid_from, valid_to, is_current)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    RETURNING page_id
                """, (
                    wikipedia_page_id,
                    current_title,
                    namespace_id,
                    page_language,
                    datetime.now(timezone.utc),
                    None,
                    True
                ))
                
                current_page_id = cursor.fetchone()[0]
            
            # Insert revision fact
            raw_ref = json.dumps({
                "raw_table": "wikipedia_pages",
                "raw_id": raw_id,
                "page_title": page_title
            })
            
            cursor.execute("""
                INSERT INTO core.revision
                (page_id, revision_id, revision_timestamp, content_len, fetched_at, raw_ref)
                VALUES (%s, %s, %s, %s, %s, %s::jsonb)
                ON CONFLICT (page_id, revision_id) DO NOTHING
            """, (
                current_page_id,
                current_revision_id,
                revision_ts,
                revision_size_bytes or 0,
                datetime.now(timezone.utc),
                raw_ref
            ))
            
            if cursor.rowcount > 0:
                revisions_inserted += 1
            
            conn.commit()
            pages_processed += 1
            logger.info(f"Processed {page_title}: page_id={current_page_id}, revision={current_revision_id}")
            
        except Exception as e:
            logger.error(f"Error processing {page_title}: {e}")
            conn.rollback()
            continue
    
    cursor.close()
    conn.close()
    
    return {
        "status": "success",
        "pages_processed": pages_processed,
        "revisions_inserted": revisions_inserted
    }

