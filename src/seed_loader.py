"""
Load seed data from YAML into database.
"""

import yaml
import psycopg2
import os
from pathlib import Path
from typing import Dict, Any
import logging

logger = logging.getLogger(__name__)

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/dw")
PROJECT_ROOT = Path(__file__).parent.parent
SEED_DATA_FILE = PROJECT_ROOT / "src" / "seed_data.yaml"


def load_seed_data() -> Dict[str, Any]:
    """Load seed data from YAML file."""
    with open(SEED_DATA_FILE) as f:
        return yaml.safe_load(f)


def ensure_location_dimension() -> Dict[str, Any]:
    """
    Ensure location dimension is up to date from seed data.
    
    Reads from seed_data.yaml and upserts into core.location.
    
    Returns:
        Dictionary with summary of locations processed
    """
    conn = psycopg2.connect(DATABASE_URL)
    cursor = conn.cursor()
    
    # Load seed data
    seed_data = load_seed_data()
    locations = seed_data.get("locations", [])
    
    locations_processed = 0
    locations_inserted = 0
    
    for loc in locations:
        try:
            cursor.execute("""
                INSERT INTO core.location
                (location_name, latitude, longitude, country, region, city)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (location_name, latitude, longitude)
                DO UPDATE SET
                    country = EXCLUDED.country,
                    region = EXCLUDED.region,
                    city = EXCLUDED.city,
                    updated_at = CURRENT_TIMESTAMP
            """, (
                loc["name"],
                loc["latitude"],
                loc["longitude"],
                loc.get("country", "US"),
                loc.get("region", ""),
                loc.get("city", loc["name"])
            ))
            
            if cursor.rowcount > 0:
                locations_inserted += 1
            
            locations_processed += 1
            
        except Exception as e:
            logger.error(f"Error processing location {loc.get('name')}: {e}")
            continue
    
    # Also handle Wikipedia pages
    wikipedia_pages = seed_data.get("wikipedia_pages", [])
    pages_processed = 0
    pages_inserted = 0
    
    # Find next available negative ID
    cursor.execute("""
        SELECT MIN(wikipedia_page_id) FROM core.wikipedia_page
        WHERE wikipedia_page_id < 0
    """)
    result = cursor.fetchone()
    min_id = result[0] if result[0] else -1
    next_id = min_id - 1
    
    for page in wikipedia_pages:
        try:
            # Check if page already exists
            cursor.execute("""
                SELECT page_id FROM core.wikipedia_page
                WHERE page_title = %s AND page_language = %s AND is_current = true
            """, (page["title"], page.get("language", "en")))
            
            existing = cursor.fetchone()
            
            if not existing:
                # Insert new page
                cursor.execute("""
                    INSERT INTO core.wikipedia_page
                    (wikipedia_page_id, page_title, namespace, page_language,
                     valid_from, valid_to, is_current)
                    VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP, NULL, true)
                """, (
                    next_id,
                    page["title"],
                    page.get("namespace", 0),
                    page.get("language", "en")
                ))
                
                if cursor.rowcount > 0:
                    pages_inserted += 1
                    next_id -= 1
            
            pages_processed += 1
            
        except Exception as e:
            logger.error(f"Error processing page {page.get('title')}: {e}")
            continue
    
    conn.commit()
    cursor.close()
    conn.close()
    
    return {
        "status": "success",
        "locations_processed": locations_processed,
        "locations_inserted": locations_inserted,
        "pages_processed": pages_processed,
        "pages_inserted": pages_inserted
    }

