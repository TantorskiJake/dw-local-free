"""
Extract functions for fetching data from external APIs.
"""

import requests
import psycopg2
import json
import os
from datetime import datetime, timedelta
from typing import Dict, Any
import logging

logger = logging.getLogger(__name__)

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/dw")
OPEN_METEO_BASE_URL = "https://api.open-meteo.com/v1/forecast"
WIKIPEDIA_BASE_URL = "https://en.wikipedia.org/api/rest_v1"


def fetch_weather_from_api(location: Dict[str, Any]) -> Dict[str, Any]:
    """
    Fetch weather data from Open-Meteo API.
    
    Args:
        location: Dictionary with location_name, latitude, longitude
        
    Returns:
        Dictionary with API response data
    """
    # Calculate time range: last 24 hours + next 7 days forecast
    end_date = datetime.utcnow()
    start_date = end_date - timedelta(days=1)
    
    params = {
        "latitude": location["latitude"],
        "longitude": location["longitude"],
        "hourly": "temperature_2m,relativehumidity_2m,precipitation,cloudcover,windspeed_10m",
        "timezone": "UTC",
        "start_date": start_date.strftime("%Y-%m-%d"),
        "end_date": (end_date + timedelta(days=7)).strftime("%Y-%m-%d")
    }
    
    logger.info(f"Fetching weather for {location['location_name']} from Open-Meteo")
    response = requests.get(OPEN_METEO_BASE_URL, params=params, timeout=30)
    response.raise_for_status()
    
    return response.json()


def store_weather_raw(location: Dict[str, Any], api_data: Dict[str, Any]) -> int:
    """
    Store weather API response in raw.weather_observations table.
    
    Args:
        location: Location dictionary
        api_data: API response JSON
        
    Returns:
        ID of inserted record
    """
    conn = psycopg2.connect(DATABASE_URL)
    cursor = conn.cursor()
    
    cursor.execute("""
        INSERT INTO raw.weather_observations 
        (location_name, latitude, longitude, payload, ingested_at, source)
        VALUES (%s, %s, %s, %s::jsonb, %s, %s)
        RETURNING id
    """, (
        location["location_name"],
        location["latitude"],
        location["longitude"],
        json.dumps(api_data),
        datetime.utcnow(),
        "open-meteo"
    ))
    
    record_id = cursor.fetchone()[0]
    conn.commit()
    cursor.close()
    conn.close()
    
    return record_id


def fetch_wikipedia_from_api(page: Dict[str, Any]) -> tuple:
    """
    Fetch Wikipedia page data from MediaWiki REST API.
    
    Args:
        page: Dictionary with page_title and page_language
        
    Returns:
        Tuple of (summary_data, content_size_bytes)
    """
    title = page["page_title"].replace(" ", "_")
    language = page.get("page_language", "en")
    
    # Fetch page summary
    summary_url = f"{WIKIPEDIA_BASE_URL}/page/summary/{title}"
    logger.info(f"Fetching Wikipedia summary for {title}")
    
    summary_response = requests.get(summary_url, timeout=30)
    summary_response.raise_for_status()
    summary_data = summary_response.json()
    
    # Fetch HTML content for size calculation
    html_url = f"{WIKIPEDIA_BASE_URL}/page/html/{title}"
    logger.info(f"Fetching Wikipedia HTML for {title}")
    
    html_response = requests.get(html_url, timeout=30)
    html_response.raise_for_status()
    content_size_bytes = len(html_response.content)
    
    return summary_data, content_size_bytes


def store_wikipedia_raw(page: Dict[str, Any], summary_data: Dict[str, Any], content_size: int) -> int:
    """
    Store Wikipedia API response in raw.wikipedia_pages table.
    
    Args:
        page: Page dictionary
        summary_data: Summary API response
        content_size: Size of HTML content in bytes
        
    Returns:
        ID of inserted record
    """
    conn = psycopg2.connect(DATABASE_URL)
    cursor = conn.cursor()
    
    # Extract fields from summary
    page_id = summary_data.get("pageid", 0)
    revision_id = str(summary_data.get("revision", ""))
    revision_timestamp = summary_data.get("timestamp", "")
    namespace_id = summary_data.get("namespace", {}).get("id", 0) if isinstance(summary_data.get("namespace"), dict) else summary_data.get("namespace", 0)
    
    # Parse timestamp
    if revision_timestamp:
        try:
            revision_ts = datetime.fromisoformat(revision_timestamp.replace("Z", "+00:00"))
        except:
            revision_ts = datetime.utcnow()
    else:
        revision_ts = datetime.utcnow()
    
    cursor.execute("""
        INSERT INTO raw.wikipedia_pages 
        (page_id, page_title, namespace, revision_id, revision_timestamp, 
         revision_size_bytes, page_language, payload, ingested_at, source)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s)
        RETURNING id
    """, (
        page_id,
        summary_data.get("title", page["page_title"]),
        namespace_id,
        revision_id,
        revision_ts,
        content_size,
        page.get("page_language", "en"),
        json.dumps(summary_data),
        datetime.utcnow(),
        "mediawiki-rest-api"
    ))
    
    record_id = cursor.fetchone()[0]
    conn.commit()
    cursor.close()
    conn.close()
    
    return record_id

