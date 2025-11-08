"""
City Lookup Tool - Get weather and Wikipedia data for any city
"""

import click
import requests
import json
from typing import Dict, Any, Optional
from src.extract import fetch_weather_from_api, store_weather_raw, fetch_wikipedia_from_api, store_wikipedia_raw
from src.transform import transform_weather_to_fact, transform_wikipedia_to_fact
from src.seed_loader import ensure_location_dimension
import psycopg2
import os

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/dw")

# Free geocoding API (Nominatim - OpenStreetMap)
GEOCODING_API = "https://nominatim.openstreetmap.org/search"


def geocode_city(city_name: str, country: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """
    Convert city name to latitude/longitude using OpenStreetMap Nominatim API.
    
    Args:
        city_name: Name of the city
        country: Optional country name to narrow search
        
    Returns:
        Dictionary with location_name, latitude, longitude, city, region, country
        or None if not found
    """
    query = city_name
    if country:
        query = f"{city_name}, {country}"
    
    params = {
        "q": query,
        "format": "json",
        "limit": 1,
        "addressdetails": 1
    }
    
    headers = {
        'User-Agent': 'DataWarehouseETL/1.0'
    }
    
    try:
        response = requests.get(GEOCODING_API, params=params, headers=headers, timeout=10)
        response.raise_for_status()
        results = response.json()
        
        if not results:
            return None
        
        result = results[0]
        address = result.get("address", {})
        
        return {
            "location_name": city_name,
            "latitude": float(result["lat"]),
            "longitude": float(result["lon"]),
            "city": address.get("city") or address.get("town") or address.get("village") or city_name,
            "region": address.get("state") or address.get("region") or "",
            "country": address.get("country") or ""
        }
    except Exception as e:
        print(f"Error geocoding city: {e}")
        return None


def get_city_data(city_name: str, country: Optional[str] = None) -> Dict[str, Any]:
    """
    Get weather and Wikipedia data for a city.
    
    Args:
        city_name: Name of the city
        country: Optional country name
        
    Returns:
        Dictionary with weather and Wikipedia data
    """
    print(f"🔍 Looking up coordinates for {city_name}...")
    
    # Step 1: Geocode city
    location = geocode_city(city_name, country)
    if not location:
        return {"error": f"City '{city_name}' not found"}
    
    print(f"✅ Found: {location['city']}, {location.get('region', '')}, {location.get('country', '')}")
    print(f"   Coordinates: {location['latitude']}, {location['longitude']}")
    
    # Step 2: Add location to database
    print(f"\n📝 Adding location to database...")
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cursor = conn.cursor()
        
        cursor.execute("""
            INSERT INTO core.location 
            (location_name, latitude, longitude, city, region, country)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (location_name, latitude, longitude) DO UPDATE SET
                city = EXCLUDED.city,
                region = EXCLUDED.region,
                country = EXCLUDED.country
            RETURNING location_id
        """, (
            location["location_name"],
            location["latitude"],
            location["longitude"],
            location["city"],
            location["region"],
            location["country"]
        ))
        
        location_id = cursor.fetchone()[0]
        conn.commit()
        cursor.close()
        conn.close()
        
        print(f"✅ Location added (ID: {location_id})")
    except Exception as e:
        print(f"⚠️  Location might already exist: {e}")
    
    # Step 3: Fetch weather data
    print(f"\n🌤️  Fetching weather data...")
    try:
        weather_data = fetch_weather_from_api(location)
        raw_id = store_weather_raw(location, weather_data)
        print(f"✅ Weather data fetched and stored (raw_id: {raw_id})")
        
        # Transform weather
        transform_result = transform_weather_to_fact()
        print(f"✅ Weather transformed: {transform_result.get('rows_inserted', 0)} rows inserted")
    except Exception as e:
        print(f"❌ Weather fetch failed: {e}")
        weather_data = None
    
    # Step 4: Fetch Wikipedia data
    print(f"\n📚 Fetching Wikipedia data for '{city_name}'...")
    try:
        wikipedia_page = {
            "page_title": city_name,
            "page_language": "en"
        }
        summary_data, content_size = fetch_wikipedia_from_api(wikipedia_page)
        raw_id = store_wikipedia_raw(wikipedia_page, summary_data, content_size)
        print(f"✅ Wikipedia data fetched and stored (raw_id: {raw_id})")
        
        # Transform Wikipedia
        transform_result = transform_wikipedia_to_fact()
        print(f"✅ Wikipedia transformed: {transform_result.get('revisions_inserted', 0)} revisions inserted")
    except Exception as e:
        print(f"⚠️  Wikipedia fetch failed: {e}")
        summary_data = None
    
    # Step 5: Query and return results
    print(f"\n📊 Retrieving data from database...")
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cursor = conn.cursor()
        
        # Get latest weather observations
        cursor.execute("""
            SELECT 
                w.observed_at,
                w.temperature_celsius,
                w.humidity_percent,
                w.wind_speed_mps
            FROM core.weather w
            WHERE w.location_id = %s
            ORDER BY w.observed_at DESC
            LIMIT 24
        """, (location_id,))
        
        weather_obs = cursor.fetchall()
        
        # Get Wikipedia summary
        wikipedia_summary = None
        if summary_data:
            wikipedia_summary = {
                "title": summary_data.get("title", city_name),
                "extract": summary_data.get("extract", "")[:500] + "...",  # First 500 chars
                "url": summary_data.get("content_urls", {}).get("desktop", {}).get("page", ""),
                "content_size_bytes": content_size
            }
        
        cursor.close()
        conn.close()
        
        return {
            "location": location,
            "weather_observations": [
                {
                    "observed_at": str(obs[0]),
                    "temperature_celsius": float(obs[1]) if obs[1] else None,
                    "humidity_percent": float(obs[2]) if obs[2] else None,
                    "wind_speed_mps": float(obs[3]) if obs[3] else None
                }
                for obs in weather_obs
            ],
            "wikipedia": wikipedia_summary
        }
    except Exception as e:
        print(f"❌ Error retrieving data: {e}")
        return {"error": str(e)}


@click.command()
@click.argument('city_name')
@click.option('--country', help='Country name to narrow search (e.g., "United States")')
@click.option('--format', type=click.Choice(['table', 'json']), default='table', help='Output format')
def lookup_city(city_name: str, country: Optional[str], format: str):
    """
    Get weather and Wikipedia data for a city.
    
    Example:
        python src/city_lookup.py "New York"
        python src/city_lookup.py "Paris" --country "France"
        python src/city_lookup.py "Tokyo" --format json
    """
    result = get_city_data(city_name, country)
    
    if "error" in result:
        print(f"❌ Error: {result['error']}")
        return
    
    if format == 'json':
        print(json.dumps(result, indent=2))
    else:
        # Table format
        print("\n" + "="*60)
        print(f"📍 LOCATION: {result['location']['city']}")
        print("="*60)
        print(f"City: {result['location']['city']}")
        print(f"Region: {result['location'].get('region', 'N/A')}")
        print(f"Country: {result['location'].get('country', 'N/A')}")
        print(f"Coordinates: {result['location']['latitude']}, {result['location']['longitude']}")
        
        if result.get('weather_observations'):
            print("\n" + "-"*60)
            print("🌤️  WEATHER DATA (Latest 24 hours)")
            print("-"*60)
            print(f"{'Time':<20} {'Temp (°C)':<12} {'Humidity (%)':<15} {'Wind (m/s)':<12}")
            print("-"*60)
            for obs in result['weather_observations'][:10]:  # Show first 10
                print(f"{obs['observed_at']:<20} {obs['temperature_celsius'] or 'N/A':<12} {obs['humidity_percent'] or 'N/A':<15} {obs['wind_speed_mps'] or 'N/A':<12}")
            if len(result['weather_observations']) > 10:
                print(f"... and {len(result['weather_observations']) - 10} more observations")
        
        if result.get('wikipedia'):
            print("\n" + "-"*60)
            print("📚 WIKIPEDIA SUMMARY")
            print("-"*60)
            print(f"Title: {result['wikipedia']['title']}")
            print(f"URL: {result['wikipedia']['url']}")
            print(f"\n{result['wikipedia']['extract']}")
            print(f"\nContent size: {result['wikipedia']['content_size_bytes']:,} bytes")


if __name__ == "__main__":
    lookup_city()

