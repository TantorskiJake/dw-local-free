"""
Flask web application for viewing data warehouse data.
"""

from flask import Flask, render_template, jsonify, request
import psycopg2
import os
from datetime import datetime
import json

app = Flask(__name__)
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/dw")


def get_db_connection():
    """Get database connection."""
    return psycopg2.connect(DATABASE_URL)


@app.route('/')
def index():
    """Home page - dashboard overview."""
    return render_template('index.html')


@app.route('/api/locations')
def get_locations():
    """Get all locations, grouped by name to show unique cities."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get all locations, but group by location_name to show unique cities
        cursor.execute("""
            SELECT DISTINCT ON (location_name)
                location_id,
                location_name,
                city,
                region,
                country,
                latitude,
                longitude,
                created_at
            FROM core.location
            ORDER BY location_name, created_at DESC
        """)
        
        locations = []
        for row in cursor.fetchall():
            locations.append({
                'id': row[0],
                'name': row[1],
                'city': row[2],
                'region': row[3],
                'country': row[4],
                'latitude': float(row[5]),
                'longitude': float(row[6]),
                'created_at': row[7].isoformat() if row[7] else None
            })
        
        cursor.close()
        conn.close()
        
        return jsonify(locations)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/location/<int:location_id>/weather')
def get_location_weather(location_id):
    """Get weather data for a specific location."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get location info
        cursor.execute("""
            SELECT location_name, city, region, country
            FROM core.location
            WHERE location_id = %s
        """, (location_id,))
        
        location = cursor.fetchone()
        if not location:
            return jsonify({'error': 'Location not found'}), 404
        
        # Get latest weather observations
        cursor.execute("""
            SELECT 
                observed_at,
                temperature_celsius,
                humidity_percent,
                wind_speed_mps
            FROM core.weather
            WHERE location_id = %s
            ORDER BY observed_at DESC
            LIMIT 168
        """, (location_id,))
        
        observations = []
        for row in cursor.fetchall():
            observations.append({
                'observed_at': row[0].isoformat() if row[0] else None,
                'temperature_celsius': float(row[1]) if row[1] else None,
                'humidity_percent': float(row[2]) if row[2] else None,
                'wind_speed_mps': float(row[3]) if row[3] else None
            })
        
        # Get summary stats
        cursor.execute("""
            SELECT 
                COUNT(*) as count,
                AVG(temperature_celsius) as avg_temp,
                MIN(temperature_celsius) as min_temp,
                MAX(temperature_celsius) as max_temp,
                AVG(humidity_percent) as avg_humidity,
                AVG(wind_speed_mps) as avg_wind
            FROM core.weather
            WHERE location_id = %s
        """, (location_id,))
        
        stats = cursor.fetchone()
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'location': {
                'name': location[0],
                'city': location[1],
                'region': location[2],
                'country': location[3]
            },
            'observations': observations,
            'stats': {
                'count': stats[0],
                'avg_temp': float(stats[1]) if stats[1] else None,
                'min_temp': float(stats[2]) if stats[2] else None,
                'max_temp': float(stats[3]) if stats[3] else None,
                'avg_humidity': float(stats[4]) if stats[4] else None,
                'avg_wind': float(stats[5]) if stats[5] else None
            }
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/wikipedia-pages')
def get_wikipedia_pages():
    """Get all Wikipedia pages."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT 
                wp.page_id,
                wp.wikipedia_page_id,
                wp.page_title,
                wp.page_language,
                wp.valid_from,
                wp.is_current
            FROM core.wikipedia_page wp
            WHERE wp.is_current = true
            ORDER BY wp.page_title
        """)
        
        pages = []
        for row in cursor.fetchall():
            pages.append({
                'page_id': row[0],
                'wikipedia_page_id': row[1],
                'title': row[2],
                'language': row[3],
                'valid_from': row[4].isoformat() if row[4] else None,
                'is_current': row[5]
            })
        
        cursor.close()
        conn.close()
        
        return jsonify(pages)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/wikipedia/<int:page_id>')
def get_wikipedia_page_details(page_id):
    """Get Wikipedia page details including summary from raw table."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get page info
        cursor.execute("""
            SELECT 
                wp.page_title,
                wp.page_language,
                wp.wikipedia_page_id
            FROM core.wikipedia_page wp
            WHERE wp.page_id = %s AND wp.is_current = true
        """, (page_id,))
        
        page = cursor.fetchone()
        if not page:
            return jsonify({'error': 'Page not found'}), 404
        
        # Get summary from raw table - try exact match first, then case-insensitive
        # Extract thumbnail source if it's an object, otherwise use the string value
        cursor.execute("""
            SELECT 
                payload->>'title' as title,
                payload->>'extract' as extract,
                payload->'content_urls'->'desktop'->>'page' as url,
                COALESCE(
                    payload->'thumbnail'->>'source',
                    payload->>'thumbnail'
                ) as thumbnail,
                payload->>'description' as description,
                ingested_at
            FROM raw.wikipedia_pages
            WHERE LOWER(page_title) = LOWER(%s)
            ORDER BY ingested_at DESC
            LIMIT 1
        """, (page[0],))
        
        raw_data = cursor.fetchone()
        
        # Get revision count
        cursor.execute("""
            SELECT COUNT(*) as revision_count
            FROM core.revision
            WHERE page_id = %s
        """, (page_id,))
        
        revision_count = cursor.fetchone()[0]
        
        cursor.close()
        conn.close()
        
        result = {
            'page_id': page_id,
            'title': page[0],
            'language': page[1],
            'wikipedia_page_id': page[2],
            'revision_count': revision_count
        }
        
        if raw_data:
            result['summary'] = {
                'title': raw_data[0],
                'extract': raw_data[1],
                'url': raw_data[2],
                'thumbnail': raw_data[3],
                'description': raw_data[4],
                'fetched_at': raw_data[5].isoformat() if raw_data[5] else None
            }
        
        return jsonify(result)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/dashboard/stats')
def get_dashboard_stats():
    """Get dashboard statistics."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Location count
        cursor.execute("SELECT COUNT(*) FROM core.location")
        location_count = cursor.fetchone()[0]
        
        # Weather observation count
        cursor.execute("SELECT COUNT(*) FROM core.weather")
        weather_count = cursor.fetchone()[0]
        
        # Wikipedia page count
        cursor.execute("SELECT COUNT(*) FROM core.wikipedia_page WHERE is_current = true")
        wikipedia_count = cursor.fetchone()[0]
        
        # Latest weather observation
        cursor.execute("""
            SELECT MAX(observed_at) FROM core.weather
        """)
        latest_weather = cursor.fetchone()[0]
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'locations': location_count,
            'weather_observations': weather_count,
            'wikipedia_pages': wikipedia_count,
            'latest_weather': latest_weather.isoformat() if latest_weather else None
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/lookup-city', methods=['POST'])
def lookup_city():
    """Look up a city and fetch weather/Wikipedia data."""
    try:
        data = request.get_json()
        city_name = data.get('city_name')
        country = data.get('country')
        
        if not city_name:
            return jsonify({'error': 'City name is required'}), 400
        
        # Import here to avoid circular imports
        import sys
        from pathlib import Path
        project_root = Path(__file__).parent.parent
        if str(project_root) not in sys.path:
            sys.path.insert(0, str(project_root))
        
        from src.extract import fetch_weather_from_api, store_weather_raw, fetch_wikipedia_from_api, store_wikipedia_raw
        from src.transform import transform_weather_to_fact, transform_wikipedia_to_fact
        import requests
        
        # Geocode city
        GEOCODING_API = "https://nominatim.openstreetmap.org/search"
        query = city_name
        if country:
            query = f"{city_name}, {country}"
        
        params = {
            "q": query,
            "format": "json",
            "limit": 1,
            "addressdetails": 1
        }
        
        headers = {'User-Agent': 'DataWarehouseETL/1.0'}
        response = requests.get(GEOCODING_API, params=params, headers=headers, timeout=10)
        response.raise_for_status()
        results = response.json()
        
        if not results:
            return jsonify({'error': f'City "{city_name}" not found'}), 404
        
        result = results[0]
        address = result.get("address", {})
        
        location = {
            "location_name": city_name,
            "latitude": float(result["lat"]),
            "longitude": float(result["lon"]),
            "city": address.get("city") or address.get("town") or address.get("village") or city_name,
            "region": address.get("state") or address.get("region") or "",
            "country": address.get("country") or ""
        }
        
        # Add location to database
        conn = get_db_connection()
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
        
        # Fetch weather
        weather_data = None
        weather_rows = 0
        try:
            weather_data = fetch_weather_from_api(location)
            store_weather_raw(location, weather_data)
            transform_result = transform_weather_to_fact()
            weather_rows = transform_result.get('rows_inserted', 0)
        except Exception as e:
            pass  # Continue even if weather fails
        
        # Fetch Wikipedia - try multiple variations of the city name
        wikipedia_success = False
        wikipedia_variations = [
            city_name,  # Just city name
        ]
        
        # Add city + region if available
        if location.get("region"):
            wikipedia_variations.append(f"{city_name}, {location['region']}")
        
        # Add city + country if available
        if location.get("country"):
            wikipedia_variations.append(f"{city_name}, {location['country']}")
        
        # Add city + region + country if both available
        if location.get("region") and location.get("country"):
            wikipedia_variations.append(f"{city_name}, {location['region']}, {location['country']}")
        
        for page_title in wikipedia_variations:
            try:
                wikipedia_page = {
                    "page_title": page_title,
                    "page_language": "en"
                }
                summary_data, content_size = fetch_wikipedia_from_api(wikipedia_page)
                store_wikipedia_raw(wikipedia_page, summary_data, content_size)
                transform_wikipedia_to_fact()
                wikipedia_success = True
                break  # Success, stop trying variations
            except Exception as e:
                continue  # Try next variation
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'location': {
                'id': location_id,
                'name': location['location_name'],
                'city': location['city'],
                'region': location['region'],
                'country': location['country'],
                'latitude': location['latitude'],
                'longitude': location['longitude']
            },
            'weather_rows_inserted': weather_rows,
            'wikipedia_success': wikipedia_success
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500


if __name__ == '__main__':
    # Use port 5001 to avoid conflict with macOS AirPlay Receiver on port 5000
    port = int(os.getenv('FLASK_PORT', 5001))
    app.run(debug=True, host='0.0.0.0', port=port)

