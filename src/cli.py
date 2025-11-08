#!/usr/bin/env python3
"""
CLI tool for managing data warehouse seeds and triggering runs.

Usage:
    python src/cli.py add-location --name "San Francisco" --lat 37.7749 --lon -122.4194 --city "San Francisco" --region "California"
    python src/cli.py add-page --title "San Francisco" --language en
    python src/cli.py run-pipeline
"""

import click
import yaml
import subprocess
import sys
import os
import re
import requests
import psycopg2
from pathlib import Path
from typing import Dict, Any, Optional


PROJECT_ROOT = Path(__file__).parent.parent
# Add project root to Python path for imports
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

SEED_DATA_FILE = PROJECT_ROOT / "src" / "seed_data.yaml"


def load_seed_data() -> Dict[str, Any]:
    """Load seed data from YAML file."""
    with open(SEED_DATA_FILE) as f:
        return yaml.safe_load(f)


def save_seed_data(data: Dict[str, Any]) -> None:
    """Save seed data to YAML file."""
    with open(SEED_DATA_FILE, 'w') as f:
        yaml.dump(data, f, default_flow_style=False, sort_keys=False)


@click.group()
def cli():
    """Data Warehouse CLI - Manage seeds and trigger pipeline runs."""
    pass


@cli.command()
@click.option('--name', required=True, help='Location name (e.g., "San Francisco")')
@click.option('--lat', '--latitude', 'latitude', required=True, type=float, help='Latitude coordinate')
@click.option('--lon', '--longitude', 'longitude', required=True, type=float, help='Longitude coordinate')
@click.option('--city', required=True, help='City name')
@click.option('--region', required=True, help='State/region name')
@click.option('--country', default='US', help='Country code (default: US)')
@click.option('--run', is_flag=True, help='Trigger pipeline run after adding location')
def add_location(name: str, latitude: float, longitude: float, city: str, region: str, country: str, run: bool):
    """Add a new location to seed data and optionally trigger a pipeline run."""
    click.echo(f"Adding location: {name}")
    
    # Load existing seed data
    data = load_seed_data()
    
    # Check if location already exists
    existing = [loc for loc in data.get('locations', []) if loc.get('name') == name]
    if existing:
        click.echo(f"⚠️  Location '{name}' already exists. Skipping.")
        return
    
    # Add new location
    new_location = {
        'name': name,
        'latitude': latitude,
        'longitude': longitude,
        'country': country,
        'region': region,
        'city': city
    }
    
    data.setdefault('locations', []).append(new_location)
    save_seed_data(data)
    
    click.echo(f"✅ Added location: {name} ({city}, {region})")
    click.echo(f"   Coordinates: {latitude}, {longitude}")
    
    # Update SQL seed file
    update_sql_seeds(name, latitude, longitude, city, region, country)
    
    if run:
        click.echo("\n🚀 Triggering pipeline run...")
        trigger_pipeline_run()


@cli.command()
@click.option('--title', required=True, help='Wikipedia page title (e.g., "San Francisco")')
@click.option('--language', default='en', help='Page language code (default: en)')
@click.option('--namespace', default=0, type=int, help='Namespace (default: 0 for main)')
@click.option('--run', is_flag=True, help='Trigger pipeline run after adding page')
def add_page(title: str, language: str, namespace: int, run: bool):
    """Add a new Wikipedia page to seed data and optionally trigger a pipeline run."""
    click.echo(f"Adding Wikipedia page: {title}")
    
    # Load existing seed data
    data = load_seed_data()
    
    # Check if page already exists
    existing = [
        page for page in data.get('wikipedia_pages', [])
        if page.get('title') == title and page.get('language') == language
    ]
    if existing:
        click.echo(f"⚠️  Page '{title}' ({language}) already exists. Skipping.")
        return
    
    # Add new page
    new_page = {
        'title': title,
        'language': language,
        'namespace': namespace
    }
    
    data.setdefault('wikipedia_pages', []).append(new_page)
    save_seed_data(data)
    
    click.echo(f"✅ Added Wikipedia page: {title} ({language})")
    
    # Update SQL seed file
    update_sql_seeds_wiki(title, language, namespace)
    
    if run:
        click.echo("\n🚀 Triggering pipeline run...")
        trigger_pipeline_run()


@cli.command()
@click.option('--deployment', default='weather-schedule', help='Deployment name (default: weather-schedule)')
def run_pipeline(deployment: str):
    """Trigger a pipeline run via Prefect deployment."""
    click.echo(f"Triggering pipeline run: {deployment}")
    trigger_pipeline_run(deployment)


@cli.command()
@click.argument('city_name')
@click.option('--country', help='Country name to narrow search (e.g., "United States")')
def lookup(city_name: str, country: Optional[str]):
    """
    Get weather and Wikipedia data for a city by name.
    
    This command will:
    1. Look up the city coordinates using geocoding
    2. Add the location to the database
    3. Fetch current weather data
    4. Fetch Wikipedia page data
    5. Store everything in the database
    6. Display the results
    
    Example:
        python src/cli.py lookup "New York"
        python src/cli.py lookup "Paris" --country "France"
    """
    from src.extract import fetch_weather_from_api, store_weather_raw, fetch_wikipedia_from_api, store_wikipedia_raw
    from src.transform import transform_weather_to_fact, transform_wikipedia_to_fact
    
    DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/dw")
    GEOCODING_API = "https://nominatim.openstreetmap.org/search"
    
    click.echo(f"🔍 Looking up coordinates for {city_name}...")
    
    # Step 1: Geocode city
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
    
    try:
        response = requests.get(GEOCODING_API, params=params, headers=headers, timeout=10)
        response.raise_for_status()
        results = response.json()
        
        if not results:
            click.echo(f"❌ City '{city_name}' not found")
            return
        
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
        
        click.echo(f"✅ Found: {location['city']}, {location.get('region', '')}, {location.get('country', '')}")
        click.echo(f"   Coordinates: {location['latitude']}, {location['longitude']}")
        
    except Exception as e:
        click.echo(f"❌ Error geocoding city: {e}")
        return
    
    # Step 2: Add location to database
    click.echo(f"\n📝 Adding location to database...")
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
        
        click.echo(f"✅ Location added (ID: {location_id})")
    except Exception as e:
        click.echo(f"⚠️  Location might already exist: {e}")
    
    # Step 3: Fetch weather data
    click.echo(f"\n🌤️  Fetching weather data...")
    try:
        weather_data = fetch_weather_from_api(location)
        raw_id = store_weather_raw(location, weather_data)
        click.echo(f"✅ Weather data fetched and stored (raw_id: {raw_id})")
        
        # Transform weather
        transform_result = transform_weather_to_fact()
        click.echo(f"✅ Weather transformed: {transform_result.get('rows_inserted', 0)} rows inserted")
    except Exception as e:
        click.echo(f"❌ Weather fetch failed: {e}")
        weather_data = None
    
    # Step 4: Fetch Wikipedia data
    click.echo(f"\n📚 Fetching Wikipedia data for '{city_name}'...")
    try:
        wikipedia_page = {
            "page_title": city_name,
            "page_language": "en"
        }
        summary_data, content_size = fetch_wikipedia_from_api(wikipedia_page)
        raw_id = store_wikipedia_raw(wikipedia_page, summary_data, content_size)
        click.echo(f"✅ Wikipedia data fetched and stored (raw_id: {raw_id})")
        
        # Transform Wikipedia
        transform_result = transform_wikipedia_to_fact()
        click.echo(f"✅ Wikipedia transformed: {transform_result.get('revisions_inserted', 0)} revisions inserted")
    except Exception as e:
        click.echo(f"⚠️  Wikipedia fetch failed: {e}")
        summary_data = None
    
    # Step 5: Query and display results
    click.echo(f"\n📊 Retrieving data from database...")
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
        
        # Get Wikipedia summary from raw table
        cursor.execute("""
            SELECT 
                payload->>'title' as title,
                payload->>'extract' as extract,
                payload->'content_urls'->'desktop'->>'page' as url,
                payload->>'pageid' as pageid
            FROM raw.wikipedia_pages
            WHERE page_title = %s
            ORDER BY ingested_at DESC
            LIMIT 1
        """, (city_name,))
        
        wiki_result = cursor.fetchone()
        
        cursor.close()
        conn.close()
        
        # Display results
        click.echo("\n" + "="*60)
        click.echo(f"📍 LOCATION: {location['city']}")
        click.echo("="*60)
        click.echo(f"City: {location['city']}")
        click.echo(f"Region: {location.get('region', 'N/A')}")
        click.echo(f"Country: {location.get('country', 'N/A')}")
        click.echo(f"Coordinates: {location['latitude']}, {location['longitude']}")
        
        if weather_obs:
            click.echo("\n" + "-"*60)
            click.echo("🌤️  WEATHER DATA (Latest observations)")
            click.echo("-"*60)
            click.echo(f"{'Time':<20} {'Temp (°C)':<12} {'Humidity (%)':<15} {'Wind (m/s)':<12}")
            click.echo("-"*60)
            for obs in weather_obs[:10]:
                temp = f"{obs[1]:.1f}" if obs[1] else "N/A"
                humidity = f"{obs[2]:.0f}" if obs[2] else "N/A"
                wind = f"{obs[3]:.1f}" if obs[3] else "N/A"
                click.echo(f"{str(obs[0]):<20} {temp:<12} {humidity:<15} {wind:<12}")
            if len(weather_obs) > 10:
                click.echo(f"... and {len(weather_obs) - 10} more observations")
        
        if wiki_result and wiki_result[0]:
            click.echo("\n" + "-"*60)
            click.echo("📚 WIKIPEDIA SUMMARY")
            click.echo("-"*60)
            click.echo(f"Title: {wiki_result[0]}")
            if wiki_result[2]:
                click.echo(f"URL: {wiki_result[2]}")
            if wiki_result[1]:
                extract = wiki_result[1][:500] + "..." if len(wiki_result[1]) > 500 else wiki_result[1]
                click.echo(f"\n{extract}")
        
        click.echo("\n✅ Data stored in database! Query it in Adminer at http://localhost:8080")
        
    except Exception as e:
        click.echo(f"❌ Error retrieving data: {e}")


def update_sql_seeds(name: str, latitude: float, longitude: float, city: str, region: str, country: str):
    """Update SQL seed file with new location."""
    sql_file = PROJECT_ROOT / "src" / "seed_reference_data.sql"
    
    # Read existing SQL
    with open(sql_file) as f:
        lines = f.readlines()
    
    # Find the INSERT INTO core.location section
    new_lines = []
    in_location_insert = False
    added = False
    
    for i, line in enumerate(lines):
        if 'INSERT INTO core.location' in line:
            in_location_insert = True
            new_lines.append(line)
        elif in_location_insert and 'VALUES' in line:
            new_lines.append(line)
        elif in_location_insert and line.strip().startswith("('") and line.strip().endswith("'),"):
            # Existing location line
            new_lines.append(line)
        elif in_location_insert and 'ON CONFLICT' in line:
            if not added:
                # Add new location before ON CONFLICT
                new_value = f"    ('{name}', {latitude}, {longitude}, '{country}', '{region}', '{city}'),\n"
                new_lines.append(new_value)
                added = True
            in_location_insert = False
            new_lines.append(line)
        else:
            new_lines.append(line)
    
    with open(sql_file, 'w') as f:
        f.writelines(new_lines)
    
    click.echo(f"   Updated {sql_file.name}")


def update_sql_seeds_wiki(title: str, language: str, namespace: int):
    """Update SQL seed file with new Wikipedia page."""
    sql_file = PROJECT_ROOT / "src" / "seed_reference_data.sql"
    
    # Read existing SQL
    with open(sql_file) as f:
        lines = f.readlines()
    
    # Find the last page_id used
    max_id = -4
    for line in lines:
        if 'wikipedia_page_id' in line:
            matches = re.findall(r'\((-?\d+)', line)
            if matches:
                for match in matches:
                    try:
                        max_id = max(max_id, int(match))
                    except ValueError:
                        pass
    
    new_id = max_id - 1
    
    # Find the INSERT INTO core.wikipedia_page section
    new_lines = []
    in_wiki_insert = False
    added = False
    
    for i, line in enumerate(lines):
        if 'INSERT INTO core.wikipedia_page' in line:
            in_wiki_insert = True
            new_lines.append(line)
        elif in_wiki_insert and 'VALUES' in line:
            new_lines.append(line)
        elif in_wiki_insert and line.strip().startswith("(") and line.strip().endswith("),"):
            # Existing page line
            new_lines.append(line)
        elif in_wiki_insert and 'ON CONFLICT' in line:
            if not added:
                # Add new page before ON CONFLICT
                new_value = f"    ({new_id}, '{title}', {namespace}, '{language}'),\n"
                new_lines.append(new_value)
                added = True
            in_wiki_insert = False
            new_lines.append(line)
        else:
            new_lines.append(line)
    
    with open(sql_file, 'w') as f:
        f.writelines(new_lines)
    
    click.echo(f"   Updated {sql_file.name}")


def trigger_pipeline_run(deployment: str = 'weather-schedule'):
    """Trigger a Prefect pipeline run."""
    try:
        # Try Prefect CLI first
        result = subprocess.run(
            ['prefect', 'deployment', 'run', f'daily_pipeline/{deployment}'],
            capture_output=True,
            text=True
        )
        
        if result.returncode == 0:
            click.echo(f"✅ Pipeline run triggered: {deployment}")
            click.echo(f"   Flow run ID: {result.stdout.strip()}")
        else:
            click.echo(f"⚠️  Prefect CLI failed, trying direct execution...")
            click.echo(f"   Error: {result.stderr}")
            
            # Fallback to direct Python execution
            result = subprocess.run(
                [sys.executable, '-m', 'workflows.daily_pipeline'],
                cwd=PROJECT_ROOT
            )
            
            if result.returncode == 0:
                click.echo("✅ Pipeline run completed")
            else:
                click.echo("❌ Pipeline run failed")
                sys.exit(1)
                
    except FileNotFoundError:
        click.echo("⚠️  Prefect CLI not found. Running pipeline directly...")
        result = subprocess.run(
            [sys.executable, '-m', 'workflows.daily_pipeline'],
            cwd=PROJECT_ROOT
        )
        
        if result.returncode == 0:
            click.echo("✅ Pipeline run completed")
        else:
            click.echo("❌ Pipeline run failed")
            sys.exit(1)


if __name__ == '__main__':
    cli()

