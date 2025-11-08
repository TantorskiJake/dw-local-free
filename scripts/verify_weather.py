#!/usr/bin/env python3
"""
Script to verify weather data accuracy by comparing with Open-Meteo API directly.
"""

import requests
import psycopg2
import os
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/dw")
OPEN_METEO_BASE_URL = "https://api.open-meteo.com/v1/forecast"


def verify_weather_data(location_name: str):
    """Verify weather data for a location by comparing database with API."""
    print(f"\n🔍 Verifying weather data for: {location_name}\n")
    
    # Connect to database
    conn = psycopg2.connect(DATABASE_URL)
    cursor = conn.cursor()
    
    # Get location from database
    cursor.execute("""
        SELECT location_id, location_name, latitude, longitude, city, region, country
        FROM core.location
        WHERE location_name = %s
        ORDER BY location_id DESC
        LIMIT 1
    """, (location_name,))
    
    location = cursor.fetchone()
    if not location:
        print(f"❌ Location '{location_name}' not found in database")
        return
    
    location_id, loc_name, lat, lon, city, region, country = location
    print(f"📍 Location: {loc_name}")
    print(f"   Coordinates: {float(lat)}, {float(lon)}")
    print(f"   City: {city}, {region}, {country}")
    print(f"   Current UTC time: {datetime.now(timezone.utc)}\n")
    
    # Get latest weather data from database
    cursor.execute("""
        SELECT observed_at, temperature_celsius, humidity_percent, wind_speed_mps
        FROM core.weather
        WHERE location_id = %s
        ORDER BY observed_at DESC
        LIMIT 5
    """, (location_id,))
    
    db_records = cursor.fetchall()
    
    if not db_records:
        print("❌ No weather data found in database")
        return
    
    print("📊 Latest 5 records from database:")
    for obs_at, temp, humidity, wind in db_records:
        print(f"   {obs_at} | Temp: {temp}°C | Humidity: {humidity}% | Wind: {wind} m/s")
    
    # Fetch current data from API
    print("\n🌐 Fetching current data from Open-Meteo API...")
    
    # Use the latest database record's date as reference
    if db_records:
        latest_db_time = db_records[0][0]
        # Get date range around the latest database record
        start_date = latest_db_time.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1)
        end_date = latest_db_time + timedelta(days=1)
    else:
        end_date = datetime.now(timezone.utc)
        start_date = end_date.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1)
    
    params = {
        "latitude": float(lat),
        "longitude": float(lon),
        "hourly": "temperature_2m,relativehumidity_2m,windspeed_10m",
        "timezone": "UTC",
        "start_date": start_date.strftime("%Y-%m-%d"),
        "end_date": end_date.strftime("%Y-%m-%d")
    }
    
    print(f"   Requesting data from {start_date.date()} to {end_date.date()}")
    
    try:
        response = requests.get(OPEN_METEO_BASE_URL, params=params, timeout=30)
        response.raise_for_status()
        api_data = response.json()
        
        hourly = api_data.get("hourly", {})
        times = hourly.get("time", [])
        temps = hourly.get("temperature_2m", [])
        humidities = hourly.get("relativehumidity_2m", [])
        wind_speeds_kmh = hourly.get("windspeed_10m", [])
        
        print(f"\n📡 API Response:")
        print(f"   Elevation: {api_data.get('elevation', 'N/A')}m")
        print(f"   Timezone: {api_data.get('timezone', 'N/A')}")
        print(f"   Data points: {len(times)}")
        
        # Compare latest database record with API
        if db_records and times:
            latest_db = db_records[0]
            db_time, db_temp, db_humidity, db_wind = latest_db
            
            # Find closest API record
            min_diff = None
            closest_idx = 0
            for i, api_time_str in enumerate(times):
                api_time = datetime.fromisoformat(api_time_str.replace("Z", "+00:00"))
                diff = abs((api_time - db_time).total_seconds())
                if min_diff is None or diff < min_diff:
                    min_diff = diff
                    closest_idx = i
            
            api_time_str = times[closest_idx]
            api_time = datetime.fromisoformat(api_time_str.replace("Z", "+00:00"))
            api_temp = temps[closest_idx] if closest_idx < len(temps) else None
            api_humidity = humidities[closest_idx] if closest_idx < len(humidities) else None
            api_wind_kmh = wind_speeds_kmh[closest_idx] if closest_idx < len(wind_speeds_kmh) else None
            api_wind_mps = (api_wind_kmh / 3.6) if api_wind_kmh is not None else None
            
            print(f"\n🔬 Comparison (Database vs API):")
            print(f"   Time difference: {abs(min_diff)} seconds")
            print(f"   Database time: {db_time}")
            print(f"   API time: {api_time}")
            
            if db_temp is not None and api_temp is not None:
                # Convert Decimal to float for comparison
                db_temp_float = float(db_temp) if hasattr(db_temp, '__float__') else db_temp
                temp_diff = abs(db_temp_float - api_temp)
                print(f"\n   Temperature:")
                print(f"      Database: {db_temp}°C")
                print(f"      API: {api_temp}°C")
                print(f"      Difference: {temp_diff:.2f}°C {'✅' if temp_diff < 1.0 else '⚠️'}")
            
            if db_humidity is not None and api_humidity is not None:
                # Convert Decimal to float for comparison
                db_humidity_float = float(db_humidity) if hasattr(db_humidity, '__float__') else db_humidity
                humidity_diff = abs(db_humidity_float - api_humidity)
                print(f"\n   Humidity:")
                print(f"      Database: {db_humidity}%")
                print(f"      API: {api_humidity}%")
                print(f"      Difference: {humidity_diff:.1f}% {'✅' if humidity_diff < 5.0 else '⚠️'}")
            
            if db_wind is not None and api_wind_mps is not None:
                # Convert Decimal to float for comparison
                db_wind_float = float(db_wind) if hasattr(db_wind, '__float__') else db_wind
                wind_diff = abs(db_wind_float - api_wind_mps)
                print(f"\n   Wind Speed:")
                print(f"      Database: {db_wind} m/s")
                print(f"      API: {api_wind_mps:.2f} m/s")
                print(f"      Difference: {wind_diff:.2f} m/s {'✅' if wind_diff < 0.5 else '⚠️'}")
        
        print("\n✅ Verification complete!")
        
    except Exception as e:
        print(f"\n❌ Error fetching from API: {e}")
    
    cursor.close()
    conn.close()


if __name__ == "__main__":
    import sys
    from datetime import timedelta
    
    if len(sys.argv) < 2:
        print("Usage: python scripts/verify_weather.py <city_name>")
        print("Example: python scripts/verify_weather.py Boston")
        sys.exit(1)
    
    city_name = sys.argv[1]
    verify_weather_data(city_name)

