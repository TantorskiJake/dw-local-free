# How to View Your Data at localhost

## Option 1: Adminer (Web UI) - Easiest! 🌐

### Step 1: Open Adminer
1. Open your web browser
2. Go to: **http://localhost:8080**

### Step 2: Login
Fill in the login form:
- **System**: `PostgreSQL`
- **Server**: `postgres`
- **Username**: `postgres`
- **Password**: `postgres`
- **Database**: `dw`
- Click **Login**

### Step 3: Browse Your Data

**View Tables:**
- Click on **"dw"** database in the left sidebar
- You'll see all your schemas: `raw`, `core`, `mart`
- Click on any schema to see tables

**Query Data:**
1. Click **SQL command** in the top menu
2. Paste a query like:
```sql
SELECT 
    l.location_name,
    COUNT(*) as observations,
    ROUND(AVG(w.temperature_celsius)::numeric, 2) as avg_temp_c
FROM core.weather w
JOIN core.location l ON w.location_id = l.location_id
GROUP BY l.location_name
ORDER BY l.location_name;
```
3. Click **Execute**

**Quick Tables to Check:**
- `raw.weather_observations` - Raw API data
- `core.weather` - Weather fact table (2,160 rows!)
- `core.location` - Location dimension
- `core.wikipedia_page` - Wikipedia pages
- `core.revision` - Revision facts
- `mart.daily_weather_aggregates` - Daily aggregates

## Option 2: Command Line (psql)

```bash
# Connect to database
docker-compose exec postgres psql -U postgres -d dw

# Then run queries:
SELECT COUNT(*) FROM core.weather;
SELECT * FROM core.location;
SELECT * FROM core.weather LIMIT 10;

# Exit with: \q
```

## Option 3: Quick SQL Queries

**See weather summary:**
```bash
docker-compose exec postgres psql -U postgres -d dw -c "
SELECT 
    l.location_name,
    COUNT(*) as observations,
    ROUND(AVG(w.temperature_celsius)::numeric, 2) as avg_temp_c,
    ROUND(AVG(w.humidity_percent)::numeric, 2) as avg_humidity,
    MIN(w.observed_at) as first_obs,
    MAX(w.observed_at) as last_obs
FROM core.weather w
JOIN core.location l ON w.location_id = l.location_id
GROUP BY l.location_name
ORDER BY l.location_name;
"
```

**See recent weather observations:**
```bash
docker-compose exec postgres psql -U postgres -d dw -c "
SELECT 
    l.location_name,
    w.observed_at,
    w.temperature_celsius,
    w.humidity_percent,
    w.wind_speed_mps
FROM core.weather w
JOIN core.location l ON w.location_id = l.location_id
ORDER BY w.observed_at DESC
LIMIT 20;
"
```

**See Wikipedia data:**
```bash
docker-compose exec postgres psql -U postgres -d dw -c "
SELECT 
    wp.page_title,
    wp.page_language,
    COUNT(r.revision_key) as revision_count
FROM core.wikipedia_page wp
LEFT JOIN core.revision r ON wp.page_id = r.page_id
WHERE wp.is_current = true
GROUP BY wp.page_title, wp.page_language;
"
```

## Troubleshooting

**If Adminer doesn't load:**
```bash
# Check if containers are running
docker-compose ps

# If not running, start them
docker-compose up -d

# Check Adminer is accessible
curl http://localhost:8080
```

**If you can't connect:**
- Make sure Docker containers are running: `docker-compose ps`
- Check the port: `docker-compose ps | grep adminer`
- Try restarting: `docker-compose restart adminer`

## Recommended: Use Adminer

Adminer is the easiest way to explore your data:
- ✅ Visual table browser
- ✅ SQL query interface
- ✅ Export data to CSV/JSON
- ✅ View table structures
- ✅ Browse relationships

Just go to **http://localhost:8080** and login!

