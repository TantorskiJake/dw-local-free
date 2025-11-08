# Web Dashboard

A modern browser interface for exploring your data warehouse.

## Features

- **Dashboard Overview**: View statistics about locations, weather observations, and Wikipedia pages
- **Location Browser**: Browse all locations in your database
- **Weather Visualization**: Interactive charts showing temperature and humidity trends
- **Wikipedia Pages**: View all Wikipedia pages with summaries

## Running the Web App

### Option 1: Using the startup script

```bash
./scripts/start-webapp.sh
```

### Option 2: Manual start

```bash
# Activate virtual environment
source .venv/bin/activate

# Start Flask app
cd webapp
python app.py
```

The web interface will be available at **http://localhost:5000**

## API Endpoints

The web app provides REST API endpoints:

- `GET /api/dashboard/stats` - Dashboard statistics
- `GET /api/locations` - List all locations
- `GET /api/location/<id>/weather` - Weather data for a location
- `GET /api/wikipedia-pages` - List all Wikipedia pages
- `GET /api/wikipedia/<id>` - Wikipedia page details

## Requirements

- Flask (installed via `requirements.txt`)
- PostgreSQL database running (via `docker-compose up -d`)
- Database initialized with schemas and data

## Development

To run in development mode with auto-reload:

```bash
export FLASK_ENV=development
cd webapp
python app.py
```

Flask will automatically reload when you make changes to the code.

