"""
Prefect configuration for local development.
"""

import os
from pathlib import Path

# Database connection from environment
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/dw")

# API endpoints
OPEN_METEO_BASE_URL = "https://api.open-meteo.com/v1/forecast"
WIKIPEDIA_BASE_URL = "https://en.wikipedia.org/api/rest_v1"

# Project paths
PROJECT_ROOT = Path(__file__).parent.parent
SRC_DIR = PROJECT_ROOT / "src"
SEED_DATA_FILE = SRC_DIR / "seed_data.yaml"

# Prefect settings
PREFECT_API_URL = os.getenv("PREFECT_API_URL", "http://127.0.0.1:4200/api")
PREFECT_LOGGING_LEVEL = os.getenv("PREFECT_LOGGING_LEVEL", "INFO")

