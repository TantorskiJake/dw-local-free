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
from pathlib import Path
from typing import Dict, Any


PROJECT_ROOT = Path(__file__).parent.parent
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

