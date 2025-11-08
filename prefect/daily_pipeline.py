"""
Daily Pipeline Flow

This Prefect flow orchestrates the daily data warehouse ETL process:
1. Ensure location dimension is up to date from seeds
2. Fetch and store raw weather data for each location (parallel)
3. Transform weather data into fact table
4. Fetch and store raw Wikipedia pages (parallel)
5. Upsert Wikipedia dimension and insert revision facts
6. Run data quality checkpoints
7. Refresh materialized views (concurrent)
"""

from prefect import flow, task, tags
from prefect.task_runners import ConcurrentTaskRunner
from typing import List, Dict, Any
import time
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ============================================================================
# Task Definitions
# ============================================================================

@task(
    name="ensure_location_dimension",
    retries=2,
    retry_delay_seconds=5,
    timeout_seconds=60,
    log_prints=True
)
def ensure_location_dimension() -> Dict[str, Any]:
    """
    Ensure location dimension is up to date from seed data.
    Reads from seed_data.yaml and upserts into core.location.
    """
    logger.info("Ensuring location dimension is up to date from seeds")
    # TODO: Implement seed data loading logic
    # - Read src/seed_data.yaml
    # - Upsert locations into core.location
    # - Return summary of locations processed
    time.sleep(1)  # Placeholder
    return {"status": "success", "locations_processed": 2}


@task(
    name="fetch_raw_weather",
    retries=3,
    retry_delay_seconds=2,  # Base delay, Prefect applies exponential backoff automatically
    timeout_seconds=120,
    log_prints=True
)
def fetch_raw_weather(location: Dict[str, Any]) -> Dict[str, Any]:
    """
    Fetch raw weather data from Open-Meteo API for a single location.
    
    Args:
        location: Dictionary with location_name, latitude, longitude
        
    Returns:
        Dictionary with location info and fetch status
    """
    logger.info(f"Fetching weather data for {location.get('location_name')}")
    # TODO: Implement Open-Meteo API call
    # - Calculate time range (last 24 hours + forecast)
    # - Call Open-Meteo API
    # - Store full JSON payload in raw.weather_observations
    # - Return status
    time.sleep(2)  # Placeholder for API call
    return {
        "location_name": location.get("location_name"),
        "status": "success",
        "records_inserted": 1
    }


@task(
    name="transform_weather_to_fact",
    retries=2,
    retry_delay_seconds=5,
    timeout_seconds=300,
    log_prints=True
)
def transform_weather_to_fact(fetch_results: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Transform raw weather data into weather fact table.
    
    Args:
        fetch_results: List of results from fetch_raw_weather tasks
        
    Returns:
        Dictionary with transform summary
    """
    logger.info("Transforming weather data to fact table")
    # TODO: Implement weather transform logic
    # - Read latest raw payload per location
    # - Explode hourly arrays into rows
    # - Convert units (km/h to m/s)
    # - Upsert into core.weather
    time.sleep(3)  # Placeholder
    return {
        "status": "success",
        "records_processed": sum(r.get("records_inserted", 0) for r in fetch_results)
    }


@task(
    name="fetch_raw_wikipedia_page",
    retries=3,
    retry_delay_seconds=2,  # Base delay, Prefect applies exponential backoff automatically
    timeout_seconds=120,
    log_prints=True
)
def fetch_raw_wikipedia_page(page: Dict[str, Any]) -> Dict[str, Any]:
    """
    Fetch raw Wikipedia page data from MediaWiki REST API.
    
    Args:
        page: Dictionary with page_title and page_language
        
    Returns:
        Dictionary with page info and fetch status
    """
    logger.info(f"Fetching Wikipedia data for {page.get('page_title')}")
    # TODO: Implement MediaWiki REST API calls
    # - Call /api/rest_v1/page/summary/{title}
    # - Call /api/rest_v1/page/html/{title} for content size
    # - Store in raw.wikipedia_pages
    # - Return status
    time.sleep(2)  # Placeholder for API call
    return {
        "page_title": page.get("page_title"),
        "status": "success",
        "records_inserted": 1
    }


@task(
    name="upsert_wikipedia_dimension_and_facts",
    retries=2,
    retry_delay_seconds=5,
    timeout_seconds=300,
    log_prints=True
)
def upsert_wikipedia_dimension_and_facts(fetch_results: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Upsert Wikipedia page dimension (type-2 SCD) and insert revision facts.
    
    Args:
        fetch_results: List of results from fetch_raw_wikipedia_page tasks
        
    Returns:
        Dictionary with upsert summary
    """
    logger.info("Upserting Wikipedia dimension and revision facts")
    # TODO: Implement Wikipedia transform logic
    # - Read latest raw payload per page
    # - Implement type-2 SCD for page dimension
    # - Insert revision facts
    time.sleep(3)  # Placeholder
    return {
        "status": "success",
        "pages_processed": len(fetch_results),
        "revisions_inserted": len(fetch_results)
    }


@task(
    name="run_data_quality_checkpoints",
    retries=1,
    retry_delay_seconds=5,
    timeout_seconds=180,
    log_prints=True
)
def run_data_quality_checkpoints() -> Dict[str, Any]:
    """
    Run data quality checkpoints to validate data integrity.
    
    Returns:
        Dictionary with quality check results
    """
    logger.info("Running data quality checkpoints")
    # TODO: Implement data quality checks
    # - Check for missing required fields
    # - Validate data ranges
    # - Check referential integrity
    # - Return quality metrics
    time.sleep(2)  # Placeholder
    return {
        "status": "success",
        "checks_passed": 5,
        "checks_failed": 0
    }


@task(
    name="refresh_materialized_view",
    retries=2,
    retry_delay_seconds=5,
    timeout_seconds=300,
    log_prints=True
)
def refresh_materialized_view(view_name: str) -> Dict[str, Any]:
    """
    Refresh a materialized view in the mart schema.
    
    Args:
        view_name: Name of the materialized view to refresh
        
    Returns:
        Dictionary with refresh status
    """
    logger.info(f"Refreshing materialized view: {view_name}")
    # TODO: Implement materialized view refresh
    # - Execute REFRESH MATERIALIZED VIEW CONCURRENTLY
    # - Return status
    time.sleep(2)  # Placeholder
    return {
        "view_name": view_name,
        "status": "success"
    }


# ============================================================================
# Flow Definition
# ============================================================================

@flow(
    name="daily_pipeline",
    description="Daily ETL pipeline for data warehouse",
    task_runner=ConcurrentTaskRunner(),
    log_prints=True
)
def daily_pipeline():
    """
    Main daily pipeline flow that orchestrates all ETL tasks.
    
    Execution order:
    1. Ensure location dimension is up to date
    2. Fetch raw weather data (parallel)
    3. Transform weather to fact
    4. Fetch raw Wikipedia pages (parallel)
    5. Upsert Wikipedia dimension and facts
    6. Run data quality checkpoints
    7. Refresh materialized views (concurrent)
    """
    with tags("env=local", "pipeline=daily"):
        logger.info("Starting daily pipeline")
        
        # Step 1: Ensure location dimension is up to date
        location_result = ensure_location_dimension()
        logger.info(f"Location dimension updated: {location_result}")
        
        # Get list of locations for parallel processing
        # TODO: Query core.location to get actual locations
        locations = [
            {"location_name": "Boston", "latitude": 42.3601, "longitude": -71.0589},
            {"location_name": "St Louis", "latitude": 38.6270, "longitude": -90.1994}
        ]
        
        # Step 2: Fetch raw weather data for each location in parallel
        weather_fetch_results = fetch_raw_weather.map(locations)
        logger.info(f"Weather fetch completed for {len(locations)} locations")
        
        # Step 3: Transform weather data to fact table
        weather_transform_result = transform_weather_to_fact(weather_fetch_results)
        logger.info(f"Weather transform completed: {weather_transform_result}")
        
        # Get list of Wikipedia pages for parallel processing
        # TODO: Query core.wikipedia_page to get actual pages
        wikipedia_pages = [
            {"page_title": "Boston", "page_language": "en"},
            {"page_title": "St. Louis", "page_language": "en"},
            {"page_title": "New England", "page_language": "en"},
            {"page_title": "Cardinals", "page_language": "en"}
        ]
        
        # Step 4: Fetch raw Wikipedia pages in parallel
        wikipedia_fetch_results = fetch_raw_wikipedia_page.map(wikipedia_pages)
        logger.info(f"Wikipedia fetch completed for {len(wikipedia_pages)} pages")
        
        # Step 5: Upsert Wikipedia dimension and insert revision facts
        wikipedia_transform_result = upsert_wikipedia_dimension_and_facts(wikipedia_fetch_results)
        logger.info(f"Wikipedia transform completed: {wikipedia_transform_result}")
        
        # Step 6: Run data quality checkpoints
        quality_result = run_data_quality_checkpoints()
        logger.info(f"Data quality checkpoints completed: {quality_result}")
        
        # Step 7: Refresh materialized views concurrently
        view_names = [
            "mart.daily_weather_aggregates",
            "mart.daily_wikipedia_page_stats"
        ]
        refresh_results = refresh_materialized_view.map(view_names)
        logger.info(f"Materialized views refreshed: {len(view_names)} views")
        
        # Summary
        summary = {
            "location_dimension": location_result,
            "weather_fetch": len(weather_fetch_results),
            "weather_transform": weather_transform_result,
            "wikipedia_fetch": len(wikipedia_fetch_results),
            "wikipedia_transform": wikipedia_transform_result,
            "data_quality": quality_result,
            "views_refreshed": len(refresh_results),
            "pipeline_status": "success"
        }
        
        logger.info(f"Daily pipeline completed successfully: {summary}")
        return summary


if __name__ == "__main__":
    # Run the flow
    result = daily_pipeline()
    print(f"Pipeline result: {result}")

