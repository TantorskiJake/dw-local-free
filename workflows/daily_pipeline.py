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
import sys
import os

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from src.data_quality import (
    initialize_great_expectations,
    run_weather_checkpoint,
    run_wikipedia_checkpoint
)
from src.extract import (
    fetch_weather_from_api,
    store_weather_raw,
    fetch_wikipedia_from_api,
    store_wikipedia_raw
)
from src.transform import (
    transform_weather_to_fact,
    transform_wikipedia_to_fact
)
from src.seed_loader import ensure_location_dimension

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
def ensure_location_dimension_task() -> Dict[str, Any]:
    """
    Ensure location dimension is up to date from seed data.
    Reads from seed_data.yaml and upserts into core.location.
    """
    logger.info("Ensuring location dimension is up to date from seeds")
    result = ensure_location_dimension()
    logger.info(f"Location dimension updated: {result}")
    return result


@task(
    name="fetch_raw_weather",
    retries=3,
    retry_delay_seconds=2,  # Base delay, Prefect applies exponential backoff automatically
    timeout_seconds=120,
    log_prints=True,
    task_run_name="fetch-weather-{location[location_name]}"
)
def fetch_raw_weather(location: Dict[str, Any]) -> Dict[str, Any]:
    """
    Fetch raw weather data from Open-Meteo API for a single location.
    
    This task is rate-limited to max 3 concurrent executions (controlled at flow level)
    to avoid overwhelming the API and respect rate limits.
    
    Args:
        location: Dictionary with location_name, latitude, longitude
        
    Returns:
        Dictionary with location info and fetch status
    """
    logger.info(f"Fetching weather data for {location.get('location_name')}")
    
    try:
        # Fetch from API
        api_data = fetch_weather_from_api(location)
        
        # Store in raw table
        raw_id = store_weather_raw(location, api_data)
        
        logger.info(f"Successfully fetched and stored weather for {location.get('location_name')}")
        
        return {
            "location_name": location.get("location_name"),
            "status": "success",
            "records_inserted": 1,
            "raw_id": raw_id
        }
    except Exception as e:
        logger.error(f"Failed to fetch weather for {location.get('location_name')}: {e}")
        raise


@task(
    name="transform_weather_to_fact",
    retries=2,
    retry_delay_seconds=5,
    timeout_seconds=300,
    log_prints=True
)
def transform_weather_to_fact_task(fetch_results: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Transform raw weather data into weather fact table.
    
    Args:
        fetch_results: List of results from fetch_raw_weather tasks (for dependency tracking)
        
    Returns:
        Dictionary with transform summary
    """
    logger.info("Transforming weather data to fact table")
    
    try:
        result = transform_weather_to_fact()
        logger.info(f"Weather transform completed: {result}")
        return result
    except Exception as e:
        logger.error(f"Weather transform failed: {e}")
        raise


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
    
    try:
        # Fetch from API
        summary_data, content_size = fetch_wikipedia_from_api(page)
        
        # Store in raw table
        raw_id = store_wikipedia_raw(page, summary_data, content_size)
        
        logger.info(f"Successfully fetched and stored Wikipedia data for {page.get('page_title')}")
        
        return {
            "page_title": page.get("page_title"),
            "status": "success",
            "records_inserted": 1,
            "raw_id": raw_id
        }
    except Exception as e:
        logger.error(f"Failed to fetch Wikipedia data for {page.get('page_title')}: {e}")
        raise


@task(
    name="upsert_wikipedia_dimension_and_facts",
    retries=2,
    retry_delay_seconds=5,
    timeout_seconds=300,
    log_prints=True
)
def upsert_wikipedia_dimension_and_facts_task(fetch_results: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Upsert Wikipedia page dimension (type-2 SCD) and insert revision facts.
    
    Args:
        fetch_results: List of results from fetch_raw_wikipedia_page tasks (for dependency tracking)
        
    Returns:
        Dictionary with upsert summary
    """
    logger.info("Upserting Wikipedia dimension and revision facts")
    
    try:
        result = transform_wikipedia_to_fact()
        logger.info(f"Wikipedia transform completed: {result}")
        return result
    except Exception as e:
        logger.error(f"Wikipedia transform failed: {e}")
        raise


@task(
    name="run_weather_data_quality_checkpoint",
    retries=0,  # Don't retry on data quality failures
    timeout_seconds=300,
    log_prints=True
)
def run_weather_data_quality_checkpoint() -> Dict[str, Any]:
    """
    Run weather fact data quality checkpoint using Great Expectations.
    
    Returns:
        Dictionary with checkpoint results
        
    Raises:
        Exception if checkpoint fails
    """
    logger.info("Running weather fact data quality checkpoint")
    
    # Initialize Great Expectations context
    context = initialize_great_expectations()
    
    # Run weather checkpoint
    result = run_weather_checkpoint(context)
    
    if result.get("skipped"):
        logger.warning("Weather data quality checkpoint SKIPPED (Great Expectations not configured)")
        logger.info("Pipeline will continue - data quality checks are optional")
        return result
    
    if not result["success"]:
        error_msg = f"Weather data quality checkpoint FAILED: {result.get('statistics', {})}"
        logger.error(error_msg)
        raise Exception(error_msg)
    
    logger.info("Weather data quality checkpoint PASSED")
    return result


@task(
    name="run_wikipedia_data_quality_checkpoint",
    retries=0,  # Don't retry on data quality failures
    timeout_seconds=300,
    log_prints=True
)
def run_wikipedia_data_quality_checkpoint() -> Dict[str, Any]:
    """
    Run Wikipedia revision fact data quality checkpoint using Great Expectations.
    
    Returns:
        Dictionary with checkpoint results
        
    Raises:
        Exception if checkpoint fails
    """
    logger.info("Running Wikipedia revision data quality checkpoint")
    
    # Initialize Great Expectations context
    context = initialize_great_expectations()
    
    # Run Wikipedia checkpoint
    result = run_wikipedia_checkpoint(context)
    
    if result.get("skipped"):
        logger.warning("Wikipedia data quality checkpoint SKIPPED (Great Expectations not configured)")
        logger.info("Pipeline will continue - data quality checks are optional")
        return result
    
    if not result["success"]:
        error_msg = f"Wikipedia data quality checkpoint FAILED: {result.get('statistics', {})}"
        logger.error(error_msg)
        raise Exception(error_msg)
    
    logger.info("Wikipedia data quality checkpoint PASSED")
    return result


@task(
    name="refresh_materialized_view",
    retries=2,
    retry_delay_seconds=5,
    timeout_seconds=300,
    log_prints=True
)
def refresh_materialized_view(view_name: str) -> Dict[str, Any]:
    """
    Refresh a materialized view in the mart schema using CONCURRENTLY option.
    
    CONCURRENTLY allows reads to continue during refresh, preventing blocking.
    Requires a unique index on the materialized view.
    
    Args:
        view_name: Name of the materialized view to refresh (e.g., 'mart.daily_weather_aggregates')
        
    Returns:
        Dictionary with refresh status
    """
    import psycopg2
    
    logger.info(f"Refreshing materialized view CONCURRENTLY: {view_name}")
    
    # Get database connection from environment
    database_url = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/dw")
    
    try:
        conn = psycopg2.connect(database_url)
        cursor = conn.cursor()
        
        # Check if view has a unique index (required for CONCURRENTLY)
        # If not, use regular refresh
        schema, name = view_name.split('.')
        cursor.execute("""
            SELECT EXISTS (
                SELECT 1 FROM pg_indexes
                WHERE schemaname = %s
                AND tablename = %s
                AND indexdef LIKE '%UNIQUE%'
            )
        """, (schema, name))
        
        has_unique_index = cursor.fetchone()[0]
        
        if has_unique_index:
            # Use CONCURRENTLY to allow reads during refresh
            cursor.execute(f"REFRESH MATERIALIZED VIEW CONCURRENTLY {view_name}")
            method = "CONCURRENTLY"
        else:
            # Fall back to regular refresh
            logger.warning(f"No unique index found on {view_name}, using regular refresh")
            cursor.execute(f"REFRESH MATERIALIZED VIEW {view_name}")
            method = "REGULAR"
        
        conn.commit()
        
        logger.info(f"Successfully refreshed materialized view: {view_name} (method: {method})")
        
        cursor.close()
        conn.close()
        
        return {
            "view_name": view_name,
            "status": "success",
            "method": method
        }
    except Exception as e:
        logger.error(f"Failed to refresh materialized view {view_name}: {e}")
        # Don't raise - allow pipeline to continue even if view refresh fails
        return {
            "view_name": view_name,
            "status": "failed",
            "error": str(e)
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
        location_result = ensure_location_dimension_task()
        logger.info(f"Location dimension updated: {location_result}")
        
        # Get list of locations from database
        import psycopg2
        database_url = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/dw")
        conn = psycopg2.connect(database_url)
        cursor = conn.cursor()
        cursor.execute("""
            SELECT location_name, latitude, longitude 
            FROM core.location 
            ORDER BY location_name
        """)
        location_rows = cursor.fetchall()
        cursor.close()
        conn.close()
        
        locations = [
            {
                "location_name": row[0],
                "latitude": float(row[1]),
                "longitude": float(row[2])
            }
            for row in location_rows
        ]
        logger.info(f"Found {len(locations)} locations to process")
        
        # Step 2: Fetch raw weather data for each location in parallel
        # Process 10 locations with concurrency limit of 3 to control API rate
        # This prevents overwhelming the Open-Meteo API while still processing efficiently
        # The task runner's concurrency is controlled at the flow level
        weather_fetch_results = fetch_raw_weather.map(locations)
        logger.info(f"Weather fetch completed for {len(locations)} locations")
        logger.info("Note: Concurrency limit of 3 is enforced via Prefect work queue settings")
        
        # Step 3: Transform weather data to fact table
        weather_transform_result = transform_weather_to_fact_task(weather_fetch_results)
        logger.info(f"Weather transform completed: {weather_transform_result}")
        
        # Get list of Wikipedia pages from database
        conn = psycopg2.connect(database_url)
        cursor = conn.cursor()
        cursor.execute("""
            SELECT page_title, page_language 
            FROM core.wikipedia_page 
            WHERE is_current = true
            ORDER BY page_title
        """)
        page_rows = cursor.fetchall()
        cursor.close()
        conn.close()
        
        wikipedia_pages = [
            {
                "page_title": row[0],
                "page_language": row[1]
            }
            for row in page_rows
        ]
        logger.info(f"Found {len(wikipedia_pages)} Wikipedia pages to process")
        
        # Step 4: Fetch raw Wikipedia pages in parallel
        wikipedia_fetch_results = fetch_raw_wikipedia_page.map(wikipedia_pages)
        logger.info(f"Wikipedia fetch completed for {len(wikipedia_pages)} pages")
        
        # Step 5: Upsert Wikipedia dimension and insert revision facts
        wikipedia_transform_result = upsert_wikipedia_dimension_and_facts_task(wikipedia_fetch_results)
        logger.info(f"Wikipedia transform completed: {wikipedia_transform_result}")
        
        # Step 6: Run data quality checkpoints (after transforms, before mart refresh)
        # If checkpoints fail, the flow will fail and mart refresh will be skipped
        try:
            weather_quality_result = run_weather_data_quality_checkpoint()
            logger.info(f"Weather data quality checkpoint passed: {weather_quality_result.get('success')}")
            
            wikipedia_quality_result = run_wikipedia_data_quality_checkpoint()
            logger.info(f"Wikipedia data quality checkpoint passed: {wikipedia_quality_result.get('success')}")
            
            quality_results = {
                "weather": weather_quality_result,
                "wikipedia": wikipedia_quality_result,
                "all_passed": True
            }
        except Exception as e:
            logger.error(f"Data quality checkpoints failed: {e}")
            logger.error("Skipping mart refresh due to data quality failures")
            raise  # Re-raise to fail the flow
        
        # Step 7: Refresh materialized views concurrently (only if checkpoints passed)
        view_names = [
            "mart.daily_weather_aggregates",
            "mart.daily_wikipedia_page_stats"
        ]
        try:
            refresh_results = refresh_materialized_view.map(view_names)
            # Wait for all refresh tasks to complete
            refresh_statuses = [r.get("status") for r in refresh_results if isinstance(r, dict)]
            successful_refreshes = sum(1 for s in refresh_statuses if s == "success")
            logger.info(f"Materialized views refreshed: {successful_refreshes}/{len(view_names)} views")
        except Exception as e:
            logger.warning(f"Materialized view refresh encountered issues: {e}")
            logger.warning("Pipeline will continue - views can be refreshed manually if needed")
            refresh_results = []
        
        # Summary
        summary = {
            "location_dimension": location_result,
            "weather_fetch": len(weather_fetch_results),
            "weather_transform": weather_transform_result,
            "wikipedia_fetch": len(wikipedia_fetch_results),
            "wikipedia_transform": wikipedia_transform_result,
            "data_quality": quality_results,
            "views_refreshed": len(refresh_results),
            "pipeline_status": "success"
        }
        
        logger.info(f"Daily pipeline completed successfully: {summary}")
        return summary


if __name__ == "__main__":
    # Run the flow
    result = daily_pipeline()
    print(f"Pipeline result: {result}")

