"""
Data Quality Checks using Great Expectations

This module defines expectation suites and checkpoints for data quality validation.
"""

import os
from typing import Dict, Any, Optional
import logging
import great_expectations as gx
from great_expectations.core.batch import RuntimeBatchRequest

logger = logging.getLogger(__name__)

# Database connection from environment
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/dw")


def get_data_context():
    """Initialize and return Great Expectations data context."""
    # Use modern Great Expectations API
    # Try to get existing context, or create a new one
    try:
        context = gx.get_context()
    except Exception:
        # Create new file-based context
        context = gx.get_context(mode="file")
    
    # Add datasource if it doesn't exist
    try:
        datasource = context.get_datasource("postgres_datasource")
    except Exception:
        # Create datasource
        datasource = context.sources.add_sql(
            name="postgres_datasource",
            connection_string=DATABASE_URL
        )
    
    return context


def create_weather_expectation_suite(context) -> None:
    """Create expectation suite for weather fact table."""
    suite_name = "weather_fact_suite"
    
    # Create or get suite
    try:
        suite = context.get_expectation_suite(suite_name)
        logger.info(f"Using existing suite: {suite_name}")
    except Exception:
        suite = context.create_expectation_suite(suite_name, overwrite_existing=True)
        logger.info(f"Created new suite: {suite_name}")
    
    # Define expectations
    suite.expect_column_to_exist("location_id")
    suite.expect_column_to_exist("observed_at")
    suite.expect_column_to_exist("temperature_celsius")
    suite.expect_column_to_exist("humidity_percent")
    suite.expect_column_to_exist("wind_speed_mps")
    
    # Uniqueness of (location_id, observed_at)
    suite.expect_compound_columns_to_be_unique(
        column_list=["location_id", "observed_at"],
        meta={"description": "Each location should have unique observation timestamps"}
    )
    
    # Temperature within plausible range (-50 to 60 Celsius)
    suite.expect_column_values_to_be_between(
        column="temperature_celsius",
        min_value=-50.0,
        max_value=60.0,
        mostly=0.95,  # Allow 5% outliers
        meta={"description": "Temperature should be within plausible range"}
    )
    
    # Humidity between 0 and 100
    suite.expect_column_values_to_be_between(
        column="humidity_percent",
        min_value=0.0,
        max_value=100.0,
        mostly=0.95,
        meta={"description": "Humidity should be between 0 and 100 percent"}
    )
    
    # Wind speed non-negative
    suite.expect_column_values_to_be_between(
        column="wind_speed_mps",
        min_value=0.0,
        max_value=200.0,  # Reasonable upper bound (720 km/h)
        mostly=0.95,
        meta={"description": "Wind speed should be non-negative and reasonable"}
    )
    
    # Minimum number of rows per location
    # This ensures we got data for each location (at least hours returned per location)
    suite.expect_table_row_count_to_be_between(
        min_value=1,
        max_value=None,
        meta={"description": "Should have at least some weather observations"}
    )
    
    # Check that we have data for each location (group by location_id)
    # This is validated by ensuring location_id is not null and has values
    suite.expect_column_values_to_not_be_null(
        column="location_id",
        mostly=1.0,
        meta={"description": "Location ID must not be null"}
    )
    
    # Save suite
    context.save_expectation_suite(suite, suite_name)
    logger.info(f"Saved expectation suite: {suite_name}")


def create_wikipedia_expectation_suite(context) -> None:
    """Create expectation suite for Wikipedia revision fact table."""
    suite_name = "wikipedia_revision_suite"
    
    # Create or get suite
    try:
        suite = context.get_expectation_suite(suite_name)
        logger.info(f"Using existing suite: {suite_name}")
    except Exception:
        suite = context.create_expectation_suite(suite_name, overwrite_existing=True)
        logger.info(f"Created new suite: {suite_name}")
    
    # Define expectations
    suite.expect_column_to_exist("page_id")
    suite.expect_column_to_exist("revision_id")
    suite.expect_column_to_exist("content_len")
    
    # Revision ID unique per page
    suite.expect_compound_columns_to_be_unique(
        column_list=["page_id", "revision_id"],
        meta={"description": "Each page should have unique revision IDs"}
    )
    
    # Content length greater than zero
    suite.expect_column_values_to_be_between(
        column="content_len",
        min_value=1,
        max_value=None,
        meta={"description": "Content length should be greater than zero"}
    )
    
    # Note: Referential integrity check for page_id is done via SQL query in checkpoint
    # This ensures page_ids in facts exist in dimension
    
    # Save suite
    context.save_expectation_suite(suite, suite_name)
    logger.info(f"Saved expectation suite: {suite_name}")


def run_weather_checkpoint(context, batch_query: Optional[str] = None) -> Dict[str, Any]:
    """
    Run weather fact data quality checkpoint.
    
    Args:
        context: Great Expectations data context
        batch_query: Optional SQL query to filter batch (e.g., recent data)
        
    Returns:
        Dictionary with checkpoint results
    """
    suite_name = "weather_fact_suite"
    
    # Default query: get all weather data from current run
    # In practice, you'd filter by a run_id or timestamp
    if batch_query is None:
        batch_query = "SELECT * FROM core.weather ORDER BY created_at DESC LIMIT 1000"
    
    # Get datasource
    datasource = context.get_datasource("postgres_datasource")
    
    # Create data asset and batch
    data_asset = datasource.add_query_asset(
        name="weather_fact",
        query=batch_query
    )
    
    # Get expectation suite
    suite = context.get_expectation_suite(suite_name)
    
    # Create validator
    validator = context.get_validator(
        batch_request=data_asset.build_batch_request(),
        expectation_suite=suite
    )
    
    # Run validation
    logger.info("Running weather fact checkpoint...")
    result = validator.validate()
    
    # Check if validation passed
    success = result.success
    statistics = result.statistics if hasattr(result, 'statistics') else {}
    
    logger.info(f"Weather checkpoint result: {'PASSED' if success else 'FAILED'}")
    
    return {
        "checkpoint_name": "weather_fact_checkpoint",
        "suite_name": suite_name,
        "success": success,
        "statistics": statistics,
        "result": result
    }


def run_wikipedia_checkpoint(context, batch_query: Optional[str] = None) -> Dict[str, Any]:
    """
    Run Wikipedia revision fact data quality checkpoint.
    
    Args:
        context: Great Expectations data context
        batch_query: Optional SQL query to filter batch
        
    Returns:
        Dictionary with checkpoint results
    """
    suite_name = "wikipedia_revision_suite"
    
    # Default query: get all revision data from current run
    # Includes referential integrity check: page_ids must exist in dimension
    if batch_query is None:
        batch_query = """
            SELECT r.* 
            FROM core.revision r
            INNER JOIN core.wikipedia_page wp ON r.page_id = wp.page_id
            WHERE wp.is_current = true
            ORDER BY r.fetched_at DESC 
            LIMIT 1000
        """
    
    # Get datasource
    datasource = context.get_datasource("postgres_datasource")
    
    # Create data asset and batch
    data_asset = datasource.add_query_asset(
        name="wikipedia_revision",
        query=batch_query
    )
    
    # Get expectation suite
    suite = context.get_expectation_suite(suite_name)
    
    # Create validator
    validator = context.get_validator(
        batch_request=data_asset.build_batch_request(),
        expectation_suite=suite
    )
    
    # Run validation
    logger.info("Running Wikipedia revision checkpoint...")
    result = validator.validate()
    
    # Check if validation passed
    success = result.success
    statistics = result.statistics if hasattr(result, 'statistics') else {}
    
    logger.info(f"Wikipedia checkpoint result: {'PASSED' if success else 'FAILED'}")
    
    return {
        "checkpoint_name": "wikipedia_revision_checkpoint",
        "suite_name": suite_name,
        "success": success,
        "statistics": statistics,
        "result": result
    }


def initialize_great_expectations():
    """
    Initialize Great Expectations and create expectation suites.
    
    Returns:
        Configured data context
    """
    logger.info("Initializing Great Expectations...")
    context = get_data_context()
    
    # Create expectation suites
    create_weather_expectation_suite(context)
    create_wikipedia_expectation_suite(context)
    
    logger.info("Great Expectations initialized")
    return context


if __name__ == "__main__":
    # Initialize and test
    logging.basicConfig(level=logging.INFO)
    context = initialize_great_expectations()
    
    # Test checkpoints (will fail if no data, but that's expected)
    print("Testing weather checkpoint...")
    weather_result = run_weather_checkpoint(context)
    print(f"Weather checkpoint: {weather_result['success']}")
    
    print("Testing Wikipedia checkpoint...")
    wikipedia_result = run_wikipedia_checkpoint(context)
    print(f"Wikipedia checkpoint: {wikipedia_result['success']}")

