"""
Script to create Prefect deployments with schedules.

Run this script to create the deployments:
    python prefect/create_deployments.py

Or use Prefect CLI:
    prefect deployment build prefect/daily_pipeline.py:daily_pipeline -n weather-schedule --schedule "30 * * * *" --timezone UTC
    prefect deployment build prefect/daily_pipeline.py:daily_pipeline -n wikipedia-schedule --schedule "0 1,13 * * *" --timezone UTC
"""

from prefect import serve
from prefect.client.schemas.schedules import CronSchedule
from prefect.daily_pipeline import daily_pipeline

def create_weather_deployment():
    """Create deployment for hourly weather schedule."""
    deployment = daily_pipeline.to_deployment(
        name="weather-schedule",
        description="Hourly weather data pipeline - runs at :30 past each hour UTC",
        schedule=CronSchedule(
            cron="30 * * * *",  # Every hour at 30 minutes past
            timezone="UTC"
        ),
        tags=["env=local", "pipeline=daily", "schedule=weather", "frequency=hourly"],
        work_queue_name="default",
        parameters={}
    )
    return deployment

def create_wikipedia_deployment():
    """Create deployment for twice-daily Wikipedia schedule."""
    deployment = daily_pipeline.to_deployment(
        name="wikipedia-schedule",
        description="Twice daily Wikipedia data pipeline - runs at 01:00 and 13:00 UTC",
        schedule=CronSchedule(
            cron="0 1,13 * * *",  # At 01:00 and 13:00 UTC daily
            timezone="UTC"
        ),
        tags=["env=local", "pipeline=daily", "schedule=wikipedia", "frequency=twice-daily"],
        work_queue_name="default",
        parameters={}
    )
    return deployment

if __name__ == "__main__":
    print("Creating Prefect deployments...")
    
    # Create deployments
    weather_deployment = create_weather_deployment()
    wikipedia_deployment = create_wikipedia_deployment()
    
    # Apply deployments
    weather_deployment.apply()
    wikipedia_deployment.apply()
    
    print("✓ Weather deployment created: weather-schedule (runs hourly at :30 UTC)")
    print("✓ Wikipedia deployment created: wikipedia-schedule (runs at 01:00 and 13:00 UTC)")
    print("\nDeployments are now available for:")
    print("  - Scheduled runs (automatic)")
    print("  - Manual 'run now' via Prefect UI or CLI")
    print("\nTo run manually:")
    print("  prefect deployment run daily_pipeline/weather-schedule")
    print("  prefect deployment run daily_pipeline/wikipedia-schedule")

