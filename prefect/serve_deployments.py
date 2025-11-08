"""
Serve Prefect deployments for manual and scheduled execution.

This script serves both deployments, enabling:
- Scheduled runs (automatic based on cron schedules)
- Manual "run now" via Prefect UI or CLI

Run with:
    python prefect/serve_deployments.py

Or use Prefect serve:
    prefect serve prefect/deployments.py
"""

from prefect import serve
from prefect.client.schemas.schedules import CronSchedule
from prefect.daily_pipeline import daily_pipeline

if __name__ == "__main__":
    # Create and serve weather deployment
    weather_deployment = daily_pipeline.to_deployment(
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
    
    # Create and serve Wikipedia deployment
    wikipedia_deployment = daily_pipeline.to_deployment(
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
    
    # Serve both deployments
    # This enables scheduled runs and manual "run now" functionality
    serve(
        weather_deployment,
        wikipedia_deployment,
        limit=10  # Maximum concurrent flow runs
    )

