"""
Prefect Deployment Configurations

This module creates deployments for the daily_pipeline flow with different schedules:
1. Weather schedule: Hourly on the half hour (UTC)
2. Wikipedia schedule: Twice daily at 01:00 and 13:00 UTC
"""

from prefect import serve
from prefect.client.schemas.schedules import CronSchedule
from prefect.daily_pipeline import daily_pipeline

# Weather deployment: Hourly on the half hour (30 minutes past each hour) in UTC
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

# Wikipedia deployment: Twice daily at 01:00 and 13:00 UTC
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

if __name__ == "__main__":
    # Serve both deployments
    # This allows manual "run now" via Prefect UI or CLI
    weather_deployment.serve(name="weather-schedule")
    wikipedia_deployment.serve(name="wikipedia-schedule")

