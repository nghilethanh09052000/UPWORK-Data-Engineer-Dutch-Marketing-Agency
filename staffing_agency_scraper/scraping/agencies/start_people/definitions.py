"""Dagster definitions for Start People scraper."""

import dagster as dg
from .assets import start_people_scrape

start_people_asset_selection = dg.AssetSelection.assets(start_people_scrape)

start_people_scrape_job = dg.define_asset_job(
    name="start_people_scrape_job",
    selection=start_people_asset_selection,
    tags={"agency": "start_people"},
)

@dg.schedule(
    job=start_people_scrape_job,
    cron_schedule="0 6 * * 1",
    execution_timezone="Europe/Amsterdam",
    default_status=dg.DefaultScheduleStatus.STOPPED,
)
def start_people_weekly_schedule():
    return dg.RunRequest()

defs = dg.Definitions(
    assets=[start_people_scrape],
    jobs=[start_people_scrape_job],
    schedules=[start_people_weekly_schedule],
)

