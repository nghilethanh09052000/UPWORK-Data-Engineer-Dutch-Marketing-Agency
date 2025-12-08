"""Dagster definitions for Olympia scraper."""

import dagster as dg
from .assets import olympia_scrape

olympia_asset_selection = dg.AssetSelection.assets(olympia_scrape)

olympia_scrape_job = dg.define_asset_job(
    name="olympia_scrape_job",
    selection=olympia_asset_selection,
    tags={"agency": "olympia"},
)

@dg.schedule(
    job=olympia_scrape_job,
    cron_schedule="0 6 * * 1",
    execution_timezone="Europe/Amsterdam",
    default_status=dg.DefaultScheduleStatus.STOPPED,
)
def olympia_weekly_schedule():
    return dg.RunRequest()

defs = dg.Definitions(
    assets=[olympia_scrape],
    jobs=[olympia_scrape_job],
    schedules=[olympia_weekly_schedule],
)

