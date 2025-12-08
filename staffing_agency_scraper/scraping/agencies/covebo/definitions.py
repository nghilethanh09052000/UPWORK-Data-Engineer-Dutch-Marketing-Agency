"""Dagster definitions for Covebo scraper."""

import dagster as dg
from .assets import covebo_scrape

covebo_asset_selection = dg.AssetSelection.assets(covebo_scrape)

covebo_scrape_job = dg.define_asset_job(
    name="covebo_scrape_job",
    selection=covebo_asset_selection,
    tags={"agency": "covebo"},
)

@dg.schedule(
    job=covebo_scrape_job,
    cron_schedule="0 6 * * 1",
    execution_timezone="Europe/Amsterdam",
    default_status=dg.DefaultScheduleStatus.STOPPED,
)
def covebo_weekly_schedule():
    return dg.RunRequest()

defs = dg.Definitions(
    assets=[covebo_scrape],
    jobs=[covebo_scrape_job],
    schedules=[covebo_weekly_schedule],
)

