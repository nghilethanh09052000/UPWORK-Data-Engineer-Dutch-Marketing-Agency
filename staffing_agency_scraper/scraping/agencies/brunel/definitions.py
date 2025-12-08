"""Dagster definitions for Brunel scraper."""

import dagster as dg
from .assets import brunel_scrape

brunel_asset_selection = dg.AssetSelection.assets(brunel_scrape)

brunel_scrape_job = dg.define_asset_job(
    name="brunel_scrape_job",
    selection=brunel_asset_selection,
    tags={"agency": "brunel"},
)

@dg.schedule(
    job=brunel_scrape_job,
    cron_schedule="0 6 * * 1",
    execution_timezone="Europe/Amsterdam",
    default_status=dg.DefaultScheduleStatus.STOPPED,
)
def brunel_weekly_schedule():
    return dg.RunRequest()

defs = dg.Definitions(
    assets=[brunel_scrape],
    jobs=[brunel_scrape_job],
    schedules=[brunel_weekly_schedule],
)

