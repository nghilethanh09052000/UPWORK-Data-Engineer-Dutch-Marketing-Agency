"""Dagster definitions for Yacht scraper."""

import dagster as dg
from .assets import yacht_scrape

yacht_asset_selection = dg.AssetSelection.assets(yacht_scrape)

yacht_scrape_job = dg.define_asset_job(
    name="yacht_scrape_job",
    selection=yacht_asset_selection,
    tags={"agency": "yacht"},
)

@dg.schedule(
    job=yacht_scrape_job,
    cron_schedule="0 6 * * 1",
    execution_timezone="Europe/Amsterdam",
    default_status=dg.DefaultScheduleStatus.STOPPED,
)
def yacht_weekly_schedule():
    return dg.RunRequest()

defs = dg.Definitions(
    assets=[yacht_scrape],
    jobs=[yacht_scrape_job],
    schedules=[yacht_weekly_schedule],
)

