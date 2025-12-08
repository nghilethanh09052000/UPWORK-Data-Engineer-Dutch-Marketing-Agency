"""Dagster definitions for Hays scraper."""

import dagster as dg
from .assets import hays_scrape

hays_asset_selection = dg.AssetSelection.assets(hays_scrape)

hays_scrape_job = dg.define_asset_job(
    name="hays_scrape_job",
    selection=hays_asset_selection,
    tags={"agency": "hays"},
)

@dg.schedule(
    job=hays_scrape_job,
    cron_schedule="0 6 * * 1",
    execution_timezone="Europe/Amsterdam",
    default_status=dg.DefaultScheduleStatus.STOPPED,
)
def hays_weekly_schedule():
    return dg.RunRequest()

defs = dg.Definitions(
    assets=[hays_scrape],
    jobs=[hays_scrape_job],
    schedules=[hays_weekly_schedule],
)

