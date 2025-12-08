"""Dagster definitions for Adecco scraper."""

import dagster as dg
from .assets import adecco_scrape

adecco_asset_selection = dg.AssetSelection.assets(adecco_scrape)

adecco_scrape_job = dg.define_asset_job(
    name="adecco_scrape_job",
    selection=adecco_asset_selection,
    tags={"agency": "adecco"},
)

@dg.schedule(
    job=adecco_scrape_job,
    cron_schedule="0 6 * * 1",
    execution_timezone="Europe/Amsterdam",
    default_status=dg.DefaultScheduleStatus.STOPPED,
)
def adecco_weekly_schedule():
    return dg.RunRequest()

defs = dg.Definitions(
    assets=[adecco_scrape],
    jobs=[adecco_scrape_job],
    schedules=[adecco_weekly_schedule],
)

