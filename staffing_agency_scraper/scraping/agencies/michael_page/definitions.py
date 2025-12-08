"""Dagster definitions for Michael Page scraper."""

import dagster as dg
from .assets import michael_page_scrape

michael_page_asset_selection = dg.AssetSelection.assets(michael_page_scrape)

michael_page_scrape_job = dg.define_asset_job(
    name="michael_page_scrape_job",
    selection=michael_page_asset_selection,
    tags={"agency": "michael_page"},
)

@dg.schedule(
    job=michael_page_scrape_job,
    cron_schedule="0 6 * * 1",
    execution_timezone="Europe/Amsterdam",
    default_status=dg.DefaultScheduleStatus.STOPPED,
)
def michael_page_weekly_schedule():
    return dg.RunRequest()

defs = dg.Definitions(
    assets=[michael_page_scrape],
    jobs=[michael_page_scrape_job],
    schedules=[michael_page_weekly_schedule],
)

