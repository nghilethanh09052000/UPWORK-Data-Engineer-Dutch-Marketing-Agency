"""Dagster definitions for Manpower scraper."""

import dagster as dg
from .assets import manpower_scrape

manpower_asset_selection = dg.AssetSelection.assets(manpower_scrape)

manpower_scrape_job = dg.define_asset_job(
    name="manpower_scrape_job",
    selection=manpower_asset_selection,
    tags={"agency": "manpower"},
)

@dg.schedule(
    job=manpower_scrape_job,
    cron_schedule="0 6 * * 1",
    execution_timezone="Europe/Amsterdam",
    default_status=dg.DefaultScheduleStatus.STOPPED,
)
def manpower_weekly_schedule():
    return dg.RunRequest()

defs = dg.Definitions(
    assets=[manpower_scrape],
    jobs=[manpower_scrape_job],
    schedules=[manpower_weekly_schedule],
)

