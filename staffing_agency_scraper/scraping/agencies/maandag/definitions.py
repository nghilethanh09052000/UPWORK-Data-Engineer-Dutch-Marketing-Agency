"""Dagster definitions for Maandag scraper."""

import dagster as dg
from .assets import maandag_scrape

maandag_asset_selection = dg.AssetSelection.assets(maandag_scrape)

maandag_scrape_job = dg.define_asset_job(
    name="maandag_scrape_job",
    selection=maandag_asset_selection,
    tags={"agency": "maandag"},
)

@dg.schedule(
    job=maandag_scrape_job,
    cron_schedule="0 6 * * 1",
    execution_timezone="Europe/Amsterdam",
    default_status=dg.DefaultScheduleStatus.STOPPED,
)
def maandag_weekly_schedule():
    return dg.RunRequest()

defs = dg.Definitions(
    assets=[maandag_scrape],
    jobs=[maandag_scrape_job],
    schedules=[maandag_weekly_schedule],
)

