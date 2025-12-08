"""Dagster definitions for TMI scraper."""

import dagster as dg
from .assets import tmi_scrape

tmi_asset_selection = dg.AssetSelection.assets(tmi_scrape)

tmi_scrape_job = dg.define_asset_job(
    name="tmi_scrape_job",
    selection=tmi_asset_selection,
    tags={"agency": "tmi"},
)

@dg.schedule(
    job=tmi_scrape_job,
    cron_schedule="0 6 * * 1",
    execution_timezone="Europe/Amsterdam",
    default_status=dg.DefaultScheduleStatus.STOPPED,
)
def tmi_weekly_schedule():
    return dg.RunRequest()

defs = dg.Definitions(
    assets=[tmi_scrape],
    jobs=[tmi_scrape_job],
    schedules=[tmi_weekly_schedule],
)

