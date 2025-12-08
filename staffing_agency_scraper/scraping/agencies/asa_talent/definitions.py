"""Dagster definitions for ASA Talent scraper."""

import dagster as dg
from .assets import asa_talent_scrape

asa_talent_asset_selection = dg.AssetSelection.assets(asa_talent_scrape)

asa_talent_scrape_job = dg.define_asset_job(
    name="asa_talent_scrape_job",
    selection=asa_talent_asset_selection,
    tags={"agency": "asa_talent"},
)

@dg.schedule(
    job=asa_talent_scrape_job,
    cron_schedule="0 6 * * 1",
    execution_timezone="Europe/Amsterdam",
    default_status=dg.DefaultScheduleStatus.STOPPED,
)
def asa_talent_weekly_schedule():
    return dg.RunRequest()

defs = dg.Definitions(
    assets=[asa_talent_scrape],
    jobs=[asa_talent_scrape_job],
    schedules=[asa_talent_weekly_schedule],
)

