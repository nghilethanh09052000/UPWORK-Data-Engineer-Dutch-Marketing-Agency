"""
Dagster definitions for Randstad scraper.
"""

import dagster as dg

from .assets import randstad_scrape

# Asset selection
randstad_asset_selection = dg.AssetSelection.assets(randstad_scrape)

# Job definition
randstad_scrape_job = dg.define_asset_job(
    name="randstad_scrape_job",
    selection=randstad_asset_selection,
    tags={"agency": "randstad"},
)


# Weekly schedule - scrape every Monday at 6:00 AM CET
@dg.schedule(
    job=randstad_scrape_job,
    cron_schedule="0 6 * * 1",  # Every Monday at 6:00 AM
    execution_timezone="Europe/Amsterdam",
    default_status=dg.DefaultScheduleStatus.STOPPED,  # Start manually for MVP
)
def randstad_weekly_schedule():
    return dg.RunRequest()


# Export definitions
defs = dg.Definitions(
    assets=[randstad_scrape],
    jobs=[randstad_scrape_job],
    schedules=[randstad_weekly_schedule],
)

