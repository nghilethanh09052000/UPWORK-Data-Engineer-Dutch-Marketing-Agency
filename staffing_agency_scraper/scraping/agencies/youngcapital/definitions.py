"""
Dagster definitions for YoungCapital scraper.
"""

import dagster as dg

from .assets import youngcapital_scrape

# Asset selection
youngcapital_asset_selection = dg.AssetSelection.assets(youngcapital_scrape)

# Job definition
youngcapital_scrape_job = dg.define_asset_job(
    name="youngcapital_scrape_job",
    selection=youngcapital_asset_selection,
    tags={"agency": "youngcapital"},
)


@dg.schedule(
    job=youngcapital_scrape_job,
    cron_schedule="0 6 * * 1",
    execution_timezone="Europe/Amsterdam",
    default_status=dg.DefaultScheduleStatus.STOPPED,
)
def youngcapital_weekly_schedule():
    return dg.RunRequest()


defs = dg.Definitions(
    assets=[youngcapital_scrape],
    jobs=[youngcapital_scrape_job],
    schedules=[youngcapital_weekly_schedule],
)

