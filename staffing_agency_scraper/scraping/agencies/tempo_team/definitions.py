"""
Dagster definitions for Tempo-Team scraper.
"""

import dagster as dg

from .assets import tempo_team_scrape

# Asset selection
tempo_team_asset_selection = dg.AssetSelection.assets(tempo_team_scrape)

# Job definition
tempo_team_scrape_job = dg.define_asset_job(
    name="tempo_team_scrape_job",
    selection=tempo_team_asset_selection,
    tags={"agency": "tempo_team"},
)


@dg.schedule(
    job=tempo_team_scrape_job,
    cron_schedule="0 6 * * 1",
    execution_timezone="Europe/Amsterdam",
    default_status=dg.DefaultScheduleStatus.STOPPED,
)
def tempo_team_weekly_schedule():
    return dg.RunRequest()


defs = dg.Definitions(
    assets=[tempo_team_scrape],
    jobs=[tempo_team_scrape_job],
    schedules=[tempo_team_weekly_schedule],
)

