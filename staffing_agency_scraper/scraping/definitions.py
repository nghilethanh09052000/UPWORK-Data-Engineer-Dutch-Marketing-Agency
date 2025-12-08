"""
Aggregation of all scraping definitions from all agencies.

If you've added a new agency scraper, add the definitions here.
"""

import dagster as dg

from .agencies.randstad import definitions as randstad_defs
from .agencies.tempo_team import definitions as tempo_team_defs
from .agencies.youngcapital import definitions as youngcapital_defs

# Import all agency assets for the combined job
from .agencies.randstad.assets import randstad_scrape
from .agencies.tempo_team.assets import tempo_team_scrape
from .agencies.youngcapital.assets import youngcapital_scrape


# Combined job to scrape all agencies at once
all_agencies_selection = dg.AssetSelection.assets(
    randstad_scrape,
    tempo_team_scrape,
    youngcapital_scrape,
    # TODO: Add more agencies as they are implemented
    # asa_talent_scrape,
    # manpower_scrape,
    # adecco_scrape,
    # olympia_scrape,
    # start_people_scrape,
    # covebo_scrape,
    # brunel_scrape,
    # yacht_scrape,
    # maandag_scrape,
    # hays_scrape,
    # michael_page_scrape,
    # tmi_scrape,
)

all_agencies_scrape_job = dg.define_asset_job(
    name="all_agencies_scrape_job",
    selection=all_agencies_selection,
    description="Scrape all configured staffing agencies",
    tags={"type": "full_scrape"},
)


@dg.schedule(
    job=all_agencies_scrape_job,
    cron_schedule="0 5 * * 1",  # Every Monday at 5:00 AM CET
    execution_timezone="Europe/Amsterdam",
    default_status=dg.DefaultScheduleStatus.STOPPED,  # Manual start for MVP
)
def all_agencies_weekly_schedule():
    """Weekly schedule to scrape all agencies."""
    return dg.RunRequest()


# Merge all definitions
defs = dg.Definitions.merge(
    randstad_defs.defs,
    tempo_team_defs.defs,
    youngcapital_defs.defs,
    dg.Definitions(
        jobs=[all_agencies_scrape_job],
        schedules=[all_agencies_weekly_schedule],
    ),
)

