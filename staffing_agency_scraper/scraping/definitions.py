"""
Aggregation of all scraping definitions from all agencies.

All 15 MVP agencies for inhuren.nl are registered here.
"""

import dagster as dg

# Import all agency definitions
from .agencies.randstad import definitions as randstad_defs
from .agencies.tempo_team import definitions as tempo_team_defs
from .agencies.youngcapital import definitions as youngcapital_defs
from .agencies.asa_talent import definitions as asa_talent_defs
from .agencies.manpower import definitions as manpower_defs
from .agencies.adecco import definitions as adecco_defs
from .agencies.olympia import definitions as olympia_defs
from .agencies.start_people import definitions as start_people_defs
from .agencies.covebo import definitions as covebo_defs
from .agencies.brunel import definitions as brunel_defs
from .agencies.yacht import definitions as yacht_defs
from .agencies.maandag import definitions as maandag_defs
from .agencies.hays import definitions as hays_defs
from .agencies.michael_page import definitions as michael_page_defs
from .agencies.tmi import definitions as tmi_defs

# Import all agency assets for the combined job
from .agencies.randstad.assets import randstad_scrape
from .agencies.tempo_team.assets import tempo_team_scrape
from .agencies.youngcapital.assets import youngcapital_scrape
from .agencies.asa_talent.assets import asa_talent_scrape
from .agencies.manpower.assets import manpower_scrape
from .agencies.adecco.assets import adecco_scrape
from .agencies.olympia.assets import olympia_scrape
from .agencies.start_people.assets import start_people_scrape
from .agencies.covebo.assets import covebo_scrape
from .agencies.brunel.assets import brunel_scrape
from .agencies.yacht.assets import yacht_scrape
from .agencies.maandag.assets import maandag_scrape
from .agencies.hays.assets import hays_scrape
from .agencies.michael_page.assets import michael_page_scrape
from .agencies.tmi.assets import tmi_scrape


# Combined job to scrape all 15 MVP agencies at once
all_agencies_selection = dg.AssetSelection.assets(
    randstad_scrape,
    tempo_team_scrape,
    youngcapital_scrape,
    asa_talent_scrape,
    manpower_scrape,
    adecco_scrape,
    olympia_scrape,
    start_people_scrape,
    covebo_scrape,
    brunel_scrape,
    yacht_scrape,
    maandag_scrape,
    hays_scrape,
    michael_page_scrape,
    tmi_scrape,
)

all_agencies_scrape_job = dg.define_asset_job(
    name="all_agencies_scrape_job",
    selection=all_agencies_selection,
    description="Scrape all 15 MVP staffing agencies",
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
    asa_talent_defs.defs,
    manpower_defs.defs,
    adecco_defs.defs,
    olympia_defs.defs,
    start_people_defs.defs,
    covebo_defs.defs,
    brunel_defs.defs,
    yacht_defs.defs,
    maandag_defs.defs,
    hays_defs.defs,
    michael_page_defs.defs,
    tmi_defs.defs,
    dg.Definitions(
        jobs=[all_agencies_scrape_job],
        schedules=[all_agencies_weekly_schedule],
    ),
)
