"""
Tempo-Team Netherlands scraper.

Website: https://www.tempo-team.nl
Part of: Randstad Groep Nederland
"""

from __future__ import annotations

import dagster as dg

from staffing_agency_scraper.models import (
    Agency,
    DigitalCapabilities,
    GeoFocusType,
)
from staffing_agency_scraper.scraping.base import BaseAgencyScraper


class TempoTeamScraper(BaseAgencyScraper):
    """Scraper for Tempo-Team Netherlands."""

    AGENCY_NAME = "Tempo-Team"
    WEBSITE_URL = "https://www.tempo-team.nl"
    BRAND_GROUP = "Randstad Groep Nederland"

    # Discovered via sitemap analysis - includes legal pages for KvK
    PAGES_TO_SCRAPE = [
        "https://www.tempo-team.nl",
        "https://www.tempo-team.nl/werkgevers",
        "https://www.tempo-team.nl/over-tempo-team",
        "https://www.tempo-team.nl/over-tempo-team/contact",
        "https://www.tempo-team.nl/over-tempo-team/organisatie/certificering",
        "https://www.tempo-team.nl/over-tempo-team/onze-labels",
        "https://www.tempo-team.nl/over-tempo-team/algemeen/disclaimer",  # Legal page
        "https://www.tempo-team.nl/over-tempo-team/algemeen/privacy",  # Privacy page
        "https://www.tempo-team.nl/over-tempo-team/voorwaarden/gebruikersvoorwaarden-algemeen",
    ]

    def scrape(self) -> Agency:
        """
        Scrape Tempo-Team Netherlands website.

        Returns
        -------
        Agency
            Scraped agency data
        """
        self.logger.info(f"Starting scrape of {self.AGENCY_NAME}")

        # Initialize agency
        agency = self.create_base_agency()
        agency.brand_group = self.BRAND_GROUP
        agency.geo_focus_type = GeoFocusType.NATIONAL
        agency.employers_page_url = f"{self.WEBSITE_URL}/werkgevers"
        agency.contact_form_url = f"{self.WEBSITE_URL}/over-tempo-team/contact"

        # Use enhanced scraping that extracts from all pages
        agency = self.scrape_all_pages(agency)

        # Known facts about Tempo-Team
        agency.services.uitzenden = True
        agency.services.detacheren = True
        agency.services.werving_selectie = True
        agency.services.payrolling = True
        agency.services.inhouse_services = True
        agency.services.opleiden_ontwikkelen = True

        agency.membership = ["ABU"]
        agency.cao_type = "ABU"

        agency.digital_capabilities = DigitalCapabilities(
            client_portal=True,
            candidate_portal=True,
            mobile_app=True,
        )

        agency.focus_segments = [
            "blue_collar",
            "studenten",
        ]

        # Override sectors if not found
        if not agency.sectors_core:
            agency.sectors_core = [
                "logistiek",
                "productie",
                "horeca",
                "retail",
            ]

        agency.regions_served = ["landelijk"]

        self.logger.info(f"Completed scrape of {self.AGENCY_NAME}")
        return agency


@dg.asset(group_name="agencies")
def tempo_team_scrape() -> dg.Output[dict]:
    """
    Scrape Tempo-Team Netherlands website.

    This asset fetches factual company data from Tempo-Team's official website
    and returns it in the standardized JSON schema format.
    """
    scraper = TempoTeamScraper()
    agency = scraper.scrape()

    output_path = scraper.save_to_json(agency)

    return dg.Output(
        value=agency.to_json_dict(),
        metadata={
            "agency_name": agency.agency_name,
            "website_url": agency.website_url,
            "pages_scraped": len(agency.evidence_urls),
            "output_file": output_path,
            "collected_at": agency.collected_at.isoformat(),
        },
    )

