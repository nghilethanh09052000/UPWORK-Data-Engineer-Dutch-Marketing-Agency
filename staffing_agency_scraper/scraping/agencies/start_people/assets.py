"""
Start People Netherlands scraper.

Website: https://www.startpeople.nl
Part of: USG People
"""

from __future__ import annotations

import dagster as dg

from staffing_agency_scraper.models import Agency, DigitalCapabilities, GeoFocusType
from staffing_agency_scraper.scraping.base import BaseAgencyScraper


class StartPeopleScraper(BaseAgencyScraper):
    """Scraper for Start People Netherlands."""

    AGENCY_NAME = "Start People"
    WEBSITE_URL = "https://www.startpeople.nl"
    BRAND_GROUP = "USG People"

    PAGES_TO_SCRAPE = [
        "https://www.startpeople.nl",
        "https://www.startpeople.nl/werkgevers",
        "https://www.startpeople.nl/over-ons",
        "https://www.startpeople.nl/contact",
        "https://www.startpeople.nl/diensten",
        "https://www.startpeople.nl/sectoren",
        "https://www.startpeople.nl/vestigingen",
    ]

    def scrape(self) -> Agency:
        self.logger.info(f"Starting scrape of {self.AGENCY_NAME}")

        agency = self.create_base_agency()
        agency.geo_focus_type = GeoFocusType.NATIONAL
        agency.employers_page_url = f"{self.WEBSITE_URL}/werkgevers"
        agency.contact_form_url = f"{self.WEBSITE_URL}/contact"

        agency = self.scrape_all_pages(agency)

        # Known facts
        agency.services.uitzenden = True
        agency.services.detacheren = True
        agency.services.werving_selectie = True
        agency.services.payrolling = True
        agency.membership = ["ABU"]
        agency.cao_type = "ABU"
        agency.regions_served = ["landelijk"]
        
        agency.digital_capabilities = DigitalCapabilities(
            client_portal=True,
            candidate_portal=True,
            mobile_app=True,
        )
        
        agency.focus_segments = ["blue_collar", "studenten"]
        agency.sectors_core = agency.sectors_core or ["logistiek", "productie", "horeca", "retail"]

        self.logger.info(f"Completed scrape of {self.AGENCY_NAME}")
        return agency


@dg.asset(group_name="agencies")
def start_people_scrape() -> dg.Output[dict]:
    """Scrape Start People Netherlands website."""
    scraper = StartPeopleScraper()
    agency = scraper.scrape()
    output_path = scraper.save_to_json(agency)
    return dg.Output(
        value=agency.to_json_dict(),
        metadata={
            "agency_name": agency.agency_name,
            "website_url": agency.website_url,
            "pages_scraped": len(agency.evidence_urls),
            "output_file": output_path,
        },
    )

