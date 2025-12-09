"""
Manpower Netherlands scraper.

Website: https://www.manpower.nl
Part of: ManpowerGroup
"""

from __future__ import annotations

import dagster as dg

from staffing_agency_scraper.models import Agency, DigitalCapabilities, GeoFocusType
from staffing_agency_scraper.scraping.base import BaseAgencyScraper


class ManpowerScraper(BaseAgencyScraper):
    """Scraper for Manpower Netherlands."""

    AGENCY_NAME = "Manpower"
    WEBSITE_URL = "https://www.manpower.nl"
    BRAND_GROUP = "ManpowerGroup"

    # Discovered via sitemap - note Manpower uses /nl/ prefix
    PAGES_TO_SCRAPE = [
        "https://www.manpower.nl",
        "https://www.manpower.nl/nl/werkgevers",
        "https://www.manpower.nl/nl/werkgevers/hr-services",
        "https://www.manpower.nl/nl/werkgevers/specialisaties",
        "https://www.manpower.nl/nl/over-manpower",
        "https://www.manpower.nl/nl/over-manpower/contact",
        "https://www.manpower.nl/nl/zoek-vacatures/dienstverband",
        "https://www.manpower.nl/nl/privacy",  # Legal page for KvK
    ]

    def scrape(self) -> Agency:
        self.logger.info(f"Starting scrape of {self.AGENCY_NAME}")

        agency = self.create_base_agency()
        agency.geo_focus_type = GeoFocusType.INTERNATIONAL
        agency.employers_page_url = f"{self.WEBSITE_URL}/werkgevers"
        agency.contact_form_url = f"{self.WEBSITE_URL}/contact"

        agency = self.scrape_all_pages(agency)

        # Known facts
        agency.services.uitzenden = True
        agency.services.detacheren = True
        agency.services.werving_selectie = True
        agency.services.payrolling = True
        agency.services.msp = True
        agency.services.rpo = True
        agency.membership = ["ABU"]
        agency.cao_type = "ABU"
        agency.regions_served = ["landelijk", "internationaal"]
        
        agency.focus_segments = ["blue_collar", "white_collar"]
        agency.sectors_core = agency.sectors_core or ["logistiek", "productie", "administratief"]

        self.logger.info(f"Completed scrape of {self.AGENCY_NAME}")
        return agency


@dg.asset(group_name="agencies")
def manpower_scrape() -> dg.Output[dict]:
    """Scrape Manpower Netherlands website."""
    scraper = ManpowerScraper()
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

