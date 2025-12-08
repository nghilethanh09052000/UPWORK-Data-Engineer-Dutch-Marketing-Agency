"""
Brunel Netherlands scraper.

Website: https://www.brunel.nl
Specializes in: Technical professionals, Engineering
"""

from __future__ import annotations

import dagster as dg

from staffing_agency_scraper.models import Agency, DigitalCapabilities, GeoFocusType
from staffing_agency_scraper.scraping.base import BaseAgencyScraper


class BrunelScraper(BaseAgencyScraper):
    """Scraper for Brunel Netherlands."""

    AGENCY_NAME = "Brunel"
    WEBSITE_URL = "https://www.brunel.nl"
    BRAND_GROUP = "Brunel International"

    PAGES_TO_SCRAPE = [
        "https://www.brunel.nl",
        "https://www.brunel.nl/werkgevers",
        "https://www.brunel.nl/over-brunel",
        "https://www.brunel.nl/contact",
        "https://www.brunel.nl/diensten",
        "https://www.brunel.nl/sectoren",
    ]

    def scrape(self) -> Agency:
        self.logger.info(f"Starting scrape of {self.AGENCY_NAME}")

        agency = self.create_base_agency()
        agency.geo_focus_type = GeoFocusType.INTERNATIONAL
        agency.employers_page_url = f"{self.WEBSITE_URL}/werkgevers"
        agency.contact_form_url = f"{self.WEBSITE_URL}/contact"

        agency = self.scrape_all_pages(agency)

        # Known facts - Brunel specializes in technical/engineering
        agency.services.detacheren = True
        agency.services.werving_selectie = True
        agency.services.executive_search = True
        agency.membership = ["ABU"]
        agency.cao_type = "ABU"
        agency.regions_served = ["landelijk", "internationaal"]
        
        agency.focus_segments = ["technisch_specialisten", "white_collar"]
        agency.role_levels = ["medior", "senior"]
        agency.sectors_core = agency.sectors_core or ["techniek", "ict", "finance", "oil_gas"]

        self.logger.info(f"Completed scrape of {self.AGENCY_NAME}")
        return agency


@dg.asset(group_name="agencies")
def brunel_scrape() -> dg.Output[dict]:
    """Scrape Brunel Netherlands website."""
    scraper = BrunelScraper()
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

