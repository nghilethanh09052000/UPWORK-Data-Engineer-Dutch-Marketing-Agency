"""
ASA Talent Netherlands scraper.

Website: https://www.asa.nl
"""

from __future__ import annotations

import dagster as dg

from staffing_agency_scraper.models import Agency, DigitalCapabilities, GeoFocusType
from staffing_agency_scraper.scraping.base import BaseAgencyScraper


class ASATalentScraper(BaseAgencyScraper):
    """Scraper for ASA Talent Netherlands."""

    AGENCY_NAME = "ASA Talent"
    WEBSITE_URL = "https://www.asa.nl"
    BRAND_GROUP = None

    PAGES_TO_SCRAPE = [
        "https://www.asa.nl",
        "https://www.asa.nl/werkgevers",
        "https://www.asa.nl/over-asa",
        "https://www.asa.nl/contact",
        "https://www.asa.nl/diensten",
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
        agency.membership = ["ABU"]
        agency.cao_type = "ABU"
        agency.regions_served = ["landelijk"]

        self.logger.info(f"Completed scrape of {self.AGENCY_NAME}")
        return agency


@dg.asset(group_name="agencies")
def asa_talent_scrape() -> dg.Output[dict]:
    """Scrape ASA Talent Netherlands website."""
    scraper = ASATalentScraper()
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

