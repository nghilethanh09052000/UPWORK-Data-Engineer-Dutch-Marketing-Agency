"""
Adecco Netherlands scraper.

Website: https://www.adecco.nl
Part of: Adecco Group
"""

from __future__ import annotations

import dagster as dg

from staffing_agency_scraper.models import Agency, DigitalCapabilities, GeoFocusType
from staffing_agency_scraper.scraping.base import BaseAgencyScraper


class AdeccoScraper(BaseAgencyScraper):
    """Scraper for Adecco Netherlands."""

    AGENCY_NAME = "Adecco"
    WEBSITE_URL = "https://www.adecco.nl"
    BRAND_GROUP = "Adecco Group"

    PAGES_TO_SCRAPE = [
        "https://www.adecco.nl",
        "https://www.adecco.nl/werkgevers",
        "https://www.adecco.nl/over-adecco",
        "https://www.adecco.nl/contact",
        "https://www.adecco.nl/werkgevers/diensten",
        "https://www.adecco.nl/werkgevers/sectoren",
        "https://www.adecco.nl/privacy-policy",
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
        
        agency.digital_capabilities = DigitalCapabilities(
            client_portal=True,
            candidate_portal=True,
            mobile_app=True,
        )
        
        agency.focus_segments = ["blue_collar", "white_collar", "technisch_specialisten"]
        agency.sectors_core = agency.sectors_core or ["logistiek", "productie", "administratief", "techniek"]

        self.logger.info(f"Completed scrape of {self.AGENCY_NAME}")
        return agency


@dg.asset(group_name="agencies")
def adecco_scrape() -> dg.Output[dict]:
    """Scrape Adecco Netherlands website."""
    scraper = AdeccoScraper()
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

