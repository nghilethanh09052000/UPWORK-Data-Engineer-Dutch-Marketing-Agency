"""
Maandag Netherlands scraper.

Website: https://www.maandag.nl
"""

from __future__ import annotations

import dagster as dg

from staffing_agency_scraper.models import Agency, DigitalCapabilities, GeoFocusType
from staffing_agency_scraper.scraping.base import BaseAgencyScraper


class MaandagScraper(BaseAgencyScraper):
    """Scraper for Maandag Netherlands."""

    AGENCY_NAME = "Maandag"
    WEBSITE_URL = "https://www.maandag.nl"
    BRAND_GROUP = None

    # Discovered via sitemap
    PAGES_TO_SCRAPE = [
        "https://www.maandag.nl",
        "https://www.maandag.nl/werkgevers",
        "https://www.maandag.nl/over-maandag",
        "https://www.maandag.nl/contact",
        "https://www.maandag.nl/diensten",
        "https://www.maandag.nl/branches",
        "https://www.maandag.nl/vestigingen",
        "https://www.maandag.nl/privacy",
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
        
        agency.focus_segments = ["blue_collar", "white_collar"]
        agency.sectors_core = agency.sectors_core or ["logistiek", "productie", "administratief"]

        self.logger.info(f"Completed scrape of {self.AGENCY_NAME}")
        return agency


@dg.asset(group_name="agencies")
def maandag_scrape() -> dg.Output[dict]:
    """Scrape Maandag Netherlands website."""
    scraper = MaandagScraper()
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

