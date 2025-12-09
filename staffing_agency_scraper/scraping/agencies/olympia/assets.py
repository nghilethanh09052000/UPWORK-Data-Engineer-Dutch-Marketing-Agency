"""
Olympia Netherlands scraper.

Website: https://www.olympia.nl
"""

from __future__ import annotations

import dagster as dg

from staffing_agency_scraper.models import Agency, DigitalCapabilities, GeoFocusType
from staffing_agency_scraper.scraping.base import BaseAgencyScraper


class OlympiaScraper(BaseAgencyScraper):
    """Scraper for Olympia Netherlands."""

    AGENCY_NAME = "Olympia"
    WEBSITE_URL = "https://www.olympia.nl"
    BRAND_GROUP = None

    # Discovered via sitemap - note Olympia uses trailing slashes
    PAGES_TO_SCRAPE = [
        "https://www.olympia.nl",
        "https://www.olympia.nl/over-olympia/",
        "https://www.olympia.nl/over-olympia/contact/",
        "https://www.olympia.nl/vacatures/werkgevers/",
        "https://www.olympia.nl/personeel/inhouse/",
        "https://www.olympia.nl/vacatures/overig/",
        "https://www.olympia.nl/werk/werkgevers/gvb/",
        "https://www.olympia.nl/privacy/",  # Legal page for KvK
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
        agency.sectors_core = agency.sectors_core or ["logistiek", "productie", "techniek"]

        self.logger.info(f"Completed scrape of {self.AGENCY_NAME}")
        return agency


@dg.asset(group_name="agencies")
def olympia_scrape() -> dg.Output[dict]:
    """Scrape Olympia Netherlands website."""
    scraper = OlympiaScraper()
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

