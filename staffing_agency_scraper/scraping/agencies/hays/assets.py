"""
Hays Netherlands scraper.

Website: https://www.hays.nl
Specializes in: Professional recruitment
"""

from __future__ import annotations

import dagster as dg

from staffing_agency_scraper.models import Agency, DigitalCapabilities, GeoFocusType
from staffing_agency_scraper.scraping.base import BaseAgencyScraper


class HaysScraper(BaseAgencyScraper):
    """Scraper for Hays Netherlands."""

    AGENCY_NAME = "Hays Nederland"
    WEBSITE_URL = "https://www.hays.nl"
    BRAND_GROUP = "Hays plc"

    # Discovered via sitemap
    PAGES_TO_SCRAPE = [
        "https://www.hays.nl",
        "https://www.hays.nl/werkgevers",
        "https://www.hays.nl/over-hays",
        "https://www.hays.nl/over-hays/hays-wereldwijd",
        "https://www.hays.nl/contact",
        "https://www.hays.nl/detachering",
        "https://www.hays.nl/privacy",  # Legal page for KvK
        "https://www.hays.nl/en/recruitment/contact-us",  # Alt contact
    ]

    def scrape(self) -> Agency:
        self.logger.info(f"Starting scrape of {self.AGENCY_NAME}")

        agency = self.create_base_agency()
        agency.geo_focus_type = GeoFocusType.INTERNATIONAL
        agency.employers_page_url = f"{self.WEBSITE_URL}/werkgevers"
        agency.contact_form_url = f"{self.WEBSITE_URL}/contact"

        agency = self.scrape_all_pages(agency)

        # Known facts - Hays specializes in professional recruitment
        agency.services.detacheren = True
        agency.services.werving_selectie = True
        agency.services.executive_search = True
        agency.services.msp = True
        agency.services.rpo = True
        agency.membership = ["ABU"]
        agency.cao_type = "ABU"
        agency.regions_served = ["landelijk", "internationaal"]
        
        agency.digital_capabilities = DigitalCapabilities(
            client_portal=True,
            candidate_portal=True,
        )
        
        agency.focus_segments = ["white_collar", "technisch_specialisten"]
        agency.role_levels = ["medior", "senior"]
        agency.sectors_core = agency.sectors_core or ["finance", "ict", "techniek", "hr", "marketing"]

        self.logger.info(f"Completed scrape of {self.AGENCY_NAME}")
        return agency


@dg.asset(group_name="agencies")
def hays_scrape() -> dg.Output[dict]:
    """Scrape Hays Netherlands website."""
    scraper = HaysScraper()
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

