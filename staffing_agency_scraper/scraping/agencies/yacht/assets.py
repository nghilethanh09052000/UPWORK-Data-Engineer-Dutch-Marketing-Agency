"""
Yacht Netherlands scraper.

Website: https://www.yacht.nl
Part of: Randstad Groep Nederland
Specializes in: Professionals, interim management
"""

from __future__ import annotations

import dagster as dg

from staffing_agency_scraper.models import Agency, DigitalCapabilities, GeoFocusType
from staffing_agency_scraper.scraping.base import BaseAgencyScraper


class YachtScraper(BaseAgencyScraper):
    """Scraper for Yacht Netherlands."""

    AGENCY_NAME = "Yacht"
    WEBSITE_URL = "https://www.yacht.nl"
    BRAND_GROUP = "Randstad Groep Nederland"

    # Discovered via sitemap
    PAGES_TO_SCRAPE = [
        "https://www.yacht.nl",
        "https://www.yacht.nl/werkgevers",
        "https://www.yacht.nl/over-yacht",
        "https://www.yacht.nl/contact",
        "https://www.yacht.nl/diensten",
        "https://www.yacht.nl/expertises",
        "https://www.yacht.nl/privacy",
    ]

    def scrape(self) -> Agency:
        self.logger.info(f"Starting scrape of {self.AGENCY_NAME}")

        agency = self.create_base_agency()
        agency.geo_focus_type = GeoFocusType.NATIONAL
        agency.employers_page_url = f"{self.WEBSITE_URL}/werkgevers"
        agency.contact_form_url = f"{self.WEBSITE_URL}/contact"

        agency = self.scrape_all_pages(agency)

        # Known facts - Yacht specializes in professionals
        agency.services.detacheren = True
        agency.services.werving_selectie = True
        agency.services.executive_search = True
        agency.membership = ["ABU"]
        agency.cao_type = "ABU"
        agency.regions_served = ["landelijk"]
        
        agency.digital_capabilities = DigitalCapabilities(
            client_portal=True,
            candidate_portal=True,
        )
        
        agency.focus_segments = ["white_collar", "technisch_specialisten"]
        agency.role_levels = ["medior", "senior"]
        agency.sectors_core = agency.sectors_core or ["finance", "ict", "techniek", "hr"]

        self.logger.info(f"Completed scrape of {self.AGENCY_NAME}")
        return agency


@dg.asset(group_name="agencies")
def yacht_scrape() -> dg.Output[dict]:
    """Scrape Yacht Netherlands website."""
    scraper = YachtScraper()
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

