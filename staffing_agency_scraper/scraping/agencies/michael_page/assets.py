"""
Michael Page / Page Personnel Netherlands scraper.

Website: https://www.michaelpage.nl
Specializes in: Executive recruitment, professional services
"""

from __future__ import annotations

import dagster as dg

from staffing_agency_scraper.models import Agency, DigitalCapabilities, GeoFocusType
from staffing_agency_scraper.scraping.base import BaseAgencyScraper


class MichaelPageScraper(BaseAgencyScraper):
    """Scraper for Michael Page Netherlands."""

    AGENCY_NAME = "Michael Page"
    WEBSITE_URL = "https://www.michaelpage.nl"
    BRAND_GROUP = "PageGroup"

    # Discovered via sitemap - Michael Page has extensive blog/advice content
    PAGES_TO_SCRAPE = [
        "https://www.michaelpage.nl",
        "https://www.michaelpage.nl/about-us",
        "https://www.michaelpage.nl/advice/loopbaanadvies/loopbaanontwikkeling/overdraagbare-vaardigheden-ontwikkelen",
        "https://www.michaelpage.nl/advice/managementadvies/it-talent-aantrekken-BITT24",
        "https://www.michaelpage.nl/advice/markt-updates/amsterdam-aantrekkelijk-voor-internationale-bedrijven",
        "https://www.michaelpage.nl/contact",
        "https://www.michaelpage.nl/privacy",  # Legal page for KvK
    ]

    def scrape(self) -> Agency:
        self.logger.info(f"Starting scrape of {self.AGENCY_NAME}")

        agency = self.create_base_agency()
        agency.geo_focus_type = GeoFocusType.INTERNATIONAL
        agency.employers_page_url = f"{self.WEBSITE_URL}/werkgevers"
        agency.contact_form_url = f"{self.WEBSITE_URL}/contact"

        agency = self.scrape_all_pages(agency)

        # Known facts - Michael Page specializes in executive recruitment
        agency.services.werving_selectie = True
        agency.services.executive_search = True
        agency.services.detacheren = True
        agency.membership = ["ABU"]
        agency.cao_type = "ABU"
        agency.regions_served = ["landelijk", "internationaal"]
        
        agency.digital_capabilities = DigitalCapabilities(
            client_portal=True,
            candidate_portal=True,
        )
        
        agency.focus_segments = ["white_collar"]
        agency.role_levels = ["medior", "senior"]
        agency.sectors_core = agency.sectors_core or ["finance", "hr", "marketing", "sales", "ict"]

        self.logger.info(f"Completed scrape of {self.AGENCY_NAME}")
        return agency


@dg.asset(group_name="agencies")
def michael_page_scrape() -> dg.Output[dict]:
    """Scrape Michael Page Netherlands website."""
    scraper = MichaelPageScraper()
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

