"""
Covebo Netherlands scraper.

Website: https://www.covebo.nl
Specializes in: International workers, logistics
"""

from __future__ import annotations

import dagster as dg

from staffing_agency_scraper.models import Agency, DigitalCapabilities, GeoFocusType
from staffing_agency_scraper.scraping.base import BaseAgencyScraper


class CoveboScraper(BaseAgencyScraper):
    """Scraper for Covebo Netherlands."""

    AGENCY_NAME = "Covebo"
    WEBSITE_URL = "https://www.covebo.nl"
    BRAND_GROUP = None

    # Discovered via sitemap - note Covebo uses trailing slashes
    PAGES_TO_SCRAPE = [
        "https://www.covebo.nl",
        "https://www.covebo.nl/over-covebo/",
        "https://www.covebo.nl/over-covebo/contact/",
        "https://www.covebo.nl/over-covebo/ons-verhaal/",
        "https://www.covebo.nl/zzp/",
        "https://www.covebo.nl/zzp/kortingen/",
        "https://www.covebo.nl/vacatures/bouw/",
        "https://www.covebo.nl/privacy-policy/",  # Legal page for KvK
    ]

    def scrape(self) -> Agency:
        self.logger.info(f"Starting scrape of {self.AGENCY_NAME}")

        agency = self.create_base_agency()
        agency.geo_focus_type = GeoFocusType.NATIONAL
        agency.employers_page_url = f"{self.WEBSITE_URL}/werkgevers"
        agency.contact_form_url = f"{self.WEBSITE_URL}/contact"

        agency = self.scrape_all_pages(agency)

        # Known facts - Covebo specializes in international workers
        agency.services.uitzenden = True
        agency.services.detacheren = True
        agency.services.werving_selectie = True
        agency.services.payrolling = True
        agency.services.inhouse_services = True
        agency.membership = ["ABU"]
        agency.cao_type = "ABU"
        agency.regions_served = ["landelijk"]
        
        agency.focus_segments = ["blue_collar"]
        agency.sectors_core = agency.sectors_core or ["logistiek", "productie", "food"]
        
        agency.volume_specialisation = "massa_50_plus"

        self.logger.info(f"Completed scrape of {self.AGENCY_NAME}")
        return agency


@dg.asset(group_name="agencies")
def covebo_scrape() -> dg.Output[dict]:
    """Scrape Covebo Netherlands website."""
    scraper = CoveboScraper()
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

