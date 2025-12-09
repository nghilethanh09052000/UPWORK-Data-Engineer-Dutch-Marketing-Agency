"""
TMI Netherlands scraper.

Website: https://www.tmi.nl
Specializes in: Healthcare (Zorg), interim professionals
"""

from __future__ import annotations

import dagster as dg

from staffing_agency_scraper.models import Agency, DigitalCapabilities, GeoFocusType
from staffing_agency_scraper.scraping.base import BaseAgencyScraper


class TMIScraper(BaseAgencyScraper):
    """Scraper for TMI Netherlands."""

    AGENCY_NAME = "TMI"
    WEBSITE_URL = "https://www.tmi.nl"
    BRAND_GROUP = None

    # Discovered via sitemap - note TMI uses trailing slashes
    PAGES_TO_SCRAPE = [
        "https://www.tmi.nl",
        "https://www.tmi.nl/over-tmi/",
        "https://www.tmi.nl/over-tmi/events/",
        "https://www.tmi.nl/over-tmi/contact/",
        "https://www.tmi.nl/opdrachtgevers/",
        "https://www.tmi.nl/opdrachtgevers/ggd/",
        "https://www.tmi.nl/opdrachtgevers/ggz/",
        "https://www.tmi.nl/academy/",
        "https://www.tmi.nl/privacy/",  # Legal page for KvK
    ]

    def scrape(self) -> Agency:
        self.logger.info(f"Starting scrape of {self.AGENCY_NAME}")

        agency = self.create_base_agency()
        agency.geo_focus_type = GeoFocusType.NATIONAL
        agency.employers_page_url = f"{self.WEBSITE_URL}/opdrachtgevers"
        agency.contact_form_url = f"{self.WEBSITE_URL}/contact"

        agency = self.scrape_all_pages(agency)

        # Known facts - TMI specializes in healthcare (Zorg)
        agency.services.detacheren = True
        agency.services.werving_selectie = True
        agency.services.zzp_bemiddeling = True
        agency.services.opleiden_ontwikkelen = True
        agency.membership = ["NBBU"]
        agency.cao_type = "NBBU"
        agency.regions_served = ["landelijk"]
        
        agency.digital_capabilities = DigitalCapabilities(
            client_portal=True,
            candidate_portal=True,
        )
        
        agency.focus_segments = ["zorgprofessionals"]
        agency.role_levels = ["medior", "senior"]
        agency.sectors_core = ["zorg"]

        self.logger.info(f"Completed scrape of {self.AGENCY_NAME}")
        return agency


@dg.asset(group_name="agencies")
def tmi_scrape() -> dg.Output[dict]:
    """Scrape TMI Netherlands website."""
    scraper = TMIScraper()
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

