"""
Randstad Netherlands scraper.

Website: https://www.randstad.nl
Part of: Randstad Groep Nederland
"""

from __future__ import annotations

import dagster as dg

from staffing_agency_scraper.lib.normalize import (
    detect_cao_type,
    detect_certifications,
)
from staffing_agency_scraper.models import (
    Agency,
    DigitalCapabilities,
    GeoFocusType,
)
from staffing_agency_scraper.scraping.base import BaseAgencyScraper


class RandstadScraper(BaseAgencyScraper):
    """Scraper for Randstad Netherlands."""

    AGENCY_NAME = "Randstad"
    WEBSITE_URL = "https://www.randstad.nl"
    BRAND_GROUP = "Randstad Groep Nederland"

    # Discovered via sitemap analysis - includes footer/legal pages for KvK
    PAGES_TO_SCRAPE = [
        "https://www.randstad.nl",
        "https://www.randstad.nl/werkgevers",
        "https://www.randstad.nl/over-randstad",
        "https://www.randstad.nl/over-randstad/contact",
        "https://www.randstad.nl/over-randstad/over-ons-bedrijf/certificering-randstad",
        "https://www.randstad.nl/over-randstad/onze-labels/detacheren-en-uitzenden",
        "https://www.randstad.nl/over-randstad/onze-specialties",
        "https://www.randstad.nl/over-randstad/disclaimer",  # Legal page with KvK
        "https://www.randstad.nl/over-randstad/cookieverklaring",  # Often has KvK
    ]

    def scrape(self) -> Agency:
        """
        Scrape Randstad Netherlands website.

        Returns
        -------
        Agency
            Scraped agency data
        """
        self.logger.info(f"Starting scrape of {self.AGENCY_NAME}")

        # Initialize agency
        agency = self.create_base_agency()
        agency.brand_group = self.BRAND_GROUP
        agency.geo_focus_type = GeoFocusType.NATIONAL
        agency.employers_page_url = f"{self.WEBSITE_URL}/werkgevers"
        agency.contact_form_url = f"{self.WEBSITE_URL}/over-randstad/contact"

        # Use enhanced scraping that extracts from all pages
        agency = self.scrape_all_pages(agency)

        # Set known facts about Randstad (verified information)
        agency.services.uitzenden = True
        agency.services.detacheren = True
        agency.services.werving_selectie = True
        agency.services.payrolling = True
        agency.services.inhouse_services = True
        agency.services.msp = True
        agency.services.rpo = True
        agency.services.opleiden_ontwikkelen = True
        agency.services.reintegratie_outplacement = True

        agency.membership = ["ABU"]
        agency.cao_type = "ABU"

        agency.digital_capabilities = DigitalCapabilities(
            client_portal=True,
            candidate_portal=True,
            mobile_app=True,
        )

        agency.focus_segments = [
            "blue_collar",
            "white_collar",
            "young_professionals",
        ]

        # Override sectors if not found
        if not agency.sectors_core:
            agency.sectors_core = [
                "logistiek",
                "productie",
                "administratief",
                "finance",
            ]

        agency.regions_served = ["landelijk"]

        self.logger.info(f"Completed scrape of {self.AGENCY_NAME}")
        return agency


@dg.asset(group_name="agencies")
def randstad_scrape() -> dg.Output[dict]:
    """
    Scrape Randstad Netherlands website.

    This asset fetches factual company data from Randstad's official website
    and returns it in the standardized JSON schema format.
    """
    scraper = RandstadScraper()
    agency = scraper.scrape()

    # Save to JSON
    output_path = scraper.save_to_json(agency)

    return dg.Output(
        value=agency.to_json_dict(),
        metadata={
            "agency_name": agency.agency_name,
            "website_url": agency.website_url,
            "pages_scraped": len(agency.evidence_urls),
            "output_file": output_path,
            "collected_at": agency.collected_at.isoformat(),
        },
    )

