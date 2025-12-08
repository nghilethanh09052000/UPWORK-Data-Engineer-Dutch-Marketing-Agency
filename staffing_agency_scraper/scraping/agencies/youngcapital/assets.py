"""
YoungCapital Netherlands scraper.

Website: https://www.youngcapital.nl
Specializes in: Students and Young Professionals
"""

from __future__ import annotations

import dagster as dg

from staffing_agency_scraper.models import (
    Agency,
    DigitalCapabilities,
    GeoFocusType,
)
from staffing_agency_scraper.scraping.base import BaseAgencyScraper


class YoungCapitalScraper(BaseAgencyScraper):
    """Scraper for YoungCapital Netherlands."""

    AGENCY_NAME = "YoungCapital"
    WEBSITE_URL = "https://www.youngcapital.nl"
    BRAND_GROUP = None  # Independent

    # Discovered via sitemap analysis - includes legal pages for KvK
    PAGES_TO_SCRAPE = [
        "https://www.youngcapital.nl",
        "https://www.youngcapital.nl/werkgevers",
        "https://www.youngcapital.nl/werkgevers/contact",
        "https://www.youngcapital.nl/werkgevers/diensten",
        "https://www.youngcapital.nl/werkgevers/specialisaties",
        "https://www.youngcapital.nl/over-yc",
        "https://www.youngcapital.nl/over-yc/certificering",
        "https://www.youngcapital.nl/over-yc/algemenevoorwaarden",  # Legal/terms
        "https://www.youngcapital.nl/over-yc/privacyverklaring",  # Privacy page
    ]

    def scrape(self) -> Agency:
        """
        Scrape YoungCapital Netherlands website.

        Returns
        -------
        Agency
            Scraped agency data
        """
        self.logger.info(f"Starting scrape of {self.AGENCY_NAME}")

        # Initialize agency
        agency = self.create_base_agency()
        agency.geo_focus_type = GeoFocusType.NATIONAL
        agency.employers_page_url = f"{self.WEBSITE_URL}/werkgevers"
        agency.contact_form_url = f"{self.WEBSITE_URL}/werkgevers/contact"

        # Use enhanced scraping that extracts from all pages
        agency = self.scrape_all_pages(agency)

        # Known facts about YoungCapital
        agency.services.uitzenden = True
        agency.services.detacheren = True
        agency.services.werving_selectie = True
        agency.services.payrolling = True
        agency.services.rpo = True
        agency.services.opleiden_ontwikkelen = True

        agency.membership = ["ABU"]
        agency.cao_type = "ABU"

        agency.digital_capabilities = DigitalCapabilities(
            client_portal=True,
            candidate_portal=True,
            mobile_app=True,
        )

        # YoungCapital specializes in young workers
        agency.focus_segments = [
            "studenten",
            "young_professionals",
        ]

        agency.role_levels = [
            "student",
            "starter",
        ]

        # Override sectors if not found
        if not agency.sectors_core:
            agency.sectors_core = [
                "horeca",
                "retail",
                "logistiek",
                "administratief",
            ]

        agency.regions_served = ["landelijk"]

        agency.typical_use_cases = [
            "piekdruk_opvangen",
            "weekenddiensten",
            "seizoenswerk",
        ]

        agency.shift_types_supported = [
            "dagdienst",
            "avonddienst",
            "weekend",
        ]

        self.logger.info(f"Completed scrape of {self.AGENCY_NAME}")
        return agency


@dg.asset(group_name="agencies")
def youngcapital_scrape() -> dg.Output[dict]:
    """
    Scrape YoungCapital Netherlands website.

    This asset fetches factual company data from YoungCapital's official website
    and returns it in the standardized JSON schema format.
    """
    scraper = YoungCapitalScraper()
    agency = scraper.scrape()

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

