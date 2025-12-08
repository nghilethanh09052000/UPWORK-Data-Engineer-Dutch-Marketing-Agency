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
    detect_focus_segments,
    detect_services,
    extract_sectors_from_text,
    normalize_geo_focus,
)
from staffing_agency_scraper.lib.parse import (
    extract_email,
    extract_kvk_number,
    extract_phone,
    get_attribute,
    get_text_content,
)
from staffing_agency_scraper.models import (
    Agency,
    AgencyServices,
    DigitalCapabilities,
    GeoFocusType,
    OfficeLocation,
)
from staffing_agency_scraper.scraping.base import BaseAgencyScraper


class RandstadScraper(BaseAgencyScraper):
    """Scraper for Randstad Netherlands."""

    AGENCY_NAME = "Randstad"
    WEBSITE_URL = "https://www.randstad.nl"
    BRAND_GROUP = "Randstad Groep Nederland"

    PAGES_TO_SCRAPE = [
        "https://www.randstad.nl",
        "https://www.randstad.nl/werkgevers",
        "https://www.randstad.nl/over-randstad",
        "https://www.randstad.nl/over-randstad/contact",
        "https://www.randstad.nl/werkgevers/uitzenden",
        "https://www.randstad.nl/werkgevers/detachering",
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

        # Initialize with known data
        agency = self.create_base_agency()
        agency.brand_group = self.BRAND_GROUP
        agency.geo_focus_type = GeoFocusType.NATIONAL

        # Scrape homepage
        try:
            homepage = self.fetch_page(self.WEBSITE_URL)
            agency.logo_url = self.extract_logo_url(homepage)
        except Exception as e:
            self.logger.warning(f"Error scraping homepage: {e}")

        # Scrape employers page (werkgevers)
        try:
            werkgevers_page = self.fetch_page(f"{self.WEBSITE_URL}/werkgevers")
            services = self.extract_services_from_page(werkgevers_page)
            agency.services = services
            agency.employers_page_url = f"{self.WEBSITE_URL}/werkgevers"

            # Look for contact form
            contact_link = self.find_page_url(
                werkgevers_page, ["contact", "neem contact op"]
            )
            if contact_link:
                agency.contact_form_url = contact_link
        except Exception as e:
            self.logger.warning(f"Error scraping werkgevers page: {e}")

        # Scrape about page
        try:
            about_page = self.fetch_page(f"{self.WEBSITE_URL}/over-randstad")
            page_text = about_page.get_text()

            # Extract certifications and memberships
            agency.certifications = detect_certifications(page_text)
            agency.cao_type = detect_cao_type(page_text)

            # Extract sectors
            agency.sectors_core = extract_sectors_from_text(page_text)

            # Extract KvK if available
            kvk = extract_kvk_number(page_text)
            if kvk:
                agency.kvk_number = kvk
        except Exception as e:
            self.logger.warning(f"Error scraping about page: {e}")

        # Scrape contact page
        try:
            contact_page = self.fetch_page(f"{self.WEBSITE_URL}/over-randstad/contact")
            contact_info = self.extract_contact_info(contact_page)
            if contact_info.get("contact_phone"):
                agency.contact_phone = contact_info["contact_phone"]
            if contact_info.get("contact_email"):
                agency.contact_email = contact_info["contact_email"]
        except Exception as e:
            self.logger.warning(f"Error scraping contact page: {e}")

        # Set evidence URLs and timestamp
        agency.evidence_urls = self.evidence_urls.copy()
        agency.collected_at = self.collected_at

        # Known facts about Randstad
        agency.services.uitzenden = True
        agency.services.detacheren = True
        agency.services.werving_selectie = True
        agency.services.payrolling = True
        agency.services.inhouse_services = True
        agency.services.msp = True

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

