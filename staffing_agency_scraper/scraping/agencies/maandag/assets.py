"""
Maandag Netherlands scraper.

Website: https://www.maandag.nl
"""

from __future__ import annotations

from typing import Any, Dict, List

import dagster as dg
from bs4 import BeautifulSoup

from staffing_agency_scraper.models import Agency, CaoType, GeoFocusType
from staffing_agency_scraper.scraping.base import BaseAgencyScraper
from staffing_agency_scraper.scraping.utils import AgencyScraperUtils


class MaandagScraper(BaseAgencyScraper):
    """Scraper for Maandag Netherlands."""

    AGENCY_NAME = "Maandag"
    WEBSITE_URL = "https://www.maandag.nl"
    BRAND_GROUP = None
    
    # Pages to scrape with specific functions per page
    PAGES_TO_SCRAPE: List[Dict[str, Any]] = [
        {
            "name": "home",
            "url": "https://www.maandag.nl",
            "functions": ["logo"],
        },
        {
            "name": "about",
            "url": "https://www.maandag.com/nl-nl/over-ons",
            "functions": [],
        },
        {
            "name": "government",
            "url": "https://www.maandag.com/nl-nl/overheid",
            "functions": [],
        },
        {
            "name": "contact",
            "url": "https://www.maandag.com/nl-nl/contact",
            "functions": ["contact"],
        },
        {
            "name": "service",
            "url": "https://www.maandag.com/nl-nl/service",
            "functions": ["services"],
        },
        {
            "name": "zzp_start",
            "url": "https://www.maandag.com/nl-nl/zzpstart",
            "functions": [],
        },
        {
            "name": "privacy",
            "url": "https://www.maandag.com/nl-nl/privacy",
            "functions": ["legal"],
        },
    ]

    def scrape(self) -> Agency:
        self.logger.info(f"Starting scrape of {self.AGENCY_NAME}")
        
        # Initialize utils
        self.utils = AgencyScraperUtils(logger=self.logger)
        
        agency = self.create_base_agency()
        agency.geo_focus_type = GeoFocusType.NATIONAL
        agency.employers_page_url = f"{self.WEBSITE_URL}/werkgevers"
        agency.contact_form_url = f"{self.WEBSITE_URL}/contact"
        
        # Known facts
        agency.membership = ["ABU"]
        agency.cao_type = CaoType.ABU
        agency.regions_served = ["landelijk"]
        
        all_text = ""
        
        for page in self.PAGES_TO_SCRAPE:
            url = page["url"]
            functions = page.get("functions", [])
            
            try:
                soup = self.fetch_page(url)
                page_text = soup.get_text(separator=" ", strip=True)
                all_text += " " + page_text
                
                # Apply specific functions for this page
                if functions:
                    self._apply_functions(agency, functions, soup, page_text, url)
                
                # Portal detection on every page
                if self.utils.detect_candidate_portal(soup, page_text, url):
                    agency.digital_capabilities.candidate_portal = True
                if self.utils.detect_client_portal(soup, page_text, url):
                    agency.digital_capabilities.client_portal = True
                
                # Extract role levels on every page
                role_levels = self.utils.fetch_role_levels(page_text, url)
                if role_levels:
                    if not agency.role_levels:
                        agency.role_levels = []
                    agency.role_levels.extend(role_levels)
                    agency.role_levels = list(set(agency.role_levels))
                
                # Extract review sources
                review_sources = self.utils.fetch_review_sources(soup, url)
                if review_sources and not agency.review_sources:
                    agency.review_sources = review_sources
                
            except Exception as e:
                self.logger.warning(f"Error scraping {url}: {e}")
        
        # Extract from aggregated text
        if not agency.certifications:
            agency.certifications = self.utils.fetch_certifications(all_text, "accumulated_text")
        
        # Extract sectors
        sectors = self.utils.fetch_sectors(all_text, "accumulated_text")
        if sectors:
            agency.sectors_core = sectors
        
        # Set HQ from office locations
        if not agency.hq_city and agency.office_locations:
            agency.hq_city = agency.office_locations[0].city
            agency.hq_province = agency.office_locations[0].province
        
        # Finalize
        agency.evidence_urls = list(self.evidence_urls)
        agency.collected_at = self.collected_at
        
        self.logger.info(f"Completed scrape of {self.AGENCY_NAME}")
        return agency
    
    def _apply_functions(
        self,
        agency: Agency,
        functions: List[str],
        soup: BeautifulSoup,
        page_text: str,
        url: str,
    ) -> None:
        """Apply extraction functions based on the functions list."""
        for func_name in functions:
            if func_name == "logo":
                if not agency.logo_url:
                    agency.logo_url = self.utils.fetch_logo(soup, url)
            
            elif func_name == "contact":
                if not agency.contact_email:
                    agency.contact_email = self.utils.fetch_contact_email(page_text, url)
                if not agency.contact_phone:
                    agency.contact_phone = self.utils.fetch_contact_phone(page_text, url)
                if not agency.office_locations:
                    agency.office_locations = self.utils.fetch_office_locations(soup, url)
            
            elif func_name == "services":
                services = self.utils.fetch_services(page_text, url)
                agency.services = services
            
            elif func_name == "legal":
                if not agency.kvk_number:
                    agency.kvk_number = self.utils.fetch_kvk_number(page_text, url)
                if not agency.legal_name:
                    agency.legal_name = self.utils.fetch_legal_name(page_text, "Maandag", url)


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
