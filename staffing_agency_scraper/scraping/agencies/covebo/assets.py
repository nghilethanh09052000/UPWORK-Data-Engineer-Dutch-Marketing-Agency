"""
Covebo Netherlands scraper.

Website: https://www.covebo.nl
"""

from __future__ import annotations

from typing import Any, Dict, List

import dagster as dg
from bs4 import BeautifulSoup

from staffing_agency_scraper.models import Agency, GeoFocusType
from staffing_agency_scraper.scraping.base import BaseAgencyScraper
from staffing_agency_scraper.scraping.utils import AgencyScraperUtils

class CoveboScraper(BaseAgencyScraper):
    """Scraper for Covebo Netherlands."""

    AGENCY_NAME = "Covebo"
    WEBSITE_URL = "https://www.covebo.nl"
    BRAND_GROUP = None
    PAGES_TO_SCRAPE: List[Dict[str, Any]] = [
        {
            "name": "home",
            "url": "https://www.covebo.nl",
            "functions": ['logo', 'services'],
        },
        {
            "name": "werkgevers",
            "url": "https://www.covebo.nl/werkgevers",
            "functions": ['sectors'],
        },
        {
            "name": "contact",
            "url": "https://www.covebo.nl/contact",
            "functions": ['contact'],
        },
        {
            "name": "privacy",
            "url": "https://www.covebo.nl/privacy",
            "functions": ['legal'],
        },
    ]

    def scrape(self) -> Agency:
        self.logger.info(f"Starting scrape of {self.AGENCY_NAME}")
        
        
        # Initialize utils
        self.utils = AgencyScraperUtils(logger=self.logger)
        # Initialize utils        
        agency = self.create_base_agency()
        agency.geo_focus_type = GeoFocusType.NATIONAL
        agency.employers_page_url = f"{self.WEBSITE_URL}/werkgevers"
        agency.contact_form_url = f"{self.WEBSITE_URL}/contact"
        
        all_sectors = set()
        
        for page in self.PAGES_TO_SCRAPE:
            url = page["url"]
            page_name = page["name"]
            functions = page.get("functions", [])            
            try:
                self.logger.info("=" * 80)
                self.logger.info(f"📄 PROCESSING: {page_name}")
                self.logger.info(f"🔗 URL: {url}")
                self.logger.info("-" * 80)
                
                # Fetch with BS4
                soup = self.fetch_page(url)
                page_text = soup.get_text(separator=" ", strip=True)
                
                # Apply normal functions
                self._apply_functions(agency, functions, soup, page_text, all_sectors, url)
                
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
                
                self.logger.info(f"✅ Completed: {page_name}")
                
            except Exception as e:
                self.logger.error(f"❌ Error scraping {url}: {e}")
        
        # Finalize
        if all_sectors:
            agency.sectors_core = sorted(list(all_sectors))
        
        agency.evidence_urls = list(self.evidence_urls)
        agency.collected_at = self.collected_at
        
        self.logger.info("=" * 80)
        self.logger.info(f"✅ Completed scrape of {self.AGENCY_NAME}")
        self.logger.info(f"📄 Evidence URLs: {len(agency.evidence_urls)}")
        self.logger.info("=" * 80)
        
        return agency
    
    def _apply_functions(
        self,
        agency: Agency,
        functions: List[str],
        soup: BeautifulSoup,
        page_text: str,
        all_sectors: Set[str],
        url: str,
    ) -> None:
        """Apply BS4/regex extraction functions."""
        for func_name in functions:
            if func_name == "logo":
                logo = self.utils.fetch_logo(soup, url)
                if logo:
                    agency.logo_url = logo
            
            elif func_name == "services":
                services = self.utils.fetch_services(page_text, url)
                agency.services = services
            
            elif func_name == "contact":
                email = self.utils.fetch_contact_email(page_text, url)
                phone = self.utils.fetch_contact_phone(page_text, url)
                offices = self.utils.fetch_office_locations(soup, url)
                if email:
                    agency.contact_email = email
                if phone:
                    agency.contact_phone = phone
                if offices:
                    agency.office_locations = offices
                    if offices and not agency.hq_city:
                        agency.hq_city = offices[0].city
                        agency.hq_province = offices[0].province
            
            elif func_name == "legal":
                kvk = self.utils.fetch_kvk_number(page_text, url)
                legal_name = self.utils.fetch_legal_name(page_text, "Covebo", url)
                if kvk:
                    agency.kvk_number = kvk
                if legal_name:
                    agency.legal_name = legal_name
            
            elif func_name == "sectors":
                # Extract sectors from page
                for link in soup.find_all("a"):
                    text = link.get_text(strip=True)
                    if text and len(text) > 3 and len(text) < 50:
                        if any(kw in text.lower() for kw in ["logistiek", "zorg", "horeca", "retail", "productie", "administratie", "techniek", "bouw", "it"]):
                            all_sectors.add(text)
                            self.logger.info(f"✓ Found sector: '{text}' | Source: {url}")


@dg.asset(group_name="agencies")
def covebo_scrape() -> dg.Output[dict]:
    """Scrape Covebo website."""
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

