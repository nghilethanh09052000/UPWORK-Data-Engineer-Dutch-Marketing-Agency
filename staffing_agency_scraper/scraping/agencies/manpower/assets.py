"""
Manpower Netherlands scraper.

Website: https://www.manpower.nl
Part of: ManpowerGroup"""

from __future__ import annotations

from typing import Any, Dict, List, Set

import dagster as dg
from bs4 import BeautifulSoup

from staffing_agency_scraper.models import Agency, GeoFocusType
from staffing_agency_scraper.scraping.base import BaseAgencyScraper
from staffing_agency_scraper.scraping.utils import AgencyScraperUtils


class ManpowerScraper(BaseAgencyScraper):
    """Scraper for Manpower Netherlands."""

    AGENCY_NAME = "Manpower"
    WEBSITE_URL = "https://www.manpower.nl"
    BRAND_GROUP = "ManpowerGroup"
    PAGES_TO_SCRAPE: List[Dict[str, Any]] = [
        {
            "name": "home",
            "url": "https://www.manpower.nl",
            "functions": ['logo', 'services'],
        },
        {
            "name": "werkgevers",
            "url": "https://www.manpower.nl/nl/werkgevers",
            "functions": ['sectors'],
        },
        {
            "name": "contact",
            "url": "https://www.manpower.nl/nl/over-manpower/contact",
            "functions": ['contact'],
        },
        {
            "name": "privacy",
            "url": "https://www.manpower.nl/nl/privacy",
            "functions": ['legal'],
        },
    ]

    def scrape(self) -> Agency:
        self.logger.info(f"Starting scrape of {self.AGENCY_NAME}")
        
        
        # Initialize utils
        self.utils = AgencyScraperUtils(logger=self.logger)
        # Initialize utils        
        agency = self.create_base_agency()
        agency.geo_focus_type = GeoFocusType.INTERNATIONAL
        agency.employers_page_url = f"{self.WEBSITE_URL}/werkgevers"
        agency.contact_form_url = f"{self.WEBSITE_URL}/contact"
        
        all_sectors: Set[str] = set()
        
        for page in self.PAGES_TO_SCRAPE:
            url = page["url"]
            page_name = page["name"]
            functions = page.get("functions", [])
            use_ai = page.get("use_ai", False)
            
            try:
                self.logger.info("=" * 80)
                self.logger.info(f"📄 PROCESSING: {page_name}")
                self.logger.info(f"🔗 URL: {url}")
                self.logger.info("-" * 80)
                
                # Fetch with BS4
                soup = self.fetch_page(url)
                page_text = soup.get_text(separator=" ", strip=True)
                
                # Apply normal functions
                self._apply_functions_normal(agency, functions, soup, page_text, all_sectors, url)
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
    
    def _apply_functions_normal(
        self,
        agency: Agency,
        functions: List[str],
        soup: BeautifulSoup,
        page_text: str,
        all_sectors: Set[str],
        url: str,
    ) -> None:
        """Apply BS4/regex extraction functions (with CLIENT IMPROVEMENTS)."""
        for func_name in functions:
            if func_name == "logo":
                # CLIENT IMPROVEMENT #1: PNG/SVG only, from header/footer
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
                legal_name = self.utils.fetch_legal_name(page_text, "Manpower", url)
                certs = self.utils.fetch_certifications(page_text, url)
                cao = self.utils.fetch_cao_type(page_text, url)
                membership = self.utils.fetch_membership(page_text, url)
                if kvk:
                    agency.kvk_number = kvk
                if legal_name:
                    agency.legal_name = legal_name
                if certs:
                    agency.certifications = certs
                if cao:
                    agency.cao_type = cao
                if membership:
                    agency.membership = membership
            
            elif func_name == "sectors":
                # CLIENT IMPROVEMENT #2: Use normalized sector list
                sectors = self.utils.fetch_sectors(page_text, url)
                all_sectors.update(sectors)
        
        # CLIENT IMPROVEMENT #3: Portal detection (on every page)
        if self.utils.detect_candidate_portal(soup, page_text, url):
            agency.digital_capabilities.candidate_portal = True
        if self.utils.detect_client_portal(soup, page_text, url):
            agency.digital_capabilities.client_portal = True
        
        # CLIENT IMPROVEMENT #4: Role levels (on every page)
        role_levels = self.utils.fetch_role_levels(page_text, url)
        if role_levels:
            if not agency.role_levels_offered:
                agency.role_levels_offered = []
            agency.role_levels_offered.extend(role_levels)
            agency.role_levels_offered = list(set(agency.role_levels_offered))  # Dedupe
        
        # CLIENT IMPROVEMENT #5: Review sources (on every page)
        review_sources = self.utils.fetch_review_sources(soup, url)
        if review_sources and not agency.review_sources:
            agency.review_sources = review_sources



@dg.asset(group_name="agencies")
def manpower_scrape() -> dg.Output[dict]:
    """Scrape Manpower Netherlands website."""
    scraper = ManpowerScraper()
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
