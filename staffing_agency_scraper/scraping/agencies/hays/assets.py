"""
Hays Netherlands scraper.

Website: https://www.hays.nl
Specializes in: Professional recruitment, RPO, MSP
International staffing company with 50+ years experience globally
"""

from __future__ import annotations

import re
from typing import Any, Dict, List

import dagster as dg
from bs4 import BeautifulSoup

from staffing_agency_scraper.models import (
    Agency,
    GeoFocusType,
    OfficeLocation,
    VolumeSpecialisation,
)
from staffing_agency_scraper.scraping.base import BaseAgencyScraper
from staffing_agency_scraper.scraping.utils import AgencyScraperUtils


class HaysScraper(BaseAgencyScraper):
    """Scraper for Hays Netherlands."""

    AGENCY_NAME = "Hays Nederland"
    WEBSITE_URL = "https://www.hays.nl"
    BRAND_GROUP = "Hays plc"
    
    # Pages to scrape with specific functions per page
    PAGES_TO_SCRAPE: List[Dict[str, Any]] = [
        {
            "name": "home",
            "url": "https://www.hays.nl/home",
            "functions": ["logo", "sectors", "services"],
        },
        {
            "name": "contact",
            "url": "https://www.hays.nl/contact",
            "functions": ["contact"],
        },
        {
            "name": "privacy",
            "url": "https://www.hays.nl/herzien-privacybeleid",
            "functions": ["legal"],
        },
        {
            "name": "services",
            "url": "https://www.hays.nl/al-onze-diensten",
            "functions": [],
        },
        {
            "name": "about",
            "url": "https://www.hays.nl/over-hays",
            "functions": [],
        },
    ]

    def scrape(self) -> Agency:
        self.logger.info(f"Starting scrape of {self.AGENCY_NAME}")
        
        # Initialize utils
        self.utils = AgencyScraperUtils(logger=self.logger)
        
        agency = self.create_base_agency()
        agency.geo_focus_type = GeoFocusType.INTERNATIONAL
        agency.employers_page_url = f"{self.WEBSITE_URL}/recruitment/contacteer-ons"
        agency.contact_form_url = f"{self.WEBSITE_URL}/contact"

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
        agency.focus_segments = self._extract_focus_segments(all_text)
        agency.regions_served = self._extract_regions_served(all_text)
        
        # Set volume specialisation
        if "1,000 vacancies" in all_text or "80 consultants" in all_text:
            agency.volume_specialisation = VolumeSpecialisation.POOLS_5_50

        # Extract certifications, CAO, membership
        agency.certifications = self.utils.fetch_certifications(all_text, "accumulated_text")
        agency.cao_type = self.utils.fetch_cao_type(all_text, "accumulated_text")
        agency.membership = self.utils.fetch_membership(all_text, "accumulated_text")

        # Update evidence URLs
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
            
            elif func_name == "sectors":
                sectors = self._extract_sectors(soup, url)
                if sectors:
                    agency.sectors_core = sectors
            
            elif func_name == "services":
                self._extract_services(soup, agency, url)
            
            elif func_name == "contact":
                self._extract_contact(soup, agency, url)
            
            elif func_name == "legal":
                self._extract_legal(page_text, agency, url)

    def _extract_sectors(self, soup: BeautifulSoup, url: str) -> list[str]:
        """Extract specialisms/sectors from page."""
        sectors = []
        seen = set()
        
        # Find the "Onze specialismes" or "Our specialisms" heading
        specialisms_heading = soup.find("h2", string=re.compile(r"(Onze specialismes|Our specialisms)", re.IGNORECASE))
        
        if specialisms_heading:
            parent_box = specialisms_heading.find_parent("div", class_="box")
            if parent_box:
                list_items = parent_box.find_all("li")
                for li in list_items:
                    link = li.find("a")
                    if link:
                        specialism_text = link.get_text(strip=True)
                        if specialism_text and specialism_text.lower() not in seen:
                            sectors.append(specialism_text)
                            seen.add(specialism_text.lower())
                            self.logger.info(f"✓ Found sector: '{specialism_text}' | Source: {url}")
        
        # Fallback: Look for specialism links in recruitment URLs
        if not sectors:
            for link in soup.find_all("a", href=re.compile(r"/recruitment/.*-recruitment")):
                specialism_text = link.get_text(strip=True).replace("►", "").strip()
                if specialism_text and specialism_text.lower() not in seen:
                    sectors.append(specialism_text)
                    seen.add(specialism_text.lower())
                    self.logger.info(f"✓ Found sector: '{specialism_text}' | Source: {url}")
        
        return sectors

    def _extract_services(self, soup: BeautifulSoup, agency: Agency, url: str) -> None:
        """Extract services from page."""
        text_lower = soup.get_text(separator=" ", strip=True).lower()

        # Find services section
        services_heading = soup.find("h2", string=re.compile(r"(All our services|Al onze diensten)", re.IGNORECASE))
        
        if services_heading:
            parent_box = services_heading.find_parent("div", class_="box")
            if parent_box:
                services_text = parent_box.get_text(separator=" ", strip=True).lower()
                
                if "perm recruitment" in services_text or "permanente recruitment" in services_text:
                    agency.services.werving_selectie = True
                    self.logger.info(f"✓ Found service 'werving_selectie' | Source: {url}")
                
                if "flex recruitment" in services_text or "flexibele inhuur" in services_text:
                    agency.services.detacheren = True
                    agency.services.uitzenden = True
                    self.logger.info(f"✓ Found service 'detacheren' | Source: {url}")
                
                if "contracting" in services_text or "freelancer" in services_text:
                    agency.services.zzp_bemiddeling = True
                    self.logger.info(f"✓ Found service 'zzp_bemiddeling' | Source: {url}")
                
                if "recruitment process outsourcing" in services_text or "rpo" in services_text:
                    agency.services.rpo = True
                    self.logger.info(f"✓ Found service 'rpo' | Source: {url}")
                
                if "managed service provider" in services_text or "msp" in services_text:
                    agency.services.msp = True
                    self.logger.info(f"✓ Found service 'msp' | Source: {url}")
        
        # Fallback to general text
        else:
            if "perm recruitment" in text_lower:
                agency.services.werving_selectie = True
            if "flex recruitment" in text_lower or "temporary" in text_lower:
                agency.services.detacheren = True
            if "contracting" in text_lower:
                agency.services.zzp_bemiddeling = True
            if "rpo" in text_lower:
                agency.services.rpo = True
            if "msp" in text_lower:
                agency.services.msp = True
            if "executive search" in text_lower:
                agency.services.executive_search = True

    def _extract_contact(self, soup: BeautifulSoup, agency: Agency, url: str) -> None:
        """Extract contact info from contact page."""
        # Province mapping for Dutch cities
        city_province_map = {
            "amsterdam": "Noord-Holland",
            "tilburg": "Noord-Brabant",
            "rotterdam": "Zuid-Holland",
            "utrecht": "Utrecht",
            "eindhoven": "Noord-Brabant",
            "den haag": "Zuid-Holland",
            "groningen": "Groningen",
        }
        
        # Find offices section
        offices_heading = soup.find("h2", string=re.compile(r"Kantoren|Offices", re.IGNORECASE))
        
        if offices_heading:
            table = offices_heading.find_next("table")
            
            if table:
                # Find all h3 elements (office names)
                office_headers = table.find_all("h3")
                
                for header in office_headers:
                    city_name = header.get_text(strip=True)
                    if city_name:
                        city_name = city_name.replace("\xa0", " ").strip()
                        province = city_province_map.get(city_name.lower(), None)
                        office = OfficeLocation(city=city_name, province=province)
                        if not agency.office_locations:
                            agency.office_locations = []
                        agency.office_locations.append(office)
                        self.logger.info(f"✓ Found office: {city_name} ({province}) | Source: {url}")
                        
                        # First office is HQ
                        if not agency.hq_city:
                            agency.hq_city = city_name
                            agency.hq_province = province
                
                # Extract phone and email from first office
                first_cell = table.find("td")
                if first_cell:
                    cell_text = first_cell.get_text(separator=" ", strip=True)
                    
                    # Extract phone
                    phone_match = re.search(r"(\d{3}[\s-]?\d{2}[\s-]?\d{2}[\s-]?\d{3})", cell_text)
                    if phone_match and not agency.contact_phone:
                        agency.contact_phone = phone_match.group(1)
                        self.logger.info(f"✓ Found phone: {agency.contact_phone} | Source: {url}")
                    
                    # Extract email
                    email_match = re.search(r"([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})", cell_text)
                    if email_match and not agency.contact_email:
                        agency.contact_email = email_match.group(1)
                        self.logger.info(f"✓ Found email: {agency.contact_email} | Source: {url}")

    def _extract_legal(self, page_text: str, agency: Agency, url: str) -> None:
        """Extract KvK and legal name."""
        # KvK number
        if not agency.kvk_number:
            kvk = self.utils.fetch_kvk_number(page_text, url)
            if kvk:
                agency.kvk_number = kvk
        
        # Legal name
        if not agency.legal_name:
            legal_name = self.utils.fetch_legal_name(page_text, "Hays", url)
            if legal_name:
                agency.legal_name = legal_name

    def _extract_focus_segments(self, text: str) -> list[str]:
        """Extract focus segments from text."""
        segments = []
        text_lower = text.lower()
        
        if any(kw in text_lower for kw in ["professional", "specialist", "white collar"]):
            segments.append("white_collar")
        
        if any(kw in text_lower for kw in ["engineering", "technical", "technology", "it"]):
            segments.append("technisch_specialisten")
        
        if any(kw in text_lower for kw in ["finance", "accounting"]):
            segments.append("finance_specialists")
        
        return segments

    def _extract_regions_served(self, text: str) -> list[str]:
        """Extract regions served from text."""
        regions = []
        text_lower = text.lower()
        
        if "netherlands" in text_lower or "nederland" in text_lower:
            regions.append("landelijk")
        
        if "worldwide" in text_lower or "international" in text_lower or "global" in text_lower:
            regions.append("internationaal")
        
        return regions


@dg.asset(group_name="agencies")
def hays_scrape() -> dg.Output[dict]:
    """Scrape Hays Netherlands website."""
    scraper = HaysScraper()
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
