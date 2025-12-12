"""
TMI Netherlands scraper.

Website: https://www.tmi.nl
"""

from __future__ import annotations

from typing import Any, Dict, List, Set
import re

import dagster as dg
from bs4 import BeautifulSoup

from staffing_agency_scraper.models import Agency, GeoFocusType, OfficeLocation
from staffing_agency_scraper.scraping.base import BaseAgencyScraper
from staffing_agency_scraper.scraping.utils import AgencyScraperUtils

class TMIScraper(BaseAgencyScraper):
    """Scraper for TMI Netherlands."""

    AGENCY_NAME = "TMI"
    WEBSITE_URL = "https://www.tmi.nl"
    BRAND_GROUP = "House of HR"
    PAGES_TO_SCRAPE: List[Dict[str, Any]] = [
        {
            "name": "home",
            "url": "https://www.tmi.nl",
            "functions": ['header', 'footer', 'sectors', 'about', 'reviews'],
        },
        {
            "name": "contact",
            "url": "https://www.tmi.nl/over-tmi/contact",
            "functions": ['contact'],
        },
        {
            "name": "privacy",
            "url": "https://www.tmi.nl/privacyverklaring",
            "functions": ['legal'],
        },
        {
            "name": "about",
            "url": "https://www.tmi.nl/over-tmi",
            "functions": ['about'],
        },
    ]

    def scrape(self) -> Agency:
        self.logger.info(f"Starting scrape of {self.AGENCY_NAME}")
        
        # Initialize utils
        self.utils = AgencyScraperUtils(logger=self.logger)
        
        agency = self.create_base_agency()
        agency.geo_focus_type = GeoFocusType.NATIONAL
        agency.employers_page_url = f"{self.WEBSITE_URL}/opdrachtgevers"
        agency.contact_form_url = f"{self.WEBSITE_URL}/over-tmi/contact"
        
        all_sectors = set()
        
        # Add contact form URL to evidence
        if agency.contact_form_url not in self.evidence_urls:
            self.evidence_urls.append(agency.contact_form_url)
        
        # Add founding year to growth signals (from footer: © 2001 - 2025 TMI)
        agency.growth_signals.append("Founded in 2001")
        
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
            if func_name == "header":
                self._extract_header(soup, page_text, agency, url)
            
            elif func_name == "footer":
                self._extract_footer(soup, page_text, agency, url)
            
            elif func_name == "reviews":
                self._extract_reviews(soup, page_text, agency, url)
            
            elif func_name == "sectors":
                self._extract_sectors(soup, page_text, all_sectors, url)
            
            elif func_name == "about":
                self._extract_about(soup, page_text, agency, url)
            
            elif func_name == "contact":
                self._extract_contact(soup, page_text, agency, url)
            
            elif func_name == "legal":
                self._extract_legal(soup, page_text, agency, url)
    
    def _extract_header(
        self, soup: BeautifulSoup, page_text: str, agency: Agency, url: str
    ) -> None:
        """Extract data from header section."""
        self.logger.info(f"🔍 Extracting header data from {url}")
        
        # Find header element
        header = soup.find("header", id="site-header")
        if not header:
            self.logger.warning(f"⚠ Header not found on {url}")
            return
        
        # Extract candidate portal link ("Het Portaal")
        portal_link = header.find("a", href=lambda x: x and "portaal.tmi.nl" in x)
        if portal_link:
            portal_url = portal_link.get("href")
            agency.digital_capabilities.candidate_portal = True
            self.logger.info(f"✓ Candidate portal detected: {portal_url} | Source: {url}")
            
            # Add portal URL to evidence
            if portal_url and portal_url not in self.evidence_urls:
                self.evidence_urls.append(portal_url)
                self.logger.info(f"✓ Added portal URL to evidence | Source: {url}")
        
        # Extract open application URL
        open_app_link = header.find("a", href=lambda x: x and "open-sollicitatie" in x if x else False)
        if open_app_link:
            open_app_url = open_app_link.get("href")
            if open_app_url:
                # Normalize URL
                if not open_app_url.startswith("http"):
                    open_app_url = f"{self.WEBSITE_URL}{open_app_url}" if open_app_url.startswith("/") else f"{self.WEBSITE_URL}/{open_app_url}"
                
                # Add to evidence URLs
                if open_app_url not in self.evidence_urls:
                    self.evidence_urls.append(open_app_url)
                    self.logger.info(f"✓ Open application URL: {open_app_url} | Source: {url}")
        
        # Extract logo (if not already extracted)
        if not agency.logo_url:
            logo = self.utils.fetch_logo(soup, url)
            if logo:
                agency.logo_url = logo
    
    def _extract_reviews(
        self, soup: BeautifulSoup, page_text: str, agency: Agency, url: str
    ) -> None:
        """Extract Google reviews from homepage."""
        self.logger.info(f"🔍 Extracting reviews from {url}")
        
        # Find Google review section
        review_section = soup.find("section", id="section-google-review")
        if not review_section:
            self.logger.warning(f"⚠ Review section not found on {url}")
            return
        
        self.logger.info(f"✓ Google review section found | Source: {url}")
        
        # Check for Google logo/links in the review section
        google_indicators = (
            review_section.find_all("a", href=lambda x: x and "google.com" in x) or
            review_section.find_all("img", alt=lambda x: x and "google" in x.lower() if x else False)
        )
        
        if google_indicators:
            # Add Google as review source
            if not agency.review_sources:
                agency.review_sources = ["Google"]
            elif "Google" not in agency.review_sources:
                agency.review_sources.append("Google")
            self.logger.info(f"✓ Review source: Google | Source: {url}")
            
            # Add growth signal for having visible Google reviews
            signal = "google_reviews_visible"
            if signal not in agency.growth_signals:
                agency.growth_signals.append(signal)
                self.logger.info(f"✓ Growth signal: {signal} | Source: {url}")
        else:
            self.logger.warning(f"⚠ Review section found but no Google indicators | Source: {url}")
    
    def _extract_footer(
        self, soup: BeautifulSoup, page_text: str, agency: Agency, url: str
    ) -> None:
        """Extract data from footer section."""
        self.logger.info(f"🔍 Extracting footer data from {url}")
        
        # Find footer element
        footer = soup.find("footer", id="section-footer")
        if not footer:
            self.logger.warning(f"⚠ Footer not found on {url}")
            return
        
        footer_text = footer.get_text(separator=" ", strip=True)
        
        # Extract contact info from footer
        # Phone: 020 – 717 3527
        phone_match = re.search(r'020\s*[–-]\s*717\s*3527', footer_text)
        if phone_match and not agency.contact_phone:
            agency.contact_phone = "020-7173527"
            self.logger.info(f"✓ Contact phone: {agency.contact_phone} | Source: {url}")
        
        # Email: info@tmi.nl
        email_match = re.search(r'info@tmi\.nl', footer_text)
        if email_match and not agency.contact_email:
            agency.contact_email = "info@tmi.nl"
            self.logger.info(f"✓ Contact email: {agency.contact_email} | Source: {url}")
        
        # HQ Address: Processorstraat 12, 1033 NZ Amsterdam
        address_match = re.search(r'Processorstraat\s+12', footer_text, re.IGNORECASE)
        if address_match and not agency.hq_city:
            agency.hq_city = "Amsterdam"
            agency.hq_province = self.utils.map_city_to_province("Amsterdam")
            self.logger.info(f"✓ HQ: {agency.hq_city}, {agency.hq_province} (Processorstraat 12, 1033 NZ) | Source: {url}")
            
            # Create office location for HQ
            hq_office = OfficeLocation(
                city="Amsterdam",
                province=agency.hq_province,
            )
            agency.office_locations = [hq_office]
        
        # Extract certifications from footer images (iso-logo divs)
        # Certification mapping based on alt text and filename keywords
        cert_mappings = {
            "crkbo": "CRKBO",
            "iso 9001": "ISO 9001",
            "iso 14001": "ISO 14001",
            "iso 27001": "ISO 27001",
            "sna": "SNA",
            "abu": "ABU",
            "nbbu": "NBBU",
            "tuv": "TÜV",
            "tr-testmark": "TÜV Rheinland ISO",
        }
        
        # Find certification images in iso-logo divs
        iso_logo_divs = footer.find_all("div", class_="iso-logo")
        for div in iso_logo_divs:
            img = div.find("img")
            if img:
                alt_text = img.get("alt", "").strip().lower()
                src = img.get("src", "").lower()
                
                # Check alt text first
                if alt_text:
                    for keyword, cert_name in cert_mappings.items():
                        if keyword in alt_text:
                            if cert_name not in agency.certifications:
                                agency.certifications.append(cert_name)
                                self.logger.info(f"✓ Certification: {cert_name} (from alt text) | Source: {url}")
                                break
                # If no alt text, check filename
                elif src:
                    for keyword, cert_name in cert_mappings.items():
                        if keyword in src:
                            if cert_name not in agency.certifications:
                                agency.certifications.append(cert_name)
                                self.logger.info(f"✓ Certification: {cert_name} (from image filename) | Source: {url}")
                                break
        
        # Extract logo
        logo = self.utils.fetch_logo(soup, url)
        if logo and not agency.logo_url:
            agency.logo_url = logo
    
    def _extract_sectors(
        self, soup: BeautifulSoup, page_text: str, all_sectors: Set[str], url: str
    ) -> None:
        """Extract healthcare sectors from navigation menu."""
        self.logger.info(f"🔍 Extracting sectors from {url}")
        
        # TMI specializes in healthcare sectors
        # Core healthcare sectors for TMI
        tmi_sectors = [
            "Ambulancezorg",
            "Ziekenhuizen",
            "GGZ",
            "VVT",
            "Kinderopvang",
            "Jeugdzorg",
            "Gehandicaptenzorg",
            "Apotheken",
            "Huisartsenzorg",
            "Sociaal werk",
            "GGD",
            "Privéklinieken",
            "Verzekeringsgeneeskunde",
            "Arbo dienstverlening",
        ]
        
        # Add all TMI core sectors
        for sector in tmi_sectors:
            all_sectors.add(sector)
            self.logger.info(f"✓ Added core sector: '{sector}' | Source: TMI healthcare specialization")
        
        # Also extract any additional sectors from links
        for link in soup.find_all("a"):
            text = link.get_text(strip=True)
            healthcare_keywords = [
                "ambulancezorg", "ziekenhuis", "ggz", "vvt", 
                "kinderopvang", "jeugdzorg", "gehandicaptenzorg",
                "apotheek", "huisartsenzorg", "sociaal werk",
                "paramedici", "verpleegkundige", "zorg", "anios",
                "operatiekamer", "medisch specialist", "ggd", "arbo"
            ]
            
            # Only add clean sector names (reasonable length, not concatenated)
            if (any(kw in text.lower() for kw in healthcare_keywords) and 
                5 < len(text) < 50 and 
                text not in all_sectors and
                not any(char.isupper() and text[i-1].islower() for i, char in enumerate(text) if i > 0)):  # Detect camelCase concatenation
                all_sectors.add(text)
                self.logger.info(f"✓ Found additional sector: '{text}' | Source: {url}")
    
    def _extract_about(
        self, soup: BeautifulSoup, page_text: str, agency: Agency, url: str
    ) -> None:
        """Extract about/mission information."""
        self.logger.info(f"🔍 Extracting about information from {url}")
        
        # Extract services from page text
        services = self.utils.fetch_services(page_text, url)
        if services:
            agency.services = services
        
        # Extract growth signals and append (don't replace)
        growth_signals = self.utils.fetch_growth_signals(page_text, url)
        if growth_signals:
            for signal in growth_signals:
                if signal not in agency.growth_signals:
                    agency.growth_signals.append(signal)
        
        # Extract certifications
        certifications = self.utils.fetch_certifications(page_text, url)
        if certifications:
            for cert in certifications:
                if cert not in agency.certifications:
                    agency.certifications.append(cert)
    
    def _extract_contact(
        self, soup: BeautifulSoup, page_text: str, agency: Agency, url: str
    ) -> None:
        """Extract contact information from contact page."""
        self.logger.info(f"🔍 Extracting contact information from {url}")
        
        # Extract KvK number
        kvk_match = re.search(r'(?:KVK|Chamber of Commerce).*?nr\.?\s*(\d{3}\s*\d{3}\s*\d{2})', page_text, re.IGNORECASE)
        if kvk_match:
            kvk_raw = kvk_match.group(1)
            agency.kvk_number = kvk_raw.replace(' ', '')
            self.logger.info(f"✓ KvK number: {agency.kvk_number} | Source: {url}")
        
        # Extract additional office locations
        contact_section = soup.find("section", id="section-contactinfo")
        if contact_section:
            # Look for Groningen office
            if "groningen" in page_text.lower():
                # Check if Groningen is already in office locations
                groningen_exists = any(office.city and office.city.lower() == "groningen" 
                                      for office in agency.office_locations)
                if not groningen_exists:
                    groningen_office = OfficeLocation(
                        city="Groningen",
                        province=self.utils.map_city_to_province("Groningen"),
                    )
                    agency.office_locations.append(groningen_office)
                    self.logger.info(f"✓ Additional office: Groningen | Source: {url}")
        
        # Extract phone if not already set
        if not agency.contact_phone:
            phone = self.utils.fetch_contact_phone(page_text, url)
            if phone:
                agency.contact_phone = phone
        
        # Extract email if not already set
        if not agency.contact_email:
            email = self.utils.fetch_contact_email(page_text, url)
            if email:
                agency.contact_email = email
        
        # Check for 24/7 or flexible availability from flex desk info
        if "buiten kantooruren" in page_text.lower() or "during and outside office hours" in page_text.lower():
            signal = "24_7_beschikbaarheid"
            if signal not in agency.growth_signals:
                agency.growth_signals.append(signal)
                self.logger.info(f"✓ Growth signal: {signal} (flex desk available outside office hours) | Source: {url}")
    
    def _extract_legal(
        self, soup: BeautifulSoup, page_text: str, agency: Agency, url: str
    ) -> None:
        """Extract legal information from privacy/terms page."""
        self.logger.info(f"🔍 Extracting legal information from {url}")
        
        # Extract KvK number
        kvk = self.utils.fetch_kvk_number(page_text, url)
        if kvk:
            agency.kvk_number = kvk
        
        # Extract legal name
        legal_name = self.utils.fetch_legal_name(page_text, "TMI", url)
        if legal_name:
            agency.legal_name = legal_name


@dg.asset(group_name="agencies")
def tmi_scrape() -> dg.Output[dict]:
    """Scrape Tmi website."""
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

