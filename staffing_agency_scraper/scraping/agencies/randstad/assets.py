"""
Randstad Netherlands scraper.

Website: https://www.randstad.nl
Part of: Randstad Groep Nederland"""

from __future__ import annotations

import re
from typing import Any, Dict, List

import dagster as dg
from bs4 import BeautifulSoup

from staffing_agency_scraper.models import Agency, GeoFocusType
from staffing_agency_scraper.scraping.base import BaseAgencyScraper
from staffing_agency_scraper.scraping.utils import AgencyScraperUtils

class RandstadScraper(BaseAgencyScraper):
    """Scraper for Randstad Netherlands."""

    AGENCY_NAME = "Randstad"
    WEBSITE_URL = "https://www.randstad.nl"
    BRAND_GROUP = "Randstad Groep Nederland"
    PAGES_TO_SCRAPE: List[Dict[str, Any]] = [
        {
            "name": "home",
            "url": "https://www.randstad.nl",
            "functions": ['logo', 'header'],
        },
        {
            "name": "services_global",
            "url": "https://www.randstad.com/services/",
            "functions": ['services_detail', 'legal_footer'],
        },
        {
            "name": "about_glance",
            "url": "https://www.randstad.com/randstad-at-a-glance/",
            "functions": ['growth_stats'],
        },
        {
            "name": "werkgevers",
            "url": "https://www.randstad.nl/werkgevers",
            "functions": ['sectors'],
        },
        {
            "name": "contact",
            "url": "https://www.randstad.nl/over-randstad/contact",
            "functions": ['contact', 'hq'],
        },
        {
            "name": "certificering",
            "url": "https://www.randstad.nl/over-randstad/over-ons-bedrijf/certificering-randstad",
            "functions": ['certifications'],
        },
        {
            "name": "disclaimer",
            "url": "https://www.randstad.nl/over-randstad/disclaimer",
            "functions": ['legal'],
        },
    ]

    def scrape(self) -> Agency:
        self.logger.info(f"Starting scrape of {self.AGENCY_NAME}")
        
        # Initialize utils
        self.utils = AgencyScraperUtils(logger=self.logger)
        agency = self.create_base_agency()
        agency.geo_focus_type = GeoFocusType.NATIONAL
        agency.employers_page_url = f"{self.WEBSITE_URL}/werkgevers"
        agency.contact_form_url = f"{self.WEBSITE_URL}/contact-randstad"
        
        all_sectors = set()
        page_texts = {}  # Store page texts for common field extraction
        
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
                page_texts[url] = page_text
                
                # Apply custom functions
                self._apply_functions(agency, functions, soup, page_text, all_sectors, url)
                
                self.logger.info(f"✅ Completed: {page_name}")
                
            except Exception as e:
                self.logger.error(f"❌ Error scraping {url}: {e}")
        
        # Extract all common fields using centralized utilities
        self.extract_all_common_fields(agency, page_texts)
        
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
        all_sectors,
        url: str,
    ) -> None:
        """Apply BS4/regex extraction functions."""
        for func_name in functions:
            if func_name == "logo":
                logo = self.utils.fetch_logo(soup, url)
                if logo:
                    agency.logo_url = logo
                    self.logger.info(f"✓ Found logo: {logo} | Source: {url}")
            
            elif func_name == "header":
                self._extract_header(soup, agency, url)
            
            elif func_name == "services":
                self._extract_services(soup, page_text, agency, url)
            
            elif func_name == "services_detail":
                self._extract_services_detail(soup, agency, url)
            
            elif func_name == "legal_footer":
                self._extract_legal_footer(soup, agency, url)
            
            elif func_name == "growth_stats":
                self._extract_growth_stats(soup, agency, url)
            
            elif func_name == "contact":
                self._extract_contact(soup, page_text, agency, url)
            
            elif func_name == "hq":
                self._extract_hq(soup, agency, url)
            
            elif func_name == "certifications":
                self._extract_certifications(soup, page_text, agency, url)
            
            elif func_name == "legal":
                kvk = self.utils.fetch_kvk_number(page_text, url)
                legal_name = self.utils.fetch_legal_name(page_text, "Randstad", url)
                if kvk:
                    agency.kvk_number = kvk
                if legal_name:
                    agency.legal_name = legal_name
            
            elif func_name == "sectors":
                self._extract_sectors(soup, all_sectors, url)
    
    def _extract_header(self, soup: BeautifulSoup, agency: Agency, url: str) -> None:
        """Extract data from header navigation."""
        # Look for navigation menu
        nav = soup.find("nav")
        if nav:
            # Extract mobile app presence
            if nav.find("a", href=lambda x: x and ("app.apple.com" in x or "play.google.com" in x)):
                agency.digital_capabilities.mobile_app = True
                self.logger.info(f"✓ Found mobile app links | Source: {url}")
    
    def _extract_services(self, soup: BeautifulSoup, page_text: str, agency: Agency, url: str) -> None:
        """Extract services from werkgevers page."""
        # Use utils for initial detection
        services = self.utils.fetch_services(page_text, url)
        agency.services = services
        
        # Check for enterprise services (MSP, RPO)
        if "msp" in page_text.lower() or "managed service" in page_text.lower():
            agency.services.msp = True
            self.logger.info(f"✓ Found MSP service | Source: {url}")
        
        if "rpo" in page_text.lower() or "recruitment process outsourcing" in page_text.lower():
            agency.services.rpo = True
            self.logger.info(f"✓ Found RPO service | Source: {url}")
        
        if "outplacement" in page_text.lower():
            agency.services.reintegratie_outplacement = True
            self.logger.info(f"✓ Found outplacement service | Source: {url}")
    
    def _extract_services_detail(self, soup: BeautifulSoup, agency: Agency, url: str) -> None:
        """
        Extract services from https://www.randstad.com/services/ page.
        
        Services listed:
        - temporary staffing
        - flexible to permanent staffing
        - permanent recruitment
        - HR support
        - workforce management
        - payrolling
        """
        # Find the "go directly to" section
        link_list = soup.find("ul", class_="link-list")
        if not link_list:
            return
        
        services_found = []
        for link in link_list.find_all("a", class_="link-list__link"):
            service_text = link.get_text(strip=True).lower()
            
            if "temporary staffing" in service_text or "temp" in service_text:
                agency.services.uitzenden = True
                services_found.append("temporary staffing")
            
            if "flexible to permanent" in service_text or "flex" in service_text:
                agency.services.detacheren = True
                services_found.append("flexible to permanent")
            
            if "permanent recruitment" in service_text or "perm" in service_text:
                agency.services.werving_selectie = True
                services_found.append("permanent recruitment")
            
            if "payrolling" in service_text:
                agency.services.payrolling = True
                services_found.append("payrolling")
            
            if "workforce management" in service_text:
                agency.services.inhouse_services = True
                services_found.append("workforce management")
        
        if services_found:
            self.logger.info(f"✓ Found {len(services_found)} services: {', '.join(services_found)} | Source: {url}")
    
    def _extract_legal_footer(self, soup: BeautifulSoup, agency: Agency, url: str) -> None:
        """
        Extract legal info from footer of https://www.randstad.com/services/.
        
        Expected format:
        - Randstad N.V.
        - Registered in The Netherlands No: 33216172
        - Registered office: Diemermere 25, 1112 TC Diemen, The Netherlands
        """
        # Find footer
        footer = soup.find("footer")
        if not footer:
            return
        
        footer_text = footer.get_text(separator=" ", strip=True)
        
        # Extract KVK number: "Registered in The Netherlands No: 33216172"
        kvk_match = re.search(r'(?:Registered|Registration).*?(?:No|Number)[\s:]*(\d{8})', footer_text)
        if kvk_match:
            agency.kvk_number = kvk_match.group(1)
            self.logger.info(f"✓ Found KVK number: {agency.kvk_number} | Source: {url}")
        
        # Extract legal name: "Randstad N.V."
        if "randstad n.v." in footer_text.lower():
            agency.legal_name = "Randstad N.V."
            self.logger.info(f"✓ Found legal name: Randstad N.V. | Source: {url}")
        
        # Extract registered office address
        if "diemermere 25" in footer_text.lower() and "diemen" in footer_text.lower():
            if not agency.hq_city:
                agency.hq_city = "Diemen"
                agency.hq_province = "Noord-Holland"
                self.logger.info(f"✓ Confirmed HQ: Diemen, Noord-Holland | Source: {url}")
    
    def _extract_growth_stats(self, soup: BeautifulSoup, agency: Agency, url: str) -> None:
        """
        Extract growth signals and stats from https://www.randstad.com/randstad-at-a-glance/.
        
        Key data:
        - "operates in 39 markets"
        - "approximately 40,000 employees"
        - "supported over 1.7 million talent"
        - "revenue of €24.1 billion"
        - "In 1960, Frits Goldschmeding, the founder..."
        """
        page_text = soup.get_text(separator=" ", strip=True)
        
        if not agency.growth_signals:
            agency.growth_signals = []
        
        # Extract number of markets
        markets_match = re.search(r'operates in (\d+) markets', page_text, re.IGNORECASE)
        if markets_match:
            markets = int(markets_match.group(1))
            signal = f"actief_in_{markets}_landen"
            if signal not in agency.growth_signals:
                agency.growth_signals.append(signal)
            self.logger.info(f"✓ Found global presence: {markets} markets | Source: {url}")
        
        # Extract employee count (global)
        employees_match = re.search(r'approximately ([\d,]+) employees', page_text, re.IGNORECASE)
        if employees_match:
            employees_str = employees_match.group(1).replace(',', '')
            employees = int(employees_str)
            signal = f"{employees}_medewerkers_wereldwijd"
            if signal not in agency.growth_signals:
                agency.growth_signals.append(signal)
            self.logger.info(f"✓ Found employee count: {employees:,} employees worldwide | Source: {url}")
        
        # Extract annual placements (global - NOT Dutch specific!)
        placements_match = re.search(r'supported over ([\d.]+) million talent', page_text, re.IGNORECASE)
        if placements_match:
            placements_millions = float(placements_match.group(1))
            placements = int(placements_millions * 1_000_000)
            signal = f"{placements}_plaatsingen_per_jaar_wereldwijd"
            if signal not in agency.growth_signals:
                agency.growth_signals.append(signal)
            self.logger.info(f"✓ Found annual placements (global): {placements:,} | Source: {url}")
        
        # Extract revenue
        revenue_match = re.search(r'revenue of €([\d.]+) billion', page_text, re.IGNORECASE)
        if revenue_match:
            revenue = float(revenue_match.group(1))
            signal = f"omzet_{int(revenue)}_miljard_euro"
            if signal not in agency.growth_signals:
                agency.growth_signals.append(signal)
            self.logger.info(f"✓ Found revenue: €{revenue} billion | Source: {url}")
        
        # Extract founding year
        year_match = re.search(r'In 1960.*?founder', page_text, re.IGNORECASE)
        if year_match:
            signal = "sinds_1960_actief"
            if signal not in agency.growth_signals:
                agency.growth_signals.append(signal)
            self.logger.info(f"✓ Found founding year: 1960 (64+ years active) | Source: {url}")
    
    def _extract_contact(self, soup: BeautifulSoup, page_text: str, agency: Agency, url: str) -> None:
        """Extract contact information."""
        email = self.utils.fetch_contact_email(page_text, url)
        phone = self.utils.fetch_contact_phone(page_text, url)
        offices = self.utils.fetch_office_locations(soup, url)
        
        if email:
            agency.contact_email = email
        if phone:
            agency.contact_phone = phone
        if offices:
            agency.office_locations = offices
    
    def _extract_hq(self, soup: BeautifulSoup, agency: Agency, url: str) -> None:
        """
        Extract HQ information from contact page.
        
        Expected format:
        - Address: Diemermere 25, NL-1112 TC Diemen
        - P.O. Box: P.O. Box 12600, NL-1100 AP Amsterdam
        - Phone: +31 (0)20 569 5911
        """
        # Look for "head office" or "hoofdkantoor" section
        page_text = soup.get_text(separator=" ", strip=True)
        
        # Extract phone
        phone_match = re.search(r'T\s*\+31\s*\(0\)20\s*569\s*5911', page_text)
        if phone_match and not agency.contact_phone:
            agency.contact_phone = "+31 (0)20 569 5911"
            self.logger.info(f"✓ Found HQ phone: +31 (0)20 569 5911 | Source: {url}")
        
        # Extract city (Diemen or Amsterdam)
        if "diemen" in page_text.lower():
            agency.hq_city = "Diemen"
            agency.hq_province = "Noord-Holland"
            self.logger.info(f"✓ Found HQ city: Diemen, Noord-Holland | Source: {url}")
        elif "amsterdam" in page_text.lower() and not agency.hq_city:
            agency.hq_city = "Amsterdam"
            agency.hq_province = "Noord-Holland"
            self.logger.info(f"✓ Found HQ city: Amsterdam, Noord-Holland | Source: {url}")
    
    def _extract_certifications(self, soup: BeautifulSoup, page_text: str, agency: Agency, url: str) -> None:
        """Extract certifications from certification page."""
        # Use utils method
        page_texts = {url: page_text}
        certs = self.utils.fetch_certifications(page_texts, url)
        if certs:
            agency.certifications = certs
    
    def _extract_sectors(self, soup: BeautifulSoup, all_sectors, url: str) -> None:
        """Extract sectors from werkgevers page."""
        # Look for sector links
        for link in soup.find_all("a"):
            text = link.get_text(strip=True)
            if text and len(text) > 3 and len(text) < 50:
                # Common sector keywords
                sector_keywords = [
                    "logistiek", "zorg", "horeca", "retail", "productie", 
                    "administratie", "techniek", "bouw", "it", "finance",
                    "engineering", "legal", "marketing", "sales"
                ]
                if any(kw in text.lower() for kw in sector_keywords):
                    all_sectors.add(text)
                    self.logger.info(f"✓ Found sector: '{text}' | Source: {url}")


@dg.asset(group_name="agencies")
def randstad_scrape() -> dg.Output[dict]:
    """Scrape Randstad Netherlands website."""
    scraper = RandstadScraper()
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
