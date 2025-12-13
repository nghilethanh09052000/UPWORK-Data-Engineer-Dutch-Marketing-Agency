"""
Randstad Netherlands scraper.

Website: https://www.randstad.nl
Part of: Randstad Groep Nederland"""

from __future__ import annotations

import json
import re
from typing import Any, Dict, List

import dagster as dg
import requests
from bs4 import BeautifulSoup

from staffing_agency_scraper.models import Agency, GeoFocusType, OfficeLocation
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
            "functions": ['services_detail', 'statistics', 'growth_signals'],
        },
        {
            "name": "vakgebieden",
            "url": "https://www.randstad.nl/werkgevers/onze-hr-diensten/personeel-vakgebied#onze-vakgebieden",
            "functions": ['sectors'],
        },
        {
            "name": "contact",
            "url": "https://www.randstad.nl/over-randstad/contact",
            "functions": ['contact', 'hq'],
        },
        {
            "name": "pers_contact",
            "url": "https://www.randstad.nl/over-randstad/pers/contact",
            "functions": ['press_contact'],
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
        {
            "name": "vestigingen",
            "url": "https://www.randstad.nl/vestigingen",
            "functions": ['office_locations'],
        },
        {
            "name": "vacatures",
            "url": "https://www.randstad.nl/vacatures",
            "functions": ['sectors_secondary'],
        },
        {
            "name": "werken_bij",
            "url": "https://www.werkenbijrandstad.nl/",
            "functions": ['contact_werken_bij'],
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
        
        all_sectors = set()  # For sectors_core
        all_sectors_secondary = set()  # For sectors_secondary
        all_text = ""  # Accumulate text from all pages for utils extraction
        
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
                all_text += " " + page_text
                
                # Apply custom functions
                self._apply_functions(agency, functions, soup, page_text, all_sectors, all_sectors_secondary, url)
                
                self.logger.info(f"✅ Completed: {page_name}")
                
            except Exception as e:
                self.logger.error(f"❌ Error scraping {url}: {e}")
        
        # Extract all common fields using centralized utilities
        self.extract_all_common_fields(agency, all_text)
        
        # Check AI capabilities via Seamly API
        self._check_ai_capabilities(agency)
        
        # Set candidate portal (mijn-randstad login)
        candidate_portal_url = "https://www.randstad.nl/mijn-randstad"
        agency.digital_capabilities.candidate_portal = True
        self.evidence_urls.append(candidate_portal_url)
        self.logger.info(f"✓ Candidate portal detected: {candidate_portal_url}")
        
        # Finalize sectors
        if all_sectors:
            agency.sectors_core = sorted(list(all_sectors))
        
        # Filter sectors_secondary: exclude any sectors that are in sectors_core
        # Normalize for comparison (case-insensitive)
        if all_sectors_secondary:
            sectors_core_lower = {s.lower() for s in all_sectors}
            sectors_secondary_filtered = [
                s for s in all_sectors_secondary 
                if s.lower() not in sectors_core_lower
            ]
            if sectors_secondary_filtered:
                agency.sectors_secondary = sorted(sectors_secondary_filtered)
                self.logger.info(f"✓ Filtered {len(sectors_secondary_filtered)} secondary sectors (excluded {len(all_sectors_secondary) - len(sectors_secondary_filtered)} that are in sectors_core)")
        
        agency.evidence_urls = list(self.evidence_urls)
        agency.collected_at = self.collected_at
        
        self.logger.info("=" * 80)
        self.logger.info(f"✅ Completed scrape of {self.AGENCY_NAME}")
        self.logger.info(f"📄 Evidence URLs: {len(agency.evidence_urls)}")
        self.logger.info("=" * 80)

        with open('all_text.txt', 'w') as f:
            f.write(all_text)
        
        return agency
    
    def _apply_functions(
        self,
        agency: Agency,
        functions: List[str],
        soup: BeautifulSoup,
        page_text: str,
        all_sectors,
        all_sectors_secondary,
        url: str,
    ) -> None:
        """Apply BS4/regex extraction functions."""
        for func_name in functions:
            if func_name == "logo":
                logo = self._extract_logo(soup, url)
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
            
            elif func_name == "contact_werken_bij":
                self._extract_contact_werken_bij(soup, agency, url)
            
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
            
            elif func_name == "sectors_secondary":
                self._extract_sectors_secondary(soup, all_sectors_secondary, url)
            
            elif func_name == "office_locations":
                self._extract_office_locations(soup, agency, url)
            
            elif func_name == "statistics":
                self._extract_statistics(soup, page_text, agency, url)
            
            elif func_name == "growth_signals":
                self._extract_growth_signals_from_werkgevers(soup, page_text, agency, url)
            
            elif func_name == "press_contact":
                self._extract_press_contact(soup, agency, url)
    
    def _extract_header(self, soup: BeautifulSoup, agency: Agency, url: str) -> None:
        """Extract data from header navigation."""
        # Look for navigation menu
        nav = soup.find("nav")
        if nav:
            # Extract mobile app presence
            if nav.find("a", href=lambda x: x and ("app.apple.com" in x or "play.google.com" in x)):
                agency.digital_capabilities.mobile_app = True
                self.logger.info(f"✓ Found mobile app links | Source: {url}")
    
    def _extract_logo(self, soup: BeautifulSoup, url: str) -> str | None:
        """
        Extract logo URL from JSON-LD schema or fall back to utils method.
        """
        # First, try to extract from JSON-LD schema
        scripts = soup.find_all("script", type="application/ld+json")
        for script in scripts:
            try:
                script_content = script.string if script.string else script.get_text()
                if not script_content:
                    continue
                
                data = json.loads(script_content)
                
                # Check if it's a Corporation or Organization schema
                if isinstance(data, dict) and data.get("@type") in ["Corporation", "Organization"]:
                    logo = data.get("logo")
                    if logo:
                        # Handle both string and dict formats
                        if isinstance(logo, str):
                            self.logger.info(f"✓ Found logo from JSON-LD: {logo} | Source: {url}")
                            return logo
                        elif isinstance(logo, dict) and "@id" in logo:
                            self.logger.info(f"✓ Found logo from JSON-LD: {logo['@id']} | Source: {url}")
                            return logo["@id"]
                        elif isinstance(logo, dict) and "url" in logo:
                            self.logger.info(f"✓ Found logo from JSON-LD: {logo['url']} | Source: {url}")
                            return logo["url"]
            
            except (json.JSONDecodeError, ValueError, KeyError) as e:
                # Continue to next script if this one fails
                continue
        
        # Fall back to utils method
        return self.utils.fetch_logo(soup, url)
    
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
        Extract services from services page or werkgevers page.
        
        Services listed:
        - temporary staffing / uitzenden
        - flexible to permanent staffing / detacheren
        - permanent recruitment / werving en selectie
        - HR support
        - workforce management
        - payrolling
        - RPO, MSP
        """
        page_text = soup.get_text(separator=" ", strip=True).lower()
        services_found = []
        
        # Try to find services from link list (services page)
        link_list = soup.find("ul", class_="link-list")
        if link_list:
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
        
        # Also extract from page text (werkgevers page)
        # Check for Dutch service terms
        if "uitzenden" in page_text or "temporary" in page_text or "tijdelijk" in page_text:
            agency.services.uitzenden = True
            if "uitzenden" not in [s.lower() for s in services_found]:
                services_found.append("uitzenden")
        
        if "detacheren" in page_text or "detachering" in page_text:
            agency.services.detacheren = True
            if "detacheren" not in [s.lower() for s in services_found]:
                services_found.append("detacheren")
        
        if "werving" in page_text and "selectie" in page_text:
            agency.services.werving_selectie = True
            if "werving en selectie" not in [s.lower() for s in services_found]:
                services_found.append("werving en selectie")
        
        if "payroll" in page_text:
            agency.services.payrolling = True
            if "payrolling" not in [s.lower() for s in services_found]:
                services_found.append("payrolling")
        
        # Check for RPO and MSP
        if "rpo" in page_text or "recruitment process outsourcing" in page_text:
            agency.services.rpo = True
            services_found.append("RPO")
        
        if "msp" in page_text or "managed service provider" in page_text:
            agency.services.msp = True
            services_found.append("MSP")
        
        # Check for outplacement
        if "outplacement" in page_text:
            agency.services.reintegratie_outplacement = True
            services_found.append("outplacement")
        
        # Check for training/development
        if "opleiden" in page_text or "ontwikkelen" in page_text or "training" in page_text:
            agency.services.opleiden_ontwikkelen = True
            services_found.append("opleiden/ontwikkelen")
        
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
    
    def _extract_contact_werken_bij(self, soup: BeautifulSoup, agency: Agency, url: str) -> None:
        """
        Extract contact email from the werken bij Randstad page.
        This email replaces any existing contact email.
        """
        self.logger.info(f"🔍 Extracting contact email from werken bij page: {url}")
        
        # Find the footer column with contact information
        footer_column = soup.find("div", id="footercolumn")
        if not footer_column:
            self.logger.warning(f"Could not find footercolumn on {url}")
            return
        
        # Find mailto link in the footer column
        mailto_link = footer_column.find("a", href=re.compile(r'^mailto:'))
        if mailto_link:
            email = mailto_link.get("href", "").replace("mailto:", "").strip()
            if email:
                # Replace existing email (not just set if empty)
                agency.contact_email = email
                self.evidence_urls.append(url)
                self.logger.info(f"✓ Found contact email from werken bij page: {email} | Source: {url}")
            else:
                self.logger.warning(f"Found mailto link but email is empty on {url}")
        else:
            # Fallback: try to extract from text
            footer_text = footer_column.get_text()
            email_match = re.search(r'info@werkenbijrandstad\.nl', footer_text, re.IGNORECASE)
            if email_match:
                email = "info@werkenbijrandstad.nl"
                agency.contact_email = email
                self.evidence_urls.append(url)
                self.logger.info(f"✓ Found contact email from werken bij page (text): {email} | Source: {url}")
            else:
                self.logger.warning(f"Could not find contact email on {url}")
    
    def _extract_press_contact(self, soup: BeautifulSoup, agency: Agency, url: str) -> None:
        """
        Extract press contact information from the press contact page.
        Extracts press email, press phone, and general contact information.
        """
        self.logger.info(f"🔍 Extracting press contact information from {url}")
        
        # Find the article content
        article = soup.find("article")
        if not article:
            self.logger.warning(f"Could not find article on {url}")
            return
        
        article_text = article.get_text(separator=" ", strip=True)
        
        # Extract press contact email
        press_email_match = re.search(r'pers@randstadgroep\.nl', article_text, re.IGNORECASE)
        if press_email_match:
            press_email = "pers@randstadgroep.nl"
            # Store in a field if available, or add to growth_signals/notes
            if not agency.contact_email:
                agency.contact_email = press_email
            self.logger.info(f"✓ Found press email: {press_email} | Source: {url}")
        
        # Extract press contact phone
        press_phone_match = re.search(r'06[-.\s]?57090598|0657090598', article_text)
        if press_phone_match:
            press_phone = "06-57090598"
            # Store in a field if available, or add to notes
            if not agency.contact_phone:
                agency.contact_phone = press_phone
            self.logger.info(f"✓ Found press phone: {press_phone} | Source: {url}")
        
        # Extract general contact email
        general_email_match = re.search(r'info@nl\.randstad\.com', article_text, re.IGNORECASE)
        if general_email_match:
            general_email = "info@nl.randstad.com"
            # Use as primary contact email if not already set
            if not agency.contact_email:
                agency.contact_email = general_email
            self.logger.info(f"✓ Found general email: {general_email} | Source: {url}")
        
        # Extract general contact phone
        general_phone_match = re.search(r'020[-.\s]?5208800|0205208800', article_text)
        if general_phone_match:
            general_phone = "020-5208800"
            # Use as primary contact phone if not already set
            if not agency.contact_phone:
                agency.contact_phone = general_phone
            self.logger.info(f"✓ Found general phone: {general_phone} | Source: {url}")
        
        # Also try to extract from mailto and tel links
        mailto_links = soup.find_all("a", href=re.compile(r'^mailto:'))
        for link in mailto_links:
            email = link.get("href", "").replace("mailto:", "").strip()
            if email:
                if not agency.contact_email:
                    agency.contact_email = email
                    self.logger.info(f"✓ Found email from link: {email} | Source: {url}")
        
        tel_links = soup.find_all("a", href=re.compile(r'^tel:'))
        for link in tel_links:
            phone = link.get("href", "").replace("tel:", "").strip()
            if phone:
                # Format phone number
                phone = phone.replace("-", "-").replace(".", "-")
                if not agency.contact_phone:
                    agency.contact_phone = phone
                    self.logger.info(f"✓ Found phone from link: {phone} | Source: {url}")
        
        self.logger.info(f"✓ Press contact extraction completed | Source: {url}")
    
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
        """
        Extract certifications from certification page FAQ items.
        Extracts certification names from FAQ question titles.
        """
        self.logger.info(f"🔍 Extracting certifications from {url}")
        
        # Find all FAQ items
        faq_items = soup.find_all("li", class_="faq__item")
        if not faq_items:
            # Fallback to utils method if FAQ structure not found
            page_texts = {url: page_text}
            certs = self.utils.fetch_certifications(page_texts, url)
            if certs:
                agency.certifications = certs
            return
        
        certs = []
        cert_mapping = {
            "abu": "ABU",
            "iso 9001": "ISO 9001",
            "iso9001": "ISO 9001",
            "iso 14001": "ISO 14001",
            "iso14001": "ISO 14001",
            "iso 26000": "ISO 26000",
            "iso26000": "ISO 26000",
            "nen-iso 26000": "ISO 26000",
            "iso 27001": "ISO 27001",
            "iso27001": "ISO 27001",
            "iso 27701": "ISO 27701",
            "iso27701": "ISO 27701",
            "vcu": "VCU",
            "nen-4400": "NEN-4400-1",
        }
        
        for faq_item in faq_items:
            try:
                # Find the question title
                question_div = faq_item.find("div", class_="faq__question")
                if not question_div:
                    continue
                
                # Get text from font tag or directly from question div
                font_tag = question_div.find("font")
                if font_tag:
                    cert_text = font_tag.get_text(strip=True)
                else:
                    cert_text = question_div.get_text(strip=True)
                
                if not cert_text:
                    continue
                
                cert_text_lower = cert_text.lower()
                
                # Map certification names
                matched_cert = None
                for keyword, cert_name in cert_mapping.items():
                    if keyword in cert_text_lower:
                        matched_cert = cert_name
                        break
                
                # Also check for ABU membership (mentioned in text)
                if "abu" in cert_text_lower and "ABU" not in certs:
                    matched_cert = "ABU"
                
                if matched_cert and matched_cert not in certs:
                    certs.append(matched_cert)
                    self.logger.info(f"  ✓ Certification: {matched_cert} | Source: {url}")
            
            except Exception as e:
                self.logger.error(f"Error processing FAQ item on {url}: {e}")
                continue
        
        # Also use utils method as fallback to catch any missed certifications
        page_texts = {url: page_text}
        utils_certs = self.utils.fetch_certifications(page_texts, url)
        for cert in utils_certs:
            if cert not in certs:
                certs.append(cert)
                self.logger.info(f"  ✓ Certification (from utils): {cert} | Source: {url}")
        
        if certs:
            agency.certifications = certs
            self.logger.info(f"✓ Total certifications extracted: {len(certs)} | Source: {url}")
    
    
    def _extract_sectors(self, soup: BeautifulSoup, all_sectors, url: str) -> None:
        """
        Extract sectors from the vakgebieden page card elements.
        Finds all sector cards and extracts sector names from company__name divs.
        Looks for all employers__block__content containers to find all sector sections.
        """
        self.logger.info(f"🔍 Extracting detailed sectors from {url}")
        
        # Find ALL containers with sector cards (there may be multiple sections)
        containers = soup.find_all("div", class_="employers__block__content")
        if not containers:
            self.logger.warning(f"Could not find employers__block__content on {url}")
            return
        
        total_cards_found = 0
        
        # Process each container
        for container in containers:
            # Find the cards container within this block
            cards_container = container.find("div", class_="employers__block__cards")
            if not cards_container:
                continue
            
            # Find all sector cards in this container
            cards = cards_container.find_all("a", class_="employers__card")
            if not cards:
                continue
            
            total_cards_found += len(cards)
            self.logger.info(f"✓ Found {len(cards)} sector cards in container | Source: {url}")
            
            for card in cards:
                try:
                    # Find the company name div
                    employer_left = card.find("div", class_="employer__left")
                    if not employer_left:
                        continue
                    
                    company_name_div = employer_left.find("div", class_="company__name")
                    if not company_name_div:
                        continue
                    
                    sector_name = company_name_div.get_text(strip=True)
                    if sector_name:
                        # Normalize the sector name (remove HTML entities like &amp;)
                        sector_name = sector_name.replace("&amp;", "&").replace("&nbsp;", " ")
                        # Skip if it's not a real sector (like navigation text)
                        if sector_name.lower() in ["contact", "onze vakgebieden", "waar ben je naar op zoek?"]:
                            continue
                        all_sectors.add(sector_name)
                        self.logger.info(f"  ✓ Sector: '{sector_name}' | Source: {url}")
                
                except Exception as e:
                    self.logger.error(f"Error processing sector card on {url}: {e}")
                    continue
        
        if total_cards_found == 0:
            self.logger.warning(f"Could not find any sector cards on {url}")
            return
        
        self.logger.info(f"✓ Total sectors extracted: {len(all_sectors)} from {total_cards_found} cards | Source: {url}")
    
    def _extract_sectors_secondary(self, soup: BeautifulSoup, all_sectors_secondary, url: str) -> None:
        """
        Extract secondary sectors from the vacatures page job board filter.
        Finds all checkbox inputs with name="vakgebied" and extracts their values.
        """
        self.logger.info(f"🔍 Extracting secondary sectors from {url}")
        
        # Find the filter block for vakgebied
        filter_block = soup.find("div", class_="jobboardfilter__block", attrs={"data-filterkey": "VAKGEBIED"})
        if not filter_block:
            self.logger.warning(f"Could not find jobboardfilter__block with data-filterkey='VAKGEBIED' on {url}")
            return
        
        # Find all checkbox inputs with name="vakgebied"
        checkboxes = filter_block.find_all("input", {"name": "vakgebied", "type": "checkbox"})
        if not checkboxes:
            self.logger.warning(f"Could not find vakgebied checkboxes on {url}")
            return
        
        self.logger.info(f"✓ Found {len(checkboxes)} sector checkboxes | Source: {url}")
        
        for checkbox in checkboxes:
            try:
                sector_value = checkbox.get("value", "").strip()
                if sector_value:
                    # Normalize the sector name (remove HTML entities like &amp;)
                    sector_value = sector_value.replace("&amp;", "&").replace("&nbsp;", " ")
                    all_sectors_secondary.add(sector_value)
                    self.logger.info(f"  ✓ Secondary sector: '{sector_value}' | Source: {url}")
            
            except Exception as e:
                self.logger.error(f"Error processing sector checkbox on {url}: {e}")
                continue
        
        self.logger.info(f"✓ Total secondary sectors extracted: {len(all_sectors_secondary)} | Source: {url}")
    
    def _extract_office_locations(self, soup: BeautifulSoup, agency: Agency, url: str) -> None:
        """
        Extract office locations from the vestigingen page.
        Finds all office cards and extracts city names, then adds office URLs to evidence_urls.
        """
        self.logger.info(f"🔍 Extracting office locations from {url}")
        
        if not agency.office_locations:
            agency.office_locations = []
        
        # Find the container with cards
        container = soup.find("div", class_="container--cards")
        if not container:
            self.logger.warning(f"Could not find container--cards on {url}")
            return
        
        # Find all office cards
        cards = container.find_all("div", class_="card")
        if not cards:
            self.logger.warning(f"Could not find office cards on {url}")
            return
        
        self.logger.info(f"✓ Found {len(cards)} office cards | Source: {url}")
        
        office_urls = set()
        
        for card in cards:
            try:
                # Extract city name from h2
                h2 = card.find("h2")
                if not h2:
                    continue
                
                title_text = h2.get_text(strip=True)
                # Format: "City, Street" or "City, Street (ID)" or "Randstad, City, Street"
                # Extract just the city name (first part before comma)
                if "," in title_text:
                    # Handle special case: "Randstad, Nijmegen, Keizer Karelplein"
                    parts = [p.strip() for p in title_text.split(",")]
                    if len(parts) >= 2 and parts[0].lower() == "randstad":
                        city_name = parts[1]  # Skip "Randstad" prefix
                    else:
                        city_name = parts[0]
                else:
                    city_name = title_text
                
                if not city_name:
                    continue
                
                # Get office URL from the link
                link = card.find("a", href=True)
                if link:
                    office_path = link.get("href", "")
                    if office_path:
                        # Convert relative URL to absolute
                        if office_path.startswith("/"):
                            office_url = f"{self.WEBSITE_URL}{office_path}"
                        else:
                            office_url = f"{self.WEBSITE_URL}/{office_path}"
                        office_urls.add(office_url)
                
                # Determine province using utils
                province = self.utils.map_city_to_province(city_name)
                
                # Create office location
                office = OfficeLocation(
                    city=city_name,
                    province=province,
                )
                
                # Check if already exists (avoid duplicates)
                if not any(off.city == city_name for off in agency.office_locations):
                    agency.office_locations.append(office)
                    self.logger.info(f"✓ Office: {city_name}, {province} | Source: {url}")
                else:
                    self.logger.info(f"  Skipped duplicate: {city_name}")
                
            except Exception as e:
                self.logger.error(f"Error processing office card: {e}")
                continue
        
        # Add all office URLs to evidence_urls (without fetching them)
        if office_urls:
            for office_url in office_urls:
                self.evidence_urls.append(office_url)
            self.logger.info(f"✓ Added {len(office_urls)} office URLs to evidence_urls | Source: {url}")
        
        self.logger.info(f"✓ Total offices extracted: {len(agency.office_locations)} | Source: {url}")
    
    def _extract_statistics(self, soup: BeautifulSoup, page_text: str, agency: Agency, url: str) -> None:
        """
        Extract statistics from the werkgevers page.
        Extracts candidate pool size, monthly visitors, etc.
        """
        import re
        
        self.logger.info(f"🔍 Extracting statistics from {url}")
        
        # Extract candidate pool size (1.5 million talents)
        candidate_pool_match = re.search(r"(\d+\.?\d*)\s*(?:million|miljoen)\s*(?:talent|talenten|kandidaten)", page_text, re.IGNORECASE)
        if candidate_pool_match:
            value = float(candidate_pool_match.group(1))
            if value < 10:  # Likely in millions
                agency.candidate_pool_size_estimate = int(value * 1_000_000)
                self.logger.info(f"✓ Found candidate pool: {agency.candidate_pool_size_estimate:,} | Source: {url}")
        
        # Extract monthly visitors (1.4 million online visitors per month)
        visitors_match = re.search(r"(\d+\.?\d*)\s*(?:million|miljoen)\s*(?:online\s*)?visitors?\s*(?:per\s*month|per\s*maand)", page_text, re.IGNORECASE)
        if visitors_match:
            value = float(visitors_match.group(1))
            if value < 10:  # Likely in millions
                monthly_visitors = int(value * 1_000_000)
                # Store as growth signal or note
                if not agency.growth_signals:
                    agency.growth_signals = []
                agency.growth_signals.append(f"{monthly_visitors:,} online visitors per month")
                self.logger.info(f"✓ Found monthly visitors: {monthly_visitors:,} | Source: {url}")
        
        # Extract years of experience (65 years)
        years_match = re.search(r"(\d+)\s*(?:years?|jaar)\s*(?:of\s*experience|ervaring)", page_text, re.IGNORECASE)
        if years_match:
            years = int(years_match.group(1))
            if not agency.growth_signals:
                agency.growth_signals = []
            agency.growth_signals.append(f"{years} years of experience")
            self.logger.info(f"✓ Found years of experience: {years} | Source: {url}")
    
    def _extract_growth_signals_from_werkgevers(self, soup: BeautifulSoup, page_text: str, agency: Agency, url: str) -> None:
        """
        Extract growth signals and key claims from the werkgevers page.
        """
        import re
        
        self.logger.info(f"🔍 Extracting growth signals from {url}")
        
        if not agency.growth_signals:
            agency.growth_signals = []
        
        # Extract key claims and signals
        signals_found = []
        
        # "largest talent database"
        if re.search(r"largest\s+talent\s+database|grootste\s+talentendatabase", page_text, re.IGNORECASE):
            signals_found.append("Largest talent database")
        
        # "worldwide" or "international" presence
        if re.search(r"worldwide|world-wide|international|wereldwijd", page_text, re.IGNORECASE):
            signals_found.append("International presence")
        
        # "most equal and specialized talent company"
        if re.search(r"most\s+equal\s+and\s+specialized|meest\s+gespecialiseerde", page_text, re.IGNORECASE):
            signals_found.append("Most specialized talent company")
        
        # Energy transition mention (74,000 extra jobs)
        energy_match = re.search(r"(\d+[,\d]*)\s*(?:extra\s*)?jobs?\s*(?:for\s*)?(?:energy\s*transition|energietransitie)", page_text, re.IGNORECASE)
        if energy_match:
            jobs = energy_match.group(1).replace(",", "")
            signals_found.append(f"{jobs} jobs in energy transition")
        
        # Brand divisions mentioned
        brand_divisions = []
        if re.search(r"randstad\s+operational", page_text, re.IGNORECASE):
            brand_divisions.append("Randstad Operational")
        if re.search(r"randstad\s+professional", page_text, re.IGNORECASE):
            brand_divisions.append("Randstad Professional")
        if re.search(r"randstad\s+digital", page_text, re.IGNORECASE):
            brand_divisions.append("Randstad Digital")
        if re.search(r"randstad\s+enterprise", page_text, re.IGNORECASE):
            brand_divisions.append("Randstad Enterprise")
        
        if brand_divisions:
            signals_found.append(f"Brand divisions: {', '.join(brand_divisions)}")
        
        # Add all found signals
        for signal in signals_found:
            if signal not in agency.growth_signals:
                agency.growth_signals.append(signal)
                self.logger.info(f"  ✓ Growth signal: {signal} | Source: {url}")
        
        self.logger.info(f"✓ Total growth signals extracted: {len(signals_found)} | Source: {url}")
    
    def _check_ai_capabilities(self, agency: Agency) -> None:
        """
        Check if Randstad has AI capabilities by calling the Seamly API.
        If the API returns status 200, set chatbot_for_candidates and chatbot_for_clients to True.
        """
        api_url = "https://api.seamly-app.com/channels/api/v2/client/7f2ceefd-2e81-4067-a930-c039dc811d25/translations/4/nl-informal.json"
        
        self.logger.info(f"🔍 Checking AI capabilities via Seamly API: {api_url}")
        
        try:
            response = requests.get(api_url, timeout=10)
            
            if response.status_code == 200:
                agency.ai_capabilities.internal_ai_matching = True
                agency.ai_capabilities.chatbot_for_candidates = True
                agency.ai_capabilities.chatbot_for_clients = True
                self.evidence_urls.append(api_url)
                self.logger.info(f"✓ AI capabilities detected: chatbot for candidates and clients enabled | API: {api_url}")
            else:
                self.logger.info(f"  AI capabilities not detected (status {response.status_code}) | API: {api_url}")
        
        except requests.exceptions.RequestException as e:
            self.logger.warning(f"⚠ Failed to check AI capabilities API: {e} | API: {api_url}")
        except Exception as e:
            self.logger.error(f"❌ Error checking AI capabilities: {e} | API: {api_url}")


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
