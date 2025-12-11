"""
Maandag Netherlands scraper.

Website: https://www.maandag.nl
"""

from __future__ import annotations

import json
import re
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
            "url": "https://www.maandag.com/nl-nl",
            "functions": ["logo", "sectors"],
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
        {
            "name": "certifications",
            "url": "https://www.maandag.com/nl-nl/certificeringen",
            "functions": ["certifications"],
        },
    ]

    def scrape(self) -> Agency:
        self.logger.info(f"Starting scrape of {self.AGENCY_NAME}")
        
        # Note: self.utils is now initialized in BaseAgencyScraper.__init__()
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
        
        # Extract sectors from aggregated text (fallback if not found on home page)
        if not agency.sectors_core:
            sectors = self.utils.fetch_sectors(all_text, "accumulated_text")
            if sectors:
                agency.sectors_core = sectors
        
        # ========================================================================
        # Extract ALL common fields using base class utility method! 🚀
        # This replaces 50+ lines of repetitive extraction code
        # ========================================================================
        self.extract_all_common_fields(agency, all_text)
        
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
                # Extract from JSON-LD structured data (also gets legal name, KvK, contact info)
                self._extract_from_jsonld(soup, agency, url)
            
            elif func_name == "sectors":
                if not agency.sectors_core:
                    sectors = self._extract_sectors(soup, url)
                    if sectors:
                        agency.sectors_core = sectors
            
            elif func_name == "contact":
                # Extract office locations from Next.js JSON data
                if not agency.office_locations:
                    offices = self._extract_office_locations(soup, url)
                    if offices:
                        agency.office_locations = offices
                    else:
                        # Fallback to generic extraction
                        agency.office_locations = self.utils.fetch_office_locations(soup, url)
                
                # Contact info is already extracted from JSON-LD on home page
                if not agency.contact_email:
                    agency.contact_email = self.utils.fetch_contact_email(page_text, url)
                if not agency.contact_phone:
                    agency.contact_phone = self.utils.fetch_contact_phone(page_text, url)
            
            elif func_name == "services":
                self._extract_services(soup, agency, url)
            
            elif func_name == "legal":
                if not agency.kvk_number:
                    agency.kvk_number = self.utils.fetch_kvk_number(page_text, url)
                if not agency.legal_name:
                    agency.legal_name = self.utils.fetch_legal_name(page_text, "Maandag", url)
            
            elif func_name == "certifications":
                if not agency.certifications:
                    certs = self._extract_certifications(soup, url)
                    if certs:
                        agency.certifications = certs
    
    def _extract_services(self, soup: BeautifulSoup, agency: Agency, url: str) -> None:
        """
        Extract services from the service page.
        
        Maandag offers checkboxes showing their service types:
        - "Professional - Detachering" (Secondment)
        - "Zzp'er" (Self-employed/freelancer)
        """
        page_text = soup.get_text(separator=" ", strip=True).lower()
        
        # Check for secondment/detachering
        if "detachering" in page_text or "secondment" in page_text:
            agency.services.detacheren = True
            self.logger.info(f"✓ Found service: detacheren | Source: {url}")
        
        # Check for ZZP/self-employed/freelancer
        if "zzp" in page_text or "self-employed" in page_text or "zelfstandig" in page_text:
            agency.services.zzp_bemiddeling = True
            self.logger.info(f"✓ Found service: zzp_bemiddeling | Source: {url}")
        
        # Also check for MSP (Managed Service Provider) mentioned on the page
        if "managed service provider" in page_text or "msp" in page_text:
            agency.services.msp = True
            self.logger.info(f"✓ Found service: msp | Source: {url}")
    
    def _extract_certifications(self, soup: BeautifulSoup, url: str) -> list[str]:
        """
        Extract ISO certifications from the certifications page.
        
        Maandag has:
        - ISO 27001 (Information security)
        - ISO 14001 (Environmental management)
        - ISO 9001 (Quality management)
        """
        certifications = []
        page_text = soup.get_text(separator=" ", strip=True).lower()
        
        # Check for ISO certifications
        if "iso 27001" in page_text or "iso27001" in page_text:
            certifications.append("ISO 27001")
            self.logger.info(f"✓ Found certification: ISO 27001 | Source: {url}")
        
        if "iso 14001" in page_text or "iso14001" in page_text:
            certifications.append("ISO 14001")
            self.logger.info(f"✓ Found certification: ISO 14001 | Source: {url}")
        
        if "iso 9001" in page_text or "iso9001" in page_text:
            certifications.append("ISO 9001")
            self.logger.info(f"✓ Found certification: ISO 9001 | Source: {url}")
        
        return certifications
    
    def _extract_sectors(self, soup: BeautifulSoup, url: str) -> list[str]:
        """
        Extract sectors from the marquee carousel on home page.
        
        Looks for the sector carousel with links like:
        <div role="marquee">
            <ul>
                <li><a href="/nl-nl/onderwijs">Onderwijs</a></li>
                <li><a href="/nl-nl/overheid">Overheid</a></li>
                ...
            </ul>
        </div>
        """
        sectors = []
        seen = set()
        
        # Find the marquee element
        marquee = soup.find("div", role="marquee")
        if marquee:
            # Find all links in the marquee
            for link in marquee.find_all("a", href=True):
                href = link.get("href", "")
                # Look for sector links (e.g., /nl-nl/onderwijs, /nl-nl/it, etc.)
                if href.startswith("/nl-nl/") and href != "/nl-nl/":
                    # Get the sector text from the span
                    span = link.find("span", class_="_1k5e5cz0")
                    if span:
                        sector_text = span.get_text(strip=True)
                        if sector_text and sector_text.lower() not in seen:
                            sectors.append(sector_text)
                            seen.add(sector_text.lower())
                            self.logger.info(f"✓ Found sector: '{sector_text}' | Source: {url}")
        
        return sectors
    
    def _extract_office_locations(self, soup: BeautifulSoup, url: str) -> list:
        """
        Extract office locations from Next.js JSON data on the contact page.
        
        The contact page has detailed office location data embedded in:
        <script>self.__next_f.push([1,"...JSON data..."])</script>
        
        Each location includes:
        - cardTitle (city name)
        - officeAddress
        - officePostalCode
        - countryCode
        """
        from staffing_agency_scraper.models import OfficeLocation
        
        offices = []
        seen_cities = set()
        
        try:
            # Find script tag containing location data - prefer the large one with all offices
            script_candidates = []
            for script in soup.find_all("script"):
                if not script.string or "self.__next_f.push" not in script.string:
                    continue
                
                if "itemOfficeLocation" not in script.string or "cardTitle" not in script.string:
                    continue
                
                script_candidates.append((len(script.string), script.string))
            
            # Sort by length descending - largest script likely has all offices
            script_candidates.sort(reverse=True)
            
            for script_len, script_content in script_candidates:
                # Find all occurrences of cardTitle followed by countryCode within 300 chars
                # Pattern: "cardTitle":"CityName" ... (within 300 chars) ... "countryCode":"nl"
                
                # In Next.js, JSON strings are escaped, so we need to look for the escaped pattern
                # Try different patterns: \\"countryCode\\":\\"nl\\" or similar
                nl_positions = []
                
                # Pattern 1: Escaped quotes
                matches1 = list(re.finditer(r'\\"countryCode\\":\\"nl\\"', script_content, re.IGNORECASE))
                nl_positions.extend([m.start() for m in matches1])
                
                # Pattern 2: Without outer quotes (in case it's in the middle of JSON)
                if not nl_positions:
                    matches2 = list(re.finditer(r'countryCode\\\\":\\\\"nl', script_content, re.IGNORECASE))
                    nl_positions.extend([m.start() for m in matches2])
                
                for nl_pos in nl_positions:
                    # Look FORWARD up to 200 chars for cardTitle (it comes after countryCode in JSON)
                    end = min(len(script_content), nl_pos + 200)
                    window = script_content[nl_pos:end]
                    
                    # Find cardTitle in the forward window
                    # The format is: \"cardTitle\":\"CityName\" (with escaped quotes)
                    card_title_match = re.search(r'\\"cardTitle\\":\\"([^\\"]+)\\"', window)
                    
                    if not card_title_match:
                        continue
                    
                    city = card_title_match.group(1).strip()
                    
                    # Skip duplicates and "Maandag® World" (it's the same as Amsterdam)
                    if city == "Maandag® World" or city in seen_cities:
                        continue
                    
                    seen_cities.add(city)
                    province = self._get_province_for_city(city)
                    
                    office = OfficeLocation(
                        city=city,
                        province=province
                    )
                    offices.append(office)
                    self.logger.info(f"✓ Found office: {city} ({province}) | Source: {url}")
            
            if offices:
                self.logger.info(f"✓ Extracted {len(offices)} office locations from Next.js JSON")
            
            return offices
        
        except Exception as e:
            self.logger.warning(f"Error extracting office locations from Next.js JSON: {e} | Source: {url}")
            return []
    
    def _get_province_for_city(self, city: str) -> str:
        """Map Dutch cities to their provinces."""
        city_to_province = {
            "Amsterdam": "Noord-Holland",
            "Alkmaar": "Noord-Holland",
            "Den Haag": "Zuid-Holland",
            "Rotterdam": "Zuid-Holland",
            "Utrecht": "Utrecht",
            "Eindhoven": "Noord-Brabant",
            "Breda": "Noord-Brabant",
            "Groningen": "Groningen",
            "Leeuwarden": "Friesland",
            "Zwolle": "Overijssel",
            "Enschede": "Overijssel",
            "Arnhem": "Gelderland",
            "Maastricht": "Limburg",
            "Middelburg": "Zeeland",
        }
        return city_to_province.get(city, "Unknown")
    
    def _extract_from_jsonld(self, soup: BeautifulSoup, agency: Agency, url: str) -> None:
        """
        Extract data from JSON-LD structured data on the home page.
        
        Maandag provides comprehensive structured data including:
        - Logo URL
        - Legal name
        - KvK number (companyRegistration)
        - Contact email and phone
        - Founding date
        - Employee count
        - HQ address
        """
        try:
            # Maandag uses Next.js format: <script>(self.__next_s=...).push([0,{"type":"application/ld+json","children":"..."}])</script>
            # Find the script tag containing the JSON-LD data
            data = None
            
            # Try standard JSON-LD format first
            script_tag = soup.find("script", {"type": "application/ld+json"})
            if script_tag and script_tag.string:
                data = json.loads(script_tag.string)
                self.logger.info(f"✓ Found standard JSON-LD script tag | Source: {url}")
            else:
                # Try Next.js format - look for script containing "application/ld+json"
                for script in soup.find_all("script"):
                    if script.string and "application/ld+json" in script.string and "children" in script.string:
                        # Extract the JSON string from the children property
                        script_content = script.string
                        # Find the JSON-LD string inside the children property
                        start_idx = script_content.find('"children":"') + len('"children":"')
                        if start_idx > len('"children":"') - 1:
                            # Find the end of the JSON string (before the closing "})
                            end_idx = script_content.find('"}])', start_idx)
                            if end_idx > start_idx:
                                json_str = script_content[start_idx:end_idx]
                                # Unescape the JSON string
                                json_str = json_str.replace('\\"', '"').replace('\\\\', '\\')
                                data = json.loads(json_str)
                                self.logger.info(f"✓ Found Next.js JSON-LD data | Source: {url}")
                                break
            
            if not data:
                self.logger.warning(f"No JSON-LD structured data found | Source: {url}")
                return
            
            # Extract logo URL
            if not agency.logo_url and "logo" in data:
                agency.logo_url = data["logo"]
                self.logger.info(f"✓ Found logo (JSON-LD): {agency.logo_url} | Source: {url}")
            
            # Extract legal name
            if not agency.legal_name and "legalName" in data:
                agency.legal_name = data["legalName"]
                self.logger.info(f"✓ Found legal name (JSON-LD): {agency.legal_name} | Source: {url}")
            
            # Extract KvK number
            if not agency.kvk_number and "companyRegistration" in data:
                agency.kvk_number = data["companyRegistration"]
                self.logger.info(f"✓ Found KvK number (JSON-LD): {agency.kvk_number} | Source: {url}")
            
            # Extract contact email
            if not agency.contact_email and "email" in data:
                agency.contact_email = data["email"]
                self.logger.info(f"✓ Found contact email (JSON-LD): {agency.contact_email} | Source: {url}")
            
            # Extract contact phone
            if not agency.contact_phone and "telephone" in data:
                agency.contact_phone = data["telephone"]
                self.logger.info(f"✓ Found contact phone (JSON-LD): {agency.contact_phone} | Source: {url}")
            
            # Extract HQ address
            if "address" in data and isinstance(data["address"], dict):
                address = data["address"]
                if not agency.hq_city and "addressLocality" in address:
                    agency.hq_city = address["addressLocality"]
                    self.logger.info(f"✓ Found HQ city (JSON-LD): {agency.hq_city} | Source: {url}")
                if not agency.hq_province and "addressRegion" in address:
                    agency.hq_province = address["addressRegion"]
                    self.logger.info(f"✓ Found HQ province (JSON-LD): {agency.hq_province} | Source: {url}")
            
            # Extract founding date for growth signals
            if "foundingDate" in data:
                founding_year = data["foundingDate"].split("-")[0]
                if not agency.growth_signals:
                    agency.growth_signals = []
                growth_signal = f"opgericht_in_{founding_year}"
                if growth_signal not in agency.growth_signals:
                    agency.growth_signals.append(growth_signal)
                    self.logger.info(f"✓ Found growth signal (JSON-LD): {growth_signal} | Source: {url}")
            
            # Extract employee count for growth signals
            if "numberOfEmployees" in data and isinstance(data["numberOfEmployees"], dict):
                emp_data = data["numberOfEmployees"]
                min_emp = emp_data.get("minValue")
                max_emp = emp_data.get("maxValue")
                if min_emp and max_emp:
                    if not agency.growth_signals:
                        agency.growth_signals = []
                    growth_signal = f"{min_emp}_tot_{max_emp}_medewerkers"
                    if growth_signal not in agency.growth_signals:
                        agency.growth_signals.append(growth_signal)
                        self.logger.info(f"✓ Found growth signal (JSON-LD): {growth_signal} | Source: {url}")
        
        except json.JSONDecodeError as e:
            self.logger.warning(f"Failed to parse JSON-LD: {e} | Source: {url}")
        except Exception as e:
            self.logger.warning(f"Error extracting JSON-LD data: {e} | Source: {url}")


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
