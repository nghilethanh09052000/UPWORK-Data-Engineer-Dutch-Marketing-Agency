"""
Yacht Netherlands scraper.

Website: https://www.yacht.nl
Part of: Randstad Groep Nederland"""

from __future__ import annotations

from typing import Any, Dict, List, Set

import dagster as dg
from bs4 import BeautifulSoup

from staffing_agency_scraper.models import Agency, GeoFocusType, OfficeLocation
from staffing_agency_scraper.scraping.base import BaseAgencyScraper
from staffing_agency_scraper.scraping.utils import AgencyScraperUtils

class YachtScraper(BaseAgencyScraper):
    """Scraper for Yacht Netherlands."""

    AGENCY_NAME = "Yacht"
    WEBSITE_URL = "https://www.yacht.nl"
    BRAND_GROUP = "Randstad Groep Nederland"
    PAGES_TO_SCRAPE: List[Dict[str, Any]] = [
        {
            "name": "home",
            "url": "https://www.yacht.nl",
            "functions": ['logo', 'services', 'sectors', 'reviews'],
        },
        {
            "name": "contact",
            "url": "https://www.yacht.nl/contact",
            "functions": ['contact'],
        },
        {
            "name": "voorwaarden",
            "url": "https://www.yacht.nl/voorwaarden",
            "functions": ['legal'],
        },
        {
            "name": "contactinformatie",
            "url": "https://www.yacht.nl/contactinformatie/#onze-kantoren",
            "functions": ['office_locations'],
        },
    ]

    def scrape(self) -> Agency:
        self.logger.info(f"Starting scrape of {self.AGENCY_NAME}")
        
        # Initialize utils
        self.utils = AgencyScraperUtils(logger=self.logger)
        
        agency = self.create_base_agency()
        agency.geo_focus_type = GeoFocusType.NATIONAL
        agency.employers_page_url = f"{self.WEBSITE_URL}/opdrachtgevers"
        agency.contact_form_url = f"{self.WEBSITE_URL}/contact"
        
        all_sectors = set()
        all_text = ""  # Accumulate text from all pages for utils extraction
        main_soup = None  # Keep homepage soup for portal/review detection
        
        
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

                # Accumulate text for utils extraction
                all_text += " " + page_text

                # Keep homepage soup for portal/review detection in extract_all_common_fields
                if page_name == "home":
                    main_soup = soup
                
                # Apply extraction functions
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
                
                # Detect chatbot on homepage
                if page_name == "home":
                    if self._detect_seamly_chatbot():
                        agency.ai_capabilities.chatbot_for_candidates = True
                        agency.ai_capabilities.chatbot_for_clients = True
                        self.logger.info(f"✓ Detected Seamly chatbot (API check) | Source: Seamly API")
                
                self.logger.info(f"✅ Completed: {page_name}")
                
            except Exception as e:
                self.logger.error(f"❌ Error scraping {url}: {e}")
        
        # Finalize
        if all_sectors:
            agency.sectors_core = sorted(list(all_sectors))

        # ==================== APPLY ALL COMMON UTILS EXTRACTIONS ====================
        self.logger.info("=" * 80)
        self.logger.info("🔧 APPLYING AUTOMATIC UTILS EXTRACTIONS")
        self.logger.info("-" * 80)
        
        # This automatically extracts 40+ fields using accumulated text from all pages
        self.extract_all_common_fields(agency, all_text, main_soup)

        self.logger.info("✅ Automatic utils extractions completed")
        self.logger.info("=" * 80)
        
        
        
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
        all_sectors: Set[str],
        url: str,
    ) -> None:
        """Apply BS4/regex extraction functions."""
        for func_name in functions:
            if func_name == "logo":
                self._extract_logo(soup, agency, url)
            
            elif func_name == "services":
                services = self.utils.fetch_services(page_text, url)
                agency.services = services
            
            elif func_name == "contact":
                self._extract_contact(soup, page_text, agency, url)
            
            elif func_name == "legal":
                self._extract_legal(agency, page_text, url)
            
            elif func_name == "sectors":
                self._extract_sectors(soup, all_sectors, url)
            
            elif func_name == "reviews":
                self._extract_reviews(soup, agency, url)
            
            elif func_name == "office_locations":
                self._extract_office_locations(soup, url, agency)
    
    def _extract_logo(self, soup: BeautifulSoup, agency: Agency, url: str) -> None:
        """Extract logo from JSON-LD schema."""
        import json
        
        self.logger.info(f"🔍 Extracting logo from JSON-LD schema on {url}")
        
        # Find JSON-LD script tags
        json_ld_scripts = soup.find_all("script", type="application/ld+json")
        
        for script in json_ld_scripts:
            try:
                data = json.loads(script.string)
                
                # Check if this is a Corporation type with logo
                if data.get("@type") == "Corporation" and "logo" in data:
                    logo_data = data["logo"]
                    
                    if isinstance(logo_data, dict):
                        logo_url = logo_data.get("url")
                        if logo_url:
                            agency.logo_url = logo_url
                            self.logger.info(f"✓ Logo extracted from JSON-LD: {logo_url} | Source: {url}")
                            return
                    elif isinstance(logo_data, str):
                        agency.logo_url = logo_data
                        self.logger.info(f"✓ Logo extracted from JSON-LD: {logo_data} | Source: {url}")
                        return
                        
            except (json.JSONDecodeError, KeyError, AttributeError) as e:
                self.logger.warning(f"⚠ Failed to parse JSON-LD for logo: {e}")
                continue
        
        # Fallback to standard logo extraction if JSON-LD didn't work
        self.logger.warning(f"Could not find logo in JSON-LD, trying fallback method")
        logo = self.utils.fetch_logo(soup, url)
        if logo:
            agency.logo_url = logo
    
    def _extract_contact(self, soup: BeautifulSoup, page_text: str, agency: Agency, url: str) -> None:
        """Extract contact information (email, phone, office locations)."""
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
            
    def _extract_legal(self, agency: Agency, page_text: str, url: str) -> None:
        """Extract KvK number, legal name, and HQ location from voorwaarden page."""
        import re
        
        # Extract KvK and legal name
        kvk = self.utils.fetch_kvk_number(page_text, url)
        legal_name = self.utils.fetch_legal_name(page_text, "Yacht", url)

        if kvk:
            agency.kvk_number = kvk
        if legal_name:
            agency.legal_name = legal_name
    
        # Extract HQ from voorwaarden page
        # Dutch: "houdt kantoor te Diemen aan de Diemermere 25"
        # English: "has its office in Diemen at Diemermere 25"
        if not agency.hq_city:
            hq_patterns = [
                r"(?:houdt|heeft)\s+(?:haar\s+)?kantoor\s+te\s+(\w+)\s+aan\s+de\s+([\w\s]+\d+)",  # Dutch
                r"has its office in (\w+) at ([\w\s]+\d+)",  # English
                r"office in (\w+)\s+(?:at|aan)\s+(?:de\s+)?([\w\s]+\d+)",  # Generic
            ]
            
            for pattern in hq_patterns:
                hq_match = re.search(pattern, page_text, re.IGNORECASE)
                if hq_match:
                    city = hq_match.group(1)
                    address = hq_match.group(2).strip()
                    agency.hq_city = city
                    agency.hq_province = self._determine_province(city)
                    self.logger.info(f"✓ Found HQ: {city} at {address} | Source: {url}")
                    break
    
    def _extract_sectors(self, soup: BeautifulSoup, all_sectors: set, url: str) -> None:
        """Extract core sectors from 'Onze vakgebieden en branches' section."""
        vakgebieden_div = soup.find("div", id="onze-vakgebieden-en-branches")
        
        if vakgebieden_div:
            # Extract all links in this section
            for link in vakgebieden_div.find_all("a", class_="link-list__single"):
                title_span = link.find("span", class_="link-list__title")
                if title_span:
                    sector_text = title_span.get_text(strip=True)
                    if sector_text and len(sector_text) > 1:
                        all_sectors.add(sector_text)
                        self.logger.info(f"✓ Found core sector: '{sector_text}' | Source: {url}")
        else:
            self.logger.warning(f"Could not find #onze-vakgebieden-en-branches section on {url}")
    
    def _extract_reviews(self, soup: BeautifulSoup, agency: Agency, url: str) -> None:
        """Extract Google reviews from footer."""
        import re
        
        footer = soup.find("footer") or soup.find(class_=lambda x: x and "pagefooter" in str(x).lower())
        if not footer:
            return
        
        rating_text_span = footer.find("span", class_="rating__text")
        if not rating_text_span:
            return
        
        rating_text = rating_text_span.get_text(strip=True)
        
        # Extract rating: "Yacht Google score 4.15 - 118 reviews"
        rating_match = re.search(r"Google\s+score\s+([\d.]+)\s*-\s*(\d+)\s+reviews?", rating_text, re.IGNORECASE)
        if rating_match:
            rating = float(rating_match.group(1))
            count = int(rating_match.group(2))
            
            if not agency.review_rating:
                agency.review_rating = rating
                self.logger.info(f"✓ Found Google rating: {rating}/5 | Source: {url}")
            
            if not agency.review_count:
                agency.review_count = count
                self.logger.info(f"✓ Found review count: {count} | Source: {url}")
            
            # Add Google to review sources
            if not agency.review_sources:
                agency.review_sources = []
            if "Google" not in agency.review_sources:
                agency.review_sources.append("Google")
                self.logger.info(f"✓ Added review source: Google | Source: {url}")
    
    def _extract_office_locations(self, soup: BeautifulSoup, url: str, agency: Agency) -> None:
        """
        Extract office locations by:
        1. Finding all office cards on the contact page
        2. Following each card link to the individual office page
        3. Extracting address, phone, postal code from each office page
        """
        import re
        
        if not agency.office_locations:
            agency.office_locations = []
        
        # Find all office cards
        office_cards = soup.find_all("a", class_="card-holder__card")
        
        if not office_cards:
            self.logger.warning(f"No office cards found on {url}")
            return
        
        office_links = []
        
        # Extract URLs and city names from cards
        for card in office_cards:
            href = card.get("href", "")
            if href and "/contactinformatie/kantoren/" in href:
                # Get city name from span
                city_span = card.find("span", class_="link__page")
                city_name = city_span.get_text(strip=True) if city_span else None
                
                # Build full URL
                full_url = href if href.startswith("http") else f"{self.WEBSITE_URL}{href}"
                
                if city_name:
                    office_links.append((full_url, city_name))
                    self.logger.info(f"✓ Found office card: {city_name} → {full_url}")
        
        self.logger.info(f"Found {len(office_links)} office locations to scrape")
        
        # Visit each office page and extract details
        for office_url, city_name in office_links:
            try:
                self.logger.info(f"📍 Scraping office: {city_name}")
                
                # Fetch the office page
                office_soup = self.fetch_page(office_url)
                
                # Find the rich-text section
                rich_text = office_soup.find("div", class_=lambda x: x and "rich-text" in str(x))
                
                if not rich_text:
                    self.logger.warning(f"Could not find rich-text section for {city_name}")
                    continue
                
                # Determine province
                province = self._determine_province(city_name)
                
                # Create office location (OfficeLocation only has city and province)
                office = OfficeLocation(
                    city=city_name,
                    province=province,
                )
                
                # Check if already exists
                if not any(off.city == city_name for off in agency.office_locations):
                    agency.office_locations.append(office)
                    self.logger.info(f"✓ Added office: {city_name}, {province}")
                
            except Exception as e:
                self.logger.error(f"Error scraping office {city_name}: {e}")
                continue
        
        self.logger.info(f"✓ Total offices extracted: {len(agency.office_locations)}")
    
    def _determine_province(self, city: str) -> str:
        """Determine province based on city name."""
        city_province_map = {
            "Amsterdam": "Noord-Holland",
            "Diemen": "Noord-Holland",
            "Utrecht": "Utrecht",
            "Eindhoven": "Noord-Brabant",
            "Groningen": "Groningen",
            "Maastricht": "Limburg",
            "Zwolle": "Overijssel",
            "Amersfoort": "Utrecht",
        }
        return city_province_map.get(city, "Unknown")
    
    def _detect_seamly_chatbot(self) -> bool:
        """
        Detect if Yacht has Seamly chatbot by checking the API endpoint.
        
        If the API returns 200, the chatbot is available.
        API: https://api.seamly-app.com/channels/api/v2/client/71497efc-a8a4-4b75-a8bb-dc235090c652/translations/4/nl-informal.json
        """
        import requests
        
        seamly_api_url = "https://api.seamly-app.com/channels/api/v2/client/71497efc-a8a4-4b75-a8bb-dc235090c652/translations/4/nl-informal.json"
        
        try:
            self.logger.info(f"Checking Seamly chatbot API: {seamly_api_url}")
            response = requests.get(seamly_api_url, timeout=10)
            
            if response.status_code == 200:
                self.logger.info(f"✓ Seamly chatbot API returned 200 - Chatbot is available")
                # Add API URL to evidence
                if seamly_api_url not in self.evidence_urls:
                    self.evidence_urls.append(seamly_api_url)
                return True
            else:
                self.logger.info(f"✗ Seamly chatbot API returned {response.status_code} - Chatbot not available")
                return False
                
        except Exception as e:
            self.logger.warning(f"Could not check Seamly chatbot API: {e}")
            return False


@dg.asset(group_name="agencies")
def yacht_scrape() -> dg.Output[dict]:
    """Scrape Yacht website."""
    scraper = YachtScraper()
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

