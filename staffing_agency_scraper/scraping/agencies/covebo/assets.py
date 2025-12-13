"""
Covebo Netherlands scraper.

Website: https://www.covebo.nl
"""

from __future__ import annotations

import html
import json
import random
import re
import time
from typing import Any, Dict, List, Set

import dagster as dg
from bs4 import BeautifulSoup

from staffing_agency_scraper.models import Agency, AgencyServices, CaoType, GeoFocusType, OfficeLocation
from staffing_agency_scraper.scraping.base import BaseAgencyScraper
from staffing_agency_scraper.scraping.utils import AgencyScraperUtils

class CoveboScraper(BaseAgencyScraper):
    """Scraper for Covebo Netherlands."""

    AGENCY_NAME = "Covebo"
    WEBSITE_URL = "https://www.covebo.nl"

    PAGES_TO_SCRAPE: List[Dict[str, Any]] = [
        {
            "name": "home",
            "url": "https://www.covebo.nl",
            "functions": ['logo', 'services', 'jsonld', 'sectors_core'],
        },
        {
            "name": "werkgevers",
            "url": "https://www.covebo.nl/werkgever/",
            "functions": ['statistics', 'growth_signals', 'membership'],
        },
        {
            "name": "contact",
            "url": "https://www.covebo.nl/contact",
            "functions": ['contact'],
        },
        {
            "name": "privacy",
            "url": "https://www.covebo.nl/privacy-statement/",
            "functions": ['legal'],
        },
        {
            "name": "certificeringen",
            "url": "https://www.covebo.nl/certificeringen/",
            "functions": ['certifications'],
        },
        {
            "name": "portal",
            "url": "https://www.covebo.nl/portal/",
            "functions": ['portals'],
        },
        {
            "name": "diensten",
            "url": "https://www.covebo.nl/werkgever/diensten/",
            "functions": ['services_detailed'],
        },
        {
            "name": "vestigingen",
            "url": "https://www.covebo.nl/over-covebo/vestigingen/",
            "functions": ['office_locations'],
        },
        {
            "name": "vacatures",
            "url": "https://www.covebo.nl/vacatures/",
            "functions": ['sectors_secondary'],
        },
        {
            "name": "ons_verhaal",
            "url": "https://www.covebo.nl/over-covebo/ons-verhaal/",
            "functions": ['statistics', 'growth_signals'],
        },
        {
            "name": "house_of_covebo_contact",
            "url": "https://www.houseofcovebo.nl/contact/",
            "functions": ['brand_group_contact'],
        },
    ]

    # def fetch_page(self, url: str) -> BeautifulSoup:
    #     """
    #     Fetch and parse a page with random delay to avoid bot detection.
        
    #     Adds a random delay between 2-5 seconds before fetching each page.
        
    #     Parameters
    #     ----------
    #     url : str
    #         URL to fetch

    #     Returns
    #     -------
    #     BeautifulSoup
    #         Parsed HTML
    #     """
    #     # Add random delay between 2-5 seconds to avoid bot detection
    #     delay = random.uniform(2.0, 5.0)
    #     self.logger.info(f"⏳ Waiting {delay:.2f}s before fetching {url} (anti-bot delay)")
    #     time.sleep(delay)
        
    #     # Call parent fetch_page method
    #     return super().fetch_page(url)

    def scrape(self) -> Agency:
        self.logger.info(f"Starting scrape of {self.AGENCY_NAME}")
        
        
        # Initialize utils
        self.utils = AgencyScraperUtils(logger=self.logger)
        # Initialize utils        
        agency = self.create_base_agency()
        #agency.geo_focus_type = GeoFocusType.NATIONAL
        agency.employers_page_url = f"{self.WEBSITE_URL}/werkgevers"
        agency.contact_form_url = f"{self.WEBSITE_URL}/contact"
        
        all_sectors = set()
        all_sectors_secondary = set()  # For sectors_secondary

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
                
                # Apply normal functions
                self._apply_functions(agency, functions, soup, page_text, all_sectors, all_sectors_secondary, url)
                
                # Portal detection on every page
                if self.utils.detect_candidate_portal(soup, page_text, url):
                    agency.digital_capabilities.candidate_portal = True
                if self.utils.detect_client_portal(soup, page_text, url):
                    agency.digital_capabilities.client_portal = True
                
                
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
        
        # Filter sectors_secondary: exclude any sectors that are in sectors_core
        if all_sectors_secondary:
            sectors_core_lower = {s.lower() for s in all_sectors} if all_sectors else set()
            sectors_secondary_filtered = [
                s for s in all_sectors_secondary 
                if s.lower() not in sectors_core_lower
            ]
            if sectors_secondary_filtered:
                agency.sectors_secondary = sorted(sectors_secondary_filtered)
                self.logger.info(f"✓ Filtered {len(sectors_secondary_filtered)} secondary sectors (excluded {len(all_sectors_secondary) - len(sectors_secondary_filtered)} that are in sectors_core)")
        

        # ==================== APPLY ALL COMMON UTILS EXTRACTIONS ====================
        self.logger.info("=" * 80)
        self.logger.info("🔧 APPLYING AUTOMATIC UTILS EXTRACTIONS")
        self.logger.info("-" * 80)
        
        # This automatically extracts 40+ fields using accumulated text from all pages
        self.extract_all_common_fields(agency, all_text, main_soup)

        self.logger.info("✅ Automatic utils extractions completed")
        self.logger.info("=" * 80)
        
        # Deduplicate evidence_urls by converting to set, then back to sorted list
        agency.evidence_urls = sorted(list(set(self.evidence_urls)))
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
        all_sectors_secondary: Set[str],
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
                self._extract_legal_info(soup, page_text, agency, url)
            
            elif func_name == "certifications":
                self._extract_certifications(soup, page_text, agency, url)
            
            elif func_name == "portals":
                self._extract_portals(soup, agency, url)
            
            elif func_name == "services_detailed":
                self._extract_services_detailed(soup, agency, url)
            
            elif func_name == "jsonld":
                self._extract_jsonld(soup, agency, url)
            
            elif func_name == "office_locations":
                self._extract_office_locations(soup, agency, url)
            
            elif func_name == "sectors_core":
                self._extract_sectors_core(soup, all_sectors, url)
            
            elif func_name == "sectors_secondary":
                self._extract_sectors_secondary(soup, all_sectors_secondary, url)
            
            elif func_name == "statistics":
                self._extract_statistics(soup, agency, url)
            
            elif func_name == "growth_signals":
                self._extract_growth_signals_from_about(soup, page_text, agency, url)
            
            elif func_name == "membership":
                self._extract_membership(soup, page_text, agency, url)
            
            elif func_name == "brand_group_contact":
                self._extract_brand_group_contact(soup, agency, url)
    
    def _extract_logo(self, soup: BeautifulSoup, agency: Agency, url: str) -> None:
        """
        Extract logo from the navbar-brand element on the homepage.
        Prefers the colored logo, falls back to white logo, then to utils.fetch_logo.
        """
        self.logger.info(f"🔍 Extracting logo from {url}")
        
        # Find the navbar-brand element
        navbar_brand = soup.find("a", class_="navbar-brand")
        if not navbar_brand:
            self.logger.warning(f"Could not find navbar-brand on {url}, falling back to utils.fetch_logo")
            logo = self.utils.fetch_logo(soup, url)
            if logo:
                agency.logo_url = logo
                self.logger.info(f"✓ Found logo via utils: {logo} | Source: {url}")
            return
        
        # Try to find the colored logo first (for mobile, d-lg-none)
        logo_img = navbar_brand.find("img", class_="logo-colored")
        if not logo_img:
            # Fall back to white logo (for desktop, d-none d-lg-block)
            logo_img = navbar_brand.find("img", class_="logo-white")
        
        if logo_img:
            logo_url = logo_img.get("src", "")
            if logo_url:
                # Convert relative URL to absolute if needed
                if logo_url.startswith("/"):
                    logo_url = f"{self.WEBSITE_URL}{logo_url}"
                elif not logo_url.startswith("http"):
                    logo_url = f"{self.WEBSITE_URL}/{logo_url}"
                
                agency.logo_url = logo_url
                self.logger.info(f"✓ Found logo: {logo_url} | Source: {url}")
                return
        
        # Fall back to utils.fetch_logo if navbar logo not found
        self.logger.warning(f"Could not find logo in navbar-brand on {url}, falling back to utils.fetch_logo")
        logo = self.utils.fetch_logo(soup, url)
        if logo:
            agency.logo_url = logo
            self.logger.info(f"✓ Found logo via utils: {logo} | Source: {url}")
    
    def _extract_brand_group_contact(self, soup: BeautifulSoup, agency: Agency, url: str) -> None:
        """
        Extract contact email and phone from the House of Covebo contact page.
        Extracts from mailto and tel links, or from text content.
        """
        self.logger.info(f"🔍 Extracting brand group contact information from {url}")
        
        # Find the contact-form-hoc section
        contact_section = soup.find("div", class_="contact-form-hoc")
        if not contact_section:
            self.logger.warning(f"Could not find contact-form-hoc section on {url}")
            return
        
        # Extract email from mailto link
        email_link = contact_section.find("a", href=re.compile(r"^mailto:", re.IGNORECASE))
        if email_link:
            email_href = email_link.get("href", "")
            if email_href.startswith("mailto:"):
                email = email_href.replace("mailto:", "").strip()
                # Use this email if we don't have one yet, or if it's from the brand group
                if not agency.contact_email:
                    agency.contact_email = email
                    self.logger.info(f"✓ Found contact email: {email} | Source: {url}")
                else:
                    # Log that we found brand group email but already have one
                    self.logger.info(f"  Found brand group email: {email} (already have {agency.contact_email}) | Source: {url}")
        else:
            # Fallback: extract email from text
            email_text = contact_section.get_text()
            email_match = re.search(r'\b([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})\b', email_text)
            if email_match:
                email = email_match.group(1)
                if not agency.contact_email:
                    agency.contact_email = email
                    self.logger.info(f"✓ Found contact email from text: {email} | Source: {url}")
        
        # Extract phone from tel link
        phone_link = contact_section.find("a", href=re.compile(r"^tel:", re.IGNORECASE))
        if phone_link:
            phone_href = phone_link.get("href", "")
            if phone_href.startswith("tel:"):
                phone = phone_href.replace("tel:", "").strip()
                # Format phone number (remove spaces, add spaces for readability)
                phone = re.sub(r'\s+', ' ', phone)
                if not agency.contact_phone:
                    agency.contact_phone = phone
                    self.logger.info(f"✓ Found contact phone: {phone} | Source: {url}")
                else:
                    # Log that we found brand group phone but already have one
                    self.logger.info(f"  Found brand group phone: {phone} (already have {agency.contact_phone}) | Source: {url}")
        else:
            # Fallback: extract phone from text
            phone_text = contact_section.get_text()
            # Pattern: "Telefoon: 033 245 0517" or "033 245 0517"
            phone_patterns = [
                r'telefoon[:\s]+([0-9\s\-\(\)]+)',
                r'(\+?\d{1,3}[\s\-]?\(?\d{1,4}\)?[\s\-]?\d{1,4}[\s\-]?\d{1,9})',
            ]
            for pattern in phone_patterns:
                phone_match = re.search(pattern, phone_text, re.IGNORECASE)
                if phone_match:
                    phone = phone_match.group(1).strip()
                    phone = re.sub(r'\s+', ' ', phone)
                    if not agency.contact_phone:
                        agency.contact_phone = phone
                        self.logger.info(f"✓ Found contact phone from text: {phone} | Source: {url}")
                        break
        
        # Add House of Covebo contact page to evidence_urls
        if url not in self.evidence_urls:
            self.evidence_urls.append(url)
        
        self.logger.info(f"✓ Brand group contact extraction completed | Source: {url}")
    
    def _extract_legal_info(self, soup: BeautifulSoup, page_text: str, agency: Agency, url: str) -> None:
        """
        Extract legal information from the privacy statement page.
        Extracts KvK numbers, legal names, and addresses from FAQ accordion cards.
        """
        self.logger.info(f"🔍 Extracting legal information from {url}")
        
        # First, try to extract from FAQ accordion cards
        faq_cards = soup.find_all("div", class_="card")
        
        for card in faq_cards:
            card_header = card.find("div", class_="card-header")
            card_body = card.find("div", class_="card-body")
            
            if not card_header or not card_body:
                continue
            
            header_text = card_header.get_text(strip=True).lower()
            body_text = card_body.get_text(strip=True)
            
            # Look for "WHO ARE WE?" or similar sections
            if "who are we" in header_text or "wie zijn wij" in header_text:
                # Extract KvK numbers from body text
                # Pattern: "Chamber of Commerce: 50572180" or "KvK: 50572180" or "KvK-nummer: 50572180"
                kvk_patterns = [
                    r'(?:chamber\s+of\s+commerce|kvk|kvk-nummer|kvk\s+nummer)[:\s]+(\d{8})',
                    r'kvk[:\s]+(\d{8})',
                    r'(\d{8})(?:\s*\(kvk\)|\s*\(chamber)',
                ]
                
                for pattern in kvk_patterns:
                    matches = re.findall(pattern, body_text, re.IGNORECASE)
                    for match in matches:
                        if isinstance(match, tuple):
                            kvk = match[0]
                        else:
                            kvk = match
                        
                        if kvk and not agency.kvk_number:
                            agency.kvk_number = kvk
                            self.logger.info(f"✓ Found KvK number from FAQ: {kvk} | Source: {url}")
                            break
                
                # Extract legal names
                # Look for "Covebo Uitzendgroep B.V." or "House of Covebo B.V."
                legal_name_patterns = [
                    r'(Covebo\s+Uitzendgroep\s+B\.V\.)',
                    r'(House\s+of\s+Covebo\s+B\.V\.)',
                    r'([A-Z][a-zA-Z\s]+B\.V\.)',
                ]
                
                for pattern in legal_name_patterns:
                    matches = re.findall(pattern, body_text, re.IGNORECASE)
                    for match in matches:
                        if "Covebo" in match or "House of Covebo" in match:
                            if not agency.legal_name:
                                agency.legal_name = match.strip()
                                self.logger.info(f"✓ Found legal name from FAQ: {match.strip()} | Source: {url}")
                                break
                
                # Extract address
                # Pattern: "Ambachtsstraat 13, 3861 RH Nijkerk"
                address_pattern = r'([A-Za-z\s]+(?:\d+[a-z]?)?(?:\s+[A-Za-z]+)*),\s*(\d{4}\s+[A-Z]{2})\s+([A-Za-z]+)'
                address_match = re.search(address_pattern, body_text)
                if address_match:
                    street = address_match.group(1).strip()
                    postal_code = address_match.group(2).strip()
                    city = address_match.group(3).strip()
                    full_address = f"{street}, {postal_code} {city}"
                    
                    # If we don't have HQ city yet, set it
                    if not agency.hq_city:
                        agency.hq_city = city
                        # Try to determine province
                        province = self.utils.map_city_to_province(city)
                        if province:
                            agency.hq_province = province
                        self.logger.info(f"✓ Found HQ address from FAQ: {full_address} | Source: {url}")
                
                # Extract brand group
                # Pattern: "is onderdeel van [Brand Group]" or "House of [Name]"
                brand_group_patterns = [
                    r'is\s+onderdeel\s+van\s+([A-Z][a-zA-Z\s]+(?:B\.V\.)?)',
                    r'(House\s+of\s+[A-Z][a-zA-Z\s]+(?:B\.V\.)?)',
                    r'([A-Z][a-zA-Z\s]+(?:B\.V\.)?),\s+gevestigd.*?is\s+samen\s+met',
                ]
                
                for pattern in brand_group_patterns:
                    match = re.search(pattern, body_text, re.IGNORECASE)
                    if match:
                        brand_group = match.group(1).strip()
                        # Clean up: remove trailing B.V. if it's a parent company mention
                        # But keep it if it's "House of X B.V."
                        if "House of" in brand_group:
                            # Keep "House of X B.V." as is
                            pass
                        else:
                            # For "is onderdeel van X B.V.", we might want to keep or remove B.V.
                            # Let's keep it for now
                            pass
                        
                        if brand_group and not agency.brand_group:
                            agency.brand_group = brand_group
                            self.logger.info(f"✓ Found brand group from FAQ: {brand_group} | Source: {url}")
                            break
        
        # Fallback: Use utils methods to extract from page text
        if not agency.kvk_number:
                kvk = self.utils.fetch_kvk_number(page_text, url)
                if kvk:
                    agency.kvk_number = kvk
                self.logger.info(f"✓ Found KvK number from utils: {kvk} | Source: {url}")
        
        if not agency.legal_name:
            legal_name = self.utils.fetch_legal_name(page_text, "Covebo", url)
            if legal_name:
                agency.legal_name = legal_name
            self.logger.info(f"✓ Found legal name from utils: {legal_name} | Source: {url}")
    
        # Fallback: Extract brand group from page text if not found in FAQ
        if not agency.brand_group:
            brand_group_patterns = [
                r'is\s+onderdeel\s+van\s+([A-Z][a-zA-Z\s]+(?:B\.V\.)?)',
                r'(House\s+of\s+[A-Z][a-zA-Z\s]+(?:B\.V\.)?)',
            ]
            
            for pattern in brand_group_patterns:
                match = re.search(pattern, page_text, re.IGNORECASE)
                if match:
                    brand_group = match.group(1).strip()
                    if brand_group:
                        agency.brand_group = brand_group
                        self.logger.info(f"✓ Found brand group from page text: {brand_group} | Source: {url}")
                        break
        
        # Add privacy statement page to evidence_urls
        if url not in self.evidence_urls:
            self.evidence_urls.append(url)
    
    def _extract_sectors_core(self, soup: BeautifulSoup, all_sectors: Set[str], url: str) -> None:
        """
        Extract core sectors from the homepage sector cards.
        Finds the sectors section first, then extracts sector titles from sector links, excluding "Overig".
        """
        self.logger.info(f"🔍 Extracting core sectors from {url}")
        
        # First, find the sectors section
        sectors_section = soup.find("section", class_=re.compile(r"sectors", re.IGNORECASE))
        if not sectors_section:
            self.logger.warning(f"⚠ No sectors section found on {url}")
            return
        
        # Find all sector links within the sectors section
        sector_links = sectors_section.find_all("a", class_="sector")
        
        if not sector_links:
            self.logger.warning(f"⚠ No sector links found in sectors section on {url}")
            return
        
        sectors_found = []
        
        for link in sector_links:
            # Find the sector title (h3 with class sector__title)
            sector_title = link.find("h3", class_="sector__title")
            if not sector_title:
                continue
            
            sector_name = sector_title.get_text(strip=True)
            
            # Skip "Overig" (Other)
            if sector_name.lower() in ["overig", "other", "overige"]:
                continue
            
            if sector_name and sector_name not in all_sectors:
                all_sectors.add(sector_name)
                sectors_found.append(sector_name)
                self.logger.info(f"✓ Found core sector: '{sector_name}' | Source: {url}")
        
        if sectors_found:
            self.logger.info(f"✓ Total core sectors extracted: {len(sectors_found)} | Source: {url}")
        else:
            self.logger.warning(f"⚠ No core sectors found on {url}")
    
    def _extract_sectors_secondary(self, soup: BeautifulSoup, all_sectors_secondary: Set[str], url: str) -> None:
        """
        Extract secondary sectors from the vacatures page job board filter.
        Finds all checkbox inputs with name="filters[industry][]" and extracts sector names from labels.
        """
        self.logger.info(f"🔍 Extracting secondary sectors from {url}")
        
        # Find the filter block for industry/vakgebied
        filter_block = soup.find("div", class_="filter-block", attrs={"data-filter": "industry"})
        if not filter_block:
            self.logger.warning(f"Could not find filter-block with data-filter='industry' on {url}")
            return
        
        # Find all checkbox inputs with name="filters[industry][]"
        checkboxes = filter_block.find_all("input", {"name": "filters[industry][]", "type": "checkbox"})
        if not checkboxes:
            self.logger.warning(f"Could not find industry checkboxes on {url}")
            return
        
        self.logger.info(f"✓ Found {len(checkboxes)} sector checkboxes | Source: {url}")
        
        sectors_found = []
        
        for checkbox in checkboxes:
            try:
                # Find the associated label
                checkbox_id = checkbox.get("id", "")
                if not checkbox_id:
                    continue
                
                label = filter_block.find("label", {"for": checkbox_id})
                if not label:
                    continue
                
                # Extract sector name from the label's span
                span = label.find("span")
                if span:
                    sector_name = span.get_text(strip=True)
                else:
                    sector_name = label.get_text(strip=True)
                
                if sector_name:
                    # Normalize the sector name (remove HTML entities)
                    sector_name = html.unescape(sector_name)
                    all_sectors_secondary.add(sector_name)
                    sectors_found.append(sector_name)
                    self.logger.info(f"  ✓ Secondary sector: '{sector_name}' | Source: {url}")
            
            except Exception as e:
                self.logger.error(f"Error processing sector checkbox on {url}: {e}")
                continue
        
        if sectors_found:
            self.logger.info(f"✓ Total secondary sectors extracted: {len(sectors_found)} | Source: {url}")
        else:
            self.logger.warning(f"⚠ No secondary sectors found on {url}")
    
    def _extract_statistics(self, soup: BeautifulSoup, agency: Agency, url: str) -> None:
        """
        Extract statistics from the "ons verhaal" page.
        Looks for numbers and statistics mentioned in the text.
        """
        import re
        
        self.logger.info(f"🔍 Extracting statistics from {url}")
        
        page_text = soup.get_text(separator=" ", strip=True)
        text_lower = page_text.lower()
        
        # Look for candidate pool size mentions
        # Pattern: "X kandidaten" or "X medewerkers"
        candidate_patterns = [
            r'(\d+(?:[.,]\d+)?)\s*(?:k|duizend|thousand)?\s*kandidaten',
            r'(\d+(?:[.,]\d+)?)\s*(?:k|duizend|thousand)?\s*medewerkers',
            r'(\d+(?:[.,]\d+)?)\s*(?:k|duizend|thousand)?\s*werknemers',
        ]
        
        for pattern in candidate_patterns:
            match = re.search(pattern, text_lower)
            if match:
                value = float(match.group(1).replace(',', '.'))
                if 'k' in match.group(0) or 'duizend' in match.group(0) or 'thousand' in match.group(0):
                    value = value * 1_000
                value = int(value)
                
                if not agency.candidate_pool_size_estimate or agency.candidate_pool_size_estimate < value:
                    agency.candidate_pool_size_estimate = value
                    self.logger.info(f"✓ Candidate pool size: {value:,} | Source: {url}")
                    break
        
        # Look for annual placements or weekly placements
        placement_patterns = [
            r'(\d+(?:[.,]\d+)?)\s*(?:k|duizend|thousand)?\s*kandidaten\s*per\s*week',
            r'(\d+(?:[.,]\d+)?)\s*(?:k|duizend|thousand)?\s*plaatsingen\s*per\s*week',
            r'(\d+(?:[.,]\d+)?)\s*(?:k|duizend|thousand)?\s*kandidaten\s*per\s*jaar',
            r'(\d+(?:[.,]\d+)?)\s*(?:k|duizend|thousand)?\s*plaatsingen\s*per\s*jaar',
        ]
        
        for pattern in placement_patterns:
            match = re.search(pattern, text_lower)
            if match:
                value = float(match.group(1).replace(',', '.'))
                if 'k' in match.group(0) or 'duizend' in match.group(0) or 'thousand' in match.group(0):
                    value = value * 1_000
                
                if 'per week' in match.group(0) or 'per week' in pattern:
                    annual_placements = int(value * 52)
                else:
                    annual_placements = int(value)
                
                if not agency.annual_placements_estimate or agency.annual_placements_estimate < annual_placements:
                    agency.annual_placements_estimate = annual_placements
                    self.logger.info(f"✓ Annual placements estimate: {annual_placements:,} | Source: {url}")
                    break
        
        # Look for office count: "40 vestigingen" or "40+ vestigingen"
        office_patterns = [
            r'(\d+)\+?\s*vestigingen',
            r'(\d+)\+?\s*offices',
            r'(\d+)\+?\s*locations',
        ]
        for pattern in office_patterns:
            match = re.search(pattern, text_lower)
            if match:
                office_count = int(match.group(1))
                # Store as a note or use for validation (we already extract actual office locations)
                self.logger.info(f"  Found office count: {office_count} | Source: {url}")
                break
        
        # Look for countries: "7 Europese landen" or "zeven Europese landen"
        country_patterns = [
            r'(\d+)\s*(?:europese\s+)?landen',
            r'(zeven|acht|negen|tien)\s*(?:europese\s+)?landen',
        ]
        country_map = {"zeven": 7, "acht": 8, "negen": 9, "tien": 10}
        for pattern in country_patterns:
            match = re.search(pattern, text_lower)
            if match:
                country_text = match.group(1).lower()
                if country_text in country_map:
                    country_count = country_map[country_text]
                else:
                    country_count = int(country_text)
                self.logger.info(f"  Found countries: {country_count} | Source: {url}")
                break
        
        # Look for rating: "7.9 beoordeling"
        rating_patterns = [
            r'(\d+[.,]\d+)\s*beoordeling',
            r'rating[:\s]+(\d+[.,]\d+)',
            r'score[:\s]+(\d+[.,]\d+)',
        ]
        for pattern in rating_patterns:
            match = re.search(pattern, text_lower)
            if match:
                rating = float(match.group(1).replace(',', '.'))
                self.logger.info(f"  Found rating: {rating} | Source: {url}")
                break
        
        # Look for founding year: "oprichting in 2001" or "sinds 2001"
        # Note: Agency model doesn't have founded_year field, so we just log it
        founding_patterns = [
            r'oprichting\s+(?:in\s+)?(\d{4})',
            r'sinds\s+(\d{4})',
            r'opgericht\s+(?:in\s+)?(\d{4})',
            r'founded\s+(?:in\s+)?(\d{4})',
        ]
        for pattern in founding_patterns:
            match = re.search(pattern, text_lower)
            if match:
                founding_year = int(match.group(1))
                if 1900 <= founding_year <= 2100:  # Sanity check
                    self.logger.info(f"  Found founding year: {founding_year} (not stored in model) | Source: {url}")
                    break
        
        # Look for years of experience: "ruim 20 jaar ervaring" or "20 jaar ervaring"
        experience_patterns = [
            r'ruim\s+(\d+)\s+jaar\s+ervaring',
            r'(\d+)\s+jaar\s+ervaring',
            r'(\d+)\s+years?\s+of\s+experience',
        ]
        for pattern in experience_patterns:
            match = re.search(pattern, text_lower)
            if match:
                years = int(match.group(1))
                self.logger.info(f"  Found years of experience: {years} | Source: {url}")
                break
        
        self.logger.info(f"✓ Statistics extraction completed | Source: {url}")
    
    def _extract_growth_signals_from_about(self, soup: BeautifulSoup, page_text: str, agency: Agency, url: str) -> None:
        """
        Extract growth signals from the "ons verhaal" page.
        Looks for keywords and phrases that indicate growth, expansion, or success.
        """
        self.logger.info(f"🔍 Extracting growth signals from {url}")
        
        text_lower = page_text.lower()
        
        # Check for specific growth signals mentioned on this page
        growth_signals_to_check = [
            ("international employees", "Internationale medewerkers"),
            ("internationale medewerkers", "Internationale medewerkers"),
            ("we have a lot to celebrate", "Veel successen te vieren"),
            ("veel te vieren", "Veel successen te vieren"),
            ("winner's mentality", "Winnaarsmentaliteit"),
            ("winnaarsmentaliteit", "Winnaarsmentaliteit"),
            ("we go to extremes every day", "Dagelijks tot het uiterste gaan"),
            ("tot het uiterste", "Dagelijks tot het uiterste gaan"),
            ("specialist in werk", "Specialist in werk"),
            ("we celebrate the successes", "Successen vieren"),
            ("successen vieren", "Successen vieren"),
            ("we are close to our customers", "Dicht bij klanten"),
            ("dicht bij klanten", "Dicht bij klanten"),
            ("we look ahead", "Vooruitkijken"),
            ("vooruitkijken", "Vooruitkijken"),
            ("omzetcijfers overtreffen de marktgemiddelden", "Omzet boven marktgemiddelde"),
            ("overtreffen de marktgemiddelden", "Omzet boven marktgemiddelde"),
            ("40+ vestigingen", "40+ vestigingen in Nederland"),
            ("zeven europese landen", "Internationale werving in 7 landen"),
            ("ruim 20 jaar ervaring", "Ruim 20 jaar ervaring"),
            ("internationale werving", "Internationale werving"),
        ]
        
        for keyword, signal_text in growth_signals_to_check:
            if keyword in text_lower:
                if not agency.growth_signals:
                    agency.growth_signals = []
                if signal_text not in agency.growth_signals:
                    agency.growth_signals.append(signal_text)
                    self.logger.info(f"✓ Found growth signal: '{signal_text}' | Source: {url}")
        
        # Also use utils to extract general growth signals
        growth_signals = self.utils.fetch_growth_signals(page_text, url)
        if growth_signals:
            if not agency.growth_signals:
                agency.growth_signals = []
            for signal in growth_signals:
                if signal not in agency.growth_signals:
                    agency.growth_signals.append(signal)
                    self.logger.info(f"✓ Found growth signal: '{signal}' | Source: {url}")
        
        self.logger.info(f"✓ Growth signals extraction completed | Source: {url}")
    
    def _extract_membership(self, soup: BeautifulSoup, page_text: str, agency: Agency, url: str) -> None:
        """
        Extract membership information from the werkgever page.
        Specifically looks for ABU-lidmaatschap and sets both membership and cao_type.
        """
        self.logger.info(f"🔍 Extracting membership from {url}")
        
        text_lower = page_text.lower()
        
        # Initialize membership list if needed
        if not agency.membership:
            agency.membership = []
        
        # Look for ABU membership: "ABU-lidmaatschap" or "ABU lidmaatschap" or "ABU-certificaat"
        abu_patterns = [
            r'\bABU[-\s]?lidmaatschap\b',
            r'\bABU[-\s]?certificaat\b',
            r'\bABU[-\s]?lid\b',
            r'\b(?:lid\s+van\s+)?ABU\b',
            r'\bAlgemene\s+Bond\s+Uitzendondernemingen\b',
        ]
        
        abu_found = False
        for pattern in abu_patterns:
            if re.search(pattern, text_lower, re.IGNORECASE):
                abu_found = True
                break
        
        # Also check for ABU in links (e.g., "ABU-certificaat" link)
        if not abu_found:
            abu_links = soup.find_all("a", href=re.compile(r"abu", re.IGNORECASE))
            for link in abu_links:
                link_text = link.get_text(strip=True).lower()
                if "abu" in link_text:
                    abu_found = True
                    break
        
        if abu_found:
            # Add ABU to membership
            if "ABU" not in agency.membership:
                agency.membership.append("ABU")
                self.logger.info(f"✓ Found membership: ABU | Source: {url}")
            
            # Set CAO type to ABU if not already set
            if agency.cao_type == CaoType.ONBEKEND:
                agency.cao_type = CaoType.ABU
                self.logger.info(f"✓ Set CAO type: ABU (from ABU membership) | Source: {url}")
        
        # Look for NBBU membership
        nbbu_patterns = [
            r'\bNBBU[-\s]?lidmaatschap\b',
            r'\bNBBU[-\s]?certificaat\b',
            r'\bNBBU[-\s]?lid\b',
            r'\b(?:lid\s+van\s+)?NBBU\b',
        ]
        
        nbbu_found = False
        for pattern in nbbu_patterns:
            if re.search(pattern, text_lower, re.IGNORECASE):
                nbbu_found = True
                break
        
        if nbbu_found:
            if "NBBU" not in agency.membership:
                agency.membership.append("NBBU")
                self.logger.info(f"✓ Found membership: NBBU | Source: {url}")
            
            # Set CAO type to NBBU if not already set and ABU not found
            if agency.cao_type == CaoType.ONBEKEND:
                agency.cao_type = CaoType.NBBU
                self.logger.info(f"✓ Set CAO type: NBBU (from NBBU membership) | Source: {url}")
        
        self.logger.info(f"✓ Membership extraction completed | Source: {url}")
    
    def _extract_certifications(self, soup: BeautifulSoup, page_text: str, agency: Agency, url: str) -> None:
        """
        Extract certifications from the certifications page.
        Finds certification names in strong tags and maps them to standardized names.
        """
        self.logger.info(f"🔍 Extracting certifications from {url}")
        
        # First, extract from strong tags
        strong_tags = soup.find_all("strong")
        cert_texts = []
        extracted_certs = []
        
        for strong in strong_tags:
            # Get text from strong tag, handling <br> tags inside
            text = strong.get_text(separator=" ", strip=True)
            if text:
                # Clean up the text (remove extra whitespace, normalize)
                text = re.sub(r'\s+', ' ', text).strip()
                # Look for certification keywords
                if any(keyword in text.lower() for keyword in [
                    "certificaat", "certificering", "certification",
                    "keurmerk", "iso", "abu", "nbbu", "vcu", "nen", "sna", "snf", "sbb"
                ]):
                    cert_texts.append(text)
                    self.logger.info(f"✓ Found certification text from strong tag: {text} | Source: {url}")
                    
                    # Try to extract certification name directly from the text
                    # Remove common words like "certificaat", "certificering", etc.
                    cert_name = text.lower()
                    # Remove "certificaat", "certificering", "certification"
                    cert_name = re.sub(r'\s*(certificaat|certificering|certification|keurmerk)\s*', '', cert_name, flags=re.IGNORECASE)
                    cert_name = cert_name.strip()
                    
                    # Map common variations to standardized names
                    cert_mapping = {
                        "abu": "ABU",
                        "nbbu": "NBBU",
                        "vcu": "VCU",
                        "nen": "NEN-4400-1",
                        "nen-4400-1": "NEN-4400-1",
                        "nen 4400-1": "NEN-4400-1",
                        "sna": "SNA",
                        "snf": "SNF",
                        "sbb": "SBB",
                        "g-rekening": "G-rekening",
                        "g rekening": "G-rekening",
                        "verklaring belastingdienst": "Verklaring Belastingdienst",
                        "kvk": "KvK",
                    }
                    
                    # Check if cert_name matches any mapping
                    cert_name_lower = cert_name.lower()
                    for key, value in cert_mapping.items():
                        if key in cert_name_lower:
                            if value not in extracted_certs:
                                extracted_certs.append(value)
                                self.logger.info(f"✓ Extracted certification from strong tag: {value} | Source: {url}")
                            break
        
        # Combine strong tag texts with page text for utils extraction
        combined_text = page_text
        if cert_texts:
            combined_text = " ".join(cert_texts) + " " + page_text
        
        # Use the utils method to extract certifications from combined text
        certs = self.utils.fetch_certifications(combined_text, url)
        
        # Merge extracted certs from strong tags with utils results
        if extracted_certs:
            if not certs:
                certs = []
            for cert in extracted_certs:
                if cert not in certs:
                    certs.append(cert)
        
        if certs:
            # Merge with existing certifications
            if not agency.certifications:
                agency.certifications = []
            for cert in certs:
                if cert not in agency.certifications:
                    agency.certifications.append(cert)
            agency.certifications = sorted(list(set(agency.certifications)))
            self.evidence_urls.append(url)
            self.logger.info(f"✓ Total certifications extracted: {len(agency.certifications)} | Source: {url}")
        else:
            self.logger.warning(f"⚠ No certifications found on {url}")
    
    def _extract_portals(self, soup: BeautifulSoup, agency: Agency, url: str) -> None:
        """
        Extract portal information from the portal page.
        Finds both employee (candidate) and employer (client) portal links.
        """
        self.logger.info(f"🔍 Extracting portal information from {url}")
        
        # Find all entrance blocks (portal sections)
        entrance_blocks = soup.find_all("div", class_="entrance-block")
        
        candidate_portal_url = None
        client_portal_url = None
        
        for block in entrance_blocks:
            # Get the title to determine if it's employee or employer portal
            title_elem = block.find("h2", class_="component--title")
            if not title_elem:
                continue
            
            title_text = title_elem.get_text(strip=True).lower()
            
            # Find the portal link in this block
            portal_link = block.find("a", href=True)
            if not portal_link:
                continue
            
            portal_url = portal_link.get("href", "")
            
            # Check if it's employee/candidate portal
            if any(keyword in title_text for keyword in ["werknemer", "employee", "ik ben werknemer", "i am an employee"]):
                if "covebo_resource" in portal_url or "werknemer" in portal_url.lower():
                    candidate_portal_url = portal_url
                    agency.digital_capabilities.candidate_portal = True
                    self.logger.info(f"✓ Found candidate portal: {candidate_portal_url} | Source: {url}")
            
            # Check if it's employer/client portal
            elif any(keyword in title_text for keyword in ["werkgever", "employer", "ik ben werkgever", "i am an employer"]):
                if "covebo_klant" in portal_url or "werkgever" in portal_url.lower():
                    client_portal_url = portal_url
                    agency.digital_capabilities.client_portal = True
                    self.logger.info(f"✓ Found client portal: {client_portal_url} | Source: {url}")
        
        # Fallback: Check for portal links directly by URL pattern
        if not candidate_portal_url:
            candidate_link = soup.find("a", href=lambda x: x and "covebo_resource" in x)
            if candidate_link:
                candidate_portal_url = candidate_link.get("href", "")
                agency.digital_capabilities.candidate_portal = True
                self.logger.info(f"✓ Found candidate portal (fallback): {candidate_portal_url} | Source: {url}")
        
        if not client_portal_url:
            client_link = soup.find("a", href=lambda x: x and "covebo_klant" in x)
            if client_link:
                client_portal_url = client_link.get("href", "")
                agency.digital_capabilities.client_portal = True
                self.logger.info(f"✓ Found client portal (fallback): {client_portal_url} | Source: {url}")
        
        # Add portal URLs to evidence_urls
        if candidate_portal_url:
            self.evidence_urls.append(candidate_portal_url)
        if client_portal_url:
            self.evidence_urls.append(client_portal_url)
        
        # Also add the portal page itself
        self.evidence_urls.append(url)
        
        if candidate_portal_url or client_portal_url:
            self.logger.info(f"✓ Portal extraction completed | Source: {url}")
        else:
            self.logger.warning(f"⚠ No portal links found on {url}")
    
    def _extract_services_detailed(self, soup: BeautifulSoup, agency: Agency, url: str) -> None:
        """
        Extract detailed services from the services page.
        Maps service titles to AgencyServices fields and adds service URLs to evidence_urls.
        """
        self.logger.info(f"🔍 Extracting detailed services from {url}")
        
        # Find all service sections (h3 headings in content sections)
        service_sections = soup.find_all("section", class_="content-text")
        services_found = []
        service_urls = []
        
        # Service mapping: Dutch/English service names to AgencyServices fields
        service_mapping = {
            "uitzenden": "uitzenden",
            "broadcasting": "uitzenden",  # English translation
            "temporary employment": "uitzenden",
            "detachering": "detacheren",
            "secondment": "detacheren",
            "werving": "werving_selectie",
            "recruitment": "werving_selectie",
            "selectie": "werving_selectie",
            "selection": "werving_selectie",
            "zzp": "zzp_bemiddeling",
            "self-employed": "zzp_bemiddeling",
            "freelance": "zzp_bemiddeling",
            "poolmanagement": "inhouse_services",
            "pool management": "inhouse_services",
            "test": "opleiden_ontwikkelen",
            "training": "opleiden_ontwikkelen",
            "opleiden": "opleiden_ontwikkelen",
            "ontwikkelen": "opleiden_ontwikkelen",
        }
        
        for section in service_sections:
            # Find all h3 headings (service titles)
            h3_headings = section.find_all("h3")
            
            for h3 in h3_headings:
                service_title = h3.get_text(strip=True).lower()
                
                # Find the service link if available
                service_link = h3.find_next("a", class_="btn-link")
                if service_link:
                    service_url = service_link.get("href", "")
                    if service_url and service_url not in service_urls:
                        service_urls.append(service_url)
                
                # Map service title to AgencyServices field
                for keyword, service_field in service_mapping.items():
                    if keyword in service_title:
                        if not hasattr(agency.services, service_field):
                            continue
                        setattr(agency.services, service_field, True)
                        if service_title not in services_found:
                            services_found.append(service_title)
                        self.logger.info(f"✓ Found service: {service_title} -> {service_field} | Source: {url}")
                        break
        
        # Also check page text for additional service mentions
        page_text = soup.get_text(separator=" ", strip=True).lower()
        
        # Check for ZZP (self-employed) service
        if "zzp" in page_text or "zelfstandige" in page_text or "self-employed" in page_text:
            agency.services.zzp_bemiddeling = True
            if "zzp" not in [s.lower() for s in services_found]:
                services_found.append("zzp")
        
        # Add service URLs to evidence_urls
        for service_url in service_urls:
            # Convert relative URLs to absolute
            if service_url.startswith("/"):
                service_url = f"{self.WEBSITE_URL}{service_url}"
            elif not service_url.startswith("http"):
                service_url = f"{self.WEBSITE_URL}/{service_url}"
            
            if service_url not in self.evidence_urls:
                self.evidence_urls.append(service_url)
                self.logger.info(f"✓ Added service URL to evidence: {service_url}")
        
        # Add the services page itself to evidence_urls
        if url not in self.evidence_urls:
            self.evidence_urls.append(url)
        
        if services_found:
            self.logger.info(f"✓ Total services extracted: {len(services_found)} | Source: {url}")
        else:
            self.logger.warning(f"⚠ No services found on {url}")
    
    def _extract_jsonld(self, soup: BeautifulSoup, agency: Agency, url: str) -> None:
        """
        Extract data from JSON-LD schema on the homepage.
        Extracts services from description and logo if available.
        """
        self.logger.info(f"🔍 Extracting JSON-LD schema from {url}")
        
        # Find all JSON-LD script tags
        scripts = soup.find_all("script", type="application/ld+json")
        
        if not scripts:
            self.logger.warning(f"⚠ No JSON-LD schema found on {url}")
            return
        
        services_found = []
        
        for script in scripts:
            try:
                # Parse JSON-LD
                json_data = json.loads(script.string)
                
                # Handle both single objects and @graph arrays
                items = []
                if isinstance(json_data, dict):
                    if "@graph" in json_data:
                        items = json_data["@graph"]
                    else:
                        items = [json_data]
                elif isinstance(json_data, list):
                    items = json_data
                
                for item in items:
                    # Extract services from description
                    description = item.get("description", "")
                    if description:
                        description_lower = description.lower()
                        
                        # Service mapping from description text
                        if "uitzenden" in description_lower or "temporary" in description_lower:
                            agency.services.uitzenden = True
                            services_found.append("uitzenden")
                            self.logger.info(f"✓ Found service from JSON-LD: uitzenden | Source: {url}")
                        
                        if "detacheren" in description_lower or "detachering" in description_lower or "secondment" in description_lower:
                            agency.services.detacheren = True
                            services_found.append("detacheren")
                            self.logger.info(f"✓ Found service from JSON-LD: detacheren | Source: {url}")
                        
                        if "werving" in description_lower or "selectie" in description_lower or "recruitment" in description_lower or "selection" in description_lower:
                            agency.services.werving_selectie = True
                            services_found.append("werving_selectie")
                            self.logger.info(f"✓ Found service from JSON-LD: werving_selectie | Source: {url}")
                    
                    # Extract logo if available
                    logo = item.get("logo")
                    if logo:
                        if isinstance(logo, dict):
                            logo_url = logo.get("url") or logo.get("@id")
                        elif isinstance(logo, str):
                            logo_url = logo
                        else:
                            logo_url = None
                        
                        if logo_url and not agency.logo_url:
                            # Convert relative URLs to absolute
                            if logo_url.startswith("/"):
                                logo_url = f"{self.WEBSITE_URL}{logo_url}"
                            elif not logo_url.startswith("http"):
                                logo_url = f"{self.WEBSITE_URL}/{logo_url}"
                            
                            agency.logo_url = logo_url
                            self.logger.info(f"✓ Found logo from JSON-LD: {logo_url} | Source: {url}")
                    
                    # Also check for logo in Organization type
                    if item.get("@type") == "Organization":
                        logo = item.get("logo")
                        if logo:
                            if isinstance(logo, dict):
                                logo_url = logo.get("url") or logo.get("@id")
                            elif isinstance(logo, str):
                                logo_url = logo
                            else:
                                logo_url = None
                            
                            if logo_url and not agency.logo_url:
                                if logo_url.startswith("/"):
                                    logo_url = f"{self.WEBSITE_URL}{logo_url}"
                                elif not logo_url.startswith("http"):
                                    logo_url = f"{self.WEBSITE_URL}/{logo_url}"
                                
                                agency.logo_url = logo_url
                                self.logger.info(f"✓ Found logo from JSON-LD Organization: {logo_url} | Source: {url}")
            
            except json.JSONDecodeError as e:
                self.logger.warning(f"⚠ Failed to parse JSON-LD schema: {e} | Source: {url}")
            except Exception as e:
                self.logger.warning(f"⚠ Error extracting JSON-LD data: {e} | Source: {url}")
        
        if services_found:
            self.logger.info(f"✓ Total services extracted from JSON-LD: {len(services_found)} | Source: {url}")
        
        # Add homepage to evidence_urls
        if url not in self.evidence_urls:
            self.evidence_urls.append(url)
    
    def _extract_office_locations(self, soup: BeautifulSoup, agency: Agency, url: str) -> None:
        """
        Extract office locations from the vestigingen page.
        First tries to extract from data-markers JSON attribute, then falls back to card elements.
        """
        self.logger.info(f"🔍 Extracting office locations from {url}")
        
        if not agency.office_locations:
            agency.office_locations = []
        
        offices_found = []
        office_urls = set()
        
        # First, try to extract from data-markers JSON attribute
        map_items = soup.find("div", id="map-items")
        if map_items:
            data_markers = map_items.get("data-markers", "")
            if data_markers:
                try:
                    # Decode HTML entities in the JSON string
                    decoded_markers = html.unescape(data_markers)
                    markers = json.loads(decoded_markers)
                    
                    for marker in markers:
                        title = marker.get("title", "")
                        address = marker.get("address", "")
                        
                        # Extract city from title (e.g., "Covebo Almelo Bouw & Techniek" -> "Almelo")
                        # Or from address if title doesn't contain city
                        city_name = None
                        
                        # Try to extract city from title
                        # Format: "Covebo [City]" or "Covebo [City] [Type]"
                        if "Covebo" in title:
                            parts = title.replace("Covebo", "").strip().split()
                            if parts:
                                # First word after "Covebo" is usually the city
                                city_name = parts[0]
                        
                        # If no city from title, try to extract from address
                        if not city_name and address:
                            # Address format: "Street, Postal Code City" or "Street, Postal Code"
                            # Try to find city name (usually after postal code)
                            # Match postal code pattern (4 digits, space, 2 letters)
                            postal_match = re.search(r'\d{4}\s?[A-Z]{2}', address)
                            if postal_match:
                                # City might be after postal code
                                after_postal = address[postal_match.end():].strip()
                                if after_postal:
                                    # Take first word as city
                                    city_name = after_postal.split()[0] if after_postal.split() else None
                        
                        if not city_name:
                            continue
                        
                        # Determine province using utils
                        province = self.utils.map_city_to_province(city_name)
                        
                        # Create office location
                        office = OfficeLocation(
                            city=city_name,
                            province=province,
                        )
                        
                        # Check if already exists (avoid duplicates)
                        if not any(off.city == city_name for off in offices_found):
                            offices_found.append(office)
                            self.logger.info(f"✓ Office from JSON: {city_name}, {province} | Source: {url}")
                    
                    self.logger.info(f"✓ Extracted {len(offices_found)} offices from JSON markers | Source: {url}")
                except (json.JSONDecodeError, Exception) as e:
                    self.logger.warning(f"⚠ Failed to parse data-markers JSON: {e} | Source: {url}")
        
        # Fallback: Extract from card elements if JSON extraction didn't work or found fewer offices
        if len(offices_found) == 0:
            cards = soup.find_all("div", class_="cvb-card--office")
            if not cards:
                # Try alternative class
                cards = soup.find_all("div", class_=lambda x: x and "cvb-card" in x and "office" in x)
            
            for card in cards:
                try:
                    # Extract title from h3
                    h3 = card.find("h3", class_="cvb-card__title")
                    if not h3:
                        continue
                    
                    title_text = h3.get_text(strip=True)
                    
                    # Extract city from title
                    # Format: "Covebo [City]" or "Covebo [City] [Type]"
                    city_name = None
                    if "Covebo" in title_text:
                        parts = title_text.replace("Covebo", "").strip().split()
                        if parts:
                            city_name = parts[0]
                    
                    # If no city from title, try address
                    if not city_name:
                        address_elem = card.find("span", class_="address")
                        if address_elem:
                            address = address_elem.get_text(strip=True)
                            # Extract city from address (after postal code)
                            postal_match = re.search(r'\d{4}\s?[A-Z]{2}', address)
                            if postal_match:
                                after_postal = address[postal_match.end():].strip()
                                if after_postal:
                                    city_name = after_postal.split()[0] if after_postal.split() else None
                    
                    if not city_name:
                        continue
                    
                    # Get office URL from the link
                    link = card.find("a", class_="cvb-card__block-link", href=True)
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
                    if not any(off.city == city_name for off in offices_found):
                        offices_found.append(office)
                        self.logger.info(f"✓ Office from card: {city_name}, {province} | Source: {url}")
                
                except Exception as e:
                    self.logger.error(f"Error processing office card: {e}")
                    continue
        
        # Add offices to agency
        for office in offices_found:
            if not any(off.city == office.city for off in agency.office_locations):
                agency.office_locations.append(office)
        
        # Add office URLs to evidence_urls
        for office_url in office_urls:
            if office_url not in self.evidence_urls:
                self.evidence_urls.append(office_url)
                self.logger.info(f"✓ Added office URL to evidence: {office_url}")
        
        # Add the vestigingen page itself to evidence_urls
        if url not in self.evidence_urls:
            self.evidence_urls.append(url)
        
        if offices_found:
            self.logger.info(f"✓ Total offices extracted: {len(offices_found)} | Source: {url}")
        else:
            self.logger.warning(f"⚠ No offices found on {url}")


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

