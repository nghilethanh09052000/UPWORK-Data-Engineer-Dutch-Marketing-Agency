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
            "url": "https://www.randstad.nl/werkgevers/onze-hr-diensten",
            "functions": ['services'],
        },
        {
            "name": "werkgevers",
            "url": "https://www.randstad.nl/werkgevers",
            "functions": ['statistics', 'growth_signals'],
        },
        {
            "name": "vakgebieden",
            "url": "https://www.randstad.nl/werkgevers/onze-hr-diensten/personeel-vakgebied#onze-vakgebieden",
            "functions": ['sectors'],
        },
        {
            "name": "werkgevers_contact",
            "url": "https://www.randstad.nl/werkgevers/contact",
            "functions": ['werkgevers_contact_phone'],
        },
        {
            "name": "hq",
            "url": "https://www.randstad.nl/over-randstad/contact",
            "functions": ['hq'],
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
        }
    ]

    def scrape(self) -> Agency:
        self.logger.info(f"Starting scrape of {self.AGENCY_NAME}")
        
        # Initialize utils
        self.utils = AgencyScraperUtils(logger=self.logger)
        agency = self.create_base_agency()
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
        
        # AI capabilities: Only set to True if explicitly stated on website
        # Removed API-based detection as per client feedback
        
        # Finalize sectors: normalize first, then filter
        from staffing_agency_scraper.lib.normalize import normalize_sectors
        
        if all_sectors:
            original_sectors = sorted(list(all_sectors))
            agency.sectors_core = normalize_sectors(original_sectors)
            if agency.sectors_core != original_sectors:
                self.logger.info(f"✓ Normalized sectors_core: {len(original_sectors)} -> {len(agency.sectors_core)} | Original: {original_sectors[:5]}... | Normalized: {agency.sectors_core[:5]}...")
        
        # Normalize sectors_secondary first, then filter out any that are in sectors_core
        if all_sectors_secondary:
            original_secondary = sorted(list(all_sectors_secondary))
            normalized_secondary = normalize_sectors(original_secondary)
            if normalized_secondary != original_secondary:
                self.logger.info(f"✓ Normalized sectors_secondary: {len(original_secondary)} -> {len(normalized_secondary)} | Original: {original_secondary[:5]}... | Normalized: {normalized_secondary[:5]}...")
            
            # Filter: exclude any sectors that are already in sectors_core (case-insensitive comparison)
            if agency.sectors_core:
                sectors_core_lower = {s.lower() for s in agency.sectors_core}
                sectors_secondary_filtered = [
                    s for s in normalized_secondary 
                    if s.lower() not in sectors_core_lower
                ]
                excluded_count = len(normalized_secondary) - len(sectors_secondary_filtered)
                if excluded_count > 0:
                    self.logger.info(f"✓ Filtered sectors_secondary: removed {excluded_count} duplicate(s) that are in sectors_core")
                agency.sectors_secondary = sectors_secondary_filtered
            else:
                agency.sectors_secondary = normalized_secondary
        
        # agency.evidence_urls = self.get_filtered_evidence_urls()
        agency.evidence_urls = self.evidence_urls.copy()
        agency.collected_at = self.collected_at
        agency.avg_hourly_rate_low = None
        agency.avg_hourly_rate_high = None
        agency.min_hours_per_week = None
        
        self.logger.info("=" * 80)
        self.logger.info(f"✅ Completed scrape of {self.AGENCY_NAME}")
        self.logger.info(f"📄 Evidence URLs: {len(agency.evidence_urls)}")
        self.logger.info("=" * 80)

        with open("all_text.txt", "w") as f:
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
            
            elif func_name == "legal_footer":
                self._extract_legal_footer(soup, agency, url)
            
            elif func_name == "werkgevers_contact_phone":
                self._extract_werkgevers_contact_phone(soup, agency, url)
            
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
    
    def _extract_header(self, soup: BeautifulSoup, agency: Agency, url: str) -> None:
        """Extract data from header navigation and footer (mobile app detection)."""
        # Look for navigation menu
        nav = soup.find("nav")
        if nav:
            # Extract mobile app presence from nav
            if nav.find("a", href=lambda x: x and ("apps.apple.com" in x or "itunes.apple.com" in x or "play.google.com" in x)):
                agency.digital_capabilities.mobile_app = True
                self.logger.info(f"✓ Found mobile app links in navigation | Source: {url}")
        
        # Also check footer for mobile app links (more common location)
        footer = soup.find("footer")
        if footer:
            # Look for Apple App Store links (itunes.apple.com or apps.apple.com)
            app_store_link = footer.find("a", href=lambda x: x and ("itunes.apple.com" in x or "apps.apple.com" in x))
            # Look for Google Play Store links
            google_play_link = footer.find("a", href=lambda x: x and "play.google.com" in x)
            
            if app_store_link or google_play_link:
                agency.digital_capabilities.mobile_app = True
                stores = []
                if app_store_link:
                    stores.append("Apple App Store")
                if google_play_link:
                    stores.append("Google Play Store")
                self.logger.info(f"✓ Found mobile app links in footer: {', '.join(stores)} | Source: {url}")
    
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
        """
        Extract services from the HR services page with card-based layout.
        
        Expected HTML structure:
        <div class="employers__block__content">
          <div class="employers__block__cards">
            <a class="employers__card" data-analytics-label="uitzenden" href="/uitzenden">
              <div class="company__name">uitzenden</div>
            </a>
            ...
          </div>
        </div>
        """
        self.logger.info(f"🔍 Extracting services from {url}")
        
        services_found = []
        
        # First, try to find within employers__block__content (more specific)
        content_blocks = soup.find_all("div", class_="employers__block__content")
        for content_block in content_blocks:
            cards_container = content_block.find("div", class_="employers__block__cards")
            if cards_container:
                service_cards = cards_container.find_all("a", class_="employers__card")
                if service_cards:
                    # Found service cards, process them
                    for card in service_cards:
                        # Get service name from company__name div or data-analytics-label
                        company_name_div = card.find("div", class_="company__name")
                        service_name = None
                        
                        if company_name_div:
                            service_name = company_name_div.get_text(strip=True).lower()
                        else:
                            # Fallback to data-analytics-label
                            service_name = card.get("data-analytics-label", "").lower()
                        
                        if not service_name:
                            continue
                        
                        # Skip non-service cards (like "onze diensten", "contact")
                        if service_name in ["onze diensten", "contact", "onze hr-diensten"]:
                            continue
                        
                        # Map service names to service fields
                        service_name_clean = service_name.strip()
                        
                        # uitzenden
                        if service_name_clean == "uitzenden":
                            agency.services.uitzenden = True
                            services_found.append("uitzenden")
                        
                        # detacheren
                        elif service_name_clean == "detacheren":
                            agency.services.detacheren = True
                            services_found.append("detacheren")
                        
                        # werving & selectie
                        elif "werving" in service_name_clean and "selectie" in service_name_clean:
                            agency.services.werving_selectie = True
                            services_found.append("werving & selectie")
                        
                        # payroll
                        elif service_name_clean == "payroll":
                            agency.services.payrolling = True
                            services_found.append("payroll")
                        
                        # zzp bemiddeling
                        elif "zzp" in service_name_clean:
                            agency.services.zzp_bemiddeling = True
                            services_found.append("zzp bemiddeling")
                        
                        # opleiden & ontwikkelen
                        elif "opleiden" in service_name_clean or "ontwikkelen" in service_name_clean:
                            agency.services.opleiden_ontwikkelen = True
                            services_found.append("opleiden & ontwikkelen")
                        
                        # reintegratie / outplacement
                        elif "re-integratie" in service_name_clean or service_name_clean == "outplacement":
                            agency.services.reintegratie_outplacement = True
                            services_found.append("reintegratie/outplacement")
                        
                        # MSP
                        elif service_name_clean == "msp":
                            agency.services.msp = True
                            services_found.append("MSP")
                        
                        # Executive Search
                        elif "executive search" in service_name_clean:
                            agency.services.executive_search = True
                            services_found.append("executive search")
                        
                        # Inhouse services (personeelsplanning, contractmanagement, leveranciersmanagement)
                        elif any(keyword in service_name_clean for keyword in [
                            "personeelsplanning", "contractmanagement", "leveranciersmanagement",
                            "inhouse", "in-house"
                        ]):
                            agency.services.inhouse_services = True
                            services_found.append(service_name_clean)
                    
                    # If we found services, break out of the loop
                    if services_found:
                        break
        
        # Fallback: if no services found in content blocks, try direct search
        if not services_found:
            cards_container = soup.find("div", class_="employers__block__cards")
            if cards_container:
                service_cards = cards_container.find_all("a", class_="employers__card")
                if service_cards:
                    # Process cards using the same logic
                    for card in service_cards:
                        # Get service name from company__name div or data-analytics-label
                        company_name_div = card.find("div", class_="company__name")
                        service_name = None
                        
                        if company_name_div:
                            service_name = company_name_div.get_text(strip=True).lower()
                        else:
                            # Fallback to data-analytics-label
                            service_name = card.get("data-analytics-label", "").lower()
                        
                        if not service_name:
                            continue
                        
                        # Skip non-service cards (like "onze diensten", "contact")
                        if service_name in ["onze diensten", "contact", "onze hr-diensten"]:
                            continue
                        
                        # Map service names to service fields
                        service_name_clean = service_name.strip()
                        
                        # uitzenden
                        if service_name_clean == "uitzenden":
                            agency.services.uitzenden = True
                            services_found.append("uitzenden")
                        
                        # detacheren
                        elif service_name_clean == "detacheren":
                            agency.services.detacheren = True
                            services_found.append("detacheren")
                        
                        # werving & selectie
                        elif "werving" in service_name_clean and "selectie" in service_name_clean:
                            agency.services.werving_selectie = True
                            services_found.append("werving & selectie")
                        
                        # payroll
                        elif service_name_clean == "payroll":
                            agency.services.payrolling = True
                            services_found.append("payroll")
                        
                        # zzp bemiddeling
                        elif "zzp" in service_name_clean:
                            agency.services.zzp_bemiddeling = True
                            services_found.append("zzp bemiddeling")
                        
                        # opleiden & ontwikkelen
                        elif "opleiden" in service_name_clean or "ontwikkelen" in service_name_clean:
                            agency.services.opleiden_ontwikkelen = True
                            services_found.append("opleiden & ontwikkelen")
                        
                        # reintegratie / outplacement
                        elif "re-integratie" in service_name_clean or service_name_clean == "outplacement":
                            agency.services.reintegratie_outplacement = True
                            services_found.append("reintegratie/outplacement")
                        
                        # MSP
                        elif service_name_clean == "msp":
                            agency.services.msp = True
                            services_found.append("MSP")
                        
                        # Executive Search
                        elif "executive search" in service_name_clean:
                            agency.services.executive_search = True
                            services_found.append("executive search")
                        
                        # Inhouse services (personeelsplanning, contractmanagement, leveranciersmanagement)
                        elif any(keyword in service_name_clean for keyword in [
                            "personeelsplanning", "contractmanagement", "leveranciersmanagement",
                            "inhouse", "in-house"
                        ]):
                            agency.services.inhouse_services = True
                            services_found.append(service_name_clean)
                else:
                    self.logger.warning(f"Could not find any employers__card elements on {url}")
            else:
                self.logger.warning(f"Could not find employers__block__cards on {url}")
        
        if services_found:
            self.logger.info(f"✓ Found {len(services_found)} services: {', '.join(services_found)} | Source: {url}")
        else:
            self.logger.warning(f"No services extracted from {url}")
    
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
        
        # Do NOT extract annual placements if it requires calculation (million conversion)
        # Client feedback: avoid all calculations, set to null if not explicitly stated as exact number
        # placements_match = re.search(r'supported over ([\d.]+)\s+million\s+talent', page_text, re.IGNORECASE)
        # Skipped: requires calculation (* 1_000_000)
        self.logger.info(f"  Skipped annual placements (requires calculation: million conversion) | Source: {url}")
        
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
    
    def _extract_werkgevers_contact_phone(self, soup: BeautifulSoup, agency: Agency, url: str) -> None:
        """
        Extract general telephone number from werkgevers contact page.
        
        Expected format:
        <h4>General telephone number</h4>
        <p><a href="tel:080072637823">0800 72 63 78 23</a></p>
        """
        self.logger.info(f"🔍 Extracting general telephone number from {url}")
        
        # Find the article content
        article = soup.find("article")
        if not article:
            self.logger.warning(f"Could not find article on {url}")
            return
        
        # Look for "General telephone number" heading
        h4_headings = article.find_all("h4")
        for h4 in h4_headings:
            h4_text = h4.get_text(strip=True).lower()
            if "general telephone number" in h4_text or "algemeen telefoonnummer" in h4_text:
                # Find the next <p> tag with a tel link
                next_p = h4.find_next_sibling("p")
                if next_p:
                    tel_link = next_p.find("a", href=re.compile(r'^tel:'))
                    if tel_link:
                        phone = tel_link.get("href", "").replace("tel:", "").strip()
                        # Format: remove any colons or extra spaces, keep the format from the link text
                        phone_text = tel_link.get_text(strip=True)
                        if phone_text:
                            # Use the formatted text from the link (e.g., "0800 72 63 78 23")
                            from staffing_agency_scraper.lib.normalize import normalize_contact_phone
                            normalized_phone = normalize_contact_phone(phone_text)
                            agency.contact_phone = normalized_phone
                            if normalized_phone != phone_text:
                                self.logger.info(f"✓ Found general telephone number: {phone_text} -> normalized to: {normalized_phone} | Source: {url}")
                            else:
                                self.logger.info(f"✓ Found general telephone number: {normalized_phone} | Source: {url}")
                            return
                        elif phone:
                            # Fallback to the href value, normalize it
                            from staffing_agency_scraper.lib.normalize import normalize_contact_phone
                            normalized_phone = normalize_contact_phone(phone)
                            agency.contact_phone = normalized_phone
                            self.logger.info(f"✓ Found general telephone number: {phone} -> normalized to: {normalized_phone} | Source: {url}")
                            return
        
        self.logger.warning(f"Could not find general telephone number on {url}")
    
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
            from staffing_agency_scraper.lib.normalize import normalize_contact_phone
            phone = "+31 (0)20 569 5911"
            normalized_phone = normalize_contact_phone(phone)
            agency.contact_phone = normalized_phone
            self.logger.info(f"✓ Found HQ phone: {phone} -> normalized to: {normalized_phone} | Source: {url}")
        
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
                    agency.membership.append("ABU")
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
        Extract representative office locations from the vestigingen page (max 10).
        Finds office cards and extracts city names.
        Does NOT add individual office card URLs to evidence_urls.
        """
        self.logger.info(f"🔍 Extracting representative office locations from {url} (limited to 10)")
        
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
        
        # Limit to 10 representative offices
        for card in cards[:10]:
            # Stop if we've reached the limit
            if len(agency.office_locations) >= 10:
                self.logger.info(f"✓ Limited office locations to 10 (per client requirement)")
                break
            
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
                
                # Check if already exists (avoid duplicates)
                if any(off.city == city_name for off in agency.office_locations):
                    self.logger.info(f"  Skipped duplicate: {city_name}")
                    continue
                
                # Determine province using utils
                province = self.utils.map_city_to_province(city_name)
                
                # Create office location
                office = OfficeLocation(
                    city=city_name,
                    province=province,
                )
                
                agency.office_locations.append(office)
                self.logger.info(f"✓ Office: {city_name}, {province} | Source: {url}")
                
            except Exception as e:
                self.logger.error(f"Error processing office card: {e}")
                continue
        
        # Ensure we only have max 10 offices
        agency.office_locations = agency.office_locations[:10]
        
        self.logger.info(f"✓ Total representative offices extracted (limited to 10): {len(agency.office_locations)} | Source: {url}")
    
    def _extract_statistics(self, soup: BeautifulSoup, page_text: str, agency: Agency, url: str) -> None:
        """
        Extract statistics from the werkgevers page.
        Extracts candidate pool size, monthly visitors, etc.
        """
        import re
        
        self.logger.info(f"🔍 Extracting statistics from {url}")
        
        # Helper to parse numbers written with comma or dot decimals (e.g., 1,5 million)
        def _parse_million_number(raw: str) -> float:
            try:
                return float(raw.replace(",", "."))
            except Exception:
                return 0.0
        
        # Extract candidate pool size from "talentendatabase" (talent database)
        # The website says: "talentendatabase met meer dan 1,5 miljoen talenten"
        # Note: This is the database size (people they can source from), which we use as candidate_pool_size_estimate
        # We specifically look for mentions of "database" or "talentendatabase" to ensure we get the right number
        
        # Pattern 1: Look for "talentendatabase" or "database" with number (e.g., "talentendatabase met meer dan 1,5 miljoen talenten")
        database_pattern = r"(?:talentendatabase|database).{0,100}?(?:meer\s+dan\s+)?(\d+[.,]\d+|\d+)\s*(?:million|miljoen)\s*(?:talent|talenten|kandidaten)"
        candidate_pool_match = re.search(database_pattern, page_text, re.IGNORECASE)
        
        # Pattern 2: Fallback - look for number near "talenten" but only if it's clearly about database/pool
        # This is less specific, so we only use it if database pattern didn't match
        if not candidate_pool_match:
            # Look for decimal/comma numbers first (more specific, like "1,5")
            decimal_pattern = r"(?:meer\s+dan\s+)?(\d+[.,]\d+)\s*(?:million|miljoen)\s*(?:talent|talenten|kandidaten)"
            candidate_pool_match = re.search(decimal_pattern, page_text, re.IGNORECASE)
        
        if candidate_pool_match:
            matched_text = candidate_pool_match.group(0)
            raw_value = candidate_pool_match.group(1)
            self.logger.info(f"   Matched text: '{matched_text}' | Raw value: '{raw_value}'")
            
            # Do NOT extract candidate pool size if it requires calculation (million conversion)
            # Client feedback: avoid all calculations, set to null if not explicitly stated as exact number
            value = _parse_million_number(raw_value)
            if value > 0:
                # If it mentions "million" or "miljoen", skip (requires calculation)
                if "million" in matched_text.lower() or "miljoen" in matched_text.lower():
                    self.logger.info(f"  Skipped candidate pool (requires calculation: million conversion from '{matched_text}') | Source: {url}")
                else:
                    # Only extract if it's an exact number without unit conversion
                    agency.candidate_pool_size_estimate = int(value)
                    self.logger.info(f"✓ Found candidate pool (exact number, no calculation): {agency.candidate_pool_size_estimate:,} (from '{matched_text}') | Source: {url}")
            else:
                self.logger.warning(f"⚠ Invalid candidate pool value: '{raw_value}' | Source: {url}")
        
        # Extract monthly visitors (e.g., "1,4 miljoen online bezoekers per maand")
        visitors_match = re.search(
            r"(\d+[.,]?\d*)\s*(?:million|miljoen)\s*(?:online\s*)?(?:visitors?|bezoekers?)\s*(?:per\s*month|per\s*maand)",
            page_text,
            re.IGNORECASE,
        )
        # Do NOT extract monthly visitors if it requires calculation (million conversion)
        # Client feedback: avoid all calculations, set to null if not explicitly stated as exact number
        if visitors_match:
            matched_text = visitors_match.group(0).lower()
            # If it mentions "million" or "miljoen", skip (requires calculation)
            if "million" in matched_text or "miljoen" in matched_text:
                self.logger.info(f"  Skipped monthly visitors (requires calculation: million conversion from '{matched_text}') | Source: {url}")
            else:
                # Only extract if it's an exact number without unit conversion
                value = _parse_million_number(visitors_match.group(1))
                if value > 0:
                    monthly_visitors = int(value)
                    if not agency.growth_signals:
                        agency.growth_signals = []
                    note = f"{monthly_visitors:,} online visitors per month"
                    if note not in agency.growth_signals:
                        agency.growth_signals.append(note)
                    self.logger.info(f"✓ Found monthly visitors (exact number, no calculation): {monthly_visitors:,} | Source: {url}")
        
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
    
    # Removed _check_ai_capabilities method - AI capabilities should only be set
    # to True if explicitly stated on the website, not inferred from API calls


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
