"""
YoungCapital Netherlands scraper.

Website: https://www.youngcapital.nl
"""

from __future__ import annotations

import io
from typing import Any, Dict, List, Set

import dagster as dg
import pdfplumber
import requests
from bs4 import BeautifulSoup

from staffing_agency_scraper.models import Agency, AgencyServices, GeoFocusType, OfficeLocation
from staffing_agency_scraper.scraping.base import BaseAgencyScraper
from staffing_agency_scraper.scraping.utils import AgencyScraperUtils

class YoungCapitalScraper(BaseAgencyScraper):
    """Scraper for YoungCapital Netherlands."""

    AGENCY_NAME = "YoungCapital"
    WEBSITE_URL = "https://www.youngcapital.nl"
    BRAND_GROUP = None
    PAGES_TO_SCRAPE: List[Dict[str, Any]] = [
        {
            "name": "home",
            "url": "https://www.youngcapital.nl",
            "functions": ['json_ld', 'services'],
        },
        {
            "name": "specialisaties",
            "url": "https://www.youngcapital.nl/werkgevers/specialisaties",
            "functions": ['sectors'],
        },
        {
            "name": "contact",
            "url": "https://www.youngcapital.nl/werkgevers/contact",
            "functions": ['contact'],
        },
        {
            "name": "privacy",
            "url": "https://www.youngcapital.nl/over-yc/privacyverklaring",
            "functions": ['legal'],
        },
        {
            "name": "certificering",
            "url": "https://www.youngcapital.nl/over-yc/certificering",
            "functions": ['certifications'],
        },
        {
            "name": "vestigingen",
            "url": "https://www.youngcapital.nl/uitzendbureau",
            "functions": ['office_locations'],
        },
        {
            "name": "diensten",
            "url": "https://www.youngcapital.nl/werkgevers/diensten",
            "functions": ['services_detailed'],
        },
        {
            "name": "over_yc",
            "url": "https://www.youngcapital.nl/over-yc",
            "functions": ['statistics', 'growth_signals'],
        },
        {
            "name": "algemene_voorwaarden",
            "url": "https://www.youngcapital.nl/over-yc/algemenevoorwaarden",
            "functions": ['terms_conditions'],
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
            if func_name == "json_ld":
                self._extract_json_ld(soup, agency, url)
            
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
                legal_name = self.utils.fetch_legal_name(page_text, "YoungCapital", url)
                if kvk:
                    agency.kvk_number = kvk
                if legal_name:
                    agency.legal_name = legal_name
            
            elif func_name == "sectors":
                self._extract_sectors(soup, all_sectors, url)
            
            elif func_name == "certifications":
                self._extract_certifications(soup, agency, url)
            
            elif func_name == "office_locations":
                self._extract_office_locations(soup, agency, url)
            
            elif func_name == "services_detailed":
                self._extract_services_detailed(soup, agency, url)
            
            elif func_name == "statistics":
                self._extract_statistics(soup, agency, url)
            
            elif func_name == "growth_signals":
                self._extract_growth_signals_from_about(soup, page_text, agency, url)
            
            elif func_name == "terms_conditions":
                self._extract_terms_conditions(soup, agency, url)
    
    def _extract_json_ld(self, soup: BeautifulSoup, agency: Agency, url: str) -> None:
        """Extract comprehensive data from JSON-LD schema."""
        import json
        
        self.logger.info(f"🔍 Extracting data from JSON-LD schema on {url}")
        
        # Find JSON-LD script tags
        json_ld_scripts = soup.find_all("script", type="application/ld+json")
        
        for script in json_ld_scripts:
            try:
                data = json.loads(script.string)
                
                # Check if this is an Organization type
                if "@type" in data:
                    types = data["@type"] if isinstance(data["@type"], list) else [data["@type"]]
                    
                    if "Organization" in types or "WebSite" in types:
                        self.logger.info(f"✓ Found Organization/WebSite JSON-LD schema | Source: {url}")
                        
                        # Extract logo
                        if "logo" in data and not agency.logo_url:
                            logo_data = data["logo"]
                            if isinstance(logo_data, dict):
                                logo_url = logo_data.get("url")
                                if logo_url:
                                    agency.logo_url = logo_url
                                    self.logger.info(f"✓ Logo: {logo_url} | Source: {url}")
                            elif isinstance(logo_data, str):
                                agency.logo_url = logo_data
                                self.logger.info(f"✓ Logo: {logo_data} | Source: {url}")
                        
                        # Extract contact info
                        if "contactPoint" in data:
                            contact_points = data["contactPoint"]
                            if isinstance(contact_points, list):
                                contact = contact_points[0] if contact_points else {}
                            else:
                                contact = contact_points
                            
                            if "telephone" in contact and not agency.contact_phone:
                                phone = contact["telephone"]
                                # Clean phone number (remove +31 prefix format)
                                agency.contact_phone = phone.replace("+31", "0").replace(" ", "")
                                self.logger.info(f"✓ Phone: {agency.contact_phone} | Source: {url}")
                            
                            if "email" in contact and not agency.contact_email:
                                agency.contact_email = contact["email"]
                                self.logger.info(f"✓ Email: {agency.contact_email} | Source: {url}")
                        
                        # Extract VAT ID (BTW-nummer) - extract KvK from it
                        if "vatID" in data and not agency.kvk_number:
                            vat_id = data["vatID"]
                            # VAT ID format: NL812131629B01
                            # KvK number is typically the numeric part after NL
                            import re
                            kvk_match = re.search(r'NL(\d{8,9})', vat_id)
                            if kvk_match:
                                agency.kvk_number = kvk_match.group(1)
                                self.logger.info(f"✓ KvK (from VAT ID): {agency.kvk_number} | Source: {url}")
                        
                        # Extract founding date
                        if "foundingDate" in data:
                            founding_date = data["foundingDate"]
                            # Add to growth signals
                            year = founding_date.split("-")[0]
                            signal = f"Founded in {year}"
                            if signal not in agency.growth_signals:
                                agency.growth_signals.append(signal)
                                self.logger.info(f"✓ {signal} | Source: {url}")
                        
                        # Extract HQ address
                        if "address" in data and not agency.hq_city:
                            address = data["address"]
                            if isinstance(address, dict):
                                city = address.get("addressLocality")
                                if city:
                                    agency.hq_city = city
                                    # Map city to province
                                    agency.hq_province = self.utils.map_city_to_province(city)
                                    street = address.get("streetAddress", "")
                                    postal = address.get("postalCode", "")
                                    self.logger.info(f"✓ HQ: {city}, {agency.hq_province} ({street}, {postal}) | Source: {url}") 
                        
                        return
                        
            except (json.JSONDecodeError, KeyError, AttributeError) as e:
                self.logger.warning(f"⚠ Failed to parse JSON-LD: {e}")
                continue
    
    def _extract_certifications(self, soup: BeautifulSoup, agency: Agency, url: str) -> None:
        """Extract certifications from the certification page."""
        import re
        
        self.logger.info(f"🔍 Extracting certifications from {url}")
        
        if not agency.certifications:
            agency.certifications = []
        if not agency.membership:
            agency.membership = []
        
        page_text = soup.get_text(separator=" ", strip=True)
        
        # ABU Certificate
        if "ABU" in page_text or "Algemene Bond Uitzendondernemingen" in page_text:
            if "ABU" not in agency.membership:
                agency.membership.append("ABU")
                self.logger.info(f"✓ Membership: ABU | Source: {url}")
            
            # Check if they have ABU certificate
            abu_cert_link = soup.find("a", href=re.compile(r"ABU.*certificaat", re.IGNORECASE))
            if abu_cert_link:
                if "ABU Certificaat" not in agency.certifications:
                    agency.certifications.append("ABU Certificaat")
                    self.logger.info(f"✓ Certification: ABU Certificaat | Source: {url}")
        
        # ISO Certificates - look for ISO 9001, 14001, 27001
        iso_list = soup.find("ul", id="certIso")
        if iso_list:
            for li in iso_list.find_all("li"):
                li_text = li.get_text(strip=True)
                
                # ISO 9001
                if "ISO 9001" in li_text or "ISO9001" in li_text:
                    if "ISO 9001" not in agency.certifications:
                        agency.certifications.append("ISO 9001")
                        self.logger.info(f"✓ Certification: ISO 9001 (quality management) | Source: {url}")
                
                # ISO 14001
                elif "ISO 14001" in li_text or "ISO14001" in li_text:
                    if "ISO 14001" not in agency.certifications:
                        agency.certifications.append("ISO 14001")
                        self.logger.info(f"✓ Certification: ISO 14001 (environmental management) | Source: {url}")
                
                # ISO 27001
                elif "ISO 27001" in li_text or "ISO27001" in li_text:
                    if "ISO 27001" not in agency.certifications:
                        agency.certifications.append("ISO 27001")
                        self.logger.info(f"✓ Certification: ISO 27001 (information security) | Source: {url}")
                
                # CO2 Reduction Management
                elif "CO2" in li_text or "CO₂" in li_text:
                    if "CO₂ Reductiemanagement" not in agency.certifications:
                        agency.certifications.append("CO₂ Reductiemanagement")
                        self.logger.info(f"✓ Certification: CO₂ Reductiemanagement | Source: {url}")
        
        # NEN 4400 / SNA
        if "NEN 4400" in page_text or "NEN4400" in page_text:
            if "NEN 4400" not in agency.certifications:
                agency.certifications.append("NEN 4400")
                self.logger.info(f"✓ Certification: NEN 4400 (SNA keurmerk) | Source: {url}")
            
            # Add SNA membership
            if "SNA" not in agency.membership and "Stichting Normering Arbeid" in page_text:
                agency.membership.append("SNA")
                self.logger.info(f"✓ Membership: SNA (Stichting Normering Arbeid) | Source: {url}")
        
        # EcoVadis
        if "EcoVadis" in page_text or "Ecovadis" in page_text:
            if "EcoVadis" not in agency.certifications:
                agency.certifications.append("EcoVadis")
                self.logger.info(f"✓ Certification: EcoVadis (sustainability) | Source: {url}")
        
        # PSO (Prestatieladder Sociaal Ondernemen)
        if "PSO" in page_text or "Prestatieladder Sociaal Ondernemen" in page_text:
            # Look for trede/step level in heading (h2, h3)
            pso_heading = soup.find(["h2", "h3"], string=re.compile(r"(?:PSO|Prestatieladder).*(?:trede|step)\s*\d", re.IGNORECASE))
            if pso_heading:
                heading_text = pso_heading.get_text(strip=True)
                pso_match = re.search(r"(?:trede|step)\s*(\d)", heading_text, re.IGNORECASE)
                if pso_match:
                    level = pso_match.group(1)
                    cert_name = f"PSO Trede {level}"
                    if cert_name not in agency.certifications:
                        agency.certifications.append(cert_name)
                        self.logger.info(f"✓ Certification: {cert_name} (Social Entrepreneurship) | Source: {url}")
            else:
                # Fallback: look in page text for PSO trede mention
                pso_match = re.search(r"(?:PSO|Prestatieladder)[^\d]*(?:trede|step)\s*(\d)", page_text, re.IGNORECASE)
                if pso_match:
                    level = pso_match.group(1)
                    cert_name = f"PSO Trede {level}"
                    if cert_name not in agency.certifications:
                        agency.certifications.append(cert_name)
                        self.logger.info(f"✓ Certification: {cert_name} (Social Entrepreneurship) | Source: {url}")
                else:
                    # Generic PSO if no level found
                    pso_exists = any("PSO" in cert for cert in agency.certifications)
                    if not pso_exists:
                        agency.certifications.append("PSO")
                        self.logger.info(f"✓ Certification: PSO (Social Entrepreneurship) | Source: {url}")
        
        self.logger.info(f"✓ Total certifications: {len(agency.certifications)} | Total memberships: {len(agency.membership)}")
    
    def _extract_sectors(self, soup: BeautifulSoup, all_sectors: Set[str], url: str) -> None:
        """Extract sectors from specialisaties page."""
        self.logger.info(f"🔍 Extracting sectors from {url}")
        
        # Find the tags list containing specializations
        tags_ul = soup.find("ul", class_="tags")
        
        if not tags_ul:
            self.logger.warning(f"Could not find <ul class='tags'> on {url}")
            return
        
        # Extract all links within the tags list
        sector_links = tags_ul.find_all("a")
        
        if not sector_links:
            self.logger.warning(f"No sector links found in tags list on {url}")
            return
        
        self.logger.info(f"✓ Found {len(sector_links)} specializations | Source: {url}")
        
        # Sector mapping to normalize names
        sector_mapping = {
            "mkb": None,  # Skip - this is company size, not a sector
            "logistiek": "Logistiek",
            "productie": "Productie",
            "callcenter": "Callcenter",
            "administratief": "Administratief",
            "horeca": "Horeca",
            "winkel": "Retail",  # Normalize "Winkel" to "Retail"
            "banking-insurance": "Finance",  # Normalize to standard sector
            "hospitality": "Horeca",  # Merge with Horeca (similar sector)
            "luchtvaart": "Transport",  # Aviation -> Transport sector
            "publieke-sector": None,  # Skip - this is company type, not sector
            "multilingual": None,  # Skip - this is a service type, not sector
            "zorg": "Zorg",
        }
        
        for link in sector_links:
            sector_text = link.get_text(strip=True)
            href = link.get("href", "")
            
            # Extract sector key from href (e.g., "/werkgevers/specialisaties/logistiek" -> "logistiek")
            sector_key = href.split("/")[-1] if href else None
            
            if sector_key and sector_key in sector_mapping:
                normalized_sector = sector_mapping[sector_key]
                
                if normalized_sector:
                    # Only add if not already present (avoid duplicates from merging)
                    if normalized_sector not in all_sectors:
                        all_sectors.add(normalized_sector)
                        self.logger.info(f"✓ Sector: '{normalized_sector}' (from '{sector_text}') | Source: {url}")
                    else:
                        self.logger.info(f"  Skipped duplicate: '{sector_text}' (already have '{normalized_sector}') | Source: {url}")
                else:
                    self.logger.info(f"  Skipped: '{sector_text}' (not a sector - company size/service type) | Source: {url}")
            else:
                self.logger.warning(f"  Unknown sector key: '{sector_key}' from link '{sector_text}' | Source: {url}")
        
        self.logger.info(f"✓ Total unique sectors extracted: {len(all_sectors)} | Source: {url}")
    
    def _extract_office_locations(self, soup: BeautifulSoup, agency: Agency, url: str) -> None:
        """
        Extract office locations from uitzendbureauJSON JavaScript variable on the main page,
        then fetch individual office pages to add URLs to evidence_urls.
        """
        import re
        import json

        
        self.logger.info(f"🔍 Extracting office locations from uitzendbureauJSON on {url}")
        
        if not agency.office_locations:
            agency.office_locations = []
        
        # Find all script tags (don't filter by type, as it might not be set)
        scripts = soup.find_all("script")
        self.logger.info(f"✓ Found {len(scripts)} scripts | Source: {url}")
        
        offices_data = None
        
        for script in scripts:
            # Try both string and get_text() methods
            script_text = script.string if script.string else (script.get_text() if script else "")
            
            if not script_text or "var uitzendbureauJSON" not in script_text:
                continue
            
            # Try to capture the object assigned to uitzendbureauJSON
            # Pattern: var uitzendbureauJSON = { ... };
            # Use a more robust pattern that matches balanced braces
            match = re.search(r"var\s+uitzendbureauJSON\s*=\s*(\{)", script_text, re.DOTALL)
            if not match:
                continue
            
            self.logger.info(f"✓ Found uitzendbureauJSON script | Source: {url}")
            
            # Find the start position of the JSON object
            start_pos = match.end(1) - 1  # Position of the opening brace
            
            # Find the matching closing brace by counting braces
            brace_count = 0
            json_end = start_pos
            for i in range(start_pos, len(script_text)):
                if script_text[i] == '{':
                    brace_count += 1
                elif script_text[i] == '}':
                    brace_count -= 1
                    if brace_count == 0:
                        json_end = i + 1
                        break
            
            if brace_count != 0:
                self.logger.warning(f"⚠ Could not find matching closing brace for uitzendbureauJSON")
                continue
            
            json_str = script_text[start_pos:json_end].strip()
            
            # Remove trailing semicolon if present
            json_str = json_str.rstrip(";").strip()
            
            try:
                parsed = json.loads(json_str)
            except (json.JSONDecodeError, ValueError) as e:
                self.logger.warning(f"⚠ Failed to parse uitzendbureauJSON: {e}")
                continue
            
            if "offices" in parsed and parsed["offices"]:
                offices_data = parsed
                self.logger.info(f"✓ Successfully parsed JSON data with {len(parsed['offices'])} offices | Source: {url}")
                break
        
        if not offices_data or "offices" not in offices_data:
            self.logger.warning(f"Could not find office data in uitzendbureauJSON on {url}")
            return
        
        offices_list = offices_data["offices"]
        self.logger.info(f"✓ Found {len(offices_list)} offices in JSON data | Source: {url}")
        
        # Extract office locations from JSON data
        office_urls = set()
        for office_data in offices_list:
            try:
                # Get city name from address.city (preferred) or name field
                address = office_data.get("address", {})
                city_name = address.get("city") if isinstance(address, dict) else None
                
                # Fallback to name field if address.city is not available
                if not city_name:
                    city_name = office_data.get("name", "")
                
                if not city_name:
                    self.logger.warning(f"  Skipped office with no city name: {office_data.get('name', 'Unknown')}")
                    continue
                
                # Get office URL for evidence tracking
                office_url_path = office_data.get("url", "")
                if office_url_path:
                    office_url = f"{self.WEBSITE_URL}{office_url_path}"
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
                self.logger.error(f"Error processing office data {office_data.get('name', 'Unknown')}: {e}")
                continue
        
        # Fetch individual office pages to add URLs to evidence_urls
        if office_urls:
            self.logger.info(f"📄 Fetching {len(office_urls)} individual office pages for evidence tracking...")
            for office_url in office_urls:
                try:
                    self.logger.info(f"  Fetching: {office_url}")
                    office_soup = self.fetch_page(office_url)
                    # Add URL to evidence_urls
                    self.evidence_urls.append(office_url)
                except Exception as e:
                    self.logger.warning(f"  ⚠ Failed to fetch {office_url}: {e}")

        
        self.logger.info(f"✓ Total offices extracted: {len(agency.office_locations)} | Source: {url}")
    
    def _extract_services_detailed(self, soup: BeautifulSoup, agency: Agency, url: str) -> None:
        """
        Extract services from the diensten page card elements.
        Maps Dutch service names to AgencyServices fields.
        """
        self.logger.info(f"🔍 Extracting detailed services from {url}")
        
        # Initialize services if not already set
        if not agency.services:
            agency.services = AgencyServices()
        
        # Find all service cards - they have class "card card--horizontal"
        service_cards = soup.find_all("div", class_=lambda x: x and "card" in x and "card--horizontal" in x)
        
        if not service_cards:
            self.logger.warning(f"Could not find service cards on {url}")
            return
        
        self.logger.info(f"✓ Found {len(service_cards)} service cards | Source: {url}")
        
        # Service name mapping (Dutch -> AgencyServices field)
        service_mapping = {
            "werving": "werving_selectie",
            "selectie": "werving_selectie",
            "werving & selectie": "werving_selectie",
            "werving en selectie": "werving_selectie",
            "uitzendkrachten": "uitzenden",
            "uitzenden": "uitzenden",
            "detachering": "detacheren",
            "detacheren": "detacheren",
            "vakprofessionals": "detacheren",
            "freelance": "zzp_bemiddeling",
            "freelancers": "zzp_bemiddeling",
            "freelancer": "zzp_bemiddeling",
            "zzp": "zzp_bemiddeling",
            "vacature plaatsen": "vacaturebemiddeling_only",
            "vacaturebemiddeling": "vacaturebemiddeling_only",
            "trainingen": "opleiden_ontwikkelen",
            "training": "opleiden_ontwikkelen",
            "opleiden": "opleiden_ontwikkelen",
            "ontwikkelen": "opleiden_ontwikkelen",
        }
        
        for card in service_cards:
            try:
                # Extract service name from h2 tag
                h2 = card.find("h2")
                if not h2:
                    continue
                
                service_name = h2.get_text(strip=True).lower()
                
                # Extract description text for additional context
                description_p = card.find("p")
                description_text = description_p.get_text(strip=True).lower() if description_p else ""
                
                # Map service to AgencyServices field
                # Prioritize exact matches in service name, then check description
                matched_service = None
                
                # First, try to match against service name (more reliable)
                for key, field_name in service_mapping.items():
                    if key in service_name:
                        matched_service = field_name
                        self.logger.info(f"  Matched '{key}' in service name '{service_name}' → {field_name}")
                        break
                
                # If no match in service name, check description
                if not matched_service:
                    for key, field_name in service_mapping.items():
                        if key in description_text:
                            matched_service = field_name
                            self.logger.info(f"  Matched '{key}' in description → {field_name}")
                            break
                
                if matched_service:
                    # Set the service field to True
                    if hasattr(agency.services, matched_service):
                        setattr(agency.services, matched_service, True)
                        self.logger.info(f"✓ Service: {matched_service} (from '{service_name}') | Source: {url}")
                    else:
                        self.logger.warning(f"  Unknown service field: {matched_service} | Source: {url}")
                else:
                    self.logger.info(f"  No mapping found for service: '{service_name}' | Source: {url}")
                
            except Exception as e:
                self.logger.error(f"Error processing service card: {e}")
                continue
        
        self.logger.info(f"✓ Services extraction completed | Source: {url}")
    
    def _extract_statistics(self, soup: BeautifulSoup, agency: Agency, url: str) -> None:
        """
        Extract statistics from the about-us page counter section.
        Extracts candidate pool size, annual placements, etc.
        """
        import re
        
        self.logger.info(f"🔍 Extracting statistics from {url}")
        
        # Find the statistics section with class "about-us-counter"
        stats_section = soup.find("section", class_="about-us-counter")
        if not stats_section:
            self.logger.warning(f"Could not find statistics section on {url}")
            return
        
        # Find all stat elements with class "stat-count"
        stat_elements = stats_section.find_all("span", class_="stat-count")
        
        for stat_elem in stat_elements:
            try:
                # Get the data-count attribute or text content
                data_count = stat_elem.get("data-count", "")
                stat_text = stat_elem.get_text(strip=True)
                
                # Get the label from the next sibling <p> tag
                label_p = stat_elem.find_next("p", class_="text--bold")
                label = label_p.get_text(strip=True).lower() if label_p else ""
                
                # Get the unit (M, K) from next sibling span if exists
                # The unit is in a sibling span with class "about-us__stats" but not "stat-count"
                unit_span = stat_elem.find_next_sibling("span", class_="about-us__stats")
                if not unit_span:
                    # Try finding in parent's next siblings
                    parent = stat_elem.parent
                    if parent:
                        next_spans = parent.find_all("span", class_="about-us__stats")
                        for span in next_spans:
                            if "stat-count" not in span.get("class", []):
                                unit_span = span
                                break
                unit = unit_span.get_text(strip=True) if unit_span else ""
                
                # Determine the value
                if data_count:
                    value = float(data_count)
                else:
                    # Try to parse from text
                    value_match = re.search(r'(\d+(?:[.,]\d+)?)', stat_text)
                    if value_match:
                        value = float(value_match.group(1).replace(',', '.'))
                    else:
                        continue
                
                # Apply unit multiplier
                if unit == "M":
                    value = int(value * 1_000_000)
                elif unit == "K":
                    value = int(value * 1_000)
                else:
                    value = int(value)
                
                # Map to agency fields based on label
                if "kandidaten in database" in label or "candidates in database" in label:
                    if not agency.candidate_pool_size_estimate or agency.candidate_pool_size_estimate < value:
                        agency.candidate_pool_size_estimate = value
                        self.logger.info(f"✓ Candidate pool size: {value:,} | Source: {url}")
                
                elif "kandidaten per dag" in label or "candidates per day" in label:
                    # Calculate annual placements: daily * 365
                    annual_placements = value * 365
                    if not agency.annual_placements_estimate or agency.annual_placements_estimate < annual_placements:
                        agency.annual_placements_estimate = annual_placements
                        self.logger.info(f"✓ Annual placements estimate: {annual_placements:,} (from {value:,} per day) | Source: {url}")
                
                elif "kandidaten per week" in label or "candidates per week" in label:
                    # Calculate annual placements: weekly * 52
                    annual_placements = value * 52
                    if not agency.annual_placements_estimate or agency.annual_placements_estimate < annual_placements:
                        agency.annual_placements_estimate = annual_placements
                        self.logger.info(f"✓ Annual placements estimate: {annual_placements:,} (from {value:,} per week) | Source: {url}")
                
                elif "vestigingen" in label or "offices" in label or "locations" in label:
                    # This is just a count, not a field we track separately
                    # (we already extract actual office locations)
                    self.logger.info(f"  Found office count: {value} | Source: {url}")
                
            except (ValueError, AttributeError) as e:
                self.logger.warning(f"  Error parsing statistic: {e}")
                continue
        
        # Also check for mentions in text (e.g., "meer dan 20.000 kandidaten per week")
        page_text = soup.get_text(separator=" ", strip=True)
        
        # Pattern: "meer dan 20.000 kandidaten per week"
        weekly_match = re.search(r'(\d+(?:[.,]\d+)?)\s*(?:k|duizend|thousand)?\s*kandidaten\s*per\s*week', page_text.lower())
        if weekly_match:
            weekly_value = float(weekly_match.group(1).replace(',', '.'))
            if 'k' in weekly_match.group(0) or 'duizend' in weekly_match.group(0):
                weekly_value = weekly_value * 1_000
            annual_placements = int(weekly_value * 52)
            if not agency.annual_placements_estimate or agency.annual_placements_estimate < annual_placements:
                agency.annual_placements_estimate = annual_placements
                self.logger.info(f"✓ Annual placements estimate: {annual_placements:,} (from text: {weekly_value:,.0f} per week) | Source: {url}")
        
        self.logger.info(f"✓ Statistics extraction completed | Source: {url}")
    
    def _extract_growth_signals_from_about(self, soup: BeautifulSoup, page_text: str, agency: Agency, url: str) -> None:
        """
        Extract growth signals from the about-us page.
        """
        self.logger.info(f"🔍 Extracting growth signals from {url}")
        
        text_lower = page_text.lower()
        
        # Check for specific growth signals mentioned on this page
        growth_signals_to_check = [
            ("grootste jongerendatabase van europa", "Grootste jongerendatabase van Europa"),
            ("grootste database", "Grootste jongerendatabase"),
            ("meer dan 20.000 kandidaten per week", "20.000+ kandidaten per week"),
            ("online beter vindbaar", "Online beter vindbaar dan andere uitzendbureaus"),
            ("meer dan twintig jaar", "Actief sinds 2000 (meer dan 20 jaar)"),
        ]
        
        for keyword, signal_text in growth_signals_to_check:
            if keyword in text_lower:
                if signal_text not in agency.growth_signals:
                    agency.growth_signals.append(signal_text)
                    self.logger.info(f"✓ Growth signal: {signal_text} | Source: {url}")
        
        # Check for founding year mention (2000)
        if "2000" in page_text and any(keyword in text_lower for keyword in ["opgericht", "begonnen", "founded", "gestart"]):
            signal = "Opgericht in 2000"
            if signal not in agency.growth_signals:
                agency.growth_signals.append(signal)
                self.logger.info(f"✓ Growth signal: {signal} | Source: {url}")
        
        self.logger.info(f"✓ Growth signals extraction completed | Source: {url}")
    
    def _extract_terms_conditions(self, soup: BeautifulSoup, agency: Agency, url: str) -> None:
        """
        Extract data from terms & conditions PDF.
        Downloads the Dutch PDF, extracts text, and parses phase system and other data.
        """
        self.logger.info(f"🔍 Extracting terms & conditions from {url}")
        
        # Find the Dutch PDF link
        # Look for link with text "Algemene Voorwaarden (NL)" or similar
        pdf_links = soup.find_all("a", href=True)
        dutch_pdf_url = None
        
        for link in pdf_links:
            href = link.get("href", "")
            link_text = link.get_text(strip=True).lower()
            
            # Look for Dutch terms & conditions link
            # Check for "algemene voorwaarden" and "nl" or just "algemene voorwaarden" with PDF extension
            if href.endswith(".pdf"):
                if ("algemene voorwaarden" in link_text and "nl" in link_text) or \
                   ("algemene voorwaarden" in link_text and "general" not in link_text):
                    dutch_pdf_url = href
                    # Convert relative URL to absolute if needed
                    if dutch_pdf_url.startswith("/"):
                        dutch_pdf_url = f"{self.WEBSITE_URL}{dutch_pdf_url}"
                    elif not dutch_pdf_url.startswith("http"):
                        dutch_pdf_url = f"{self.WEBSITE_URL}/{dutch_pdf_url}"
                    break
        
        # Fallback: if no specific Dutch link found, try any PDF with "algemene voorwaarden" in URL
        if not dutch_pdf_url:
            for link in pdf_links:
                href = link.get("href", "")
                if href.endswith(".pdf") and "algemene-voorwaarden" in href.lower():
                    dutch_pdf_url = href
                    if dutch_pdf_url.startswith("/"):
                        dutch_pdf_url = f"{self.WEBSITE_URL}{dutch_pdf_url}"
                    elif not dutch_pdf_url.startswith("http"):
                        dutch_pdf_url = f"{self.WEBSITE_URL}/{dutch_pdf_url}"
                    break
        
        if not dutch_pdf_url:
            self.logger.warning(f"Could not find Dutch PDF URL on {url}")
            return
        
        self.logger.info(f"✓ Found Dutch PDF: {dutch_pdf_url}")
        
        try:
            # Download PDF
            response = requests.get(dutch_pdf_url, timeout=30)
            response.raise_for_status()
            
            # Extract text from PDF using pdfplumber
            pdf_text = ""
            with pdfplumber.open(io.BytesIO(response.content)) as pdf:
                for page in pdf.pages:
                    page_text = page.extract_text()
                    if page_text:
                        pdf_text += page_text + "\n"
            
            if not pdf_text:
                self.logger.warning(f"Could not extract text from PDF: {dutch_pdf_url}")
                return
            
            self.logger.info(f"✓ Extracted {len(pdf_text)} characters from PDF | Source: {dutch_pdf_url}")
            
            # Add PDF URL to evidence_urls
            self.evidence_urls.append(dutch_pdf_url)
            
            # Extract phase system
            self._extract_phase_system_from_pdf(pdf_text, agency, dutch_pdf_url)
            
            # Extract other relevant data (takeover policy, pricing, etc.)
            self._extract_other_terms_data(pdf_text, agency, dutch_pdf_url)
            
        except Exception as e:
            self.logger.error(f"Error downloading/parsing PDF {dutch_pdf_url}: {e}")
    
    def _extract_phase_system_from_pdf(self, pdf_text: str, agency: Agency, url: str) -> None:
        """
        Extract phase system information from PDF text.
        """
        import re
        
        self.logger.info(f"🔍 Extracting phase system from PDF | Source: {url}")
        
        pdf_text_lower = pdf_text.lower()
        
        # Check if ABU CAO is mentioned
        is_abu = "abu cao" in pdf_text_lower or "abu-cao" in pdf_text_lower
        
        # Initialize phase system if not exists
        if not agency.phase_system:
            from staffing_agency_scraper.models.agency import PhaseSystem
            agency.phase_system = PhaseSystem()
        
        # Extract phase definitions
        # Pattern: "UITZENDFASE A", "UITZENDFASE B", "UITZENDFASE C"
        phases_found = []
        
        # Look for phase definitions - prioritize "UITZENDFASE" pattern
        phase_patterns = [
            r'uitzendfase\s+([A-D])',  # "UITZENDFASE A", "UITZENDFASE B"
            r'fase\s+([A-D])\s+(?:is|betekent|duurt|werkzaam)',  # "Fase A is werkzaam"
            r'fase\s+([A-D])',  # Generic "fase A"
            r'phase\s+([A-D])',  # "phase A"
        ]
        
        for pattern in phase_patterns:
            matches = re.findall(pattern, pdf_text, re.IGNORECASE)
            for match in matches:
                phase_letter = match.upper()
                if phase_letter in ['A', 'B', 'C', 'D'] and phase_letter not in phases_found:
                    phases_found.append(phase_letter)
        
        if phases_found:
            phases_list = sorted(phases_found)  # Sort: A, B, C, D
            
            # Determine if ABU or NBBU (default to ABU if mentioned)
            if is_abu:
                agency.phase_system.abu_phases = phases_list
                self.logger.info(f"✓ Found ABU phases in PDF: {phases_list} | Source: {url}")
            else:
                # Check for NBBU mention
                if "nbbu" in pdf_text_lower:
                    agency.phase_system.nbbu_phases = phases_list
                    self.logger.info(f"✓ Found NBBU phases in PDF: {phases_list} | Source: {url}")
                else:
                    # Default to ABU if no clear indication
                    agency.phase_system.abu_phases = phases_list
                    self.logger.info(f"✓ Found phases in PDF: {phases_list} (defaulting to ABU) | Source: {url}")
        
        # Also check for phase descriptions to extract more details
        # Look for "Fase A", "Fase B", "Fase C" with descriptions
        phase_descriptions = {}
        for phase in ['A', 'B', 'C', 'D']:
            # Pattern to find phase description
            pattern = rf'fase\s+{phase}[^\w]*(?:is|betekent|duurt|werkzaam)[^.]*\.'
            match = re.search(pattern, pdf_text, re.IGNORECASE | re.DOTALL)
            if match:
                phase_descriptions[phase] = match.group(0)
        
        if phase_descriptions:
            self.logger.info(f"✓ Found phase descriptions for: {list(phase_descriptions.keys())} | Source: {url}")
    
    def _extract_other_terms_data(self, pdf_text: str, agency: Agency, url: str) -> None:
        """
        Extract other relevant data from terms & conditions PDF.
        Includes: takeover policy, pricing info, minimum assignment duration, etc.
        """
        import re
        
        self.logger.info(f"🔍 Extracting other terms data from PDF | Source: {url}")
        
        pdf_text_lower = pdf_text.lower()
        
        # Extract takeover policy (overname)
        # Look for takeover fee information
        takeover_section = re.search(
            r'(?:overname|takeover|arbeidsverhouding.*flexkracht)[^.]{0,500}',
            pdf_text_lower,
            re.IGNORECASE | re.DOTALL
        )
        
        if takeover_section:
            takeover_text = takeover_section.group(0)
            
            # Check for free takeover period
            # Pattern: "na X uren gratis overnemen", "na X weken overname zonder kosten"
            hours_match = re.search(r'na\s+(\d+)\s+uren?\s+(?:gratis|kosteloos|zonder\s+kosten)?\s*(?:overnemen|overname)', takeover_text)
            if hours_match:
                if not agency.takeover_policy:
                    from staffing_agency_scraper.models.agency import TakeoverPolicy
                    agency.takeover_policy = TakeoverPolicy()
                agency.takeover_policy.free_takeover_hours = int(hours_match.group(1))
                self.logger.info(f"✓ Free takeover hours: {agency.takeover_policy.free_takeover_hours} | Source: {url}")
            
            weeks_match = re.search(r'na\s+(\d+)\s+weken?\s+(?:gratis|kosteloos|zonder\s+kosten)?\s*(?:overnemen|overname)', takeover_text)
            if weeks_match:
                if not agency.takeover_policy:
                    from staffing_agency_scraper.models.agency import TakeoverPolicy
                    agency.takeover_policy = TakeoverPolicy()
                agency.takeover_policy.free_takeover_weeks = int(weeks_match.group(1))
                self.logger.info(f"✓ Free takeover weeks: {agency.takeover_policy.free_takeover_weeks} | Source: {url}")
            
            # Check for takeover fee
            # Pattern: "30% van het Opdrachtgeverstarief"
            fee_match = re.search(r'(\d+)%\s*(?:van|of)?\s*(?:het\s+)?(?:laatst\s+)?(?:geldende\s+)?(?:opdrachtgeverstarief|tarief)', takeover_text)
            if fee_match:
                if not agency.takeover_policy:
                    from staffing_agency_scraper.models.agency import TakeoverPolicy
                    agency.takeover_policy = TakeoverPolicy()
                from staffing_agency_scraper.models.agency import OvernameFeeModel
                agency.takeover_policy.overname_fee_model = OvernameFeeModel.PERCENTAGE_SALARY
                percentage = fee_match.group(1)
                agency.takeover_policy.overname_fee_hint = f"{percentage}% van Opdrachtgeverstarief"
                self.logger.info(f"✓ Takeover fee: {agency.takeover_policy.overname_fee_hint} | Source: {url}")
            
            # Set contract reference
            if not agency.takeover_policy.overname_contract_reference:
                agency.takeover_policy.overname_contract_reference = url
        
        # Extract minimum assignment duration
        # Pattern: "minimaal X weken", "minimum X maanden"
        min_duration_match = re.search(r'minim(?:aal|um)\s+(\d+)\s+(?:weken?|maanden?)', pdf_text_lower)
        if min_duration_match:
            duration = int(min_duration_match.group(1))
            unit = min_duration_match.group(0).split()[-1]
            if "maand" in unit:
                duration_weeks = duration * 4
            else:
                duration_weeks = duration
            
            if not agency.min_assignment_duration_weeks or agency.min_assignment_duration_weeks > duration_weeks:
                agency.min_assignment_duration_weeks = duration_weeks
                self.logger.info(f"✓ Min assignment duration: {duration_weeks} weeks | Source: {url}")
        
        # Extract minimum hours per week
        min_hours_match = re.search(r'minim(?:aal|um)\s+(\d+)\s+uur\s+(?:per\s+week)?', pdf_text_lower)
        if min_hours_match:
            hours = int(min_hours_match.group(1))
            if not agency.min_hours_per_week or agency.min_hours_per_week > hours:
                agency.min_hours_per_week = hours
                self.logger.info(f"✓ Min hours per week: {hours} | Source: {url}")
        
        # Extract omrekenfactor if mentioned
        omrekenfactor_match = re.search(r'omrekenfactor\s+(?:van\s+)?(\d+[.,]\d+)', pdf_text_lower)
        if omrekenfactor_match:
            factor = float(omrekenfactor_match.group(1).replace(',', '.'))
            if not agency.omrekenfactor_min or agency.omrekenfactor_min > factor:
                agency.omrekenfactor_min = factor
            if not agency.omrekenfactor_max or agency.omrekenfactor_max < factor:
                agency.omrekenfactor_max = factor
            self.logger.info(f"✓ Omrekenfactor: {factor} | Source: {url}")
        
        # Extract CAO type if mentioned
        if "abu cao" in pdf_text_lower or "abu-cao" in pdf_text_lower:
            from staffing_agency_scraper.models.agency import CaoType
            if agency.cao_type == CaoType.ONBEKEND:
                agency.cao_type = CaoType.ABU
                self.logger.info(f"✓ CAO type: ABU (from PDF) | Source: {url}")
        
        self.logger.info(f"✓ Other terms data extraction completed | Source: {url}")


@dg.asset(group_name="agencies")
def youngcapital_scrape() -> dg.Output[dict]:
    """Scrape Youngcapital website."""
    scraper = YoungCapitalScraper()
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

