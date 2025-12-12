"""
YoungCapital Netherlands scraper.

Website: https://www.youngcapital.nl
"""

from __future__ import annotations

from typing import Any, Dict, List, Set

import dagster as dg
from bs4 import BeautifulSoup

from staffing_agency_scraper.models import Agency, GeoFocusType, OfficeLocation
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
                        
                        # Extract social media links
                        if "sameAs" in data:
                            social_links = data["sameAs"]
                            if social_links:
                                self.logger.info(f"✓ Found {len(social_links)} social media links | Source: {url}")
                                # Add social media presence to growth signals
                                for link in social_links:
                                    if "linkedin.com" in link:
                                        agency.growth_signals.append("linkedin_presence")
                                    elif "facebook.com" in link:
                                        agency.growth_signals.append("facebook_presence")
                                    elif "twitter.com" in link or "x.com" in link:
                                        agency.growth_signals.append("twitter_presence")
                                    elif "youtube.com" in link:
                                        agency.growth_signals.append("youtube_presence")
                                    elif "instagram.com" in link:
                                        agency.growth_signals.append("instagram_presence")
                                    elif "tiktok.com" in link:
                                        agency.growth_signals.append("tiktok_presence")
                                
                                # Deduplicate growth signals
                                agency.growth_signals = list(set(agency.growth_signals))
                        
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
        
        # Add growth signal for comprehensive certifications
        if len(agency.certifications) >= 5:
            signal = f"{len(agency.certifications)} certifications"
            if signal not in agency.growth_signals:
                agency.growth_signals.append(signal)
                self.logger.info(f"✓ Growth signal: {signal} | Source: {url}")
        
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
        Extract office locations from embedded JSON data.
        The page loads offices dynamically via JavaScript variable 'uitzendbureauJSON'.
        """
        import re
        import json
        
        self.logger.info(f"🔍 Extracting office locations from embedded JSON on {url}")
        
        if not agency.office_locations:
            agency.office_locations = []
        
        # Find the script tag containing uitzendbureauJSON
        scripts = soup.find_all("script")
        
        offices_data = None
        
        for script in scripts:
            # script.string is often None for inline JS; get_text() is safer
            script_text = script.get_text() if script else ""
            
            if "var uitzendbureauJSON" not in script_text:
                continue
            
            # Try to capture the object assigned to uitzendbureauJSON
            match = re.search(r"var\\s+uitzendbureauJSON\\s*=\\s*(\\{.*?\\});", script_text, re.DOTALL)
            if not match:
                continue
            
            self.logger.info(f"✓ Found uitzendbureauJSON script | Source: {url}")
            
            json_str = match.group(1).rstrip(";").strip()
            try:
                parsed = json.loads(json_str)
            except (json.JSONDecodeError, ValueError) as e:
                self.logger.warning(f"⚠ Failed to parse uitzendbureauJSON: {e}")
                continue
            
            if "offices" in parsed and parsed["offices"]:
                offices_data = parsed
                self.logger.info(f"✓ Successfully parsed JSON data with {len(parsed['offices'])} offices | Source: {url}")
                break
            else:
                self.logger.info("  Parsed uitzendbureauJSON but no offices found; continuing search.")
        
        if not offices_data or "offices" not in offices_data:
            self.logger.warning(f"Could not find office data in uitzendbureauJSON on {url}")
            return
        
        offices_list = offices_data["offices"]
        self.logger.info(f"✓ Found {len(offices_list)} offices in JSON data | Source: {url}")
        
        # Extract each office
        for office_data in offices_list:
            try:
                # Get city name
                city_name = office_data.get("name", "")
                
                if not city_name:
                    # Try from address
                    address = office_data.get("address", {})
                    city_name = address.get("city", "")
                
                if not city_name:
                    self.logger.warning(f"  Skipped office with no city name: {office_data}")
                    continue
                
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
                self.logger.error(f"Error processing office data {office_data}: {e}")
                continue
        
        self.logger.info(f"✓ Total offices extracted: {len(agency.office_locations)} | Source: {url}")


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

