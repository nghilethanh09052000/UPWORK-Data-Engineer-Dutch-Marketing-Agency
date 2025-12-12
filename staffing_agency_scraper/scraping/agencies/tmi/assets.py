"""
TMI Netherlands scraper.

Website: https://www.tmi.nl
"""

from __future__ import annotations

from typing import Any, Dict, List
import json
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
        {
            "name": "employers",
            "url": "https://www.tmi.nl/opdrachtgevers",
            "functions": ['certifications', 'services', 'regions'],
        },
        {
            "name": "vacancies_hq",
            "url": "https://www.tmi.nl/over-tmi/vacatures-hoofdkantoor",
            "functions": ['office_locations'],
        },
        {
            "name": "working_abroad",
            "url": "https://www.tmi.nl/werken-in-zorg-buitenland",
            "functions": ['regions'],
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
        
        all_sectors = []
        all_text = ""  # Accumulate text from all pages for utils extraction
        main_soup = None  # Keep homepage soup for portal/review detection
        
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
                
                self.logger.info(f"✅ Completed: {page_name}")
                
            except Exception as e:
                self.logger.error(f"❌ Error scraping {url}: {e}")
        
        # Finalize sectors
        if all_sectors:
            agency.sectors_core = sorted(list(set(all_sectors)))
        
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
        all_sectors: List[str],
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
            
            elif func_name == "certifications":
                self._extract_certifications(soup, page_text, agency, url)
            
            elif func_name == "services":
                self._extract_services(soup, page_text, agency, url)
            
            elif func_name == "office_locations":
                self._extract_office_locations_from_vacancies(soup, page_text, agency, url)
            
            elif func_name == "regions":
                self._extract_regions_served(soup, page_text, agency, url)
    
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
        
        # Extract logo from JSON-LD schema (if not already extracted)
        if not agency.logo_url:
            # First try JSON-LD extraction
            json_ld_scripts = soup.find_all("script", type="application/ld+json")
            for script in json_ld_scripts:
                try:
                    data = json.loads(script.string)
                    
                    # Handle @graph structure
                    items = data.get("@graph", [data]) if "@graph" in data else [data]
                    
                    # Look for Organization type with logo
                    for item in items:
                        if item.get("@type") == "Organization" and "logo" in item:
                            logo_data = item["logo"]
                            if isinstance(logo_data, dict):
                                # Try 'url' first, then 'contentUrl'
                                logo_url = logo_data.get("url") or logo_data.get("contentUrl")
                                if logo_url:
                                    agency.logo_url = logo_url
                                    self.logger.info(f"✓ Logo extracted from JSON-LD: {logo_url} | Source: {url}")
                                    break
                            elif isinstance(logo_data, str):
                                agency.logo_url = logo_data
                                self.logger.info(f"✓ Logo extracted from JSON-LD: {logo_data} | Source: {url}")
                                break
                        
                        if agency.logo_url:
                            break
                except (json.JSONDecodeError, KeyError, AttributeError) as e:
                    self.logger.warning(f"⚠ Failed to parse JSON-LD for logo: {e}")
                    continue
            
            # Fallback to standard logo extraction if JSON-LD didn't work
            if not agency.logo_url:
                logo = self.utils.fetch_logo(soup, url)
                if logo:
                    agency.logo_url = logo
    
    def _extract_reviews(
        self, soup: BeautifulSoup, page_text: str, agency: Agency, url: str
    ) -> None:
        """Extract Google reviews from homepage, including count and average rating."""
        self.logger.info(f"🔍 Extracting reviews from {url}")
        
        # Find Google review section
        review_section = soup.find("section", id="section-google-review")
        if not review_section:
            self.logger.warning(f"⚠ Review section not found on {url}")
            return
        
        self.logger.info(f"✓ Google review section found | Source: {url}")
        
        # Count review cards
        review_cards = review_section.find_all("div", class_="indrevdiv")
        review_count = len(review_cards)
        
        if review_count > 0:
            self.logger.info(f"✓ Found {review_count} visible review cards | Source: {url}")
            
            # Calculate average rating by counting stars in each card
            total_rating = 0
            ratings_found = 0
            
            for card in review_cards:
                # Find star container
                star_div = card.find("div", class_="wpproslider_t6_star_DIV")
                if star_div:
                    # Count filled stars (svg-wprsp-star)
                    filled_stars = star_div.find_all("span", class_="svg-wprsp-star")
                    # Count empty stars (svg-wprsp-star-o)
                    empty_stars = star_div.find_all("span", class_="svg-wprsp-star-o")
                    
                    rating = len(filled_stars)
                    if rating > 0:
                        total_rating += rating
                        ratings_found += 1
                        self.logger.info(f"  Review {ratings_found}: {rating} stars (filled: {len(filled_stars)}, empty: {len(empty_stars)})")
            
            # Calculate average rating
            if ratings_found > 0:
                avg_rating = round(total_rating / ratings_found, 1)
                agency.review_rating = avg_rating
                agency.review_count = review_count
                self.logger.info(f"✓ Review rating: {avg_rating}/5.0 (from {ratings_found} reviews) | Source: {url}")
                self.logger.info(f"✓ Review count: {review_count} visible reviews | Source: {url}")
        
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
    
    def _extract_sectors(
        self, soup: BeautifulSoup, page_text: str, all_sectors: List[str], url: str
    ) -> None:
        """TMI is 100% healthcare-focused - hardcode 'Zorg' as the only sector."""
        self.logger.info(f"🔍 Setting TMI sector to 'Zorg' (Healthcare)")
        
        # TMI is exclusively a healthcare staffing agency
        # All their sub-sectors (Apotheken, Artsen, GGD, etc.) fall under "Zorg"
        if "Zorg" not in all_sectors:
            all_sectors.append("Zorg")
            self.logger.info(f"✓ Sector: 'Zorg' (Healthcare vertical) | Source: {url}")
    
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
    
    def _extract_certifications(
        self, soup: BeautifulSoup, page_text: str, agency: Agency, url: str
    ) -> None:
        """Extract certifications from employers/benefits page."""
        self.logger.info(f"🔍 Extracting certifications from {url}")
        
        # Find the benefits section
        benefits_section = soup.find("section", id="section-benefits")
        if not benefits_section:
            self.logger.warning(f"⚠ Benefits section not found on {url}")
            return
        
        # Certification mapping for images
        cert_image_keywords = {
            "crkbo": "CRKBO",
            "tr-testmark": "TÜV Rheinland ISO",
            "tuv": "TÜV Rheinland ISO",
        }
        
        # Extract certifications from images
        iso_div = benefits_section.find("div", class_="iso")
        if iso_div:
            images = iso_div.find_all("img")
            for img in images:
                alt_text = img.get("alt", "").strip().lower()
                src = img.get("src", "").lower()
                
                # Check alt text and src for certification keywords
                for keyword, cert_name in cert_image_keywords.items():
                    if (keyword in alt_text or keyword in src):
                        if cert_name not in agency.certifications:
                            agency.certifications.append(cert_name)
                            source = "alt text" if keyword in alt_text else "filename"
                            self.logger.info(f"✓ Certification: {cert_name} (from image {source}) | Source: {url}")
                            break
        
        # Extract certifications from benefits text list
        benefits_ul = benefits_section.find("ul", class_="benefits")
        if benefits_ul:
            benefits_text = benefits_ul.get_text(separator=" ", strip=True)
            
            # Look for specific certifications mentioned in text
            cert_text_keywords = {
                "NEN 4400-1": ["nen 4400-1", "nen4400-1", "nen 4400"],
                "ISO 9001": ["iso 9001", "iso9001"],
                "ISO 14001": ["iso 14001", "iso14001"],
                "ISO 27001": ["iso 27001", "iso27001"],
                "WAADI": ["waadi"],
            }
            
            benefits_lower = benefits_text.lower()
            for cert_name, keywords in cert_text_keywords.items():
                for keyword in keywords:
                    if keyword in benefits_lower and cert_name not in agency.certifications:
                        agency.certifications.append(cert_name)
                        self.logger.info(f"✓ Certification: {cert_name} (from benefits text) | Source: {url}")
                        break
        
        # Add URL to evidence if certifications were found
        if agency.certifications and url not in self.evidence_urls:
            self.evidence_urls.append(url)
    
    def _extract_services(
        self, soup: BeautifulSoup, page_text: str, agency: Agency, url: str
    ) -> None:
        """Extract services from employers page."""
        self.logger.info(f"🔍 Extracting services from {url}")
        
        # Find the content section
        content_section = soup.find("section", id="section-content")
        if not content_section:
            self.logger.warning(f"⚠ Content section not found on {url}")
            return
        
        # Get text from the section
        section_text = content_section.get_text(separator=" ", strip=True).lower()
        
        # Service keyword mapping
        service_keywords = {
            "detacheren": ["detachering"],
            "werving_selectie": ["werving & selectie", "werving en selectie"],
            "zzp_bemiddeling": ["zzp-bemiddeling", "zzp bemiddeling", "zelfstandige professionals"],
            "flexbureau": ["flexbureau"],
        }
        
        services_found = []
        
        # Check for each service
        for service_attr, keywords in service_keywords.items():
            for keyword in keywords:
                if keyword in section_text:
                    # Set the service attribute
                    if service_attr == "detacheren":
                        agency.services.detacheren = True
                        services_found.append("Detachering")
                    elif service_attr == "werving_selectie":
                        agency.services.werving_selectie = True
                        services_found.append("Werving & Selectie")
                    elif service_attr == "zzp_bemiddeling":
                        agency.services.zzp_bemiddeling = True
                        services_found.append("ZZP Bemiddeling")
                    elif service_attr == "flexbureau":
                        # Flexbureau is a form of uitzenden (temporary staffing)
                        agency.services.uitzenden = True
                        services_found.append("Uitzenden (Flexbureau)")
                    
                    self.logger.info(f"✓ Service: {services_found[-1]} | Source: {url}")
                    break
        
        # Add URL to evidence if services were found
        if services_found and url not in self.evidence_urls:
            self.evidence_urls.append(url)
    
    def _extract_office_locations_from_vacancies(
        self, soup: BeautifulSoup, page_text: str, agency: Agency, url: str
    ) -> None:
        """Extract office locations from vacancy cards."""
        self.logger.info(f"🔍 Extracting office locations from vacancy cards on {url}")
        
        # Find all vacancy items
        vacancy_items = soup.find_all("div", class_="vacancy-item")
        if not vacancy_items:
            self.logger.warning(f"⚠ No vacancy items found on {url}")
            return
        
        self.logger.info(f"✓ Found {len(vacancy_items)} vacancy cards | Source: {url}")
        
        # Use a set to track unique locations
        locations_found = set()
        
        for vacancy in vacancy_items:
            # Find the meta div with location icon
            meta_info = vacancy.find("div", class_="meta-info")
            if not meta_info:
                continue
            
            # Find the location meta div (has icon-location-open)
            location_meta = meta_info.find("span", class_="icon-location-open")
            if location_meta:
                # Get the sibling span with h5 class that contains the location text
                location_span = location_meta.find_next_sibling("span", class_="h5")
                if location_span:
                    location_text = location_span.get_text(strip=True)
                    
                    # Parse "City, Province" format
                    if "," in location_text:
                        parts = [p.strip() for p in location_text.split(",")]
                        if len(parts) == 2:
                            city, province = parts
                            
                            # Create location tuple for deduplication
                            location_key = (city, province)
                            
                            if location_key not in locations_found:
                                locations_found.add(location_key)
                                
                                # Check if this location already exists in agency.office_locations
                                existing = any(
                                    loc.city == city and loc.province == province 
                                    for loc in agency.office_locations
                                )
                                
                                if not existing:
                                    office = OfficeLocation(city=city, province=province)
                                    agency.office_locations.append(office)
                                    self.logger.info(f"✓ Office location: {city}, {province} | Source: {url}")
        
        # Add URL to evidence if locations were found
        if locations_found and url not in self.evidence_urls:
            self.evidence_urls.append(url)
        
        self.logger.info(f"✓ Total unique locations from vacancies: {len(locations_found)} | Source: {url}")
    
    def _extract_regions_served(
        self, soup: BeautifulSoup, page_text: str, agency: Agency, url: str
    ) -> None:
        """Extract regions served from page (nationwide coverage and international regions)."""
        self.logger.info(f"🌍 Extracting regions served from {url}")
        
        # Initialize regions_served if not already set
        if not agency.regions_served:
            agency.regions_served = []
        
        regions_found = []
        
        # Check for nationwide coverage (landelijk)
        page_text_lower = page_text.lower()
        if any(keyword in page_text_lower for keyword in ["landelijk", "landelijke dekking", "landelijk netwerk", "heel nederland"]):
            if "landelijk" not in agency.regions_served:
                regions_found.append("landelijk")
                agency.regions_served.append("landelijk")
                self.logger.info(f"✓ Region: landelijk (nationwide coverage) | Source: {url}")
        
        # Look for "Werken in het buitenland" navigation menu
        nav = soup.find("nav", class_="main-nav")
        if nav:
            # Find the "Werken in het buitenland" menu item
            abroad_menu = None
            for link in nav.find_all("a"):
                if "werken in het buitenland" in link.get_text(strip=True).lower():
                    # Found the link, now find its parent menu structure
                    parent_li = link.find_parent("li", class_="menu-item-has-children")
                    if parent_li:
                        abroad_menu = parent_li
                        break
            
            if abroad_menu:
                # Find sub-menu with countries/regions
                sub_menu = abroad_menu.find("ul", class_="sub-menu")
                if sub_menu:
                    # Get all menu items (excluding the parent "Werken in het buitenland")
                    for menu_item in sub_menu.find_all("li", class_="menu-item", recursive=False):
                        link = menu_item.find("a", recursive=False)
                        if link:
                            region_text = link.get_text(strip=True)
                            
                            # Skip the parent wrapper link "Werken in het buitenland"
                            if region_text.lower() == "werken in het buitenland":
                                continue
                            
                            # Check if this is a submenu item (like Caribbean countries)
                            has_submenu = menu_item.find("ul", class_="sub-menu")
                            
                            if has_submenu:
                                # This is a region group like "Caribbean", extract countries
                                for sub_item in has_submenu.find_all("li", class_="menu-item"):
                                    sub_link = sub_item.find("a")
                                    if sub_link:
                                        country = sub_link.get_text(strip=True)
                                        if country and country not in agency.regions_served:
                                            regions_found.append(country)
                                            agency.regions_served.append(country)
                                            self.logger.info(f"✓ Region: {country} | Source: {url}")
                            else:
                                # Direct region like "Suriname" or "Zwitserland"
                                if region_text and region_text not in agency.regions_served:
                                    regions_found.append(region_text)
                                    agency.regions_served.append(region_text)
                                    self.logger.info(f"✓ Region: {region_text} | Source: {url}")
        
        # Fallback: look for mentions in page content (only on working abroad page)
        if "werken-in-zorg-buitenland" in url and len(regions_found) <= 1:
            self.logger.warning(f"⚠ Could not extract regions from navigation, using fallback | Source: {url}")
            
            # Look for explicit country mentions in text
            fallback_regions = {
                "Aruba": ["aruba"],
                "Bonaire": ["bonaire"],
                "Curaçao": ["curaçao", "curacao"],
                "Saba": ["saba"],
                "Sint Eustatius": ["sint eustatius", "st. eustatius"],
                "Sint Maarten": ["sint maarten", "st. maarten"],
                "Suriname": ["suriname"],
                "Zwitserland": ["zwitserland", "switzerland"],
            }
            
            for region, keywords in fallback_regions.items():
                for keyword in keywords:
                    if keyword in page_text_lower and region not in agency.regions_served:
                        regions_found.append(region)
                        agency.regions_served.append(region)
                        self.logger.info(f"✓ Region: {region} (fallback) | Source: {url}")
                        break
        
        # Sort and deduplicate regions_served
        if agency.regions_served:
            agency.regions_served = sorted(list(set(agency.regions_served)))
            self.logger.info(f"✓ Total regions served: {len(agency.regions_served)} | Source: {url}")
            
            # Add URL to evidence
            if regions_found and url not in self.evidence_urls:
                self.evidence_urls.append(url)


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

