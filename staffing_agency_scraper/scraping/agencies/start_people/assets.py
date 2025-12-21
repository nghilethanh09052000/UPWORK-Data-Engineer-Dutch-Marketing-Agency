"""
Start People Netherlands scraper.

Website: https://www.startpeople.nl
Part of: RGF Staffing"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Set

import dagster as dg
from bs4 import BeautifulSoup

from staffing_agency_scraper.models import Agency, CaoType, GeoFocusType, OfficeLocation
from staffing_agency_scraper.scraping.base import BaseAgencyScraper
from staffing_agency_scraper.scraping.utils import AgencyScraperUtils

class StartPeopleScraper(BaseAgencyScraper):
    """Scraper for Start People Netherlands."""

    AGENCY_NAME = "Start People"
    WEBSITE_URL = "https://www.startpeople.nl"
    BRAND_GROUP = "RGF Staffing"
    PAGES_TO_SCRAPE: List[Dict[str, Any]] = [
        {
            "name": "home",
            "url": "https://www.startpeople.nl",
            "functions": ['logo', 'header', 'footer'],
        },
        {
            "name": "werkgevers",
            "url": "https://startpeople.nl/werkgevers",
            "functions": ['services', 'value_props'],
        },
        {
            "name": "vakgebieden",
            "url": "https://www.startpeople.nl/vacatures/vakgebieden",
            "functions": ['sectors'],
        },
        {
            "name": "over_ons",
            "url": "https://www.startpeople.nl/over-ons",
            "functions": ['history', 'training'],
        },
        {
            "name": "keurmerken",
            "url": "https://www.startpeople.nl/keurmerken-en-certificaten",
            "functions": ['certifications'],
        },
        {
            "name": "vestigingen",
            "url": "https://www.startpeople.nl/vestigingen",
            "functions": ['offices_paginated'],
        },
        {
            "name": "privacy",
            "url": "https://startpeople.nl/legal/privacy-statement",
            "functions": ['legal_info'],
        },
    ]

    def scrape(self) -> Agency:
        self.logger.info(f"Starting scrape of {self.AGENCY_NAME}")
        
        # Initialize utils
        self.utils = AgencyScraperUtils(logger=self.logger)
        agency = self.create_base_agency()
        # geo_focus_type will be extracted from over-ons page
        agency.employers_page_url = f"{self.WEBSITE_URL}/werkgevers"
        agency.contact_form_url = f"{self.WEBSITE_URL}/contact"
        
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
        # Combine all page texts into a single string for common field extraction
        all_text = " ".join(page_texts.values())
        self.extract_all_common_fields(agency, all_text)
        
        # Finalize
        if all_sectors:
            agency.sectors_core = sorted(list(all_sectors))
        
        # Filter evidence URLs: exclude privacy statement page
        agency.evidence_urls = self._filter_start_people_evidence_urls()
        agency.collected_at = self.collected_at
        
        # Final normalization: ensure contact_phone is normalized
        from staffing_agency_scraper.lib.normalize import normalize_contact_phone
        if agency.contact_phone:
            original_phone = agency.contact_phone
            agency.contact_phone = normalize_contact_phone(agency.contact_phone)
            if agency.contact_phone != original_phone:
                self.logger.info(f"✓ Final normalization: contact_phone {original_phone} -> {agency.contact_phone}")
        
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
                logo = self.utils.fetch_logo(soup, url)
                if logo:
                    agency.logo_url = logo
                    self.logger.info(f"✓ Found logo: {logo} | Source: {url}")
            
            elif func_name == "header":
                self._extract_header(soup, agency, url)
            
            elif func_name == "footer":
                self._extract_footer(soup, agency, url)
            
            elif func_name == "services":
                self._extract_services(soup, page_text, agency, url)
            
            elif func_name == "value_props":
                self._extract_value_props(soup, agency, url)
            
            elif func_name == "sectors":
                self._extract_sectors(soup, all_sectors, url)
            
            elif func_name == "certifications":
                self._extract_certifications(soup, page_text, agency, url)
            
            elif func_name == "history":
                self._extract_history(page_text, agency, url)
            
            elif func_name == "training":
                self._extract_training(page_text, agency, url)
            
            elif func_name == "legal_info":
                self._extract_legal_info(soup, agency, url)
            
            elif func_name == "offices_paginated":
                self._extract_offices_paginated(agency, url)
            
    
    def _extract_header(self, soup: BeautifulSoup, agency: Agency, url: str) -> None:
        """
        Extract data from header:
        - Logo (proper SVG)
        - Multi-language support (growth signal)
        - Portal links
        - Services from navigation
        """
        header = soup.find("header")
        if not header:
            return
        
        # Extract logo (SVG version)
        logo_link = header.find("img", src=lambda x: x and "logo-start-people.svg" in x)
        if logo_link:
            src = logo_link.get("src")
            if src:
                if not src.startswith("http"):
                    src = f"https://www.startpeople.nl{src}" if src.startswith("/") else f"https://www.startpeople.nl/{src}"
                agency.logo_url = src
                self.logger.info(f"✓ Found logo (SVG): {src} | Source: {url}")
        
        # Extract multi-language support (growth signal)
        lang_select = header.find("select", id=lambda x: x and "LanguageSelector" in x)
        if lang_select:
            languages = [opt.get("value") for opt in lang_select.find_all("option") if opt.get("value")]
            # Removed threshold assumption (>= 3) - only set if explicitly stated
            # Multi-language support should be explicitly mentioned, not inferred from language count
            if languages:
                self.logger.info(f"✓ Found {len(languages)} languages: {', '.join(languages)} (explicitly listed) | Source: {url}")
        
        # Detect portals from login menu (can be in header or anywhere on page)
        # The login menu has id="login-menu" and contains portal links
        login_menu = soup.find("div", id="login-menu")
        if not login_menu:
            # Also try to find it in header specifically
            login_menu = header.find("div", id="login-menu")
        
        if login_menu:
            # Find all links in the login menu
            all_links = login_menu.find_all("a")
            
            for link in all_links:
                href = link.get("href", "").lower()
                link_text = link.get_text(strip=True).lower()
                
                # Candidate portal: link to "mijn-start-people" or text contains "flexmedewerker"
                if ("mijn-start-people" in href or "flexmedewerker" in link_text) and "werkgevers" not in href:
                    agency.digital_capabilities.candidate_portal = True
                    self.logger.info(f"✓ Found candidate portal: {link.get_text(strip=True)} ({href}) | Source: {url}")
                
                # Client portal: link to "mijnstartpeople" with "werkgevers" or text contains "opdrachtgever"
                elif ("mijnstartpeople" in href and "werkgevers" in href) or "opdrachtgever" in link_text:
                    agency.digital_capabilities.client_portal = True
                    self.logger.info(f"✓ Found client portal: {link.get_text(strip=True)} ({href}) | Source: {url}")
    
    def _extract_footer(self, soup: BeautifulSoup, agency: Agency, url: str) -> None:
        """
        Extract data from footer:
        - HQ address: P.J. Oudweg 61, 1314 CK Almere
        - Contact email: info@startpeople.nl
        - Social media: LinkedIn, Facebook, Instagram
        - Top employers (growth signal)
        """
        footer = soup.find("footer")
        if not footer:
            return
        
        footer_text = footer.get_text(separator=" ", strip=True)
        
        # Extract contact email
        if "info@startpeople.nl" in footer_text:
            agency.contact_email = "info@startpeople.nl"
            self.logger.info(f"✓ Found contact email: info@startpeople.nl | Source: {url}")
        
        # Extract social media
        social_platforms = []
        social_links = {
            "linkedin.com": "LinkedIn",
            "facebook.com": "Facebook",
            "instagram.com": "Instagram"
        }
        for domain, platform in social_links.items():
            link = footer.find("a", href=lambda x: x and domain in x)
            if link:
                social_platforms.append(platform)
        
        if social_platforms:
            self.logger.info(f"✓ Found social media: {', '.join(social_platforms)} | Source: {url}")
        
        # Extract top employers (major clients) - growth signal
        top_employers = []
        employer_keywords = ["frieslandcampina", "abbott", "coa", "defensie", "ups", "ind", "reclassering", "tilburg"]
        for link in footer.find_all("a"):
            href = link.get("href", "").lower()
            text = link.get_text(strip=True).lower()
            if any(kw in href or kw in text for kw in employer_keywords):
                employer_name = link.get_text(strip=True)
                if employer_name and employer_name not in top_employers and len(employer_name) > 2:
                    top_employers.append(employer_name)
        
        # Removed threshold assumption (>= 3) - only set if explicitly listed
        if top_employers:
            if not agency.growth_signals:
                agency.growth_signals = []
            if "werkt_met_fortune500_klanten" not in agency.growth_signals:
                agency.growth_signals.append("werkt_met_fortune500_klanten")
            self.logger.info(f"✓ Found {len(top_employers)} top employers: {', '.join(top_employers[:5])}... (explicitly listed) | Source: {url}")
    
    def _extract_services(self, soup: BeautifulSoup, page_text: str, agency: Agency, url: str) -> None:
        """
        Extract services from werkgevers page using BeautifulSoup.
        Looks for service cards with titles like "Uitzenden", "Detacheren", etc.
        """
        services_found = []
        
        # Look for service cards - they have class 'text-2xl xl:text-6xl font-title font-bold'
        service_headings = soup.find_all('div', class_=lambda x: x and 'text-2xl xl:text-6xl font-title font-bold' in x)
        
        for heading in service_headings:
            service_title = heading.get_text(strip=True).lower()
            
            if "uitzenden" in service_title or "broadcast" in service_title:
                agency.services.uitzenden = True
                services_found.append("uitzenden")
                self.logger.info(f"  → Found service: Uitzenden (temporary employment)")
            
            elif "detacheren" in service_title or "assign" in service_title:
                agency.services.detacheren = True
                services_found.append("detacheren")
                self.logger.info(f"  → Found service: Detacheren (secondment)")
            
            elif "werving" in service_title or "selectie" in service_title or "recruitment" in service_title:
                agency.services.werving_selectie = True
                services_found.append("werving_selectie")
                self.logger.info(f"  → Found service: Werving & Selectie")
            
            elif "zzp" in service_title or "zelfstandig" in service_title or "self-employed" in service_title:
                agency.services.zzp_bemiddeling = True
                services_found.append("zzp_bemiddeling")
                self.logger.info(f"  → Found service: ZZP/Freelance")
        
        # Also check for inhouse services and 24/7 in page text
        if "inhouse" in page_text.lower():
            agency.services.inhouse_services = True
            services_found.append("inhouse_services")
            self.logger.info(f"  → Found service: Inhouse Services")
        
        if "24/7" in page_text or "bereikbaarheidsservice" in page_text.lower():
            if not agency.growth_signals:
                agency.growth_signals = []
            if "24_7_bereikbaar" not in agency.growth_signals:
                agency.growth_signals.append("24_7_bereikbaar")
            self.logger.info(f"  → Found service: 24/7 Bereikbaarheidsservice")
        
        # Check for training/opleidingen
        if "opleidingen" in page_text.lower() or "training" in page_text.lower():
            agency.services.opleiden_ontwikkelen = True
            services_found.append("opleiden_ontwikkelen")
            self.logger.info(f"  → Found service: Opleidingen en Training")
        
        if services_found:
            self.logger.info(f"✓ Total services found: {len(services_found)} | Source: {url}")
    
    def _extract_value_props(self, soup: BeautifulSoup, agency: Agency, url: str) -> None:
        """
        Extract value propositions from the werkgevers page:
        - Large candidate database
        - National coverage
        - Experienced employees
        """
        value_props_found = []
        
        # Look for the "Wat wij u bieden" section
        # These are in cards with icon-people, icon-hat, icon-home
        value_prop_cards = soup.find_all('h3', class_='text-4xl')
        
        for card in value_prop_cards:
            title_text = card.get_text(strip=True).lower()
            
            # Only extract factual signals - NO marketing language
            # "grote database" and "ervaren medewerkers" are marketing claims, not factual growth signals
            if "dekking" in title_text or "landelijke dekking" in title_text:
                value_props_found.append("landelijke_dekking")
                self.logger.info(f"  → Value prop: National coverage")
        
        # Add only factual value props to growth signals
        if value_props_found:
            if not agency.growth_signals:
                agency.growth_signals = []
            for prop in value_props_found:
                if prop not in agency.growth_signals:
                    agency.growth_signals.append(prop)
            self.logger.info(f"✓ Total value propositions: {len(value_props_found)} | Source: {url}")
    
    def _extract_sectors(self, soup: BeautifulSoup, all_sectors, url: str) -> None:
        """
        Extract 8 core sectors from vakgebieden page swiper slider.
        
        Extracts directly from swiper-slide h3 elements without any mapping.
        Expected sectors: Productie, Logistiek, Overheid, Administratie, 
                         Klantenservice, Techniek, Hr, Operator
        """
        # Find swiper-wrapper div containing sector cards
        swiper = soup.find("div", class_=lambda x: x and "swiper-wrapper" in x)
        if swiper:
            # Extract h3 text from each swiper-slide
            for slide in swiper.find_all("div", class_=lambda x: x and "swiper-slide" in x):
                h3 = slide.find("h3")
                if h3:
                    sector = h3.get_text(strip=True)
                    if sector:
                        all_sectors.add(sector)
                        self.logger.info(f"✓ Found sector: {sector} | Source: {url}")
        else:
            self.logger.warning(f"⚠ No swiper-wrapper found for sectors | Source: {url}")
    
    def _extract_certifications(self, soup: BeautifulSoup, page_text: str, agency: Agency, url: str) -> None:
        """
        Extract certifications from keurmerken-en-certificaten page using BeautifulSoup.
        Looks for h3 headings and certification names.
        """
        if not agency.certifications:
            agency.certifications = []
        
        certs_found = []
        
        # Look for all h3 headings which contain certification titles
        headings = soup.find_all('h3')
        
        for heading in headings:
            text = heading.get_text(strip=True).lower()
            
            # ABU (Algemene Bond van Uitzendondernemingen)
            if "abu" in text and "ABU" not in agency.certifications:
                agency.certifications.append("ABU")
                certs_found.append("ABU")
                self.logger.info(f"  → Certification: ABU (General Association of Temporary Employment Agencies)")
                
                # Add ABU to membership and set CAO type
                if not agency.membership:
                    agency.membership = []
                if "ABU" not in agency.membership:
                    agency.membership.append("ABU")
                    self.logger.info(f"  → Membership: ABU | Source: {url}")
                
                # Set CAO type to ABU if not already set
                if not agency.cao_type or agency.cao_type == CaoType.ONBEKEND:
                    agency.cao_type = CaoType.ABU
                    self.logger.info(f"  → CAO type: ABU (from ABU membership) | Source: {url}")
            
            # SNA (Stichting Normering Arbeid)
            if "sna" in text or "normering arbeid" in text:
                if "SNA" not in agency.certifications:
                    agency.certifications.append("SNA")
                    certs_found.append("SNA")
                    self.logger.info(f"  → Certification: SNA (Labour Standards Foundation)")
            
            # PSO (Prestatieladder Sociaal Ondernemen)
            if "pso" in text or "sociaal ondernemen" in text:
                if "PSO" not in agency.certifications:
                    agency.certifications.append("PSO")
                    certs_found.append("PSO")
                    self.logger.info(f"  → Certification: PSO (Social Entrepreneurship)")
        
        # Look for ISO certifications in the page text
        iso_certs = {
            "ISO_9001": ["iso 9001", "iso9001"],
            "ISO_14001": ["iso 14001", "iso14001"],
            "ISO_27001": ["iso 27001", "iso27001"],
        }
        
        for cert_name, keywords in iso_certs.items():
            if any(kw in page_text.lower() for kw in keywords):
                if cert_name not in agency.certifications:
                    agency.certifications.append(cert_name)
                    certs_found.append(cert_name)
                    self.logger.info(f"  → Certification: {cert_name}")
        
        # Check for other certifications
        other_certs = {
            "NEN4400-1": ["nen4400", "nen 4400"],
            "Ecovadis": ["ecovadis"],
            "CO2_Reductiemanagement": ["co2", "reductiemanagement"],
            "Doorzaam": ["doorzaam", "leerwerkintermediair"],
            "SBB_Erkend_Leerbedrijf": ["sbb", "erkend leerbedrijf", "samenwerkingsorganisatie beroepsonderwijs"],
        }
        
        for cert_name, keywords in other_certs.items():
            if any(kw in page_text.lower() for kw in keywords):
                if cert_name not in agency.certifications:
                    agency.certifications.append(cert_name)
                    certs_found.append(cert_name)
                    self.logger.info(f"  → Certification: {cert_name}")
        
        if certs_found:
            self.logger.info(f"✓ Total certifications found: {len(certs_found)} | Source: {url}")
        else:
            self.logger.warning(f"⚠ No certifications found on page | Source: {url}")
    
    def _extract_history(self, page_text: str, agency: Agency, url: str) -> None:
        """Extract company history/founding year."""
        # Look for founding year (e.g., "Sinds 1977")
        year_match = re.search(r'(?:sinds|founded|opgericht).*?(\d{4})', page_text, re.IGNORECASE)
        if year_match:
            year = int(year_match.group(1))
            if 1900 <= year <= 2024:  # Sanity check
                if not agency.growth_signals:
                    agency.growth_signals = []
                signal = f"sinds_{year}_actief"
                if signal not in agency.growth_signals:
                    agency.growth_signals.append(signal)
                current_year = 2025
                years_active = current_year - year
                self.logger.info(f"✓ Found founding year: {year} ({years_active} years active) | Source: {url}")
    
    def _extract_training(self, page_text: str, agency: Agency, url: str) -> None:
        """
        Extract training program information from over-ons page:
        - Training thousands of flex workers annually
        - Confirms opleiden_ontwikkelen service
        - National coverage (geo_focus_type)
        """
        # Check for training mentions
        if "duizenden flexmedewerkers op" in page_text.lower() or "thousands of flex workers" in page_text.lower():
            agency.services.opleiden_ontwikkelen = True
            self.logger.info(f"✓ Found training program: trains thousands of flex workers annually | Source: {url}")
            
            if not agency.growth_signals:
                agency.growth_signals = []
            if "traint_duizenden_flexwerkers_per_jaar" not in agency.growth_signals:
                agency.growth_signals.append("traint_duizenden_flexwerkers_per_jaar")
        
        # Check for national coverage - indicates geo_focus_type
        if "vestigingen door het hele land" in page_text.lower() or "branches throughout the country" in page_text.lower() or "landelijke dekking" in page_text.lower() or "national coverage" in page_text.lower():
            # Set geo_focus_type to national
            agency.geo_focus_type = GeoFocusType.NATIONAL
            self.logger.info(f"✓ Found geo_focus_type: NATIONAL (branches throughout the country) | Source: {url}")
            
            if not agency.growth_signals:
                agency.growth_signals = []
            if "landelijke_dekking" not in agency.growth_signals:
                agency.growth_signals.append("landelijke_dekking")
        
        # Note: "langdurige_klantrelaties" is marketing language, not a factual growth signal
        # Removed from growth_signals per client feedback
    
    def _extract_offices_paginated(self, agency: Agency, base_url: str) -> None:
        """
        Extract representative office locations from vestigingen page (first page only, limited to 10).
        """
        self.logger.info(f"🔍 Fetching offices from: {base_url}")
        
        try:
            soup = self.fetch_page(base_url)
            
            # Find all office cards in the list
            office_list = soup.find("ul", {"data-insights-index": "prd_start_people_office"})
            if not office_list:
                self.logger.warning(f"No office list found on {base_url}")
                return
            
            office_cards = office_list.find_all("li", class_=lambda x: x and "border-gradient" in x)
            
            if not office_cards:
                self.logger.warning(f"No office cards found on {base_url}")
                return
            
            all_offices = []
            
            # Limit to 10 representative offices
            for card in office_cards[:10]:
                try:
                    # Extract office name
                    h3 = card.find("h3")
                    if not h3:
                        continue
                    
                    office_name = h3.get_text(strip=True)
                    
                    # Extract address
                    address_div = card.find("div", class_="flex gap-3 mb-4")
                    if not address_div:
                        continue
                    
                    address_text = address_div.get_text(separator=" ", strip=True)
                    
                    # Parse address (format: "Street Number PostCode City")
                    # Example: "P.J. Oudweg 61 1314 CK Almere"
                    address_match = re.search(r'Bezoekadres\s+(.+?)\s+(\d{4}\s+[A-Z]{2})\s+(.+)', address_text)
                    if address_match:
                        street = address_match.group(1).strip()
                        postcode = address_match.group(2).strip()
                        city = address_match.group(3).strip()
                    else:
                        # Fallback: try to extract city from the last part
                        parts = address_text.split()
                        if len(parts) >= 2:
                            city = parts[-1]
                            street = " ".join(parts[:-3]) if len(parts) > 3 else ""
                            postcode = " ".join(parts[-3:-1]) if len(parts) > 3 else ""
                        else:
                            continue
                    
                    # Extract phone
                    phone_link = card.find("a", href=lambda x: x and "tel:" in x)
                    phone = phone_link.get("href").replace("tel:", "").strip() if phone_link else None
                    
                    # Normalize phone if present
                    if phone:
                        from staffing_agency_scraper.lib.normalize import normalize_contact_phone
                        phone = normalize_contact_phone(phone)
                    
                    # Map city to province
                    province = self.utils.map_city_to_province(city)
                    
                    office = OfficeLocation(
                        city=city,
                        province=province,
                        street=street if street else None,
                        postcode=postcode if postcode else None,
                        phone=phone
                    )
                    
                    all_offices.append(office)
                    self.logger.info(f"✓ Found office: {city}, {province} | Source: {base_url}")
                    
                except Exception as e:
                    self.logger.warning(f"Error parsing office card: {e}")
                    continue
            
            if all_offices:
                agency.office_locations = all_offices
                self.logger.info(f"✅ Extracted {len(all_offices)} representative offices (limited to 10) | Source: {base_url}")
            else:
                self.logger.warning(f"No offices extracted from {base_url}")
                
        except Exception as e:
            self.logger.error(f"Error fetching offices from {base_url}: {e}")
    
    def _extract_legal_info(self, soup: BeautifulSoup, agency: Agency, url: str) -> None:
        """
        Extract legal information from Start People privacy statement page.
        
        URL: https://startpeople.nl/legal/privacy-statement (HTML page with PDF link)
        
        The page contains a link to the actual PDF at https://rgfstaffing.nl/privacy-statement
        
        Expected data from PDF:
        - Legal name: "RGF Staffing the Netherlands B.V."
        - HQ: "P.J. Oudweg 61 in (1314 CK) Almere"
        - Phone: "+31 (0)36 529 9555"
        - Parent: "Recruit Holdings Co. Ltd"
        """
        try:
            # Find the PDF link in the sfContentBlock div
            pdf_url = None
            pdf_text = ""
            
            # Look for the link in the content block
            content_block = soup.find("div", class_=lambda x: x and "sfContentBlock" in x)
            if content_block:
                # Find the link to the privacy statement PDF
                pdf_link = content_block.find("a", href=lambda x: x and "privacy-statement" in str(x).lower())
                if pdf_link:
                    pdf_url = pdf_link.get("href")
                    # Make absolute URL if relative
                    if pdf_url and not pdf_url.startswith("http"):
                        if pdf_url.startswith("/"):
                            pdf_url = f"https://rgfstaffing.nl{pdf_url}"
                        else:
                            pdf_url = f"https://rgfstaffing.nl/{pdf_url}"
                    self.logger.info(f"🔗 Found PDF link in HTML: {pdf_url}")
            
            # Fallback: try to find any link with .pdf or privacy-statement
            if not pdf_url:
                pdf_link = soup.find("a", href=lambda x: x and ('.pdf' in str(x).lower() or 'privacy-statement' in str(x).lower()))
                if pdf_link:
                    pdf_url = pdf_link.get("href")
                    if pdf_url and not pdf_url.startswith("http"):
                        if pdf_url.startswith("/"):
                            pdf_url = f"https://rgfstaffing.nl{pdf_url}"
                        else:
                            pdf_url = f"https://rgfstaffing.nl/{pdf_url}"
                    self.logger.info(f"🔗 Found PDF link (fallback): {pdf_url}")
            
            if not pdf_url:
                self.logger.warning(f"⚠ No PDF link found on privacy statement page | Source: {url}")
                return
            
            # Add PDF URL to evidence URLs
            self.evidence_urls.append(pdf_url)
            self.logger.info(f"✓ Added PDF URL to evidence_urls: {pdf_url}")
            
            # Download and parse the PDF
            import requests
            import io
            import pdfplumber
            
            try:
                self.logger.info(f"⬇️  Downloading PDF from: {pdf_url}")
                response = requests.get(pdf_url, timeout=30)
                response.raise_for_status()
                
                # Check if it's actually a PDF
                content_type = response.headers.get('content-type', '').lower()
                if 'pdf' not in content_type and not pdf_url.lower().endswith('.pdf'):
                    self.logger.warning(f"⚠ Response may not be a PDF (content-type: {content_type}) | Source: {pdf_url}")
                
                # Parse PDF
                pdf_file = io.BytesIO(response.content)
                pdf_text = ""
                
                with pdfplumber.open(pdf_file) as pdf:
                    self.logger.info(f"📄 PDF has {len(pdf.pages)} pages")
                    for page_num, page in enumerate(pdf.pages, 1):
                        page_text = page.extract_text() or ""
                        pdf_text += page_text + " "
                        # Debug first page
                        if page_num == 1:
                            self.logger.info(f"   Page 1 preview (first 300 chars): {page_text[:300]}...")
                
                self.logger.info(f"📄 Extracted PDF text, total length: {len(pdf_text)} chars")
            
            except Exception as pdf_error:
                self.logger.error(f"❌ Error downloading/parsing PDF: {pdf_error}")
                import traceback
                self.logger.error(f"   Traceback: {traceback.format_exc()}")
                return
            
            if not pdf_text or len(pdf_text) < 100:
                self.logger.warning(f"⚠ PDF text too short or empty (length: {len(pdf_text)}) | Source: {pdf_url}")
                return
            
            # Debug: Show sample around phone keywords
            self.logger.info(f"🔍 Searching for keywords in PDF text (length: {len(pdf_text)})...")
            for keyword in ["telephone", "phone", "tel.", "tel:", "+31", "036", "529", "9555", "Oudweg", "Almere"]:
                pos = pdf_text.lower().find(keyword.lower())
                if pos >= 0:
                    sample = pdf_text[max(0, pos - 80):min(len(pdf_text), pos + 200)]
                    self.logger.info(f"   Found '{keyword}' at position {pos}: ...{sample}...")
            
            # Extract legal name
            # Pattern: "RGF Staffing the Netherlands B.V." or similar variations
            legal_name_patterns = [
                r'(RGF Staffing[^,\n]{0,50}?B\.V\.)',  # Main pattern
                r'privacy statement of ([^,\n]+?B\.V\.)',  # From "privacy statement of X"
            ]
            for pattern in legal_name_patterns:
                legal_name_match = re.search(pattern, pdf_text, re.IGNORECASE)
                if legal_name_match:
                    agency.legal_name = legal_name_match.group(1).strip()
                    self.logger.info(f"✓ Found legal name: {agency.legal_name} | Source: {pdf_url}")
                    break
            
            # Extract HQ address
            # Patterns for both English and Dutch formats:
            # English: "head office at P.J. Oudweg 61 in (1314 CK) Almere"
            # Dutch: "hoofdkantoor aan de P.J. Oudweg 61 te (1314 CK) Almere"
            # Dutch: "hebben hun hoofdkantoor aan de P.J. Oudweg 61 te (1314 CK) Almere"
            hq_patterns = [
                # English format
                r'head office at\s+(.+?)\s+in\s+\((\d{4}\s+[A-Z]{2})\)\s+([A-Za-z]+)',
                # Dutch format: "hebben hun hoofdkantoor aan de ... te (postcode) city"
                r'hebben\s+hun\s+hoofdkantoor\s+aan\s+(?:de\s+)?(.+?)\s+te\s+\((\d{4}\s+[A-Z]{2})\)\s+([A-Za-z]+)',
                # Dutch format: "hoofdkantoor aan de ... te (postcode) city"
                r'hoofdkantoor\s+(?:aan de|aan)\s+(.+?)\s+te\s+\((\d{4}\s+[A-Z]{2})\)\s+([A-Za-z]+)',
                # Fallback: direct match for P.J. Oudweg 61 pattern
                r'P\.?J\.?\s*Oudweg\s+61[^,\n]{0,50}?\((\d{4}\s+[A-Z]{2})\)\s+([A-Za-z]+)',
            ]
            for pattern in hq_patterns:
                hq_match = re.search(pattern, pdf_text, re.IGNORECASE)
                if hq_match:
                    # Extract HQ details
                    if len(hq_match.groups()) >= 3:
                        hq_street = hq_match.group(1).strip()
                        hq_zip = hq_match.group(2).strip()
                        hq_city = hq_match.group(3).strip()
                    elif len(hq_match.groups()) >= 2:
                        # Fallback pattern matched (P.J. Oudweg 61)
                        hq_street = "P.J. Oudweg 61"
                        hq_zip = hq_match.group(1).strip()
                        hq_city = hq_match.group(2).strip()
                    else:
                        # Simple pattern matched, use defaults
                        hq_street = "P.J. Oudweg 61"
                        hq_zip = "1314 CK"
                        hq_city = "Almere"
                    
                    # Set hq_city and hq_province on agency
                    agency.hq_city = hq_city
                    agency.hq_province = self.utils.map_city_to_province(hq_city)
                    
                    # Log the full HQ address (street/zip are not stored in the model)
                    self.logger.info(f"✓ Found HQ: {hq_street}, {hq_zip} {hq_city} ({agency.hq_province}) | Source: {pdf_url}")
                    break
            
            # Extract contact phone
            # Multiple patterns to handle different formats
            phone_patterns = [
                # Pattern 1: "telephone number +31 (0)36 529 9555"
                r'telephone\s+number[:\s]+(\+31\s*\(?0?\)?\s*\d+\s+\d+\s+\d+)',
                # Pattern 2: "tel: +31 (0)36 529 9555" or "tel. +31..."
                r'tel\.?\s*:?\s*(\+31\s*\(?0?\)?\s*\d+\s+\d+\s+\d+)',
                # Pattern 3: "+31 (0)36 529 9555" anywhere
                r'(\+31\s*\(0\)\s*\d{1,3}\s+\d{3}\s+\d{4})',
                # Pattern 4: Specific number for Start People
                r'(\+31\s*\(?0?\)?\s*36\s*529\s*9555)',
                # Pattern 5: Just the number with +31
                r'(\+31\s*0?36\s*\d{7})',
            ]
            
            for pattern in phone_patterns:
                phone_match = re.search(pattern, pdf_text, re.IGNORECASE)
                if phone_match:
                    phone = phone_match.group(1).strip()
                    # Normalize phone to digits-only format (with + for international)
                    from staffing_agency_scraper.lib.normalize import normalize_contact_phone
                    normalized_phone = normalize_contact_phone(phone)
                    agency.contact_phone = normalized_phone
                    if normalized_phone != phone:
                        self.logger.info(f"✓ Found contact phone: {phone} -> normalized to: {normalized_phone} | Source: {pdf_url}")
                    else:
                        self.logger.info(f"✓ Found contact phone: {normalized_phone} | Source: {pdf_url}")
                    break
            
            # If still not found, try a more aggressive search
            if not agency.contact_phone:
                # Look for any +31 phone number in the text
                generic_phone_match = re.search(r'\+31[\s\(\)0-9]{10,20}', pdf_text)
                if generic_phone_match:
                    phone = generic_phone_match.group(0).strip()
                    # Normalize phone to digits-only format (with + for international)
                    from staffing_agency_scraper.lib.normalize import normalize_contact_phone
                    normalized_phone = normalize_contact_phone(phone)
                    agency.contact_phone = normalized_phone
                    if normalized_phone != phone:
                        self.logger.info(f"✓ Found contact phone (generic match): {phone} -> normalized to: {normalized_phone} | Source: {pdf_url}")
                    else:
                        self.logger.info(f"✓ Found contact phone (generic match): {normalized_phone} | Source: {pdf_url}")
            
            # Extract KvK number (Chamber of Commerce number)
            # Pattern: "Trade Register number" or "KvK" or "Chamber of Commerce number"
            kvk_patterns = [
                r'(?:Trade Register|KvK|Chamber of Commerce|Handelsregister)[\s\w]*?(?:number|nummer)[:\s]+(\d{8})',
                r'KvK[:\s-]+(\d{8})',
                r'registered (?:in|under) number[:\s]+(\d{8})',
            ]
            for pattern in kvk_patterns:
                kvk_match = re.search(pattern, pdf_text, re.IGNORECASE)
                if kvk_match:
                    agency.kvk_number = kvk_match.group(1).strip()
                    self.logger.info(f"✓ Found KvK number: {agency.kvk_number} | Source: {pdf_url}")
                    break
            
            # Extract parent company
            # Pattern: "We are part of Recruit Holdings Co. Ltd, based in Japan"
            parent_patterns = [
                r'(?:part of|onderdeel van)\s+(Recruit Holdings[^,\n]{0,30}?Ltd)',
                r'(Recruit Holdings Co\.? Ltd)',
            ]
            for pattern in parent_patterns:
                parent_match = re.search(pattern, pdf_text, re.IGNORECASE)
                if parent_match:
                    parent_company = parent_match.group(1).strip()
                    if not agency.growth_signals:
                        agency.growth_signals = []
                    signal = f"onderdeel_van_recruit_holdings"
                    if signal not in agency.growth_signals:
                        agency.growth_signals.append(signal)
                    self.logger.info(f"✓ Found parent company: {parent_company} (Japan-based global group) | Source: {pdf_url}")
                    break
            
            # If nothing was extracted, log a warning
            if not agency.legal_name and not agency.hq_city and not agency.contact_phone:
                self.logger.warning(f"⚠ No legal data extracted from privacy statement | Source: {pdf_url}")
                self.logger.info(f"   First 500 chars of PDF text: {pdf_text[:500]}...")
        
        except Exception as e:
            self.logger.error(f"❌ Error extracting legal info: {e}")
            import traceback
            self.logger.error(f"   Traceback: {traceback.format_exc()}")
    
    def _filter_start_people_evidence_urls(self) -> list[str]:
        """
        Filter evidence URLs for Start People: exclude privacy statement page.
        Keep the PDF URL if it was added, but exclude the HTML privacy statement page.
        """
        from urllib.parse import urlparse
        
        filtered = []
        privacy_statement_url = "https://startpeople.nl/legal/privacy-statement"
        
        for url in self.evidence_urls:
            # Normalize URL for comparison
            parsed = urlparse(url)
            normalized = f"{parsed.scheme}://{parsed.netloc}{parsed.path}".rstrip('/')
            privacy_normalized = urlparse(privacy_statement_url)
            privacy_normalized_str = f"{privacy_normalized.scheme}://{privacy_normalized.netloc}{privacy_normalized.path}".rstrip('/')
            
            # Exclude privacy statement HTML page
            if normalized.lower() == privacy_normalized_str.lower():
                self.logger.info(f"  Excluded privacy statement page: {url}")
                continue
            
            filtered.append(url)
        
        return filtered
    


@dg.asset(group_name="agencies")
def start_people_scrape() -> dg.Output[dict]:
    """Scrape Start People website."""
    scraper = StartPeopleScraper()
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

