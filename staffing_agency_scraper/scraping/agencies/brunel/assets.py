"""
Brunel Netherlands scraper.

Website: https://www.brunel.net/nl-nl
Specializes in: Detachering, interim en recruitment voor IT, Engineering, Legal en Finance
"""

from __future__ import annotations

import re

import dagster as dg
from bs4 import BeautifulSoup

from staffing_agency_scraper.models import Agency, AgencyServices, AICapabilities, CaoType, DigitalCapabilities, GeoFocusType, OfficeLocation
from staffing_agency_scraper.scraping.base import BaseAgencyScraper
from staffing_agency_scraper.scraping.utils import AgencyScraperUtils


class BrunelScraper(BaseAgencyScraper):
    """Scraper for Brunel Netherlands."""

    AGENCY_NAME = "Brunel"
    WEBSITE_URL = "https://www.brunel.net/nl-nl"
    BRAND_GROUP = "Brunel International"

    # Note: Brunel website is heavily JavaScript-rendered
    # Many pages return partial content or 404 without JS
    PAGES_TO_SCRAPE = [
        "https://www.brunel.net/nl-nl",  # Main Dutch page - sectors, certifications
        "https://www.brunel.net/nl-nl/contact",  # Contact page - discover office URLs dynamically
        "https://www.brunel.net/nl-nl/voor-opdrachtgevers",  # For employers
        "https://www.brunel.net/nl-nl/over-ons",  # About
        "https://www.brunel.net/nl-nl/ons-verhaal",  # History - growth signals
        "https://www.brunel.net/nl-nl/myapplications/login",  # Candidate portal login
    ]

    def scrape(self) -> Agency:
        self.logger.info(f"Starting scrape of {self.AGENCY_NAME}")

        # Initialize utils for client improvements
        self.utils = AgencyScraperUtils(logger=self.logger)

        agency = self.create_base_agency()
        agency.geo_focus_type = GeoFocusType.INTERNATIONAL
        agency.employers_page_url = f"{self.WEBSITE_URL}/voor-opdrachtgevers"
        agency.contact_form_url = f"{self.WEBSITE_URL}/contact"

        all_text = ""
        all_sectors = set()
        office_urls = []  # Will be populated from contact page
        has_chatbot = False  # Track if chatbot is found in __NEXT_DATA__

        for url in self.PAGES_TO_SCRAPE:
            try:
                soup = self.fetch_page(url)
                page_text = soup.get_text(separator=" ", strip=True)
                all_text += " " + page_text
                
                # Check for chatbot in __NEXT_DATA__
                if not has_chatbot:
                    has_chatbot = self._check_chatbot_in_next_data(soup, url)

                # Extract logo from main page (use utils for PNG/SVG filtering and banner exclusion)
                if url == self.WEBSITE_URL and not agency.logo_url:
                    logo = self.utils.fetch_logo(soup, url)
                    if logo:
                        agency.logo_url = logo
                        self.logger.info(f"✓ Found logo (PNG/SVG, no banner): {logo}")
                    else:
                        # Fallback to custom method (but this might get banner images)
                        logo = self._extract_logo(soup)
                        if logo:
                            self.logger.warning(f"⚠️ Using fallback logo (might be banner): {logo}")
                            agency.logo_url = logo

                # Extract sectors from homepage cards (also use normalized utils method)
                if url == self.WEBSITE_URL:
                    sectors = self._extract_sectors_from_homepage(soup, url)
                    all_sectors.update(sectors)
                    
                    # Add normalized sectors from utils
                    norm_sectors = self.utils.fetch_sectors(page_text, url)
                    all_sectors.update(norm_sectors)
                    
                    # Also extract HQ from main page if mentioned
                    if "amsterdam" in page_text.lower():
                        agency.hq_city = "Amsterdam"
                        agency.hq_province = "Noord-Holland"
                        self.logger.info(f"✓ Found HQ city: Amsterdam | Source: {url}")
                    
                    # Extract services from main page text
                    main_services = self._extract_services(soup, url)
                    if main_services.detacheren:
                        agency.services.detacheren = True
                    if main_services.werving_selectie:
                        agency.services.werving_selectie = True
                    
                    # Extract certifications from homepage
                    certs = self._extract_certifications_from_homepage(soup, url)
                    if certs:
                        agency.certifications = certs

                # Extract services from diensten page
                if "onze-diensten" in url.lower() or "detachering" in url.lower():
                    services = self._extract_services(soup, url)
                    # Merge services (keep True values)
                    if services.detacheren:
                        agency.services.detacheren = True
                    if services.werving_selectie:
                        agency.services.werving_selectie = True
                    if services.executive_search:
                        agency.services.executive_search = True
                    if services.zzp_bemiddeling:
                        agency.services.zzp_bemiddeling = True
                    if services.opleiden_ontwikkelen:
                        agency.services.opleiden_ontwikkelen = True

                # Extract sectors and services from voor-opdrachtgevers page
                if "voor-opdrachtgevers" in url.lower():
                    sectors = self._extract_sectors_from_opdrachtgevers(soup, url)
                    all_sectors.update(sectors)
                    
                    # Extract services from this page (merge all fields)
                    services = self._extract_services(soup, url)
                    if services.detacheren:
                        agency.services.detacheren = True
                    if services.werving_selectie:
                        agency.services.werving_selectie = True
                    if services.zzp_bemiddeling:
                        agency.services.zzp_bemiddeling = True
                    if services.opleiden_ontwikkelen:
                        agency.services.opleiden_ontwikkelen = True
                    if services.uitzenden:
                        agency.services.uitzenden = True
                    if services.payrolling:
                        agency.services.payrolling = True
                    if services.msp:
                        agency.services.msp = True
                    if services.rpo:
                        agency.services.rpo = True
                    if services.inhouse_services:
                        agency.services.inhouse_services = True
                    if services.executive_search:
                        agency.services.executive_search = True

                # Extract office locations from vestigingen page
                if "vestigingen" in url.lower():
                    locations = self._extract_office_locations(soup, url)
                    if locations:
                        agency.office_locations = locations

                # Extract certifications from text (add to existing if not already covered)
                if "over-ons" in url.lower() or "over-brunel" in url.lower():
                    text_certs = self._extract_certifications(page_text)
                    existing_lower = [c.lower() for c in agency.certifications]
                    for cert in text_certs:
                        # Check if cert or its substring is already in existing certs
                        if not any(cert.lower() in existing for existing in existing_lower):
                            agency.certifications.append(cert)

                # Extract offices from contact page and discover office URLs
                if url == "https://www.brunel.net/nl-nl/contact":
                    offices, discovered_urls = self._extract_offices_from_contact(soup, url)
                    if offices:
                        agency.office_locations = offices
                    # Store discovered office URLs to scrape later
                    office_urls.extend(discovered_urls)
                    
                    # Extract HQ city from head office section
                    hq = self._extract_hq_from_contact(soup, url)
                    if hq.get("city"):
                        agency.hq_city = hq["city"]
                        agency.hq_province = hq.get("province")
                        # The HQ URL will be first in office_urls (Amsterdam)
                
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

                # Extract HQ info from privacy page
                if "privacy" in url.lower():
                    kvk = self._extract_kvk(page_text)
                    if not kvk:
                        # Fallback to utils
                        kvk = self.utils.fetch_kvk_number(page_text, url)
                    if kvk:
                        agency.kvk_number = kvk
                    
                    legal_name = self._extract_legal_name(page_text)
                    if not legal_name:
                        # Fallback to utils
                        legal_name = self.utils.fetch_legal_name(page_text, self.AGENCY_NAME, url)
                    if legal_name:
                        agency.legal_name = legal_name
                    
                    # Extract HQ address and phone from privacy page
                    hq_info = self._extract_hq_from_privacy(soup, url)
                    if hq_info.get("hq_city"):
                        agency.hq_city = hq_info["hq_city"]
                    if hq_info.get("hq_province"):
                        agency.hq_province = hq_info["hq_province"]
                    if hq_info.get("phone") and not agency.contact_phone:
                        agency.contact_phone = hq_info["phone"]

            except Exception as e:
                self.logger.warning(f"Error scraping {url}: {e}")

        # Scrape discovered office URLs (from contact page)
        for office_url in office_urls:
            try:
                self.logger.info(f"Scraping office page: {office_url}")
                soup = self.fetch_page(office_url)
                page_text = soup.get_text(separator=" ", strip=True)
                all_text += " " + page_text
                
                # Extract data from __NEXT_DATA__ (phone, email)
                office_data = self._extract_from_next_data(soup, office_url)
                
                # Use HQ (first/Amsterdam) data for main contact
                if office_data.get("phone") and not agency.contact_phone:
                    agency.contact_phone = office_data["phone"]
                if office_data.get("email") and not agency.contact_email:
                    agency.contact_email = office_data["email"]
                    
            except Exception as e:
                self.logger.warning(f"Error scraping office {office_url}: {e}")

        # Set sectors_core from combined set
        if all_sectors:
            agency.sectors_core = sorted(list(all_sectors))
            self.logger.info(f"✓ Total unique sectors: {len(agency.sectors_core)}")

        # Derive CAO type and membership from certifications
        if agency.certifications:
            if "ABU" in agency.certifications:
                agency.cao_type = CaoType.ABU
                agency.membership = ["ABU"]
            elif "NBBU" in agency.certifications:
                agency.cao_type = CaoType.NBBU
                agency.membership = ["NBBU"]

        # Derive regions_served from office locations
        if agency.office_locations and len(agency.office_locations) >= 3:
            agency.regions_served = ["landelijk"]
        
        # Check for international presence in text
        if "internationaal" in all_text.lower() or "worldwide" in all_text.lower():
            if "landelijk" not in agency.regions_served:
                agency.regions_served.append("landelijk")
            agency.regions_served.append("internationaal")

        # Extract focus segments from text
        agency.focus_segments = self._extract_focus_segments(all_text)

        # Extract digital capabilities from text (preserve portal values set by utils)
        digital_caps = self._extract_digital_capabilities(all_text)
        # Only update mobile_app (portals already set by utils.detect_*_portal)
        agency.digital_capabilities.mobile_app = digital_caps.mobile_app

        # Extract AI capabilities from __NEXT_DATA__ and text
        agency.ai_capabilities = self._extract_ai_capabilities(all_text, has_chatbot)
        
        # Extract growth signals from accumulated text
        agency.growth_signals = self.utils.fetch_growth_signals(all_text, "accumulated_text")
        
        # Extract additional market positioning fields from aggregated text
        if not agency.company_size_fit:
            agency.company_size_fit = self.utils.fetch_company_size_fit(all_text, "accumulated_text")
        
        if not agency.customer_segments:
            agency.customer_segments = self.utils.fetch_customer_segments(all_text, "accumulated_text")
        
        if not agency.shift_types_supported:
            agency.shift_types_supported = self.utils.fetch_shift_types_supported(all_text, "accumulated_text")
        
        if not agency.typical_use_cases:
            agency.typical_use_cases = self.utils.fetch_typical_use_cases(all_text, "accumulated_text")
        
        if not agency.speed_claims:
            agency.speed_claims = self.utils.fetch_speed_claims(all_text, "accumulated_text")
        
        # Infer volume specialisation if not already set
        if agency.volume_specialisation == "unknown":
            agency.volume_specialisation = self.utils.fetch_volume_specialisation(all_text, "accumulated_text")
        
        # Extract pricing & commercial fields
        if agency.pricing_model == "unknown":
            agency.pricing_model = self.utils.fetch_pricing_model(all_text, "accumulated_text")
        
        if not agency.pricing_transparency:
            agency.pricing_transparency = self.utils.fetch_pricing_transparency(all_text, "accumulated_text")
        
        if agency.no_cure_no_pay is None:
            agency.no_cure_no_pay = self.utils.fetch_no_cure_no_pay(all_text, "accumulated_text")
        
        if not agency.omrekenfactor_min and not agency.omrekenfactor_max:
            omrekenfactor_min, omrekenfactor_max = self.utils.fetch_omrekenfactor(all_text, "accumulated_text")
            if omrekenfactor_min:
                agency.omrekenfactor_min = omrekenfactor_min
            if omrekenfactor_max:
                agency.omrekenfactor_max = omrekenfactor_max
        
        # Extract performance metrics
        if not agency.avg_time_to_fill_days:
            agency.avg_time_to_fill_days = self.utils.fetch_avg_time_to_fill(all_text, "accumulated_text")
        
        if not agency.candidate_pool_size_estimate:
            agency.candidate_pool_size_estimate = self.utils.fetch_candidate_pool_size(all_text, "accumulated_text")
        
        if not agency.annual_placements_estimate:
            agency.annual_placements_estimate = self.utils.fetch_annual_placements(all_text, "accumulated_text")
        
        # Extract legal/compliance fields
        if agency.uses_inlenersbeloning is None:
            agency.uses_inlenersbeloning = self.utils.fetch_uses_inlenersbeloning(all_text, "accumulated_text")
        
        if agency.applies_inlenersbeloning_from_day1 is None:
            agency.applies_inlenersbeloning_from_day1 = self.utils.fetch_applies_inlenersbeloning_from_day1(all_text, "accumulated_text")
        
        # Extract assignment conditions
        if not agency.min_assignment_duration_weeks:
            agency.min_assignment_duration_weeks = self.utils.fetch_min_assignment_duration(all_text, "accumulated_text")
        
        if not agency.min_hours_per_week:
            agency.min_hours_per_week = self.utils.fetch_min_hours_per_week(all_text, "accumulated_text")

        # Update evidence URLs
        agency.evidence_urls = self.evidence_urls.copy()
        agency.collected_at = self.collected_at

        # Log extraction summary
        self.logger.info(f"--- Extraction Summary for {self.AGENCY_NAME} ---")
        self.logger.info(f"  Legal Name: {agency.legal_name or 'NOT FOUND'}")
        self.logger.info(f"  KvK: {agency.kvk_number or 'NOT FOUND'}")
        self.logger.info(f"  Phone: {agency.contact_phone or 'NOT FOUND'}")
        self.logger.info(f"  Email: {agency.contact_email or 'NOT FOUND'}")
        self.logger.info(f"  Logo: {'YES' if agency.logo_url else 'NOT FOUND'}")
        self.logger.info(f"  Offices: {len(agency.office_locations or [])} found")
        self.logger.info(f"  Sectors: {len(agency.sectors_core or [])} found")
        self.logger.info(f"  Certifications: {len(agency.certifications or [])} found")

        self.logger.info(f"Completed scrape of {self.AGENCY_NAME}")
        return agency

    def _extract_logo(self, soup: BeautifulSoup) -> str | None:
        """
        Extract logo from __NEXT_DATA__ JSON or header.
        
        Brunel stores the logo in __NEXT_DATA__ JSON:
        "navigationLogo": {
            "image": {
                "value": {
                    "src": "https://edge.sitecorecloud.io/.../imagery-logo-light-brunel.png"
                }
            }
        }
        """
        import json
        
        # First try to extract from __NEXT_DATA__
        next_data_script = soup.find("script", id="__NEXT_DATA__")
        if next_data_script and next_data_script.string:
            try:
                # Use regex to find navigationLogo image src (faster than parsing full JSON)
                script_text = next_data_script.string
                
                # Look for navigationLogo pattern
                logo_match = re.search(r'"navigationLogo"\s*:\s*\{[^}]*?"image"\s*:\s*\{[^}]*?"value"\s*:\s*\{[^}]*?"src"\s*:\s*"([^"]+)"', script_text, re.DOTALL)
                if logo_match:
                    src = logo_match.group(1)
                    # Decode JSON escape sequences (e.g., \u0026 → &)
                    # The URL is inside a JSON string, so we need to decode it
                    try:
                        # Use json.loads to properly decode the string
                        src = json.loads(f'"{src}"')
                    except:
                        # If decoding fails, use the raw string
                        pass
                    
                    # Filter out non-logo images (search, icons, etc.)
                    if "logo" in src.lower() and "search" not in src.lower():
                        self.logger.info(f"✓ Found logo from __NEXT_DATA__: {src}")
                        return src
                
            except Exception as e:
                self.logger.warning(f"Error parsing __NEXT_DATA__ for logo: {e}")
        
        # Fallback: Try SVG mask logo
        span_logo = soup.find("span", role="img", attrs={"style": lambda s: s and "mask:url(" in s if s else False})
        if span_logo:
            style = span_logo.get("style", "")
            mask_match = re.search(r'mask:url\(([^)]+)\)', style)
            if mask_match:
                src = mask_match.group(1)
                # Filter out search.svg and other non-logo files
                if "logo" in src.lower() and "search" not in src.lower():
                    if not src.startswith("http"):
                        src = f"https://www.brunel.net{src}"
                    self.logger.info(f"✓ Found logo (SVG mask): {src}")
                    return src
        
        # Fallback: Traditional img elements
        logo = soup.select_one("header img, .site-header img, [class*='logo'] img, a[aria-label*='logo'] img")
        if logo:
            src = logo.get("src") or logo.get("data-src")
            if src and not src.startswith("data:"):
                # Filter out search icons and other non-logo images
                if "search" not in src.lower():
                    if not src.startswith("http"):
                        src = f"https://www.brunel.net{src}"
                    self.logger.info(f"✓ Found logo: {src}")
                    return src
        
        return None

    def _extract_sectors_from_opdrachtgevers(self, soup: BeautifulSoup, url: str) -> list[str]:
        """
        Extract sectors (Branches) from the voor-opdrachtgevers page.
        
        The page lists branches like:
        - Bouw & Infra
        - Energie
        - Financiële Dienstverlening
        - High Tech
        - Industrie
        - Life Science
        - Non-profit
        - IT & Telecom
        """
        sectors = []
        text = soup.get_text(separator="\n", strip=True)
        
        # Look for branch/sector keywords in the text
        sector_keywords = [
            ("bouw", "bouw_infra"),
            ("infra", "bouw_infra"),
            ("energie", "energie"),
            ("energy", "energie"),
            ("financieel", "finance"),
            ("financial", "finance"),
            ("high tech", "high_tech"),
            ("hightech", "high_tech"),
            ("industrie", "industrie"),
            ("industry", "industrie"),
            ("life science", "life_science"),
            ("lifescience", "life_science"),
            ("non-profit", "non_profit"),
            ("nonprofit", "non_profit"),
            ("it & telecom", "it_telecom"),
            ("it en telecom", "it_telecom"),
            ("telecom", "it_telecom"),
        ]
        
        text_lower = text.lower()
        seen = set()
        
        for keyword, sector in sector_keywords:
            if keyword in text_lower and sector not in seen:
                sectors.append(sector)
                seen.add(sector)
                self.logger.info(f"✓ Found sector '{sector}' (from keyword '{keyword}') | Source: {url}")
        
        return sectors

    def _extract_sectors_from_homepage(self, soup: BeautifulSoup, url: str) -> list[str]:
        """
        Extract sectors from the homepage cards container.
        
        HTML structure:
        <div id="cards-container">
            <a href="/nl-nl/carriere/engineering">
                <p class="text-m-body-medium...">Engineering</p>
            </a>
            ...
        </div>
        """
        sectors = []
        seen = set()

        # Find the cards container
        cards_container = soup.find("div", id="cards-container")
        if not cards_container:
            # Fallback: look for career links
            cards_container = soup

        # Find all career/expertise links
        career_links = cards_container.find_all("a", href=lambda h: h and ("/carriere/" in h or "/communities/" in h))

        for link in career_links:
            # Extract sector name from <p> element inside the link
            p = link.find("p")
            if p:
                sector = p.get_text(strip=True).lower()
                if sector and sector not in seen:
                    sectors.append(sector)
                    seen.add(sector)
                    self.logger.info(f"✓ Found sector '{sector}' | Source: {url}")

        return sectors

    def _extract_services(self, soup: BeautifulSoup, url: str) -> AgencyServices:
        """
        Extract services from page text.
        
        Mapping from page content to service fields:
        - "detachering", "detacheren" → detacheren
        - "werving", "selectie", "recruitment" → werving_selectie
        - "interim" → zzp_bemiddeling
        - "uitzenden", "uitzendwerk" → uitzenden
        - "payroll" → payrolling
        - "outsourcing" → msp
        - "consultancy", "advies" → rpo (when related to recruitment process)
        - "executive search" → executive_search
        - "traineeship", "opleiding", "training" → opleiden_ontwikkelen
        - "inhouse" → inhouse_services
        """
        text_lower = soup.get_text(separator=" ", strip=True).lower()

        detacheren = False
        werving_selectie = False
        executive_search = False
        zzp_bemiddeling = False
        opleiden_ontwikkelen = False
        uitzenden = False
        payrolling = False
        msp = False
        rpo = False
        inhouse_services = False

        # Detachering
        if "detachering" in text_lower or "detacheren" in text_lower:
            detacheren = True
            self.logger.info(f"✓ Found service 'detacheren' | Source: {url}")

        # Werving & Selectie / Recruitment
        if "werving" in text_lower and "selectie" in text_lower:
            werving_selectie = True
            self.logger.info(f"✓ Found service 'werving_selectie' | Source: {url}")

        if "recruitment" in text_lower:
            werving_selectie = True
            self.logger.info(f"✓ Found service 'werving_selectie' (recruitment) | Source: {url}")

        # Interim / ZZP bemiddeling
        if "interim" in text_lower:
            zzp_bemiddeling = True
            self.logger.info(f"✓ Found service 'zzp_bemiddeling' (interim) | Source: {url}")

        # Uitzenden
        if "uitzenden" in text_lower or "uitzendwerk" in text_lower:
            uitzenden = True
            self.logger.info(f"✓ Found service 'uitzenden' | Source: {url}")

        # Payrolling
        if "payroll" in text_lower:
            payrolling = True
            self.logger.info(f"✓ Found service 'payrolling' | Source: {url}")

        # Outsourcing / MSP
        if "outsourcing" in text_lower:
            msp = True
            self.logger.info(f"✓ Found service 'msp' (outsourcing) | Source: {url}")

        # Consultancy
        if "consultancy" in text_lower or "adviestraject" in text_lower:
            rpo = True
            self.logger.info(f"✓ Found service 'rpo' (consultancy/advies) | Source: {url}")

        # Executive Search
        if "executive search" in text_lower:
            executive_search = True
            self.logger.info(f"✓ Found service 'executive_search' | Source: {url}")

        # Opleiden / Training
        if "traineeship" in text_lower or "opleiding" in text_lower or "training" in text_lower:
            opleiden_ontwikkelen = True
            self.logger.info(f"✓ Found service 'opleiden_ontwikkelen' | Source: {url}")

        # Inhouse services
        if "inhouse" in text_lower or "in-house" in text_lower:
            inhouse_services = True
            self.logger.info(f"✓ Found service 'inhouse_services' | Source: {url}")

        return AgencyServices(
            detacheren=detacheren,
            werving_selectie=werving_selectie,
            executive_search=executive_search,
            zzp_bemiddeling=zzp_bemiddeling,
            opleiden_ontwikkelen=opleiden_ontwikkelen,
            uitzenden=uitzenden,
            payrolling=payrolling,
            msp=msp,
            rpo=rpo,
            inhouse_services=inhouse_services,
        )

    def _extract_offices_from_contact(self, soup: BeautifulSoup, url: str) -> tuple[list[OfficeLocation], list[str]]:
        """
        Extract office locations and office URLs from contact page.
        
        HTML structure:
        <a href="/nl-nl/contact/delft">
            <p class="...">Delft</p>
            <p class="...">Molengraaffsingel 8, 2629 JD</p>
        </a>
        
        Returns:
            (locations, office_urls) - list of OfficeLocation and list of full URLs to scrape
        """
        from staffing_agency_scraper.lib.dutch import DUTCH_POSTAL_TO_PROVINCE
        
        locations = []
        office_urls = []
        seen = set()
        
        # Find all office card links
        office_links = soup.find_all("a", href=lambda h: h and "/nl-nl/contact/" in h and h != "/nl-nl/contact")
        
        for link in office_links:
            href = link.get("href", "")
            
            # Get city from first <p> element
            p_elements = link.find_all("p")
            if len(p_elements) >= 1:
                city = p_elements[0].get_text(strip=True)
                
                # Get province from postal code if available
                province = None
                if len(p_elements) >= 2:
                    address = p_elements[1].get_text(strip=True)
                    # Extract postal code (e.g., "1066 EP" from "John M. Keynesplein 33, 1066 EP")
                    postal_match = re.search(r"(\d{4})\s*([A-Z]{2})", address)
                    if postal_match:
                        postal_prefix = postal_match.group(1)[:2]
                        province = DUTCH_POSTAL_TO_PROVINCE.get(postal_prefix)
                
                if city and city.lower() not in seen:
                    locations.append(OfficeLocation(city=city, province=province))
                    seen.add(city.lower())
                    self.logger.info(f"✓ Found office: {city}, {province} | Source: {url}")
                    
                    # Build full URL and add to list (will be added to evidence_urls when scraped)
                    if href and not href.startswith("http"):
                        full_url = f"https://www.brunel.net{href}"
                    else:
                        full_url = href
                    
                    if full_url not in office_urls:
                        office_urls.append(full_url)
                        self.logger.info(f"✓ Discovered office URL: {full_url}")
        
        return locations, office_urls

    def _extract_hq_from_contact(self, soup: BeautifulSoup, url: str) -> dict:
        """
        Extract HQ info from contact page head office section.
        
        Look for "Head office" / "Hoofdkantoor" section.
        """
        from staffing_agency_scraper.lib.dutch import DUTCH_POSTAL_TO_PROVINCE
        
        result = {}
        
        # Find head office section by looking for h2 containing "Head office" or "Hoofdkantoor"
        h2_elements = soup.find_all("h2")
        for h2 in h2_elements:
            text = h2.get_text(strip=True).lower()
            if "head office" in text or "hoofdkantoor" in text:
                # Find the associated office card link after this h2
                parent = h2.find_parent("div", class_=lambda c: c and "component" in c)
                if parent:
                    office_link = parent.find("a", href=lambda h: h and "/contact/" in h)
                    if office_link:
                        p_elements = office_link.find_all("p")
                        if len(p_elements) >= 1:
                            result["city"] = p_elements[0].get_text(strip=True)
                            self.logger.info(f"✓ Found HQ city: {result['city']} | Source: {url}")
                            
                            # Try to get province from postal code
                            if len(p_elements) >= 2:
                                address = p_elements[1].get_text(strip=True)
                                postal_match = re.search(r"(\d{4})\s*([A-Z]{2})", address)
                                if postal_match:
                                    postal_prefix = postal_match.group(1)[:2]
                                    result["province"] = DUTCH_POSTAL_TO_PROVINCE.get(postal_prefix)
                        break
        
        return result

    def _extract_from_next_data(self, soup: BeautifulSoup, url: str) -> dict:
        """
        Extract data from __NEXT_DATA__ script tag.
        
        Contains structured data like:
        - Phone: "+31 20 312 50 00"
        - Email: "info@brunel.net"
        - City, Address, PostalCode, etc.
        """
        import json
        
        result = {}
        
        # Find __NEXT_DATA__ script tag
        next_data_script = soup.find("script", id="__NEXT_DATA__")
        if not next_data_script:
            return result
        
        try:
            # Try to extract phone directly using regex (faster than parsing full JSON)
            script_text = next_data_script.string or ""
            
            # Extract phone
            phone_match = re.search(r'"Phone"\s*:\s*\{\s*"value"\s*:\s*"([^"]+)"', script_text)
            if phone_match:
                result["phone"] = phone_match.group(1)
                self.logger.info(f"✓ Found phone from __NEXT_DATA__: {result['phone']} | Source: {url}")
            
            # Extract email (but we might not want the privacy email)
            email_match = re.search(r'"Email"\s*:\s*\{\s*"value"\s*:\s*"([^"]+)"', script_text)
            if email_match:
                email = email_match.group(1)
                # Only use general contact email, not privacy email
                if "privacy" not in email.lower():
                    result["email"] = email
                    self.logger.info(f"✓ Found email from __NEXT_DATA__: {result['email']} | Source: {url}")
            
        except Exception as e:
            self.logger.warning(f"Error parsing __NEXT_DATA__: {e}")
        
        return result

    def _extract_office_locations(self, soup: BeautifulSoup, url: str) -> list[OfficeLocation]:
        """Extract office locations from vestigingen page."""
        locations = []
        seen = set()

        # Look for location cards or links
        location_links = soup.find_all("a", href=lambda h: h and "/vestigingen/" in h)

        for link in location_links:
            # Try to get city name from text
            text = link.get_text(strip=True)
            if text and len(text) < 50:  # Avoid long descriptions
                city = text.split(",")[0].strip()
                city = city.replace("Brunel", "").strip()
                if city and city.lower() not in seen and len(city) > 2:
                    locations.append(OfficeLocation(city=city))
                    seen.add(city.lower())
                    self.logger.info(f"✓ Found office: {city} | Source: {url}")

        return locations

    def _extract_certifications_from_homepage(self, soup: BeautifulSoup, url: str) -> list[str]:
        """
        Extract certifications from homepage using BS4.
        
        HTML structure:
        <div class="... lg:py-12">
            <h2>Certificaten</h2>
            <div id="cards-container">
                <a href="...">
                    <p>MVO prestatieladder</p>
                </a>
                <a href="...">
                    <p>Ecovadis Gold medal</p>
                </a>
                ...
            </div>
        </div>
        """
        certs = []
        seen = set()
        
        # Find the "Certificaten" heading
        cert_heading = soup.find("h2", string=re.compile(r"Certificaten", re.IGNORECASE))
        
        if cert_heading:
            # Find the parent container that holds both heading and cards
            parent_container = cert_heading.find_parent("div", class_=lambda c: c and "lg:py-12" in c if c else False)
            
            if parent_container:
                # Find the cards-container within this specific parent
                cards_container = parent_container.find("div", id="cards-container")
                
                if cards_container:
                    # Find all certification cards (a elements)
                    cert_cards = cards_container.find_all("a")
                    
                    for card in cert_cards:
                        # Get the certification name from the <p> element
                        cert_p = card.find("p")
                        
                        if cert_p:
                            cert_name = cert_p.get_text(strip=True)
                            if cert_name and cert_name.lower() not in seen:
                                certs.append(cert_name)
                                seen.add(cert_name.lower())
                                self.logger.info(f"✓ Found certification: '{cert_name}' | Source: {url}")
        
        return certs

    def _extract_certifications(self, text: str) -> list[str]:
        """Extract certifications from text."""
        certs = []
        text_upper = text.upper()

        if "ABU" in text_upper:
            certs.append("ABU")
            self.logger.info("✓ Found certification: ABU")

        if "MVO" in text_upper or "MAATSCHAPPELIJK VERANTWOORD" in text_upper:
            certs.append("MVO")
            self.logger.info("✓ Found certification: MVO")

        if "VCU" in text_upper:
            certs.append("VCU")
            self.logger.info("✓ Found certification: VCU")

        if "ECOVADIS" in text_upper:
            certs.append("EcoVadis")
            self.logger.info("✓ Found certification: EcoVadis")

        if "ISO" in text_upper:
            if "9001" in text:
                certs.append("ISO 9001")
                self.logger.info("✓ Found certification: ISO 9001")

        return certs

    def _extract_phone(self, text: str) -> str | None:
        """Extract phone number from text."""
        # Dutch phone patterns
        phone_match = re.search(r"(?:\+31|0)[- ]?\d{2,3}[- ]?\d{3}[- ]?\d{2}[- ]?\d{2}", text)
        if phone_match:
            phone = phone_match.group(0)
            self.logger.info(f"✓ Found phone: {phone}")
            return phone
        return None

    def _extract_email(self, text: str) -> str | None:
        """Extract email from text."""
        email_match = re.search(r"[a-zA-Z0-9._%+-]+@brunel\.[a-z]{2,}", text, re.IGNORECASE)
        if email_match:
            email = email_match.group(0)
            self.logger.info(f"✓ Found email: {email}")
            return email
        return None

    def _extract_kvk(self, text: str) -> str | None:
        """Extract KvK number from text."""
        kvk_match = re.search(r"KvK[:\s]*(\d{8})", text, re.IGNORECASE)
        if kvk_match:
            kvk = kvk_match.group(1)
            self.logger.info(f"✓ Found KvK: {kvk}")
            return kvk
        return None

    def _extract_legal_name(self, text: str) -> str | None:
        """Extract legal name from text."""
        # Pattern for N.V. or B.V. names
        match = re.search(r"(Brunel[A-Za-z\s\-]+(?:N\.V\.|B\.V\.))", text, re.IGNORECASE)
        if match:
            legal_name = match.group(1).strip()
            self.logger.info(f"✓ Found legal name: {legal_name}")
            return legal_name
        return None

    def _extract_hq_from_privacy(self, soup: BeautifulSoup, url: str) -> dict:
        """
        Extract HQ address and phone from privacy page.
        
        HTML structure:
        <p>...Brunel's head office in Amsterdam...
        John M. Keynesplein 33
        1066 EP Amsterdam
        The Netherlands
        +31 20 312 50 00...</p>
        
        Note: We don't extract the privacy email (privacy@brunel.net)
        """
        result = {}
        page_text = soup.get_text(separator="\n", strip=True)
        
        # Look for Amsterdam HQ address pattern
        # Pattern: postal code (1066 EP) followed by Amsterdam
        postal_match = re.search(r"(\d{4})\s*([A-Z]{2})\s+Amsterdam", page_text)
        if postal_match:
            result["hq_city"] = "Amsterdam"
            result["hq_province"] = "Noord-Holland"
            self.logger.info(f"✓ Found HQ city: Amsterdam | Source: {url}")
        
        # Extract phone number (but not the privacy email)
        phone_match = re.search(r"\+31\s*\d{2}\s*\d{3}\s*\d{2}\s*\d{2}", page_text)
        if phone_match:
            phone = phone_match.group(0)
            result["phone"] = phone
            self.logger.info(f"✓ Found HQ phone: {phone} | Source: {url}")
        
        return result

    def _extract_focus_segments(self, text: str) -> list[str]:
        """Extract focus segments from text."""
        segments = []
        text_lower = text.lower()

        if "specialist" in text_lower or "professional" in text_lower:
            segments.append("specialisten")
        if "technisch" in text_lower or "engineer" in text_lower:
            segments.append("technisch_specialisten")
        if "senior" in text_lower:
            segments.append("senior")
        if "medior" in text_lower:
            segments.append("medior")
        if "trainee" in text_lower or "starter" in text_lower:
            segments.append("starters")

        return list(set(segments))

    def _extract_digital_capabilities(self, text: str) -> DigitalCapabilities:
        """Extract digital capabilities from text."""
        text_lower = text.lower()

        candidate_portal = False
        client_portal = False
        mobile_app = False

        if "mijn brunel" in text_lower or "portal" in text_lower or "inloggen" in text_lower:
            candidate_portal = True
            self.logger.info("✓ Found digital capability: candidate_portal")

        if "werkgeversportaal" in text_lower or "client portal" in text_lower:
            client_portal = True
            self.logger.info("✓ Found digital capability: client_portal")

        if "app store" in text_lower or "google play" in text_lower or "mobiele app" in text_lower:
            mobile_app = True
            self.logger.info("✓ Found digital capability: mobile_app")

        return DigitalCapabilities(
            candidate_portal=candidate_portal,
            client_portal=client_portal,
            mobile_app=mobile_app,
        )

    def _check_chatbot_in_next_data(self, soup: BeautifulSoup, url: str) -> bool:
        """
        Check if chatbot component exists in __NEXT_DATA__.
        
        Looks for patterns like:
        - "componentName": "Chatbot"
        - "chatbotUrl": "..."
        - "chatbotWidgetId": "..."
        """
        script_tag = soup.find("script", id="__NEXT_DATA__")
        if script_tag and script_tag.string:
            json_str = script_tag.string
            
            # Use regex to find chatbot-related patterns
            chatbot_patterns = [
                r'"componentName"\s*:\s*"Chatbot"',
                r'"chatbotUrl"\s*:\s*"[^"]+"',
                r'"chatbotWidgetId"\s*:\s*"[^"]+"',
                r'"chatbotSettingsId"\s*:\s*"[^"]+"',
            ]
            
            for pattern in chatbot_patterns:
                if re.search(pattern, json_str, re.IGNORECASE):
                    self.logger.info(f"✓ Found chatbot in __NEXT_DATA__ | Source: {url}")
                    return True
        
        return False

    def _extract_ai_capabilities(self, text: str, has_chatbot: bool = False) -> AICapabilities:
        """
        Extract AI capabilities from text and __NEXT_DATA__.
        
        Checks for:
        - chatbot_for_candidates: Chatbot component in __NEXT_DATA__ (available on main page)
        - chatbot_for_clients: Same chatbot serves both candidates and clients
        """
        text_lower = text.lower()
        
        # Chatbot is on main page - available for both candidates and clients
        chatbot_for_candidates = has_chatbot
        chatbot_for_clients = has_chatbot  # Same chatbot serves everyone
        internal_ai_matching = False
        ai_screening = False
        
        # Check for AI matching keywords
        ai_keywords = ["ai matching", "ai-matching", "machine learning", "kunstmatige intelligentie", "artificial intelligence"]
        for keyword in ai_keywords:
            if keyword in text_lower:
                internal_ai_matching = True
                self.logger.info(f"✓ Found AI capability: internal_ai_matching (keyword: '{keyword}')")
                break
        
        # Check for AI screening
        if "ai screening" in text_lower or "automatische screening" in text_lower:
            ai_screening = True
            self.logger.info("✓ Found AI capability: ai_screening")
        
        if chatbot_for_candidates:
            self.logger.info("✓ Found AI capability: chatbot_for_candidates")
        if chatbot_for_clients:
            self.logger.info("✓ Found AI capability: chatbot_for_clients")
        
        return AICapabilities(
            chatbot_for_candidates=chatbot_for_candidates,
            chatbot_for_clients=chatbot_for_clients,
            internal_ai_matching=internal_ai_matching,
            ai_screening=ai_screening,
        )


@dg.asset(group_name="agencies")
def brunel_scrape() -> dg.Output[dict]:
    """Scrape Brunel Netherlands website."""
    scraper = BrunelScraper()
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