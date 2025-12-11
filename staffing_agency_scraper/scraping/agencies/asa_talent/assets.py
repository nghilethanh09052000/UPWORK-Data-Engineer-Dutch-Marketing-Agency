"""
ASA Talent Netherlands scraper.

Website: https://asatalent.nl
Part of: RGF Staffing

ASA Talent is "hét uitzendbureau voor studenten, starters en professionals".
"""

from __future__ import annotations

import io
import re

import requests
import dagster as dg
from bs4 import BeautifulSoup

from staffing_agency_scraper.lib.fetch import get_chrome_user_agent, fetch_with_retry
from staffing_agency_scraper.lib.dutch import DUTCH_POSTAL_TO_PROVINCE
from staffing_agency_scraper.models import Agency, AgencyServices, CaoType, DigitalCapabilities, GeoFocusType, OfficeLocation
from staffing_agency_scraper.scraping.base import BaseAgencyScraper
from staffing_agency_scraper.scraping.utils import AgencyScraperUtils


class ASATalentScraper(BaseAgencyScraper):
    """Scraper for ASA Talent Netherlands."""

    AGENCY_NAME = "ASA Talent"
    WEBSITE_URL = "https://asatalent.nl"
    BRAND_GROUP = "RGF Staffing"

    PAGES_TO_SCRAPE = [
        "https://asatalent.nl",
        "https://asatalent.nl/werkgevers",
        "https://asatalent.nl/over-ons",
        "https://asatalent.nl/contact",
        "https://asatalent.nl/werkgevers/diensten",  # Services
        "https://asatalent.nl/werkgevers/expertises",  # Sectors/expertises for sectors_core
        "https://asatalent.nl/vacatures/vakgebieden",  # Additional sectors from vakgebieden
        "https://asatalent.nl/vestigingen",  # Office locations
        "https://asatalent.nl/kenniscentrum",  # For contact email (info@asatalent.nl)
        "https://asatalent.nl/over-ons/keurmerken-en-certificaten",
        "https://asatalent.nl/legal/privacy-statement",
        "https://asatalent.nl/legal/disclaimer",
    ]
    
    # Privacy statement page contains link to RGF Staffing PDF
    PRIVACY_STATEMENT_PAGE = "https://asatalent.nl/legal/privacy-statement"
    RGF_PRIVACY_PDF_URL = "https://rgfstaffing.nl/privacy-statement"  # Redirects to PDF
    

    def scrape(self) -> Agency:
        self.logger.info(f"Starting scrape of {self.AGENCY_NAME}")

        # Note: self.utils is now initialized in BaseAgencyScraper.__init__()
        agency = self.create_base_agency()
        agency.geo_focus_type = GeoFocusType.NATIONAL
        agency.employers_page_url = f"{self.WEBSITE_URL}/werkgevers"
        agency.contact_form_url = f"{self.WEBSITE_URL}/contact"

        all_text = ""
        all_sectors = set()  # Use set to avoid duplicates from multiple pages
        
        for url in self.PAGES_TO_SCRAPE:
            try:
                soup = self.fetch_page(url)
                page_text = soup.get_text(separator=" ", strip=True)
                all_text += " " + page_text

                # Extract logo from main page (use utils for PNG/SVG filtering)
                if url == self.WEBSITE_URL and not agency.logo_url:
                    logo = self.utils.fetch_logo(soup, url)
                    if logo:
                        agency.logo_url = logo
                    else:
                        # Fallback to custom method
                        agency.logo_url = self._extract_logo(soup)

                # Extract office locations from vestigingen page
                if url == "https://asatalent.nl/vestigingen":
                    locations, _, _ = self._extract_office_locations_from_html(soup)
                    if locations:
                        agency.office_locations = locations

                # Extract certifications from keurmerken page
                if "keurmerken" in url.lower():
                    agency.certifications = self._extract_certifications(page_text)
                
                # Extract legal name and KvK from disclaimer/privacy/legal pages
                if "disclaimer" in url.lower() or "privacy" in url.lower() or "legal" in url.lower():
                    if not agency.legal_name:
                        legal_name = self._extract_legal_name(page_text)
                        if legal_name:
                            agency.legal_name = legal_name
                        else:
                            # Fallback to utils
                            legal_name = self.utils.fetch_legal_name(page_text, self.AGENCY_NAME, url)
                            if legal_name:
                                agency.legal_name = legal_name
                    
                    # Try to extract KvK using utils
                    if not agency.kvk_number:
                        kvk = self.utils.fetch_kvk_number(page_text, url)
                        if kvk:
                            agency.kvk_number = kvk
                
                # Extract contact email from kenniscentrum page
                if "kenniscentrum" in url.lower():
                    email = self._extract_email_from_kenniscentrum(soup, url)
                    if email and not agency.contact_email:
                        agency.contact_email = email
                
                # Extract sectors from expertises page (use normalized utils method)
                if "expertises" in url.lower():
                    sectors = self.utils.fetch_sectors(page_text, url)
                    all_sectors.update(sectors)
                
                # Extract sectors from vakgebieden page (use normalized utils method)
                if "vakgebieden" in url.lower():
                    sectors = self.utils.fetch_sectors(page_text, url)
                    all_sectors.update(sectors)
                
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
                
                # Extract services from diensten page
                if url == "https://asatalent.nl/werkgevers/diensten":
                    services = self._extract_services_from_diensten(soup, url)
                    if services:
                        agency.services = services

            except Exception as e:
                self.logger.warning(f"Error scraping {url}: {e}")
        
        # Set sectors_core from combined set (removes duplicates)
        if all_sectors:
            agency.sectors_core = sorted(list(all_sectors))
            self.logger.info(f"✓ Total unique sectors: {len(agency.sectors_core)}")
        
        # Try to extract HQ info from RGF Staffing privacy PDF
        try:
            pdf_data = self._extract_from_rgf_privacy_pdf()
            if pdf_data:
                # RGF Staffing HQ is in Almere - this is the parent company HQ
                if pdf_data.get("hq_city"):
                    agency.hq_city = pdf_data["hq_city"]
                    agency.hq_province = pdf_data.get("hq_province", "Flevoland")
                    self.logger.info(f"✓ Found HQ from RGF PDF: {agency.hq_city}, {agency.hq_province}")
                # Use HQ phone from PDF as main contact phone
                if pdf_data.get("hq_phone"):
                    agency.contact_phone = pdf_data["hq_phone"]
                    self.logger.info(f"✓ Found HQ phone from PDF: {agency.contact_phone}")
                if pdf_data.get("hq_address"):
                    # Store full address in notes
                    agency.notes = f"HQ Address: {pdf_data['hq_address']}"
        except Exception as e:
            self.logger.warning(f"Error extracting from RGF PDF: {e}")
        
        # Fallback to Utrecht if HQ not found
        if not agency.hq_city:
            agency.hq_city = "Utrecht"
            agency.hq_province = "Utrecht"
        
        # Extract focus segments (students, starters, professionals)
        agency.focus_segments = self._extract_focus_segments(all_text)
        
        # Derive CAO type and membership from certifications
        if agency.certifications:
            if "ABU" in agency.certifications:
                agency.cao_type = CaoType.ABU
                agency.membership = ["ABU"]
            elif "NBBU" in agency.certifications:
                agency.cao_type = CaoType.NBBU
                agency.membership = ["NBBU"]
        
        # Derive regions_served from office locations
        if agency.office_locations and len(agency.office_locations) >= 5:
            # If offices in 5+ cities, likely national coverage
            agency.regions_served = ["landelijk"]
        
        # Extract digital capabilities from website
        agency.digital_capabilities = self._extract_digital_capabilities(all_text)
        
        # ========================================================================
        # Extract ALL common fields using base class utility method! 🚀
        # This replaces 50+ lines of repetitive extraction code
        # ========================================================================
        self.extract_all_common_fields(agency, all_text)

        # Update evidence URLs
        agency.evidence_urls = self.evidence_urls.copy()
        agency.collected_at = self.collected_at

        # Log extraction summary
        self.logger.info(f"--- Extraction Summary for {self.AGENCY_NAME} ---")
        self.logger.info(f"  Legal Name: {agency.legal_name or 'NOT FOUND'}")
        self.logger.info(f"  KvK: {agency.kvk_number or 'NOT FOUND'}")
        self.logger.info(f"  Phone: {agency.contact_phone or 'NOT FOUND'}")
        self.logger.info(f"  Email: {agency.contact_email or 'NOT FOUND'}")
        self.logger.info(f"  HQ: {agency.hq_city or 'NOT FOUND'}, {agency.hq_province or 'NOT FOUND'}")
        self.logger.info(f"  Logo: {'YES' if agency.logo_url else 'NOT FOUND'}")
        self.logger.info(f"  Offices: {len(agency.office_locations or [])} found")
        self.logger.info(f"  Sectors: {len(agency.sectors_core or [])} found")
        self.logger.info(f"  Certifications: {len(agency.certifications or [])} found")
        self.logger.info(f"  Pages scraped: {len(self.evidence_urls)}")

        self.logger.info(f"Completed scrape of {self.AGENCY_NAME}")
        return agency

    def _extract_logo(self, soup: BeautifulSoup) -> str | None:
        """Extract logo from header."""
        logo = soup.select_one("header img, .site-header img, [class*='logo'] img")
        if logo:
            src = logo.get("src") or logo.get("data-src")
            if src and not src.startswith("data:"):
                if not src.startswith("http"):
                    src = f"{self.WEBSITE_URL}{src}"
                self.logger.info(f"Found logo: {src}")
                return src
        return None

    def _extract_email_from_kenniscentrum(self, soup: BeautifulSoup, url: str) -> str | None:
        """
        Extract contact email from kenniscentrum page.
        
        HTML structure:
        <div class="transition flex-1 flex flex-col...">
            <h3>Mail ons</h3>
            <p>Neem contact op via info@asatalent.nl ⇢</p>
        </div>
        """
        # Look for email in page text
        page_text = soup.get_text()
        
        # Look for info@asatalent.nl specifically
        email_match = re.search(r"(info@asatalent\.nl)", page_text)
        if email_match:
            email = email_match.group(1)
            self.logger.info(f"✓ Found contact email: {email} | Source: {url}")
            return email
        
        # Fallback: look for any @asatalent.nl email
        email_match = re.search(r"([a-zA-Z0-9._%+-]+@asatalent\.nl)", page_text)
        if email_match:
            email = email_match.group(1)
            self.logger.info(f"✓ Found contact email: {email} | Source: {url}")
            return email
            
        return None

    def _extract_legal_name(self, text: str) -> str | None:
        """
        Extract legal name from disclaimer page.
        
        ASA operates under Start People B.V.
        """
        # Pattern: "Deze website is van Start People B.V."
        match = re.search(r"website is van\s+([A-Za-z\s\-]+B\.V\.)", text, re.IGNORECASE)
        if match:
            legal_name = match.group(1).strip()
            self.logger.info(f"Found legal name: {legal_name}")
            return legal_name
        return None

    def _extract_office_locations_from_html(self, soup: BeautifulSoup) -> tuple[list[OfficeLocation], str | None, str | None]:
        """
        Extract office locations from vestigingen page HTML.
        
        HTML structure:
        <a href="/vestigingen/...">
            <div>
                <h3>ASA Amsterdam</h3>
                <p>Paalbergweg 2\n1105 AG Amsterdam</p>
                <span onclick="tel:020-6626161">020-6626161</span>
            </div>
        </a>
        
        Returns:
            (locations, main_phone, main_email)
        """
        locations = []
        seen_cities = set()
        main_phone = None
        main_email = None
        
        # Find all office cards in grid
        office_cards = soup.select('div.grid a[href*="/vestigingen/"]')
        
        for card in office_cards:
            # Get office name from h3
            h3 = card.find("h3")
            if not h3:
                continue
            
            # Get address from p tag
            p = card.find("p")
            if not p:
                continue
            
            address_text = p.get_text(separator=" ", strip=True)
            
            # Extract postal code and city (pattern: "1105 AG Amsterdam")
            postal_match = re.search(r"(\d{4})\s*([A-Z]{2})\s+([A-Za-z\-]+)", address_text)
            if not postal_match:
                continue
            
            postal_code = postal_match.group(1)
            postal_letters = postal_match.group(2)
            city = postal_match.group(3)
            
            if city.lower() in seen_cities:
                continue
            
            # Get province from postal code
            postal_prefix = postal_code[:2]
            province = DUTCH_POSTAL_TO_PROVINCE.get(postal_prefix)
            
            # Extract phone from span onclick (tel:030 23 33 444)
            phone = None
            span = card.find("span", onclick=True)
            if span:
                onclick = span.get("onclick", "")
                tel_match = re.search(r"tel:([^'\"]+)", onclick)
                if tel_match:
                    phone = tel_match.group(1).strip().rstrip("'")
            
            locations.append(OfficeLocation(city=city, province=province))
            seen_cities.add(city.lower())
            self.logger.info(f"Found office: {city}, {province} - {phone}")
            
            # Use Utrecht as main contact
            if city.lower() == "utrecht":
                main_phone = phone
                main_email = "utrecht@asatalent.nl"
        
        return locations, main_phone, main_email

    def _extract_certifications(self, text: str) -> list[str]:
        """
        Extract certifications from keurmerken page.
        
        ASA has: ABU, ISO 9001, ISO 14001, ISO 27001, NEN4400-1/SNA
        """
        certs = []
        
        if "ABU" in text or "Algemene Bond" in text:
            certs.append("ABU")
            self.logger.info("Found certification: ABU")
        
        if "ISO 9001" in text:
            certs.append("ISO 9001")
            self.logger.info("Found certification: ISO 9001")
        
        if "ISO 14001" in text:
            certs.append("ISO 14001")
            self.logger.info("Found certification: ISO 14001")
        
        if "ISO 27001" in text:
            certs.append("ISO 27001")
            self.logger.info("Found certification: ISO 27001")
        
        if "NEN4400" in text or "NEN 4400" in text:
            certs.append("NEN 4400-1")
            self.logger.info("Found certification: NEN 4400-1")
        
        if "SNA" in text or "Stichting Normering Arbeid" in text:
            certs.append("SNA")
            self.logger.info("Found certification: SNA")
        
        if "CO2" in text and "Reductie" in text:
            certs.append("CO2-Reductiemanagement")
            self.logger.info("Found certification: CO2-Reductiemanagement")
        
        return certs

    def _extract_sectors_from_expertises(self, soup: BeautifulSoup, url: str) -> list[str]:
        """
        Extract sectors_core from the expertises page.
        
        Source: https://asatalent.nl/werkgevers/expertises
        
        HTML structure:
        <a href="/werkgevers/expertise/onderwijs" class="pb-6 block h-full">
            <div class="group transition...">
                <h3 class="text-xl md:text-[2rem] text-black uppercase">Onderwijs</h3>
                <span>Bekijk wat onze invalpool...</span>
            </div>
        </a>
        
        Returns sector names extracted directly from h3 elements.
        """
        sectors = []
        seen = set()
        
        # Find all expertise links (anchors with href containing /werkgevers/expertise/)
        expertise_links = soup.find_all("a", href=lambda h: h and "/werkgevers/expertise/" in h)
        
        for link in expertise_links:
            # Extract sector name from h3 inside the link
            h3 = link.find("h3")
            if not h3:
                continue
            
            # Get the text and normalize to lowercase
            sector = h3.get_text(strip=True).lower()
            
            if sector and sector not in seen:
                sectors.append(sector)
                seen.add(sector)
                self.logger.info(f"✓ Found sector '{sector}' | Source: {url}")
        
        return sectors

    def _extract_sectors_from_vakgebieden(self, soup: BeautifulSoup, url: str) -> list[str]:
        """
        Extract sectors from the vakgebieden page.
        
        Source: https://asatalent.nl/vacatures/vakgebieden
        
        HTML structure:
        <a href="/vacatures/administratief">
            <div class="transition...">
                <h3 class="text-2xl lg:text-4xl text-black uppercase my-2">Administratief</h3>
                <p>Bekijk de leukste (bij)banen ></p>
            </div>
        </a>
        
        Returns sector names extracted directly from h3 elements.
        """
        sectors = []
        seen = set()
        
        # Find all vacancy links (anchors with href containing /vacatures/)
        vacancy_links = soup.find_all("a", href=lambda h: h and "/vacatures/" in h)
        
        for link in vacancy_links:
            # Extract sector name from h3 inside the link
            h3 = link.find("h3")
            if not h3:
                continue
            
            # Get the text and normalize to lowercase
            sector = h3.get_text(strip=True).lower()
            
            if sector and sector not in seen:
                sectors.append(sector)
                seen.add(sector)
                self.logger.info(f"✓ Found sector '{sector}' | Source: {url}")
        
        return sectors

    def _extract_services_from_diensten(self, soup: BeautifulSoup, url: str) -> AgencyServices:
        """
        Extract services from the diensten page using BeautifulSoup.
        
        Source: https://asatalent.nl/werkgevers/diensten
        
        HTML structure:
        <a href="/werkgevers/uitzenden">
            <div class="group transition...">
                <h3 class="text-xl md:text-[2rem] text-black uppercase">Uitzenden</h3>
            </div>
        </a>
        
        Maps extracted service names to AgencyServices model fields.
        """
        # Mapping from h3 text (lowercase) or href keywords to service fields
        service_mapping = {
            # h3 text -> service field
            "uitzenden": "uitzenden",
            "broadcast": "uitzenden",  # English translation
            "werving en selectie": "werving_selectie",
            "recruitment and selection": "werving_selectie",  # English
            "freelance": "zzp_bemiddeling",
            "workshops": "opleiden_ontwikkelen",
            "detacheren": "detacheren",
            "detachering": "detacheren",
            "payroll": "payrolling",
            "payrolling": "payrolling",
        }
        
        found_services = {}
        
        # Find all service cards (anchors with h3 inside)
        all_anchors = soup.find_all("a", href=True)
        
        for link in all_anchors:
            h3 = link.find("h3")
            if not h3:
                continue
            
            service_text = h3.get_text(strip=True).lower()
            href = link.get("href", "").lower()
            
            # Check h3 text against mapping
            for keyword, field in service_mapping.items():
                if keyword in service_text or keyword in href:
                    found_services[field] = True
                    self.logger.info(f"✓ Found service '{field}' (from '{service_text}') | Source: {url}")
                    break
        
        # Also check href patterns for additional services
        href_patterns = {
            "/flexpool": "inhouse_services",
            "/snelle-levering": None,  # Speed claim, not a service type
            "/vakantiekrachten": None,  # Candidate type, not a service
            "/ervaren-professionals": None,  # Candidate type, not a service
        }
        
        for link in all_anchors:
            href = link.get("href", "").lower()
            for pattern, field in href_patterns.items():
                if pattern in href and field:
                    found_services[field] = True
                    self.logger.info(f"✓ Found service '{field}' (from href '{href}') | Source: {url}")
        
        # Build AgencyServices with found services set to True, others False
        return AgencyServices(
            uitzenden=found_services.get("uitzenden", False),
            detacheren=found_services.get("detacheren", False),
            werving_selectie=found_services.get("werving_selectie", False),
            payrolling=found_services.get("payrolling", False),
            zzp_bemiddeling=found_services.get("zzp_bemiddeling", False),
            inhouse_services=found_services.get("inhouse_services", False),
            opleiden_ontwikkelen=found_services.get("opleiden_ontwikkelen", False),
            vacaturebemiddeling_only=False,
            msp=False,
            rpo=False,
            executive_search=False,
            reintegratie_outplacement=False,
        )

    def _extract_focus_segments(self, text: str) -> list[str]:
        """
        Extract focus segments from website text.
        """
        segments = []
        text_lower = text.lower()
        
        if "student" in text_lower:
            segments.append("studenten")
            self.logger.info("✓ Found focus segment: studenten")
        if "starter" in text_lower:
            segments.append("starters")
            self.logger.info("✓ Found focus segment: starters")
        if "professional" in text_lower:
            segments.append("young_professionals")
            self.logger.info("✓ Found focus segment: young_professionals")
        if "bijbaan" in text_lower or "flexibel" in text_lower:
            if "studenten" not in segments:
                segments.append("studenten")
                self.logger.info("✓ Found focus segment: studenten (bijbaan/flexibel)")
        
        unique_segments = list(set(segments))
        self.logger.info(f"Total focus segments found: {len(unique_segments)}")
        return unique_segments

    def _extract_digital_capabilities(self, text: str) -> DigitalCapabilities:
        """
        Extract digital capabilities from website text.
        
        Looks for keywords indicating portals, apps, APIs, etc.
        """
        text_lower = text.lower()
        
        # Check for candidate portal (Mijn ASA, mijn omgeving, etc.)
        candidate_portal = False
        if "mijn asa" in text_lower or "mijn omgeving" in text_lower or "inloggen" in text_lower:
            candidate_portal = True
            self.logger.info("✓ Found digital capability: candidate_portal (Mijn ASA)")
        
        # Check for client portal
        client_portal = False
        if "werkgeversportaal" in text_lower or "klantenportaal" in text_lower or "client portal" in text_lower:
            client_portal = True
            self.logger.info("✓ Found digital capability: client_portal")
        
        # Check for mobile app
        mobile_app = False
        if "app store" in text_lower or "google play" in text_lower or "mobiele app" in text_lower:
            mobile_app = True
            self.logger.info("✓ Found digital capability: mobile_app")
        
        # Check for API
        api_available = False
        if "api" in text_lower and ("koppeling" in text_lower or "integratie" in text_lower):
            api_available = True
            self.logger.info("✓ Found digital capability: api_available")
        
        return DigitalCapabilities(
            candidate_portal=candidate_portal,
            client_portal=client_portal,
            mobile_app=mobile_app,
            api_available=api_available,
        )

    def _extract_from_rgf_privacy_pdf(self) -> dict | None:
        """
        Extract HQ info from RGF Staffing privacy statement PDF.
        
        Step 1: Go to https://asatalent.nl/legal/privacy-statement
        Step 2: Find link to PDF (href="https://rgfstaffing.nl/privacy-statement")
        Step 3: Download and parse PDF
        
        The PDF contains:
        - HQ Address: P.J. Oudweg 61 te (1314 CK) Almere
        - Phone: +31 (0)36 529 9555
        - Legal entities including "Start People B.V. (ook h.o.d.n. ASA)"
        
        Returns:
            dict with hq_city, hq_province, hq_address, hq_phone or None
        """
        try:
            import pdfplumber
        except ImportError:
            self.logger.warning("pdfplumber not installed, skipping PDF extraction")
            return None
        
        try:
            # Step 1: Fetch privacy statement page to get PDF link
            self.logger.info(f"Fetching privacy page: {self.PRIVACY_STATEMENT_PAGE}")
            page_response = requests.get(
                self.PRIVACY_STATEMENT_PAGE,
                headers={"User-Agent": get_chrome_user_agent()},
                timeout=30
            )
            page_response.raise_for_status()
            
            soup = BeautifulSoup(page_response.text, "html.parser")
            
            # Step 2: Find PDF link - look for rgfstaffing.nl/privacy-statement
            pdf_url = None
            for link in soup.find_all("a", href=True):
                href = link.get("href", "")
                if "rgfstaffing" in href and "privacy" in href:
                    pdf_url = href
                    break
            
            if not pdf_url:
                # Fallback to known URL
                pdf_url = self.RGF_PRIVACY_PDF_URL
            
            self.logger.info(f"Fetching RGF PDF from: {pdf_url}")
            
            # Step 3: Download PDF (may redirect)
            pdf_response = requests.get(
                pdf_url,
                headers={"User-Agent": get_chrome_user_agent()},
                timeout=30,
                allow_redirects=True
            )
            pdf_response.raise_for_status()
            
            # Check if it's actually a PDF
            content_type = pdf_response.headers.get("Content-Type", "")
            if "pdf" not in content_type.lower() and not pdf_response.content[:4] == b"%PDF":
                self.logger.warning(f"Response is not a PDF: {content_type}")
                return None
            
            pdf_bytes = io.BytesIO(pdf_response.content)
            
            with pdfplumber.open(pdf_bytes) as pdf:
                text = ""
                for page in pdf.pages[:3]:  # First 3 pages should have HQ info
                    text += page.extract_text() or ""
            
            self.logger.info(f"Extracted {len(text)} chars from RGF PDF")
            
            result = {}
            
            # Extract HQ address: "P.J. Oudweg 61 te (1314 CK) Almere"
            # Pattern: address te (postal_code) city
            addr_match = re.search(
                r"hoofdkantoor aan de\s+(.+?)\s+te\s*\(?\s*(\d{4})\s*([A-Z]{2})\s*\)?\s*([A-Za-z]+)",
                text, re.IGNORECASE
            )
            if addr_match:
                street = addr_match.group(1).strip()
                postal_code = addr_match.group(2)
                postal_letters = addr_match.group(3)
                city = addr_match.group(4).strip()
                
                result["hq_address"] = f"{street}, {postal_code} {postal_letters} {city}"
                result["hq_city"] = city
                
                # 13xx = Flevoland (Almere)
                prefix = postal_code[:2]
                if prefix == "13":
                    result["hq_province"] = "Flevoland"
                
                self.logger.info(f"Found HQ: {result['hq_address']}")
            
            # Extract phone: "+31 (0)36 529 9555"
            phone_match = re.search(
                r"telefoonnummer\s*\+?31\s*\(?0?\)?\s*(\d{2})\s*(\d{3})\s*(\d{4})",
                text, re.IGNORECASE
            )
            if phone_match:
                phone = f"0{phone_match.group(1)} {phone_match.group(2)} {phone_match.group(3)}"
                result["hq_phone"] = phone
                self.logger.info(f"Found phone: {phone}")
            
            return result if result else None
            
        except Exception as e:
            self.logger.warning(f"Error parsing RGF PDF: {e}")
            return None


@dg.asset(group_name="agencies")
def asa_talent_scrape() -> dg.Output[dict]:
    """Scrape ASA Talent Netherlands website."""
    scraper = ASATalentScraper()
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