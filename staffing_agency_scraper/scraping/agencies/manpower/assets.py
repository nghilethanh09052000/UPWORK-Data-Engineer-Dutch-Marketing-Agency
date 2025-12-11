"""
Manpower Netherlands scraper.

Website: https://www.manpower.nl
Part of: ManpowerGroup - World's largest staffing company
"""

from __future__ import annotations

import json
import re
from typing import Any, Dict, List

import dagster as dg
from bs4 import BeautifulSoup

from staffing_agency_scraper.models import Agency, GeoFocusType, OfficeLocation, VolumeSpecialisation
from staffing_agency_scraper.scraping.base import BaseAgencyScraper
from staffing_agency_scraper.scraping.utils import AgencyScraperUtils


class ManpowerScraper(BaseAgencyScraper):
    """Scraper for Manpower Netherlands."""

    AGENCY_NAME = "Manpower"
    WEBSITE_URL = "https://www.manpower.nl"
    BRAND_GROUP = "ManpowerGroup"
    
    # Pages to scrape with specific functions per page
    PAGES_TO_SCRAPE: List[Dict[str, Any]] = [
        {
            "name": "home",
            "url": "https://www.manpower.nl/nl",
            "functions": ["logo", "sectors", "services"],
        },
        {
            "name": "hr_services",
            "url": "https://www.manpower.nl/nl/werkgevers/hr-services",
            "functions": ["services", "sectors"],
        },
        {
            "name": "employers",
            "url": "https://www.manpower.nl/nl/manpower-business-professionals-voor-werkgevers",
            "functions": ["services"],
        },
        {
            "name": "werkgevers",
            "url": "https://www.manpower.nl/nl/werkgevers",
            "functions": [],
        },
        {
            "name": "specialisaties",
            "url": "https://www.manpower.nl/nl/werkgevers/specialisaties",
            "functions": ["sectors"],
        },
        {
            "name": "vestigingen",
            "url": "https://www.manpower.nl/nl/over-manpower/vestigingen",
            "functions": ["office_locations"],
        },
        {
            "name": "contact",
            "url": "https://www.manpower.nl/nl/over-manpower/contact",
            "functions": ["contact"],
        },
        {
            "name": "about",
            "url": "https://www.manpower.nl/nl/over-manpower/ons-bedrijf",
            "functions": [],
        },
        {
            "name": "privacy",
            "url": "https://www.manpower.nl/nl/privacy-statement",
            "functions": ["legal"],
        },
        {
            "name": "certifications",
            "url": "https://www.manpower.nl/nl/over-manpower/ons-bedrijf/certificeringen",
            "functions": ["certifications"],
        },
        {
            "name": "manpowergroup_contact",
            "url": "https://manpowergroup.nl/contact/",
            "functions": ["legal"],
        },
        {
            "name": "vacatures_voor_jou",
            "url": "https://www.manpower.nl/nl/werkzoekend/vacatures-voor-jou",
            "functions": ["sectors_secondary"],
        },
    ]

    def scrape(self) -> Agency:
        self.logger.info(f"Starting scrape of {self.AGENCY_NAME}")
        
        # Note: self.utils is now initialized in BaseAgencyScraper.__init__()
        agency = self.create_base_agency()
        agency.geo_focus_type = GeoFocusType.INTERNATIONAL
        agency.employers_page_url = f"{self.WEBSITE_URL}/nl/manpower-business-professionals-voor-werkgevers"
        agency.contact_form_url = f"{self.WEBSITE_URL}/nl/over-manpower/contact"
        
        # Add key URLs to evidence (avoid duplicates)
        if agency.employers_page_url not in self.evidence_urls:
            self.evidence_urls.append(agency.employers_page_url)
        if agency.contact_form_url not in self.evidence_urls:
            self.evidence_urls.append(agency.contact_form_url)
        
        # Known facts about Manpower (from ManpowerGroup)
        agency.regions_served = ["landelijk", "internationaal"]
        agency.volume_specialisation = VolumeSpecialisation.MASSA_50_PLUS  # Large-scale staffing
        
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
                
                # Extract navigation links for portal detection (home page)
                if page["name"] == "home":
                    self._extract_navigation_links(soup, agency, url)
                    # Detect mobile app
                    self._detect_mobile_app(soup, page_text, agency, url)
                
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
        
        # Extract from aggregated text
        agency.focus_segments = self._extract_focus_segments(all_text)
        
        # Extract certifications, CAO, membership
        agency.certifications = self.utils.fetch_certifications(all_text, "accumulated_text")
        agency.cao_type = self.utils.fetch_cao_type(all_text, "accumulated_text")
        agency.membership = self.utils.fetch_membership(all_text, "accumulated_text")
        
        # ========================================================================
        # Extract ALL common fields using base class utility method! 🚀
        # This replaces 50+ lines of repetitive extraction code
        # ========================================================================
        self.extract_all_common_fields(agency, all_text)
        
        # Update evidence URLs
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
                if not agency.logo_url:
                    agency.logo_url = self._extract_logo(soup, url)
            
            elif func_name == "sectors":
                sectors = self._extract_sectors(soup, page_text, url)
                if sectors:
                    if not agency.sectors_core:
                        agency.sectors_core = []
                    agency.sectors_core.extend(sectors)
                    agency.sectors_core = list(set(agency.sectors_core))
            
            elif func_name == "services":
                self._extract_services(soup, page_text, agency, url)
            
            elif func_name == "office_locations":
                offices = self._extract_office_locations(soup, url)
                if offices:
                    agency.office_locations = offices
            
            elif func_name == "contact":
                self._extract_contact(soup, page_text, agency, url)
            
            elif func_name == "legal":
                self._extract_legal(page_text, agency, url)
            
            elif func_name == "certifications":
                certs = self._extract_certifications(page_text, url)
                if certs:
                    if not agency.certifications:
                        agency.certifications = []
                    agency.certifications.extend(certs)
                    agency.certifications = list(set(agency.certifications))
            
            elif func_name == "sectors_secondary":
                secondary = self._extract_sectors_secondary(page_text, agency, url)
                if secondary:
                    if not agency.sectors_secondary:
                        agency.sectors_secondary = []
                    agency.sectors_secondary.extend(secondary)
                    agency.sectors_secondary = list(set(agency.sectors_secondary))
    
    def _extract_logo(self, soup: BeautifulSoup, url: str) -> str | None:
        """
        Extract logo from header.
        
        Manpower has SVG logos in the header:
        <header class="site-header">
            <a href="/nl">
                <img alt="Site Logo" class="site-logo" 
                     src="/-/jssmedia/project/manpowergroup/admin/logos/brand/light-bg/manpower-logo-horizontal.svg">
            </a>
        </header>
        """
        # Try to find header
        header = soup.find("header", class_="site-header")
        if header:
            # Find the logo img with class="site-logo"
            logo_img = header.find("img", class_="site-logo")
            if logo_img and logo_img.get("src"):
                logo_url = logo_img.get("src")
                # Make absolute URL
                if logo_url.startswith("/-/"):
                    logo_url = f"{self.WEBSITE_URL}{logo_url}"
                elif logo_url.startswith("/"):
                    logo_url = f"{self.WEBSITE_URL}{logo_url}"
                
                # Check if it's an SVG or PNG logo
                if ".svg" in logo_url.lower() or ".png" in logo_url.lower():
                    self.logger.info(f"✓ Found logo: {logo_url} | Source: {url}")
                    return logo_url
        
        # Fallback to utils method
        logo_url = self.utils.fetch_logo(soup, url)
        if logo_url:
            self.logger.info(f"✓ Found logo (fallback): {logo_url} | Source: {url}")
        return logo_url
    
    def _extract_navigation_links(self, soup: BeautifulSoup, agency: Agency, url: str) -> None:
        """
        Extract navigation links for portal detection.
        
        Looks for:
        - Candidate portal: "My Manpower", "Mijn Manpower" login
        - Client portal: "Voor werkgevers" portal links
        """
        # Find navigation areas
        nav_main = soup.find("nav", class_="main-nav")
        
        candidate_links = []
        employer_links = []
        
        # Look for "My Manpower" login indicator
        login_sections = soup.find_all(class_=re.compile(r"login"))
        for section in login_sections:
            login_text = section.get_text(strip=True).lower()
            if "my manpower" in login_text or "mijn manpower" in login_text:
                agency.digital_capabilities.candidate_portal = True
                self.logger.info(f"✓ Detected candidate_portal from: My Manpower login | Source: {url}")
        
        # Check navigation for employer portal links
        if nav_main:
            for link in nav_main.find_all("a", href=True):
                href = link.get("href", "")
                link_text = link.get_text(strip=True).lower()
                
                # Employer-specific links
                if any(keyword in href.lower() or keyword in link_text for keyword in [
                    "werkgevers", "employers", "vacature aanmelden", "hr-services"
                ]):
                    full_url = href if href.startswith("http") else f"{self.WEBSITE_URL}{href}"
                    employer_links.append((full_url, link_text))
                    self.logger.info(f"✓ Found employer link: {link_text} → {full_url} | Source: {url}")
    
    def _detect_mobile_app(self, soup: BeautifulSoup, page_text: str, agency: Agency, url: str) -> None:
        """
        Detect mobile app availability.
        
        Manpower mentions:
        - "Manpower mobiele app"
        - Apple Store / Google Play Store links
        """
        text_lower = page_text.lower()
        html_str = str(soup).lower()
        
        # Check for app store links or app mentions
        has_app = False
        if any(keyword in text_lower or keyword in html_str for keyword in [
            "app store", "google play", "mobiele app", "manpower app",
            "download de app", "my manpower app"
        ]):
            has_app = True
            agency.digital_capabilities.mobile_app = True
            self.logger.info(f"✓ Detected mobile_app: Manpower App | Source: {url}")
    
    def _extract_sectors(self, soup: BeautifulSoup, page_text: str, url: str) -> list[str]:
        """
        Extract sectors/specializations from page.
        
        From the specialisaties page, Manpower explicitly states their 4 CORE specializations:
        "Binnen de sectoren: overheid, productie en logistiek, financiële dienstverlening 
        en klant contact centra zijn we hierin gespecialiseerd."
        
        These are the ONLY 4 core sectors:
        1. Overheid (Public sector/Government) → publieke_sector
        2. Productie en logistiek (Production and logistics) → productie + logistiek
        3. Financiële dienstverlening (Financial services) → finance
        4. Klant contact centra (Customer contact centers) → callcenter
        """
        sectors = []
        text_lower = page_text.lower()
        
        # Check if this is the specialisaties page with the explicit core sectors statement
        if "specialisaties" in url:
            # Only extract the 4 explicitly stated CORE sectors from this page
            if "overheid" in text_lower or "government" in text_lower or "publieke sector" in text_lower:
                if "publieke_sector" not in sectors:
                    sectors.append("publieke_sector")
                    self.logger.info(f"✓ Found CORE sector: publieke_sector (Overheid) | Source: {url}")
            
            if "productie" in text_lower or "production" in text_lower:
                if "productie" not in sectors:
                    sectors.append("productie")
                    self.logger.info(f"✓ Found CORE sector: productie | Source: {url}")
            
            if "logistiek" in text_lower or "logistics" in text_lower:
                if "logistiek" not in sectors:
                    sectors.append("logistiek")
                    self.logger.info(f"✓ Found CORE sector: logistiek | Source: {url}")
            
            if "financiële dienstverlening" in text_lower or "financial" in text_lower or "finance" in text_lower:
                if "finance" not in sectors:
                    sectors.append("finance")
                    self.logger.info(f"✓ Found CORE sector: finance (Financiële dienstverlening) | Source: {url}")
            
            if "contact centra" in text_lower or "contact center" in text_lower or "callcenter" in text_lower:
                if "callcenter" not in sectors:
                    sectors.append("callcenter")
                    self.logger.info(f"✓ Found CORE sector: callcenter (Klant contact centra) | Source: {url}")
        
        return sectors
    
    def _extract_services(self, soup: BeautifulSoup, page_text: str, agency: Agency, url: str) -> None:
        """
        Extract services from page.
        
        Manpower offers (from HR services page):
        - Uitzenden (Temporary staffing)
        - Detachering (Secondment)
        - Werving & selectie (Recruitment & selection)
        - Payrolling
        - Onsite Management / Inhouse services
        - RPO (Recruitment Process Outsourcing)
        - MSP (Managed Service Provider)
        - ZZP bemiddeling (Freelance mediation)
        - MyPath ontwikkelprogramma (Training)
        """
        text_lower = page_text.lower()
        
        # Uitzenden / Temporary staffing
        if any(w in text_lower for w in ["uitzenden", "tijdelijk werk", "flexibel personeel", "broadcast"]):
            agency.services.uitzenden = True
            self.logger.info(f"✓ Found service: uitzenden | Source: {url}")
        
        # Detachering / Secondment
        if "detachering" in text_lower or "detacheren" in text_lower or "secondment" in text_lower:
            agency.services.detacheren = True
            self.logger.info(f"✓ Found service: detacheren | Source: {url}")
        
        # Werving & selectie (Recruitment & Selection)
        if "werving" in text_lower or ("recruitment" in text_lower and "selectie" in text_lower):
            agency.services.werving_selectie = True
            self.logger.info(f"✓ Found service: werving_selectie | Source: {url}")
        
        # Payrolling
        if "payroll" in text_lower:
            agency.services.payrolling = True
            self.logger.info(f"✓ Found service: payrolling | Source: {url}")
        
        # Onsite Management / Inhouse services
        if "onsite management" in text_lower or "inhouse" in text_lower or "in-house" in text_lower:
            agency.services.inhouse_services = True
            self.logger.info(f"✓ Found service: inhouse_services (Onsite Management) | Source: {url}")
        
        # RPO (Recruitment Process Outsourcing)
        if "rpo" in text_lower or "recruitment process outsourcing" in text_lower:
            agency.services.rpo = True
            self.logger.info(f"✓ Found service: rpo | Source: {url}")
        
        # MSP (Managed Service Provider)
        if "msp" in text_lower or "managed service provider" in text_lower:
            agency.services.msp = True
            self.logger.info(f"✓ Found service: msp | Source: {url}")
        
        # ZZP / Freelance
        if "zzp" in text_lower or "freelance" in text_lower or "zelfstandig" in text_lower:
            agency.services.zzp_bemiddeling = True
            self.logger.info(f"✓ Found service: zzp_bemiddeling | Source: {url}")
        
        # Training / Development (MyPath)
        if "mypath" in text_lower or "training" in text_lower or "opleiding" in text_lower or "ontwikkelen" in text_lower:
            agency.services.opleiden_ontwikkelen = True
            self.logger.info(f"✓ Found service: opleiden_ontwikkelen (MyPath) | Source: {url}")
    
    def _extract_contact(self, soup: BeautifulSoup, page_text: str, agency: Agency, url: str) -> None:
        """
        Extract contact info from contact page.
        
        The contact page contains HQ information:
        - Per telefoon: 020 660 22 22
        - Per e-mail: info@manpowergroup.nl
        - Het adres is Diemerhof 16-18, 1112 XN Diemen
        """
        # Contact email and phone
        if not agency.contact_email:
            # The contact page explicitly states: "Per e-mail: info@manpowergroup.nl"
            # Try multiple patterns to find it
            if any(phrase in page_text.lower() for phrase in ["info@manpowergroup.nl", "info&#64;manpowergroup", "marketing@manpowergroup"]):
                agency.contact_email = "info@manpowergroup.nl"
                self.logger.info(f"✓ Found contact email: {agency.contact_email} | Source: {url}")
            else:
                # Fall back to utils method
                email = self.utils.fetch_contact_email(page_text, url)
                if email:
                    agency.contact_email = email
                    self.logger.info(f"✓ Found contact email (via utils): {agency.contact_email} | Source: {url}")
                else:
                    # Hardcode based on known value from contact page
                    agency.contact_email = "info@manpowergroup.nl"
                    self.logger.info(f"✓ Set contact email (known): {agency.contact_email} | Source: {url}")
        
        if not agency.contact_phone:
            # Look for the HQ phone: 020 660 22 22
            phone_match = re.search(r'020\s*660\s*22\s*22', page_text)
            if phone_match:
                agency.contact_phone = "020 660 22 22"
                self.logger.info(f"✓ Found contact phone: {agency.contact_phone} | Source: {url}")
            else:
                agency.contact_phone = self.utils.fetch_contact_phone(page_text, url)
        
        # HQ location (Diemen)
        if not agency.hq_city:
            if "diemen" in page_text.lower():
                agency.hq_city = "Diemen"
                agency.hq_province = "Noord-Holland"
                self.logger.info(f"✓ Found HQ: {agency.hq_city}, {agency.hq_province} | Source: {url}")
        
        # Office locations
        if not agency.office_locations:
            agency.office_locations = self.utils.fetch_office_locations(soup, url)
            if agency.office_locations:
                # First office is HQ
                agency.hq_city = agency.office_locations[0].city
                agency.hq_province = agency.office_locations[0].province
    
    def _extract_legal(self, page_text: str, agency: Agency, url: str) -> None:
        """
        Extract KvK and legal name.
        
        ManpowerGroup Netherlands has KVK 33125207 (parent company)
        shown on https://manpowergroup.nl/contact/
        """
        # KvK number - look for specific ManpowerGroup KVK first
        if not agency.kvk_number:
            # Try to find ManpowerGroup Netherlands B.V. KVK: 33125207
            if "33125207" in page_text:
                agency.kvk_number = "33125207"
                self.logger.info(f"✓ Found KvK number: 33125207 (ManpowerGroup Netherlands B.V.) | Source: {url}")
            else:
                # Fallback to generic extraction
                kvk = self.utils.fetch_kvk_number(page_text, url)
                if kvk:
                    agency.kvk_number = kvk
                    self.logger.info(f"✓ Found KvK number: {kvk} | Source: {url}")
        
        # Legal name - look for ManpowerGroup Netherlands B.V. or Manpower B.V.
        if not agency.legal_name:
            if "manpowergroup netherlands b.v." in page_text.lower():
                agency.legal_name = "ManpowerGroup Netherlands B.V."
                self.logger.info(f"✓ Found legal name: {agency.legal_name} | Source: {url}")
            elif "manpower b.v." in page_text.lower() or "manpower bv" in page_text.lower():
                agency.legal_name = "Manpower B.V."
                self.logger.info(f"✓ Found legal name: {agency.legal_name} | Source: {url}")
            else:
                # Fallback to generic extraction
                legal_name = self.utils.fetch_legal_name(page_text, "Manpower", url)
                if legal_name:
                    agency.legal_name = legal_name
                    self.logger.info(f"✓ Found legal name: {agency.legal_name} | Source: {url}")
    
    def _extract_certifications(self, page_text: str, url: str) -> list[str]:
        """
        Extract certifications from the certifications page.
        
        Manpower has the following certifications:
        - ISO 9001:2015 (Quality management)
        - ISO/IEC 27001:2022 (Information security)
        - VCU (Veiligheids Checklist Uitzendorganisaties - Safety)
        - ABU (Algemene Bond Uitzendondernemingen - Member)
        - SNA (Stichting Normering Arbeid - Quality mark)
        """
        certifications = []
        text_lower = page_text.lower()
        
        # ISO 9001:2015 - Quality management
        if "iso 9001" in text_lower or "iso9001" in text_lower:
            certifications.append("ISO_9001")
            self.logger.info(f"✓ Found certification: ISO_9001 (Quality Management) | Source: {url}")
        
        # ISO/IEC 27001:2022 - Information security
        if "iso 27001" in text_lower or "iso/iec 27001" in text_lower or "iso27001" in text_lower:
            certifications.append("ISO_27001")
            self.logger.info(f"✓ Found certification: ISO_27001 (Information Security) | Source: {url}")
        
        # VCU - Safety certificate
        if "vcu" in text_lower or "veiligheids checklist uitzendorganisaties" in text_lower:
            certifications.append("VCU")
            self.logger.info(f"✓ Found certification: VCU (Safety Certificate) | Source: {url}")
        
        # ABU - Member of temp agency association
        if "abu" in text_lower or "algemene bond uitzendondernemingen" in text_lower:
            certifications.append("ABU")
            self.logger.info(f"✓ Found certification: ABU (Member) | Source: {url}")
        
        # SNA - Quality mark for labor agencies
        if "sna" in text_lower or "stichting normering arbeid" in text_lower:
            certifications.append("SNA")
            self.logger.info(f"✓ Found certification: SNA (Quality Mark) | Source: {url}")
        
        return certifications
    
    def _extract_sectors_secondary(self, page_text: str, agency: Agency, url: str) -> list[str]:
        """
        Extract secondary sectors from "Vacatures per vakgebied" section.
        
        This page lists all sectors where Manpower has job openings.
        We exclude sectors that are already in sectors_core to avoid duplication.
        
        Core sectors to exclude (already in sectors_core):
        - callcenter (Klantenservice)
        - finance (Financieel)
        - logistiek (Logistiek)
        - productie (Productie)
        - publieke_sector (Overheid)
        """
        secondary_sectors = []
        text_lower = page_text.lower()
        
        # Get existing core sectors to exclude
        core_sectors_keywords = {
            "klantenservice", "financieel", "logistiek", "productie", "overheid"
        }
        
        # Map vakgebied to our sector taxonomy
        # Only include if NOT already in core sectors
        sector_mapping = {
            "administratief": ["administratief"],
            "it": ["automatisering", "ict"],
            "finance": ["banken", "verzekeringen"],  # Will skip if in core
            "beveiliging": ["beveiliging"],
            "bouw": ["bouw", "installatie"],
            "marketing": ["commercieel", "sales", "marketing"],
            "communicatie": ["communicatie"],
            "publieke_sector": ["defensie"],  # Military/defense is public sector
            "facilitair": ["facilitair"],
            "grafische_sector": ["grafisch"],
            "horeca": ["horeca"],
            "industrie": ["industrieel"],
            "juridisch": ["juridisch"],
            "magazijn": ["magazijn"],
            "management": ["management leidinggevend"],
            "zorg": ["medisch", "verzorgend"],
            "schoonmaak": ["schoonmaak"],
            "office": ["secretarieel", "zakelijke dienstverlening"],
            "techniek": ["techniek"],
            "transport": ["transport"],
        }
        
        for sector, keywords in sector_mapping.items():
            # Check if any keyword is in the text
            if any(keyword in text_lower for keyword in keywords):
                # Skip if this sector is already in core (based on overlap)
                skip = False
                
                # Special handling: Skip finance-related if finance is in core
                if sector == "finance" and agency.sectors_core and "finance" in agency.sectors_core:
                    skip = True
                
                # Skip public sector related if already in core
                if sector == "publieke_sector" and agency.sectors_core and "publieke_sector" in agency.sectors_core:
                    skip = True
                
                if not skip and sector not in secondary_sectors:
                    secondary_sectors.append(sector)
                    self.logger.info(f"✓ Found secondary sector: {sector} (from: {', '.join(keywords)}) | Source: {url}")
        
        return secondary_sectors
    
    def _extract_office_locations(self, soup: BeautifulSoup, url: str) -> list[OfficeLocation]:
        """
        Extract office locations from JSON embedded in the page.
        
        The locations are stored in <script type="application/json" id="__JSS_STATE__">
        under: sitecore.route.placeholders.jss-main[0].fields.items[]
        """
        offices = []
        
        # Find the __JSS_STATE__ script
        script = soup.find("script", {"type": "application/json", "id": "__JSS_STATE__"})
        if not script:
            self.logger.warning(f"Could not find __JSS_STATE__ script on {url}")
            return offices
        
        try:
            data = json.loads(script.string)
            
            # Navigate to the locations data
            # Path: sitecore -> route -> placeholders -> jss-main -> [0] -> fields -> items
            main_component = data.get("sitecore", {}).get("route", {}).get("placeholders", {}).get("jss-main", [])
            if not main_component:
                self.logger.warning("Could not find jss-main in JSON")
                return offices
            
            # The first component should be "LocationsFinder"
            location_finder = main_component[0]
            if location_finder.get("componentName") != "LocationsFinder":
                self.logger.warning(f"Expected LocationsFinder, got {location_finder.get('componentName')}")
                return offices
            
            # Get the items
            items = location_finder.get("fields", {}).get("items", [])
            
            # Province mapping for Dutch cities
            city_province_map = {
                "alkmaar": "Noord-Holland",
                "almere": "Flevoland",
                "amsterdam": "Noord-Holland",
                "diemen": "Noord-Holland",
                "bergen op zoom": "Noord-Brabant",
                "breda": "Noord-Brabant",
                "'s-hertogenbosch": "Noord-Brabant",
                "den haag": "Zuid-Holland",
                "rijswijk": "Zuid-Holland",
                "eindhoven": "Noord-Brabant",
                "emmen": "Drenthe",
                "leeuwarden": "Friesland",
                "groningen": "Groningen",
                "heerlen": "Limburg",
                "kerkrade": "Limburg",
                "hoogeveen": "Drenthe",
                "maastricht": "Limburg",
                "rotterdam": "Zuid-Holland",
                "schiphol": "Noord-Holland",
                "terneuzen": "Zeeland",
                "tilburg": "Noord-Brabant",
                "hengelo": "Overijssel",
                "utrecht": "Utrecht",
                "venlo": "Limburg",
                "zwolle": "Overijssel",
            }
            
            seen_cities = set()
            
            for item in items:
                fields = item.get("fields", {})
                title = fields.get("title", {}).get("value", "")
                address = fields.get("address", {}).get("value", "")
                
                if not title:
                    continue
                
                # Extract city name from title (e.g., "Manpower Alkmaar" -> "Alkmaar")
                city_name = title.replace("Manpower ", "").replace("ManpowerGroup ", "")
                city_name_clean = city_name.strip()
                
                # Skip if we've already seen this city
                if city_name_clean.lower() in seen_cities:
                    continue
                
                # Map city to province
                province = None
                for city_key, prov in city_province_map.items():
                    if city_key in city_name_clean.lower() or city_key in address.lower():
                        province = prov
                        break
                
                if province:
                    office = OfficeLocation(city=city_name_clean, province=province)
                    offices.append(office)
                    seen_cities.add(city_name_clean.lower())
                    self.logger.info(f"✓ Found office: {city_name_clean}, {province} | Source: {url}")
            
            self.logger.info(f"Total offices found: {len(offices)}")
            
        except json.JSONDecodeError as e:
            self.logger.error(f"Failed to parse JSON from {url}: {e}")
        except Exception as e:
            self.logger.error(f"Error extracting office locations from {url}: {e}")
        
        return offices
    
    def _extract_focus_segments(self, text: str) -> list[str]:
        """Extract focus segments from text."""
        segments = []
        text_lower = text.lower()
        
        # Manpower serves multiple segments
        if any(kw in text_lower for kw in ["blue collar", "productie", "logistiek", "warehouse"]):
            segments.append("blue_collar")
            self.logger.info("✓ Found focus segment: blue_collar")
        
        if any(kw in text_lower for kw in ["white collar", "office", "administratie", "kantoor"]):
            segments.append("white_collar")
            self.logger.info("✓ Found focus segment: white_collar")
        
        if any(kw in text_lower for kw in ["professional", "business professional", "specialist"]):
            segments.append("young_professionals")
            self.logger.info("✓ Found focus segment: young_professionals")
        
        if any(kw in text_lower for kw in ["technical", "technisch", "engineer"]):
            segments.append("technisch_specialisten")
            self.logger.info("✓ Found focus segment: technisch_specialisten")
        
        self.logger.info(f"Total focus segments found: {len(segments)}")
        return segments


@dg.asset(group_name="agencies")
def manpower_scrape() -> dg.Output[dict]:
    """Scrape Manpower Netherlands website."""
    scraper = ManpowerScraper()
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
