"""
Hays Netherlands scraper.

Website: https://www.hays.nl
Specializes in: Professional recruitment, RPO, MSP
International staffing company with 50+ years experience globally
"""

from __future__ import annotations

import re
from typing import Any, Dict, List

import dagster as dg
from bs4 import BeautifulSoup

from staffing_agency_scraper.models import (
    Agency,
    GeoFocusType,
    OfficeLocation,
    VolumeSpecialisation,
)
from staffing_agency_scraper.scraping.base import BaseAgencyScraper
from staffing_agency_scraper.scraping.utils import AgencyScraperUtils


class HaysScraper(BaseAgencyScraper):
    """Scraper for Hays Netherlands."""

    AGENCY_NAME = "Hays Nederland"
    WEBSITE_URL = "https://www.hays.nl"
    BRAND_GROUP = "Hays plc"
    
    # Pages to scrape with specific functions per page
    PAGES_TO_SCRAPE: List[Dict[str, Any]] = [
        {
            "name": "home",
            "url": "https://www.hays.nl/home",
            "functions": ["logo", "sectors", "services"],
        },
        {
            "name": "contact",
            "url": "https://www.hays.nl/contact",
            "functions": ["contact"],
        },
        {
            "name": "privacy",
            "url": "https://www.hays.nl/herzien-privacybeleid",
            "functions": ["legal"],
        },
        {
            "name": "terms",
            "url": "https://www.hays.nl/gebruiksvoorwaarden",
            "functions": ["legal"],
        }
    ]

    def scrape(self) -> Agency:
        self.logger.info(f"Starting scrape of {self.AGENCY_NAME}")
        
        # Note: self.utils is now initialized in BaseAgencyScraper.__init__()
        agency = self.create_base_agency()
        agency.geo_focus_type = GeoFocusType.INTERNATIONAL
        #agency.employers_page_url = f"{self.WEBSITE_URL}/recruitment/contacteer-ons"
        agency.contact_form_url = f"{self.WEBSITE_URL}/contact"
        
        # Add key URLs to evidence (avoid duplicates)
        if agency.employers_page_url not in self.evidence_urls:
            self.evidence_urls.append(agency.employers_page_url)
        if agency.contact_form_url not in self.evidence_urls:
            self.evidence_urls.append(agency.contact_form_url)

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
                    # AI capabilities: Only set to True if explicitly stated on website
                    # Removed chatbot detection as per client feedback
                
                # Extract role levels on every page
                # role_levels = self.utils.fetch_role_levels(page_text, url)
                # if role_levels:
                #     if not agency.role_levels:
                #         agency.role_levels = []
                #     agency.role_levels.extend(role_levels)
                #     agency.role_levels = list(set(agency.role_levels))
                
                # Extract review sources
                # Review extraction removed per client requirement
                # Reviews must be explicitly shown/linked on the website, not inferred

            except Exception as e:
                self.logger.warning(f"Error scraping {url}: {e}")

        # Extract from aggregated text and normalize
        from staffing_agency_scraper.lib.normalize import normalize_focus_segments, normalize_regions_served
        focus_segments = self._extract_focus_segments(all_text)
        agency.focus_segments = normalize_focus_segments(focus_segments)
        regions = self._extract_regions_served(all_text)
        agency.regions_served = normalize_regions_served(regions)
        
        # Set volume specialisation
        if "1,000 vacancies" in all_text or "80 consultants" in all_text:
            agency.volume_specialisation = VolumeSpecialisation.POOLS_5_50

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
        # agency.evidence_urls = self.get_filtered_evidence_urls()
        agency.evidence_urls = self.evidence_urls.copy()
        agency.collected_at = self.collected_at

        self.logger.info(f"Completed scrape of {self.AGENCY_NAME}")
        with open("all_text.txt", "w") as f:
            f.write(all_text)

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
                sectors = self._extract_sectors(soup, url)
                if sectors:
                    agency.sectors_core = sectors
            
            elif func_name == "services":
                self._extract_services(soup, agency, url)
            
            elif func_name == "contact":
                self._extract_contact(soup, agency, url)
            
            elif func_name == "legal":
                self._extract_legal(page_text, agency, url)

    def _extract_sectors(self, soup: BeautifulSoup, url: str) -> list[str]:
        """Extract specialisms/sectors from page."""
        sectors = []
        seen = set()
        
        # Find the "Onze specialismes" or "Our specialisms" heading
        specialisms_heading = soup.find("h2", string=re.compile(r"(Onze specialismes|Our specialisms)", re.IGNORECASE))
        
        if specialisms_heading:
            parent_box = specialisms_heading.find_parent("div", class_="box")
            if parent_box:
                list_items = parent_box.find_all("li")
                for li in list_items:
                    link = li.find("a")
                    if link:
                        specialism_text = link.get_text(strip=True)
                        if specialism_text and specialism_text.lower() not in seen:
                            sectors.append(specialism_text)
                            seen.add(specialism_text.lower())
                            self.logger.info(f"✓ Found sector: '{specialism_text}' | Source: {url}")
        
        # Fallback: Look for specialism links in recruitment URLs
        if not sectors:
            for link in soup.find_all("a", href=re.compile(r"/recruitment/.*-recruitment")):
                specialism_text = link.get_text(strip=True).replace("►", "").strip()
                if specialism_text and specialism_text.lower() not in seen:
                    sectors.append(specialism_text)
                    seen.add(specialism_text.lower())
                    self.logger.info(f"✓ Found sector: '{specialism_text}' | Source: {url}")
        
        return sectors

    def _extract_services(self, soup: BeautifulSoup, agency: Agency, url: str) -> None:
        """Extract services from page."""
        text_lower = soup.get_text(separator=" ", strip=True).lower()

        # Find services section
        services_heading = soup.find("h2", string=re.compile(r"(All our services|Al onze diensten)", re.IGNORECASE))
        
        if services_heading:
            parent_box = services_heading.find_parent("div", class_="box")
            if parent_box:
                services_text = parent_box.get_text(separator=" ", strip=True).lower()
                
                if "perm recruitment" in services_text or "permanente recruitment" in services_text:
                    agency.services.werving_selectie = True
                    self.logger.info(f"✓ Found service 'werving_selectie' | Source: {url}")
                
                if "flex recruitment" in services_text or "flexibele inhuur" in services_text:
                    agency.services.detacheren = True
                    agency.services.uitzenden = True
                    self.logger.info(f"✓ Found service 'detacheren' | Source: {url}")
                
                if "contracting" in services_text or "freelancer" in services_text:
                    agency.services.zzp_bemiddeling = True
                    self.logger.info(f"✓ Found service 'zzp_bemiddeling' | Source: {url}")
                
                if "recruitment process outsourcing" in services_text or "rpo" in services_text:
                    agency.services.rpo = True
                    self.logger.info(f"✓ Found service 'rpo' | Source: {url}")
                
                if "managed service provider" in services_text or "msp" in services_text:
                    agency.services.msp = True
                    self.logger.info(f"✓ Found service 'msp' | Source: {url}")
        
        # Fallback to general text - Only set common services, not enterprise services
        # Enterprise services (MSP, RPO, executive_search) require explicit confirmation
        else:
            if "perm recruitment" in text_lower:
                agency.services.werving_selectie = True
            if "flex recruitment" in text_lower or "temporary" in text_lower:
                agency.services.detacheren = True
            if "contracting" in text_lower:
                agency.services.zzp_bemiddeling = True
            # Note: MSP, RPO, executive_search are NOT set in fallback - require explicit confirmation

    def _extract_contact(self, soup: BeautifulSoup, agency: Agency, url: str) -> None:
        """Extract contact info from contact page."""
        # Province mapping for Dutch cities
        city_province_map = {
            "amsterdam": "Noord-Holland",
            "tilburg": "Noord-Brabant",
            "rotterdam": "Zuid-Holland",
            "utrecht": "Utrecht",
            "eindhoven": "Noord-Brabant",
            "den haag": "Zuid-Holland",
            "groningen": "Groningen",
        }
        
        # Find offices section
        offices_heading = soup.find("h2", string=re.compile(r"Kantoren|Offices", re.IGNORECASE))
        
        if offices_heading:
            table = offices_heading.find_next("table")
            
            if table:
                # Find all h3 elements (office names)
                office_headers = table.find_all("h3")
                
                for header in office_headers:
                    city_name = header.get_text(strip=True)
                    if city_name:
                        city_name = city_name.replace("\xa0", " ").strip()
                        province = city_province_map.get(city_name.lower(), None)
                        office = OfficeLocation(city=city_name, province=province)
                        if not agency.office_locations:
                            agency.office_locations = []
                        agency.office_locations.append(office)
                        self.logger.info(f"✓ Found office: {city_name} ({province}) | Source: {url}")
                        
                        # First office is HQ
                        if not agency.hq_city:
                            agency.hq_city = city_name
                            agency.hq_province = province
                
                # Extract phone and email from first office
                first_cell = table.find("td")
                if first_cell:
                    cell_text = first_cell.get_text(separator=" ", strip=True)
                    
                    # Extract phone
                    phone_match = re.search(r"(\d{3}[\s-]?\d{2}[\s-]?\d{2}[\s-]?\d{3})", cell_text)
                    if phone_match and not agency.contact_phone:
                        agency.contact_phone = phone_match.group(1)
                        self.logger.info(f"✓ Found phone: {agency.contact_phone} | Source: {url}")
                    
                    # Extract email
                    email_match = re.search(r"([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})", cell_text)
                    if email_match and not agency.contact_email:
                        agency.contact_email = email_match.group(1)
                        self.logger.info(f"✓ Found email: {agency.contact_email} | Source: {url}")

    def _extract_legal(self, page_text: str, agency: Agency, url: str) -> None:
        """Extract KvK and legal name."""
        # KvK number
        if not agency.kvk_number:
            kvk = self.utils.fetch_kvk_number(page_text, url)
            if kvk:
                agency.kvk_number = kvk
        
        # Legal name
        if not agency.legal_name:
            legal_name = self.utils.fetch_legal_name(page_text, "Hays", url)
            if legal_name:
                agency.legal_name = legal_name
    
    def _extract_logo(self, soup: BeautifulSoup, url: str) -> str | None:
        """
        Extract logo from header.
        
        Hays has a specific webp logo in the header:
        <header id="banner">
            <a class="logo custom-logo">
                <img alt="Hays Netherlands" src="https://www9.hays.com/UI/storybook/assets/live/img/webp/logo.webp">
            </a>
        </header>
        """
        # Try to find header banner
        header = soup.find("header", id="banner")
        if header:
            # Find the logo link
            logo_link = header.find("a", class_="logo")
            if logo_link:
                # Find the img tag inside
                img = logo_link.find("img")
                if img and img.get("src"):
                    logo_url = img.get("src")
                    # Ensure it's a full URL
                    if logo_url.startswith("http"):
                        # Check if it's a logo file (webp, png, svg, jpg)
                        if any(ext in logo_url.lower() for ext in [".webp", ".png", ".svg", ".jpg", ".jpeg"]):
                            # Avoid non-logo images (search, banner, etc.)
                            if not any(avoid in logo_url.lower() for avoid in ["search", "banner", "hero", "background"]):
                                self.logger.info(f"✓ Found logo (webp): {logo_url} | Source: {url}")
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
        - Candidate portal: login, register, "mijn account", send CV
        - Client portal: register vacancy, recruitment services
        """
        # Find navigation areas
        nav_user = soup.find("nav", id="nav-user")
        nav_main = soup.find("nav", id="nav-main")
        employer_subnav = soup.find("div", id="hays-employer-subNav")
        
        candidate_links = []
        employer_links = []
        
        # Extract candidate portal links from nav-user
        if nav_user:
            for link in nav_user.find_all("a", href=True):
                href = link.get("href", "")
                link_text = link.get_text(strip=True).lower()
                
                # Candidate-specific links
                if any(keyword in href.lower() or keyword in link_text for keyword in [
                    "login", "account", "aanmelden", "register", "mijn-account", "cv"
                ]):
                    full_url = href if href.startswith("http") else f"{self.WEBSITE_URL}{href}"
                    candidate_links.append((full_url, link_text))
                    self.logger.info(f"✓ Found candidate link: {link_text} → {full_url} | Source: {url}")
        

    def _extract_focus_segments(self, text: str) -> list[str]:
        """Extract focus segments from text."""
        segments = []
        text_lower = text.lower()
        
        if any(kw in text_lower for kw in ["professional", "specialist", "white collar"]):
            segments.append("white_collar")
            self.logger.info("✓ Found focus segment: white_collar")
        
        if any(kw in text_lower for kw in ["engineering", "technical", "technology", "it"]):
            segments.append("technisch_specialisten")
            self.logger.info("✓ Found focus segment: technisch_specialisten")
        
        if any(kw in text_lower for kw in ["finance", "accounting"]):
            segments.append("finance_specialists")
            self.logger.info("✓ Found focus segment: finance_specialists")
        
        self.logger.info(f"Total focus segments found: {len(segments)}")
        return segments

    def _extract_regions_served(self, text: str) -> list[str]:
        """Extract regions served from text."""
        regions = []
        text_lower = text.lower()
        
        if "netherlands" in text_lower or "nederland" in text_lower:
            regions.append("landelijk")
            self.logger.info("✓ Found region: landelijk (Netherlands)")
        
        # Note: International countries are filtered out by normalization
        # Only "landelijk", "Randstad", and provinces are allowed
        
        # Normalize regions_served to use only controlled labels
        from staffing_agency_scraper.lib.normalize import normalize_regions_served
        normalized = normalize_regions_served(regions)
        
        self.logger.info(f"Total regions found: {len(normalized)}")
        return normalized


@dg.asset(group_name="agencies")
def hays_scrape() -> dg.Output[dict]:
    """Scrape Hays Netherlands website."""
    scraper = HaysScraper()
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
