"""
Michael Page Netherlands scraper.

Website: https://www.michaelpage.nl
Part of: PageGroup (KvK: 33194106)
Listed: London Stock Exchange since April 2001
"""

from __future__ import annotations

from typing import Any, Dict, List

import dagster as dg
from bs4 import BeautifulSoup

from staffing_agency_scraper.models import Agency, GeoFocusType, OfficeLocation
from staffing_agency_scraper.scraping.base import BaseAgencyScraper
from staffing_agency_scraper.scraping.utils import AgencyScraperUtils


class MichaelPageScraper(BaseAgencyScraper):
    """Scraper for Michael Page Netherlands."""

    AGENCY_NAME = "Michael Page"
    WEBSITE_URL = "https://www.michaelpage.nl"
    BRAND_GROUP = "PageGroup"
    
    PAGES_TO_SCRAPE: List[Dict[str, Any]] = [
        {
            "name": "home",
            "url": "https://www.michaelpage.nl",
            "functions": ["logo", "services", "legal", "header"],
        },
        {
            "name": "about",
            "url": "https://www.michaelpage.nl/over-ons",
            "functions": ["sectors", "about", "legal"],
        },
        {
            "name": "contact",
            "url": "https://www.michaelpage.nl/contact",
            "functions": ["contact", "offices"],
        },
        {
            "name": "privacy",
            "url": "https://www.michaelpage.nl/privacy-beleid",
            "functions": ["legal", "contact"],
        },
    ]

    def scrape(self) -> Agency:
        self.logger.info(f"Starting scrape of {self.AGENCY_NAME}")
        
        # Note: self.utils is initialized in BaseAgencyScraper.__init__()
        agency = self.create_base_agency()
        agency.geo_focus_type = GeoFocusType.INTERNATIONAL
        agency.employers_page_url = f"{self.WEBSITE_URL}/werkgevers"
        agency.contact_form_url = f"{self.WEBSITE_URL}/contact"
        
        # Known facts
        agency.regions_served = ["landelijk", "internationaal"]
        
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
        
        # Extract from aggregated text (merge with existing)
        cert_from_utils = self.utils.fetch_certifications(all_text, "accumulated_text")
        if cert_from_utils:
            if not agency.certifications:
                agency.certifications = []
            agency.certifications.extend(cert_from_utils)
            agency.certifications = list(set(agency.certifications))  # Remove duplicates
        
        agency.cao_type = self.utils.fetch_cao_type(all_text, "accumulated_text")
        agency.membership = self.utils.fetch_membership(all_text, "accumulated_text")
        
        # Extract ALL common fields using base class utility method
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
            
            elif func_name == "services":
                self._extract_services(page_text, agency, url)
            
            elif func_name == "sectors":
                sectors = self._extract_sectors(soup, page_text, url)
                if sectors:
                    if not agency.sectors_core:
                        agency.sectors_core = []
                    agency.sectors_core.extend(sectors)
                    agency.sectors_core = list(set(agency.sectors_core))
            
            elif func_name == "about":
                self._extract_about(page_text, agency, url)
            
            elif func_name == "contact":
                self._extract_contact(soup, page_text, agency, url)
            
            elif func_name == "offices":
                offices = self._extract_offices(soup, url)
                if offices:
                    agency.office_locations = offices
                    if not agency.hq_city and offices:
                        agency.hq_city = offices[0].city
                        agency.hq_province = offices[0].province
            
            elif func_name == "legal":
                self._extract_legal(soup, page_text, agency, url)
            
            elif func_name == "header":
                self._extract_header(soup, agency, url)
    
    def _extract_logo(self, soup: BeautifulSoup, url: str) -> str | None:
        """
        Extract logo URL from header.
        
        Logo is in header: <img src="/themes/custom/mp_theme/logo.png" alt="Michael Page...">
        """
        # Try to find the specific logo in header
        header = soup.find("header", class_="content-header")
        if header:
            logo_img = header.find("img", alt=lambda x: x and "michael page" in x.lower())
            if logo_img and logo_img.get("src"):
                logo_url = logo_img["src"]
                # Make absolute URL
                if logo_url.startswith("/"):
                    logo_url = f"{self.WEBSITE_URL}{logo_url}"
                self.logger.info(f"✓ Found logo: {logo_url} | Source: {url}")
                return logo_url
        
        # Fallback to utils method
        logo_url = self.utils.fetch_logo(soup, url)
        if logo_url:
            self.logger.info(f"✓ Found logo (fallback): {logo_url} | Source: {url}")
        return logo_url
    
    def _extract_services(self, page_text: str, agency: Agency, url: str) -> None:
        """
        Extract services from page.
        
        Michael Page offers:
        - Werving & Selectie (Recruitment & Selection)
        - Interim Management / Temporary placement
        - Page Executive (Executive Search)
        """
        text_lower = page_text.lower()
        
        # Werving & selectie
        if "werving" in text_lower or ("recruitment" in text_lower and "selection" in text_lower):
            agency.services.werving_selectie = True
            self.logger.info(f"✓ Found service: werving_selectie | Source: {url}")
        
        # Interim/Temporary
        if "interim" in text_lower or "temporary" in text_lower or "tijdelijk" in text_lower:
            agency.services.detacheren = True
            self.logger.info(f"✓ Found service: detacheren (Interim) | Source: {url}")
        
        # Executive Search (Page Executive)
        if "executive" in text_lower or "page executive" in text_lower:
            agency.services.executive_search = True
            self.logger.info(f"✓ Found service: executive_search (Page Executive) | Source: {url}")
    
    def _extract_sectors(self, soup: BeautifulSoup, page_text: str, url: str) -> list[str]:
        """
        Extract the 15 core specializations from the about page.
        
        Michael Page specializes in:
        1. Banking & Financial Services
        2. Digital
        3. Engineering & Manufacturing
        4. Finance
        5. Healthcare & Life Sciences
        6. Human Resources
        7. Information Technology
        8. Interim Management
        9. Legal
        10. Procurement & Supply Chain
        11. Property & Constructions
        12. Risk, Compliance & Internal Audit
        13. Sales & Marketing
        14. Tax
        15. Page Executive
        """
        sectors = []
        text_lower = page_text.lower()
        
        # Map Michael Page specializations to our sector taxonomy
        sector_mapping = {
            "finance": ["banking & financial services", "banking", "financial services", "finance", "tax"],
            "it": ["information technology", "digital"],
            "techniek": ["engineering & manufacturing", "engineering"],
            "zorg": ["healthcare & life sciences", "healthcare", "life sciences"],
            "hr": ["human resources"],
            "juridisch": ["legal"],
            "logistiek": ["procurement & supply chain", "supply chain"],
            "bouw": ["property & constructions", "property", "construction"],
            "compliance": ["risk, compliance & internal audit", "risk", "compliance", "internal audit"],
            "marketing": ["sales & marketing", "sales", "marketing"],
            "management": ["interim management", "executive", "page executive"],
        }
        
        for sector, keywords in sector_mapping.items():
            if any(keyword in text_lower for keyword in keywords):
                if sector not in sectors:
                    sectors.append(sector)
                    self.logger.info(f"✓ Found core sector: {sector} | Source: {url}")
        
        return sectors
    
    def _extract_about(self, page_text: str, agency: Agency, url: str) -> None:
        """Extract information from the about page."""
        text_lower = page_text.lower()
        
        # Check for global presence
        if "150" in page_text and ("offices" in text_lower or "vestigingen" in text_lower):
            self.logger.info(f"✓ Found: 150+ offices globally | Source: {url}")
        
        # Check for London Stock Exchange listing
        if "london stock exchange" in text_lower:
            self.logger.info(f"✓ Found: Listed on London Stock Exchange | Source: {url}")
    
    def _extract_contact(self, soup: BeautifulSoup, page_text: str, agency: Agency, url: str) -> None:
        """
        Extract contact information.
        
        Main contact phone should be Amsterdam office (HQ): +31 205789444
        """
        if not agency.contact_email:
            agency.contact_email = self.utils.fetch_contact_email(page_text, url)
        
        if not agency.contact_phone:
            # Look for Amsterdam phone specifically (HQ)
            if "205789444" in page_text or "+31 205789444" in page_text:
                agency.contact_phone = "+31 205789444"
                self.logger.info(f"✓ Found contact phone: {agency.contact_phone} (Amsterdam HQ) | Source: {url}")
            else:
                agency.contact_phone = self.utils.fetch_contact_phone(page_text, url)
    
    def _extract_offices(self, soup: BeautifulSoup, url: str) -> list[OfficeLocation]:
        """
        Extract office locations with phone numbers.
        
        From contact page:
        - Amsterdam: +31 205789444 (HQ - World Trade Centre)
        - Rotterdam: +31 102176565
        - Utrecht: +31 307999040
        - Tilburg: +31 137999200
        
        HTML structure:
        <li>
          <a href="/contact/amsterdam">
            <div class="office_location">
              <h3>Amsterdam</h3>
              <div class="label-text"><span>t: </span>+31 205789444</div>
            </div>
          </a>
        </li>
        """
        offices = []
        
        # Look for office list with class "office_list"
        office_list = soup.find("div", class_="office_list")
        if office_list:
            for li in office_list.find_all("li"):
                # Find city name (h3)
                city_elem = li.find("h3")
                # Find phone number (label-text)
                phone_elem = li.find("div", class_="label-text")
                
                if city_elem:
                    city_name = city_elem.get_text(strip=True)
                    phone = phone_elem.get_text(strip=True).replace("t: ", "").strip() if phone_elem else None
                    
                    # Map city to province
                    city_province_map = {
                        "Amsterdam": "Noord-Holland",
                        "Rotterdam": "Zuid-Holland",
                        "Utrecht": "Utrecht",
                        "Tilburg": "Noord-Brabant",
                    }
                    
                    province = city_province_map.get(city_name)
                    if province:
                        office = OfficeLocation(city=city_name, province=province)
                        offices.append(office)
                        phone_info = f" (Phone: {phone})" if phone else ""
                        self.logger.info(f"✓ Found office: {city_name}, {province}{phone_info} | Source: {url}")
        
        # Fallback: Look for office links in navigation if no structured list found
        if not offices:
            for link in soup.find_all("a", href=True):
                href = link.get("href", "").lower()
                text = link.get_text(strip=True)
                
                # Check for office pages
                if "amsterdam" in href or "amsterdam" in text.lower():
                    if not any(o.city == "Amsterdam" for o in offices):
                        offices.append(OfficeLocation(city="Amsterdam", province="Noord-Holland"))
                        self.logger.info(f"✓ Found office: Amsterdam, Noord-Holland | Source: {url}")
                
                elif "utrecht" in href or "utrecht" in text.lower():
                    if not any(o.city == "Utrecht" for o in offices):
                        offices.append(OfficeLocation(city="Utrecht", province="Utrecht"))
                        self.logger.info(f"✓ Found office: Utrecht, Utrecht | Source: {url}")
                
                elif "rotterdam" in href or "rotterdam" in text.lower():
                    if not any(o.city == "Rotterdam" for o in offices):
                        offices.append(OfficeLocation(city="Rotterdam", province="Zuid-Holland"))
                        self.logger.info(f"✓ Found office: Rotterdam, Zuid-Holland | Source: {url}")
                
                elif "tilburg" in href or "tilburg" in text.lower():
                    if not any(o.city == "Tilburg" for o in offices):
                        offices.append(OfficeLocation(city="Tilburg", province="Noord-Brabant"))
                        self.logger.info(f"✓ Found office: Tilburg, Noord-Brabant | Source: {url}")
        
        return offices
    
    def _extract_legal(self, soup: BeautifulSoup, page_text: str, agency: Agency, url: str) -> None:
        """
        Extract KvK and legal name.
        
        PageGroup KvK: 33194106
        Address: World Trade Centre, Tower 10, 9th floor, Strawinskylaan 959, Amsterdam, 1077 XX
        """
        # KvK number - look for PageGroup KvK: 33194106
        if not agency.kvk_number:
            if "33194106" in page_text:
                agency.kvk_number = "33194106"
                self.logger.info(f"✓ Found KvK number: 33194106 (PageGroup) | Source: {url}")
            else:
                kvk = self.utils.fetch_kvk_number(page_text, url)
                if kvk:
                    agency.kvk_number = kvk
        
        # Legal name
        if not agency.legal_name:
            if "michael page international (netherlands)" in page_text.lower():
                agency.legal_name = "Michael Page International (Netherlands)"
                self.logger.info(f"✓ Found legal name: {agency.legal_name} | Source: {url}")
            else:
                legal_name = self.utils.fetch_legal_name(page_text, "Michael Page", url)
                if legal_name:
                    agency.legal_name = legal_name
        
        # HQ Address (Amsterdam - World Trade Centre)
        if not agency.hq_city:
            if "strawinskylaan" in page_text.lower() or "world trade centre" in page_text.lower():
                agency.hq_city = "Amsterdam"
                agency.hq_province = "Noord-Holland"
                self.logger.info(f"✓ Found HQ: Amsterdam (World Trade Centre) | Source: {url}")
    
    def _extract_header(self, soup: BeautifulSoup, agency: Agency, url: str) -> None:
        """
        Extract information from the header.
        
        Header contains:
        - Candidate portal link: /mypage
        - Saved jobs: /mypage/saved-jobs
        - Job search functionality
        """
        header = soup.find("header", class_="content-header")
        if not header:
            return
        
        # Look for "mypage" links (candidate portal)
        mypage_links = soup.find_all("a", href=lambda x: x and "mypage" in x.lower())
        if mypage_links:
            agency.digital_capabilities.candidate_portal = True
            self.logger.info(f"✓ Detected candidate_portal from header: /mypage | Source: {url}")
        
        # Check for saved jobs feature (indicates candidate portal)
        if "saved-jobs" in str(soup).lower():
            agency.digital_capabilities.candidate_portal = True
            self.logger.info(f"✓ Detected saved jobs feature (candidate portal) | Source: {url}")
        
        # Mobile app detection from footer
        footer = soup.find("footer", id="footer")
        if footer:
            # Check for app store links
            app_store_link = footer.find("a", href=lambda x: x and "apps.apple.com" in x)
            google_play_link = footer.find("a", href=lambda x: x and "play.google.com" in x)
            
            if app_store_link or google_play_link:
                agency.digital_capabilities.mobile_app = True
                self.logger.info(f"✓ Detected mobile_app: iOS + Android apps available | Source: {url}")
            
            # Check for Google reviews
            if "richplugins" in str(footer).lower() or "google rating" in footer.get_text().lower():
                if not agency.review_sources:
                    agency.review_sources = []
                if "google" not in agency.review_sources:
                    agency.review_sources.append("google")
                    self.logger.info(f"✓ Found review source: google | Source: {url}")
            
            # Check for Top Employer badge
            if "top employer" in footer.get_text().lower() or "top_employer" in str(footer).lower():
                if not agency.certifications:
                    agency.certifications = []
                if "Top_Employer" not in agency.certifications:
                    agency.certifications.append("Top_Employer")
                    self.logger.info(f"✓ Found certification: Top_Employer | Source: {url}")
            
            # Check for ISO27001
            if "iso-27001" in str(footer).lower() or "iso27001" in footer.get_text().lower():
                if not agency.certifications:
                    agency.certifications = []
                if "ISO_27001" not in agency.certifications:
                    agency.certifications.append("ISO_27001")
                    self.logger.info(f"✓ Found certification: ISO_27001 | Source: {url}")


@dg.asset(group_name="agencies")
def michael_page_scrape() -> dg.Output[dict]:
    """Scrape Michael Page website."""
    scraper = MichaelPageScraper()
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

