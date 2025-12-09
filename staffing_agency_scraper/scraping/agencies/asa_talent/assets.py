"""
ASA Talent Netherlands scraper.

Website: https://asatalent.nl
Part of: RGF Staffing

ASA Talent is "hét uitzendbureau voor studenten, starters en professionals".
"""

from __future__ import annotations

import re

import requests
import dagster as dg
from bs4 import BeautifulSoup

from staffing_agency_scraper.lib.fetch import get_chrome_user_agent
from staffing_agency_scraper.lib.dutch import DUTCH_POSTAL_TO_PROVINCE
from staffing_agency_scraper.models import Agency, AgencyServices, DigitalCapabilities, GeoFocusType, OfficeLocation
from staffing_agency_scraper.scraping.base import BaseAgencyScraper


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
        "https://asatalent.nl/werkgevers/diensten",
        "https://asatalent.nl/vestigingen",  # Office locations
        "https://asatalent.nl/over-ons/keurmerken-en-certificaten",
        "https://asatalent.nl/legal/privacy-statement",
        "https://asatalent.nl/legal/disclaimer",
    ]
    

    def scrape(self) -> Agency:
        self.logger.info(f"Starting scrape of {self.AGENCY_NAME}")

        agency = self.create_base_agency()
        agency.geo_focus_type = GeoFocusType.NATIONAL
        agency.employers_page_url = f"{self.WEBSITE_URL}/werkgevers"
        agency.contact_form_url = f"{self.WEBSITE_URL}/contact"

        all_text = ""
        
        for url in self.PAGES_TO_SCRAPE:
            try:
                soup = self.fetch_page(url)
                page_text = soup.get_text(separator=" ", strip=True)
                all_text += " " + page_text

                # Extract logo from main page
                if url == self.WEBSITE_URL and not agency.logo_url:
                    agency.logo_url = self._extract_logo(soup)

                # Extract office locations from vestigingen page
                if url == "https://asatalent.nl/vestigingen":
                    locations, main_phone, main_email = self._extract_office_locations_from_html(soup)
                    if locations:
                        agency.office_locations = locations
                    if main_phone and not agency.contact_phone:
                        agency.contact_phone = main_phone
                    if main_email and not agency.contact_email:
                        agency.contact_email = main_email

                # Extract certifications from keurmerken page
                if "keurmerken" in url.lower():
                    agency.certifications = self._extract_certifications(page_text)
                
                # Extract legal name from disclaimer
                if "disclaimer" in url.lower():
                    agency.legal_name = self._extract_legal_name(page_text)

            except Exception as e:
                self.logger.warning(f"Error scraping {url}: {e}")
        
        # Set HQ to Utrecht (main office)
        if not agency.hq_city:
            agency.hq_city = "Utrecht"
            agency.hq_province = "Utrecht"

        # Extract sectors from vakgebieden links
        agency.sectors_core = self._extract_sectors(all_text)
        
        # Extract services
        agency.services = self._extract_services(all_text)
        
        # Extract focus segments (students, starters, professionals)
        agency.focus_segments = self._extract_focus_segments(all_text)
        
        # Known facts from website
        agency.membership = ["ABU"]
        agency.cao_type = "ABU"
        agency.regions_served = ["landelijk"]
        
        # Digital capabilities
        agency.digital_capabilities = DigitalCapabilities(
            candidate_portal=True,  # "Mijn ASA" portal
            client_portal=False,
            mobile_app=False,
            api_available=False,
            realtime_vacancy_feed=False,
            realtime_availability_feed=False,
            self_service_contracting=False,
        )

        # Update evidence URLs
        agency.evidence_urls = self.evidence_urls.copy()
        agency.collected_at = self.collected_at

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

    def _extract_sectors(self, text: str) -> list[str]:
        """Extract sectors (vakgebieden) from text."""
        sectors = []
        text_lower = text.lower()
        
        sector_map = {
            "bezorging": "logistiek",
            "administratief": "administratief",
            "klantenservice": "callcenter",
            "horeca": "horeca",
            "logistiek": "logistiek",
            "retail": "retail",
            "zorg": "zorg",
            "financieel": "finance",
        }
        
        for keyword, sector in sector_map.items():
            if keyword in text_lower and sector not in sectors:
                sectors.append(sector)
        
        return sectors

    def _extract_services(self, text: str) -> AgencyServices:
        """Extract services from text."""
        text_lower = text.lower()
        
        return AgencyServices(
            uitzenden="uitzenden" in text_lower,
            detacheren="detacheren" in text_lower or "detachering" in text_lower,
            werving_selectie="werving en selectie" in text_lower or "werving & selectie" in text_lower,
            payrolling="payroll" in text_lower,
            zzp_bemiddeling="freelance" in text_lower or "zzp" in text_lower,
            vacaturebemiddeling_only=False,
            inhouse_services="flexpool" in text_lower,
            msp=False,
            rpo=False,
            executive_search=False,
            opleiden_ontwikkelen="training" in text_lower or "opleiden" in text_lower,
            reintegratie_outplacement=False,
        )

    def _extract_focus_segments(self, text: str) -> list[str]:
        """
        Extract focus segments.
        
        ASA targets: students, starters, professionals
        """
        segments = []
        text_lower = text.lower()
        
        if "student" in text_lower:
            segments.append("studenten")
        if "starter" in text_lower:
            segments.append("starters")
        if "professional" in text_lower:
            segments.append("young_professionals")
        if "bijbaan" in text_lower or "flexibel" in text_lower:
            segments.append("studenten")
        
        return list(set(segments))


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
