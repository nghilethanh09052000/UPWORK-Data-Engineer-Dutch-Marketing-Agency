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
        "https://asatalent.nl/vestigingen",
        "https://asatalent.nl/over-ons/keurmerken-en-certificaten",
        "https://asatalent.nl/legal/privacy-statement",
        "https://asatalent.nl/legal/disclaimer",
        "https://asatalent.nl/sitemap-asatalent",
    ]
    
    # Individual vestiging pages for contact details
    VESTIGING_PAGES = [
        "https://asatalent.nl/vestigingen/asa-amsterdam-uitzendbureau",
        "https://asatalent.nl/vestigingen/deventer-uitzendbureau",
        "https://asatalent.nl/vestigingen/enschede-uitzendbureau",
        "https://asatalent.nl/vestigingen/groningen-uitzendbureau",
        "https://asatalent.nl/vestigingen/maastricht-uitzendbureau",
        "https://asatalent.nl/vestigingen/nijmegen-uitzendbureau",
        "https://asatalent.nl/vestigingen/utrecht-uitzendbureau",
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
                if "/vestigingen" in url.lower() and "uitzendbureau" not in url:
                    locations = self._extract_office_locations(page_text)
                    if locations:
                        agency.office_locations = locations

                # Extract certifications from keurmerken page
                if "keurmerken" in url.lower():
                    agency.certifications = self._extract_certifications(page_text)
                
                # Extract legal name from disclaimer
                if "disclaimer" in url.lower():
                    agency.legal_name = self._extract_legal_name(page_text)

            except Exception as e:
                self.logger.warning(f"Error scraping {url}: {e}")
        
        # Fetch individual vestiging pages for contact details
        vestiging_contacts = self._fetch_vestiging_contacts()
        if vestiging_contacts:
            # Use Utrecht as main contact (largest/most central office)
            utrecht = vestiging_contacts.get("utrecht")
            if utrecht:
                agency.contact_phone = utrecht.get("phone")
                agency.contact_email = utrecht.get("email")
                self.logger.info(f"Set main contact: {agency.contact_phone}, {agency.contact_email}")
            
            # Set HQ to Utrecht
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

    def _fetch_vestiging_contacts(self) -> dict:
        """
        Fetch contact details from individual vestiging pages.
        
        Returns dict with city -> {phone, email} mapping.
        """
        contacts = {}
        
        for url in self.VESTIGING_PAGES:
            try:
                soup = self.fetch_page(url)
                raw_html = str(soup)
                text = soup.get_text(separator=" ", strip=True)
                
                # Extract city from URL
                city = url.split("/")[-1].replace("-uitzendbureau", "").replace("asa-", "").lower()
                
                # Find email (mailto or text pattern)
                email = None
                mailto_match = re.search(r'mailto:([^"?]+@asatalent\.nl)', raw_html)
                if mailto_match:
                    email = mailto_match.group(1)
                else:
                    email_match = re.search(r"([\w\.\-]+@asatalent\.nl)", text)
                    if email_match:
                        email = email_match.group(1)
                
                # Find phone number
                phone = None
                phone_match = re.search(r"(\d{3}[-\s]?\d{3}[-\s]?\d{4}|\d{3}[-\s]?\d{7})", text)
                if phone_match:
                    phone = phone_match.group(1)
                
                if email or phone:
                    contacts[city] = {"email": email, "phone": phone}
                    self.logger.info(f"Found contact for {city}: {phone}, {email}")
                    
            except Exception as e:
                self.logger.warning(f"Error fetching vestiging {url}: {e}")
        
        return contacts

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

    def _extract_office_locations(self, text: str) -> list[OfficeLocation]:
        """
        Extract office locations from vestigingen page.
        
        Pattern: "1105 AG Amsterdam" -> Amsterdam, Noord-Holland
        """
        locations = []
        seen_cities = set()
        
        # Find all postal code + city patterns
        matches = re.findall(r"(\d{4})\s*([A-Z]{2})\s+([A-Za-z\-]+)", text)
        
        for postal_code, postal_letters, city in matches:
            city_normalized = city.title()
            if city_normalized.lower() not in seen_cities:
                # Get province from postal code prefix
                postal_prefix = postal_code[:2]
                province = DUTCH_POSTAL_TO_PROVINCE.get(postal_prefix)
                
                locations.append(OfficeLocation(city=city_normalized, province=province))
                seen_cities.add(city_normalized.lower())
                self.logger.info(f"Found office location: {city_normalized}, {province}")
        
        return locations

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
