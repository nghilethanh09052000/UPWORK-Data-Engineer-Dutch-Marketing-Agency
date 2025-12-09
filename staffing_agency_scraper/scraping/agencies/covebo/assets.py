"""
Covebo Netherlands scraper.

Website: https://www.covebo.nl
Specializes in: Uitzenden, Detachering, Werving & Selectie for Bouw, Techniek, Productie, Logistiek
International workers (Poland, Hungary, Romania, Lithuania, Spain, etc.)
"""

from __future__ import annotations

import re

import dagster as dg
from bs4 import BeautifulSoup

from staffing_agency_scraper.models import (
    Agency,
    AgencyServices,
    AICapabilities,
    CaoType,
    DigitalCapabilities,
    GeoFocusType,
    OfficeLocation,
    VolumeSpecialisation,
)
from staffing_agency_scraper.scraping.base import BaseAgencyScraper


class CoveboScraper(BaseAgencyScraper):
    """Scraper for Covebo Netherlands."""

    AGENCY_NAME = "Covebo"
    WEBSITE_URL = "https://www.covebo.nl"
    BRAND_GROUP = "House of Covebo"

    PAGES_TO_SCRAPE = [
        "https://www.covebo.nl",
        "https://www.covebo.nl/werkgever/",
        "https://www.covebo.nl/werkgever/diensten/",
        "https://www.covebo.nl/over-covebo/",
        "https://www.covebo.nl/over-covebo/ons-verhaal/",
        "https://www.covebo.nl/over-covebo/contact/",
        "https://www.covebo.nl/over-covebo/vestigingen/",
        "https://www.covebo.nl/certificeringen/",
        "https://www.covebo.nl/privacy-statement/",
    ]

    def scrape(self) -> Agency:
        self.logger.info(f"Starting scrape of {self.AGENCY_NAME}")

        agency = self.create_base_agency()
        agency.geo_focus_type = GeoFocusType.NATIONAL
        agency.employers_page_url = f"{self.WEBSITE_URL}/werkgever/"
        agency.contact_form_url = f"{self.WEBSITE_URL}/over-covebo/contact/"

        all_text = ""
        all_sectors = set()

        for url in self.PAGES_TO_SCRAPE:
            try:
                soup = self.fetch_page(url)
                page_text = soup.get_text(separator=" ", strip=True)
                all_text += " " + page_text

                # Extract logo from main page
                if url == self.WEBSITE_URL and not agency.logo_url:
                    agency.logo_url = self._extract_logo(soup)

                # Extract sectors from homepage
                if url == self.WEBSITE_URL:
                    sectors = self._extract_sectors_from_homepage(soup, url)
                    all_sectors.update(sectors)
                    
                    # Extract statistics (reviews, office count)
                    stats = self._extract_statistics(soup, url)
                    if stats.get("review_rating"):
                        agency.review_rating = stats["review_rating"]
                    if stats.get("review_count"):
                        agency.review_count = stats["review_count"]
                    
                    # Extract certifications from homepage
                    certs = self._extract_certifications_from_homepage(soup, url)
                    if certs:
                        agency.certifications = certs

                # Extract services from werkgever/diensten page
                if "werkgever" in url.lower():
                    services = self._extract_services(soup, url)
                    if services.uitzenden:
                        agency.services.uitzenden = True
                    if services.detacheren:
                        agency.services.detacheren = True
                    if services.werving_selectie:
                        agency.services.werving_selectie = True
                    if services.zzp_bemiddeling:
                        agency.services.zzp_bemiddeling = True
                    if services.inhouse_services:
                        agency.services.inhouse_services = True

                # Extract office locations from vestigingen page
                if "vestigingen" in url.lower():
                    locations = self._extract_office_locations(soup, url)
                    if locations:
                        agency.office_locations = locations

                # Extract certifications from certificeringen page
                if "certificeringen" in url.lower():
                    certs = self._extract_certifications_from_page(soup, url)
                    if certs:
                        # Merge with existing certifications
                        existing = set(agency.certifications or [])
                        existing.update(certs)
                        agency.certifications = sorted(list(existing))

                # Extract legal info from privacy policy
                if "privacy" in url.lower():
                    kvk = self._extract_kvk(page_text)
                    if kvk:
                        agency.kvk_number = kvk
                    legal_name = self._extract_legal_name(page_text)
                    if legal_name:
                        agency.legal_name = legal_name

                # Extract contact info
                if "contact" in url.lower():
                    phone = self._extract_phone(page_text)
                    if phone and not agency.contact_phone:
                        agency.contact_phone = phone
                    email = self._extract_email(page_text)
                    if email and not agency.contact_email:
                        agency.contact_email = email

            except Exception as e:
                self.logger.warning(f"Error scraping {url}: {e}")

        # Set sectors_core from combined set
        if all_sectors:
            agency.sectors_core = sorted(list(all_sectors))
            self.logger.info(f"✓ Total unique sectors: {len(agency.sectors_core)}")

        # Derive CAO type and membership from certifications
        if agency.certifications:
            if any("ABU" in cert.upper() for cert in agency.certifications):
                agency.cao_type = CaoType.ABU
                agency.membership = ["ABU"]
                self.logger.info("✓ Found ABU membership from certifications")
            elif any("NBBU" in cert.upper() for cert in agency.certifications):
                agency.cao_type = CaoType.NBBU
                agency.membership = ["NBBU"]

        # Derive regions_served from office locations
        if agency.office_locations and len(agency.office_locations) >= 10:
            agency.regions_served = ["landelijk"]
            self.logger.info("✓ Set regions_served to 'landelijk' (10+ offices)")

        # Extract focus segments from text
        agency.focus_segments = self._extract_focus_segments(all_text)

        # Extract digital capabilities
        agency.digital_capabilities = self._extract_digital_capabilities(all_text)

        # Set volume specialisation based on company stats
        if "8800" in all_text or "massa" in all_text.lower():
            agency.volume_specialisation = VolumeSpecialisation.MASSA_50_PLUS
            self.logger.info("✓ Set volume_specialisation to 'massa_50_plus'")

        # Check for no_cure_no_pay
        if "no cure" in all_text.lower() and "no pay" in all_text.lower():
            agency.no_cure_no_pay = True
            self.logger.info("✓ Found 'no cure, no pay' policy")

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
        self.logger.info(f"  Review Rating: {agency.review_rating or 'NOT FOUND'}")

        self.logger.info(f"Completed scrape of {self.AGENCY_NAME}")
        return agency

    def _extract_logo(self, soup: BeautifulSoup) -> str | None:
        """Extract logo from header."""
        # Covebo has logo in navbar-brand
        logo = soup.select_one(".navbar-brand img.logo-colored, .navbar-brand img")
        if logo:
            src = logo.get("src") or logo.get("data-src")
            if src:
                self.logger.info(f"✓ Found logo: {src}")
                return src
        return None

    def _extract_sectors_from_homepage(self, soup: BeautifulSoup, url: str) -> list[str]:
        """
        Extract sectors from homepage sector cards.
        
        HTML structure:
        <section class="sectors">
            <a class="sector" href="...">
                <h3 class="sector__title">Bouw</h3>
            </a>
        </section>
        """
        sectors = []
        seen = set()
        
        # Find sector titles
        sector_titles = soup.find_all("h3", class_="sector__title")
        
        sector_mapping = {
            "bouw": "bouw_infra",
            "techniek": "techniek",
            "productie": "productie",
            "logistiek": "logistiek",
            "overig": None,  # Skip generic "Overig"
        }
        
        for title in sector_titles:
            text = title.get_text(strip=True).lower()
            if text in sector_mapping and sector_mapping[text] and text not in seen:
                sectors.append(sector_mapping[text])
                seen.add(text)
                self.logger.info(f"✓ Found sector: '{sector_mapping[text]}' | Source: {url}")
        
        return sectors

    def _extract_statistics(self, soup: BeautifulSoup, url: str) -> dict:
        """
        Extract statistics from homepage numbers section.
        
        HTML structure:
        <section class="numbers">
            <span class="counter-section__number" data-countto="40">40</span>
            <h3 class="numbers__title">Vestigingen</h3>
        </section>
        """
        stats = {}
        
        # Find all number cards
        number_cards = soup.find_all("div", class_="numbers__card")
        
        for card in number_cards:
            number_span = card.find("span", class_="counter-section__number")
            title = card.find("h3", class_="numbers__title")
            subtitle = card.find("p", class_="numbers__subtitle")
            
            if number_span and title:
                value = number_span.get("data-countto") or number_span.get_text(strip=True)
                title_text = title.get_text(strip=True).lower()
                subtitle_text = subtitle.get_text(strip=True).lower() if subtitle else ""
                
                # Parse based on title
                if "beoordeling" in title_text:
                    try:
                        rating = float(value)
                        if "medewerkers" in subtitle_text:
                            stats["review_rating"] = rating
                            self.logger.info(f"✓ Found employee rating: {rating} | Source: {url}")
                        elif "klanten" in subtitle_text:
                            # Client rating - could store separately if needed
                            self.logger.info(f"✓ Found client rating: {rating} | Source: {url}")
                    except ValueError:
                        pass
                elif "vestigingen" in title_text:
                    try:
                        stats["office_count"] = int(value)
                        self.logger.info(f"✓ Found office count: {value} | Source: {url}")
                    except ValueError:
                        pass
                elif "mensen" in title_text:
                    try:
                        stats["employees"] = int(value)
                        self.logger.info(f"✓ Found employee count: {value} | Source: {url}")
                    except ValueError:
                        pass
        
        return stats

    def _extract_certifications_from_homepage(self, soup: BeautifulSoup, url: str) -> list[str]:
        """
        Extract certifications from homepage text and logos.
        
        Text mentions: "ABU, SNA, SNF, NEN-4400-1"
        Logo images in .text-media__logo
        """
        certs = []
        seen = set()
        
        # Look for certification text pattern
        text = soup.get_text(separator=" ", strip=True)
        
        # Known certifications to look for
        cert_patterns = [
            (r"\bABU\b", "ABU"),
            (r"\bSNA\b", "SNA"),
            (r"\bSNF\b", "SNF"),
            (r"\bNEN[- ]?4400[- ]?1\b", "NEN-4400-1"),
            (r"\bVCU\b", "VCU"),
            (r"\bISO[- ]?9001\b", "ISO 9001"),
        ]
        
        for pattern, cert_name in cert_patterns:
            if re.search(pattern, text, re.IGNORECASE) and cert_name.lower() not in seen:
                certs.append(cert_name)
                seen.add(cert_name.lower())
                self.logger.info(f"✓ Found certification: '{cert_name}' | Source: {url}")
        
        return certs

    def _extract_certifications_from_page(self, soup: BeautifulSoup, url: str) -> list[str]:
        """Extract certifications from dedicated certifications page."""
        certs = []
        text = soup.get_text(separator=" ", strip=True)
        
        cert_patterns = [
            (r"\bABU\b", "ABU"),
            (r"\bSNA\b", "SNA"),
            (r"\bSNF\b", "SNF"),
            (r"\bNEN[- ]?4400[- ]?1\b", "NEN-4400-1"),
            (r"\bVCU\b", "VCU"),
            (r"\bISO[- ]?9001\b", "ISO 9001"),
        ]
        
        seen = set()
        for pattern, cert_name in cert_patterns:
            if re.search(pattern, text, re.IGNORECASE) and cert_name.lower() not in seen:
                certs.append(cert_name)
                seen.add(cert_name.lower())
                self.logger.info(f"✓ Found certification: '{cert_name}' | Source: {url}")
        
        return certs

    def _extract_services(self, soup: BeautifulSoup, url: str) -> AgencyServices:
        """
        Extract services from page text.
        
        Covebo offers: Uitzenden, Detachering, Werving & Selectie, ZZP
        """
        text_lower = soup.get_text(separator=" ", strip=True).lower()

        uitzenden = False
        detacheren = False
        werving_selectie = False
        zzp_bemiddeling = False
        inhouse_services = False

        if "uitzenden" in text_lower:
            uitzenden = True
            self.logger.info(f"✓ Found service 'uitzenden' | Source: {url}")

        if "detachering" in text_lower or "detacheren" in text_lower:
            detacheren = True
            self.logger.info(f"✓ Found service 'detacheren' | Source: {url}")

        if "werving" in text_lower and "selectie" in text_lower:
            werving_selectie = True
            self.logger.info(f"✓ Found service 'werving_selectie' | Source: {url}")

        if "zzp" in text_lower:
            zzp_bemiddeling = True
            self.logger.info(f"✓ Found service 'zzp_bemiddeling' | Source: {url}")

        if "inhouse" in text_lower or "in-house" in text_lower:
            inhouse_services = True
            self.logger.info(f"✓ Found service 'inhouse_services' | Source: {url}")

        return AgencyServices(
            uitzenden=uitzenden,
            detacheren=detacheren,
            werving_selectie=werving_selectie,
            zzp_bemiddeling=zzp_bemiddeling,
            inhouse_services=inhouse_services,
        )

    def _extract_office_locations(self, soup: BeautifulSoup, url: str) -> list[OfficeLocation]:
        """Extract office locations from vestigingen page."""
        locations = []
        # Will be implemented when we have the actual HTML structure
        # For now, return empty list - can be populated from vestigingen page
        return locations

    def _extract_kvk(self, text: str) -> str | None:
        """Extract KvK number from text."""
        # KvK patterns: "KvK: 12345678" or "KvK-nummer: 12345678"
        kvk_match = re.search(r"KvK[- ]?(?:nummer)?[:\s]*(\d{8})", text, re.IGNORECASE)
        if kvk_match:
            kvk = kvk_match.group(1)
            self.logger.info(f"✓ Found KvK number: {kvk}")
            return kvk
        return None

    def _extract_legal_name(self, text: str) -> str | None:
        """Extract legal name from text."""
        # Look for patterns like "Covebo B.V." or "Covebo Uitzendbureau B.V."
        legal_patterns = [
            r"(Covebo\s+(?:Uitzendbureau\s+)?(?:&\s+Detachering\s+)?B\.?V\.?)",
            r"(Covebo\s+[A-Z][a-z]+\s+B\.?V\.?)",
        ]
        for pattern in legal_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                legal_name = match.group(1).strip()
                self.logger.info(f"✓ Found legal name: {legal_name}")
                return legal_name
        return None

    def _extract_phone(self, text: str) -> str | None:
        """Extract phone number from text."""
        phone_match = re.search(r"(?:\+31|0)[- ]?\d{2,3}[- ]?\d{3}[- ]?\d{2}[- ]?\d{2}", text)
        if phone_match:
            phone = phone_match.group(0)
            self.logger.info(f"✓ Found phone: {phone}")
            return phone
        return None

    def _extract_email(self, text: str) -> str | None:
        """Extract email from text."""
        email_match = re.search(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}", text)
        if email_match:
            email = email_match.group(0)
            # Filter out common non-contact emails
            if not any(x in email.lower() for x in ["noreply", "no-reply", "example", "test"]):
                self.logger.info(f"✓ Found email: {email}")
                return email
        return None

    def _extract_focus_segments(self, text: str) -> list[str]:
        """Extract focus segments from text."""
        segments = []
        text_lower = text.lower()
        
        # Covebo focuses on blue collar / production workers
        if any(kw in text_lower for kw in ["bouw", "techniek", "productie", "logistiek"]):
            segments.append("blue_collar")
            self.logger.info("✓ Found focus segment: blue_collar")
        
        # International workers
        if any(kw in text_lower for kw in ["internationaal", "international", "polen", "hongarije", "roemenië"]):
            segments.append("internationale_medewerkers")
            self.logger.info("✓ Found focus segment: internationale_medewerkers")
        
        return segments

    def _extract_digital_capabilities(self, text: str) -> DigitalCapabilities:
        """Extract digital capabilities from text."""
        text_lower = text.lower()

        candidate_portal = False
        client_portal = False
        mobile_app = False

        # Check for portal
        if "portal" in text_lower or "mijn covebo" in text_lower:
            candidate_portal = True
            self.logger.info("✓ Found digital capability: candidate_portal")

        # Check for client portal
        if "werkgeversportaal" in text_lower or "client portal" in text_lower:
            client_portal = True
            self.logger.info("✓ Found digital capability: client_portal")

        # Check for mobile app (Square App mentioned in FAQ)
        if "square app" in text_lower or "mobiele app" in text_lower or "app store" in text_lower:
            mobile_app = True
            self.logger.info("✓ Found digital capability: mobile_app")

        return DigitalCapabilities(
            candidate_portal=candidate_portal,
            client_portal=client_portal,
            mobile_app=mobile_app,
        )


@dg.asset(group_name="agencies")
def covebo_scrape() -> dg.Output[dict]:
    """Scrape Covebo Netherlands website."""
    scraper = CoveboScraper()
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
