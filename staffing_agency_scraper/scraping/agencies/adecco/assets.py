"""
Adecco Netherlands scraper.

Website: https://www.adecco.nl
Part of: Adecco Group

Extraction logic specific to Adecco's website structure.
"""

from __future__ import annotations

import re
from urllib.parse import urljoin

import dagster as dg
from bs4 import BeautifulSoup

from staffing_agency_scraper.lib.fetch import fetch_with_retry
from staffing_agency_scraper.lib.parse import parse_html
from staffing_agency_scraper.models import Agency, AgencyServices, DigitalCapabilities, GeoFocusType
from staffing_agency_scraper.scraping.base import BaseAgencyScraper


class AdeccoScraper(BaseAgencyScraper):
    """Scraper for Adecco Netherlands."""

    AGENCY_NAME = "Adecco"
    WEBSITE_URL = "https://www.adecco.nl"
    BRAND_GROUP = "Adecco Group"

    # Adecco uses /nl-nl/ path prefix for Dutch content
    # URLs discovered from sitemap analysis
    PAGES_TO_SCRAPE = [
        "https://www.adecco.nl",
        "https://www.adecco.nl/nl-nl/werkgevers",
        "https://www.adecco.nl/nl-nl/over-adecco",
        "https://www.adecco.nl/nl-nl/over-adecco/de-adecco-group",
        "https://www.adecco.nl/nl-nl/contact",
        "https://www.adecco.nl/nl-nl/policy/privacy-policy",
        "https://www.adecco-jobs.com/amazon/en-nl/contact/",  # Has phone & email
    ]

    def scrape(self) -> Agency:
        self.logger.info(f"Starting scrape of {self.AGENCY_NAME}")

        agency = self.create_base_agency()
        agency.geo_focus_type = GeoFocusType.INTERNATIONAL
        agency.employers_page_url = f"{self.WEBSITE_URL}/nl-nl/werkgevers"
        agency.contact_form_url = f"{self.WEBSITE_URL}/nl-nl/contact"

        # Scrape all pages and extract data
        all_text = ""
        for url in self.PAGES_TO_SCRAPE:
            try:
                soup = self.fetch_page(url)
                page_text = soup.get_text(separator=" ", strip=True)
                all_text += " " + page_text

                # Extract logo from homepage
                if url == self.WEBSITE_URL and not agency.logo_url:
                    agency.logo_url = self._extract_logo(soup)

                # Extract phone and email from contact pages
                if "contact" in url.lower():
                    if not agency.contact_phone:
                        agency.contact_phone = self._extract_phone(soup, page_text)
                    if not agency.contact_email:
                        agency.contact_email = self._extract_email(soup, page_text)

                # Extract KvK from legal pages
                if any(p in url.lower() for p in ["privacy", "terms", "policy"]):
                    if not agency.kvk_number:
                        agency.kvk_number = self._extract_kvk(page_text)

            except Exception as e:
                self.logger.warning(f"Error scraping {url}: {e}")

        # Extract all data from accumulated text
        agency.sectors_core = self._extract_sectors(all_text)
        agency.services = self._extract_services(all_text)
        agency.focus_segments = self._extract_focus_segments(all_text)
        agency.regions_served = self._extract_regions(all_text)
        agency.certifications = self._extract_certifications(all_text)
        agency.membership = self._extract_membership(all_text)
        agency.cao_type = self._extract_cao_type(all_text)
        agency.digital_capabilities = self._extract_digital_capabilities(all_text)
        agency.hq_city = self._extract_hq_city(all_text)

        # Update evidence URLs
        agency.evidence_urls = self.evidence_urls.copy()
        agency.collected_at = self.collected_at

        self.logger.info(f"Completed scrape of {self.AGENCY_NAME}")
        return agency

    def _extract_logo(self, soup: BeautifulSoup) -> str | None:
        """Extract logo URL from Adecco's website."""
        # Try OG image first (Adecco uses this)
        og_image = soup.find("meta", attrs={"property": "og:image"})
        if og_image and og_image.get("content"):
            logo_url = og_image.get("content")
            self.logger.info(f"Found logo: {logo_url[:50]}...")
            return logo_url

        # Try header images
        header = soup.find("header")
        if header:
            img = header.find("img")
            if img and img.get("src"):
                src = img.get("src")
                self.logger.info(f"Found logo via header img")
                return self._make_absolute_url(src)

        # Try logo class
        logo = soup.select_one("[class*='logo'] img, img[class*='logo']")
        if logo and logo.get("src"):
            self.logger.info(f"Found logo via class selector")
            return self._make_absolute_url(logo.get("src"))

        return None

    def _extract_phone(self, soup: BeautifulSoup, text: str) -> str | None:
        """Extract phone number - simple regex based."""
        # Dutch phone patterns
        patterns = [
            r"(0\d{2}\s?\d{3,4}\s?\d{3,4})",  # 065 3940431
            r"(\+31\s?\d{1,3}\s?\d{3}\s?\d{4})",  # +31 format
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text)
            if match:
                phone = match.group(1).strip()
                self.logger.info(f"Found phone: {phone}")
                return phone
        
        return None

    def _extract_email(self, soup: BeautifulSoup, text: str) -> str | None:
        """Extract email - simple regex based."""
        # Look for adecco emails
        patterns = [
            r"([\w\.\-]+@adecco\.nl)",
            r"([\w\.\-]+@adecco\.com)",
            r"([\w\.\-]+@adecco[\w\-]*\.[\w]+)",
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                email = match.group(1)
                self.logger.info(f"Found email: {email}")
                return email
        
        return None

    def _extract_kvk(self, text: str) -> str | None:
        """Extract KvK number from text."""
        patterns = [
            r"(?:KvK|KVK|kvk|Kamer van Koophandel)[:\s\-]*(\d{8})",
            r"(?:handelsregister)[:\s\-]*(\d{8})",
        ]

        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                kvk = match.group(1)
                self.logger.info(f"Found KvK: {kvk}")
                return kvk

        return None

    def _extract_sectors(self, text: str) -> list[str]:
        """
        Extract sectors from Adecco's vakgebied section.
        
        Adecco's footer lists these vakgebieden:
        Administratief, Callcenter, Commercieel, Financieel, Horeca, HR, IT,
        Juridisch, Logistiek, Medisch, Productie, Secretarieel, Techniek, Verzekeringen
        """
        sectors = []
        text_lower = text.lower()

        # Map Adecco's vakgebieden to standardized sector names
        # Only include if explicitly mentioned in context of services/vakgebied
        adecco_vakgebieden = {
            "administratief": "administratief",
            "callcenter": "callcenter", 
            "commercieel": "sales",
            "financieel": "finance",
            "horeca": "horeca",
            "hr": "hr",
            "it": "ict",
            "juridisch": "juridisch",
            "logistiek": "logistiek",
            "medisch": "zorg",
            "productie": "productie",
            "secretarieel": "secretarieel",
            "techniek": "techniek",
            "verzekeringen": "verzekeringen",
        }

        # Check for VAKGEBIED section which lists their actual sectors
        import re
        vakgebied_match = re.search(r'VAKGEBIED(.{0,600})', text, re.IGNORECASE | re.DOTALL)
        if vakgebied_match:
            vakgebied_text = vakgebied_match.group(1).lower()
            for adecco_term, standard_sector in adecco_vakgebieden.items():
                if adecco_term in vakgebied_text:
                    sectors.append(standard_sector)
        
        # If VAKGEBIED not found, fall back to broader matching
        if not sectors:
            for adecco_term, standard_sector in adecco_vakgebieden.items():
                if adecco_term in text_lower:
                    sectors.append(standard_sector)

        return list(set(sectors))

    def _extract_services(self, text: str) -> AgencyServices:
        """
        Extract services offered by Adecco.
        
        Be precise: only mark true if the service term appears in a services context,
        not just anywhere on the page.
        """
        text_lower = text.lower()
        
        # Check for diensten/services context
        import re
        diensten_match = re.search(r'diensten(.{0,1500})', text_lower, re.DOTALL)
        diensten_context = diensten_match.group(1) if diensten_match else text_lower

        return AgencyServices(
            # Core staffing services - look for Dutch terms specifically
            uitzenden=any(w in diensten_context for w in ["uitzenden", "uitzendwerk", "flexibel personeel", "tijdelijke krachten"]),
            detacheren=any(w in diensten_context for w in ["detacheren", "detachering"]),
            werving_selectie=any(w in diensten_context for w in ["werving en selectie", "werving & selectie", "recruitment"]),
            payrolling="payroll" in diensten_context,
            
            # Specialized services - be strict
            zzp_bemiddeling=any(w in diensten_context for w in ["zzp bemiddeling", "freelance bemiddeling"]),
            vacaturebemiddeling_only=False,
            inhouse_services=any(w in diensten_context for w in ["inhouse", "in-house", "on-site services"]),
            msp=any(w in diensten_context for w in ["managed service provider", "msp diensten"]),
            rpo=any(w in diensten_context for w in ["recruitment process outsourcing", "rpo diensten"]),
            executive_search="executive search" in diensten_context,
            
            # Training/development - be strict
            opleiden_ontwikkelen=any(w in text_lower for w in ["opleiden en ontwikkelen", "training en ontwikkeling", "adecco academy"]),
            reintegratie_outplacement=any(w in diensten_context for w in ["reïntegratie", "outplacement"]),
        )

    def _extract_focus_segments(self, text: str) -> list[str]:
        """
        Extract focus segments from text.
        
        Derive from the vakgebieden found - be precise about what Adecco actually offers.
        """
        segments = []
        text_lower = text.lower()
        
        # Check for VAKGEBIED to determine actual focus
        import re
        vakgebied_match = re.search(r'VAKGEBIED(.{0,600})', text, re.IGNORECASE | re.DOTALL)
        vakgebied_text = vakgebied_match.group(1).lower() if vakgebied_match else text_lower

        # Blue collar indicators (productie, logistiek)
        if any(w in vakgebied_text for w in ["productie", "logistiek"]):
            segments.append("blue_collar")
        
        # White collar indicators (administratief, hr, finance, juridisch)
        if any(w in vakgebied_text for w in ["administratief", "financieel", "hr", "juridisch", "secretarieel"]):
            segments.append("white_collar")
        
        # Technical specialists
        if "techniek" in vakgebied_text or "it" in vakgebied_text:
            segments.append("technisch_specialisten")
        
        # Healthcare
        if "medisch" in vakgebied_text or "zorg" in text_lower:
            segments.append("zorgprofessionals")
        
        # Students - only if explicitly mentioned
        if any(w in text_lower for w in ["studentenwerk", "bijbaan", "studenten vacatures"]):
            segments.append("studenten")
        
        # Young professionals - only if explicitly mentioned
        if any(w in text_lower for w in ["young professional", "traineeship", "starter"]):
            segments.append("young_professionals")

        return list(set(segments))

    def _extract_regions(self, text: str) -> list[str]:
        """
        Extract regions served.
        
        Adecco is part of The Adecco Group - world's largest HR solutions company.
        They have offices across Netherlands and operate internationally.
        """
        regions = []
        text_lower = text.lower()

        # Check for national coverage - Adecco has offices in many Dutch cities
        # From footer: Amsterdam, Arnhem, Den Bosch, Den Haag, Eindhoven, etc.
        dutch_cities = ["amsterdam", "rotterdam", "den haag", "eindhoven", "utrecht", 
                        "groningen", "arnhem", "den bosch", "tilburg", "zwolle"]
        cities_found = sum(1 for city in dutch_cities if city in text_lower)
        
        if cities_found >= 3 or "heel nederland" in text_lower or "landelijk" in text_lower:
            regions.append("landelijk")
        
        # Adecco Group is international
        if any(w in text_lower for w in ["adecco group", "worldwide", "global", "landen"]):
            regions.append("internationaal")

        return regions

    def _extract_certifications(self, text: str) -> list[str]:
        """Extract certifications."""
        certs = set()
        text_lower = text.lower()

        cert_patterns = {
            "NEN-4400-1": ["nen-4400", "nen 4400"],
            "SNA": ["sna-keurmerk", "sna keurmerk", "stichting normering arbeid"],
            "VCU": ["vcu", "veiligheid gezondheid welzijn"],
            "ISO9001": ["iso 9001", "iso9001"],
            "ISO27001": ["iso 27001", "iso27001"],
        }

        for cert, keywords in cert_patterns.items():
            if any(kw in text_lower for kw in keywords):
                certs.add(cert)

        return list(certs)

    def _extract_membership(self, text: str) -> list[str]:
        """Extract membership/branche organizations."""
        memberships = set()
        text_lower = text.lower()

        if "abu" in text_lower or "algemene bond uitzendondernemingen" in text_lower:
            memberships.add("ABU")
        if "nbbu" in text_lower:
            memberships.add("NBBU")

        return list(memberships)

    def _extract_cao_type(self, text: str) -> str:
        """Extract CAO type."""
        text_lower = text.lower()

        if "abu" in text_lower and "cao" in text_lower:
            return "ABU"
        if "nbbu" in text_lower and "cao" in text_lower:
            return "NBBU"
        if "abu" in text_lower:
            return "ABU"
        if "nbbu" in text_lower:
            return "NBBU"

        return "onbekend"

    def _extract_digital_capabilities(self, text: str, soup: BeautifulSoup = None) -> DigitalCapabilities:
        """
        Extract digital capabilities.
        
        Be precise - only mark true if there's clear evidence of the capability.
        """
        text_lower = text.lower()

        # Check for actual portal mentions (not just generic text)
        has_client_portal = any(w in text_lower for w in [
            "werkgeversportaal", "client portal", "klantenportaal", "inloggen werkgever"
        ])
        
        # Adecco has "Mijn Adecco" for candidates
        has_candidate_portal = any(w in text_lower for w in [
            "mijn adecco", "mijn vacatures", "inloggen kandidaat", "mijn account"
        ])
        
        # Check for app store links in HTML if soup provided
        has_app = any(w in text_lower for w in ["app store", "google play", "download app"])
        
        return DigitalCapabilities(
            client_portal=has_client_portal,
            candidate_portal=has_candidate_portal,
            mobile_app=has_app,
            api_available=False,  # No evidence of public API
            realtime_vacancy_feed=False,  # Would need specific evidence
            realtime_availability_feed=False,
            self_service_contracting=False,
        )

    def _extract_hq_city(self, text: str) -> str | None:
        """Extract HQ city from text."""
        # Look for address patterns with Dutch cities
        patterns = [
            r"(?:hoofdkantoor|head office|kantoor)[:\s]+[^,]*,?\s*(\d{4}\s*[A-Z]{2})\s+([A-Za-z\s\-]+)",
            r"(?:gevestigd|located)[:\s]+(?:in|te)\s+([A-Za-z\s\-]+)",
        ]

        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                city = match.group(2 if len(match.groups()) > 1 else 1).strip()
                self.logger.info(f"Found HQ city: {city}")
                return city

        return None

    def _make_absolute_url(self, url: str) -> str:
        """Convert relative URL to absolute."""
        if not url:
            return url
        if url.startswith("//"):
            return f"https:{url}"
        elif url.startswith("/"):
            return urljoin(self.WEBSITE_URL, url)
        elif not url.startswith("http"):
            return urljoin(self.WEBSITE_URL, url)
        return url


@dg.asset(group_name="agencies")
def adecco_scrape() -> dg.Output[dict]:
    """Scrape Adecco Netherlands website."""
    scraper = AdeccoScraper()
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
