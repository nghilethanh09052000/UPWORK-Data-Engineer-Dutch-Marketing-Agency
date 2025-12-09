"""
Hays Netherlands scraper.

Website: https://www.hays.nl
Specializes in: Professional recruitment, RPO, MSP
International staffing company with 50+ years experience globally
"""

from __future__ import annotations

import re

import dagster as dg
from bs4 import BeautifulSoup
from typing import Any, Dict, List, Set

from staffing_agency_scraper.lib.fetch import fetch_with_retry
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


class HaysScraper(BaseAgencyScraper):
    """Scraper for Hays Netherlands."""

    AGENCY_NAME = "Hays Nederland"
    WEBSITE_URL = "https://www.hays.nl"
    BRAND_GROUP = "Hays plc"
    SITEMAP_URL = "https://www.hays.nl/sitemap.xml"

    # Each page has a name, url, and list of extraction functions to apply
    PAGES_TO_SCRAPE: List[Dict[str, Any]] = [
        {
            "name": "home_nl",
            "url": "https://www.hays.nl/home",
            "functions": ["logo", "sectors", "services", "chatbot"],
        },
        {
            "name": "contact_nl",
            "url": "https://www.hays.nl/contact",
            "functions": ["contact"],
        },
        {
            "name": "services_nl",
            "url": "https://www.hays.nl/al-onze-diensten",
            "functions": ["services"],
        },
        {
            "name": "about_nl",
            "url": "https://www.hays.nl/over-hays",
            "functions": ["logo", "sectors"],
        },
        {
            "name": "privacy_nl",
            "url": "https://www.hays.nl/herzien-privacybeleid",
            "functions": ["legal"],
        },
        {
            "name": "detaching_nl",
            "url": "https://www.hays.nl/detachering",
            "functions": ["services"],
        },
    ]

    def scrape(self) -> Agency:
        self.logger.info(f"Starting scrape of {self.AGENCY_NAME}")

        agency = self.create_base_agency()
        agency.geo_focus_type = GeoFocusType.INTERNATIONAL
        agency.employers_page_url = f"{self.WEBSITE_URL}/recruitment/contacteer-ons"
        agency.contact_form_url = f"{self.WEBSITE_URL}/contact"

        all_text = ""
        all_sectors: Set[str] = set()
        has_chatbot = False

        # Fetch sitemap upstream and prepare nl-only URLs
        sitemap_urls = self.get_sitemap_urls()
        nl_sitemap_urls = self._filter_nl_urls(sitemap_urls)
        base_urls = {p["url"] for p in self.PAGES_TO_SCRAPE}

        for page in self.PAGES_TO_SCRAPE:
            url = page["url"]
            try:
                soup = self.fetch_page(url)
                page_text = soup.get_text(separator=" ", strip=True)
                all_text += " " + page_text
                functions = set(page.get("functions", []))

                # Extract logo
                if "logo" in functions and not agency.logo_url:
                    agency.logo_url = self._extract_logo(soup)

                # Extract specialisms/sectors
                if "sectors" in functions:
                    sectors = self._extract_specialisms(soup, url)
                    all_sectors.update(sectors)

                # Extract services
                if "services" in functions:
                    services = self._extract_services(soup, url)
                    if services.detacheren:
                        agency.services.detacheren = True
                    if services.werving_selectie:
                        agency.services.werving_selectie = True
                    if services.msp:
                        agency.services.msp = True
                    if services.rpo:
                        agency.services.rpo = True
                    if services.executive_search:
                        agency.services.executive_search = True
                    if services.zzp_bemiddeling:
                        agency.services.zzp_bemiddeling = True

                # Chatbot detection
                if "chatbot" in functions:
                    has_chatbot = has_chatbot or self._check_chatbot(soup, page_text)

                # Extract legal info from privacy page
                if "legal" in functions:
                    kvk = self._extract_kvk(page_text)
                    if kvk:
                        agency.kvk_number = kvk
                    legal_name = self._extract_legal_name(page_text)
                    if legal_name:
                        agency.legal_name = legal_name

                # Extract contact info
                if "contact" in functions:
                    contact_info = self._extract_contact_info(soup, url)
                    if contact_info.get("offices"):
                        agency.office_locations = contact_info["offices"]
                    if contact_info.get("phone") and not agency.contact_phone:
                        agency.contact_phone = contact_info["phone"]
                    if contact_info.get("email") and not agency.contact_email:
                        agency.contact_email = contact_info["email"]
                    if contact_info.get("hq_city") and not agency.hq_city:
                        agency.hq_city = contact_info["hq_city"]
                    if contact_info.get("hq_province") and not agency.hq_province:
                        agency.hq_province = contact_info["hq_province"]

            except Exception as e:
                self.logger.warning(f"Error scraping {url}: {e}")

        # If data is still too sparse, leverage sitemap URLs (nl only) as fallback
        if self._needs_more_data(agency):
            extra_urls = [u for u in nl_sitemap_urls if u not in base_urls]
            self._crawl_additional_pages(agency, extra_urls, all_sectors)

        # Set sectors_core from extracted specialisms (remove duplicates)
        if all_sectors:
            # Clean up duplicates by normalizing and keeping original names
            unique_sectors = []
            seen_normalized = set()
            for sector in sorted(all_sectors):
                # Normalize for comparison (lowercase, remove spaces)
                normalized = sector.lower().replace(" ", "").replace("(", "").replace(")", "")
                # Handle specific duplicates
                if "informationtechnology" in normalized or normalized == "it":
                    normalized = "it"
                if "logistics" in normalized or "logistiek" in normalized:
                    normalized = "logistics"
                
                if normalized not in seen_normalized:
                    unique_sectors.append(sector)
                    seen_normalized.add(normalized)
            
            agency.sectors_core = unique_sectors
            self.logger.info(f"✓ Total unique sectors: {len(agency.sectors_core)}")

        # Extract company statistics from text
        self._extract_company_stats(agency, all_text)

        # Extract digital capabilities
        agency.digital_capabilities = self._extract_digital_capabilities(all_text)

        # Extract AI capabilities (chatbot detection)
        agency.ai_capabilities = self._extract_ai_capabilities(all_text, has_chatbot)

        # Derive focus segments and role levels from text
        agency.focus_segments = self._extract_focus_segments(all_text)
        agency.role_levels = self._extract_role_levels(all_text)

        # Derive regions served from text
        agency.regions_served = self._extract_regions_served(all_text)

        # Derive membership and CAO from text
        self._extract_membership_and_cao(agency, all_text)

        # Set volume specialisation based on company size
        if "1,000 vacancies" in all_text or "80 consultants" in all_text:
            agency.volume_specialisation = VolumeSpecialisation.POOLS_5_50
            self.logger.info("✓ Set volume_specialisation to 'pools_5_50'")

        # Update evidence URLs
        agency.evidence_urls = self.evidence_urls.copy()
        agency.collected_at = self.collected_at

        # Log extraction summary
        self.logger.info(f"--- Extraction Summary for {self.AGENCY_NAME} ---")
        self.logger.info(f"  Legal Name: {agency.legal_name or 'NOT FOUND'}")
        self.logger.info(f"  KvK: {agency.kvk_number or 'NOT FOUND'}")
        self.logger.info(f"  Logo: {'YES' if agency.logo_url else 'NOT FOUND'}")
        self.logger.info(f"  Sectors: {len(agency.sectors_core or [])} found")
        self.logger.info(f"  Services RPO: {agency.services.rpo}")
        self.logger.info(f"  Services MSP: {agency.services.msp}")
        self.logger.info(f"  Chatbot: {agency.ai_capabilities.chatbot_for_candidates}")

        self.logger.info(f"Completed scrape of {self.AGENCY_NAME}")
        return agency

    # ---------------------------
    # Upstream: sitemap handling
    # ---------------------------
    def get_sitemap_urls(self) -> List[str]:
        """Fetch and parse sitemap URLs (upstream helper)."""
        try:
            resp = fetch_with_retry(self.SITEMAP_URL)
            locs = re.findall(r"<loc>(.*?)</loc>", resp.text)
            urls = [u.strip() for u in locs if u.strip()]
            self.logger.info(f"✓ Fetched {len(urls)} URLs from sitemap")
            return urls
        except Exception as e:
            self.logger.warning(f"Failed to fetch sitemap: {e}")
            return []

    def _filter_nl_urls(self, urls: List[str]) -> List[str]:
        """Keep only NL paths (exclude /en/ or other languages)."""
        filtered = []
        for u in urls:
            if not u.startswith(self.WEBSITE_URL):
                continue
            path = u[len(self.WEBSITE_URL):]
            if "/en/" in path or path.startswith("/en"):
                continue
            filtered.append(u)
        self.logger.info(f"✓ Filtered to {len(filtered)} NL URLs from sitemap")
        return filtered

    # ---------------------------
    # Fallback crawling for missing data
    # ---------------------------
    def _needs_more_data(self, agency: Agency) -> bool:
        """Check if key fields are missing and we should try extra pages."""
        if not agency.sectors_core:
            return True
        if not agency.office_locations:
            return True
        if not agency.contact_phone or not agency.contact_email:
            return True
        if not agency.logo_url:
            return True
        # If all services are False, try more
        services = agency.services
        if not any(
            [
                services.uitzenden,
                services.detacheren,
                services.werving_selectie,
                services.zzp_bemiddeling,
                services.msp,
                services.rpo,
                services.executive_search,
            ]
        ):
            return True
        return False

    def _crawl_additional_pages(
        self, agency: Agency, urls: List[str], all_sectors: Set[str], max_pages: int = 15
    ) -> None:
        """Use extra sitemap URLs to fill missing fields (lightweight fallback)."""
        tried = 0
        for url in urls:
            if tried >= max_pages:
                break
            tried += 1
            try:
                soup = self.fetch_page(url)
                page_text = soup.get_text(separator=" ", strip=True)

                # Sectors
                if not agency.sectors_core:
                    sectors = self._extract_specialisms(soup, url)
                    all_sectors.update(sectors)

                # Services
                if not any(
                    [
                        agency.services.uitzenden,
                        agency.services.detacheren,
                        agency.services.werving_selectie,
                        agency.services.zzp_bemiddeling,
                        agency.services.msp,
                        agency.services.rpo,
                        agency.services.executive_search,
                    ]
                ):
                    services = self._extract_services(soup, url)
                    if services.detacheren:
                        agency.services.detacheren = True
                    if services.werving_selectie:
                        agency.services.werving_selectie = True
                    if services.msp:
                        agency.services.msp = True
                    if services.rpo:
                        agency.services.rpo = True
                    if services.executive_search:
                        agency.services.executive_search = True
                    if services.zzp_bemiddeling:
                        agency.services.zzp_bemiddeling = True

                # Contact / offices
                if not agency.contact_phone or not agency.contact_email or not agency.office_locations:
                    if "contact" in url or "office" in url or "kantoor" in url:
                        contact_info = self._extract_contact_info(soup, url)
                        if contact_info.get("offices"):
                            agency.office_locations = contact_info["offices"]
                        if contact_info.get("phone") and not agency.contact_phone:
                            agency.contact_phone = contact_info["phone"]
                        if contact_info.get("email") and not agency.contact_email:
                            agency.contact_email = contact_info["email"]
                        if contact_info.get("hq_city") and not agency.hq_city:
                            agency.hq_city = contact_info["hq_city"]
                        if contact_info.get("hq_province") and not agency.hq_province:
                            agency.hq_province = contact_info["hq_province"]

                # Legal
                if not agency.kvk_number or not agency.legal_name:
                    kvk = self._extract_kvk(page_text)
                    if kvk and not agency.kvk_number:
                        agency.kvk_number = kvk
                    legal_name = self._extract_legal_name(page_text)
                    if legal_name and not agency.legal_name:
                        agency.legal_name = legal_name

                # AI / chatbot
                if not agency.ai_capabilities.chatbot_for_candidates:
                    has_chatbot = self._check_chatbot(soup, page_text)
                    if has_chatbot:
                        agency.ai_capabilities.chatbot_for_candidates = True
                        agency.ai_capabilities.chatbot_for_clients = True

                # Digital capabilities
                if not agency.digital_capabilities.candidate_portal:
                    agency.digital_capabilities = self._extract_digital_capabilities(page_text)

                if not self._needs_more_data(agency):
                    break

            except Exception as e:
                self.logger.warning(f"Fallback crawl error on {url}: {e}")

    def _extract_logo(self, soup: BeautifulSoup) -> str | None:
        """Extract logo from header."""
        # Hays has logo in header
        logo = soup.select_one("header img, .logo img, a.logo img")
        if logo:
            src = logo.get("src") or logo.get("data-src")
            if src:
                self.logger.info(f"✓ Found logo: {src}")
                return src
        
        # Also check for known Hays logo URL pattern
        for img in soup.find_all("img"):
            src = img.get("src", "")
            if "logo" in src.lower() and "hays" in src.lower():
                self.logger.info(f"✓ Found logo: {src}")
                return src
        
        return None

    def _extract_specialisms(self, soup: BeautifulSoup, url: str) -> list[str]:
        """
        Extract specialisms/sectors from homepage "Onze specialismes" section.
        
        HTML structure:
        <div class="hays-col hays-col-4">
            <h2>Onze specialismes</h2>
            <ul>
                <li><a href="...">Accounting & Finance</a></li>
                ...
            </ul>
        </div>
        """
        sectors = []
        seen = set()
        
        # Find the "Onze specialismes" or "Our specialisms" heading
        specialisms_heading = soup.find("h2", string=re.compile(r"(Onze specialismes|Our specialisms)", re.IGNORECASE))
        
        if specialisms_heading:
            # Navigate to the parent container
            parent_box = specialisms_heading.find_parent("div", class_="box")
            
            if parent_box:
                # Find all list items with links
                list_items = parent_box.find_all("li")
                
                for li in list_items:
                    # Get the link text (specialism name)
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
                specialism_text = link.get_text(strip=True)
                if specialism_text and specialism_text.lower() not in seen:
                    # Clean up the text (remove arrows, etc.)
                    specialism_text = specialism_text.replace("►", "").strip()
                    if specialism_text:
                        sectors.append(specialism_text)
                        seen.add(specialism_text.lower())
                        self.logger.info(f"✓ Found sector: '{specialism_text}' | Source: {url}")
        
        return sectors

    def _extract_services(self, soup: BeautifulSoup, url: str) -> AgencyServices:
        """
        Extract services from "All our services" / "Al onze diensten" section.
        
        HTML structure:
        <div class="hays-col hays-col-4">
            <h2>All our services</h2>
            <ul>
                <li>► Perm recruitment, flex recruitment, contracting recruitment</li>
                <li>► Recruitment Process Outsourcing (RPO)</li>
                <li>► Managed Service Provider (MSP)</li>
                ...
            </ul>
        </div>
        """
        text_lower = soup.get_text(separator=" ", strip=True).lower()

        detacheren = False
        uitzenden = False
        werving_selectie = False
        msp = False
        rpo = False
        executive_search = False
        zzp_bemiddeling = False
        payrolling = False

        # Find services section by heading
        services_heading = soup.find("h2", string=re.compile(r"(All our services|Al onze diensten)", re.IGNORECASE))
        
        if services_heading:
            parent_box = services_heading.find_parent("div", class_="box")
            if parent_box:
                services_text = parent_box.get_text(separator=" ", strip=True).lower()
                
                # Map services from HTML
                if "perm recruitment" in services_text or "permanente recruitment" in services_text:
                    werving_selectie = True
                    self.logger.info(f"✓ Found service 'werving_selectie' (perm recruitment) | Source: {url}")
                
                if "flex recruitment" in services_text or "flexibele inhuur" in services_text:
                    detacheren = True
                    uitzenden = True
                    self.logger.info(f"✓ Found service 'detacheren' (flex recruitment) | Source: {url}")
                
                if "contracting" in services_text or "freelancer" in services_text:
                    zzp_bemiddeling = True
                    self.logger.info(f"✓ Found service 'zzp_bemiddeling' (contracting) | Source: {url}")
                
                if "recruitment process outsourcing" in services_text or "rpo" in services_text:
                    rpo = True
                    self.logger.info(f"✓ Found service 'rpo' | Source: {url}")
                
                if "managed service provider" in services_text or "msp" in services_text:
                    msp = True
                    self.logger.info(f"✓ Found service 'msp' | Source: {url}")
        
        # Fallback to general text search
        if not any([detacheren, werving_selectie, msp, rpo, zzp_bemiddeling]):
            if "perm recruitment" in text_lower or "permanent recruitment" in text_lower:
                werving_selectie = True
                self.logger.info(f"✓ Found service 'werving_selectie' | Source: {url}")

            if "flex recruitment" in text_lower or "temporary" in text_lower:
                detacheren = True
                self.logger.info(f"✓ Found service 'detacheren' | Source: {url}")

            if "contracting" in text_lower or "contractor" in text_lower:
                zzp_bemiddeling = True
                self.logger.info(f"✓ Found service 'zzp_bemiddeling' | Source: {url}")

            if "recruitment process outsourcing" in text_lower or "rpo" in text_lower:
                rpo = True
                self.logger.info(f"✓ Found service 'rpo' | Source: {url}")

            if "managed service provider" in text_lower or "msp" in text_lower:
                msp = True
                self.logger.info(f"✓ Found service 'msp' | Source: {url}")

        if "executive search" in text_lower:
            executive_search = True
            self.logger.info(f"✓ Found service 'executive_search' | Source: {url}")

        return AgencyServices(
            uitzenden=uitzenden,
            detacheren=detacheren,
            werving_selectie=werving_selectie,
            msp=msp,
            rpo=rpo,
            executive_search=executive_search,
            zzp_bemiddeling=zzp_bemiddeling,
            payrolling=payrolling,
        )

    def _extract_contact_info(self, soup: BeautifulSoup, url: str) -> dict:
        """
        Extract contact info from contact page.
        
        HTML structure:
        <h2>Kantoren</h2>
        <table>
            <tr>
                <td>
                    <h3>Amsterdam</h3>
                    <p>Rijnsburgstraat 9-11<br>1059 AT Amsterdam</p>
                    <p>Telefoonnummer<br>020 36 30 310</p>
                    <p>E-mailadres<br>info@hays.nl</p>
                </td>
                <td>
                    <h3>Tilburg</h3>
                    ...
                </td>
            </tr>
        </table>
        """
        result = {
            "offices": [],
            "phone": None,
            "email": None,
            "hq_city": None,
            "hq_province": None,
        }
        
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
            # Look for the table or container after heading
            table = offices_heading.find_next("table")
            
            if table:
                # Find all h3 elements (office names)
                office_headers = table.find_all("h3")
                
                for header in office_headers:
                    city_name = header.get_text(strip=True)
                    if city_name and city_name.strip():
                        # Clean up city name
                        city_name = city_name.replace("\xa0", " ").strip()
                        if city_name:
                            province = city_province_map.get(city_name.lower(), None)
                            office = OfficeLocation(city=city_name, province=province)
                            result["offices"].append(office)
                            self.logger.info(f"✓ Found office: {city_name} ({province}) | Source: {url}")
                            
                            # First office is typically HQ (Amsterdam)
                            if not result["hq_city"]:
                                result["hq_city"] = city_name
                                result["hq_province"] = province
                                self.logger.info(f"✓ Set HQ: {city_name}")
                
                # Extract phone and email from first office
                first_cell = table.find("td")
                if first_cell:
                    cell_text = first_cell.get_text(separator=" ", strip=True)
                    
                    # Extract phone
                    phone_match = re.search(r"(\d{3}[\s-]?\d{2}[\s-]?\d{2}[\s-]?\d{3})", cell_text)
                    if phone_match:
                        result["phone"] = phone_match.group(1).replace(" ", " ")
                        self.logger.info(f"✓ Found phone: {result['phone']} | Source: {url}")
                    
                    # Extract email
                    email_match = re.search(r"([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})", cell_text)
                    if email_match:
                        result["email"] = email_match.group(1)
                        self.logger.info(f"✓ Found email: {result['email']} | Source: {url}")
        
        # Fallback: search for email and phone in general text
        if not result["email"]:
            page_text = soup.get_text(separator=" ", strip=True)
            email_match = re.search(r"info@hays\.nl", page_text, re.IGNORECASE)
            if email_match:
                result["email"] = "info@hays.nl"
                self.logger.info(f"✓ Found email: info@hays.nl | Source: {url}")
        
        return result

    def _extract_kvk(self, text: str) -> str | None:
        """Extract KvK number from text."""
        kvk_match = re.search(r"KvK[- ]?(?:nummer)?[:\s]*(\d{8})", text, re.IGNORECASE)
        if kvk_match:
            kvk = kvk_match.group(1)
            self.logger.info(f"✓ Found KvK number: {kvk}")
            return kvk
        return None

    def _extract_legal_name(self, text: str) -> str | None:
        """Extract legal name from text."""
        # Look for patterns like "Hays B.V." or "Hays Specialist Recruitment B.V."
        legal_patterns = [
            r"(Hays\s+(?:Specialist\s+)?(?:Recruitment\s+)?(?:Netherlands\s+)?B\.?V\.?)",
            r"(Hays\s+[A-Z][a-z]+\s+B\.?V\.?)",
        ]
        for pattern in legal_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                legal_name = match.group(1).strip()
                self.logger.info(f"✓ Found legal name: {legal_name}")
                return legal_name
        return None

    def _check_chatbot(self, soup: BeautifulSoup, text: str) -> bool:
        """Check for chatbot presence (Zendesk, etc.)."""
        # Look for Zendesk script
        scripts = soup.find_all("script")
        for script in scripts:
            script_text = script.get_text() if script.string else str(script)
            if "zendesk" in script_text.lower() or "zdassets" in script_text.lower():
                self.logger.info("✓ Found Zendesk chatbot")
                return True
        
        # Check text for chatbot indicators
        if "live chat" in text.lower() or "chatbot" in text.lower():
            self.logger.info("✓ Found chatbot indicator in text")
            return True
        
        return False

    def _extract_company_stats(self, agency: Agency, text: str) -> None:
        """Extract company statistics from text."""
        # Look for years of experience
        if "50 years" in text.lower() or "50+ years" in text.lower():
            self.logger.info("✓ Found: 50+ years experience globally")
        
        if "35 years" in text.lower():
            self.logger.info("✓ Found: 35+ years in Netherlands")

        # Look for consultant count
        consultant_match = re.search(r"(\d+)\s*consultants?", text.lower())
        if consultant_match:
            count = consultant_match.group(1)
            self.logger.info(f"✓ Found: {count} consultants")

    def _extract_digital_capabilities(self, text: str) -> DigitalCapabilities:
        """Extract digital capabilities from text."""
        text_lower = text.lower()

        candidate_portal = False
        client_portal = False
        mobile_app = False

        # Check for portals
        if "sign in" in text_lower or "my account" in text_lower or "create an account" in text_lower:
            candidate_portal = True
            self.logger.info("✓ Found digital capability: candidate_portal")

        if "client portal" in text_lower or "employer portal" in text_lower:
            client_portal = True
            self.logger.info("✓ Found digital capability: client_portal")

        if "app store" in text_lower or "mobile app" in text_lower:
            mobile_app = True
            self.logger.info("✓ Found digital capability: mobile_app")

        return DigitalCapabilities(
            candidate_portal=candidate_portal,
            client_portal=client_portal,
            mobile_app=mobile_app,
        )

    def _extract_ai_capabilities(self, text: str, has_chatbot: bool = False) -> AICapabilities:
        """Extract AI capabilities from text."""
        chatbot_for_candidates = False
        chatbot_for_clients = False

        if has_chatbot:
            chatbot_for_candidates = True
            chatbot_for_clients = True
            self.logger.info("✓ Found AI capability: chatbot_for_candidates")
            self.logger.info("✓ Found AI capability: chatbot_for_clients")

        return AICapabilities(
            chatbot_for_candidates=chatbot_for_candidates,
            chatbot_for_clients=chatbot_for_clients,
        )

    def _extract_focus_segments(self, text: str) -> list[str]:
        """Extract focus segments from text."""
        segments = []
        text_lower = text.lower()
        
        # Hays focuses on professional / white collar
        if any(kw in text_lower for kw in ["professional", "specialist", "white collar"]):
            segments.append("white_collar")
            self.logger.info("✓ Found focus segment: white_collar")
        
        # Technical specialists
        if any(kw in text_lower for kw in ["engineering", "technical", "technology", "it"]):
            segments.append("technisch_specialisten")
            self.logger.info("✓ Found focus segment: technisch_specialisten")
        
        # Finance specialists
        if any(kw in text_lower for kw in ["finance", "accounting"]):
            segments.append("finance_specialists")
            self.logger.info("✓ Found focus segment: finance_specialists")
        
        return segments

    def _extract_role_levels(self, text: str) -> list[str]:
        """Extract role levels from text."""
        levels = []
        text_lower = text.lower()
        
        if "senior" in text_lower:
            levels.append("senior")
            self.logger.info("✓ Found role level: senior")
        
        if "medior" in text_lower or "mid-level" in text_lower:
            levels.append("medior")
            self.logger.info("✓ Found role level: medior")
        
        if "junior" in text_lower:
            levels.append("junior")
            self.logger.info("✓ Found role level: junior")
        
        if "executive" in text_lower or "management" in text_lower:
            levels.append("executive")
            self.logger.info("✓ Found role level: executive")
        
        # Default to medior/senior if nothing found (typical for professional recruitment)
        if not levels:
            levels = ["medior", "senior"]
            self.logger.info("✓ Default role levels: medior, senior")
        
        return levels

    def _extract_regions_served(self, text: str) -> list[str]:
        """Extract regions served from text."""
        regions = []
        text_lower = text.lower()
        
        # Check for national coverage
        if "netherlands" in text_lower or "nederland" in text_lower:
            regions.append("landelijk")
            self.logger.info("✓ Found region: landelijk")
        
        # Check for international
        if "worldwide" in text_lower or "international" in text_lower or "global" in text_lower:
            regions.append("internationaal")
            self.logger.info("✓ Found region: internationaal")
        
        return regions

    def _extract_membership_and_cao(self, agency: Agency, text: str) -> None:
        """Extract membership and CAO type from text."""
        text_lower = text.lower()
        
        # Check for ABU membership
        if "abu" in text_lower:
            agency.membership = ["ABU"]
            agency.cao_type = CaoType.ABU
            self.logger.info("✓ Found ABU membership")
        elif "nbbu" in text_lower:
            agency.membership = ["NBBU"]
            agency.cao_type = CaoType.NBBU
            self.logger.info("✓ Found NBBU membership")
        else:
            # Hays is typically ABU member
            agency.membership = ["ABU"]
            agency.cao_type = CaoType.ABU
            self.logger.info("✓ Default: ABU membership (typical for major agencies)")


@dg.asset(group_name="agencies")
def hays_sitemap() -> List[str]:
    """Upstream asset: fetch sitemap URLs for Hays (nl-only filtered downstream)."""
    scraper = HaysScraper()
    return scraper.get_sitemap_urls()


@dg.asset(group_name="agencies")
def hays_scrape(hays_sitemap: List[str]) -> dg.Output[dict]:
    """Scrape Hays Netherlands website."""
    scraper = HaysScraper()
    # We pass sitemap into scraper via instance variable for potential future use
    # (currently sitemap is fetched again inside scrape for freshness).
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
