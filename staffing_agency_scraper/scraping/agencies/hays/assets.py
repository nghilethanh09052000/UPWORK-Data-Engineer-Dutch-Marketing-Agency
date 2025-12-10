"""
Hays Netherlands scraper.

Website: https://www.hays.nl
Specializes in: Professional recruitment, RPO, MSP
International staffing company with 50+ years experience globally
"""

from __future__ import annotations

import re
import asyncio

import dagster as dg
from bs4 import BeautifulSoup
from typing import Any, Dict, List, Set

from crawl4ai import AsyncWebCrawler, CrawlerRunConfig, LLMConfig
from crawl4ai.extraction_strategy import LLMExtractionStrategy
from pydantic import BaseModel, Field
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
from staffing_agency_scraper.scraping.utils import AgencyScraperUtils


class HaysScraper(BaseAgencyScraper):
    """Scraper for Hays Netherlands."""

    AGENCY_NAME = "Hays Nederland"
    WEBSITE_URL = "https://www.hays.nl"
    BRAND_GROUP = "Hays plc"
    
    # 🔥 LLM Configuration
    # Set to True to use Ollama LLM for intelligent extraction (slower, uses CPU/GPU)
    # Set to False to use only crawl4ai for clean HTML (faster, cooler MacBook!)
    USE_LLM = False  # Default: False (crawl4ai only, no Ollama)
    
    # Each page has a name, url, list of extraction functions (HTML/bs4 only),
    # and whether to use AI (crawl4ai) for secondary extraction.
    PAGES_TO_SCRAPE: List[Dict[str, Any]] = [
        {
            "name": "home_nl",
            "url": "https://www.hays.nl/home",
            "functions": ["logo", "sectors", "services"],  # Unique to this page
            "use_ai": True,
        },
        {
            "name": "contact_nl",
            "url": "https://www.hays.nl/contact",
            "functions": ["contact"],  # Unique to this page
            "use_ai": True,
        },
        {
            "name": "privacy_nl",
            "url": "https://www.hays.nl/herzien-privacybeleid",
            "functions": ["legal"],  # Unique to this page
            "use_ai": True,
        },
        {
            "name": "services_nl",
            "url": "https://www.hays.nl/al-onze-diensten",
            "functions": [],  # No specific functions, just AI
            "use_ai": True,
        },
        {
            "name": "about_nl",
            "url": "https://www.hays.nl/over-hays",
            "functions": [],  # No specific functions, just AI
            "use_ai": True,
        },
        {
            "name": "detaching_nl",
            "url": "https://www.hays.nl/detachering",
            "functions": [],  # No specific functions, just AI
            "use_ai": True,
        },
    ]

    def scrape(self) -> Agency:
        self.logger.info(f"Starting scrape of {self.AGENCY_NAME}")
        
        
        # Initialize utils
        self.utils = AgencyScraperUtils(logger=self.logger, use_llm=self.USE_LLM)
        # Initialize utils helper
        agency = self.create_base_agency()
        agency.geo_focus_type = GeoFocusType.INTERNATIONAL
        agency.employers_page_url = f"{self.WEBSITE_URL}/recruitment/contacteer-ons"
        agency.contact_form_url = f"{self.WEBSITE_URL}/contact"

        all_text = ""
        all_sectors: Set[str] = set()

        for page in self.PAGES_TO_SCRAPE:
            url = page["url"]
            page_name = page["name"]
            functions = page.get("functions", [])
            use_ai = page.get("use_ai", False)
            
            try:
                self.logger.info("=" * 80)
                self.logger.info(f"📄 PROCESSING: {page_name}")
                self.logger.info(f"🔗 URL: {url}")
                self.logger.info(f"⚙️  Functions: {functions if functions else 'None (AI only)'}")
                self.logger.info(f"🤖 Use AI: {use_ai}")
                self.logger.info("-" * 80)
                
                # Step 1: Normal fetch with BeautifulSoup
                self.logger.info("📥 STEP 1: Fetching with BeautifulSoup...")
                soup = self.fetch_page(url)
                page_text = soup.get_text(separator=" ", strip=True)
                all_text += " " + page_text
                self.logger.info(f"✓ Page fetched ({len(page_text)} characters)")
                
                # Step 2: Call specific functions from the functions array
                if functions:
                    self.logger.info(f"🔧 STEP 2: Applying {len(functions)} normal functions...")
                    self._apply_functions_normal(
                        agency=agency,
                        functions=functions,
                        soup=soup,
                        page_text=page_text,
                        all_sectors=all_sectors,
                        url=url,
                    )
                else:
                    self.logger.info("⏭️  STEP 2: No normal functions to apply")

                # Step 3: If use_ai=True, fetch with crawl4ai and call ALL AI extraction functions
                if use_ai:
                    self.logger.info("🤖 STEP 3: AI extraction (crawl4ai)...")
                    ai_content = self.utils.fetch_page_ai(url)
                    if ai_content:
                        all_text += " " + ai_content
                        self.logger.info(f"✓ AI content fetched ({len(ai_content)} characters)")
                    self._apply_functions_ai(
                        agency=agency,
                        content=ai_content,
                        all_sectors=all_sectors,
                        url=url,
                    )
                else:
                    self.logger.info("⏭️  STEP 3: AI extraction disabled for this page")
                
                self.logger.info(f"✅ Completed: {page_name}")

            except Exception as e:
                self.logger.error(f"❌ Error scraping {url}: {e}")

        # Clean up and deduplicate sectors
        if all_sectors:
            unique_sectors = []
            seen_normalized = set()
            for sector in sorted(all_sectors):
                normalized = sector.lower().replace(" ", "").replace("(", "").replace(")", "")
                if "informationtechnology" in normalized or normalized == "it":
                    normalized = "it"
                if "logistics" in normalized or "logistiek" in normalized:
                    normalized = "logistics"
                
                if normalized not in seen_normalized:
                    unique_sectors.append(sector)
                    seen_normalized.add(normalized)
            
            agency.sectors_core = unique_sectors
            self.logger.info(f"✓ Total unique sectors: {len(agency.sectors_core)}")

        # Extract from aggregated text
        agency.focus_segments = self._extract_focus_segments(all_text)
        agency.role_levels = self._extract_role_levels(all_text)
        agency.regions_served = self._extract_regions_served(all_text)
        
        # Set volume specialisation based on company size
        if "1,000 vacancies" in all_text or "80 consultants" in all_text:
            agency.volume_specialisation = VolumeSpecialisation.POOLS_5_50
            self.logger.info("✓ Set volume_specialisation to 'pools_5_50'")

        # Update evidence URLs
        agency.evidence_urls = list(self.evidence_urls)
        agency.collected_at = self.collected_at

        # Final summary
        self.logger.info("=" * 80)
        self.logger.info(f"📊 EXTRACTION SUMMARY - {self.AGENCY_NAME}")
        self.logger.info("=" * 80)
        
        # Basic info
        self.logger.info(f"✓ Legal name: {agency.legal_name or '❌ NOT FOUND'}")
        self.logger.info(f"✓ KvK: {agency.kvk_number or '❌ NOT FOUND'}")
        self.logger.info(f"✓ Logo: {agency.logo_url or '❌ NOT FOUND'}")
        
        # Contact
        self.logger.info(f"✓ Email: {agency.contact_email or '❌ NOT FOUND'}")
        self.logger.info(f"✓ Phone: {agency.contact_phone or '❌ NOT FOUND'}")
        self.logger.info(f"✓ HQ: {agency.hq_city or '❌ NOT FOUND'}")
        self.logger.info(f"✓ Offices: {len(agency.office_locations)} found")
        
        # Core data
        self.logger.info(f"✓ Sectors: {len(agency.sectors_core or [])} found")
        self.logger.info(f"✓ Services - RPO: {agency.services.rpo}, MSP: {agency.services.msp}")
        
        # AI-extracted fields
        self.logger.info(f"✓ Digital portal: {agency.digital_capabilities.candidate_portal}")
        self.logger.info(f"✓ Mobile app: {agency.digital_capabilities.mobile_app}")
        self.logger.info(f"✓ Chatbot: {agency.ai_capabilities.chatbot_for_candidates}")
        self.logger.info(f"✓ Membership: {agency.membership or '❌ NOT FOUND'}")
        self.logger.info(f"✓ CAO: {agency.cao_type}")
        self.logger.info(f"✓ Phase system: {agency.phase_system or '❌ NOT FOUND'}")
        self.logger.info(f"✓ Pricing transparency: {agency.pricing_transparency or '❌ NOT FOUND'}")
        self.logger.info(f"✓ Hourly rates: €{agency.avg_hourly_rate_low or '?'} - €{agency.avg_hourly_rate_high or '?'}")
        self.logger.info(f"✓ Reviews: {agency.review_rating or '❌ NOT FOUND'} ({agency.review_count or 0} reviews)")
        self.logger.info(f"✓ Certifications: {agency.certifications or '❌ NOT FOUND'}")
        
        self.logger.info("=" * 80)
        self.logger.info(f"✅ Completed scrape of {self.AGENCY_NAME}")
        self.logger.info(f"📄 Evidence URLs: {len(agency.evidence_urls)}")
        self.logger.info("=" * 80)
        
        return agency

    def _apply_functions_normal(
        self,
        agency: Agency,
        functions: List[str],
        soup: BeautifulSoup,
        page_text: str,
        all_sectors: Set[str],
        url: str,
    ) -> None:
        """
        Call specific fetch functions based on the functions array.
        Each function extracts specific data using BeautifulSoup.
        """
        for func_name in functions:
            if func_name == "logo":
                self.utils.fetch_logo(agency, soup, url)
            
            elif func_name == "sectors":
                self.fetch_sectors(agency, soup, all_sectors, url)
            
            elif func_name == "services":
                self.utils.fetch_services(agency, soup, url)
            
            elif func_name == "legal":
                self.fetch_legal(agency, page_text, url)
            
            elif func_name == "contact":
                self.fetch_contact(agency, soup, url)

    def _apply_functions_ai(
        self,
        agency: Agency,
        content: str,
        all_sectors: Set[str],
        url: str,
    ) -> None:
        """
        Call AI extraction functions using crawl4ai + Ollama.
        Only extract fields that are still missing (skip if already found).
        """
        ai_mode = "LLM" if self.USE_LLM else "Text Search"
        self.logger.info(f"   🔍 AI Mode: {ai_mode}")
        
        # Digital capabilities
        if not agency.digital_capabilities.candidate_portal:
            self.logger.info("   → Extracting digital_capabilities...")
            self.utils.fetch_ai_digital_capabilities(agency, url)
        else:
            self.logger.info("   ⏭️  Skipping digital_capabilities (already found)")
        
        # AI capabilities
        if not agency.ai_capabilities.chatbot_for_candidates:
            self.logger.info("   → Extracting ai_capabilities...")
            self.utils.fetch_ai_ai_capabilities(agency, url)
        else:
            self.logger.info("   ⏭️  Skipping ai_capabilities (already found)")
        
        # Membership & CAO
        if not agency.membership or agency.cao_type == CaoType.ONBEKEND:
            self.logger.info("   → Extracting membership & CAO...")
            self.fetch_ai_membership_cao(agency, url)
        else:
            self.logger.info("   ⏭️  Skipping membership & CAO (already found)")
        
        # Phase system
        if not agency.phase_system:
            self.logger.info("   → Extracting phase_system...")
            self.fetch_ai_phase_system(agency, url)
        else:
            self.logger.info("   ⏭️  Skipping phase_system (already found)")
        
        # Pricing
        if not agency.avg_hourly_rate_low:
            self.logger.info("   → Extracting pricing...")
            self.utils.fetch_ai_pricing(agency, url)
        else:
            self.logger.info("   ⏭️  Skipping pricing (already found)")
        
        # Reviews
        if not agency.review_rating:
            self.logger.info("   → Extracting reviews...")
            self.utils.fetch_ai_reviews(agency, url)
        else:
            self.logger.info("   ⏭️  Skipping reviews (already found)")
        
        # Certifications
        if not agency.certifications:
            self.logger.info("   → Extracting certifications...")
            self.fetch_ai_certifications(agency, url)
        else:
            self.logger.info("   ⏭️  Skipping certifications (already found)")
        
        # Takeover policy
        if not agency.takeover_policy.free_takeover_hours:
            self.logger.info("   → Extracting takeover_policy...")
            self.fetch_ai_takeover_policy(agency, url)
        else:
            self.logger.info("   ⏭️  Skipping takeover_policy (already found)")
        
        # Performance metrics
        if not agency.avg_time_to_fill_days:
            self.logger.info("   → Extracting performance_metrics...")
            self.fetch_ai_performance_metrics(agency, url)
        else:
            self.logger.info("   ⏭️  Skipping performance_metrics (already found)")
        
        # KvK (if still missing from legal page)
        if not agency.kvk_number:
            self.logger.info("   → Extracting KvK (fallback)...")
            self.fetch_ai_kvk_number(agency, url)
        else:
            self.logger.info("   ⏭️  Skipping KvK (already found)")
        
        # Office locations (if still empty from contact page)
        if not agency.office_locations:
            self.logger.info("   → Extracting office_locations...")
            self.fetch_ai_office_locations(agency, url)
        else:
            self.logger.info(f"   ⏭️  Skipping office_locations (already found {len(agency.office_locations)})")

    # ---------------------------
    # AI extraction using Ollama (local LLM)
    # ---------------------------
    
    def fetch_page_ai(self, url: str) -> str:
        """Fetch page using crawl4ai (delegates to utils)."""
        content = self.utils.fetch_page_ai(url)
        if url not in self.evidence_urls:
            self.evidence_urls.add(url)
        return content

    # ---------------------------
    # Normal fetch functions (BeautifulSoup-based)
    # ---------------------------
    
    def fetch_logo(self, agency: Agency, soup: BeautifulSoup, url: str) -> None:
        """Extract logo (delegates to utils)."""
        if agency.logo_url:
            return
        
        logo_url = self.utils.fetch_logo(soup, url)
        if logo_url:
            agency.logo_url = logo_url

    def fetch_sectors(self, agency: Agency, soup: BeautifulSoup, all_sectors: Set[str], url: str) -> None:
        """Extract specialisms/sectors from page."""
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
                            all_sectors.add(specialism_text)
                            seen.add(specialism_text.lower())
                            self.logger.info(f"✓ Found sector: '{specialism_text}' | Source: {url}")
        
        # Fallback: Look for specialism links in recruitment URLs
        if not seen:
            for link in soup.find_all("a", href=re.compile(r"/recruitment/.*-recruitment")):
                specialism_text = link.get_text(strip=True).replace("►", "").strip()
                if specialism_text and specialism_text.lower() not in seen:
                    all_sectors.add(specialism_text)
                    seen.add(specialism_text.lower())
                    self.logger.info(f"✓ Found sector: '{specialism_text}' | Source: {url}")

    def fetch_services(self, agency: Agency, soup: BeautifulSoup, url: str) -> None:
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
        
        # Fallback to general text
        else:
            if "perm recruitment" in text_lower:
                agency.services.werving_selectie = True
                self.logger.info(f"✓ Found service 'werving_selectie' | Source: {url}")
            if "flex recruitment" in text_lower or "temporary" in text_lower:
                agency.services.detacheren = True
                self.logger.info(f"✓ Found service 'detacheren' | Source: {url}")
            if "contracting" in text_lower:
                agency.services.zzp_bemiddeling = True
                self.logger.info(f"✓ Found service 'zzp_bemiddeling' | Source: {url}")
            if "rpo" in text_lower:
                agency.services.rpo = True
                self.logger.info(f"✓ Found service 'rpo' | Source: {url}")
            if "msp" in text_lower:
                agency.services.msp = True
                self.logger.info(f"✓ Found service 'msp' | Source: {url}")
            if "executive search" in text_lower:
                agency.services.executive_search = True
                self.logger.info(f"✓ Found service 'executive_search' | Source: {url}")

    def fetch_contact(self, agency: Agency, soup: BeautifulSoup, url: str) -> None:
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
        
        # Update agency with contact info
        if result["offices"]:
            agency.office_locations = result["offices"]
        if result["phone"] and not agency.contact_phone:
            agency.contact_phone = result["phone"]
        if result["email"] and not agency.contact_email:
            agency.contact_email = result["email"]
        if result["hq_city"] and not agency.hq_city:
            agency.hq_city = result["hq_city"]
        if result["hq_province"] and not agency.hq_province:
            agency.hq_province = result["hq_province"]

    def fetch_legal(self, agency: Agency, page_text: str, url: str) -> None:
        """Extract KvK and legal name (delegates to utils)."""
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

    # ---------------------------
    # AI extraction functions using Ollama
    # ---------------------------

    def fetch_ai_digital_capabilities(self, agency: Agency, url: str) -> None:
        """Extract digital capabilities (delegates to utils)."""
        if agency.digital_capabilities.candidate_portal:
            return  # Already found
        
        try:
            self.utils.fetch_ai_digital_capabilities(agency, url)
        except Exception as e:
            self.logger.warning(f"Error in fetch_ai_digital_capabilities: {e}")

    def fetch_ai_ai_capabilities(self, agency: Agency, url: str) -> None:
        """Extract AI capabilities (delegates to utils)."""
        if agency.ai_capabilities.chatbot_for_candidates:
            return  # Already found
        
        try:
            self.utils.fetch_ai_ai_capabilities(agency, url)
        except Exception as e:
            self.logger.warning(f"Error in fetch_ai_ai_capabilities: {e}")

    def fetch_ai_membership_cao(self, agency: Agency, url: str) -> None:
        """Extract membership and CAO type (delegates to utils)."""
        try:
            content = self.utils.fetch_page_ai(url)
            cao = self.utils.fetch_cao_type(content, url)
            membership = self.utils.fetch_membership(content, url)
            
            if cao != CaoType.ONBEKEND:
                agency.cao_type = cao
            if membership:
                agency.membership = membership
        except Exception as e:
            self.logger.warning(f"Error in fetch_ai_membership_cao: {e}")

    def fetch_ai_phase_system(self, agency: Agency, url: str) -> None:
        """Extract phase system (delegates to utils)."""
        if agency.phase_system:
            return
        
        try:
            content = self.utils.fetch_page_ai(url)
            phase = self.utils.fetch_phase_system(content, url)
            if phase:
                agency.phase_system = phase
            
            # Check for inlenersbeloning
            content_lower = content.lower()
            if "inlenersbeloning" in content_lower:
                if "dag 1" in content_lower or "day 1" in content_lower:
                    agency.applies_inlenersbeloning_from_day1 = True
                    self.logger.info(f"✓ [AI] Found applies_inlenersbeloning_from_day1 | Source: {url}")
                agency.uses_inlenersbeloning = True
                self.logger.info(f"✓ [AI] Found uses_inlenersbeloning | Source: {url}")
        except Exception as e:
            self.logger.warning(f"Error in fetch_ai_phase_system: {e}")

    def fetch_ai_pricing(self, agency: Agency, url: str) -> None:
        """Extract pricing (delegates to utils)."""
        if agency.avg_hourly_rate_low:
            return
        
        try:
            self.utils.fetch_ai_pricing(agency, url)
        except Exception as e:
            self.logger.warning(f"Error in fetch_ai_pricing: {e}")

    def fetch_ai_reviews(self, agency: Agency, url: str) -> None:
        """Extract reviews (delegates to utils)."""
        if agency.review_rating:
            return
        
        try:
            self.utils.fetch_ai_reviews(agency, url)
        except Exception as e:
            self.logger.warning(f"Error in fetch_ai_reviews: {e}")

    def fetch_ai_certifications(self, agency: Agency, url: str) -> None:
        """Extract certifications (delegates to utils)."""
        if agency.certifications:
            return
        
        try:
            content = self.utils.fetch_page_ai(url)
            certs = self.utils.fetch_certifications(content, url)
            if certs:
                agency.certifications = certs
        except Exception as e:
            self.logger.warning(f"Error in fetch_ai_certifications: {e}")

    def fetch_ai_takeover_policy(self, agency: Agency, url: str) -> None:
        """Extract takeover policy using simple text search."""
        if agency.takeover_policy.free_takeover_hours:
            return
        
        try:
            content = self.utils.fetch_page_ai(url).lower()
            
            if "overname" in content or "takeover" in content:
                # Look for free hours/weeks
                hours_match = re.search(r'(\d+)\s*uur.*?(?:gratis|free|kosteloos)', content)
                if hours_match:
                    agency.takeover_policy.free_takeover_hours = int(hours_match.group(1))
                    self.logger.info(f"✓ [AI] Found free_takeover_hours: {hours_match.group(1)} | Source: {url}")
                
                weeks_match = re.search(r'(\d+)\s*(?:weken|weeks).*?(?:gratis|free|kosteloos)', content)
                if weeks_match:
                    agency.takeover_policy.free_takeover_weeks = int(weeks_match.group(1))
                    self.logger.info(f"✓ [AI] Found free_takeover_weeks: {weeks_match.group(1)} | Source: {url}")
                
                fee_match = re.search(r'overname.*?€\s*(\d+[.,]?\d*)', content)
                if fee_match:
                    agency.takeover_policy.overname_fee_hint = f"€{fee_match.group(1)}"
                    self.logger.info(f"✓ [AI] Found overname_fee_hint: €{fee_match.group(1)} | Source: {url}")
        except Exception as e:
            self.logger.warning(f"Error in fetch_ai_takeover_policy: {e}")

    def fetch_ai_performance_metrics(self, agency: Agency, url: str) -> None:
        """Extract performance metrics using Ollama LLM (if USE_LLM=True) or simple text search."""
        if agency.avg_time_to_fill_days:
            return
        
        # If LLM is disabled, use simple text search
        if not self.USE_LLM:
            try:
                content = self.utils.fetch_page_ai(url).lower()
                # Time to fill
                time_match = re.search(r'(\d+)\s*(?:dagen|days)', content)
                if time_match:
                    try:
                        days = int(time_match.group(1))
                        if 1 <= days <= 90:
                            agency.avg_time_to_fill_days = days
                            self.logger.info(f"✓ [AI] Found avg_time_to_fill_days: {days} | Source: {url}")
                    except:
                        pass
                
                # Speed claims
                speed_keywords = [
                    ("24 uur", "24_hours"),
                    ("24 hours", "24_hours"),
                    ("snel", "fast_turnaround"),
                ]
                for keyword, claim in speed_keywords:
                    if keyword in content and claim not in agency.speed_claims:
                        agency.speed_claims.append(claim)
                        self.logger.info(f"✓ [AI] Found speed_claim: {claim} | Source: {url}")
            except Exception as e:
                self.logger.warning(f"Error in fetch_ai_performance_metrics: {e}")
            return
        
        # Use LLM extraction
        try:
            result = asyncio.run(self._extract_with_llm(
                url=url,
                schema=PerformanceMetricsSchema,
                instruction="Extract performance metrics: time to fill (in days), annual placements, candidate pool size, speed claims."
            ))
            
            if result:
                if result.avg_time_to_fill_days:
                    agency.avg_time_to_fill_days = result.avg_time_to_fill_days
                    self.logger.info(f"✓ [AI] Found avg_time_to_fill_days: {result.avg_time_to_fill_days} | Source: {url}")
                if result.annual_placements_estimate:
                    agency.annual_placements_estimate = result.annual_placements_estimate
                    self.logger.info(f"✓ [AI] Found annual_placements_estimate: {result.annual_placements_estimate} | Source: {url}")
                if result.candidate_pool_size_estimate:
                    agency.candidate_pool_size_estimate = result.candidate_pool_size_estimate
                    self.logger.info(f"✓ [AI] Found candidate_pool_size_estimate: {result.candidate_pool_size_estimate} | Source: {url}")
                if result.speed_claims:
                    agency.speed_claims.extend(result.speed_claims)
                    self.logger.info(f"✓ [AI] Found speed_claims: {result.speed_claims} | Source: {url}")
        except Exception as e:
            self.logger.warning(f"Error in fetch_ai_performance_metrics: {e}")

    def fetch_ai_kvk_number(self, agency: Agency, url: str) -> None:
        """Extract KvK number (delegates to utils)."""
        if agency.kvk_number:
            return
        
        try:
            content = self.utils.fetch_page_ai(url)
            kvk = self.utils.fetch_kvk_number(content, url)
            if kvk:
                agency.kvk_number = kvk
        except Exception as e:
            self.logger.warning(f"Error in fetch_ai_kvk_number: {e}")

    def fetch_ai_office_locations(self, agency: Agency, url: str) -> None:
        """Extract office locations using simple text search."""
        if agency.office_locations:
            return
        
        try:
            content = self.utils.fetch_page_ai(url)
            cities = ["Amsterdam", "Rotterdam", "Utrecht", "Den Haag", "Eindhoven", "Tilburg", "Groningen"]
            city_province_map = {
                "amsterdam": "Noord-Holland",
                "tilburg": "Noord-Brabant",
                "rotterdam": "Zuid-Holland",
                "utrecht": "Utrecht",
                "eindhoven": "Noord-Brabant",
                "den haag": "Zuid-Holland",
                "groningen": "Groningen",
            }
            
            for city in cities:
                if city in content and not any(office.city == city for office in agency.office_locations):
                    province = city_province_map.get(city.lower())
                    office = OfficeLocation(city=city, province=province)
                    agency.office_locations.append(office)
                    self.logger.info(f"✓ [AI] Found office: {city} | Source: {url}")
                    
                    if not agency.hq_city:
                        agency.hq_city = city
                        agency.hq_province = province
                        self.logger.info(f"✓ [AI] Set HQ: {city}")
        except Exception as e:
            self.logger.warning(f"Error in fetch_ai_office_locations: {e}")

    # ---------------------------
    # Helper functions for text analysis
    # ---------------------------

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
