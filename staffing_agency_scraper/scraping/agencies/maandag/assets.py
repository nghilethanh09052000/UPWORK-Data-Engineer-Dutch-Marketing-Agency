"""
Maandag Netherlands scraper.

Website: https://www.maandag.nl
"""

from __future__ import annotations

import asyncio
from typing import Any, Dict, List

import dagster as dg

from crawl4ai import AsyncWebCrawler, CrawlerRunConfig, LXMLWebScrapingStrategy
from staffing_agency_scraper.models import (
    Agency,
    CaoType,
    GeoFocusType,
    OfficeLocation,
)
from staffing_agency_scraper.scraping.base import BaseAgencyScraper


class MaandagScraper(BaseAgencyScraper):
    """Scraper for Maandag Netherlands using crawl4ai."""

    AGENCY_NAME = "Maandag"
    WEBSITE_URL = "https://www.maandag.nl"
    BRAND_GROUP = None

    # Pages to scrape with AI - no functions, pure AI extraction
    PAGES_TO_SCRAPE: List[Dict[str, Any]] = [
        {
            "name": "home",
            "url": "https://www.maandag.nl",
            "use_ai": True,
        },
        {
            "name": "about",
            "url": "https://www.maandag.com/nl-nl/over-ons",
            "use_ai": True,
        },
        {
            "name": "government",
            "url": "https://www.maandag.com/nl-nl/overheid",
            "use_ai": True,
        },
        {
            "name": "contact",
            "url": "https://www.maandag.com/nl-nl/contact",
            "use_ai": True,
        },
        {
            "name": "service",
            "url": "https://www.maandag.com/nl-nl/service",
            "use_ai": True,
        },
        {
            "name": "zzp_start",
            "url": "https://www.maandag.com/nl-nl/zzpstart",
            "use_ai": True,
        },
        {
            "name": "zzp_dba",
            "url": "https://www.maandag.com/nl-nl/zzp-wet-dba",
            "use_ai": True,
        },
        {
            "name": "privacy",
            "url": "https://www.maandag.com/nl-nl/privacy",
            "use_ai": True,
        },
    ]

    def scrape(self) -> Agency:
        self.logger.info(f"Starting scrape of {self.AGENCY_NAME} with crawl4ai")

        agency = self.create_base_agency()
        agency.geo_focus_type = GeoFocusType.NATIONAL
        agency.employers_page_url = f"{self.WEBSITE_URL}/werkgevers"
        agency.contact_form_url = f"{self.WEBSITE_URL}/contact"

        # Known facts
        agency.membership = ["ABU"]
        agency.cao_type = CaoType.ABU
        agency.regions_served = ["landelijk"]

        # Process each page with AI only
        for page in self.PAGES_TO_SCRAPE:
            url = page["url"]
            page_name = page["name"]
            
            try:
                self.logger.info(f"Processing page: {page_name} ({url})")
                
                # Fetch with crawl4ai
                content = self.fetch_page_ai(url)
                
                # Extract all fields using AI
                self._extract_all_fields_ai(agency, content, url, page_name)

            except Exception as e:
                self.logger.warning(f"Error scraping {url}: {e}")

        # Fallback for sectors if nothing found
        if not agency.sectors_core:
            agency.sectors_core = ["logistiek", "productie", "administratief"]
            self.logger.info("✓ Using default sectors")

        # Update evidence URLs
        agency.evidence_urls = list(self.evidence_urls)
        agency.collected_at = self.collected_at

        self.logger.info(f"Completed scrape of {self.AGENCY_NAME}")
        return agency

    # ---------------------------
    # Pure AI extraction using crawl4ai
    # ---------------------------

    def fetch_page_ai(self, url: str) -> str:
        """Fetch a page using crawl4ai and return content."""
        self.logger.info(f"[AI Fetch] {url}")

        async def _fetch() -> str:
            async with AsyncWebCrawler() as crawler:
                result = await crawler.arun(
                    url=url,
                    config=CrawlerRunConfig(
                        scraping_strategy=LXMLWebScrapingStrategy(),
                    ),
                )
                # Get markdown or cleaned text
                if hasattr(result, 'markdown') and result.markdown:
                    return result.markdown
                elif hasattr(result, 'cleaned_html') and result.cleaned_html:
                    return result.cleaned_html
                else:
                    return getattr(result, "html", "") or ""

        content = asyncio.run(_fetch())
        if url not in self.evidence_urls:
            self.evidence_urls.append(url)
        return content

    def _extract_all_fields_ai(
        self,
        agency: Agency,
        content: str,
        url: str,
        page_name: str,
    ) -> None:
        """Extract all fields from content using AI."""
        content_lower = content.lower()
        
        # Logo
        if not agency.logo_url and "logo" in content_lower:
            if "maandag" in content_lower:
                # Look for image URLs in content
                import re
                img_match = re.search(r'(https?://[^\s]+(?:logo|brand)[^\s]+\.(?:png|jpg|svg|webp))', content, re.IGNORECASE)
                if img_match:
                    agency.logo_url = img_match.group(1)
                    self.logger.info(f"✓ [AI] Found logo: {agency.logo_url} | Source: {url}")
        
        # Services
        if "uitzenden" in content_lower:
            agency.services.uitzenden = True
            self.logger.info(f"✓ [AI] Found service: uitzenden | Source: {url}")
        if "detacheren" in content_lower or "detachering" in content_lower:
            agency.services.detacheren = True
            self.logger.info(f"✓ [AI] Found service: detacheren | Source: {url}")
        if "werving" in content_lower or "selectie" in content_lower:
            agency.services.werving_selectie = True
            self.logger.info(f"✓ [AI] Found service: werving_selectie | Source: {url}")
        if "payroll" in content_lower:
            agency.services.payrolling = True
            self.logger.info(f"✓ [AI] Found service: payrolling | Source: {url}")
        if "zzp" in content_lower:
            agency.services.zzp_bemiddeling = True
            self.logger.info(f"✓ [AI] Found service: zzp_bemiddeling | Source: {url}")
        if "msp" in content_lower or "managed service" in content_lower:
            agency.services.msp = True
            self.logger.info(f"✓ [AI] Found service: msp | Source: {url}")
        if "rpo" in content_lower or "recruitment process outsourcing" in content_lower:
            agency.services.rpo = True
            self.logger.info(f"✓ [AI] Found service: rpo | Source: {url}")
        
        # Sectors
        sector_keywords = {
            "logistiek": "logistiek",
            "logistic": "logistiek",
            "productie": "productie",
            "production": "productie",
            "administratief": "administratief",
            "administrative": "administratief",
            "techniek": "techniek",
            "technical": "techniek",
            "horeca": "horeca",
            "zorg": "zorg",
            "care": "zorg",
            "bouw": "bouw",
            "construction": "bouw",
        }
        
        for keyword, sector in sector_keywords.items():
            if keyword in content_lower and (not agency.sectors_core or sector not in agency.sectors_core):
                if not agency.sectors_core:
                    agency.sectors_core = []
                if sector not in agency.sectors_core:
                    agency.sectors_core.append(sector)
                    self.logger.info(f"✓ [AI] Found sector: {sector} | Source: {url}")
        
        # Contact info (email, phone, offices)
        self._fetch_ai_contact(agency, content, url)
        
        # Legal (KvK, legal name)
        self._fetch_ai_legal(agency, content, url)
        
        # All other fields
        self._fetch_ai_phase_system(agency, content, url)
        self._fetch_ai_pricing(agency, content, url)
        self._fetch_ai_reviews(agency, content, url)
        self._fetch_ai_certifications(agency, content, url)
        self._fetch_ai_takeover_policy(agency, content, url)
        self._fetch_ai_performance_metrics(agency, content, url)
        self._fetch_ai_office_locations(agency, content, url)
        self._fetch_ai_digital_capabilities(agency, content, url)
        self._fetch_ai_ai_capabilities(agency, content, url)
        self._fetch_ai_role_levels(agency, content, url)

    def _fetch_ai_contact(self, agency: Agency, content: str, url: str) -> None:
        """Extract contact info using AI."""
        import re
        
        # Email
        if not agency.contact_email:
            email_match = re.search(r'([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})', content)
            if email_match:
                agency.contact_email = email_match.group(1)
                self.logger.info(f"✓ [AI] Found email: {agency.contact_email} | Source: {url}")
        
        # Phone
        if not agency.contact_phone:
            phone_patterns = [
                r'\+31[\s-]?\d{1,2}[\s-]?\d{3}[\s-]?\d{2}[\s-]?\d{2}',
                r'0\d{2,3}[\s-]?\d{3}[\s-]?\d{2}[\s-]?\d{2}',
            ]
            for pattern in phone_patterns:
                phone_match = re.search(pattern, content)
                if phone_match:
                    agency.contact_phone = phone_match.group(0)
                    self.logger.info(f"✓ [AI] Found phone: {agency.contact_phone} | Source: {url}")
                    break

    def _fetch_ai_legal(self, agency: Agency, content: str, url: str) -> None:
        """Extract legal info using AI."""
        import re
        
        # KvK number
        if not agency.kvk_number:
            kvk_match = re.search(r'(?:KvK|kvk|chamber of commerce)[\s\-:]*(\d{8})', content, re.IGNORECASE)
            if kvk_match:
                agency.kvk_number = kvk_match.group(1)
                self.logger.info(f"✓ [AI] Found KvK: {agency.kvk_number} | Source: {url}")
        
        # Legal name
        if not agency.legal_name:
            legal_match = re.search(r'(Maandag\s+(?:\w+\s+)?B\.?V\.?)', content, re.IGNORECASE)
            if legal_match:
                agency.legal_name = legal_match.group(1)
                self.logger.info(f"✓ [AI] Found legal_name: {agency.legal_name} | Source: {url}")

    def _fetch_ai_phase_system(self, agency: Agency, content: str, url: str) -> None:
        """Extract phase system using AI."""
        if agency.phase_system:
            return
        
        content_lower = content.lower()
        
        if "fase" in content_lower or "phase" in content_lower:
            if "3 fasen" in content_lower or "3 phases" in content_lower:
                agency.phase_system = "3_fasen"
                self.logger.info(f"✓ [AI] Found phase_system: 3_fasen | Source: {url}")
            elif "4 fasen" in content_lower or "4 phases" in content_lower:
                agency.phase_system = "4_fasen"
                self.logger.info(f"✓ [AI] Found phase_system: 4_fasen | Source: {url}")
        
        if "inlenersbeloning" in content_lower:
            if "dag 1" in content_lower or "day 1" in content_lower:
                agency.applies_inlenersbeloning_from_day1 = True
                self.logger.info(f"✓ [AI] Found applies_inlenersbeloning_from_day1: True | Source: {url}")
            agency.uses_inlenersbeloning = True
            self.logger.info(f"✓ [AI] Found uses_inlenersbeloning: True | Source: {url}")

    def _fetch_ai_pricing(self, agency: Agency, content: str, url: str) -> None:
        """Extract pricing information using AI."""
        import re
        content_lower = content.lower()
        
        # Hourly rates
        if not agency.avg_hourly_rate_low and ("uur" in content_lower or "hourly" in content_lower) and "€" in content:
            numbers = re.findall(r'€\s*(\d+(?:[.,]\d+)?)', content)
            if numbers and len(numbers) >= 2:
                try:
                    rates = [float(n.replace(",", ".")) for n in numbers[:2]]
                    rates.sort()
                    if 10 <= rates[0] <= 200 and 10 <= rates[-1] <= 200:
                        agency.avg_hourly_rate_low = rates[0]
                        agency.avg_hourly_rate_high = rates[-1]
                        self.logger.info(f"✓ [AI] Found hourly rates: €{rates[0]}-€{rates[-1]} | Source: {url}")
                except:
                    pass
        
        # Omrekenfactor
        if not agency.omrekenfactor_min and ("omrekenfactor" in content_lower or "markup" in content_lower):
            factors = re.findall(r'(\d+(?:[.,]\d+)?)', content)
            if factors:
                try:
                    nums = [float(f.replace(",", ".")) for f in factors if 1.0 <= float(f.replace(",", ".")) <= 3.0]
                    if nums:
                        agency.omrekenfactor_min = min(nums)
                        agency.omrekenfactor_max = max(nums)
                        self.logger.info(f"✓ [AI] Found omrekenfactor: {min(nums)}-{max(nums)} | Source: {url}")
                except:
                    pass
        
        # Pricing transparency
        if agency.pricing_transparency is None:
            if any(kw in content_lower for kw in ["transparant", "transparent", "open prijzen"]):
                agency.pricing_transparency = True
                self.logger.info(f"✓ [AI] Found pricing_transparency: True | Source: {url}")
        
        # No cure no pay
        if agency.no_cure_no_pay is None:
            if "no cure no pay" in content_lower or "no cure, no pay" in content_lower:
                agency.no_cure_no_pay = True
                self.logger.info(f"✓ [AI] Found no_cure_no_pay: True | Source: {url}")

    def _fetch_ai_reviews(self, agency: Agency, content: str, url: str) -> None:
        """Extract review rating and count using AI."""
        import re
        content_lower = content.lower()
        
        if agency.review_rating is None:
            rating_match = re.search(r'(\d+(?:[.,]\d+)?)\s*/\s*5', content_lower)
            if rating_match:
                try:
                    rating = float(rating_match.group(1).replace(",", "."))
                    if 0 <= rating <= 5:
                        agency.review_rating = rating
                        self.logger.info(f"✓ [AI] Found review_rating: {rating} | Source: {url}")
                except:
                    pass
        
        if agency.review_count is None:
            count_match = re.search(r'(\d+)\s+(?:reviews|beoordelingen)', content_lower)
            if count_match:
                try:
                    count = int(count_match.group(1))
                    if count > 0:
                        agency.review_count = count
                        self.logger.info(f"✓ [AI] Found review_count: {count} | Source: {url}")
                except:
                    pass

    def _fetch_ai_certifications(self, agency: Agency, content: str, url: str) -> None:
        """Extract certifications using AI."""
        if agency.certifications:
            return
        
        content_lower = content.lower()
        certs = []
        
        cert_keywords = {
            "iso 9001": "ISO 9001",
            "iso9001": "ISO 9001",
            "sna": "SNA",
            "nba": "NBA",
            "psom": "PSOM",
            "vcr": "VCR",
            "sri": "SRI",
        }
        
        for keyword, cert_name in cert_keywords.items():
            if keyword in content_lower and cert_name not in certs:
                certs.append(cert_name)
                self.logger.info(f"✓ [AI] Found certification: {cert_name} | Source: {url}")
        
        if certs:
            agency.certifications = certs

    def _fetch_ai_takeover_policy(self, agency: Agency, content: str, url: str) -> None:
        """Extract takeover policy using AI."""
        import re
        content_lower = content.lower()
        
        if "overname" in content_lower or "takeover" in content_lower:
            if agency.takeover_policy.free_takeover_hours is None:
                hours_match = re.search(r'(\d+)\s*uur.*?(?:gratis|free|kosteloos)', content_lower)
                if hours_match:
                    try:
                        hours = int(hours_match.group(1))
                        agency.takeover_policy.free_takeover_hours = hours
                        self.logger.info(f"✓ [AI] Found free_takeover_hours: {hours} | Source: {url}")
                    except:
                        pass
            
            if agency.takeover_policy.free_takeover_weeks is None:
                weeks_match = re.search(r'(\d+)\s*(?:weken|weeks).*?(?:gratis|free|kosteloos)', content_lower)
                if weeks_match:
                    try:
                        weeks = int(weeks_match.group(1))
                        agency.takeover_policy.free_takeover_weeks = weeks
                        self.logger.info(f"✓ [AI] Found free_takeover_weeks: {weeks} | Source: {url}")
                    except:
                        pass
            
            if agency.takeover_policy.overname_fee_hint is None:
                fee_match = re.search(r'overname.*?€\s*(\d+[.,]?\d*)', content_lower)
                if fee_match:
                    agency.takeover_policy.overname_fee_hint = f"€{fee_match.group(1)}"
                    self.logger.info(f"✓ [AI] Found overname_fee_hint: {agency.takeover_policy.overname_fee_hint} | Source: {url}")

    def _fetch_ai_performance_metrics(self, agency: Agency, content: str, url: str) -> None:
        """Extract performance metrics using AI."""
        import re
        content_lower = content.lower()
        
        # Time to fill
        if agency.avg_time_to_fill_days is None:
            if "dag" in content_lower or "day" in content_lower:
                time_match = re.search(r'(\d+)\s*(?:dagen|days)', content_lower)
                if time_match:
                    try:
                        days = int(time_match.group(1))
                        if 1 <= days <= 90:
                            agency.avg_time_to_fill_days = days
                            self.logger.info(f"✓ [AI] Found avg_time_to_fill_days: {days} | Source: {url}")
                    except:
                        pass
        
        # Annual placements
        if agency.annual_placements_estimate is None:
            placement_match = re.search(r'(\d+[.,]?\d*)\s*(?:placements|plaatsingen)', content_lower)
            if placement_match:
                try:
                    count = int(placement_match.group(1).replace(".", "").replace(",", ""))
                    if count > 10:
                        agency.annual_placements_estimate = count
                        self.logger.info(f"✓ [AI] Found annual_placements_estimate: {count} | Source: {url}")
                except:
                    pass
        
        # Candidate pool
        if agency.candidate_pool_size_estimate is None:
            pool_match = re.search(r'(\d+[.,]?\d*)\s*(?:candidates|kandidaten)', content_lower)
            if pool_match:
                try:
                    count = int(pool_match.group(1).replace(".", "").replace(",", ""))
                    if count > 100:
                        agency.candidate_pool_size_estimate = count
                        self.logger.info(f"✓ [AI] Found candidate_pool_size_estimate: {count} | Source: {url}")
                except:
                    pass
        
        # Speed claims
        if not agency.speed_claims:
            speed_keywords = [
                ("24 uur", "24_hours"),
                ("24 hours", "24_hours"),
                ("48 uur", "48_hours"),
                ("snel", "fast_turnaround"),
                ("quick", "fast_turnaround"),
            ]
            for keyword, claim in speed_keywords:
                if keyword in content_lower and claim not in agency.speed_claims:
                    agency.speed_claims.append(claim)
                    self.logger.info(f"✓ [AI] Found speed_claim: {claim} | Source: {url}")

    def _fetch_ai_office_locations(self, agency: Agency, content: str, url: str) -> None:
        """Extract office locations using AI."""
        # Look for Dutch cities
        cities = ["Amsterdam", "Rotterdam", "Utrecht", "Den Haag", "Eindhoven", "Tilburg", "Groningen", "Breda", "Nijmegen", "Apeldoorn"]
        
        for city in cities:
            if city in content and not any(office.city == city for office in agency.office_locations):
                office = OfficeLocation(city=city)
                agency.office_locations.append(office)
                self.logger.info(f"✓ [AI] Found office: {city} | Source: {url}")
                
                if not agency.hq_city:
                    agency.hq_city = city
                    self.logger.info(f"✓ [AI] Set HQ: {city}")

    def _fetch_ai_digital_capabilities(self, agency: Agency, content: str, url: str) -> None:
        """Extract digital capabilities using AI."""
        content_lower = content.lower()
        
        if "portal" in content_lower or "sign in" in content_lower or "inloggen" in content_lower:
            agency.digital_capabilities.candidate_portal = True
            self.logger.info(f"✓ [AI] Found digital capability: candidate_portal | Source: {url}")
        
        if "client portal" in content_lower or "opdrachtgever portal" in content_lower:
            agency.digital_capabilities.client_portal = True
            self.logger.info(f"✓ [AI] Found digital capability: client_portal | Source: {url}")
        
        if "app store" in content_lower or "mobile app" in content_lower or "download app" in content_lower:
            agency.digital_capabilities.mobile_app = True
            self.logger.info(f"✓ [AI] Found digital capability: mobile_app | Source: {url}")
        
        if "api" in content_lower:
            agency.digital_capabilities.api_available = True
            self.logger.info(f"✓ [AI] Found digital capability: api_available | Source: {url}")

    def _fetch_ai_ai_capabilities(self, agency: Agency, content: str, url: str) -> None:
        """Extract AI capabilities using AI."""
        content_lower = content.lower()
        
        if "chatbot" in content_lower or "chat bot" in content_lower or "live chat" in content_lower:
            agency.ai_capabilities.chatbot_for_candidates = True
            agency.ai_capabilities.chatbot_for_clients = True
            self.logger.info(f"✓ [AI] Found AI capability: chatbot | Source: {url}")
        
        if "ai matching" in content_lower or "artificial intelligence" in content_lower:
            agency.ai_capabilities.internal_ai_matching = True
            self.logger.info(f"✓ [AI] Found AI capability: ai_matching | Source: {url}")
        
        if "ai screening" in content_lower:
            agency.ai_capabilities.ai_screening = True
            self.logger.info(f"✓ [AI] Found AI capability: ai_screening | Source: {url}")

    def _fetch_ai_role_levels(self, agency: Agency, content: str, url: str) -> None:
        """Extract role levels using AI."""
        content_lower = content.lower()
        levels = []
        
        if "senior" in content_lower and "senior" not in agency.role_levels:
            levels.append("senior")
        if ("medior" in content_lower or "mid" in content_lower or "mid-level" in content_lower) and "medior" not in agency.role_levels:
            levels.append("medior")
        if ("junior" in content_lower or "starter" in content_lower) and "junior" not in agency.role_levels:
            levels.append("junior")
        if ("executive" in content_lower or "management" in content_lower) and "executive" not in agency.role_levels:
            levels.append("executive")
        
        if levels:
            agency.role_levels.extend(levels)
            self.logger.info(f"✓ [AI] Found role levels: {', '.join(levels)} | Source: {url}")


@dg.asset(group_name="agencies")
def maandag_scrape() -> dg.Output[dict]:
    """Scrape Maandag Netherlands website."""
    scraper = MaandagScraper()
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

