"""
Maandag Netherlands scraper.

Website: https://www.maandag.nl
100% AI extraction using crawl4ai + utils
"""

from __future__ import annotations

from typing import Any, Dict, List

import dagster as dg

from staffing_agency_scraper.models import Agency, CaoType, GeoFocusType
from staffing_agency_scraper.scraping.base import BaseAgencyScraper
from staffing_agency_scraper.scraping.utils import AgencyScraperUtils


class MaandagScraper(BaseAgencyScraper):
    """Scraper for Maandag Netherlands using 100% AI extraction."""

    AGENCY_NAME = "Maandag"
    WEBSITE_URL = "https://www.maandag.nl"
    BRAND_GROUP = None
    
    # LLM Configuration
    USE_LLM = False  # Text search mode by default
    
    # Pages to scrape - 100% AI, no BS4 functions
    PAGES_TO_SCRAPE: List[Dict[str, Any]] = [
        {
            "name": "home",
            "url": "https://www.maandag.nl",
            "functions": [],  # No BS4 functions
            "use_ai": True,  # 100% AI
        },
        {
            "name": "about",
            "url": "https://www.maandag.com/nl-nl/over-ons",
            "functions": [],
            "use_ai": True,
        },
        {
            "name": "government",
            "url": "https://www.maandag.com/nl-nl/overheid",
            "functions": [],
            "use_ai": True,
        },
        {
            "name": "contact",
            "url": "https://www.maandag.com/nl-nl/contact",
            "functions": [],
            "use_ai": True,
        },
        {
            "name": "service",
            "url": "https://www.maandag.com/nl-nl/service",
            "functions": [],
            "use_ai": True,
        },
        {
            "name": "zzp_start",
            "url": "https://www.maandag.com/nl-nl/zzpstart",
            "functions": [],
            "use_ai": True,
        },
        {
            "name": "privacy",
            "url": "https://www.maandag.com/nl-nl/privacy",
            "functions": [],
            "use_ai": True,
        },
    ]

    def scrape(self) -> Agency:
        self.logger.info(f"Starting scrape of {self.AGENCY_NAME} (100% AI mode)")
        
        # Initialize utils        
        agency = self.create_base_agency()
        agency.geo_focus_type = GeoFocusType.NATIONAL
        agency.employers_page_url = f"{self.WEBSITE_URL}/werkgevers"
        agency.contact_form_url = f"{self.WEBSITE_URL}/contact"
        
        # Known facts (from website)
        agency.membership = ["ABU"]
        agency.cao_type = CaoType.ABU
        agency.regions_served = ["landelijk"]
        
        for page in self.PAGES_TO_SCRAPE:
            url = page["url"]
            page_name = page["name"]
            use_ai = page.get("use_ai", False)
            
            try:
                self.logger.info("=" * 80)
                self.logger.info(f"📄 PROCESSING: {page_name}")
                self.logger.info(f"🔗 URL: {url}")
                self.logger.info("-" * 80)
                
                # Skip BS4 fetch - go straight to AI
                if use_ai:
                    self.logger.info("🤖 100% AI extraction...")
                    self._apply_functions_ai(agency, url)
                
                self.logger.info(f"✅ Completed: {page_name}")
                
            except Exception as e:
                self.logger.error(f"❌ Error scraping {url}: {e}")
        
        # Finalize
        agency.evidence_urls = list(self.evidence_urls)
        agency.collected_at = self.collected_at
        
        self.logger.info("=" * 80)
        self.logger.info(f"✅ Completed scrape of {self.AGENCY_NAME}")
        self.logger.info(f"📄 Evidence URLs: {len(agency.evidence_urls)}")
        self.logger.info("=" * 80)
        
        return agency
    
    def _apply_functions_ai(self, agency: Agency, url: str) -> None:
        """Apply ALL AI extraction functions (100% AI mode)."""
        # Logo (extract from crawl4ai content)
        if not agency.logo_url:
            content = self.utils.fetch_page_ai(url)
            # Try to find logo URL in content
            import re
            logo_match = re.search(r'https?://[^\s"\']+logo[^\s"\']*\.(?:png|jpg|jpeg|svg|webp)', content, re.IGNORECASE)
            if logo_match:
                agency.logo_url = logo_match.group(0)
                self.logger.info(f"✓ [AI] Found logo: {agency.logo_url} | Source: {url}")
        
        # Contact info
        if not agency.contact_email:
            content = self.utils.fetch_page_ai(url)
            email = self.utils.fetch_contact_email(content, url)
            if email:
                agency.contact_email = email
        
        if not agency.contact_phone:
            content = self.utils.fetch_page_ai(url)
            phone = self.utils.fetch_contact_phone(content, url)
            if phone:
                agency.contact_phone = phone
        
        # KvK and legal name
        if not agency.kvk_number:
            content = self.utils.fetch_page_ai(url)
            kvk = self.utils.fetch_kvk_number(content, url)
            if kvk:
                agency.kvk_number = kvk
        
        if not agency.legal_name:
            content = self.utils.fetch_page_ai(url)
            legal_name = self.utils.fetch_legal_name(content, "Maandag", url)
            if legal_name:
                agency.legal_name = legal_name
        
        # Services
        if not any([agency.services.uitzenden, agency.services.detacheren, agency.services.werving_selectie]):
            content = self.utils.fetch_page_ai(url)
            services = self.utils.fetch_services(content, url)
            agency.services = services
        
        # Digital capabilities
        if not agency.digital_capabilities.candidate_portal:
            self.utils.fetch_ai_digital_capabilities(agency, url)
        
        # AI capabilities
        if not agency.ai_capabilities.chatbot_for_candidates:
            self.utils.fetch_ai_ai_capabilities(agency, url)
        
        # Phase system
        if not agency.phase_system:
            content = self.utils.fetch_page_ai(url)
            phase = self.utils.fetch_phase_system(content, url)
            if phase:
                agency.phase_system = phase
        
        # Certifications
        if not agency.certifications:
            content = self.utils.fetch_page_ai(url)
            certs = self.utils.fetch_certifications(content, url)
            if certs:
                agency.certifications = certs
        
        # Pricing
        if not agency.avg_hourly_rate_low:
            self.utils.fetch_ai_pricing(agency, url)
        
        # Reviews
        if not agency.review_rating:
            self.utils.fetch_ai_reviews(agency, url)


@dg.asset(group_name="agencies")
def maandag_scrape() -> dg.Output[dict]:
    """Scrape Maandag Netherlands website (100% AI mode)."""
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
