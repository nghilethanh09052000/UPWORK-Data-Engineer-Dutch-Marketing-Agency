"""
Base scraper class for staffing agencies.

Provides common functionality for all agency scrapers.
Use AgencyScraperUtils for extraction methods.
"""

from __future__ import annotations

import json
import os
from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING
from urllib.parse import urljoin

import dagster as dg

from staffing_agency_scraper.lib.extract import (
    extract_contact_from_page,
    extract_from_footer,
    extract_kvk_from_text,
    extract_dutch_phone,
    extract_business_email,
    extract_structured_data,
)
from staffing_agency_scraper.lib.fetch import fetch_with_retry
from staffing_agency_scraper.lib.normalize import (
    detect_cao_type,
    detect_certifications,
    detect_focus_segments,
    detect_services,
    extract_sectors_from_text,
    normalize_geo_focus,
)
from staffing_agency_scraper.lib.parse import (
    clean_text,
    extract_email,
    extract_kvk_number,
    extract_phone,
    extract_urls_from_page,
    get_attribute,
    get_text_content,
    parse_html,
)
from staffing_agency_scraper.models import Agency, AgencyServices
from staffing_agency_scraper.scraping.utils import AgencyScraperUtils

if TYPE_CHECKING:
    from bs4 import BeautifulSoup


class BaseAgencyScraper(ABC):
    """
    Abstract base class for agency scrapers.

    Each agency scraper should inherit from this class and implement
    the required abstract methods.
    
    Use AgencyScraperUtils for reusable extraction methods.
    """

    # Agency configuration - override in subclass
    AGENCY_NAME: str = ""
    WEBSITE_URL: str = ""
    BRAND_GROUP: str | None = None

    # Pages to scrape - override in subclass
    PAGES_TO_SCRAPE: list[str] = []

    def __init__(self):
        self.logger = dg.get_dagster_logger(f"{self.__class__.__name__}_scraper")
        self.evidence_urls: list[str] = []  # List of URLs used as evidence
        self.collected_at = datetime.utcnow()
        # Initialize utility functions (can be overridden in subclass)
        self.utils = AgencyScraperUtils(logger=self.logger)

    @abstractmethod
    def scrape(self) -> Agency:
        """
        Main scraping method. Must be implemented by each agency scraper.

        Returns
        -------
        Agency
            The scraped agency data
        """
        ...

    def fetch_page(self, url: str) -> BeautifulSoup:
        """
        Fetch and parse a page.

        Parameters
        ----------
        url : str
            URL to fetch

        Returns
        -------
        BeautifulSoup
            Parsed HTML
        """
        self.logger.info(f"Fetching: {url}")
        response = fetch_with_retry(url)
        if url not in self.evidence_urls:
            self.evidence_urls.append(url)
        return parse_html(response.text)
    def extract_contact_info(self, soup: BeautifulSoup) -> dict:
        """
        Extract contact information from a page using enhanced extraction.

        Uses multiple methods:
        - JSON-LD structured data
        - Footer extraction
        - Full page text scanning

        Parameters
        ----------
        soup : BeautifulSoup
            Parsed HTML

        Returns
        -------
        dict
            Contact information including kvk_number, contact_phone, contact_email
        """
        # Use enhanced extraction that checks multiple sources
        return extract_contact_from_page(soup)

    def extract_logo_url(self, soup: BeautifulSoup) -> str | None:
        """
        Basic logo extraction - override in subclass for site-specific logic.

        Parameters
        ----------
        soup : BeautifulSoup
            Parsed HTML

        Returns
        -------
        str | None
            Logo URL or None
        """
        # Try OG image as a basic fallback
        og_image = soup.find("meta", attrs={"property": "og:image"})
        if og_image and og_image.get("content"):
            return og_image.get("content")
        
        return None

    def extract_services_from_page(self, soup: BeautifulSoup) -> AgencyServices:
        """
        Extract services from page text.

        Parameters
        ----------
        soup : BeautifulSoup
            Parsed HTML

        Returns
        -------
        AgencyServices
            Detected services
        """
        page_text = soup.get_text()
        services_dict = detect_services(page_text)
        return AgencyServices(**services_dict)

    def extract_certifications_from_page(self, soup: BeautifulSoup) -> list[str]:
        """
        Extract certifications from page.

        Parameters
        ----------
        soup : BeautifulSoup
            Parsed HTML

        Returns
        -------
        list[str]
            List of certifications
        """
        page_text = soup.get_text()
        return detect_certifications(page_text)

    def extract_sectors_from_page(self, soup: BeautifulSoup) -> list[str]:
        """
        Extract sectors from page.

        Parameters
        ----------
        soup : BeautifulSoup
            Parsed HTML

        Returns
        -------
        list[str]
            List of sectors
        """
        page_text = soup.get_text()
        return extract_sectors_from_text(page_text)

    def find_page_url(self, soup: BeautifulSoup, patterns: list[str]) -> str | None:
        """
        Find a page URL by matching link text patterns.

        Parameters
        ----------
        soup : BeautifulSoup
            Parsed HTML
        patterns : list[str]
            Patterns to match in link text

        Returns
        -------
        str | None
            Found URL or None
        """
        for pattern in patterns:
            links = soup.find_all("a", string=lambda t: t and pattern.lower() in t.lower())
            if links:
                href = get_attribute(links[0], "href")
                if href:
                    from urllib.parse import urljoin
                    return urljoin(self.WEBSITE_URL, href)
        return None

    def save_to_json(self, agency: Agency, output_dir: str = "./output") -> str:
        """
        Save agency data to JSON file.

        Parameters
        ----------
        agency : Agency
            Agency data to save
        output_dir : str
            Output directory

        Returns
        -------
        str
            Path to saved file
        """
        os.makedirs(output_dir, exist_ok=True)
        filename = f"{agency.agency_name.lower().replace(' ', '_')}.json"
        filepath = Path(output_dir) / filename

        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(agency.to_json_dict(), f, indent=2, ensure_ascii=False)

        self.logger.info(f"Saved agency data to {filepath}")
        return str(filepath)

    def create_base_agency(self) -> Agency:
        """
        Create a base Agency object with default values.

        Returns
        -------
        Agency
            Base agency object
        """
        return Agency(
            agency_name=self.AGENCY_NAME,
            website_url=self.WEBSITE_URL,
            brand_group=self.BRAND_GROUP,
            evidence_urls=self.evidence_urls.copy(),
            collected_at=self.collected_at,
        )

    def extract_all_common_fields(self, agency: Agency, all_text: str, soup: BeautifulSoup = None) -> None:
        """
        Extract all common fields using AgencyScraperUtils.
        
        This method centralizes the extraction of all standard fields to reduce
        code duplication across scrapers. Call this method from your scraper's
        scrape() method after collecting all page text.
        
        Override individual util methods in subclass if you need custom extraction logic.
        
        Parameters
        ----------
        agency : Agency
            Agency object to update with extracted data
        all_text : str
            Accumulated text from all scraped pages
        soup : BeautifulSoup, optional
            Main page soup for portal detection and review extraction
        
        Example
        -------
        ```python
        def scrape(self) -> Agency:
            agency = self.create_base_agency()
            all_text = ""
            
            # Scrape all pages
            for url in self.PAGES_TO_SCRAPE:
                soup = self.fetch_page(url)
                all_text += soup.get_text(separator=" ", strip=True)
            
            # Extract all common fields in one call!
            self.extract_all_common_fields(agency, all_text, soup)
            
            # Do any custom extractions specific to this agency
            agency.special_field = self._custom_extraction(all_text)
            
            return agency
        ```
        """
        url = "accumulated_text"
        
        # ==================== Market Positioning ====================
        if not agency.company_size_fit:
            agency.company_size_fit = self.utils.fetch_company_size_fit(all_text, url)
        
        if not agency.customer_segments:
            agency.customer_segments = self.utils.fetch_customer_segments(all_text, url)
        
        if not agency.focus_segments:
            agency.focus_segments = self.utils.fetch_focus_segments(all_text, url)
        
        if not agency.shift_types_supported:
            agency.shift_types_supported = self.utils.fetch_shift_types_supported(all_text, url)
        
        if not agency.typical_use_cases:
            agency.typical_use_cases = self.utils.fetch_typical_use_cases(all_text, url)
        
        if not agency.role_levels:
            agency.role_levels = self.utils.fetch_role_levels(all_text, url)
        
        # ==================== Volume & Performance ====================
        if agency.volume_specialisation == "unknown":
            agency.volume_specialisation = self.utils.fetch_volume_specialisation(all_text, url)
        
        if not agency.speed_claims:
            agency.speed_claims = self.utils.fetch_speed_claims(all_text, url)
        
        if not agency.avg_time_to_fill_days:
            agency.avg_time_to_fill_days = self.utils.fetch_avg_time_to_fill(all_text, url)
        
        if not agency.candidate_pool_size_estimate:
            agency.candidate_pool_size_estimate = self.utils.fetch_candidate_pool_size(all_text, url)
        
        if not agency.annual_placements_estimate:
            agency.annual_placements_estimate = self.utils.fetch_annual_placements(all_text, url)
        
        # ==================== Pricing & Commercial ====================
        if agency.pricing_model == "unknown":
            agency.pricing_model = self.utils.fetch_pricing_model(all_text, url)
        
        if not agency.pricing_transparency:
            agency.pricing_transparency = self.utils.fetch_pricing_transparency(all_text, url)
        
        if agency.no_cure_no_pay is None:
            agency.no_cure_no_pay = self.utils.fetch_no_cure_no_pay(all_text, url)
        
        if not agency.omrekenfactor_min and not agency.omrekenfactor_max:
            omrekenfactor_min, omrekenfactor_max = self.utils.fetch_omrekenfactor(all_text, url)
            if omrekenfactor_min:
                agency.omrekenfactor_min = omrekenfactor_min
            if omrekenfactor_max:
                agency.omrekenfactor_max = omrekenfactor_max
        
        if not agency.avg_hourly_rate_low and not agency.avg_hourly_rate_high:
            rate_low, rate_high = self.utils.fetch_avg_hourly_rate(all_text, url)
            if rate_low:
                agency.avg_hourly_rate_low = rate_low
            if rate_high:
                agency.avg_hourly_rate_high = rate_high
        
        # ==================== Legal & Compliance ====================
        if agency.uses_inlenersbeloning is None:
            agency.uses_inlenersbeloning = self.utils.fetch_uses_inlenersbeloning(all_text, url)
        
        if agency.applies_inlenersbeloning_from_day1 is None:
            agency.applies_inlenersbeloning_from_day1 = self.utils.fetch_applies_inlenersbeloning_from_day1(all_text, url)
        
        # ==================== Assignment Conditions ====================
        if not agency.min_assignment_duration_weeks:
            agency.min_assignment_duration_weeks = self.utils.fetch_min_assignment_duration(all_text, url)
        
        if not agency.min_hours_per_week:
            agency.min_hours_per_week = self.utils.fetch_min_hours_per_week(all_text, url)
        
        # ==================== Takeover Policy ====================
        if not agency.takeover_policy or (agency.takeover_policy.overname_fee_model == "unknown" if hasattr(agency.takeover_policy, 'overname_fee_model') else True):
            takeover_data = self.utils.fetch_takeover_policy(all_text, url)
            if takeover_data and takeover_data.get("overname_fee_model") != "unknown":
                # Convert dict to model if needed
                from staffing_agency_scraper.models import TakeoverPolicy
                if isinstance(takeover_data, dict):
                    agency.takeover_policy = TakeoverPolicy(**takeover_data)
                else:
                    agency.takeover_policy = takeover_data
        
        # ==================== Portal & Review Detection (requires soup) ====================
        if soup:
            # Portal detection
            if self.utils.detect_candidate_portal(soup, all_text, url):
                agency.digital_capabilities.candidate_portal = True
            if self.utils.detect_client_portal(soup, all_text, url):
                agency.digital_capabilities.client_portal = True
            
            # Review sources
            if not agency.review_sources:
                agency.review_sources = self.utils.fetch_review_sources(soup, url)
            
            # Review rating and count
            if not agency.review_rating and not agency.review_count:
                rating, count = self.utils.fetch_review_rating_and_count(soup, url)
                if rating:
                    agency.review_rating = rating
                if count:
                    agency.review_count = count
            
            # External review URLs
            if agency.review_sources and not agency.external_review_urls:
                agency.external_review_urls = self.utils.fetch_external_review_urls(agency.review_sources)
        
        # ==================== Growth Signals ====================
        if not agency.growth_signals:
            agency.growth_signals = self.utils.fetch_growth_signals(all_text, url)
    
    def scrape_all_pages(self, agency: Agency) -> Agency:
        """
        Enhanced scrape of all configured pages with automatic extraction.
        
        Extracts contact info, KvK, logo, sectors, certifications from all pages.
        Override specific extraction methods in subclasses for custom logic.
        
        Parameters
        ----------
        agency : Agency
            Agency object to update
        
        Returns
        -------
        Agency
            Updated agency object
        """
        all_text = ""
        all_certifications = []
        all_sectors = []
        
        for url in self.PAGES_TO_SCRAPE:
            try:
                soup = self.fetch_page(url)
                page_text = soup.get_text()
                all_text += " " + page_text
                
                # Extract contact info from each page
                contact_info = self.extract_contact_info(soup)
                
                # Update agency with found contact info (don't overwrite existing)
                if not agency.kvk_number and contact_info.get("kvk_number"):
                    agency.kvk_number = contact_info["kvk_number"]
                    self.logger.info(f"✓ Found KvK: {agency.kvk_number} | Source: {url}")
                
                if not agency.contact_phone and contact_info.get("contact_phone"):
                    agency.contact_phone = contact_info["contact_phone"]
                    self.logger.info(f"✓ Found phone: {agency.contact_phone} | Source: {url}")
                
                if not agency.contact_email and contact_info.get("contact_email"):
                    agency.contact_email = contact_info["contact_email"]
                    self.logger.info(f"✓ Found email: {agency.contact_email} | Source: {url}")
                
                if not agency.hq_city and contact_info.get("hq_city"):
                    agency.hq_city = contact_info["hq_city"]
                    self.logger.info(f"✓ Found HQ city: {agency.hq_city} | Source: {url}")
                
                if not agency.hq_province and contact_info.get("hq_province"):
                    agency.hq_province = contact_info["hq_province"]
                    self.logger.info(f"✓ Found HQ province: {agency.hq_province} | Source: {url}")
                
                if not agency.legal_name and contact_info.get("legal_name"):
                    agency.legal_name = contact_info["legal_name"]
                    self.logger.info(f"✓ Found legal name: {agency.legal_name} | Source: {url}")
                
                # Extract logo from each page (use first found)
                if not agency.logo_url:
                    logo = self.extract_logo_url(soup)
                    if logo:
                        agency.logo_url = logo
                        self.logger.info(f"✓ Found logo: {agency.logo_url} | Source: {url}")
                
                # Collect certifications and sectors
                certs = self.extract_certifications_from_page(soup)
                if certs:
                    self.logger.info(f"✓ Found {len(certs)} certifications | Source: {url}")
                all_certifications.extend(certs)
                
                sectors = self.extract_sectors_from_page(soup)
                if sectors:
                    self.logger.info(f"✓ Found {len(sectors)} sectors | Source: {url}")
                all_sectors.extend(sectors)
                
            except Exception as e:
                self.logger.warning(f"✗ Error scraping {url}: {e}")
        
        # Dedupe and set certifications
        if all_certifications:
            agency.certifications = list(set(all_certifications))
            self.logger.info(f"Total certifications: {agency.certifications}")
        
        # Dedupe and set sectors
        if all_sectors:
            agency.sectors_core = list(set(all_sectors))[:10]  # Limit to top 10
            self.logger.info(f"Total sectors: {agency.sectors_core}")
        
        # Log summary of what was found
        self.logger.info(f"--- Extraction Summary for {self.AGENCY_NAME} ---")
        self.logger.info(f"  KvK: {agency.kvk_number or 'NOT FOUND'}")
        self.logger.info(f"  Phone: {agency.contact_phone or 'NOT FOUND'}")
        self.logger.info(f"  Email: {agency.contact_email or 'NOT FOUND'}")
        self.logger.info(f"  HQ City: {agency.hq_city or 'NOT FOUND'}")
        self.logger.info(f"  Logo: {'YES' if agency.logo_url else 'NOT FOUND'}")
        self.logger.info(f"  Sectors: {len(agency.sectors_core or [])} found")
        self.logger.info(f"  Certifications: {len(agency.certifications or [])} found")
        self.logger.info(f"  Pages scraped: {len(self.evidence_urls)}")
        
        # Update evidence URLs
        agency.evidence_urls = self.evidence_urls.copy()
        agency.collected_at = self.collected_at
        
        return agency

