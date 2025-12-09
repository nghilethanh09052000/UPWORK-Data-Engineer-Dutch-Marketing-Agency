"""
Base scraper class for staffing agencies.

Provides common functionality for all agency scrapers.
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

if TYPE_CHECKING:
    from bs4 import BeautifulSoup


class BaseAgencyScraper(ABC):
    """
    Abstract base class for agency scrapers.

    Each agency scraper should inherit from this class and implement
    the required abstract methods.
    """

    # Agency configuration - override in subclass
    AGENCY_NAME: str = ""
    WEBSITE_URL: str = ""
    BRAND_GROUP: str | None = None

    # Pages to scrape - override in subclass
    PAGES_TO_SCRAPE: list[str] = []

    def __init__(self):
        self.logger = dg.get_dagster_logger(f"{self.__class__.__name__}_scraper")
        self.evidence_urls: list[str] = []
        self.collected_at = datetime.utcnow()

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
                    self.logger.info(f"Found KvK: {agency.kvk_number}")
                
                if not agency.contact_phone and contact_info.get("contact_phone"):
                    agency.contact_phone = contact_info["contact_phone"]
                    self.logger.info(f"Found phone: {agency.contact_phone}")
                
                if not agency.contact_email and contact_info.get("contact_email"):
                    agency.contact_email = contact_info["contact_email"]
                    self.logger.info(f"Found email: {agency.contact_email}")
                
                if not agency.hq_city and contact_info.get("hq_city"):
                    agency.hq_city = contact_info["hq_city"]
                
                if not agency.hq_province and contact_info.get("hq_province"):
                    agency.hq_province = contact_info["hq_province"]
                
                # Extract logo from each page (use first found)
                if not agency.logo_url:
                    logo = self.extract_logo_url(soup)
                    if logo:
                        agency.logo_url = logo
                        self.logger.info(f"Found logo: {agency.logo_url}")
                
                # Collect certifications and sectors
                certs = self.extract_certifications_from_page(soup)
                all_certifications.extend(certs)
                
                sectors = self.extract_sectors_from_page(soup)
                all_sectors.extend(sectors)
                
            except Exception as e:
                self.logger.warning(f"Error scraping {url}: {e}")
        
        # Dedupe and set certifications
        if all_certifications:
            agency.certifications = list(set(all_certifications))
        
        # Dedupe and set sectors
        if all_sectors:
            agency.sectors_core = list(set(all_sectors))[:10]  # Limit to top 10
        
        # Update evidence URLs
        agency.evidence_urls = self.evidence_urls.copy()
        agency.collected_at = self.collected_at
        
        return agency

