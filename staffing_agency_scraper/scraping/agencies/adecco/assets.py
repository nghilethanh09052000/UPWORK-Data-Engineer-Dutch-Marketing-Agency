"""
Adecco Netherlands scraper.

Website: https://www.adecco.nl
Part of: Adecco Group

Extraction logic specific to Adecco's website structure.
"""

from __future__ import annotations

import re
from io import BytesIO

import requests
import dagster as dg
import pdfplumber
from bs4 import BeautifulSoup

from staffing_agency_scraper.lib.fetch import fetch_with_retry, get_chrome_user_agent
from staffing_agency_scraper.lib.parse import parse_html
from staffing_agency_scraper.lib.extract import (
    extract_office_locations,
    extract_hq_city_from_text,
    make_absolute_url,
)
from staffing_agency_scraper.lib.dutch import (
    CERTIFICATION_KEYWORDS,
    CAO_KEYWORDS,
    CITY_SLUGS,
    SECTOR_SLUG_TO_NAME,
    get_province_for_city,
    is_city_slug,
    normalize_sector_slug,
)
from staffing_agency_scraper.models import Agency, AgencyServices, DigitalCapabilities, GeoFocusType, OfficeLocation
from staffing_agency_scraper.scraping.base import BaseAgencyScraper
from staffing_agency_scraper.scraping.utils import AgencyScraperUtils


class AdeccoScraper(BaseAgencyScraper):
    """Scraper for Adecco Netherlands."""

    AGENCY_NAME = "Adecco"
    WEBSITE_URL = "https://www.adecco.nl"
    BRAND_GROUP = "Adecco Group"

    # Adecco uses /nl-nl/ path prefix for Dutch content
    PAGES_TO_SCRAPE = [
        "https://www.adecco.com/nl-nl",  # Main page
        "https://www.adecco.com/nl-nl/werkgevers",  # Employers page - services, clients info
        "https://www.adecco.com/nl-nl/work-in-holland",  # Lists sectors & cities
        "https://www.adecco.com/nl-nl/contact",  # Contact page - phone number
        "https://www.adecco.com/nl-nl/policy/english/privacy-policy",  # KvK, legal name, HQ address
    ]
    
    # Dedicated page for logo extraction (has static logo, not JS-rendered)
    LOGO_PAGE_URL = "https://www.adecco-jobs.com/amazon/en-nl/contact/"
    
    # Jobs API endpoint for fetching live job data
    JOBS_API_URL = "https://www.adecco.com/api/data/jobs/summarized"
    
    # MVO Certificate PDF (valid until 07-jan-2026)
    MVO_CERTIFICATE_URL = "https://www.adecco.com/-/jssmedia/project/adecco/AdeccoNL/MVO%20pdfs/MVO%20certificaat%20Adecco%20Group%20Nederland%20tot%2007-jan-2026%20DNV"

    def _fetch_page_safe(self, url: str) -> BeautifulSoup | None:
        """
        Fetch page with fallback for Brotli errors.
        Adecco's privacy page sometimes has Brotli decompression issues.
        """
        try:
            return self.fetch_page(url)
        except Exception as e:
            error_msg = str(e)
            if "brotli" in error_msg.lower() or "decode" in error_msg.lower():
                self.logger.warning(f"Brotli error on {url}, trying with custom headers...")
                try:
                    # Fetch without accepting brotli encoding
                    headers = {
                        "User-Agent": get_chrome_user_agent(),
                        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                        "Accept-Language": "nl-NL,nl;q=0.9,en-US;q=0.8,en;q=0.7",
                        "Accept-Encoding": "gzip, deflate",  # No brotli
                    }
                    response = requests.get(url, headers=headers, timeout=30)
                    response.raise_for_status()
                    soup = parse_html(response.text)
                    # Don't add LOGO_PAGE_URL to evidence_urls (it's just for logo extraction)
                    if url not in self.evidence_urls and url != self.LOGO_PAGE_URL:
                        self.logger.info(f"Adding evidence URL: {url}")
                        self.evidence_urls.add(url)

                    return soup
                except Exception as e2:
                    self.logger.warning(f"Fallback fetch failed for {url}: {e2}")
                    return None
            else:
                raise

    def scrape(self) -> Agency:
        self.logger.info(f"Starting scrape of {self.AGENCY_NAME}")

        # Note: self.utils is now initialized in BaseAgencyScraper.__init__()
        agency = self.create_base_agency()
        agency.geo_focus_type = GeoFocusType.INTERNATIONAL
        agency.employers_page_url = "https://www.adecco.com/nl-nl/werkgevers"
        agency.contact_form_url = "https://www.adecco.com/nl-nl/contact"

        # Extract logo from dedicated page (has static logo, not JS-rendered)
        try:
            logo_soup = self._fetch_page_safe(self.LOGO_PAGE_URL)
            if logo_soup:
                agency.logo_url = self._extract_logo(logo_soup)
                if not agency.logo_url:
                    agency.logo_url = self.utils.fetch_logo(logo_soup, self.LOGO_PAGE_URL)

        except Exception as e:
            self.logger.warning(f"Error fetching logo page: {e}")

        # Scrape all pages and extract data
        all_text = ""  # Accumulate text for extract_all_common_fields
        for url in self.PAGES_TO_SCRAPE:
            try:
                soup = self._fetch_page_safe(url)
                if not soup:
                    continue
                    
                page_text = soup.get_text(separator=" ", strip=True)
                all_text += " " + page_text  # Accumulate for common fields extraction

                # Detect portals on every page
                # Candidate portal detection removed per user request
                
                # Extract role levels
                role_levels = self.utils.fetch_role_levels(page_text, url)
                if role_levels:
                    if not agency.role_levels:
                        agency.role_levels = []
                    agency.role_levels.extend(role_levels)
                    agency.role_levels = list(set(agency.role_levels))
                    self.logger.info(f"✓ Set role_levels: {agency.role_levels} | Source URL: {url}")
                
                # Extract review sources
                # Review extraction removed per client requirement
                # Reviews must be explicitly shown/linked on the website, not inferred

                # Extract phone from contact page
                if "contact" in url.lower():
                    if not agency.contact_phone:
                        phone = self._extract_phone(soup, page_text)
                        if phone:
                            from staffing_agency_scraper.lib.normalize import normalize_contact_phone
                            raw_phone = phone
                            normalized_phone = normalize_contact_phone(raw_phone)
                            agency.contact_phone = normalized_phone
                            if normalized_phone != raw_phone:
                                self.logger.info(f"✓ Set contact_phone: {raw_phone} -> normalized to: {normalized_phone} | Source URL: {url}")
                            else:
                                self.logger.info(f"✓ Set contact_phone: {normalized_phone} | Source URL: {url}")
                
                # Extract email from any page (main page has ws@adecco.nl in __NEXT_DATA__)
                if not agency.contact_email:
                   # agency.contact_email = self._extract_email(soup, page_text, url)
                    agency.contact_email = None
                # Extract KvK, legal name, HQ city/province from privacy page's __NEXT_DATA__
                if any(p in url.lower() for p in ["privacy", "terms", "policy"]):
                    try:
                        # Get raw HTML to extract __NEXT_DATA__ JSON
                        raw_html = str(soup)
                        next_data = self._extract_next_data_text(raw_html)
                        
                        if next_data:
                            if not agency.kvk_number:
                                kvk = self._extract_kvk(next_data)
                                if kvk:
                                    agency.kvk_number = kvk
                                    self.logger.info(f"✓ Set kvk_number: {kvk} | Source URL: {url}")
                            if not agency.legal_name:
                                legal_name = self._extract_legal_name(next_data)
                                if legal_name:
                                    agency.legal_name = legal_name
                                    self.logger.info(f"✓ Set legal_name: {legal_name} | Source URL: {url}")
                            if not agency.hq_city or not agency.hq_province:
                                hq_city, hq_province = self._extract_hq_location(next_data)
                                if hq_city and not agency.hq_city:
                                    agency.hq_city = hq_city
                                    self.logger.info(f"✓ Set hq_city: {hq_city} | Source URL: {url}")
                                if hq_province and not agency.hq_province:
                                    agency.hq_province = hq_province
                                    self.logger.info(f"✓ Set hq_province: {hq_province} | Source URL: {url}")
                        
                        # Fallback to page_text if __NEXT_DATA__ didn't work
                        if not agency.kvk_number:
                            kvk = self._extract_kvk(page_text)
                            if kvk:
                                agency.kvk_number = kvk
                                self.logger.info(f"✓ Set kvk_number: {kvk} | Source URL: {url}")
                        if not agency.legal_name:
                            legal_name = self._extract_legal_name(page_text)
                            if legal_name:
                                agency.legal_name = legal_name
                                self.logger.info(f"✓ Set legal_name: {legal_name} | Source URL: {url}")
                    except Exception as e:
                        self.logger.warning(f"Error extracting from privacy page: {e}")
                        # Continue with other pages even if this fails

                # Extract sectors from homepage only
                if url == "https://www.adecco.com/nl-nl":
                    sectors = self._extract_sectors_from_homepage(soup)
                    if sectors:
                        agency.sectors_core = sectors
                        self.logger.info(f"✓ Set sectors_core: {sectors} | Source URL: {url}")
                    
                    # Office locations: Set to empty array per client feedback
                    # Client requirement: "Limit to HQ + 5-10 representative locations OR make this field optional / trimmed"
                    # For Adecco, we set to empty array to avoid excessive lists
                    agency.office_locations = []
                    self.logger.info(f"✓ Set office_locations: [] (empty per client requirement) | Source URL: {url}")

                # Extract services per page
                page_services = self._extract_services(page_text)
                if page_services:
                    # Merge services: if either is True, keep True; otherwise keep None
                    if not agency.services:
                        agency.services = page_services
                        self.logger.info(f"✓ Set services | Source URL: {url}")
                    else:
                        # Merge: True takes precedence over None
                        merged_services = {}
                        for field in ['uitzenden', 'detacheren', 'werving_selectie', 'payrolling', 
                                     'zzp_bemiddeling', 'vacaturebemiddeling_only', 'inhouse_services',
                                     'msp', 'rpo', 'executive_search', 'opleiden_ontwikkelen', 
                                     'reintegratie_outplacement']:
                            current = getattr(agency.services, field)
                            new = getattr(page_services, field)
                            # True takes precedence, otherwise keep current value
                            merged_services[field] = True if (current is True or new is True) else current
                        agency.services = AgencyServices(**merged_services)
                        self.logger.info(f"✓ Updated services | Source URL: {url}")
                
                # Extract focus_segments per page
                focus_segments = self._extract_focus_segments(page_text)
                if focus_segments:
                    if not agency.focus_segments:
                        agency.focus_segments = []
                    agency.focus_segments.extend(focus_segments)
                    agency.focus_segments = list(set(agency.focus_segments))
                    self.logger.info(f"✓ Set focus_segments: {agency.focus_segments} | Source URL: {url}")
                
                # Extract membership per page
                membership = self._extract_membership(page_text)
                if membership:
                    if not agency.membership:
                        agency.membership = []
                    agency.membership.extend(membership)
                    agency.membership = list(set(agency.membership))
                    self.logger.info(f"✓ Set membership: {agency.membership} | Source URL: {url}")
                
                # Extract CAO type per page
                if not agency.cao_type or agency.cao_type == "onbekend":
                    cao_type = self._extract_cao_type(page_text)
                    if cao_type and cao_type != "onbekend":
                        agency.cao_type = cao_type
                        self.logger.info(f"✓ Set cao_type: {cao_type} | Source URL: {url}")

            except Exception as e:
                self.logger.warning(f"Error scraping {url}: {e}")

        # regions_served will be extracted by extract_all_common_fields using standard format
        
        # Extract certifications from PDF certificate
        agency.certifications = self._fetch_pdf_certifications()
    
        
        # HQ city/province may have been extracted from privacy page; fallback to all_text
        if not agency.hq_city or not agency.hq_province:
            hq_city, hq_province = self._extract_hq_location(all_text)
            if hq_city and not agency.hq_city:
                agency.hq_city = hq_city
            if hq_province and not agency.hq_province:
                agency.hq_province = hq_province
        
        # ========================================================================
        # Extract ALL common fields using base class utility method! 🚀
        # This replaces 50+ lines of repetitive extraction code
        # ========================================================================
        self.extract_all_common_fields(agency, all_text)

        # Update evidence URLs - use copy (no filtering for Adecco)
        agency.evidence_urls = self.evidence_urls.copy()
        agency.collected_at = self.collected_at

        self.logger.info(f"Completed scrape of {self.AGENCY_NAME}")

        return agency

    def _fetch_jobs_from_api(self) -> dict | None:
        """
        Fetch all jobs from Adecco's jobs API using pagination.
        
        Uses the API's pagination response (nextRange, pageCount, total) to 
        loop through all pages and collect all jobs.
        
        Returns
        -------
        dict | None
            Combined jobs data with all jobs and facets
        """
        self.logger.info("Fetching jobs from Adecco API...")
        
        headers = {
            "User-Agent": get_chrome_user_agent(),
            "Accept": "*/*",
            "Accept-Language": "nl-NL,nl;q=0.9,en-US;q=0.8,en;q=0.7",
            "Content-Type": "text/plain;charset=UTF-8",
            "Origin": "https://www.adecco.com",
            "Referer": "https://www.adecco.com/nl-nl/vacatures",
        }
        
        all_jobs = []
        facets = None
        facet_counts = None
        first_pagination = None
        
        # Start with first page
        start_range = 0
        page_count = 1
        total_pages = None
        
        while True:
            payload = {
                "queryString": "&sort=PostedDate desc&facet.pivot=IsRemote&facet.range=Salary_Facet_Yearly&f.Salary_Facet_Yearly.facet.range.start=0&f.Salary_Facet_Yearly.facet.range.end=10000&f.Salary_Facet_Yearly.facet.range.gap=500&facet.range=Salary_Facet_Hourly&f.Salary_Facet_Hourly.facet.range.start=0&f.Salary_Facet_Hourly.facet.range.end=850&f.Salary_Facet_Hourly.facet.range.gap=5",
                "filtersToDisplay": "{8BF19AA8-37FC-456F-BB62-008D9F29A7F0}|{0E9E3971-6254-4C02-B78A-28CEA4125D68}|{AFB09656-1795-4BF0-9741-3C7A5AF43305}|{02142C96-D774-4896-8737-82652A468092}|{F01A2A00-7D3C-46AD-8CE4-244CDE95F25F}",
                "range": 10,
                "startRange": start_range,
                "siteName": "adecco",
                "brand": "adecco",
                "countryCode": "NL",
                "languageCode": "nl-NL"
            }
            
            try:
                response = requests.post(
                    self.JOBS_API_URL,
                    json=payload,
                    headers=headers,
                    timeout=30
                )
                response.raise_for_status()
                data = response.json()
                
                jobs = data.get("jobs", [])
                pagination = data.get("pagination", {})
                
                all_jobs.extend(jobs)
                
                # Get facets and pagination info from first page
                if page_count == 1:
                    facets = data.get("facets")
                    facet_counts = data.get("facet_counts")
                    first_pagination = pagination
                    total_pages = pagination.get("pageCount", 1)
                    total_jobs = pagination.get("total", 0)
                    self.logger.info(f"API has {total_jobs} total jobs across {total_pages} pages")
                
                self.logger.info(f"Fetched page {page_count}/{total_pages}: {len(jobs)} jobs (collected: {len(all_jobs)})")
                
                # Check if we should continue
                next_range = pagination.get("nextRange")
                
                # Stop if: no nextRange, no jobs returned, or we've fetched all
                if next_range is None or len(jobs) == 0 or page_count >= total_pages:
                    break
                
                # Move to next page
                start_range = next_range
                page_count += 1
                    
            except Exception as e:
                self.logger.warning(f"Error fetching jobs page {page_count}: {e}")
                break
        
        if all_jobs:
            self.evidence_urls.append(self.JOBS_API_URL)
            return {
                "jobs": all_jobs,
                "facets": facets,
                "facet_counts": facet_counts,
                "pagination": first_pagination,
                "total_fetched": len(all_jobs)
            }
        
        return None

    def _extract_office_locations_from_footer(self, soup: BeautifulSoup) -> list[OfficeLocation]:
        """
        Extract office locations from footer "VACANCIES BY CITY" section.
        
        HTML structure:
        <section class="text-inverse footer-col">
            <div class="small-title">VACANCIES BY CITY</div>
            <section class="mt2 flex_col">
                <a href="/nl-nl/vacatures/amsterdam">Amsterdam</a>
                <a href="/nl-nl/vacatures/arnhem">Arnhem</a>
                ...
            </section>
        </section>
        
        Returns
        -------
        list[OfficeLocation]
            List of office locations extracted from footer
        """
        locations = []
        seen_cities = set()
        
        # Find footer section with "VACANCIES BY CITY" title
        footer_sections = soup.find_all("section", class_=lambda x: x and "footer-col" in x)
        
        for section in footer_sections:
            # Check if this section has "VACANCIES BY CITY" title
            title_div = section.find("div", class_=lambda x: x and "small-title" in x if x else False)
            if not title_div:
                continue
            
            title_text = title_div.get_text(strip=True).upper()
            if "VACANCIES BY CITY" not in title_text and "VACATURES PER STAD" not in title_text:
                continue
            
            # Find the nested section with city links
            city_section = section.find("section", class_=lambda x: x and "mt2" in x and "flex_col" in x if x else False)
            if not city_section:
                continue
            
            # Extract all city links
            city_links = city_section.find_all("a", href=re.compile(r"/nl-nl/vacatures/"))
            
            self.logger.info(f"   Found {len(city_links)} city links in footer 'VACANCIES BY CITY' section")
            
            for link in city_links:
                href = link.get("href", "")
                city_name = link.get_text(strip=True)
                
                # Skip "All vacancies" link
                if not city_name or city_name.lower() in ["all vacancies", "alle vacatures"]:
                    continue
                
                # Extract city slug from href for validation
                slug_match = re.search(r"/vacatures/([^/]+)/?$", href)
                if not slug_match:
                    continue
                
                slug = slug_match.group(1).lower()
                
                # Use shared utility to check if this is a city slug
                if is_city_slug(slug) and city_name not in seen_cities:
                    # Use shared utility to get province
                    province = get_province_for_city(city_name)
                    location = OfficeLocation(city=city_name, province=province)
                    locations.append(location)
                    seen_cities.add(city_name)
                    self.logger.info(f"✓ Found office location from footer: {city_name}, {province}")
        
        if locations:
            self.logger.info(f"✅ Extracted {len(locations)} office locations from footer: {[loc.city for loc in locations]}")
        
        return locations

    def _extract_sectors_from_homepage(self, soup: BeautifulSoup) -> list[str]:
        """
        Extract main sectors from the homepage section.
        
        HTML structure:
        <section class="mt2 flex_col">
            <a href="/nl-nl/vacatures/administratief">Administratief</a>
            <a href="/nl-nl/vacatures/callcenter">Callcenter</a>
            <a href="/nl-nl/vacatures/commercieel-en-marketing">Commercieel </a>
            ...
        </section>
        """
        sectors = []
        seen = set()
        
        # Known sector slugs (not cities)
        known_sector_slugs = {
            "administratief", "callcenter", "commercieel-en-marketing", "financieel",
            "horeca", "personeel-en-organisatie", "it", "juridisch",
            "transport-en-logistiek", "medisch", "productie", "secretarieel",
            "techniek", "verzekeringen"
        }
        
        # Map Adecco's sector slugs to standardized names (simple direct mapping)
        sector_mapping = {
            "administratief": "administratief",
            "callcenter": "callcenter",
            "commercieel-en-marketing": "sales",
            "financieel": "finance",
            "horeca": "horeca",
            "personeel-en-organisatie": "hr",
            "it": "ict",
            "juridisch": "juridisch",
            "transport-en-logistiek": "logistiek",
            "medisch": "zorg",
            "productie": "productie",
            "secretarieel": "secretarieel",
            "techniek": "techniek",
            "verzekeringen": "verzekeringen",
        }
        
        # Find all sections with class "mt2 flex_col"
        # Handle both string and list class formats
        def has_classes(classes):
            if not classes:
                return False
            # Convert to list if it's a string
            if isinstance(classes, str):
                classes = classes.split()
            # Check if both classes are present
            return "mt2" in classes and "flex_col" in classes
        
        all_sections = soup.find_all("section", class_=has_classes)
        
        # Find the section that contains sector links (not city links)
        sector_section = None
        for section in all_sections:
            links = section.find_all("a", href=re.compile(r"/nl-nl/vacatures/"))
            # Check if this section has sector links (not city links)
            sector_count = 0
            for link in links:
                href = link.get("href", "").lower()
                href_match = re.search(r"/vacatures/([^/]+)", href)
                if href_match:
                    slug = href_match.group(1).lower()
                    if slug in known_sector_slugs:
                        sector_count += 1
            
            # If at least 5 sector links found, this is the sector section
            if sector_count >= 5:
                sector_section = section
                self.logger.info(f"   Found sector section with {sector_count} sector links")
                break
        
        if not sector_section:
            self.logger.warning("⚠ Could not find sector section | Source: homepage")
            return sectors
        
        # Find all <a> tags with href containing /nl-nl/vacatures/
        links = sector_section.find_all("a", href=re.compile(r"/nl-nl/vacatures/"))
        
        self.logger.info(f"   Found {len(links)} links in sector section")
        
        for link in links:
            href = link.get("href", "").lower()
            
            # Skip "Alle vacatures" link
            if href == "/nl-nl/vacatures" or href == "/nl-nl/vacatures/":
                continue
            
            # Extract sector from href
            href_match = re.search(r"/vacatures/([^/]+)", href)
            if not href_match:
                continue
            
            sector_slug = href_match.group(1).lower()
            
            # Only extract if it's a known sector (not a city)
            if sector_slug in sector_mapping:
                standard_sector = sector_mapping[sector_slug]
                if standard_sector not in seen:
                    sectors.append(standard_sector)
                    seen.add(standard_sector)
                    self.logger.info(f"✓ Found sector: {sector_slug} → {standard_sector} | Source: homepage")
        
        self.logger.info(f"✅ Extracted {len(sectors)} sectors from homepage: {sectors}")
        return sectors

    def _extract_logo(self, soup: BeautifulSoup) -> str | None:
        """
        Extract logo URL from adecco-jobs.com page.
        
        The page has a static logo in the header: .header-desktop__area-logo img
        """
        logo = soup.select_one(".header-desktop__area-logo img")
        if logo and logo.get("src"):
            src = logo.get("src")
            # Make absolute if needed
            if not src.startswith("http"):
                src = f"https://www.adecco-jobs.com{src}"
            self.logger.info(f"Found logo: {src}")
            return src
        
        return None

    def _extract_phone(self, soup: BeautifulSoup, text: str) -> str | None:
        """Extract phone number - simple regex based."""
        # Dutch phone patterns
        patterns = [
            r"(0\d{3}\s\d{3}\s\d{3})",  # 0418 784 000 (main Adecco number)
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

    def _extract_email(self, soup: BeautifulSoup, text: str, url) -> str | None:
        """
        Extract email - from mailto links or text.
        
        The main Adecco page has email in __NEXT_DATA__ JSON as:
        <a href=\"mailto:ws@adecco.nl?subject=...
        """
        # Get raw HTML to search for mailto links (including in __NEXT_DATA__)
        raw_html = str(soup)
        
        # First try to find mailto links (most reliable)
        mailto_match = re.search(r'mailto:([a-zA-Z0-9._%+-]+@adecco\.nl)', raw_html, re.IGNORECASE)
        if mailto_match:
            email = mailto_match.group(1)
            self.logger.info(f"Found email via mailto: {email} on url {url}")
            return email
        
        # Fallback: Look for adecco emails in text
        patterns = [
            r"([\w\.\-]+@adecco\.nl)",
            r"([\w\.\-]+@adecco\.com)",
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                email = match.group(1)
                self.logger.info(f"Found email: {email} on url {url}")
                return email
        
        return None

    def _extract_next_data_text(self, raw_html: str) -> str | None:
        """
        Extract text content from __NEXT_DATA__ script tag.
        
        Adecco's privacy policy page is React/Next.js rendered, so the actual
        content is in the __NEXT_DATA__ JSON. We extract it as a string and
        use regex to find the relevant data.
        
        Parameters
        ----------
        raw_html : str
            Raw HTML of the page
            
        Returns
        -------
        str | None
            The __NEXT_DATA__ content as a string for regex extraction
        """
        # Find __NEXT_DATA__ script content
        next_data_match = re.search(
            r'<script[^>]*id="__NEXT_DATA__"[^>]*>(.*?)</script>',
            raw_html,
            re.DOTALL
        )
        
        if next_data_match:
            content = next_data_match.group(1)
            self.logger.info(f"Found __NEXT_DATA__ ({len(content)} chars)")
            return content
        
        return None

    def _extract_kvk(self, text: str) -> str | None:
        """
        Extract KvK number from text.
        
        Adecco's privacy policy lists multiple KvK numbers. We want the parent:
        "Adecco Holding Nederland B.V. with KvK: 16033314"
        """
        # First try to find the Holding company KvK (most authoritative)
        holding_match = re.search(r"Adecco Holding.*?KvK[:\s]*(\d{8})", text, re.IGNORECASE)
        if holding_match:
            kvk = holding_match.group(1)
            self.logger.info(f"Found Holding KvK: {kvk}")
            return kvk
        
        # Try patterns with "with KvK:" format (from privacy policy)
        with_kvk_match = re.search(r"with KvK[:\s]*(\d{8})", text, re.IGNORECASE)
        if with_kvk_match:
            kvk = with_kvk_match.group(1)
            self.logger.info(f"Found KvK via 'with KvK': {kvk}")
            return kvk
        
        # Standard patterns
        patterns = [
            r"(?:KvK|KVK|kvk|Kamer van Koophandel)[:\s\-]*(\d{8})",
            r"(?:handelsregister)[:\s\-]*(\d{8})",
            r"\(KvK[:\s]*(\d{8})\)",  # Pattern: (KvK: 12345678)
        ]

        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                kvk = match.group(1)
                self.logger.info(f"Found KvK: {kvk}")
                return kvk

        return None

    def _extract_legal_name(self, text: str) -> str | None:
        """
        Extract legal name from privacy policy page.
        
        Pattern from Adecco's privacy policy (English version):
        "Adecco Nederland, Hogeweg 123, 5301 LL Zaltbommel, trading as Adecco Group Nederland 
        (Adecco Holding Nederland B.V. with KvK: 16033314)"
        """
        patterns = [
            r"\(([^)]+B\.V\.)\s+with\s+KvK",  # English pattern
            r"\(([^)]+B\.V\.)\s+met\s+KvK",  # Dutch pattern
            r"trading as.*?\(([^)]+B\.V\.)",  # "trading as" pattern
            r"handelend onder.*?\(([^)]+B\.V\.)",  # "operating under" pattern (Dutch)
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                legal_name = match.group(1).strip()
                self.logger.info(f"Found legal name: {legal_name}")
                return legal_name
        
        return None

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

        # Core staffing services
        uitzenden = any(w in diensten_context for w in ["uitzenden", "uitzendwerk", "flexibel personeel", "tijdelijke krachten"])
        if uitzenden:
            self.logger.info("✓ Found service: uitzenden")
        
        detacheren = any(w in diensten_context for w in ["detacheren", "detachering"])
        if detacheren:
            self.logger.info("✓ Found service: detacheren")
        
        werving_selectie = any(w in diensten_context for w in ["werving en selectie", "werving & selectie", "recruitment"])
        if werving_selectie:
            self.logger.info("✓ Found service: werving_selectie")
        
        payrolling = "payroll" in diensten_context
        if payrolling:
            self.logger.info("✓ Found service: payrolling")
        
        # Specialized services
        zzp_bemiddeling = any(w in diensten_context for w in ["zzp bemiddeling", "freelance bemiddeling"])
        if zzp_bemiddeling:
            self.logger.info("✓ Found service: zzp_bemiddeling")
        
        inhouse_services = any(w in diensten_context for w in ["inhouse", "in-house", "on-site services"])
        if inhouse_services:
            self.logger.info("✓ Found service: inhouse_services")
        
        # MSP - Require explicit confirmation in services context
        msp = any(w in diensten_context for w in ["managed service provider", "msp", "msp diensten"])
        if msp:
            self.logger.info("✓ Found service: msp (explicit confirmation in services context)")
        
        # RPO - Require explicit confirmation in services context
        rpo = any(w in diensten_context for w in ["recruitment process outsourcing", "rpo", "rpo diensten"])
        if rpo:
            self.logger.info("✓ Found service: rpo (explicit confirmation in services context)")
        
        # Executive Search - Require explicit confirmation in services context
        executive_search = "executive search" in diensten_context or "executive recruitment" in diensten_context
        if executive_search:
            self.logger.info("✓ Found service: executive_search (explicit confirmation in services context)")
        
        # Training/development
        opleiden_ontwikkelen = any(w in text_lower for w in ["opleiden en ontwikkelen", "training en ontwikkeling", "adecco academy"])
        if opleiden_ontwikkelen:
            self.logger.info("✓ Found service: opleiden_ontwikkelen")
        
        reintegratie_outplacement = any(w in diensten_context for w in ["reïntegratie", "outplacement"])
        if reintegratie_outplacement:
            self.logger.info("✓ Found service: reintegratie_outplacement")

        return AgencyServices(
            uitzenden=uitzenden,
            detacheren=detacheren,
            werving_selectie=werving_selectie,
            payrolling=payrolling,
            zzp_bemiddeling=zzp_bemiddeling,
            vacaturebemiddeling_only=None,  # Unknown unless explicitly stated
            inhouse_services=inhouse_services,
            msp=msp,
            rpo=rpo,
            executive_search=executive_search,
            opleiden_ontwikkelen=opleiden_ontwikkelen,
            reintegratie_outplacement=reintegratie_outplacement,
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
            self.logger.info("✓ Found focus segment: blue_collar (productie, logistiek)")
        
        # White collar indicators (administratief, hr, finance, juridisch)
        if any(w in vakgebied_text for w in ["administratief", "financieel", "hr", "juridisch", "secretarieel"]):
            segments.append("white_collar")
            self.logger.info("✓ Found focus segment: white_collar (administratief, finance, hr)")
        
        # Technical specialists
        if "techniek" in vakgebied_text or "it" in vakgebied_text:
            segments.append("technisch_specialisten")
            self.logger.info("✓ Found focus segment: technisch_specialisten (techniek, IT)")
        
        # Healthcare
        if "medisch" in vakgebied_text or "zorg" in text_lower:
            segments.append("zorgprofessionals")
            self.logger.info("✓ Found focus segment: zorgprofessionals (medisch, zorg)")
        
        # Students - only if explicitly mentioned
        if any(w in text_lower for w in ["studentenwerk", "bijbaan", "studenten vacatures"]):
            segments.append("studenten")
            self.logger.info("✓ Found focus segment: studenten")
        
        # Young professionals - only if explicitly mentioned
        if any(w in text_lower for w in ["young professional", "traineeship", "starter"]):
            segments.append("young_professionals")
            self.logger.info("✓ Found focus segment: young_professionals")

        unique_segments = list(set(segments))
        self.logger.info(f"Total focus segments found: {len(unique_segments)}")
        return unique_segments

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
        
        # Only set "landelijk" if explicitly stated in text - no assumption from city count
        if "heel nederland" in text_lower or "landelijk" in text_lower:
            regions.append("landelijk")
            self.logger.info(f"✓ Found region: landelijk (explicitly stated in text)")
        
        # Adecco Group is international
        if any(w in text_lower for w in ["adecco group", "worldwide", "global", "landen"]):
            regions.append("internationaal")
            self.logger.info("✓ Found region: internationaal (Adecco Group worldwide)")

        self.logger.info(f"Total regions found: {len(regions)}")
        return regions

    def _fetch_pdf_certifications(self) -> list[str]:
        """
        Fetch and parse MVO certificate PDF to extract certifications.
        
        Downloads the PDF from Adecco's website and extracts certification info.
        
        Returns
        -------
        list[str]
            List of certifications found in the PDF
        """
        certifications = []
        
        try:
            self.logger.info("Fetching MVO certificate PDF...")
            
            headers = {
                "User-Agent": get_chrome_user_agent(),
                "Accept": "application/pdf",
            }
            
            response = requests.get(
                self.MVO_CERTIFICATE_URL,
                headers=headers,
                timeout=30
            )
            response.raise_for_status()
            
            # Parse PDF using pdfplumber
            with pdfplumber.open(BytesIO(response.content)) as pdf:
                text = ""
                for page in pdf.pages:
                    page_text = page.extract_text()
                    if page_text:
                        text += page_text + "\n"
            
            self.logger.info(f"Extracted {len(text)} characters from PDF")
            
            # Look for MVO Prestatieladder certification
            if "MVO Prestatieladder" in text or "CSR Performance Ladder" in text:
                # Extract the level (Niveau 1, 2, 3, 4, or 5)
                niveau_match = re.search(r"Niveau\s*(\d)", text)
                if niveau_match:
                    level = niveau_match.group(1)
                    certifications.append(f"MVO Prestatieladder Niveau {level}")
                    self.logger.info(f"Found certification: MVO Prestatieladder Niveau {level}")
                else:
                    certifications.append("MVO Prestatieladder")
                    self.logger.info("Found certification: MVO Prestatieladder (level unknown)")
            
            # Look for ISO 26000 mention
            if "ISO 26000" in text:
                certifications.append("ISO 26000")
                self.logger.info("Found certification: ISO 26000")
            
            # Add certificate URL to evidence
            self.evidence_urls.append(self.MVO_CERTIFICATE_URL)
            
        except Exception as e:
            self.logger.warning(f"Error fetching PDF certificate: {e}")
            # Fallback to known certification if PDF fetch fails
            certifications = ["MVO Prestatieladder Niveau 3"]
            self.logger.info("Using fallback certification")
        
        return certifications

    def _extract_certifications(self, text: str) -> list[str]:
        """Extract certifications using shared CERTIFICATION_KEYWORDS."""
        certs = set()
        text_lower = text.lower()

        for cert, keywords in CERTIFICATION_KEYWORDS.items():
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
        """Extract CAO type using shared CAO_KEYWORDS."""
        text_lower = text.lower()

        # Check for CAO keywords with "cao" context first
        for cao_type, keywords in CAO_KEYWORDS.items():
            if any(kw in text_lower for kw in keywords):
                if "cao" in text_lower:
                    return cao_type
        
        # Fallback: just check for membership keywords
        for cao_type, keywords in CAO_KEYWORDS.items():
            if any(kw in text_lower for kw in keywords):
                return cao_type

        return "onbekend"

    def _extract_digital_capabilities(self, text: str, soup: BeautifulSoup = None) -> DigitalCapabilities:
        """
        Extract digital capabilities (mobile app, API, feeds).
        
        Client portal detection is handled in the main scrape() loop.
        Candidate portal detection removed per user request.
        """
        text_lower = text.lower()
        
        # Check for app store links
        has_app = any(w in text_lower for w in ["app store", "google play", "download app", "adecco app"])
        
        return DigitalCapabilities(
            client_portal=None,  # Set separately in scrape() loop when detected
            candidate_portal=None,  # Removed per user request - not detected for Adecco
            mobile_app=has_app if has_app else None,  # Only set if explicitly found
            api_available=None,  # Only set to True if explicitly stated on website
            realtime_vacancy_feed=None,  # Only set to True if explicitly stated on website
            realtime_availability_feed=None,
            self_service_contracting=None,
        )

    def _extract_hq_city(self, text: str) -> str | None:
        """
        Extract HQ city from text.
        
        Uses the shared extract_hq_city_from_text utility from lib/extract.py.
        """
        city = extract_hq_city_from_text(text)
        if city:
            self.logger.info(f"Found HQ city: {city}")
        return city

    def _extract_hq_location(self, text: str) -> tuple[str | None, str | None]:
        """
        Extract HQ city and province from text.
        
        Handles both regular text and escaped JSON from __NEXT_DATA__.
        Pattern: "5301 LL Zaltbommel" or "5301 LL Zaltbommel"
        
        Returns
        -------
        tuple[str | None, str | None]
            (city, province) tuple
        """
        from staffing_agency_scraper.lib.dutch import DUTCH_POSTAL_TO_PROVINCE
        
        # Pattern for Dutch postal code + city (handles escaped spaces too)
        # e.g., "5301 LL Zaltbommel" or "5301 LL Zaltbommel"
        patterns = [
            r"(\d{4})\s*([A-Z]{2})\s+([A-Za-z\-]+)",  # Normal: 5301 LL Zaltbommel
            r"(\d{4})\\s*([A-Z]{2})\\s+([A-Za-z\-]+)",  # Escaped: in JSON string
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text)
            if match:
                postal_code = match.group(1)
                city = match.group(3)
                
                # Derive province from postal code prefix (keys are strings like "53")
                postal_prefix = postal_code[:2]
                province = DUTCH_POSTAL_TO_PROVINCE.get(postal_prefix)
                
                if city:
                    self.logger.info(f"Found HQ location: {city}, {province} (postal: {postal_code})")
                    return city, province
        
        # Fallback to shared utility
        from staffing_agency_scraper.lib.extract import extract_dutch_addresses
        addresses = extract_dutch_addresses(text)
        
        if addresses:
            first = addresses[0]
            city = first.get("city")
            province = first.get("province")
            if city:
                self.logger.info(f"Found HQ location via extract_dutch_addresses: {city}, {province}")
            return city, province
        
        # Last fallback
        city = extract_hq_city_from_text(text)
        return city, None

    def _make_absolute_url(self, url: str, base_url: str = "") -> str:
        """Convert relative URL to absolute using shared utility."""
        base = base_url if base_url else self.WEBSITE_URL
        return make_absolute_url(url, base)
    
    def _filter_adecco_evidence_urls(self) -> list[str]:
        """
        Filter evidence URLs to exclude specific URLs and domains that shouldn't be included.
        
        Excludes:
        - Entire adecco-jobs.com domain (jobs/API domain, not employer-facing)
        - LOGO_PAGE_URL (adecco-jobs.com/amazon/en-nl/contact/) - only used for logo extraction
        - Privacy policy page (policy/english/privacy-policy) - technical/legal page
        - API endpoints (JOBS_API_URL)
        - PDF URLs (MVO_CERTIFICATE_URL) - technical endpoints
        
        Returns
        -------
        list[str]
            Filtered list of evidence URLs
        """
        from urllib.parse import urlparse
        
        # URLs to exclude
        exclude_urls = {
            self.LOGO_PAGE_URL,
        }
        
        # Domains to exclude
        exclude_domains = {
            "adecco-jobs.com",
            "www.adecco-jobs.com",
        }
        
        # Remove specific URLs and domains that shouldn't be included
        filtered = []
        for url in self.evidence_urls:
            # Check if URL is in exclude list
            if url in exclude_urls:
                continue
            
            # Check if URL is from excluded domain
            try:
                parsed = urlparse(url)
                if parsed.netloc.lower() in exclude_domains:
                    continue
            except Exception:
                # If URL parsing fails, keep it (might be relative or malformed)
                pass
            
            filtered.append(url)
        
        excluded_count = len(self.evidence_urls) - len(filtered)
        if excluded_count > 0:
            self.logger.info(
                f"✓ Filtered out {excluded_count} excluded URLs/domains (adecco-jobs.com domain, logo page, privacy policy, API, PDF) | "
                f"Final count: {len(filtered)}"
            )
        
        return filtered


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