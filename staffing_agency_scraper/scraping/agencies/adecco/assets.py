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
                    if url not in self.evidence_urls:
                        self.evidence_urls.add(url)
                    return soup
                except Exception as e2:
                    self.logger.warning(f"Fallback fetch failed for {url}: {e2}")
                    return None
            else:
                raise

    def scrape(self) -> Agency:
        self.logger.info(f"Starting scrape of {self.AGENCY_NAME}")

        # Initialize utils
        self.utils = AgencyScraperUtils(logger=self.logger)

        agency = self.create_base_agency()
        agency.geo_focus_type = GeoFocusType.INTERNATIONAL
        agency.employers_page_url = "https://www.adecco.com/nl-nl/werkgevers"
        agency.contact_form_url = "https://www.adecco.com/nl-nl/contact"

        # Extract logo from dedicated page (has static logo, not JS-rendered)
        try:
            logo_soup = self._fetch_page_safe(self.LOGO_PAGE_URL)
            if logo_soup:
                agency.logo_url = self.utils.fetch_logo(logo_soup, self.LOGO_PAGE_URL)
                if not agency.logo_url:
                    agency.logo_url = self._extract_logo(logo_soup)
        except Exception as e:
            self.logger.warning(f"Error fetching logo page: {e}")

        # Scrape all pages and extract data
        all_text = ""
        for url in self.PAGES_TO_SCRAPE:
            try:
                soup = self._fetch_page_safe(url)
                if not soup:
                    continue
                    
                page_text = soup.get_text(separator=" ", strip=True)
                all_text += " " + page_text

                # Detect portals on every page
                if self.utils.detect_candidate_portal(soup, page_text, url):
                    agency.digital_capabilities.candidate_portal = True
                if self.utils.detect_client_portal(soup, page_text, url):
                    agency.digital_capabilities.client_portal = True
                
                # Extract role levels
                role_levels = self.utils.fetch_role_levels(page_text, url)
                if role_levels:
                    if not agency.role_levels:
                        agency.role_levels = []
                    agency.role_levels.extend(role_levels)
                    agency.role_levels = list(set(agency.role_levels))
                
                # Extract review sources
                review_sources = self.utils.fetch_review_sources(soup, url)
                if review_sources and not agency.review_sources:
                    agency.review_sources = review_sources

                # Extract phone from contact page
                if "contact" in url.lower():
                    if not agency.contact_phone:
                        agency.contact_phone = self._extract_phone(soup, page_text)
                
                # Extract email from any page (main page has ws@adecco.nl in __NEXT_DATA__)
                if not agency.contact_email:
                    agency.contact_email = self._extract_email(soup, page_text)

                # Extract KvK, legal name, HQ city/province from privacy page's __NEXT_DATA__
                if any(p in url.lower() for p in ["privacy", "terms", "policy"]):
                    try:
                        # Get raw HTML to extract __NEXT_DATA__ JSON
                        raw_html = str(soup)
                        next_data = self._extract_next_data_text(raw_html)
                        
                        if next_data:
                            if not agency.kvk_number:
                                agency.kvk_number = self._extract_kvk(next_data)
                            if not agency.legal_name:
                                agency.legal_name = self._extract_legal_name(next_data)
                            if not agency.hq_city or not agency.hq_province:
                                hq_city, hq_province = self._extract_hq_location(next_data)
                                if hq_city and not agency.hq_city:
                                    agency.hq_city = hq_city
                                if hq_province and not agency.hq_province:
                                    agency.hq_province = hq_province
                        
                        # Fallback to page_text if __NEXT_DATA__ didn't work
                        if not agency.kvk_number:
                            agency.kvk_number = self._extract_kvk(page_text)
                        if not agency.legal_name:
                            agency.legal_name = self._extract_legal_name(page_text)
                    except Exception as e:
                        self.logger.warning(f"Error extracting from privacy page: {e}")
                        # Continue with other pages even if this fails

                # Extract towns (office locations) and fields (sectors) from main page
                if url == "https://www.adecco.nl":
                    towns = self._extract_towns_from_homepage(soup)
                    if towns:
                        # Merge with existing office locations
                        existing_cities = {loc.city.lower() for loc in (agency.office_locations or [])}
                        for town in towns:
                            if town.city.lower() not in existing_cities:
                                if agency.office_locations is None:
                                    agency.office_locations = []
                                agency.office_locations.append(town)
                                existing_cities.add(town.city.lower())
                    
                    fields = self._extract_fields_from_homepage(soup)
                    if fields:
                        # Will be merged with sectors later
                        if not hasattr(self, '_homepage_sectors'):
                            self._homepage_sectors = []
                        self._homepage_sectors.extend(fields)

            except Exception as e:
                self.logger.warning(f"Error scraping {url}: {e}")

        # Extract all data from accumulated text
        agency.sectors_core = self._extract_sectors(all_text)
        
        # Add normalized sectors
        normalized_sectors = self.utils.fetch_sectors(all_text, "accumulated_text")
        if normalized_sectors:
            existing = set(agency.sectors_core or [])
            for sector in normalized_sectors:
                if sector not in existing:
                    if agency.sectors_core is None:
                        agency.sectors_core = []
                    agency.sectors_core.append(sector)
                    existing.add(sector)
        
        # Merge homepage sectors if found
        if hasattr(self, '_homepage_sectors') and self._homepage_sectors:
            existing = set(agency.sectors_core or [])
            for sector in self._homepage_sectors:
                if sector not in existing:
                    agency.sectors_core.append(sector)
                    existing.add(sector)
        agency.services = self._extract_services(all_text)
        agency.focus_segments = self._extract_focus_segments(all_text)
        agency.regions_served = self._extract_regions(all_text)
        
        # Extract certifications from PDF certificate
        agency.certifications = self._fetch_pdf_certifications()
        
        agency.membership = self._extract_membership(all_text)
        agency.cao_type = self._extract_cao_type(all_text)
        
        # Extract digital capabilities (mobile app, API, feeds)
        # Note: Portal detection was already done in the scrape loop above
        digital_caps = self._extract_digital_capabilities(all_text)
        agency.digital_capabilities.mobile_app = digital_caps.mobile_app
        agency.digital_capabilities.api_available = digital_caps.api_available
        agency.digital_capabilities.realtime_vacancy_feed = digital_caps.realtime_vacancy_feed
        agency.digital_capabilities.realtime_availability_feed = digital_caps.realtime_availability_feed
        agency.digital_capabilities.self_service_contracting = digital_caps.self_service_contracting
        
        # HQ city/province may have been extracted from privacy page; fallback to all_text
        if not agency.hq_city or not agency.hq_province:
            hq_city, hq_province = self._extract_hq_location(all_text)
            if hq_city and not agency.hq_city:
                agency.hq_city = hq_city
            if hq_province and not agency.hq_province:
                agency.hq_province = hq_province
        
        # Extract additional market positioning fields from aggregated text
        if not agency.company_size_fit:
            agency.company_size_fit = self.utils.fetch_company_size_fit(all_text, "accumulated_text")
        
        if not agency.customer_segments:
            agency.customer_segments = self.utils.fetch_customer_segments(all_text, "accumulated_text")
        
        if not agency.shift_types_supported:
            agency.shift_types_supported = self.utils.fetch_shift_types_supported(all_text, "accumulated_text")
        
        if not agency.typical_use_cases:
            agency.typical_use_cases = self.utils.fetch_typical_use_cases(all_text, "accumulated_text")
        
        if not agency.speed_claims:
            agency.speed_claims = self.utils.fetch_speed_claims(all_text, "accumulated_text")
        
        # Extract pricing & commercial fields
        if agency.pricing_model == "unknown":
            agency.pricing_model = self.utils.fetch_pricing_model(all_text, "accumulated_text")
        
        if not agency.pricing_transparency:
            agency.pricing_transparency = self.utils.fetch_pricing_transparency(all_text, "accumulated_text")
        
        if agency.no_cure_no_pay is None:
            agency.no_cure_no_pay = self.utils.fetch_no_cure_no_pay(all_text, "accumulated_text")
        
        if not agency.omrekenfactor_min and not agency.omrekenfactor_max:
            omrekenfactor_min, omrekenfactor_max = self.utils.fetch_omrekenfactor(all_text, "accumulated_text")
            if omrekenfactor_min:
                agency.omrekenfactor_min = omrekenfactor_min
            if omrekenfactor_max:
                agency.omrekenfactor_max = omrekenfactor_max
        
        # Extract performance metrics
        if not agency.avg_time_to_fill_days:
            agency.avg_time_to_fill_days = self.utils.fetch_avg_time_to_fill(all_text, "accumulated_text")
        
        if not agency.candidate_pool_size_estimate:
            agency.candidate_pool_size_estimate = self.utils.fetch_candidate_pool_size(all_text, "accumulated_text")
        
        if not agency.annual_placements_estimate:
            agency.annual_placements_estimate = self.utils.fetch_annual_placements(all_text, "accumulated_text")
        
        # Extract legal/compliance fields
        if agency.uses_inlenersbeloning is None:
            agency.uses_inlenersbeloning = self.utils.fetch_uses_inlenersbeloning(all_text, "accumulated_text")
        
        if agency.applies_inlenersbeloning_from_day1 is None:
            agency.applies_inlenersbeloning_from_day1 = self.utils.fetch_applies_inlenersbeloning_from_day1(all_text, "accumulated_text")
        
        # Extract assignment conditions
        if not agency.min_assignment_duration_weeks:
            agency.min_assignment_duration_weeks = self.utils.fetch_min_assignment_duration(all_text, "accumulated_text")
        
        if not agency.min_hours_per_week:
            agency.min_hours_per_week = self.utils.fetch_min_hours_per_week(all_text, "accumulated_text")

        # Fetch jobs from API for additional data
        try:
            jobs_data = self._fetch_jobs_from_api()
            if jobs_data:
                self._enrich_from_jobs_data(agency, jobs_data)
        except Exception as e:
            self.logger.warning(f"Error fetching jobs API: {e}")

        # Update evidence URLs
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

    def _enrich_from_jobs_data(self, agency: Agency, jobs_data: dict) -> None:
        """
        Enrich agency data from jobs API response.
        
        Extracts:
        - Office locations from job cities
        - Sectors from job categories
        - Contract types (uitzenden vs permanent)
        - Job count statistics
        
        Parameters
        ----------
        agency : Agency
            Agency object to enrich
        jobs_data : dict
            Jobs API response data
        """
        jobs = jobs_data.get("jobs", [])
        facets = jobs_data.get("facets", {})
        pagination = jobs_data.get("pagination", {})
        
        self.logger.info(f"Enriching agency data from {len(jobs)} jobs...")
        
        # Extract cities from jobs
        cities_from_jobs = {}
        for job in jobs:
            city = job.get("jobLocation")
            if city:
                # Normalize city name (API returns uppercase sometimes)
                city_normalized = city.title()
                if city_normalized not in cities_from_jobs:
                    province = get_province_for_city(city_normalized)
                    cities_from_jobs[city_normalized] = province
        
        # Merge with existing office locations
        existing_cities = {loc.city.lower() for loc in (agency.office_locations or [])}
        for city, province in cities_from_jobs.items():
            if city.lower() not in existing_cities:
                if agency.office_locations is None:
                    agency.office_locations = []
                agency.office_locations.append(OfficeLocation(city=city, province=province))
                existing_cities.add(city.lower())
        
        self.logger.info(f"Added {len(cities_from_jobs)} cities from jobs API")
        
        # Extract sectors from facets
        if facets:
            category_buckets = facets.get("category", {}).get("buckets", [])
            sectors_from_api = []
            
            # Map API category IDs to our standardized sector names
            api_category_map = {
                "Transport en logistiek": "logistiek",
                "Productie": "productie",
                "Techniek": "techniek",
                "Administratief": "administratief",
                "Commercieel en marketing": "sales",
                "Horeca": "horeca",
                "Personeel en organisatie": "hr",
                "Secretarieel": "secretarieel",
                "Callcenter": "callcenter",
                "Detailhandel": "retail",
                "Financieel": "finance",
                "Medisch": "zorg",
                "Bank en verzekeringen": "verzekeringen",
                "IT": "ict",
                "Juridisch": "juridisch",
            }
            
            for bucket in category_buckets:
                val = bucket.get("val", "")
                count = bucket.get("count", 0)
                # Format: "ADCNLCAT011|Transport en logistiek"
                if "|" in val:
                    category_name = val.split("|")[1].strip()
                    if category_name in api_category_map and count > 0:
                        sector = api_category_map[category_name]
                        if sector not in sectors_from_api:
                            sectors_from_api.append(sector)
                            self.logger.info(f"Found sector from API: {category_name} ({count} jobs) -> {sector}")
            
            # Merge with existing sectors
            if sectors_from_api:
                existing_sectors = set(agency.sectors_core or [])
                for sector in sectors_from_api:
                    if sector not in existing_sectors:
                        if agency.sectors_core is None:
                            agency.sectors_core = []
                        agency.sectors_core.append(sector)
                        existing_sectors.add(sector)
        
        # Extract contract type info (services)
        temp_count = 0
        perm_count = 0
        for job in jobs:
            contract_type = job.get("contractTypeId")
            if contract_type == "TEMP":
                temp_count += 1
            elif contract_type == "PERM":
                perm_count += 1
        
        # Update services based on contract types found
        if temp_count > 0:
            agency.services.uitzenden = True
            self.logger.info(f"Found {temp_count} temporary (uitzenden) jobs")
        if perm_count > 0:
            agency.services.werving_selectie = True
            self.logger.info(f"Found {perm_count} permanent (werving & selectie) jobs")
        
        # Note: We don't extract the following from API as they're job-specific, not agency policies:
        # - avg_hourly_rate: API salary is worker wages, not agency rates
        # - annual_placements_estimate: Active jobs ≠ annual placements
        # - min_hours_per_week: Job-specific workMinHours, not agency minimum
        # - role_levels: exeprienceLevel is usually null, educationLevel ≠ role level
        # - shift_types_supported: Unreliable to parse from job titles

    def _extract_towns_from_homepage(self, soup: BeautifulSoup) -> list[OfficeLocation]:
        """
        Extract towns (office locations) from the homepage.
        
        The homepage has /vacatures/{city} links for cities where Adecco operates.
        Uses shared CITY_SLUGS and get_province_for_city from lib/dutch.py.
        """
        locations = []
        seen_cities = set()
        
        # Find all /vacatures/ links
        links = soup.find_all("a", href=re.compile(r"/vacatures/"))
        
        for link in links:
            href = link.get("href", "")
            slug_match = re.search(r"/vacatures/([^/]+)/?$", href)
            if slug_match:
                slug = slug_match.group(1).lower()
                # Use shared utility to check if this is a city slug
                if is_city_slug(slug) and slug not in seen_cities:
                    # Convert slug to proper name
                    city_name = link.get_text(strip=True) or slug.replace("-", " ").title()
                    # Use shared utility to get province
                    province = get_province_for_city(city_name)
                    location = OfficeLocation(city=city_name, province=province)
                    locations.append(location)
                    seen_cities.add(slug)
                    self.logger.info(f"Found town from homepage: {city_name}, {province}")
        
        return locations

    def _extract_fields_from_homepage(self, soup: BeautifulSoup) -> list[str]:
        """
        Extract fields (sectors) from the homepage.
        
        The homepage has /vacatures/{sector} links for sectors.
        Uses shared SECTOR_SLUG_TO_NAME from lib/dutch.py.
        """
        sectors = []
        
        # Find all /vacatures/ links
        links = soup.find_all("a", href=re.compile(r"/vacatures/"))
        
        for link in links:
            href = link.get("href", "")
            slug_match = re.search(r"/vacatures/([^/]+)/?$", href)
            if slug_match:
                slug = slug_match.group(1).lower()
                # Use shared utility to normalize sector slug
                sector = normalize_sector_slug(slug)
                if sector and sector not in sectors:
                    sectors.append(sector)
                    self.logger.info(f"Found field from homepage: {slug} -> {sector}")
        
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

    def _extract_email(self, soup: BeautifulSoup, text: str) -> str | None:
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
            self.logger.info(f"Found email via mailto: {email}")
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
                self.logger.info(f"Found email: {email}")
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
        
        Portal detection is handled in the main scrape() loop.
        """
        text_lower = text.lower()
        
        # Check for app store links
        has_app = any(w in text_lower for w in ["app store", "google play", "download app", "adecco app"])
        
        return DigitalCapabilities(
            client_portal=False,
            candidate_portal=False,
            mobile_app=has_app,
            api_available=False,  # No evidence of public API
            realtime_vacancy_feed=False,  # Would need specific evidence
            realtime_availability_feed=False,
            self_service_contracting=False,
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

    def _extract_office_locations(self, text: str) -> list[OfficeLocation]:
        """
        Extract office locations from privacy policy.
        
        Uses the shared extract_office_locations utility from lib/extract.py
        which handles Dutch postal code to province mapping.
        """
        # Use shared utility
        location_dicts = extract_office_locations(text)
        
        locations = []
        for loc in location_dicts:
            location = OfficeLocation(city=loc["city"], province=loc.get("province"))
            locations.append(location)
            self.logger.info(f"Found office location: {loc['city']}, {loc.get('province')}")
        
        return locations

    def _make_absolute_url(self, url: str, base_url: str = "") -> str:
        """Convert relative URL to absolute using shared utility."""
        base = base_url if base_url else self.WEBSITE_URL
        return make_absolute_url(url, base)


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