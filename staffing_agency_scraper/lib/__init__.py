"""
Shared library utilities for the staffing agency scraper.

This module provides reusable utilities for:
- Fetching web pages
- Parsing HTML
- Extracting Dutch business data (KvK, phone, address)
- Dutch-specific constants (postal codes, provinces)
- Data normalization
"""

from staffing_agency_scraper.lib.dutch import (
    DUTCH_POSTAL_TO_PROVINCE,
    DUTCH_PROVINCES,
    MAJOR_DUTCH_CITIES,
    NON_CITY_WORDS,
    SERVICE_KEYWORDS,
    SECTOR_KEYWORDS,
    CERTIFICATION_KEYWORDS,
    CAO_KEYWORDS,
    CITY_TO_PROVINCE,
    CITY_SLUGS,
    SECTOR_SLUG_TO_NAME,
    get_province_from_postal_code,
    get_province_for_city,
    normalize_sector_slug,
    is_city_slug,
    is_valid_dutch_city,
)

from staffing_agency_scraper.lib.extract import (
    extract_kvk_from_text,
    extract_dutch_phone,
    extract_business_email,
    extract_from_footer,
    extract_structured_data,
    extract_contact_from_page,
    extract_dutch_addresses,
    extract_office_locations,
    extract_hq_city_from_text,
    make_absolute_url,
    normalize_phone,
    is_business_email,
)

from staffing_agency_scraper.lib.fetch import (
    fetch_with_retry,
    get_chrome_user_agent,
    get_default_headers,
)

from staffing_agency_scraper.lib.parse import (
    parse_html,
    clean_text,
    get_text_content,
    get_attribute,
)

__all__ = [
    # Dutch constants
    "DUTCH_POSTAL_TO_PROVINCE",
    "DUTCH_PROVINCES",
    "MAJOR_DUTCH_CITIES",
    "NON_CITY_WORDS",
    "SERVICE_KEYWORDS",
    "SECTOR_KEYWORDS",
    "CERTIFICATION_KEYWORDS",
    "CAO_KEYWORDS",
    "CITY_TO_PROVINCE",
    "CITY_SLUGS",
    "SECTOR_SLUG_TO_NAME",
    # Dutch utilities
    "get_province_from_postal_code",
    "get_province_for_city",
    "normalize_sector_slug",
    "is_city_slug",
    "is_valid_dutch_city",
    # Extraction utilities
    "extract_kvk_from_text",
    "extract_dutch_phone",
    "extract_business_email",
    "extract_from_footer",
    "extract_structured_data",
    "extract_contact_from_page",
    "extract_dutch_addresses",
    "extract_office_locations",
    "extract_hq_city_from_text",
    "make_absolute_url",
    "normalize_phone",
    "is_business_email",
    # Fetch utilities
    "fetch_with_retry",
    "get_chrome_user_agent",
    "get_default_headers",
    # Parse utilities
    "parse_html",
    "clean_text",
    "get_text_content",
    "get_attribute",
]
