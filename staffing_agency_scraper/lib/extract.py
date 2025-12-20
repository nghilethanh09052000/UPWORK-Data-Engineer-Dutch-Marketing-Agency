"""
Advanced data extraction utilities for Dutch staffing agency websites.

Provides specialized extractors for Dutch company data like KvK numbers,
phone numbers, addresses, and business information typically found in
footers and legal pages.
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING
from urllib.parse import urljoin

from bs4 import BeautifulSoup
from bs4.element import Tag

if TYPE_CHECKING:
    pass


def extract_from_footer(soup: BeautifulSoup) -> dict:
    """
    Extract company information from page footer.
    
    Many Dutch sites put KvK, contact info in the footer.
    
    Parameters
    ----------
    soup : BeautifulSoup
        Parsed HTML document
    
    Returns
    -------
    dict
        Extracted information
    """
    result = {
        "kvk_number": None,
        "contact_phone": None,
        "contact_email": None,
        "address": None,
    }
    
    # Find footer element
    footer = soup.find("footer")
    if not footer:
        # Try common footer class names
        footer = soup.find(class_=re.compile(r"footer|Footer|site-footer", re.I))
    
    if not footer:
        # Try div with footer-like content
        footer = soup.find(id=re.compile(r"footer", re.I))
    
    if footer:
        footer_text = footer.get_text(separator=" ", strip=True)
        
        # Extract KvK from footer
        kvk = extract_kvk_from_text(footer_text)
        if kvk:
            result["kvk_number"] = kvk
        
        # Extract phone from footer
        phone = extract_dutch_phone(footer_text)
        if phone:
            result["contact_phone"] = phone
        
        # Extract email from footer
        email = extract_business_email(footer_text)
        if email:
            result["contact_email"] = email
        
        # Also check for mailto links
        email_link = footer.find("a", href=re.compile(r"^mailto:", re.I))
        if email_link:
            href = email_link.get("href", "")
            email_from_link = href.replace("mailto:", "").split("?")[0]
            if is_business_email(email_from_link):
                result["contact_email"] = email_from_link
        
        # Check for tel links
        tel_link = footer.find("a", href=re.compile(r"^tel:", re.I))
        if tel_link:
            href = tel_link.get("href", "")
            phone_from_link = href.replace("tel:", "").strip()
            if phone_from_link:
                result["contact_phone"] = normalize_phone(phone_from_link)
    
    return result


def extract_kvk_from_text(text: str) -> str | None:
    """
    Extract KvK (Kamer van Koophandel) number from text.
    
    KvK numbers are 8 digits, often preceded by identifiers.
    
    Parameters
    ----------
    text : str
        Text to search
    
    Returns
    -------
    str | None
        KvK number or None
    """
    # Common patterns for KvK numbers
    patterns = [
        # "KvK: 12345678" or "KVK 12345678" or "KvK-nummer: 12345678"
        r"(?:KvK|KVK|kvk)[-\s:nummer]*[:\s]*(\d{8})",
        # "Handelsregister: 12345678"
        r"(?:handelsregister|HR)[-\s:]*(\d{8})",
        # "Chamber of Commerce: 12345678"
        r"(?:chamber\s*of\s*commerce)[-\s:]*(\d{8})",
        # "Kamer van Koophandel: 12345678"
        r"(?:kamer\s*van\s*koophandel)[-\s:]*(\d{8})",
        # Standalone 8 digits near "KvK" text
        r"(?:KvK|kvk|KVK)[^\d]*(\d{8})",
    ]
    
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            return match.group(1)
    
    return None


def extract_dutch_phone(text: str) -> str | None:
    """
    Extract Dutch phone number from text.
    
    Dutch phone numbers:
    - Start with 0 (domestic) or +31 (international)
    - Area codes: 010-079 (2-3 digits) for landlines
    - Mobile: 06
    
    Parameters
    ----------
    text : str
        Text to search
    
    Returns
    -------
    str | None
        Normalized phone number or None
    """
    patterns = [
        # +31 format: +31 20 123 4567 or +31(0)20-1234567
        r"\+31\s*\(?\s*0?\s*\)?\s*(\d[\d\s\-\.]{8,})",
        # 0xx format with separators: 020-123 4567 or 020 123 45 67
        r"(0\d{2}[\s\-\.]\d{3}[\s\-\.]?\d{2}[\s\-\.]?\d{2})",
        # 0xx format compact: 0201234567
        r"(0\d{9})",
        # 0x-xxxxxxx format (some areas)
        r"(0\d[\s\-\.]\d{7,8})",
        # Mobile: 06 12345678
        r"(06[\s\-\.]\d{8})",
        r"(06\d{8})",
        # General: 0xx-xxxxxxx
        r"(0\d{1,3}[\s\-\.]\d{6,8})",
    ]
    
    for pattern in patterns:
        match = re.search(pattern, text)
        if match:
            return normalize_phone(match.group(1))
    
    return None


def normalize_phone(phone: str) -> str:
    """
    Normalize a phone number to consistent format.
    
    Parameters
    ----------
    phone : str
        Raw phone number
    
    Returns
    -------
    str
        Normalized phone number
    """
    # Remove all non-digit characters except +
    cleaned = re.sub(r"[^\d+]", "", phone)
    
    # If starts with 31, add +
    if cleaned.startswith("31") and not cleaned.startswith("+"):
        cleaned = "+" + cleaned
    
    # Format as +31 XX XXX XXXX or 0XX XXX XXXX
    if cleaned.startswith("+31"):
        # +31 20 123 4567
        digits = cleaned[3:]
        if len(digits) >= 9:
            return f"+31 {digits[:2]} {digits[2:5]} {digits[5:]}"
    elif cleaned.startswith("0") and len(cleaned) == 10:
        # 020 123 4567
        return f"{cleaned[:3]} {cleaned[3:6]} {cleaned[6:]}"
    
    return cleaned


def extract_business_email(text: str) -> str | None:
    """
    Extract business email address from text.
    
    Prefers generic business emails (info@, contact@, werkgevers@).
    
    Parameters
    ----------
    text : str
        Text to search
    
    Returns
    -------
    str | None
        Email address or None
    """
    # Find all email addresses
    pattern = r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}"
    emails = re.findall(pattern, text)
    
    if not emails:
        return None
    
    # Prefer business emails
    business_prefixes = [
        "info@", "contact@", "werkgevers@", "sales@", 
        "service@", "klantenservice@", "bedrijven@"
    ]
    
    for email in emails:
        email_lower = email.lower()
        for prefix in business_prefixes:
            if email_lower.startswith(prefix):
                return email
    
    # Return first non-personal email
    for email in emails:
        if is_business_email(email):
            return email
    
    return emails[0] if emails else None


def is_business_email(email: str) -> bool:
    """
    Check if email is a business email (not personal).
    
    Parameters
    ----------
    email : str
        Email address to check
    
    Returns
    -------
    bool
        True if business email
    """
    personal_domains = [
        "gmail.com", "hotmail.com", "outlook.com", "yahoo.com",
        "live.com", "icloud.com", "me.com"
    ]
    
    email_lower = email.lower()
    
    # Check domain
    domain = email_lower.split("@")[-1]
    if domain in personal_domains:
        return False
    
    # Check for personal name patterns
    local_part = email_lower.split("@")[0]
    if re.match(r"^[a-z]+\.[a-z]+$", local_part):  # firstname.lastname
        return False
    
    return True


def extract_from_meta_tags(soup: BeautifulSoup) -> dict:
    """
    Extract information from HTML meta tags.
    
    Parameters
    ----------
    soup : BeautifulSoup
        Parsed HTML document
    
    Returns
    -------
    dict
        Extracted information
    """
    result = {
        "description": None,
        "og_image": None,
        "canonical_url": None,
    }
    
    # Meta description
    desc_tag = soup.find("meta", attrs={"name": "description"})
    if desc_tag:
        result["description"] = desc_tag.get("content")
    
    # OG image (logo)
    og_image = soup.find("meta", attrs={"property": "og:image"})
    if og_image:
        result["og_image"] = og_image.get("content")
    
    # Canonical URL
    canonical = soup.find("link", attrs={"rel": "canonical"})
    if canonical:
        result["canonical_url"] = canonical.get("href")
    
    return result


def extract_structured_data(soup: BeautifulSoup) -> dict:
    """
    Extract JSON-LD structured data from page.
    
    Many sites include schema.org Organization data.
    
    Parameters
    ----------
    soup : BeautifulSoup
        Parsed HTML document
    
    Returns
    -------
    dict
        Extracted structured data
    """
    import json
    
    result = {}
    
    # Find JSON-LD scripts
    scripts = soup.find_all("script", type="application/ld+json")
    
    for script in scripts:
        try:
            data = json.loads(script.string)
            
            # Handle array of items
            if isinstance(data, list):
                for item in data:
                    result.update(_extract_org_data(item))
            else:
                result.update(_extract_org_data(data))
        except (json.JSONDecodeError, TypeError):
            continue
    
    return result


def _extract_org_data(data: dict) -> dict:
    """Extract organization data from JSON-LD."""
    result = {}
    
    if not isinstance(data, dict):
        return result
    
    schema_type = data.get("@type", "")
    
    # Organization or LocalBusiness
    if schema_type in ("Organization", "LocalBusiness", "Corporation"):
        if "name" in data:
            result["legal_name"] = data["name"]
        if "logo" in data:
            logo = data["logo"]
            if isinstance(logo, dict):
                result["logo_url"] = logo.get("url")
            else:
                result["logo_url"] = logo
        if "telephone" in data:
            result["contact_phone"] = data["telephone"]
        if "email" in data:
            result["contact_email"] = data["email"]
        if "address" in data:
            addr = data["address"]
            if isinstance(addr, dict):
                result["hq_city"] = addr.get("addressLocality")
                result["hq_province"] = addr.get("addressRegion")
    
    return result


def extract_contact_from_page(soup: BeautifulSoup) -> dict:
    """
    Extract all contact information from a page.
    
    Combines multiple extraction methods.
    
    Parameters
    ----------
    soup : BeautifulSoup
        Parsed HTML document
    
    Returns
    -------
    dict
        Combined contact information
    """
    result = {
        "kvk_number": None,
        "contact_phone": None,
        "contact_email": None,
        "hq_city": None,
        "hq_province": None,
        "logo_url": None,
        "legal_name": None,
    }
    
    # Try structured data first (most reliable)
    structured = extract_structured_data(soup)
    for key, value in structured.items():
        if value and key in result:
            result[key] = value
    
    # Try footer extraction
    footer_data = extract_from_footer(soup)
    for key, value in footer_data.items():
        if value and not result.get(key):
            result[key] = value
    
    # Try full page text for remaining fields
    page_text = soup.get_text(separator=" ", strip=True)
    
    if not result["kvk_number"]:
        result["kvk_number"] = extract_kvk_from_text(page_text)
    
    if not result["contact_phone"]:
        result["contact_phone"] = extract_dutch_phone(page_text)
    
    if not result["contact_email"]:
        result["contact_email"] = extract_business_email(page_text)
    
    # Try meta tags for logo
    if not result["logo_url"]:
        meta = extract_from_meta_tags(soup)
        if meta.get("og_image"):
            result["logo_url"] = meta["og_image"]
    
    return result


def find_page_urls_by_pattern(
    soup: BeautifulSoup, 
    patterns: list[str],
    base_url: str,
) -> list[str]:
    """
    Find URLs matching patterns in page links.
    
    Parameters
    ----------
    soup : BeautifulSoup
        Parsed HTML document
    patterns : list[str]
        Regex patterns to match in URLs or link text
    base_url : str
        Base URL for resolving relative links
    
    Returns
    -------
    list[str]
        List of matching URLs
    """
    found_urls = []
    
    for link in soup.find_all("a", href=True):
        href = str(link.get("href", ""))
        text = link.get_text(strip=True).lower()
        
        for pattern in patterns:
            if re.search(pattern, href, re.I) or re.search(pattern, text, re.I):
                full_url = urljoin(base_url, href)
                if full_url not in found_urls:
                    found_urls.append(full_url)
                break
    
    return found_urls


# =============================================================================
# DUTCH ADDRESS EXTRACTION
# =============================================================================

def extract_dutch_addresses(text: str) -> list[dict]:
    """
    Extract Dutch addresses from text.
    
    Dutch addresses typically have format:
    "Street Name 123, 1234 AB CityName"
    
    Parameters
    ----------
    text : str
        Text to search
    
    Returns
    -------
    list[dict]
        List of address dictionaries with keys: postal_code, city, province
    """
    from staffing_agency_scraper.lib.dutch import (
        DUTCH_POSTAL_TO_PROVINCE,
        NON_CITY_WORDS,
    )
    
    addresses = []
    seen_cities = set()
    
    # Pattern matches: 4 digits, space, 2 letters, space, city name
    # Examples: "5301 LL Zaltbommel", "1012 AB Amsterdam"
    address_pattern = re.compile(
        r"(\d{4})\s*([A-Z]{2})\s+([A-Za-z\-']+)",
        re.IGNORECASE
    )
    
    for match in address_pattern.finditer(text):
        postal_code = match.group(1)
        postal_letters = match.group(2).upper()
        city = match.group(3).strip()
        
        # Skip non-city words
        if city.lower() in NON_CITY_WORDS:
            continue
        
        # Avoid duplicates
        if city.lower() in seen_cities:
            continue
        seen_cities.add(city.lower())
        
        # Get province from postal code prefix
        prefix = postal_code[:2]
        province = DUTCH_POSTAL_TO_PROVINCE.get(prefix)
        
        addresses.append({
            "postal_code": f"{postal_code} {postal_letters}",
            "city": city,
            "province": province,
        })
    
    return addresses


def extract_office_locations(text: str) -> list[dict]:
    """
    Extract office locations from text.
    
    Returns locations with city and province derived from postal codes.
    
    Parameters
    ----------
    text : str
        Text to search (e.g., privacy policy, contact page)
    
    Returns
    -------
    list[dict]
        List of location dictionaries with keys: city, province
    """
    addresses = extract_dutch_addresses(text)
    
    locations = []
    for addr in addresses:
        locations.append({
            "city": addr["city"],
            "province": addr["province"],
        })
    
    return locations


def extract_hq_city_from_text(text: str) -> str | None:
    """
    Extract headquarters city from text.
    
    Looks for addresses in common HQ-related patterns.
    
    Parameters
    ----------
    text : str
        Text to search
    
    Returns
    -------
    str | None
        City name or None
    """
    from staffing_agency_scraper.lib.dutch import NON_CITY_WORDS
    
    # Pattern for Dutch postal code followed by city
    # Format: 1234 AB CityName
    postcode_city = re.search(r"(\d{4})\s*([A-Z]{2})\s+([A-Za-z\-']+)", text)
    if postcode_city:
        city = postcode_city.group(3).strip()
        if city.lower() not in NON_CITY_WORDS:
            return city
    
    # Pattern: "Street 123, PostalCode City" after company name
    company_address = re.search(
        r"(?:B\.?V\.?|N\.?V\.?|Nederland)[,\s]+[^,]+,\s*\d{4}\s*[A-Z]{2}\s+([A-Za-z\-']+)", 
        text, 
        re.IGNORECASE
    )
    if company_address:
        city = company_address.group(1).strip()
        if city.lower() not in NON_CITY_WORDS:
            return city
    
    # Fallback patterns
    patterns = [
        r"(?:hoofdkantoor|head office|kantoor)[:\s]+[^,]*,?\s*\d{4}\s*[A-Z]{2}\s+([A-Za-z\s\-']+)",
        r"(?:gevestigd|located)[:\s]+(?:in|te)\s+([A-Za-z\s\-']+)",
    ]

    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            city = match.group(1).strip()
            if city.lower() not in NON_CITY_WORDS:
                return city

    return None


def make_absolute_url(url: str, base_url: str) -> str:
    """
    Convert a relative URL to absolute.
    
    Parameters
    ----------
    url : str
        URL to convert (may be relative or absolute)
    base_url : str
        Base URL for resolving relative URLs
    
    Returns
    -------
    str
        Absolute URL
    """
    from urllib.parse import urlparse
    
    if not url:
        return url
    
    # Already absolute
    if url.startswith("http://") or url.startswith("https://"):
        return url
    
    # Protocol-relative
    if url.startswith("//"):
        return f"https:{url}"
    
    # Relative URL
    if url.startswith("/"):
        # Extract origin from base_url
        if base_url.startswith("http"):
            parsed = urlparse(base_url)
            origin = f"{parsed.scheme}://{parsed.netloc}"
            return urljoin(origin, url)
    
    # Relative without leading slash
    return urljoin(base_url, url)

