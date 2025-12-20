"""
HTML parsing utilities for extracting data from web pages.

Provides helpers for common extraction patterns using BeautifulSoup.
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING

from bs4 import BeautifulSoup
from bs4.element import Tag

if TYPE_CHECKING:
    pass


def parse_html(html: str, parser: str = "lxml") -> BeautifulSoup:
    """
    Parse HTML content into a BeautifulSoup object.

    Parameters
    ----------
    html : str
        HTML content to parse
    parser : str
        Parser to use (lxml, html.parser, html5lib)

    Returns
    -------
    BeautifulSoup
        Parsed HTML document
    """
    return BeautifulSoup(html, parser)


def get_text_content(element: Tag | None, strip: bool = True) -> str | None:
    """
    Safely get text content from a BeautifulSoup element.

    Parameters
    ----------
    element : Tag | None
        BeautifulSoup element
    strip : bool
        Whether to strip whitespace

    Returns
    -------
    str | None
        Text content or None if element is None
    """
    if element is None:
        return None
    text = element.get_text(strip=strip)
    return text if text else None


def get_attribute(element: Tag | None, attr: str) -> str | None:
    """
    Safely get an attribute from a BeautifulSoup element.

    Parameters
    ----------
    element : Tag | None
        BeautifulSoup element
    attr : str
        Attribute name to get

    Returns
    -------
    str | None
        Attribute value or None
    """
    if element is None:
        return None
    value = element.get(attr)
    if isinstance(value, list):
        return value[0] if value else None
    return str(value) if value else None


def find_by_text(
    soup: BeautifulSoup,
    tag: str,
    text: str,
    exact: bool = False,
) -> Tag | None:
    """
    Find an element by its text content.

    Parameters
    ----------
    soup : BeautifulSoup
        Parsed HTML document
    tag : str
        Tag name to search
    text : str
        Text to search for
    exact : bool
        Whether to match exactly or contains

    Returns
    -------
    Tag | None
        Found element or None
    """
    if exact:
        return soup.find(tag, string=text)
    return soup.find(tag, string=lambda t: t and text.lower() in t.lower())


def extract_email(text: str) -> str | None:
    """
    Extract email address from text.

    Parameters
    ----------
    text : str
        Text to search

    Returns
    -------
    str | None
        Email address or None
    """
    pattern = r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}"
    match = re.search(pattern, text)
    return match.group(0) if match else None


def extract_phone(text: str) -> str | None:
    """
    Extract Dutch phone number from text.

    Parameters
    ----------
    text : str
        Text to search

    Returns
    -------
    str | None
        Phone number or None
    """
    # Dutch phone patterns
    patterns = [
        r"\+31\s*\d[\d\s-]{8,}",  # +31 format
        r"0\d{1,3}[-\s]?\d{6,8}",  # 0xx format
        r"\(\d{3}\)\s*\d{3}[-\s]?\d{4}",  # (0xx) format
    ]
    for pattern in patterns:
        match = re.search(pattern, text)
        if match:
            # Normalize the phone number
            phone = re.sub(r"[\s-]", "", match.group(0))
            return phone
    return None


def extract_kvk_number(text: str) -> str | None:
    """
    Extract KvK (Chamber of Commerce) number from text.

    KvK numbers are 8 digits.

    Parameters
    ----------
    text : str
        Text to search

    Returns
    -------
    str | None
        KvK number or None
    """
    # KvK number is 8 digits, often preceded by "KvK" or "KVK"
    patterns = [
        r"(?:KvK|KVK|kvk)[:\s]*(\d{8})",
        r"(?:handelsregister|HR)[:\s]*(\d{8})",
        r"\b(\d{8})\b",  # Standalone 8 digits
    ]
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            return match.group(1)
    return None


def extract_urls_from_page(soup: BeautifulSoup, base_url: str) -> list[str]:
    """
    Extract all internal links from a page.

    Parameters
    ----------
    soup : BeautifulSoup
        Parsed HTML document
    base_url : str
        Base URL for resolving relative links

    Returns
    -------
    list[str]
        List of URLs
    """
    from urllib.parse import urljoin, urlparse

    base_domain = urlparse(base_url).netloc
    urls = set()

    for link in soup.find_all("a", href=True):
        href = link.get("href")
        if href:
            full_url = urljoin(base_url, str(href))
            parsed = urlparse(full_url)
            # Only include same-domain links
            if parsed.netloc == base_domain and parsed.scheme in ("http", "https"):
                urls.add(full_url)

    return sorted(urls)


def clean_text(text: str | None) -> str | None:
    """
    Clean text by removing extra whitespace and normalizing.

    Parameters
    ----------
    text : str | None
        Text to clean

    Returns
    -------
    str | None
        Cleaned text or None
    """
    if text is None:
        return None
    # Replace multiple whitespace with single space
    cleaned = re.sub(r"\s+", " ", text).strip()
    return cleaned if cleaned else None

