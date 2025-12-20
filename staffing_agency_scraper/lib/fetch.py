"""
HTTP fetching utilities for the scraper.

Provides functions to fetch web pages with proper headers,
user agent rotation, and error handling.
"""

from __future__ import annotations

import time
from typing import TYPE_CHECKING

import requests
import truststore
from fake_useragent import UserAgent
from requests import Response

truststore.inject_into_ssl()

if TYPE_CHECKING:
    pass


def get_chrome_user_agent() -> str:
    """Get a random Chrome user agent string."""
    ua = UserAgent()
    return str(ua.chrome)


def get_default_headers(referer: str | None = None) -> dict[str, str]:
    """
    Get default headers for HTTP requests that mimic a real browser.
    
    Note: We intentionally avoid Sec-Fetch-* headers as they can trigger
    bot detection on some websites.

    Parameters
    ----------
    referer : str, optional
        The referer URL to include in headers

    Returns
    -------
    dict[str, str]
        Headers dictionary
    """
    headers = {
        "User-Agent": get_chrome_user_agent(),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "nl-NL,nl;q=0.9,en-US;q=0.8,en;q=0.7",
        "Connection": "keep-alive",
    }

    if referer:
        headers["Referer"] = referer

    return headers


def fetch_url(
    url: str,
    headers: dict[str, str] | None = None,
    timeout: int = 30,
    verify: bool = True,
) -> Response:
    """
    Fetch a URL with Chrome-like headers.

    Parameters
    ----------
    url : str
        The URL to fetch
    headers : dict[str, str], optional
        Custom headers to use (merged with defaults)
    timeout : int
        Request timeout in seconds
    verify : bool
        Whether to verify SSL certificates

    Returns
    -------
    Response
        The requests Response object
    """
    default_headers = get_default_headers()
    if headers:
        default_headers.update(headers)

    response = requests.get(
        url=url,
        headers=default_headers,
        timeout=timeout,
        verify=verify,
    )

    return response


def fetch_with_retry(
    url: str,
    max_retries: int = 3,
    delay: float = 1.0,
    headers: dict[str, str] | None = None,
    timeout: int = 30,
) -> Response:
    """
    Fetch a URL with retry logic.

    Parameters
    ----------
    url : str
        The URL to fetch
    max_retries : int
        Maximum number of retry attempts
    delay : float
        Delay between retries in seconds
    headers : dict[str, str], optional
        Custom headers to use
    timeout : int
        Request timeout in seconds

    Returns
    -------
    Response
        The requests Response object

    Raises
    ------
    requests.RequestException
        If all retries fail
    """
    last_exception: Exception | None = None

    for attempt in range(max_retries):
        try:
            response = fetch_url(url, headers=headers, timeout=timeout)
            response.raise_for_status()
            return response
        except requests.RequestException as e:
            last_exception = e
            if attempt < max_retries - 1:
                time.sleep(delay * (attempt + 1))  # Exponential backoff

    raise last_exception if last_exception else RuntimeError(f"Failed to fetch {url}")


def create_session(headers: dict[str, str] | None = None) -> requests.Session:
    """
    Create a requests Session with default headers.

    Parameters
    ----------
    headers : dict[str, str], optional
        Custom headers to set on the session

    Returns
    -------
    requests.Session
        Configured session object
    """
    session = requests.Session()
    session.headers.update(get_default_headers())
    if headers:
        session.headers.update(headers)
    return session

