"""
Playwright browser utilities for scraping JavaScript-heavy pages.

Provides context managers and helpers for browser automation.
"""

from __future__ import annotations

import contextlib
from typing import TYPE_CHECKING, Generator

from playwright.sync_api import Browser, BrowserContext, Page, sync_playwright

if TYPE_CHECKING:
    pass


@contextlib.contextmanager
def get_browser(headless: bool = True) -> Generator[Browser, None, None]:
    """
    Context manager for creating a Playwright browser instance.

    Parameters
    ----------
    headless : bool
        Whether to run in headless mode

    Yields
    ------
    Browser
        Playwright browser instance
    """
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=headless)
        try:
            yield browser
        finally:
            browser.close()


@contextlib.contextmanager
def get_browser_context(
    headless: bool = True,
    locale: str = "nl-NL",
    timezone: str = "Europe/Amsterdam",
) -> Generator[BrowserContext, None, None]:
    """
    Context manager for creating a browser context with Dutch locale.

    Parameters
    ----------
    headless : bool
        Whether to run in headless mode
    locale : str
        Browser locale
    timezone : str
        Browser timezone

    Yields
    ------
    BrowserContext
        Playwright browser context
    """
    with get_browser(headless=headless) as browser:
        context = browser.new_context(
            locale=locale,
            timezone_id=timezone,
            viewport={"width": 1920, "height": 1080},
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
        )
        try:
            yield context
        finally:
            context.close()


@contextlib.contextmanager
def get_page(
    headless: bool = True,
    locale: str = "nl-NL",
    timezone: str = "Europe/Amsterdam",
) -> Generator[Page, None, None]:
    """
    Context manager for creating a browser page with Dutch locale.

    Parameters
    ----------
    headless : bool
        Whether to run in headless mode
    locale : str
        Browser locale
    timezone : str
        Browser timezone

    Yields
    ------
    Page
        Playwright page instance
    """
    with get_browser_context(headless=headless, locale=locale, timezone=timezone) as context:
        page = context.new_page()
        try:
            yield page
        finally:
            page.close()


def wait_for_page_load(page: Page, timeout: int = 30000) -> None:
    """
    Wait for page to fully load.

    Parameters
    ----------
    page : Page
        Playwright page instance
    timeout : int
        Timeout in milliseconds
    """
    page.wait_for_load_state("networkidle", timeout=timeout)


def scroll_to_bottom(page: Page) -> None:
    """
    Scroll to the bottom of the page to trigger lazy loading.

    Parameters
    ----------
    page : Page
        Playwright page instance
    """
    page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
    page.wait_for_timeout(1000)  # Wait for lazy-loaded content


def get_page_html(page: Page) -> str:
    """
    Get the full HTML content of the page.

    Parameters
    ----------
    page : Page
        Playwright page instance

    Returns
    -------
    str
        HTML content of the page
    """
    return page.content()


def click_and_wait(page: Page, selector: str, timeout: int = 5000) -> None:
    """
    Click an element and wait for navigation.

    Parameters
    ----------
    page : Page
        Playwright page instance
    selector : str
        CSS selector of element to click
    timeout : int
        Timeout in milliseconds
    """
    page.click(selector)
    page.wait_for_load_state("networkidle", timeout=timeout)

