"""Tests for parsing utilities."""

from staffing_agency_scraper.lib.parse import (
    clean_text,
    extract_email,
    extract_kvk_number,
    extract_phone,
    parse_html,
)


def test_parse_html():
    """Test HTML parsing."""
    html = "<html><body><h1>Test</h1><p>Content</p></body></html>"
    soup = parse_html(html)

    assert soup.find("h1").get_text() == "Test"
    assert soup.find("p").get_text() == "Content"


def test_extract_email():
    """Test email extraction."""
    text = "Contact us at info@example.nl or sales@company.com"
    email = extract_email(text)

    assert email == "info@example.nl"


def test_extract_phone():
    """Test Dutch phone number extraction."""
    # Test +31 format
    text1 = "Bel ons: +31 20 123 4567"
    assert extract_phone(text1) is not None

    # Test 0xx format
    text2 = "Telefoon: 020-1234567"
    assert extract_phone(text2) is not None

    # Test no phone
    text3 = "No phone number here"
    assert extract_phone(text3) is None


def test_extract_kvk_number():
    """Test KvK number extraction."""
    text1 = "KvK: 12345678"
    assert extract_kvk_number(text1) == "12345678"

    text2 = "Handelsregister 87654321"
    assert extract_kvk_number(text2) == "87654321"


def test_clean_text():
    """Test text cleaning."""
    text = "  Multiple   spaces  and \n newlines  "
    cleaned = clean_text(text)

    assert cleaned == "Multiple spaces and newlines"
    assert clean_text(None) is None
    assert clean_text("") is None

