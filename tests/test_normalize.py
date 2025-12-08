"""Tests for normalization utilities."""

from staffing_agency_scraper.lib.normalize import (
    detect_cao_type,
    detect_certifications,
    detect_focus_segments,
    detect_services,
    extract_sectors_from_text,
    normalize_geo_focus,
    normalize_province,
    normalize_sector,
)


def test_normalize_province():
    """Test province normalization."""
    assert normalize_province("noord-holland") == "Noord-Holland"
    assert normalize_province("Zuid-Holland") == "Zuid-Holland"
    assert normalize_province("NH") == "Noord-Holland"
    assert normalize_province("unknown") is None


def test_normalize_sector():
    """Test sector normalization."""
    assert normalize_sector("logistiek") == "logistiek"
    assert normalize_sector("transport") == "logistiek"
    assert normalize_sector("zorg") == "zorg"
    assert normalize_sector("unknown") is None


def test_extract_sectors_from_text():
    """Test sector extraction from text."""
    text = "Wij leveren personeel voor logistiek, productie en de zorg sector."
    sectors = extract_sectors_from_text(text)

    assert "logistiek" in sectors
    assert "productie" in sectors
    assert "zorg" in sectors


def test_detect_services():
    """Test service detection."""
    text = "Wij bieden uitzenden, detachering en werving & selectie diensten."
    services = detect_services(text)

    assert services["uitzenden"] is True
    assert services["detacheren"] is True
    assert services["werving_selectie"] is True
    assert services["payrolling"] is False


def test_detect_certifications():
    """Test certification detection."""
    text = "Wij zijn gecertificeerd met NEN-4400-1 en hebben SNA keurmerk."
    certs = detect_certifications(text)

    assert "NEN-4400-1" in certs
    assert "SNA" in certs


def test_detect_cao_type():
    """Test CAO type detection."""
    assert detect_cao_type("Wij werken volgens ABU CAO") == "ABU"
    assert detect_cao_type("NBBU lidmaatschap") == "NBBU"
    assert detect_cao_type("No CAO mentioned") == "onbekend"


def test_normalize_geo_focus():
    """Test geographic focus normalization."""
    assert normalize_geo_focus("landelijke dekking") == "national"
    assert normalize_geo_focus("regio Rotterdam") == "regional"
    assert normalize_geo_focus("internationaal actief in Europa") == "international"
    assert normalize_geo_focus("lokaal bureau") == "local"


def test_detect_focus_segments():
    """Test focus segment detection."""
    text = "Specialisten in studenten, logistiek personeel en zorgprofessionals."
    segments = detect_focus_segments(text)

    assert "studenten" in segments
    assert "blue_collar" in segments
    assert "zorgprofessionals" in segments

