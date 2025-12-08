"""Tests for agency models."""

from datetime import datetime
from uuid import UUID

from staffing_agency_scraper.models import (
    Agency,
    AgencyServices,
    DigitalCapabilities,
    OfficeLocation,
)


def test_agency_creation():
    """Test creating a basic Agency model."""
    agency = Agency(
        agency_name="Test Agency",
        website_url="https://www.test-agency.nl",
    )

    assert agency.agency_name == "Test Agency"
    assert agency.website_url == "https://www.test-agency.nl"
    assert isinstance(agency.id, UUID)
    assert isinstance(agency.collected_at, datetime)


def test_agency_with_services():
    """Test Agency with services."""
    services = AgencyServices(
        uitzenden=True,
        detacheren=True,
        werving_selectie=True,
    )

    agency = Agency(
        agency_name="Test Agency",
        website_url="https://www.test-agency.nl",
        services=services,
    )

    assert agency.services.uitzenden is True
    assert agency.services.detacheren is True
    assert agency.services.werving_selectie is True
    assert agency.services.payrolling is False  # Default


def test_agency_with_locations():
    """Test Agency with office locations."""
    locations = [
        OfficeLocation(city="Amsterdam", province="Noord-Holland"),
        OfficeLocation(city="Rotterdam", province="Zuid-Holland"),
    ]

    agency = Agency(
        agency_name="Test Agency",
        website_url="https://www.test-agency.nl",
        office_locations=locations,
    )

    assert len(agency.office_locations) == 2
    assert agency.office_locations[0].city == "Amsterdam"


def test_agency_to_json():
    """Test Agency JSON serialization."""
    agency = Agency(
        agency_name="Test Agency",
        website_url="https://www.test-agency.nl",
        sectors_core=["logistiek", "productie"],
        certifications=["NEN-4400-1", "SNA"],
    )

    json_dict = agency.to_json_dict()

    assert json_dict["agency_name"] == "Test Agency"
    assert json_dict["sectors_core"] == ["logistiek", "productie"]
    assert isinstance(json_dict["id"], str)  # UUID serialized to string
    assert isinstance(json_dict["collected_at"], str)  # datetime to ISO string


def test_digital_capabilities():
    """Test DigitalCapabilities model."""
    caps = DigitalCapabilities(
        client_portal=True,
        mobile_app=True,
    )

    assert caps.client_portal is True
    assert caps.mobile_app is True
    assert caps.api_available is False  # Default

