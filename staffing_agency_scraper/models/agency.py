"""
Pydantic models for the staffing agency data schema.

This schema follows the client's specification for inhuren.nl.
All fields are optional to allow partial data extraction.
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Literal
from uuid import UUID, uuid4

from pydantic import BaseModel, Field, HttpUrl


class GeoFocusType(str, Enum):
    """Geographic focus type."""

    LOCAL = "local"
    REGIONAL = "regional"
    NATIONAL = "national"
    INTERNATIONAL = "international"


class CaoType(str, Enum):
    """CAO (collective labor agreement) type."""

    ABU = "ABU"
    NBBU = "NBBU"
    EIGEN_CAO = "eigen_cao"
    ONBEKEND = "onbekend"


class PricingModel(str, Enum):
    """Pricing model type."""

    OMREKENFACTOR = "omrekenfactor"
    FIXED_MARGIN = "fixed_margin"
    FIXED_FEE = "fixed_fee"
    UNKNOWN = "unknown"


class PricingTransparency(str, Enum):
    """Pricing transparency level."""

    PUBLIC_EXAMPLES = "public_examples"
    EXPLAINER_ONLY = "explainer_only"
    QUOTE_ONLY = "quote_only"


class VolumeSpecialisation(str, Enum):
    """Volume specialisation type."""

    AD_HOC_1_5 = "ad_hoc_1_5"
    POOLS_5_50 = "pools_5_50"
    MASSA_50_PLUS = "massa_50_plus"
    UNKNOWN = "unknown"


class OvernameFeeModel(str, Enum):
    """Takeover fee model."""

    NONE = "none"
    FLAT_FEE = "flat_fee"
    PERCENTAGE_SALARY = "percentage_salary"
    SCALED = "scaled"
    UNKNOWN = "unknown"


class OfficeLocation(BaseModel):
    """Office location model."""

    city: str | None = None
    province: str | None = None


class AgencyServices(BaseModel):
    """
    Services offered by the staffing agency.

    All fields default to None (null in JSON).
    - None (null) = unknown / not stated
    - True = explicitly confirmed as offered
    - False = explicitly stated as NOT offered (rare - only use when website clearly states service is not available)
    
    Rule: Use None (null) for unknown. Only set to False if website explicitly states service is not offered.
    """

    uitzenden: bool | None = None
    detacheren: bool | None = None
    werving_selectie: bool | None = None
    payrolling: bool | None = None
    zzp_bemiddeling: bool | None = None
    vacaturebemiddeling_only: bool | None = None
    inhouse_services: bool | None = None
    msp: bool | None = None
    rpo: bool | None = None
    executive_search: bool | None = None
    opleiden_ontwikkelen: bool | None = None
    reintegratie_outplacement: bool | None = None


class PhaseSystem(BaseModel):
    """CAO phase system."""

    abu_phases: list[str] | None = None
    nbbu_phases: list[str] | None = None


class TakeoverPolicy(BaseModel):
    """Takeover (overname) policy details."""

    free_takeover_hours: int | None = None
    free_takeover_weeks: int | None = None
    overname_fee_model: OvernameFeeModel = OvernameFeeModel.UNKNOWN
    overname_fee_hint: str | None = None
    overname_contract_reference: str | None = None


class DigitalCapabilities(BaseModel):
    """
    Digital capabilities of the agency.
    
    All fields default to None (null in JSON).
    Only set to True when explicitly confirmed on the website.
    Never set to False - use None (null) for unknown/unconfirmed.
    """

    client_portal: bool | None = None
    candidate_portal: bool | None = None
    mobile_app: bool | None = None
    api_available: bool | None = None
    realtime_vacancy_feed: bool | None = None
    realtime_availability_feed: bool | None = None
    self_service_contracting: bool | None = None


class AICapabilities(BaseModel):
    """
    AI capabilities of the agency.
    
    All fields default to None (null in JSON).
    Only set to True when explicitly confirmed on the website.
    Never set to False - use None (null) for unknown/unconfirmed.
    """

    internal_ai_matching: bool | None = None
    predictive_planning: bool | None = None
    chatbot_for_candidates: bool | None = None
    chatbot_for_clients: bool | None = None
    ai_screening: bool | None = None


class Agency(BaseModel):
    """
    Main staffing agency model following the inhuren.nl schema.

    This represents one JSON record per agency with all available
    factual company data scraped from official websites.
    """

    # --- Identifier ---
    id: UUID = Field(default_factory=uuid4)

    # --- Basic identity ---
    agency_name: str
    legal_name: str | None = None
    logo_url: str | None = None
    website_url: str
    brand_group: str | None = None
    hq_city: str | None = None
    hq_province: str | None = None
    kvk_number: str | None = None

    # --- Contact (business-only, no personal data) ---
    contact_phone: str | None = None
    contact_email: str | None = None
    contact_form_url: str | None = None
    employers_page_url: str | None = None

    # --- Geographic coverage ---
    regions_served: list[str] = Field(default_factory=list)
    office_locations: list[OfficeLocation] = Field(default_factory=list)
    geo_focus_type: GeoFocusType = GeoFocusType.NATIONAL

    # --- Market positioning / target clients ---
    sectors_core: list[str] = Field(default_factory=list)
    sectors_secondary: list[str] = Field(default_factory=list)
    role_levels: list[str] = Field(default_factory=list)
    company_size_fit: list[str] = Field(default_factory=list)
    customer_segments: list[str] = Field(default_factory=list)

    # --- Specialisations & strengths ---
    focus_segments: list[str] = Field(default_factory=list)
    shift_types_supported: list[str] = Field(default_factory=list)
    volume_specialisation: VolumeSpecialisation = VolumeSpecialisation.UNKNOWN
    typical_use_cases: list[str] = Field(default_factory=list)

    # --- Services / contract types ---
    services: AgencyServices = Field(default_factory=AgencyServices)

    # --- Legal / CAO & compliance ---
    cao_type: CaoType = CaoType.ONBEKEND
    phase_system: PhaseSystem | None = None
    applies_inlenersbeloning_from_day1: bool | None = None
    uses_inlenersbeloning: bool | None = None
    certifications: list[str] = Field(default_factory=list)
    membership: list[str] = Field(default_factory=list)

    # --- Pricing & commercial conditions ---
    pricing_model: PricingModel = PricingModel.UNKNOWN
    pricing_transparency: PricingTransparency | None = None
    omrekenfactor_min: float | None = None
    omrekenfactor_max: float | None = None
    example_pricing_hint: str | None = None
    no_cure_no_pay: bool | None = None
    min_assignment_duration_weeks: int | None = None
    min_hours_per_week: int | None = None
    takeover_policy: TakeoverPolicy = Field(default_factory=TakeoverPolicy)

    # --- Operational claims / performance ---
    avg_hourly_rate_low: float | None = None
    avg_hourly_rate_high: float | None = None
    avg_markup_factor: float | None = None
    avg_time_to_fill_days: int | None = None
    speed_claims: list[str] = Field(default_factory=list)
    annual_placements_estimate: int | None = None
    candidate_pool_size_estimate: int | None = None

    # --- Digital & AI capabilities ---
    digital_capabilities: DigitalCapabilities = Field(default_factory=DigitalCapabilities)
    ai_capabilities: AICapabilities = Field(default_factory=AICapabilities)

    # --- Review / reputation signals ---
    review_rating: float | None = None
    review_count: int | None = None
    review_sources: list[str] = Field(default_factory=list)
    external_review_urls: list[str] = Field(default_factory=list)
    review_themes_positive: list[str] = Field(default_factory=list)
    review_themes_negative: list[str] = Field(default_factory=list)

    # --- Scenario-strength hints ---
    scenario_strengths: list[str] = Field(default_factory=list)

    # --- Meta / provenance ---
    growth_signals: list[str] = Field(default_factory=list)
    notes: str | None = None
    evidence_urls: list[str] = Field(default_factory=list)
    collected_at: datetime = Field(default_factory=datetime.utcnow)

    class Config:
        """Pydantic config."""

        json_encoders = {
            datetime: lambda v: v.isoformat(),
            UUID: lambda v: str(v),
        }

    def to_json_dict(self) -> dict:
        """Convert to JSON-serializable dictionary."""
        # Exclude scenario_strengths - it's LLM-generated later, not part of scraping
        return self.model_dump(mode="json", exclude={"scenario_strengths"})

