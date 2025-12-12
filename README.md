# Dutch Staffing Agency Scraper

A Dagster-based data pipeline for scraping factual company data from Dutch staffing agency (uitzendbureau) websites for [inhuren.nl](https://inhuren.nl).

## Overview

This project scrapes only **factual company data** from official staffing agency websites:
- No personal data / No CVs
- No login areas
- No third-party directories
- Only publicly visible information

### MVP Phase 1: 15 Agencies

**✅ ALL COMPLETED (15/15)** 🎉:

1. ✅ **Adecco** - Complex scraper with API integration + PDF parsing
2. ✅ **ASA Talent** - Standard scraper with normalized sectors
3. ✅ **Brunel** - International agency with JSON extraction
4. ✅ **Covebo** - Standard scraper with sector extraction
5. ✅ **Hays Nederland** - Multiple page sources, chatbot detection
6. ✅ **Maandag** - JSON-LD extraction, certification pages
7. ✅ **Manpower** - 23 office locations, sector categorization
8. ✅ **Michael Page** - 4 offices, ISO certifications, Google reviews
9. ✅ **Olympia** - Paginated offices, SMB focus, ABU CAO
10. ✅ **Randstad** - Global HQ, comprehensive services extraction
11. ✅ **Start People** - PDF legal extraction, RGF Staffing group
12. ✅ **Tempo-Team** - Standard scraper with sector normalization
13. ✅ **TMI** - Healthcare specialization (Zorg), review rating extraction
14. ✅ **Yacht** - JSON-LD extraction, 7 offices, Seamly chatbot detection, 9 sectors
15. ✅ **YoungCapital** - JSON-LD extraction, 6 social platforms, founded 2000

**Key Features Across All Scrapers**: JSON-LD extraction, logo filtering, sector normalization, portal detection, role levels, review sources, certifications, office locations, Dutch keyword optimization

## Architecture & Improvements

### Scraper Architecture

All agency scrapers follow a consistent architecture:

1. **BaseAgencyScraper**: Base class in `scraping/base.py` with common functionality
   - URL management and evidence tracking
   - Page fetching with retry logic
   - JSON output generation

2. **AgencyScraperUtils**: Reusable extraction methods in `scraping/utils.py` (69 methods)
   - Logo extraction (PNG/SVG filtering, banner exclusion)
   - Sector normalization (15 standardized Dutch sectors)
   - Portal detection (candidate/client portal keywords)
   - Role level inference (student, starter, medior, senior)
   - Review source extraction (Google Reviews, Trustpilot, Indeed)
   - Contact info, certifications, CAO types, etc.

3. **Agency-Specific Scrapers**: Custom logic in `agencies/{name}/assets.py`
   - Use `self.utils` for standard extractions
   - Implement custom logic for unique website structures
   - Can include API calls, PDF parsing, JSON extraction

### Recent Improvements (December 2025)

**Core Enhancements Across All Scrapers**:

1. **Logo Extraction**: PNG/SVG filtering, banner exclusion, JSON-LD extraction
2. **Sector Normalization**: 15 standardized Dutch sectors with keyword mapping
3. **Portal Detection**: Improved candidate/client portal keyword detection
4. **Role Levels**: Automatic inference (student, starter, medior, senior) with word boundaries
5. **Review Sources**: Extract Google Reviews, Trustpilot, Indeed links
6. **Office Locations**: Paginated scraping, city-to-province mapping (50+ cities)
7. **Legal Data**: KVK extraction, legal name from footer/legal pages
8. **Certifications**: Enhanced extraction including PDF parsing (ABU, SNA, ISO, etc.)
9. **API Integration**: Support for JSON APIs (Adecco jobs API, Next.js data)
10. **Error Handling**: Comprehensive logging with source URL tracking

**All 10 Completed Scrapers Include**:
- ✅ BeautifulSoup-based extraction (no AI/LLM dependencies)
- ✅ Structured page task system with custom extraction functions
- ✅ Reusable utility methods from `AgencyScraperUtils` (69 methods)
- ✅ Evidence URL tracking for all scraped pages
- ✅ JSON output with 70+ structured fields

## Tech Stack

- **Orchestration**: Dagster (with local file-based logging)
- **Scraping**: BeautifulSoup + requests (pure Python, no browser automation)
- **PDF Parsing**: pdfplumber (for certificates, legal documents)
- **JSON Extraction**: Native Python (`json.loads`, regex for embedded data)
- **Data Storage**: PostgreSQL
- **Package Management**: uv
- **Linting**: ruff
- **Type Checking**: pyright
- **Database Migrations**: Prisma

**Note**: All scrapers use lightweight BeautifulSoup parsing. No AI, LLM, or browser automation dependencies.

## Setup

### Prerequisites

1. Install uv:
```bash
curl -sSf https://astral.sh/uv/install.sh | bash
```

2. Install nvm (for Node/Prisma):
```bash
curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.40.2/install.sh | bash
```

### Initial Setup

1. Clone the repository and navigate to it:
```bash
cd UPWORK-Scraping-Staffing-Agency
```

2. Create virtual environment and install dependencies:
```bash
uv venv
source .venv/bin/activate
uv pip install -e ".[dev]"
```

3. Install Playwright browsers:
```bash
playwright install --with-deps
```

4. Install Node dependencies (for Prisma):
```bash
nvm use
npm i
```

5. Copy environment file:
```bash
cp .env.example .env
# Edit .env with your database credentials
```

6. Run database migrations:
```bash
make migrate
```

### Running the Pipeline

Start Dagster development server:
```bash
make dev
```

Or without database (for testing):
```bash
make dev-no-db
```

Access the Dagster UI at http://localhost:3001

### Running Individual Scrapers

Run a single agency scraper:
```bash
uv run dagster asset materialize -m staffing_agency_scraper.definitions -a adecco_scrape
```

Run multiple agencies:
```bash
# Run 3 completed scrapers
uv run dagster asset materialize -m staffing_agency_scraper.definitions \
  -a adecco_scrape \
  -a asa_talent_scrape \
  -a brunel_scrape

# Run all 10 completed scrapers
uv run dagster asset materialize -m staffing_agency_scraper.definitions \
  -a adecco_scrape \
  -a asa_talent_scrape \
  -a brunel_scrape \
  -a hays_scrape \
  -a maandag_scrape \
  -a manpower_scrape \
  -a michael_page_scrape \
  -a olympia_scrape \
  -a start_people_scrape \
  -a tmi_scrape
```

### Viewing Logs

All logs are written locally to the `logs/` directory:

```bash
# View main Dagster log
tail -f logs/dagster.log

# View latest scraper run output
cat logs/compute_logs/*/compute.out

# Or use the interactive log viewer (if available)
./view_logs.sh
```

## Project Structure

```
.
├── staffing_agency_scraper/
│   ├── __init__.py
│   ├── definitions.py              # Main Dagster definitions
│   ├── resources.py                # Shared resources (DB, etc.)
│   ├── scraping/
│   │   ├── __init__.py
│   │   ├── base.py                 # BaseAgencyScraper class
│   │   ├── utils.py                # AgencyScraperUtils (69 reusable extraction methods)
│   │   ├── definitions.py          # Scraping module definitions
│   │   └── agencies/               # Per-agency scrapers (10/15 complete)
│   │       ├── __init__.py
│   │       ├── adecco/             # ✅ Complex: API + PDF parsing
│   │       ├── asa_talent/         # ✅ Standard: Normalized sectors
│   │       ├── brunel/             # ✅ Advanced: __NEXT_DATA__ extraction
│   │       ├── hays/               # ✅ Standard: Legal pages, chatbot
│   │       ├── maandag/            # ✅ Advanced: JSON-LD, Next.js data
│   │       ├── manpower/           # ✅ Medium: 23 offices, sectors
│   │       ├── michael_page/       # ✅ Advanced: 4 offices, ISO certs
│   │       ├── olympia/            # ✅ Medium: Paginated, SMB stats
│   │       ├── start_people/       # ✅ Medium: PDF legal, RGF group
│   │       ├── tmi/                # ✅ Healthcare: Review ratings, sectors
│   │       ├── randstad/           # 🔄 In Progress
│   │       └── ...                 # 4 agencies to do
│   ├── lib/
│   │   ├── fetch.py                # HTTP utilities
│   │   ├── browser.py              # Playwright utilities (optional)
│   │   ├── parse.py                # HTML parsing utilities
│   │   ├── extract.py              # Common extraction patterns
│   │   ├── dutch.py                # Dutch-specific utilities (postal codes, sectors)
│   │   └── normalize.py            # Data normalization
│   └── models/
│       ├── __init__.py
│       └── agency.py               # Pydantic schema models (70+ fields)
├── prisma/
│   ├── schema.prisma
│   └── migrations/
├── dagster_home/                   # Dagster instance data
│   ├── runs/
│   ├── history/
│   └── schedules/
├── logs/                           # Local log files
│   ├── dagster.log                 # Main Dagster log
│   └── compute_logs/               # Per-run execution logs
├── output/                         # JSON output (10 agencies completed)
├── tests/
├── dagster.yaml                    # Dagster logging configuration
├── pyproject.toml
├── Makefile
└── README.md
```

## Output Schema

Each agency produces a JSON record with 70+ fields organized into categories:

### 1. Basic Identity (8 fields)
| Field | Type | Description | Source |
|-------|------|-------------|--------|
| `id` | UUID | Unique identifier | Auto-generated |
| `agency_name` | string | Marketing name (e.g., "Randstad Student") | Homepage, header |
| `legal_name` | string | Legal entity name | Legal page, footer |
| `logo_url` | string | Direct URL to logo | og:image, header |
| `website_url` | string | Canonical homepage URL | Config |
| `brand_group` | string | Parent company (e.g., "Randstad Groep") | About page |
| `hq_city` | string | Headquarters city | Contact/about page |
| `hq_province` | string | Headquarters province | Contact/about page |
| `kvk_number` | string | Chamber of Commerce number (8 digits) | Footer, legal page |

### 2. Contact Information (4 fields)
| Field | Type | Description | Source |
|-------|------|-------------|--------|
| `contact_phone` | string | Generic business number only | Contact page |
| `contact_email` | string | Generic mailbox (info@, sales@) | Contact page |
| `contact_form_url` | string | URL of contact form | Contact page |
| `employers_page_url` | string | Landing page for werkgevers | Navigation |

### 3. Geographic Coverage (3 fields)
| Field | Type | Description | Source |
|-------|------|-------------|--------|
| `regions_served` | array | Regions/labels: "landelijk", "Randstad", etc. | About page |
| `office_locations` | array | `[{city, province}]` - Office addresses | Vestigingen page |
| `geo_focus_type` | enum | `local \| regional \| national \| international` | About page |

### 4. Market Positioning (5 fields)
| Field | Type | Description | Source |
|-------|------|-------------|--------|
| `sectors_core` | array | Main sectors: logistiek, productie, zorg, etc. | Vakgebieden, diensten |
| `sectors_secondary` | array | Secondary sectors | Vakgebieden |
| `role_levels` | array | Candidate seniority: student, starter, medior, senior | Vacatures |
| `company_size_fit` | array | Client types: micro, smb, enterprise, public_sector | Werkgevers page |
| `customer_segments` | array | MKB, grootbedrijf, overheid, zorginstelling | Werkgevers page |

### 5. Specialisations & Strengths (4 fields)
| Field | Type | Description | Source |
|-------|------|-------------|--------|
| `focus_segments` | array | Who they staff: studenten, blue_collar, white_collar, etc. | About, diensten |
| `shift_types_supported` | array | dagdienst, avonddienst, nachtdienst, weekend, 24_7 | Vacatures, diensten |
| `volume_specialisation` | enum | `ad_hoc_1_5 \| pools_5_50 \| massa_50_plus \| unknown` | Marketing text |
| `typical_use_cases` | array | piekdruk_opvangen, langdurige_detachering, etc. | Marketing text |

### 6. Services / Contract Types (1 object, 12 boolean fields)
| Field | Type | Description |
|-------|------|-------------|
| `services.uitzenden` | bool | Temporary staffing |
| `services.detacheren` | bool | Secondment/contractor |
| `services.werving_selectie` | bool | Recruitment & selection |
| `services.payrolling` | bool | Payroll services |
| `services.zzp_bemiddeling` | bool | Freelancer mediation |
| `services.vacaturebemiddeling_only` | bool | Job board only |
| `services.inhouse_services` | bool | On-site staffing |
| `services.msp` | bool | Managed Service Provider |
| `services.rpo` | bool | Recruitment Process Outsourcing |
| `services.executive_search` | bool | Executive recruitment |
| `services.opleiden_ontwikkelen` | bool | Training & development |
| `services.reintegratie_outplacement` | bool | Reintegration services |

### 7. Legal / CAO & Compliance (6 fields)
| Field | Type | Description | Source |
|-------|------|-------------|--------|
| `cao_type` | enum | `ABU \| NBBU \| eigen_cao \| onbekend` | Legal page, about |
| `phase_system` | object | ABU/NBBU phase info if mentioned | Legal page |
| `applies_inlenersbeloning_from_day1` | bool | Inlenersbeloning from day 1 | Terms |
| `uses_inlenersbeloning` | bool | Uses inlenersbeloning | Terms |
| `certifications` | array | NEN-4400-1, SNA, VCU, ISO9001 | Footer, about |
| `membership` | array | ABU, NBBU, NRTO memberships | Footer, about |

### 8. Pricing & Commercial (9 fields)
| Field | Type | Description | Source |
|-------|------|-------------|--------|
| `pricing_model` | enum | `omrekenfactor \| fixed_margin \| fixed_fee \| unknown` | Tarieven page |
| `pricing_transparency` | enum | `public_examples \| explainer_only \| quote_only` | Website |
| `omrekenfactor_min` | number | Minimum markup factor | Tarieven |
| `omrekenfactor_max` | number | Maximum markup factor | Tarieven |
| `example_pricing_hint` | string | Literal pricing example | Tarieven |
| `no_cure_no_pay` | bool | If stated | Terms |
| `min_assignment_duration_weeks` | number | Minimum assignment | Terms |
| `min_hours_per_week` | number | Minimum hours | Terms |
| `takeover_policy` | object | Overname policy details | Voorwaarden |

### 9. Operational Claims (7 fields)
| Field | Type | Description | Source |
|-------|------|-------------|--------|
| `avg_hourly_rate_low` | number | Only if explicitly stated | Marketing |
| `avg_hourly_rate_high` | number | Only if explicitly stated | Marketing |
| `avg_markup_factor` | number | Average markup | Marketing |
| `avg_time_to_fill_days` | number | "binnen 24 uur" → 1 | Marketing |
| `speed_claims` | array | binnen_24_uur_kandidaten, snel_schakelen | Marketing |
| `annual_placements_estimate` | number | Annual placements | About page |
| `candidate_pool_size_estimate` | number | Database size | Marketing |

### 10. Digital & AI Capabilities (2 objects)
| Field | Type | Description |
|-------|------|-------------|
| `digital_capabilities.client_portal` | bool | Werkgeversportaal |
| `digital_capabilities.candidate_portal` | bool | Mijn omgeving |
| `digital_capabilities.mobile_app` | bool | App store presence |
| `digital_capabilities.api_available` | bool | Public API |
| `digital_capabilities.realtime_vacancy_feed` | bool | Live vacatures |
| `digital_capabilities.realtime_availability_feed` | bool | Live beschikbaarheid |
| `digital_capabilities.self_service_contracting` | bool | Online contracting |
| `ai_capabilities.internal_ai_matching` | bool | AI matching |
| `ai_capabilities.predictive_planning` | bool | Predictive staffing |
| `ai_capabilities.chatbot_for_candidates` | bool | Candidate chatbot |
| `ai_capabilities.chatbot_for_clients` | bool | Client chatbot |
| `ai_capabilities.ai_screening` | bool | AI screening |

### 11. Review / Reputation (7 fields)
| Field | Type | Description | Source |
|-------|------|-------------|--------|
| `review_rating` | number | Aggregate rating on THEIR site | Website |
| `review_count` | number | Number of reviews | Website |
| `review_sources` | array | google_reviews, indeed, trustpilot | Website |
| `external_review_urls` | array | Links to external review pages | Website |
| `review_themes_positive` | array | Positive themes | Website |
| `review_themes_negative` | array | Negative themes | Website |

### 12. Scenario Strengths (1 field)
| Field | Type | Description |
|-------|------|-------------|
| `scenario_strengths` | array | LLM-generated, NOT scraped. Filled later. |

### 13. Meta / Provenance (4 fields)
| Field | Type | Description |
|-------|------|-------------|
| `growth_signals` | array | Factual claims: landelijke_dekking, sinds_1995 |
| `notes` | string | Free-form notes |
| `evidence_urls` | array | ALL URLs used to fill fields |
| `collected_at` | datetime | ISO-8601 timestamp |

---

## Field Implementation Status

| Category | Fields | Implemented | Coverage | Notes |
|----------|--------|-------------|----------|-------|
| Basic Identity | 8 | ✅ 8 | 100% | Logo filtering, KVK extraction, legal names |
| Contact | 4 | ✅ 4 | 100% | Phone, email, contact forms, employer pages |
| Geographic | 3 | ✅ 3 | 100% | Office locations with province mapping (50+ cities) |
| Market Positioning | 5 | ✅ 5 | 100% | Sector normalization, role levels, company size fit |
| Specialisations | 4 | ✅ 4 | 100% | Focus segments, shift types, volume, use cases |
| Services | 12 | ✅ 12 | 100% | All service types detected from navigation/content |
| Legal/CAO | 6 | ✅ 6 | 100% | CAO types, certifications (PDF parsing), memberships |
| Pricing | 9 | ⚠️ 4 | 40% | Partial: no_cure_no_pay, pricing hints, some averages |
| Operational Claims | 7 | ⚠️ 4 | 60% | Speed claims, time-to-fill, some pool sizes |
| Digital/AI | 12 | ✅ 11 | 92% | Portal detection, mobile apps, chatbots, API presence |
| Review | 7 | ✅ 3 | 40% | Review sources, some ratings/counts from websites |
| Meta | 4 | ✅ 4 | 100% | Growth signals, evidence URLs, timestamps, notes |

**Total**: 67/81 fields implemented (83% coverage)

**December 2025 Enhancements**:
- ✅ `office_locations`: City-to-province mapping for 50+ Dutch cities
- ✅ `role_levels`: Word boundary matching to prevent false positives
- ✅ `certifications`: PDF extraction for certificate pages
- ✅ `review_sources`: Google Reviews, Trustpilot, Indeed detection
- ✅ `company_size_fit`: Automatic detection (micro, SMB, enterprise, public)
- ✅ `customer_segments`: MKB, overheid, zorg detection
- ✅ `chatbot_for_candidates/clients`: Zopim, Intercom detection
- ✅ `api_available`: API endpoint discovery (e.g., Adecco jobs API)

---

## Development

### Linting & Formatting
```bash
make lint
```

### Running Tests
```bash
make test
```

### Creating Migrations
```bash
make migration
```

### Sitemap Discovery
```bash
python scripts/discover_sitemap.py
```

## Debugging & Troubleshooting

### Local Logging

All Dagster logs are written to the `logs/` directory:

- **`logs/dagster.log`**: Main Dagster system log (INFO level)
- **`logs/compute_logs/{run_id}/compute.out`**: Per-run execution logs (stdout/stderr)
- **`logs/compute_logs/{run_id}/compute.err`**: Per-run error logs

Configuration in `dagster.yaml`:
- Log rotation: 10 MB per file, 5 backups
- Format: JSON for structured logging
- Location: `logs/` directory (auto-created)

### Common Issues

**Issue**: Scraper returns empty fields
- Check `logs/compute_logs/*/compute.out` for extraction warnings
- Verify website structure hasn't changed
- Test selectors manually in browser devtools

**Issue**: Logo is a banner image (e.g., Brunel)
- Check if website has PNG/SVG logo in header
- May need to use inline SVG or hardcode URL
- See `utils.fetch_logo()` for filtering logic

**Issue**: Sectors include non-sectors
- Sectors normalized to 15 standard Dutch sectors in `utils.fetch_sectors()`
- Non-sectors like "thuiswerk", "oproepkracht" are filtered out

**Issue**: Portals not detected
- Check keywords in `utils.detect_candidate_portal()` and `utils.detect_client_portal()`
- Portal detection looks for: "login", "inloggen", "mijn...", "portal"
- Some agencies may not have portals

**Issue**: Brotli decompression error (Adecco)
- Custom `_fetch_page_safe()` method handles this
- Falls back to non-Brotli encoding if needed

### Testing Individual Extractions

You can test extraction methods directly:

```python
from staffing_agency_scraper.scraping.utils import AgencyScraperUtils
from staffing_agency_scraper.scraping.base import BaseAgencyScraper
import logging

# Create utils instance
utils = AgencyScraperUtils(logger=logging.getLogger(__name__))

# Test logo extraction
soup = ...  # Your BeautifulSoup object
logo = utils.fetch_logo(soup, "https://example.nl")

# Test sector normalization
sectors = utils.fetch_sectors("text with logistiek, horeca, thuiswerk", "https://example.nl")
# Returns: ["logistiek", "horeca"] (thuiswerk filtered out)
```

## Adding a New Agency

### Step 1: Create Directory Structure

```bash
mkdir -p staffing_agency_scraper/scraping/agencies/new_agency
cd staffing_agency_scraper/scraping/agencies/new_agency
touch __init__.py assets.py definitions.py
```

### Step 2: Create `assets.py`

Use the standard template with `AgencyScraperUtils`:

```python
"""
New Agency scraper.

Website: https://www.newagency.nl
"""
from __future__ import annotations

import dagster as dg
from bs4 import BeautifulSoup

from staffing_agency_scraper.models import Agency, GeoFocusType
from staffing_agency_scraper.scraping.base import BaseAgencyScraper
from staffing_agency_scraper.scraping.utils import AgencyScraperUtils


class NewAgencyScraper(BaseAgencyScraper):
    """Scraper for New Agency."""

    AGENCY_NAME = "New Agency"
    WEBSITE_URL = "https://www.newagency.nl"
    BRAND_GROUP = "New Agency Group"
    
    PAGES_TO_SCRAPE = [
        "https://www.newagency.nl",
        "https://www.newagency.nl/over-ons",
        "https://www.newagency.nl/contact",
        "https://www.newagency.nl/diensten",
    ]

    def scrape(self) -> Agency:
        self.logger.info(f"Starting scrape of {self.AGENCY_NAME}")
        
        # Initialize utils for standard extractions
        self.utils = AgencyScraperUtils(logger=self.logger)
        
        agency = self.create_base_agency()
        agency.geo_focus_type = GeoFocusType.NATIONAL
        
        for url in self.PAGES_TO_SCRAPE:
            try:
                soup = self.fetch_page(url)
                page_text = soup.get_text(separator=" ", strip=True)
                
                # Use utils methods for standard extractions
                if not agency.logo_url:
                    agency.logo_url = self.utils.fetch_logo(soup, url)
                
                # Sectors (normalized)
                sectors = self.utils.fetch_sectors(page_text, url)
                if sectors and not agency.sectors_core:
                    agency.sectors_core = sectors
                
                # Portal detection
                if self.utils.detect_candidate_portal(soup, page_text, url):
                    agency.digital_capabilities.candidate_portal = True
                if self.utils.detect_client_portal(soup, page_text, url):
                    agency.digital_capabilities.client_portal = True
                
                # Role levels
                role_levels = self.utils.fetch_role_levels(page_text, url)
                if role_levels:
                    if not agency.role_levels:
                        agency.role_levels = []
                    agency.role_levels.extend(role_levels)
                    agency.role_levels = list(set(agency.role_levels))
                
                # Review sources
                review_sources = self.utils.fetch_review_sources(soup, url)
                if review_sources and not agency.review_sources:
                    agency.review_sources = review_sources
                
                # Add custom extractions here...
                
            except Exception as e:
                self.logger.warning(f"Error scraping {url}: {e}")
        
        agency.evidence_urls = list(self.evidence_urls)
        agency.collected_at = self.collected_at
        
        return agency


@dg.asset(group_name="agencies")
def new_agency_scrape() -> dg.Output[dict]:
    """Scrape New Agency website."""
    scraper = NewAgencyScraper()
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
```

### Step 3: Create `definitions.py`

```python
"""Dagster definitions for New Agency scraper."""

from dagster import load_assets_from_modules

from . import assets

new_agency_assets = load_assets_from_modules([assets])
```

### Step 4: Register in Main Definitions

Add to `staffing_agency_scraper/scraping/definitions.py`:

```python
from .agencies.new_agency.definitions import new_agency_assets

all_scraper_assets = [
    # ... existing assets ...
    *new_agency_assets,
]
```

### Step 5: Test

```bash
uv run dagster asset materialize -m staffing_agency_scraper.definitions -a new_agency_scrape
```

Check output:
```bash
cat output/new_agency.json
tail -f logs/dagster.log
```

### Examples to Follow

**By Complexity Level**:

- **Simple/Standard**: 
  - `asa_talent/assets.py` - Basic sector extraction
  - `hays/assets.py` - KVK from legal pages, chatbot detection
  
- **Medium Complexity**:
  - `manpower/assets.py` - 23 offices, JSON extraction, sector categorization
  - `olympia/assets.py` - Paginated scraping, SMB statistics
  - `start_people/assets.py` - PDF privacy statement, province mapping
  
- **Advanced**:
  - `adecco/assets.py` - API integration, PDF parsing, Brotli handling
  - `brunel/assets.py` - International agency, __NEXT_DATA__ extraction
  - `michael_page/assets.py` - Multi-office, ISO certs, takeover policy
  - `maandag/assets.py` - JSON-LD, Next.js data, certification pages
  - `yacht/assets.py` - JSON-LD extraction, 7 offices, Seamly chatbot API, modular helpers
  - `youngcapital/assets.py` - JSON-LD extraction, VAT→KvK conversion, 6 social platforms
  - `tmi/assets.py` - Healthcare vertical ("Zorg"), review extraction, simplified sectors

**Common Patterns Across All Scrapers**:
- Structured page tasks with custom functions
- `self.utils` for 69+ reusable extractions
- Evidence URL automatic tracking
- City-to-province mapping for offices
- Sector normalization to 15 standard categories
- Portal and chatbot detection
- Comprehensive error handling with source logging

## Environment Variables

| Name | Required | Example | Description |
|------|----------|---------|-------------|
| `PIPELINE_PG_CONNSTRING` | Yes | `postgres://user:pass@host:5432/db` | PostgreSQL connection string |
| `OUTPUT_DIR` | No | `./output` | Directory for JSON output |
| `LOG_INVALID_ROWS` | No | `true` | Log invalid rows during parsing |
| `DAGSTER_HOME` | No | `./dagster_home` | Dagster instance data directory |

**Note**: Logging configuration is in `dagster.yaml`. Logs are written to `logs/` directory by default.

## Quick Reference

### Common Commands

```bash
# Development
make dev                 # Start Dagster UI (with DB)
make dev-no-db          # Start Dagster UI (without DB)
make lint               # Run linter
make test               # Run tests

# Scraping (single agency)
uv run dagster asset materialize -m staffing_agency_scraper.definitions -a yacht_scrape
uv run dagster asset materialize -m staffing_agency_scraper.definitions -a youngcapital_scrape
uv run dagster asset materialize -m staffing_agency_scraper.definitions -a tmi_scrape

# Scraping (multiple agencies)
uv run dagster asset materialize -m staffing_agency_scraper.definitions \
  -a yacht_scrape -a youngcapital_scrape -a tmi_scrape

# Scraping ALL 15 MVP agencies
uv run dagster asset materialize -m staffing_agency_scraper.definitions \
  -a adecco_scrape -a asa_talent_scrape -a brunel_scrape -a covebo_scrape \
  -a hays_scrape -a maandag_scrape -a manpower_scrape -a michael_page_scrape \
  -a olympia_scrape -a randstad_scrape -a start_people_scrape -a tempo_team_scrape \
  -a tmi_scrape -a yacht_scrape -a youngcapital_scrape

# Logging
tail -f logs/dagster.log                    # View main log
cat logs/compute_logs/*/compute.out         # View latest run
./view_logs.sh                              # Interactive log viewer (if available)

# Output
cat output/yacht.json                       # View Yacht data
cat output/youngcapital.json                # View YoungCapital data
cat output/tmi.json                         # View TMI data
grep "role_levels" output/*.json            # Check specific fields across all agencies
grep "office_locations" output/*.json -c    # Count offices per agency
ls -lh output/                              # List all 15 completed JSONs
```

### Key Files

- `dagster.yaml`: Logging configuration
- `pyproject.toml`: Dependencies and project metadata
- `staffing_agency_scraper/scraping/utils.py`: 69 reusable extraction methods
- `staffing_agency_scraper/scraping/base.py`: BaseAgencyScraper class
- `output/`: JSON output directory (gitignored)
- `logs/`: Local log files (gitignored)

### Useful Patterns

**Extract sectors with normalization**:
```python
sectors = self.utils.fetch_sectors(page_text, url)
# Returns only: logistiek, horeca, zorg, techniek, office, finance, etc.
```

**Detect portals**:
```python
if self.utils.detect_candidate_portal(soup, page_text, url):
    agency.digital_capabilities.candidate_portal = True
```

**Extract role levels**:
```python
role_levels = self.utils.fetch_role_levels(page_text, url)
# Returns: ["student", "starter", "medior", "senior"]
```

**Logo with filtering**:
```python
logo = self.utils.fetch_logo(soup, url)
# Only PNG/SVG, excludes banners/hero images
```

## License

Proprietary - inhuren.nl

---

## Project Status

**Last Updated**: December 12, 2025  
**MVP Progress**: ✅ **15/15 agencies completed (100%)** 🎉  
**Field Coverage**: 67/81 fields implemented (83%)  
**Utility Functions**: 69 reusable extraction methods in `utils.py`

**Recent Milestones**:
- ✅ **ALL 15 MVP SCRAPERS COMPLETED** - Production-ready with comprehensive data extraction
- ✅ Standardized architecture with `BaseAgencyScraper` + `AgencyScraperUtils`
- ✅ BeautifulSoup-only approach (removed AI/LLM dependencies)
- ✅ JSON-LD extraction for structured data (Yacht, YoungCapital, Maandag)
- ✅ Dutch keyword optimization - 75+ terms for service detection
- ✅ City-to-province mapping for 50+ Dutch cities
- ✅ Paginated scraping support for office locations
- ✅ PDF extraction for legal documents and certifications
- ✅ API integration support (JSON endpoints, Next.js data, Seamly chatbot)
- ✅ Enhanced logging with source URL tracking

**Latest Additions** (December 12, 2025):
- ✅ **Yacht** - Randstad Professional, 7 offices, JSON-LD extraction, Seamly chatbot, 9 sectors
- ✅ **YoungCapital** - Youth-focused, JSON-LD extraction, 6 social platforms, founded 2000
- ✅ **TMI** - Healthcare vertical simplified to "Zorg", review ratings
- ✅ **Dutch Service Keywords** - 75+ optimized terms (detacheren, werving, zzp, etc.)

**Next Steps**:
1. Enhance pricing and review data extraction
2. Add more growth signals and operational metrics
3. Expand to additional agencies beyond MVP 15
4. Add automated data validation and quality checks
