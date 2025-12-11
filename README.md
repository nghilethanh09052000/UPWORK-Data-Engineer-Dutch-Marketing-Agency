# Dutch Staffing Agency Scraper

A Dagster-based data pipeline for scraping factual company data from Dutch staffing agency (uitzendbureau) websites for [inhuren.nl](https://inhuren.nl).

## Overview

This project scrapes only **factual company data** from official staffing agency websites:
- No personal data / No CVs
- No login areas
- No third-party directories
- Only publicly visible information

### MVP Phase 1: 15 Agencies
1. Randstad
2. Tempo-Team
3. YoungCapital
4. ASA Talent
5. Manpower
6. Adecco ✅ (with API + PDF parsing)
7. Olympia
8. Start People
9. Covebo
10. Brunel ✅
11. Yacht
12. Maandag®
13. Hays Nederland
14. Michael Page / Page Personnel
15. TMI (Zorg)

✅ = Includes client feedback improvements (logo filtering, sector normalization, portal detection, role levels, review sources)

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

### Client Feedback Improvements (December 2025)

Five key improvements implemented across all scrapers:

1. **Logo Scraping**: Extract only PNG/SVG files from header/footer, exclude banners
2. **Sector Normalization**: Restrict to 15 normalized sectors (logistiek, horeca, zorg, etc.)
3. **Portal Detection**: Detect "login", "inloggen", "mijn...", "client portal" keywords
4. **Role Levels**: Infer student, starter, medior, senior from website content
5. **Review Sources**: Extract review platform names and URLs from footer/about pages

**Current Status**:
- ✅ Adecco: All 5 improvements implemented
- ✅ ASA Talent: All 5 improvements implemented  
- 🔄 Brunel: 4/5 (logo issue being investigated)

## Tech Stack

- **Orchestration**: Dagster (with local file-based logging)
- **Scraping**: BeautifulSoup + requests + crawl4ai (optional)
- **PDF Parsing**: pdfplumber (for certificates, legal documents)
- **Data Storage**: PostgreSQL
- **Package Management**: uv
- **Linting**: ruff
- **Type Checking**: pyright
- **Database Migrations**: Prisma

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
uv run dagster asset materialize -m staffing_agency_scraper.definitions \
  -a adecco_scrape \
  -a asa_talent_scrape \
  -a brunel_scrape
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
│   │   └── agencies/               # Per-agency scrapers (15 MVP agencies)
│   │       ├── __init__.py
│   │       ├── randstad/
│   │       │   ├── assets.py       # Scraper implementation
│   │       │   └── definitions.py  # Dagster asset definition
│   │       ├── adecco/             # Complex scraper with API + PDF parsing
│   │       ├── asa_talent/         # Normalized sector extraction
│   │       ├── brunel/             # International agency
│   │       ├── manpower/
│   │       ├── tempo_team/
│   │       ├── youngcapital/
│   │       └── ...
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
├── output/                         # JSON output directory (15 agency JSONs)
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

| Category | Fields | Implemented | Notes |
|----------|--------|-------------|-------|
| Basic Identity | 8 | ✅ 8 | All implemented, logo filtering improved |
| Contact | 4 | ✅ 4 | All implemented |
| Geographic | 3 | ✅ 3 | office_locations from APIs, contact pages |
| Market Positioning | 5 | ✅ 5 | **NEW**: role_levels now extracted! |
| Specialisations | 4 | ✅ 3 | typical_use_cases partial |
| Services | 12 | ✅ 12 | All implemented |
| Legal/CAO | 6 | ✅ 6 | **NEW**: certifications from PDFs |
| Pricing | 9 | ⚠️ 2 | Most require manual analysis |
| Operational Claims | 7 | ⚠️ 2 | Most require marketing text analysis |
| Digital/AI | 12 | ✅ 10 | **NEW**: Portal detection improved! |
| Review | 7 | ✅ 2 | **NEW**: review_sources extraction |
| Meta | 4 | ✅ 4 | All implemented |

**Recent Improvements**:
- ✅ `role_levels`: Now inferred from website content (student, starter, medior, senior)
- ✅ `digital_capabilities.candidate_portal`: Improved keyword detection
- ✅ `digital_capabilities.client_portal`: Improved keyword detection
- ✅ `review_sources`: Extract review platform names from footer/about pages
- ✅ `certifications`: Enhanced extraction including PDF parsing
- ✅ `sectors_core`: Normalized to 15 standardized sectors

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

- **Simple scraper**: `asa_talent/assets.py`, `manpower/assets.py`
- **Complex scraper with API**: `adecco/assets.py`
- **International agency**: `brunel/assets.py`

All 69 reusable extraction methods are available in `scraping/utils.py`.

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

# Scraping
uv run dagster asset materialize -m staffing_agency_scraper.definitions -a adecco_scrape
uv run dagster asset materialize -m staffing_agency_scraper.definitions -a asa_talent_scrape -a brunel_scrape

# Logging
tail -f logs/dagster.log                    # View main log
cat logs/compute_logs/*/compute.out         # View latest run
./view_logs.sh                              # Interactive log viewer (if available)

# Output
cat output/adecco.json                      # View scraped data
grep "role_levels" output/*.json            # Check specific fields
ls -lh output/                              # List all scraped agencies
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

**Last Updated**: December 2025  
**Client Feedback Improvements**: ✅ Implemented (5/5)  
**Scrapers Completed**: 3/15 MVP (Adecco, ASA Talent, Brunel)
