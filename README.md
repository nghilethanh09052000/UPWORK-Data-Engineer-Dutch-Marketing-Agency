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
6. Adecco
7. Olympia
8. Start People
9. Covebo
10. Brunel
11. Yacht
12. Maandag®
13. Hays Nederland
14. Michael Page / Page Personnel
15. TMI (Zorg)

## Tech Stack

- **Orchestration**: Dagster
- **Scraping**: Playwright + BeautifulSoup + requests
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

## Project Structure

```
.
├── staffing_agency_scraper/
│   ├── __init__.py
│   ├── definitions.py              # Main Dagster definitions
│   ├── resources.py                # Shared resources (DB, etc.)
│   ├── scraping/
│   │   ├── __init__.py
│   │   ├── definitions.py          # Scraping module definitions
│   │   └── agencies/               # Per-agency scrapers
│   │       ├── __init__.py
│   │       ├── randstad/
│   │       │   ├── assets.py
│   │       │   └── definitions.py
│   │       ├── tempo_team/
│   │       ├── youngcapital/
│   │       └── ...
│   ├── lib/
│   │   ├── fetch.py                # HTTP utilities
│   │   ├── browser.py              # Playwright utilities
│   │   ├── parse.py                # HTML parsing utilities
│   │   └── normalize.py            # Data normalization
│   └── models/
│       ├── __init__.py
│       └── agency.py               # Pydantic schema models
├── prisma/
│   ├── schema.prisma
│   └── migrations/
├── output/                         # JSON output directory
├── tests/
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
| Basic Identity | 8 | ✅ 8 | All implemented |
| Contact | 4 | ✅ 4 | All implemented |
| Geographic | 3 | ✅ 2 | office_locations partial |
| Market Positioning | 5 | ✅ 3 | role_levels, company_size_fit TODO |
| Specialisations | 4 | ✅ 2 | shift_types, typical_use_cases TODO |
| Services | 12 | ✅ 12 | All implemented |
| Legal/CAO | 6 | ✅ 4 | phase_system, inlenersbeloning TODO |
| Pricing | 9 | ⚠️ 1 | Most require manual analysis |
| Operational Claims | 7 | ⚠️ 1 | Most require marketing text analysis |
| Digital/AI | 12 | ✅ 7 | AI capabilities TODO |
| Review | 7 | ⚠️ 0 | Requires review page scraping |
| Meta | 4 | ✅ 4 | All implemented |

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

## Adding a New Agency

1. Create a new directory under `staffing_agency_scraper/scraping/agencies/`:
```bash
mkdir -p staffing_agency_scraper/scraping/agencies/new_agency
```

2. Create `assets.py` with the scraping logic
3. Create `definitions.py` with Dagster definitions
4. Register in `staffing_agency_scraper/scraping/definitions.py`

See existing agencies (especially `adecco/assets.py`) for examples.

## Environment Variables

| Name | Required | Example | Description |
|------|----------|---------|-------------|
| `PIPELINE_PG_CONNSTRING` | Yes | `postgres://user:pass@host:5432/db` | PostgreSQL connection string |
| `OUTPUT_DIR` | No | `./output` | Directory for JSON output |
| `LOG_INVALID_ROWS` | No | `true` | Log invalid rows during parsing |

## License

Proprietary - inhuren.nl
