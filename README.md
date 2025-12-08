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

## Output Schema

Each agency produces a JSON record following this schema:

```json
{
  "id": "UUID",
  "agency_name": "string",
  "legal_name": "string",
  "logo_url": "string",
  "website_url": "string",
  "brand_group": "string",
  "hq_city": "string",
  "hq_province": "string",
  "kvk_number": "string",
  "contact_phone": "string",
  "contact_email": "string",
  "services": {
    "uitzenden": true,
    "detacheren": true,
    ...
  },
  "sectors_core": ["logistiek", "productie"],
  "certifications": ["NEN-4400-1", "SNA"],
  "evidence_urls": ["https://..."],
  "collected_at": "2025-12-08T12:00:00Z"
}
```

See `staffing_agency_scraper/models/agency.py` for the complete schema.

## Adding a New Agency

1. Create a new directory under `staffing_agency_scraper/scraping/agencies/`:
```bash
mkdir -p staffing_agency_scraper/scraping/agencies/new_agency
```

2. Create `assets.py` with the scraping logic
3. Create `definitions.py` with Dagster definitions
4. Register in `staffing_agency_scraper/scraping/definitions.py`

See existing agencies for examples.

## Environment Variables

| Name | Required | Example | Description |
|------|----------|---------|-------------|
| `PIPELINE_PG_CONNSTRING` | Yes | `postgres://user:pass@host:5432/db` | PostgreSQL connection string |
| `OUTPUT_DIR` | No | `./output` | Directory for JSON output |
| `LOG_INVALID_ROWS` | No | `true` | Log invalid rows during parsing |

## License

Proprietary - inhuren.nl

