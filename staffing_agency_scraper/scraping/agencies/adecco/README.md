# Adecco Scraper

## Overview
Scraper for **Adecco Nederland** - one of the world's largest staffing agencies, part of the Adecco Group.

## Data Sources

| Source | URL | Data Extracted |
|--------|-----|----------------|
| Main Website | `https://www.adecco.com/nl-nl` | Sectors, services |
| Employers Page | `https://www.adecco.com/nl-nl/werkgevers` | Services info |
| Work in Holland | `https://www.adecco.com/nl-nl/work-in-holland` | Sectors, cities |
| Contact Page | `https://www.adecco-jobs.com/amazon/nl-nl/contact/` | Phone, email |
| Privacy Policy | `https://www.adecco-jobs.com/amazon/nl-nl/privacy-policy/` | Logo, KvK, legal name, HQ address |
| Jobs API | `https://www.adecco.com/api/data/jobs/summarized` | Office locations, sectors, services confirmation |

---

## Data Collection Status

### ✅ Found (21 fields)

| Field | Value | Source |
|-------|-------|--------|
| `agency_name` | Adecco | Static |
| `legal_name` | Adecco Holding Nederland B.V. | Privacy policy |
| `brand_group` | Adecco Group | Static |
| `website_url` | https://www.adecco.nl | Static |
| `logo_url` | cdn.adecco-jobs.com/.../logo-red.webp | Privacy policy |
| `hq_city` | Zaltbommel | Privacy policy |
| `hq_province` | Gelderland | Privacy policy (postal code 5301) |
| `kvk_number` | 16033314 | Privacy policy |
| `contact_phone` | 065 3940431 | Contact page |
| `contact_email` | Amazonrecruitment@adecco.nl | Contact page |
| `contact_form_url` | ✓ Set | Static |
| `employers_page_url` | ✓ Set | Static |
| `regions_served` | landelijk, internationaal | Derived |
| `office_locations` | 8 cities | Privacy page + Jobs API |
| `geo_focus_type` | international | Static |
| `sectors_core` | 15 sectors | Homepage + API |
| `focus_segments` | 4 segments | Derived |
| `services.uitzenden` | ✓ true | Jobs API (534 temp jobs) |
| `services.detacheren` | ✓ true | Employers page |
| `services.werving_selectie` | ✓ true | Jobs API (356 perm jobs) |
| `services.payrolling` | ✓ true | Employers page |
| `digital_capabilities.candidate_portal` | ✓ true | Main site ("Mijn Adecco") |

### ❌ Not Found / Not Extracted (38 fields)

| Field | Reason |
|-------|--------|
| `role_levels` | API field usually null |
| `min_hours_per_week` | Job-specific, not agency policy |
| `avg_hourly_rate_*` | API shows worker wages, not agency rates |
| `annual_placements_estimate` | Active jobs ≠ annual placements |
| `shift_types_supported` | Unreliable (would need to parse job titles) |
| `certifications` | No SNA/VCU/ISO badges found |
| `membership` | No ABU/NBBU membership visible |
| `cao_type` | Not specified on site |
| `pricing_model` | Not disclosed |
| `omrekenfactor_min/max` | Not disclosed |
| `takeover_policy.*` | Not disclosed |
| `review_*` | No reviews scraped |
| `ai_capabilities.*` | Not confirmed |

---

## Extracted Details

### Office Locations (8 cities)
From privacy page and Jobs API job locations:
- Zaltbommel (HQ, Gelderland)
- Amsterdam (Noord-Holland)
- Tilburg (Noord-Brabant)
- Utrecht (Utrecht)
- Hoofddorp (Noord-Holland)
- Cuijk (Noord-Brabant)
- Varsseveld (Gelderland)
- Zeeland (province unknown)

### Sectors (15)
```
administratief, callcenter, finance, horeca, hr, ict, juridisch,
logistiek, productie, retail, sales, secretarieel, techniek,
verzekeringen, zorg
```

### Services Confirmed
- ✅ **Uitzenden** (temp staffing) - confirmed via API (534 temp jobs)
- ✅ **Werving & Selectie** (recruitment) - confirmed via API (356 perm jobs)
- ✅ **Payrolling** - mentioned on employers page
- ✅ **Detacheren** (secondment) - mentioned on employers page

### Job Statistics (from API - informational only)
| Metric | Count |
|--------|-------|
| Total active jobs | 891 |
| Temporary (uitzenden) | 534 |
| Permanent (werving & selectie) | 356 |

> Note: Job statistics are stored in `notes` field for reference only. We don't use these for `annual_placements_estimate` since active jobs ≠ annual placements.

---

## What We Don't Extract from Jobs API

The Jobs API contains job-specific data that doesn't represent agency policies:

| API Field | Why Not Extracted |
|-----------|-------------------|
| `minsalary/maxsalary` | Worker wages, not agency rates |
| `workMinHours` | Job-specific, not agency minimum |
| `exeprienceLevel` | Usually null in responses |
| `educationLevelTitle` | Education ≠ role level |

---

## Known Limitations

1. **Contact info is Amazon-specific**: The phone/email are for Amazon recruitment partnership, not general Adecco contact.
2. **Limited office locations**: Only cities from HQ address and API job locations, not full branch list.
3. **No review data**: Would need Google/Indeed/Glassdoor integration.
4. **CAO/Certification data**: Not visible on public pages, may need direct inquiry.
5. **Pricing data**: Adecco doesn't publish rates publicly.

---

## Run Command

```bash
uv run dagster job execute -m staffing_agency_scraper.definitions -j adecco_scrape_job
```

Output: `output/adecco.json`

---

## Potential Improvements

1. **Scrape general contact page** at `https://www.adecco.nl/nl-nl/contact` (requires Playwright for React content)
2. **Scrape branch/vestigingen page** for complete office locations
3. **Integrate Google Reviews API** for review data
4. **Parse Adecco Training page** to confirm `opleiden_ontwikkelen` service
5. **Check ABU membership** at ABU website

