#!/usr/bin/env python3
"""
Sitemap Discovery Script for Dutch Staffing Agencies

This script discovers and analyzes sitemaps from agency websites to help
identify all available pages for scraping. URLs are categorized by which
schema fields they could help populate.

Usage:
    python scripts/discover_sitemap.py
    python scripts/discover_sitemap.py --agency randstad
    python scripts/discover_sitemap.py --output sitemaps.json
"""

import argparse
import json
import re
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional
from urllib.parse import urljoin, urlparse
import xml.etree.ElementTree as ET
import threading

import requests
from bs4 import BeautifulSoup

# Thread-safe print
print_lock = threading.Lock()

def safe_print(*args, **kwargs):
    """Thread-safe print."""
    with print_lock:
        print(*args, **kwargs)
        sys.stdout.flush()


# Agencies to check - All 15 MVP agencies
AGENCIES = {
    "randstad": {
        "name": "Randstad",
        "base_url": "https://www.randstad.nl",
    },
    "tempo_team": {
        "name": "Tempo-Team",
        "base_url": "https://www.tempo-team.nl",
    },
    "youngcapital": {
        "name": "YoungCapital",
        "base_url": "https://www.youngcapital.nl",
    },
    "asa_talent": {
        "name": "ASA Talent",
        "base_url": "https://www.asa.nl",
    },
    "manpower": {
        "name": "Manpower",
        "base_url": "https://www.manpower.nl",
    },
    "adecco": {
        "name": "Adecco",
        "base_url": "https://www.adecco.nl",
    },
    "olympia": {
        "name": "Olympia",
        "base_url": "https://www.olympia.nl",
    },
    "start_people": {
        "name": "Start People",
        "base_url": "https://www.startpeople.nl",
    },
    "covebo": {
        "name": "Covebo",
        "base_url": "https://www.covebo.nl",
    },
    "brunel": {
        "name": "Brunel",
        "base_url": "https://www.brunel.nl",
    },
    "yacht": {
        "name": "Yacht",
        "base_url": "https://www.yacht.nl",
    },
    "maandag": {
        "name": "Maandag",
        "base_url": "https://www.maandag.nl",
    },
    "hays": {
        "name": "Hays Nederland",
        "base_url": "https://www.hays.nl",
    },
    "michael_page": {
        "name": "Michael Page",
        "base_url": "https://www.michaelpage.nl",
    },
    "tmi": {
        "name": "TMI",
        "base_url": "https://www.tmi.nl",
    },
}

# URL patterns mapped to schema fields they can populate
# Organized by which JSON schema section they relate to
URL_PATTERNS_BY_CATEGORY = {
    # --- Basic Identity ---
    "identity": {
        "description": "agency_name, legal_name, logo_url, brand_group, hq_city, kvk_number",
        "patterns": [
            r"/over[-\s]?(ons|mij)?",
            r"/about",
            r"/wie[-\s]?zijn[-\s]?wij",
            r"/bedrijf",
            r"/organisatie",
            r"/geschiedenis",
            r"/onze[-\s]?organisatie",
        ],
    },
    
    # --- Contact Information ---
    "contact": {
        "description": "contact_phone, contact_email, contact_form_url, employers_page_url",
        "patterns": [
            r"/contact",
            r"/werkgevers",
            r"/opdrachtgevers",
            r"/klanten",
            r"/zakelijk",
            r"/voor[-\s]?werkgevers",
            r"/voor[-\s]?bedrijven",
            r"/personeel[-\s]?nodig",
            r"/personeel[-\s]?zoeken",
            r"/aanvragen",
            r"/offerte",
        ],
    },
    
    # --- Geographic Coverage (office_locations, regions_served) ---
    "locations": {
        "description": "office_locations, regions_served, geo_focus_type",
        "patterns": [
            r"/vestiging",
            r"/locatie",
            r"/filiaal",
            r"/kantoor",
            r"/branches",
            r"/regio",
            r"/in[-\s]?de[-\s]?buurt",
            r"/amsterdam|rotterdam|den[-\s]?haag|utrecht|eindhoven",
            r"/noord|zuid|oost|west",
        ],
    },
    
    # --- Services (services object) ---
    "services": {
        "description": "services.uitzenden, detacheren, werving_selectie, payrolling, etc.",
        "patterns": [
            r"/dienst",
            r"/service",
            r"/uitzend",
            r"/detacher",
            r"/werving",
            r"/selectie",
            r"/recruitment",
            r"/payroll",
            r"/zzp",
            r"/freelance",
            r"/inhouse",
            r"/on[-\s]?site",
            r"/msp",
            r"/rpo",
            r"/executive[-\s]?search",
            r"/opleid",
            r"/training",
            r"/academy",
            r"/outplacement",
            r"/re[-\s]?integratie",
        ],
    },
    
    # --- Sectors (sectors_core, sectors_secondary) ---
    "sectors": {
        "description": "sectors_core, sectors_secondary",
        "patterns": [
            r"/sector",
            r"/branche",
            r"/vakgebied",
            r"/industrie",
            r"/logistiek",
            r"/productie",
            r"/zorg",
            r"/healthcare",
            r"/techniek",
            r"/engineering",
            r"/it[-\s]?|/ict",
            r"/finance",
            r"/administratief",
            r"/hr",
            r"/sales",
            r"/marketing",
            r"/horeca",
            r"/retail",
            r"/bouw",
            r"/transport",
        ],
    },
    
    # --- Focus Segments (focus_segments, role_levels) ---
    "segments": {
        "description": "focus_segments, role_levels, customer_segments",
        "patterns": [
            r"/student",
            r"/starter",
            r"/young[-\s]?professional",
            r"/traineeship",
            r"/professional",
            r"/specialist",
            r"/executive",
            r"/interim",
            r"/mkb",
            r"/enterprise",
            r"/overheid",
        ],
    },
    
    # --- Legal/CAO/Compliance ---
    "legal": {
        "description": "cao_type, certifications, membership, kvk_number",
        "patterns": [
            r"/voorwaarden",
            r"/algemene[-\s]?voorwaarden",
            r"/terms",
            r"/privacy",
            r"/disclaimer",
            r"/juridisch",
            r"/legal",
            r"/kvk",
            r"/certificer",
            r"/kwaliteit",
            r"/keurmerk",
            r"/sna",
            r"/nen[-\s]?4400",
            r"/vcu",
            r"/iso",
            r"/abu",
            r"/nbbu",
            r"/cao",
            r"/compliance",
        ],
    },
    
    # --- Pricing (pricing_model, takeover_policy) ---
    "pricing": {
        "description": "pricing_model, pricing_transparency, takeover_policy, omrekenfactor",
        "patterns": [
            r"/tarief",
            r"/prijs",
            r"/kosten",
            r"/tarieven",
            r"/pricing",
            r"/overname",
            r"/overstap",
        ],
    },
    
    # --- Digital Capabilities ---
    "digital": {
        "description": "digital_capabilities (portal, app, api)",
        "patterns": [
            r"/portaal",
            r"/portal",
            r"/platform",
            r"/mijn[-\s]?",
            r"/login",
            r"/inloggen",
            r"/app",
            r"/api",
            r"/koppeling",
            r"/integratie",
        ],
    },
    
    # --- Reviews/Reputation ---
    "reviews": {
        "description": "review_rating, review_sources, review_themes",
        "patterns": [
            r"/review",
            r"/ervaring",
            r"/testimonial",
            r"/klantervaring",
            r"/waardering",
            r"/referentie",
        ],
    },
    
    # --- News/About (growth_signals) ---
    "news": {
        "description": "growth_signals, notes",
        "patterns": [
            r"/nieuws",
            r"/news",
            r"/blog",
            r"/artikel",
            r"/pers",
            r"/media",
            r"/carriere",
            r"/werken[-\s]?bij",
        ],
    },
}

# URLs to EXCLUDE (job listings, individual vacatures, etc.)
EXCLUDE_PATTERNS = [
    r"/vacature/",  # Individual job listings
    r"/vacatures/\d",  # Job listing with ID
    r"/job/",
    r"/jobs/",
    r"/baan/",
    r"/werk/\d",
    r"/apply",
    r"/sollicit",
    r"/cv",
    r"/profiel",
    r"/inschrijven",
    r"/login",
    r"/register",
    r"/kandidaat",
    r"/medewerker/",
    r"\?page=",
    r"\?p=",
    r"/page/\d",
    r"/tag/",
    r"/category/",
    r"/author/",
]


@dataclass
class CategorizedUrl:
    """A URL with its category."""
    url: str
    category: str
    description: str


@dataclass
class SitemapResult:
    """Result from sitemap discovery."""
    agency: str
    base_url: str
    sitemap_found: bool = False
    sitemap_url: Optional[str] = None
    total_urls: int = 0
    categorized_urls: dict = field(default_factory=dict)  # category -> list of urls
    uncategorized_urls: list = field(default_factory=list)
    all_urls: list = field(default_factory=list)
    recommended_scrape_urls: list = field(default_factory=list)
    errors: list = field(default_factory=list)


def get_headers():
    """Get browser-like headers."""
    return {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "nl-NL,nl;q=0.9,en-US;q=0.8,en;q=0.7",
    }


def should_exclude_url(url: str) -> bool:
    """Check if URL should be excluded (job listings, etc.)"""
    url_lower = url.lower()
    for pattern in EXCLUDE_PATTERNS:
        if re.search(pattern, url_lower):
            return True
    return False


def categorize_url(url: str) -> tuple[str, str] | None:
    """
    Categorize a URL based on which schema fields it might help populate.
    
    Returns (category, description) or None if no match.
    """
    url_lower = url.lower()
    
    for category, config in URL_PATTERNS_BY_CATEGORY.items():
        for pattern in config["patterns"]:
            if re.search(pattern, url_lower):
                return (category, config["description"])
    
    return None


def fetch_robots_txt(base_url: str, agency_name: str = "") -> list[str]:
    """
    Fetch robots.txt and extract sitemap URLs.
    """
    robots_url = urljoin(base_url, "/robots.txt")
    sitemaps = []
    prefix = f"[{agency_name}] " if agency_name else ""
    
    try:
        response = requests.get(robots_url, headers=get_headers(), timeout=10)
        if response.ok:
            for line in response.text.split("\n"):
                line = line.strip()
                if line.lower().startswith("sitemap:"):
                    sitemap_url = line.split(":", 1)[1].strip()
                    sitemaps.append(sitemap_url)
            if sitemaps:
                safe_print(f"{prefix}📄 Found {len(sitemaps)} sitemap(s) in robots.txt")
    except Exception as e:
        safe_print(f"{prefix}⚠️  Could not fetch robots.txt: {e}")
    
    return sitemaps


def parse_sitemap_xml(url: str, agency_name: str = "") -> tuple[list[str], list[str]]:
    """
    Parse a sitemap XML file.
    
    Returns tuple of (page_urls, sitemap_urls)
    """
    page_urls = []
    sitemap_urls = []
    prefix = f"[{agency_name}] " if agency_name else ""
    
    try:
        response = requests.get(url, headers=get_headers(), timeout=10)
        if not response.ok:
            return [], []
        
        # Parse XML
        root = ET.fromstring(response.content)
        
        # Handle namespace
        ns = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}
        
        # Check for sitemap index
        for sitemap in root.findall(".//sm:sitemap", ns):
            loc = sitemap.find("sm:loc", ns)
            if loc is not None and loc.text:
                sitemap_urls.append(loc.text)
        
        # Check for regular URLs
        for url_elem in root.findall(".//sm:url", ns):
            loc = url_elem.find("sm:loc", ns)
            if loc is not None and loc.text:
                page_urls.append(loc.text)
        
        # Try without namespace if nothing found
        if not page_urls and not sitemap_urls:
            for sitemap in root.findall(".//sitemap"):
                loc = sitemap.find("loc")
                if loc is not None and loc.text:
                    sitemap_urls.append(loc.text)
            
            for url_elem in root.findall(".//url"):
                loc = url_elem.find("loc")
                if loc is not None and loc.text:
                    page_urls.append(loc.text)
    
    except ET.ParseError:
        pass  # Silent fail for parse errors
    except Exception:
        pass  # Silent fail for network errors
    
    return page_urls, sitemap_urls


def crawl_homepage_links(base_url: str, depth: int = 2) -> set[str]:
    """
    Crawl homepage and key pages to discover internal links.
    """
    urls = set()
    visited = set()
    to_visit = [base_url]
    domain = urlparse(base_url).netloc
    
    current_depth = 0
    while to_visit and current_depth < depth:
        next_level = []
        
        for url in to_visit:
            if url in visited:
                continue
            visited.add(url)
            
            try:
                response = requests.get(url, headers=get_headers(), timeout=10)
                if response.ok:
                    soup = BeautifulSoup(response.text, "lxml")
                    
                    for link in soup.find_all("a", href=True):
                        href = link["href"]
                        full_url = urljoin(url, href)
                        parsed = urlparse(full_url)
                        
                        # Only include same-domain links
                        if parsed.netloc == domain and parsed.scheme in ("http", "https"):
                            clean_url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
                            if clean_url not in urls:
                                urls.add(clean_url)
                                if current_depth < depth - 1:
                                    next_level.append(clean_url)
            except Exception:
                pass
        
        to_visit = next_level[:50]  # Limit per level
        current_depth += 1
    
    return urls


def discover_sitemap(agency_key: str, agency_config: dict) -> SitemapResult:
    """
    Discover sitemap for an agency and categorize URLs.
    """
    name = agency_config["name"]
    base = agency_config["base_url"]
    
    result = SitemapResult(
        agency=name,
        base_url=base,
    )
    
    safe_print(f"\n🔍 [{name}] Starting discovery...")
    
    all_urls = set()
    sitemaps_to_check = []
    
    # Check robots.txt
    robots_sitemaps = fetch_robots_txt(base, name)
    sitemaps_to_check.extend(robots_sitemaps)
    
    # Add common sitemap URLs
    common_sitemaps = [
        f"{base}/sitemap.xml",
        f"{base}/sitemap_index.xml",
        f"{base}/sitemap-index.xml",
        f"{base}/wp-sitemap.xml",
    ]
    sitemaps_to_check.extend(common_sitemaps)
    
    # Remove duplicates
    sitemaps_to_check = list(set(sitemaps_to_check))
    
    # Process all sitemaps
    processed_sitemaps = set()
    sitemap_count = 0
    
    for sitemap_url in sitemaps_to_check:
        if sitemap_url in processed_sitemaps:
            continue
        if "robots.txt" in sitemap_url:
            continue
            
        processed_sitemaps.add(sitemap_url)
        
        page_urls, nested_sitemaps = parse_sitemap_xml(sitemap_url, name)
        
        if page_urls:
            result.sitemap_found = True
            result.sitemap_url = sitemap_url
            all_urls.update(page_urls)
            sitemap_count += 1
            safe_print(f"[{name}] ✅ Sitemap #{sitemap_count}: {len(page_urls)} URLs from {sitemap_url.split('/')[-1]}")
        
        if nested_sitemaps:
            safe_print(f"[{name}] 📁 Found {len(nested_sitemaps)} nested sitemaps")
            for nested in nested_sitemaps:
                if nested not in processed_sitemaps:
                    sitemaps_to_check.append(nested)
    
    # If no sitemap found, crawl the homepage (quick crawl)
    if not all_urls:
        safe_print(f"[{name}] ⚠️  No sitemap, crawling homepage...")
        homepage_urls = crawl_homepage_links(base, depth=1)
        all_urls.update(homepage_urls)
        safe_print(f"[{name}] Found {len(homepage_urls)} URLs from homepage")
    
    # Filter out excluded URLs and categorize
    result.categorized_urls = {cat: [] for cat in URL_PATTERNS_BY_CATEGORY.keys()}
    
    for url in all_urls:
        if should_exclude_url(url):
            continue
        
        result.all_urls.append(url)
        
        category_info = categorize_url(url)
        if category_info:
            category, _ = category_info
            result.categorized_urls[category].append(url)
        else:
            result.uncategorized_urls.append(url)
    
    result.total_urls = len(result.all_urls)
    
    # Build recommended scrape URLs
    result.recommended_scrape_urls = [base]
    
    for category in ["identity", "contact", "services", "sectors", "legal", "locations"]:
        urls = result.categorized_urls.get(category, [])
        if urls:
            best = sorted(urls, key=len)[:3]
            result.recommended_scrape_urls.extend(best)
    
    result.recommended_scrape_urls = list(dict.fromkeys(result.recommended_scrape_urls))[:15]
    
    # Print summary
    categories_found = sum(1 for urls in result.categorized_urls.values() if urls)
    safe_print(f"[{name}] ✅ DONE: {result.total_urls} URLs, {categories_found} categories, {len(result.recommended_scrape_urls)} recommended")
    
    return result


def save_results(results: list[SitemapResult], output_file: str):
    """Save results to JSON file."""
    output = {
        "generated_at": datetime.utcnow().isoformat(),
        "total_agencies": len(results),
        "agencies": []
    }
    
    for result in results:
        agency_data = {
            "name": result.agency,
            "base_url": result.base_url,
            "sitemap_found": result.sitemap_found,
            "sitemap_url": result.sitemap_url,
            "total_urls": result.total_urls,
            "recommended_scrape_urls": result.recommended_scrape_urls,
            "urls_by_category": {},
            "uncategorized_urls": result.uncategorized_urls[:50],
        }
        
        for category, urls in result.categorized_urls.items():
            if urls:
                agency_data["urls_by_category"][category] = {
                    "description": URL_PATTERNS_BY_CATEGORY[category]["description"],
                    "count": len(urls),
                    "urls": urls[:30],  # Limit for file size
                }
        
        output["agencies"].append(agency_data)
    
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)
    
    print(f"\n💾 Results saved to: {output_file}")


def generate_scraper_config(results: list[SitemapResult], output_file: str):
    """Generate a Python config file with recommended URLs for each agency."""
    
    config_lines = [
        '"""',
        'Auto-generated scraper URLs configuration.',
        f'Generated: {datetime.utcnow().isoformat()}',
        '',
        'These URLs were discovered from sitemaps and categorized',
        'based on which JSON schema fields they can help populate.',
        '"""',
        '',
        'AGENCY_SCRAPE_URLS = {',
    ]
    
    for result in results:
        agency_key = result.agency.lower().replace(" ", "_").replace("-", "_")
        config_lines.append(f'    "{agency_key}": {{')
        config_lines.append(f'        "name": "{result.agency}",')
        config_lines.append(f'        "base_url": "{result.base_url}",')
        config_lines.append(f'        "pages_to_scrape": [')
        
        for url in result.recommended_scrape_urls[:15]:
            config_lines.append(f'            "{url}",')
        
        config_lines.append(f'        ],')
        
        # Add categorized URLs for reference
        config_lines.append(f'        "urls_by_category": {{')
        for category, urls in result.categorized_urls.items():
            if urls:
                config_lines.append(f'            "{category}": [')
                for url in urls[:5]:
                    config_lines.append(f'                "{url}",')
                config_lines.append(f'            ],')
        config_lines.append(f'        }},')
        config_lines.append(f'    }},')
    
    config_lines.append('}')
    
    with open(output_file, "w", encoding="utf-8") as f:
        f.write("\n".join(config_lines))
    
    print(f"💾 Scraper config saved to: {output_file}")


def main():
    parser = argparse.ArgumentParser(description="Discover sitemaps for staffing agencies")
    parser.add_argument(
        "--agency",
        choices=list(AGENCIES.keys()),
        help="Specific agency to check (default: all)",
    )
    parser.add_argument(
        "--output",
        default="output/sitemaps.json",
        help="Output JSON file (default: output/sitemaps.json)",
    )
    parser.add_argument(
        "--config",
        default="output/scraper_urls.py",
        help="Output Python config file (default: output/scraper_urls.py)",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=5,
        help="Number of parallel workers (default: 5)",
    )
    
    args = parser.parse_args()
    
    # Select agencies
    if args.agency:
        agencies_to_check = {args.agency: AGENCIES[args.agency]}
    else:
        agencies_to_check = AGENCIES
    
    print("🚀 Starting sitemap discovery for Dutch staffing agencies")
    print(f"   Checking {len(agencies_to_check)} agencies with {args.workers} parallel workers...")
    print("=" * 70)
    
    # Discover sitemaps in parallel
    results = []
    
    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        # Submit all tasks
        future_to_agency = {
            executor.submit(discover_sitemap, key, config): key
            for key, config in agencies_to_check.items()
        }
        
        # Collect results as they complete
        for future in as_completed(future_to_agency):
            agency_key = future_to_agency[future]
            try:
                result = future.result()
                results.append(result)
            except Exception as e:
                safe_print(f"❌ [{agency_key}] Error: {e}")
    
    # Sort results by agency name
    results.sort(key=lambda r: r.agency)
    
    # Print final summary
    print("\n" + "=" * 70)
    print("📊 FINAL SUMMARY")
    print("=" * 70)
    
    total_urls = 0
    for result in results:
        sitemap_status = "✅" if result.sitemap_found else "❌"
        print(f"{sitemap_status} {result.agency:20} | {result.total_urls:5} URLs | Recommended: {len(result.recommended_scrape_urls)}")
        total_urls += result.total_urls
    
    print("=" * 70)
    print(f"📈 TOTAL: {total_urls} URLs across {len(results)} agencies")
    
    # Save results
    save_results(results, args.output)
    generate_scraper_config(results, args.config)


if __name__ == "__main__":
    main()
