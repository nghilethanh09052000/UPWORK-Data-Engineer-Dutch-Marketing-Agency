#!/usr/bin/env python3
"""
Sitemap Discovery Script for Dutch Staffing Agencies

This script discovers and analyzes sitemaps from agency websites to help
identify all available pages for scraping.

Usage:
    python scripts/discover_sitemap.py
    python scripts/discover_sitemap.py --agency randstad
    python scripts/discover_sitemap.py --output sitemaps.json
"""

import argparse
import json
import re
import sys
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional
from urllib.parse import urljoin, urlparse
import xml.etree.ElementTree as ET

import requests
from bs4 import BeautifulSoup


# Agencies to check - All 15 MVP agencies
AGENCIES = {
    "randstad": {
        "name": "Randstad",
        "base_url": "https://www.randstad.nl",
        "sitemap_urls": [
            "https://www.randstad.nl/sitemap.xml",
            "https://www.randstad.nl/sitemap_index.xml",
            "https://www.randstad.nl/robots.txt",
        ],
    },
    "tempo_team": {
        "name": "Tempo-Team",
        "base_url": "https://www.tempo-team.nl",
        "sitemap_urls": [
            "https://www.tempo-team.nl/sitemap.xml",
            "https://www.tempo-team.nl/sitemap_index.xml",
            "https://www.tempo-team.nl/robots.txt",
        ],
    },
    "youngcapital": {
        "name": "YoungCapital",
        "base_url": "https://www.youngcapital.nl",
        "sitemap_urls": [
            "https://www.youngcapital.nl/sitemap.xml",
            "https://www.youngcapital.nl/sitemap_index.xml",
            "https://www.youngcapital.nl/robots.txt",
        ],
    },
    "asa_talent": {
        "name": "ASA Talent",
        "base_url": "https://www.asa.nl",
        "sitemap_urls": [
            "https://www.asa.nl/sitemap.xml",
            "https://www.asa.nl/sitemap_index.xml",
            "https://www.asa.nl/robots.txt",
        ],
    },
    "manpower": {
        "name": "Manpower",
        "base_url": "https://www.manpower.nl",
        "sitemap_urls": [
            "https://www.manpower.nl/sitemap.xml",
            "https://www.manpower.nl/sitemap_index.xml",
            "https://www.manpower.nl/robots.txt",
        ],
    },
    "adecco": {
        "name": "Adecco",
        "base_url": "https://www.adecco.nl",
        "sitemap_urls": [
            "https://www.adecco.nl/sitemap.xml",
            "https://www.adecco.nl/sitemap_index.xml",
            "https://www.adecco.nl/robots.txt",
        ],
    },
    "olympia": {
        "name": "Olympia",
        "base_url": "https://www.olympia.nl",
        "sitemap_urls": [
            "https://www.olympia.nl/sitemap.xml",
            "https://www.olympia.nl/sitemap_index.xml",
            "https://www.olympia.nl/robots.txt",
        ],
    },
    "start_people": {
        "name": "Start People",
        "base_url": "https://www.startpeople.nl",
        "sitemap_urls": [
            "https://www.startpeople.nl/sitemap.xml",
            "https://www.startpeople.nl/sitemap_index.xml",
            "https://www.startpeople.nl/robots.txt",
        ],
    },
    "covebo": {
        "name": "Covebo",
        "base_url": "https://www.covebo.nl",
        "sitemap_urls": [
            "https://www.covebo.nl/sitemap.xml",
            "https://www.covebo.nl/sitemap_index.xml",
            "https://www.covebo.nl/robots.txt",
        ],
    },
    "brunel": {
        "name": "Brunel",
        "base_url": "https://www.brunel.nl",
        "sitemap_urls": [
            "https://www.brunel.nl/sitemap.xml",
            "https://www.brunel.nl/sitemap_index.xml",
            "https://www.brunel.nl/robots.txt",
        ],
    },
    "yacht": {
        "name": "Yacht",
        "base_url": "https://www.yacht.nl",
        "sitemap_urls": [
            "https://www.yacht.nl/sitemap.xml",
            "https://www.yacht.nl/sitemap_index.xml",
            "https://www.yacht.nl/robots.txt",
        ],
    },
    "maandag": {
        "name": "Maandag",
        "base_url": "https://www.maandag.nl",
        "sitemap_urls": [
            "https://www.maandag.nl/sitemap.xml",
            "https://www.maandag.nl/sitemap_index.xml",
            "https://www.maandag.nl/robots.txt",
        ],
    },
    "hays": {
        "name": "Hays Nederland",
        "base_url": "https://www.hays.nl",
        "sitemap_urls": [
            "https://www.hays.nl/sitemap.xml",
            "https://www.hays.nl/sitemap_index.xml",
            "https://www.hays.nl/robots.txt",
        ],
    },
    "michael_page": {
        "name": "Michael Page",
        "base_url": "https://www.michaelpage.nl",
        "sitemap_urls": [
            "https://www.michaelpage.nl/sitemap.xml",
            "https://www.michaelpage.nl/sitemap_index.xml",
            "https://www.michaelpage.nl/robots.txt",
        ],
    },
    "tmi": {
        "name": "TMI",
        "base_url": "https://www.tmi.nl",
        "sitemap_urls": [
            "https://www.tmi.nl/sitemap.xml",
            "https://www.tmi.nl/sitemap_index.xml",
            "https://www.tmi.nl/robots.txt",
        ],
    },
}

# Interesting URL patterns for scraping
INTERESTING_PATTERNS = [
    r"/werkgevers",
    r"/over",
    r"/contact",
    r"/diensten",
    r"/services",
    r"/sectoren",
    r"/branches",
    r"/vestigingen",
    r"/locaties",
    r"/filialen",
    r"/certificering",
    r"/kwaliteit",
    r"/voorwaarden",
    r"/algemene-voorwaarden",
    r"/privacy",
    r"/kvk",
    r"/about",
]


@dataclass
class SitemapResult:
    """Result from sitemap discovery."""
    agency: str
    base_url: str
    sitemap_found: bool = False
    sitemap_url: Optional[str] = None
    total_urls: int = 0
    interesting_urls: list = field(default_factory=list)
    all_urls: list = field(default_factory=list)
    errors: list = field(default_factory=list)


def get_headers():
    """Get browser-like headers."""
    return {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "nl-NL,nl;q=0.9,en-US;q=0.8,en;q=0.7",
    }


def fetch_robots_txt(base_url: str) -> list[str]:
    """
    Fetch robots.txt and extract sitemap URLs.
    
    Returns list of sitemap URLs found in robots.txt
    """
    robots_url = urljoin(base_url, "/robots.txt")
    sitemaps = []
    
    try:
        response = requests.get(robots_url, headers=get_headers(), timeout=10)
        if response.ok:
            for line in response.text.split("\n"):
                if line.lower().startswith("sitemap:"):
                    sitemap_url = line.split(":", 1)[1].strip()
                    sitemaps.append(sitemap_url)
            print(f"  📄 Found {len(sitemaps)} sitemap(s) in robots.txt")
    except Exception as e:
        print(f"  ⚠️  Could not fetch robots.txt: {e}")
    
    return sitemaps


def parse_sitemap_xml(url: str) -> tuple[list[str], list[str]]:
    """
    Parse a sitemap XML file.
    
    Returns tuple of (page_urls, sitemap_urls)
    - page_urls: URLs of actual pages
    - sitemap_urls: URLs of nested sitemaps (sitemap index)
    """
    page_urls = []
    sitemap_urls = []
    
    try:
        response = requests.get(url, headers=get_headers(), timeout=15)
        if not response.ok:
            return [], []
        
        # Parse XML
        root = ET.fromstring(response.content)
        
        # Handle namespace
        ns = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}
        
        # Check for sitemap index (contains other sitemaps)
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
        print(f"    ⚠️  XML parse error for {url}")
    except Exception as e:
        print(f"    ⚠️  Error fetching {url}: {e}")
    
    return page_urls, sitemap_urls


def discover_sitemap(agency_key: str, agency_config: dict) -> SitemapResult:
    """
    Discover sitemap for an agency.
    """
    result = SitemapResult(
        agency=agency_config["name"],
        base_url=agency_config["base_url"],
    )
    
    print(f"\n🔍 Discovering sitemap for {agency_config['name']}...")
    print(f"   Base URL: {agency_config['base_url']}")
    
    all_urls = set()
    sitemaps_to_check = []
    
    # First, check robots.txt
    robots_sitemaps = fetch_robots_txt(agency_config["base_url"])
    sitemaps_to_check.extend(robots_sitemaps)
    
    # Add default sitemap URLs
    sitemaps_to_check.extend(agency_config["sitemap_urls"])
    
    # Remove duplicates
    sitemaps_to_check = list(set(sitemaps_to_check))
    
    # Process all sitemaps
    processed_sitemaps = set()
    
    while sitemaps_to_check:
        sitemap_url = sitemaps_to_check.pop(0)
        
        if sitemap_url in processed_sitemaps:
            continue
        if "robots.txt" in sitemap_url:
            continue
            
        processed_sitemaps.add(sitemap_url)
        print(f"  📂 Checking: {sitemap_url}")
        
        page_urls, nested_sitemaps = parse_sitemap_xml(sitemap_url)
        
        if page_urls:
            result.sitemap_found = True
            result.sitemap_url = sitemap_url
            all_urls.update(page_urls)
            print(f"     Found {len(page_urls)} URLs")
        
        if nested_sitemaps:
            print(f"     Found {len(nested_sitemaps)} nested sitemaps")
            sitemaps_to_check.extend(nested_sitemaps)
    
    # If no sitemap found, try to crawl the homepage for links
    if not result.sitemap_found:
        print("  ⚠️  No sitemap found, crawling homepage for links...")
        homepage_urls = crawl_homepage_links(agency_config["base_url"])
        all_urls.update(homepage_urls)
    
    # Filter for interesting URLs
    result.all_urls = sorted(all_urls)
    result.total_urls = len(result.all_urls)
    
    for url in result.all_urls:
        for pattern in INTERESTING_PATTERNS:
            if re.search(pattern, url, re.IGNORECASE):
                result.interesting_urls.append(url)
                break
    
    result.interesting_urls = sorted(set(result.interesting_urls))
    
    print(f"  ✅ Total URLs found: {result.total_urls}")
    print(f"  ⭐ Interesting URLs: {len(result.interesting_urls)}")
    
    return result


def crawl_homepage_links(base_url: str) -> set[str]:
    """
    Crawl homepage to discover internal links.
    """
    urls = set()
    
    try:
        response = requests.get(base_url, headers=get_headers(), timeout=10)
        if response.ok:
            soup = BeautifulSoup(response.text, "lxml")
            domain = urlparse(base_url).netloc
            
            for link in soup.find_all("a", href=True):
                href = link["href"]
                full_url = urljoin(base_url, href)
                parsed = urlparse(full_url)
                
                # Only include same-domain links
                if parsed.netloc == domain and parsed.scheme in ("http", "https"):
                    # Remove fragments and query strings for cleaner URLs
                    clean_url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
                    urls.add(clean_url)
            
            print(f"     Crawled {len(urls)} links from homepage")
    except Exception as e:
        print(f"     Error crawling homepage: {e}")
    
    return urls


def print_results(results: list[SitemapResult]):
    """Print formatted results."""
    print("\n" + "=" * 80)
    print("📊 SITEMAP DISCOVERY RESULTS")
    print("=" * 80)
    
    for result in results:
        print(f"\n🏢 {result.agency}")
        print(f"   URL: {result.base_url}")
        print(f"   Sitemap found: {'✅ Yes' if result.sitemap_found else '❌ No'}")
        if result.sitemap_url:
            print(f"   Sitemap URL: {result.sitemap_url}")
        print(f"   Total URLs: {result.total_urls}")
        print(f"   Interesting URLs: {len(result.interesting_urls)}")
        
        if result.interesting_urls:
            print("\n   📌 Interesting URLs for scraping:")
            for url in result.interesting_urls[:30]:  # Limit to 30
                print(f"      - {url}")
            if len(result.interesting_urls) > 30:
                print(f"      ... and {len(result.interesting_urls) - 30} more")


def save_results(results: list[SitemapResult], output_file: str):
    """Save results to JSON file."""
    output = {
        "generated_at": datetime.utcnow().isoformat(),
        "agencies": []
    }
    
    for result in results:
        output["agencies"].append({
            "name": result.agency,
            "base_url": result.base_url,
            "sitemap_found": result.sitemap_found,
            "sitemap_url": result.sitemap_url,
            "total_urls": result.total_urls,
            "interesting_urls": result.interesting_urls,
            "all_urls": result.all_urls[:500],  # Limit to 500 for file size
        })
    
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)
    
    print(f"\n💾 Results saved to: {output_file}")


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
        "--list-all",
        action="store_true",
        help="List all URLs (not just interesting ones)",
    )
    
    args = parser.parse_args()
    
    # Select agencies to check
    if args.agency:
        agencies_to_check = {args.agency: AGENCIES[args.agency]}
    else:
        agencies_to_check = AGENCIES
    
    # Discover sitemaps
    results = []
    for agency_key, agency_config in agencies_to_check.items():
        result = discover_sitemap(agency_key, agency_config)
        results.append(result)
    
    # Print results
    print_results(results)
    
    # Optionally list all URLs
    if args.list_all:
        print("\n" + "=" * 80)
        print("📋 ALL DISCOVERED URLs")
        print("=" * 80)
        for result in results:
            print(f"\n{result.agency}:")
            for url in result.all_urls:
                print(f"  {url}")
    
    # Save results
    save_results(results, args.output)


if __name__ == "__main__":
    main()

