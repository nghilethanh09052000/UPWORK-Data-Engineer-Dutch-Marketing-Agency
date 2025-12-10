"""
Shared utility functions for all staffing agency scrapers.

This module provides reusable extraction methods using BS4 and regex.
Based on the 69 fields defined in output/_sample.json.
"""

from __future__ import annotations

import re
from typing import Dict, List, Optional

from bs4 import BeautifulSoup

from staffing_agency_scraper.models import (
    AgencyServices,
    CaoType,
    OfficeLocation,
)


class AgencyScraperUtils:
    """
    Utility class with reusable extraction methods for all agencies.
    
    Uses BeautifulSoup and regex for fast, reliable extraction.
    """
    
    def __init__(self, logger):
        """
        Initialize scraper utilities.
        
        Args:
            logger: Dagster logger instance
        """
        self.logger = logger
    
    # ========================================================================
    # BASIC IDENTITY (Fields 1-10 from _sample.json)
    # ========================================================================
    
    def fetch_logo(self, soup: BeautifulSoup, url: str) -> Optional[str]:
        """
        Extract logo URL from page (PNG/SVG only, from header/footer).
        
        Client requirement: Only real logos, not banners or hero images.
        """
        # Priority 1: Header/footer logos with PNG/SVG
        for section in soup.select("header, footer, .header, .footer, nav, .navbar"):
            for img in section.find_all("img"):
                src = img.get("src") or img.get("data-src") or ""
                alt = img.get("alt", "").lower()
                
                # Must be PNG or SVG
                if not (src.endswith('.png') or src.endswith('.svg') or '.png?' in src or '.svg?' in src):
                    continue
                
                # Check if it looks like a logo
                if any(keyword in src.lower() or keyword in alt for keyword in ['logo', 'brand']):
                    self.logger.info(f"✓ Found logo (PNG/SVG): {src} | Source: {url}")
                    return src
        
        # Priority 2: Any PNG/SVG with "logo" in filename (but NOT banner/hero)
        for img in soup.find_all("img"):
            src = img.get("src") or img.get("data-src") or ""
            
            # Must be PNG/SVG
            if not (src.endswith('.png') or src.endswith('.svg') or '.png?' in src or '.svg?' in src):
                continue
            
            # Must have "logo" in path, but NOT banner/hero/slide
            src_lower = src.lower()
            if "logo" in src_lower and not any(x in src_lower for x in ['banner', 'hero', 'slide', 'carousel']):
                self.logger.info(f"✓ Found logo (PNG/SVG): {src} | Source: {url}")
                return src
        
        return None
    
    def fetch_kvk_number(self, text: str, url: str) -> Optional[str]:
        """Extract KvK (Chamber of Commerce) number."""
        kvk_match = re.search(r'(?:KvK|kvk|chamber of commerce)[\s\-:]*(\d{8})', text, re.IGNORECASE)
        if kvk_match:
            kvk = kvk_match.group(1)
            self.logger.info(f"✓ Found KvK: {kvk} | Source: {url}")
            return kvk
        return None
    
    def fetch_legal_name(self, text: str, agency_name: str, url: str) -> Optional[str]:
        """Extract legal name (e.g., 'Company B.V.')."""
        # Generic pattern for Dutch legal entities
        patterns = [
            rf"({agency_name}\s+(?:\w+\s+)?B\.?V\.?)",
            rf"({agency_name}\s+(?:Nederland|Netherlands)\s+B\.?V\.?)",
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                legal_name = match.group(1).strip()
                self.logger.info(f"✓ Found legal_name: {legal_name} | Source: {url}")
                return legal_name
        
        return None
    
    # ========================================================================
    # CONTACT (Fields 11-14 from _sample.json)
    # ========================================================================
    
    def fetch_contact_email(self, text: str, url: str) -> Optional[str]:
        """Extract generic business email."""
        email_match = re.search(r'([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})', text)
        if email_match:
            email = email_match.group(1)
            # Prefer info@, contact@, sales@
            if any(prefix in email.lower() for prefix in ['info@', 'contact@', 'sales@', 'werkgevers@']):
                self.logger.info(f"✓ Found email: {email} | Source: {url}")
                return email
        return None
    
    def fetch_contact_phone(self, text: str, url: str) -> Optional[str]:
        """Extract business phone number."""
        phone_patterns = [
            r'\+31[\s-]?\d{1,2}[\s-]?\d{3}[\s-]?\d{2}[\s-]?\d{2}',
            r'0\d{2,3}[\s-]?\d{3}[\s-]?\d{2}[\s-]?\d{2}',
        ]
        
        for pattern in phone_patterns:
            phone_match = re.search(pattern, text)
            if phone_match:
                phone = phone_match.group(0)
                self.logger.info(f"✓ Found phone: {phone} | Source: {url}")
                return phone
        
        return None
    
    def fetch_office_locations(self, soup: BeautifulSoup, url: str) -> List[OfficeLocation]:
        """Extract office locations from page."""
        offices = []
        
        # Common Dutch cities with province mapping
        city_province_map = {
            "amsterdam": "Noord-Holland",
            "rotterdam": "Zuid-Holland",
            "utrecht": "Utrecht",
            "den haag": "Zuid-Holland",
            "eindhoven": "Noord-Brabant",
            "tilburg": "Noord-Brabant",
            "groningen": "Groningen",
            "breda": "Noord-Brabant",
            "nijmegen": "Gelderland",
            "apeldoorn": "Gelderland",
        }
        
        # Look for city names in headers
        for header in soup.find_all(['h2', 'h3', 'h4']):
            city_text = header.get_text(strip=True).lower()
            for city, province in city_province_map.items():
                if city in city_text:
                    office = OfficeLocation(city=city.title(), province=province)
                    if office not in offices:
                        offices.append(office)
                        self.logger.info(f"✓ Found office: {city.title()} | Source: {url}")
        
        return offices
    
    # ========================================================================
    # SECTORS (Normalized list from client)
    # ========================================================================
    
    def fetch_sectors(self, text: str, url: str) -> List[str]:
        """
        Extract sectors using client's normalized list.
        
        Client requirement: Only standard sectors, not work types like "thuiswerk", "oproepkracht".
        """
        text_lower = text.lower()
        sectors = []
        
        # Normalized sector list from client
        sector_keywords = {
            "logistiek": ["logistiek", "transport", "warehouse", "magazijn"],
            "horeca": ["horeca", "hospitality", "restaurant", "hotel"],
            "zorg": ["zorg", "healthcare", "care", "verpleging", "ggz"],
            "techniek": ["techniek", "technical", "engineering", "installatie"],
            "office": ["office", "kantoor", "administratie", "backoffice"],
            "finance": ["finance", "financieel", "accounting", "boekhouding"],
            "marketing": ["marketing", "communicatie", "pr", "sales"],
            "retail": ["retail", "winkel", "verkoop"],
            "industrie": ["industrie", "productie", "manufacturing"],
            "bouw": ["bouw", "construction", "aannemer"],
            "it": ["it", "ict", "software", "developer", "data"],
            "hr": ["hr", "human resources", "recruitment"],
            "legal": ["legal", "juridisch", "recht"],
            "onderwijs": ["onderwijs", "education", "leraar", "docent"],
            "overheid": ["overheid", "government", "publieke sector"],
        }
        
        for sector, keywords in sector_keywords.items():
            for keyword in keywords:
                if keyword in text_lower and sector not in sectors:
                    sectors.append(sector)
                    self.logger.info(f"✓ Found sector: {sector} | Source: {url}")
                    break
        
        return sectors
    
    # ========================================================================
    # SERVICES (Field 24 from _sample.json)
    # ========================================================================
    
    def fetch_services(self, text: str, url: str) -> AgencyServices:
        """Extract services from text."""
        text_lower = text.lower()
        
        services = AgencyServices()
        
        if "uitzenden" in text_lower:
            services.uitzenden = True
            self.logger.info(f"✓ Found service: uitzenden | Source: {url}")
        
        if "detacheren" in text_lower or "detachering" in text_lower:
            services.detacheren = True
            self.logger.info(f"✓ Found service: detacheren | Source: {url}")
        
        if "werving" in text_lower or "selectie" in text_lower:
            services.werving_selectie = True
            self.logger.info(f"✓ Found service: werving_selectie | Source: {url}")
        
        if "payroll" in text_lower:
            services.payrolling = True
            self.logger.info(f"✓ Found service: payrolling | Source: {url}")
        
        if "zzp" in text_lower or "freelance" in text_lower:
            services.zzp_bemiddeling = True
            self.logger.info(f"✓ Found service: zzp_bemiddeling | Source: {url}")
        
        if "msp" in text_lower or "managed service" in text_lower:
            services.msp = True
            self.logger.info(f"✓ Found service: msp | Source: {url}")
        
        if "rpo" in text_lower or "recruitment process outsourcing" in text_lower:
            services.rpo = True
            self.logger.info(f"✓ Found service: rpo | Source: {url}")
        
        if "executive search" in text_lower:
            services.executive_search = True
            self.logger.info(f"✓ Found service: executive_search | Source: {url}")
        
        return services
    
    # ========================================================================
    # CAO & LEGAL (Fields 28-35 from _sample.json)
    # ========================================================================
    
    def fetch_cao_type(self, text: str, url: str) -> CaoType:
        """Extract CAO type."""
        text_lower = text.lower()
        
        if "abu" in text_lower:
            self.logger.info(f"✓ Found CAO: ABU | Source: {url}")
            return CaoType.ABU
        elif "nbbu" in text_lower:
            self.logger.info(f"✓ Found CAO: NBBU | Source: {url}")
            return CaoType.NBBU
        
        return CaoType.ONBEKEND
    
    def fetch_membership(self, text: str, url: str) -> List[str]:
        """Extract membership organizations."""
        text_lower = text.lower()
        membership = []
        
        if "abu" in text_lower:
            membership.append("ABU")
            self.logger.info(f"✓ Found membership: ABU | Source: {url}")
        if "nbbu" in text_lower:
            membership.append("NBBU")
            self.logger.info(f"✓ Found membership: NBBU | Source: {url}")
        if "nrto" in text_lower:
            membership.append("NRTO")
            self.logger.info(f"✓ Found membership: NRTO | Source: {url}")
        
        return membership
    
    def fetch_phase_system(self, text: str, url: str) -> Optional[str]:
        """Extract phase system (fasensysteem)."""
        text_lower = text.lower()
        
        if "3 fasen" in text_lower or "3 phases" in text_lower:
            self.logger.info(f"✓ Found phase_system: 3_fasen | Source: {url}")
            return "3_fasen"
        elif "4 fasen" in text_lower or "4 phases" in text_lower:
            self.logger.info(f"✓ Found phase_system: 4_fasen | Source: {url}")
            return "4_fasen"
        
        return None
    
    def fetch_certifications(self, text: str, url: str) -> List[str]:
        """Extract certifications."""
        text_lower = text.lower()
        certs = []
        
        cert_keywords = {
            "iso 9001": "ISO 9001",
            "iso9001": "ISO 9001",
            "sna": "SNA",
            "nba": "NBA",
            "psom": "PSOM",
            "vcr": "VCR",
            "sri": "SRI",
            "nen-4400": "NEN-4400-1",
            "vcu": "VCU",
        }
        
        for keyword, cert_name in cert_keywords.items():
            if keyword in text_lower and cert_name not in certs:
                certs.append(cert_name)
                self.logger.info(f"✓ Found certification: {cert_name} | Source: {url}")
        
        return certs
    
    # ========================================================================
    # DIGITAL CAPABILITIES - PORTAL DETECTION (Client requirement #3)
    # ========================================================================
    
    def detect_candidate_portal(self, soup: BeautifulSoup, text: str, url: str) -> bool:
        """
        Detect candidate/employee portal.
        
        Client requirement: Look for "login", "inloggen", "mijn..." dashboards.
        """
        text_lower = text.lower()
        
        # Check for login-related text
        if any(keyword in text_lower for keyword in [
            "inloggen",
            "login",
            "mijn account",
            "mijn profiel",
            "mijn dashboard",
            "candidate login",
            "kandidaat login",
            "medewerker login",
            "employee login",
            "mijn werknemersportaal",
        ]):
            self.logger.info(f"✓ Found candidate_portal (text match) | Source: {url}")
            return True
        
        # Check for login links in navigation
        for link in soup.find_all("a", href=True):
            href = link.get("href", "").lower()
            link_text = link.get_text(strip=True).lower()
            
            if any(keyword in href or keyword in link_text for keyword in [
                "login",
                "inloggen",
                "mijn",
                "portal",
                "account",
            ]):
                # Exclude employer/client portals
                if not any(x in href or x in link_text for x in ["werkgever", "employer", "client", "opdrachtgever"]):
                    self.logger.info(f"✓ Found candidate_portal (link: {link_text}) | Source: {url}")
                    return True
        
        return False
    
    def detect_client_portal(self, soup: BeautifulSoup, text: str, url: str) -> bool:
        """
        Detect client/employer portal.
        
        Client requirement: Look for "employer portal", "client portal", "werkgever".
        """
        text_lower = text.lower()
        
        # Check for employer/client-specific text
        if any(keyword in text_lower for keyword in [
            "client portal",
            "employer portal",
            "werkgeversportaal",
            "opdrachtgever portal",
            "mijn werkgevers",
            "werkgever inloggen",
            "employer login",
        ]):
            self.logger.info(f"✓ Found client_portal (text match) | Source: {url}")
            return True
        
        # Check for employer login links
        for link in soup.find_all("a", href=True):
            href = link.get("href", "").lower()
            link_text = link.get_text(strip=True).lower()
            
            if any(keyword in href or keyword in link_text for keyword in [
                "werkgever",
                "employer",
                "client portal",
                "opdrachtgever",
            ]):
                self.logger.info(f"✓ Found client_portal (link: {link_text}) | Source: {url}")
                return True
        
        return False
    
    # ========================================================================
    # ROLE LEVELS (Client requirement #4)
    # ========================================================================
    
    def fetch_role_levels(self, text: str, url: str) -> List[str]:
        """
        Extract role levels from context.
        
        Client requirement: Detect student, starter, medior, senior from indirect mentions.
        Keep it simple: if unsure, return empty list.
        """
        text_lower = text.lower()
        levels = []
        
        # Student detection
        if any(keyword in text_lower for keyword in [
            "student",
            "studenten",
            "bijbaan",
            "bijbaantje",
            "stage",
            "stageplek",
        ]):
            levels.append("student")
            self.logger.info(f"✓ Found role_level: student | Source: {url}")
        
        # Starter/Junior detection
        if any(keyword in text_lower for keyword in [
            "starter",
            "junior",
            "beginning",
            "entree",
            "startende",
        ]):
            levels.append("starter")
            self.logger.info(f"✓ Found role_level: starter | Source: {url}")
        
        # Medior detection
        if any(keyword in text_lower for keyword in [
            "medior",
            "ervaren",
            "experienced",
            "mid-level",
        ]):
            levels.append("medior")
            self.logger.info(f"✓ Found role_level: medior | Source: {url}")
        
        # Senior detection
        if any(keyword in text_lower for keyword in [
            "senior",
            "specialist",
            "expert",
            "lead",
            "principal",
            "architect",
        ]):
            levels.append("senior")
            self.logger.info(f"✓ Found role_level: senior | Source: {url}")
        
        return list(set(levels))  # Remove duplicates
    
    # ========================================================================
    # REVIEW SOURCES (Client requirement #5)
    # ========================================================================
    
    def fetch_review_sources(self, soup: BeautifulSoup, url: str) -> List[Dict[str, str]]:
        """
        Extract review platform names and URLs.
        
        Client requirement: Extract from footers/"Over ons" pages.
        Return list of {"platform": "Google Reviews", "url": "https://..."}.
        """
        review_sources = []
        
        # Review platform patterns
        platforms = {
            "Google Reviews": ["google.com/maps", "google.nl/maps", "reviews", "beoordelingen"],
            "Trustpilot": ["trustpilot.com", "trustpilot.nl"],
            "Indeed": ["indeed.com", "indeed.nl", "indeed reviews"],
            "Glassdoor": ["glassdoor.com", "glassdoor.nl"],
        }
        
        # Check all links in footer and body
        for link in soup.find_all("a", href=True):
            href = link.get("href", "")
            link_text = link.get_text(strip=True).lower()
            
            for platform, keywords in platforms.items():
                # Check if URL matches platform
                if any(keyword in href.lower() for keyword in keywords):
                    # Check if it's a review link (not just homepage)
                    if "reviews" in href.lower() or "beoordelingen" in link_text or "review" in link_text:
                        review_sources.append({
                            "platform": platform,
                            "url": href
                        })
                        self.logger.info(f"✓ Found review source: {platform} | Source: {url}")
                        break
        
        # Remove duplicates (same platform)
        seen_platforms = set()
        unique_sources = []
        for source in review_sources:
            if source["platform"] not in seen_platforms:
                seen_platforms.add(source["platform"])
                unique_sources.append(source)
        
        return unique_sources

