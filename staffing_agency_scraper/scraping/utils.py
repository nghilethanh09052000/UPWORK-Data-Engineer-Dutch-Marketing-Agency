"""
Shared utility functions for all staffing agency scrapers.

This module provides reusable extraction methods using BS4 and regex.
Based on the 69 fields defined in output/_sample.json.
"""

from __future__ import annotations

import re
from typing import Dict, List, Optional, Union

from bs4 import BeautifulSoup

from staffing_agency_scraper.models import (
    AgencyServices,
    CaoType,
    OfficeLocation,
)


# ============================================================================
# REUSABLE CONSTANTS FOR ALL AGENCIES
# ============================================================================

# Portal Detection - Candidate URL Patterns
CANDIDATE_URL_PATTERNS = [
    "myapplication",  # /myapplications/login
    "my-application",
    "mijn-account",
    "mijn-dossier",
    "kandidaat",
    "candidate",
    "medewerker",
    "talent-portal",
]

# Portal Detection - Candidate Text Keywords
CANDIDATE_TEXT_KEYWORDS = [
    "candidate login",
    "kandidaat login",
    "kandidaat inloggen",
    "medewerker login",
    "medewerker inloggen",
    "employee login",
    "mijn account",
    "mijn profiel",
    "mijn dashboard",
    "mijn dossier",
    "mijn werknemersportaal",
    "talent portal",
]

# Portal Detection - Candidate Link Indicators
CANDIDATE_LINK_INDICATORS = [
    "mijn",  # "mijn dossier", "mijn account", etc.
    "my",  # "myapplications", "my account", etc.
    "kandidaat",
    "candidate",
    "medewerker",
    "talent",
]

# Portal Detection - Client Text Keywords
CLIENT_TEXT_KEYWORDS = [
    "client portal",
    "employer portal",
    "werkgeversportaal",
    "opdrachtgever portal",
    "mijn werkgevers",
    "werkgever inloggen",
    "employer login",
]

# Portal Detection - Employer/Client Indicators
EMPLOYER_INDICATORS = [
    "werkgever",
    "employer",
    "client",
    "opdrachtgever",
]

# Portal Detection - Login/Portal Indicators
PORTAL_INDICATORS = [
    "portal",
    "login",
    "inloggen",
    "dashboard",
]

# Role Levels - Detection Keywords
ROLE_LEVEL_KEYWORDS = {
    "student": [
        "student",
        "studenten",
        "bijbaan",
        "bijbaantje",
        "stage",
        "stageplek",
    ],
    "starter": [
        "starter",
        "junior",
        "beginning",
        "entree",
        "startende",
    ],
    "medior": [
        "medior",
        "ervaren",
        "experienced",
        "mid-level",
    ],
    "senior": [
        "senior",
        "specialist",
        "expert",
        "lead",
        "principal",
        "architect",
    ],
}

# Review Platforms - Detection Patterns
REVIEW_PLATFORMS = {
    "Google Reviews": ["google.com/maps", "google.nl/maps", "reviews", "beoordelingen"],
    "Trustpilot": ["trustpilot.com", "trustpilot.nl"],
    "Indeed": ["indeed.com", "indeed.nl", "indeed reviews"],
    "Glassdoor": ["glassdoor.com", "glassdoor.nl"],
}

# Sector Normalization - Sector Keywords Mapping
# Maps normalized sector names to detection keywords
SECTOR_KEYWORDS = {
    "logistiek": ["logistiek", "transport", "warehouse", "magazijn", "supply chain"],
    "horeca": ["horeca", "hospitality", "restaurant", "hotel", "catering"],
    "zorg": ["zorg", "healthcare", "care", "verpleging", "ggz", "thuiszorg"],
    "techniek": ["techniek", "technical", "engineering", "installatie", "montage"],
    "office": ["office", "kantoor", "administratie", "backoffice", "secretarieel"],
    "finance": ["finance", "financieel", "accounting", "boekhouding", "treasury"],
    "marketing": ["marketing", "communicatie", "pr", "sales", "commercieel"],
    "retail": ["retail", "winkel", "verkoop", "winkelier"],
    "industrie": ["industrie", "productie", "manufacturing", "fabriek"],
    "bouw": ["bouw", "construction", "aannemer", "infrastractuur"],
    "it": ["it", "ict", "software", "developer", "data", "cloud", "cyber"],
    "hr": ["hr", "human resources", "recruitment", "p&o"],
    "legal": ["legal", "juridisch", "recht", "advocatuur"],
    "onderwijs": ["onderwijs", "education", "leraar", "docent"],
    "publieke_sector": ["publieke sector", "overheid", "government", "gemeente", "rijk"],
    "automotive": ["automotive", "auto", "voertuigen"],
    "engineering": ["engineering", "ingenieur"],
    "productie": ["productie", "production"],
    "schoonmaak": ["schoonmaak", "cleaning", "facility"],
    "beveiliging": ["beveiliging", "security"],
    "callcenter": ["callcenter", "klantenservice", "customer service"],
    "energie": ["energie", "energy", "utilities"],
    "chemie": ["chemie", "chemical"],
    "pharma": ["pharma", "pharmaceutical", "geneesmiddelen"],
    "food": ["food", "voedsel", "agrifood"],
    "agri": ["agri", "landbouw", "agriculture"],
    "telecom": ["telecom", "telecommunicatie"],
    "media": ["media", "broadcasting", "publishing"],
    "consulting": ["consulting", "advisory", "advies"],
    "non_profit": ["non profit", "non-profit", "nonprofit", "ngo", "charity"],
    "high_tech": ["high tech", "high-tech", "hightech"],
    "life_science": ["life science", "life sciences", "biotech"],
    "bouw_infra": ["bouw & infra", "infrastructuur"],
    "it_telecom": ["it & telecom", "ict"],
}

# List of normalized sectors (for reference)
NORMALIZED_SECTORS = list(SECTOR_KEYWORDS.keys())

# Growth Signals - Detection Keywords
GROWTH_SIGNAL_KEYWORDS = {
    "landelijke_dekking": [
        "landelijk", "landelijke dekking", "heel nederland",
        "national coverage", "nationwide"
    ],
    "internationale_groep": [
        "internationale groep", "international group",
        "wereldwijd", "worldwide", "global presence",
        "landen", "countries",
    ],
    "beursgenoteerd": [
        "beursgenoteerd", "nyse", "euronext", "beurs", "stock exchange",
        "listed", "public company", "ticker"
    ],
    "overnames": [
        "overname", "acquisitie", "acquisition", "overgenomen",
        "acquired", "fusie", "merger"
    ],
    "awards": [
        "award", "prijs", "winnaar", "winner", "erkend",
        "recognised", "certified", "gold", "platinum"
    ],
}

# Company Size Fit - Detection Keywords
COMPANY_SIZE_FIT_KEYWORDS = {
    "micro_1_10": [
        "zzp", "freelance", "eenmanszaak", "startups", "micro bedrijf", 
        "mkb", "kleine bedrijven", "zelfstandig", "1-10 medewerkers"
    ],
    "smb_11_200": [
        "mkb", "midden", "klein", "middelgroot bedrijf", "11-200 medewerkers",
        "familiebedrijf", "sme", "small medium"
    ],
    "mid_market_201_1000": [
        "middelgroot", "mid market", "201-1000", "groeiende organisaties",
        "mid-size", "scale-up"
    ],
    "enterprise_1000_plus": [
        "grootbedrijf", "enterprise", "multinational", "corporate", 
        "1000+ medewerkers", "internationale organisaties", "groot", "Fortune"
    ],
    "public_sector": [
        "overheid", "publieke sector", "gemeente", "provincie", "rijk",
        "ministerie", "publiek", "government", "public sector", "zorg",
        "onderwijs", "gemeenten"
    ],
}

# Customer Segments - Detection Keywords
CUSTOMER_SEGMENTS_KEYWORDS = {
    "MKB": ["mkb", "midden- en kleinbedrijf", "kleine bedrijven", "middelgroot"],
    "grootbedrijf": ["grootbedrijf", "groot bedrijf", "grote bedrijven", "enterprise"],
    "overheid": ["overheid", "publieke sector", "gemeente", "provincie", "rijk"],
    "zorginstelling": ["zorginstelling", "zorg", "ziekenhuis", "verpleeghuis", "ggz"],
    "onderwijsinstelling": ["onderwijs", "school", "universiteit", "hogeschool", "mbo"],
}

# Focus Segments - Detection Keywords
FOCUS_SEGMENTS_KEYWORDS = {
    "studenten": ["student", "studenten", "bijbaan", "studiebaan"],
    "young_professionals": ["young professional", "starter", "recent graduate", "hbo", "wo"],
    "blue_collar": ["logistiek", "productie", "bouw", "technisch", "magazijn", "chauffeur"],
    "white_collar": ["kantoor", "administratie", "finance", "hr", "sales", "marketing"],
    "technisch_specialisten": ["specialist", "engineer", "technisch", "ict", "software"],
    "zorgprofessionals": ["verpleegkundige", "arts", "zorgmedewerker", "zorg"],
}

# Shift Types - Detection Keywords
SHIFT_TYPES_KEYWORDS = {
    "dagdienst": ["dagdienst", "overdag", "kantooruren"],
    "avonddienst": ["avonddienst", "avond"],
    "nachtdienst": ["nachtdienst", "nacht"],
    "weekend": ["weekend", "zaterd", "zond"],
    "24_7_bereikbaar": ["24/7", "dag en nacht", "altijd bereikbaar", "24 uur"],
}

# Typical Use Cases - Detection Keywords
TYPICAL_USE_CASES_KEYWORDS = {
    "piekdruk_opvangen": ["piekdruk", "drukke periode", "piekmomenten", "seizoen"],
    "langdurige_detachering": ["langdurig", "vast", "permanent", "structureel"],
    "projecten": ["project", "projectbasis", "tijdelijk project"],
    "seizoenswerk": ["seizoen", "seizoenswerk", "zomer", "kerst"],
    "weekenddiensten": ["weekend", "zaterd", "zond"],
    "24_7_bezetting": ["24/7", "continu", "dag en nacht", "altijd bezet"],
}

# Speed Claims - Detection Keywords
SPEED_CLAIMS_KEYWORDS = {
    "binnen_24_uur_kandidaten": ["24 uur", "binnen een dag", "morgen"],
    "snel_schakelen": ["snel", "direct", "vandaag nog", "meteen"],
    "grote_pools_direct_beschikbaar": ["directe beschikbaarheid", "groot bestand", "pool", "database"],
}

# Pricing Model - Detection Keywords
PRICING_MODEL_KEYWORDS = {
    "omrekenfactor": ["omrekenfactor", "multiplicator", "markup"],
    "fixed_margin": ["vaste marge", "fixed margin", "percentage"],
    "fixed_fee": ["vast tarief", "fixed fee", "all-in"],
}

# No Cure No Pay - Detection Keywords
NO_CURE_NO_PAY_KEYWORDS = [
    "no cure no pay", "geen resultaat geen betaling", "no cure, no pay",
    "resultaat geen kosten", "risk free", "gratis"
]

# City to Province Mapping - Dutch Cities
CITY_TO_PROVINCE = {
    # Noord-Holland
    "amsterdam": "Noord-Holland", "haarlem": "Noord-Holland", "zaandam": "Noord-Holland",
    "alkmaar": "Noord-Holland", "hoorn": "Noord-Holland", "hoofddorp": "Noord-Holland",
    "purmerend": "Noord-Holland", "beverwijk": "Noord-Holland", "hilversum": "Noord-Holland",
    "amstelveen": "Noord-Holland", "heerhugowaard": "Noord-Holland", "velsen": "Noord-Holland",
    # Zuid-Holland
    "rotterdam": "Zuid-Holland", "den haag": "Zuid-Holland", "the hague": "Zuid-Holland",
    "'s-gravenhage": "Zuid-Holland", "s-gravenhage": "Zuid-Holland",
    "leiden": "Zuid-Holland", "dordrecht": "Zuid-Holland", "zoetermeer": "Zuid-Holland",
    "delft": "Zuid-Holland", "alphen aan den rijn": "Zuid-Holland", "gouda": "Zuid-Holland",
    "schiedam": "Zuid-Holland", "spijkenisse": "Zuid-Holland", "vlaardingen": "Zuid-Holland",
    "gorinchem": "Zuid-Holland", "capelle aan den ijssel": "Zuid-Holland", "maassluis": "Zuid-Holland",
    "nieuwegein": "Zuid-Holland", "oud-beijerland": "Zuid-Holland",
    # Utrecht
    "utrecht": "Utrecht", "amersfoort": "Utrecht", "veenendaal": "Utrecht",
    "zeist": "Utrecht", "nieuwegein": "Utrecht", "woerden": "Utrecht",
    # Noord-Brabant
    "eindhoven": "Noord-Brabant", "tilburg": "Noord-Brabant", "breda": "Noord-Brabant",
    "'s-hertogenbosch": "Noord-Brabant", "den bosch": "Noord-Brabant", "helmond": "Noord-Brabant",
    "oss": "Noord-Brabant", "roosendaal": "Noord-Brabant", "bergen op zoom": "Noord-Brabant",
    "uden": "Noord-Brabant", "veghel": "Noord-Brabant", "veldhoven": "Noord-Brabant",
    "waalwijk": "Noord-Brabant", "oosterhout": "Noord-Brabant", "schijndel": "Noord-Brabant",
    # Gelderland
    "nijmegen": "Gelderland", "arnhem": "Gelderland", "apeldoorn": "Gelderland",
    "ede": "Gelderland", "zutphen": "Gelderland", "tiel": "Gelderland",
    "harderwijk": "Gelderland", "zaltbommel": "Gelderland", "zevenaar": "Gelderland",
    "lichtenvoorde": "Gelderland", "doetinchem": "Gelderland", "winterswijk": "Gelderland",
    "didam": "Gelderland", "wageningen": "Gelderland", "barneveld": "Gelderland",
    # Limburg
    "maastricht": "Limburg", "venlo": "Limburg", "roermond": "Limburg",
    "heerlen": "Limburg", "sittard": "Limburg", "sittard-geleen": "Limburg",
    # Overijssel
    "enschede": "Overijssel", "zwolle": "Overijssel", "almelo": "Overijssel",
    "deventer": "Overijssel", "hengelo": "Overijssel", "kampen": "Overijssel",
    # Groningen
    "groningen": "Groningen", "veendam": "Groningen", "hoogezand": "Groningen",
    # Friesland
    "leeuwarden": "Friesland", "drachten": "Friesland", "heerenveen": "Friesland",
    "sneek": "Friesland", "franeker": "Friesland",
    # Flevoland
    "almere": "Flevoland", "lelystad": "Flevoland", "emmeloord": "Flevoland",
    # Zeeland
    "middelburg": "Zeeland", "vlissingen": "Zeeland", "goes": "Zeeland", "terneuzen": "Zeeland",
    # Drenthe
    "emmen": "Drenthe", "assen": "Drenthe", "hoogeveen": "Drenthe", "meppel": "Drenthe",
}


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
    
    def _matches_keyword(self, keyword: str, text: str) -> bool:
        """
        Check if a keyword matches in text using word boundaries.
        
        This prevents false positives like:
        - "senior" matching "seniorim"
        - "expert" matching "expertise"
        - "abu" matching "abusive"
        
        Args:
            keyword: The keyword to search for (will be escaped)
            text: The text to search in (should be lowercase)
        
        Returns:
            True if keyword found as a whole word, False otherwise
        """
        pattern = r'\b' + re.escape(keyword) + r'\b'
        return bool(re.search(pattern, text))
    
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
        """
        Extract KvK (Chamber of Commerce) number.
        
        Supports various formats found in privacy policies, terms, and legal pages:
        - KvK nummer: 12345678
        - KvK-nummer: 12345678
        - K.v.K.: 12345678
        - Handelsregister nummer: 12345678
        - Ingeschreven onder nummer: 12345678
        - Chamber of Commerce: 12345678
        """
        # Common Dutch KvK patterns
        patterns = [
            # Standard formats with keyword
            r'(?:KvK|K\.?v\.?K\.?|kvk)[\s\-:]*(?:nummer)?[\s\-:]*(\d{8})',
            r'(?:Handelsregister|handelsregister)[\s\-:]*(?:nummer)?[\s\-:]*(\d{8})',
            r'(?:ingeschreven|registered)[\s\w]*(?:onder|with)[\s\w]*(?:nummer|number)[\s\-:]*(\d{8})',
            r'(?:chamber of commerce|kamer van koophandel)[\s\-:]*(?:number|nummer)?[\s\-:]*(\d{8})',
            r'(?:trade register|handelsregister)[\s\-:]*(?:number|nummer)?[\s\-:]*(\d{8})',
            # Registration number (as one word or two words) - common in terms/privacy pages
            r'(?:registratie[\s\-]?nummer|registration[\s\-]?number)[\s\-:]*(\d{8})',
            # Format with dots (e.g., 12.34.56.78)
            r'(?:KvK|K\.?v\.?K\.?|kvk)[\s\-:]*(?:nummer)?[\s\-:]*(\d{2}[\.\s]?\d{2}[\.\s]?\d{2}[\.\s]?\d{2})',
            # Standalone 8-digit number after specific context
            r'(?:geregistreerd|registered)[\s\w,]*(?:B\.?V\.?|N\.?V\.?)[\s\w,]*(?:onder|with)[\s\w]*(\d{8})',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                kvk = match.group(1)
                # Remove dots and spaces if present (e.g., 12.34.56.78 → 12345678)
                kvk_clean = re.sub(r'[\.\s]', '', kvk)
                # Verify it's exactly 8 digits
                if len(kvk_clean) == 8 and kvk_clean.isdigit():
                    self.logger.info(f"✓ Found KvK: {kvk_clean} | Source: {url}")
                    return kvk_clean
        
        return None
    
    def fetch_legal_name(self, text: str, agency_name: str, url: str) -> Optional[str]:
        """
        Extract legal name (e.g., 'Hays B.V.', 'Brunel International N.V.').
        
        Supports various Dutch/international legal entity formats found in:
        - Privacy policies
        - Terms and conditions
        - About pages
        - Footer sections
        """
        # Escape agency_name for regex (in case it contains special chars)
        escaped_name = re.escape(agency_name)
        
        # Pattern for Dutch legal entities with various formats
        patterns = [
            # Standard B.V. formats
            rf"({escaped_name}\s+(?:\w+\s+)?B\.?V\.?)",
            rf"({escaped_name}\s+(?:Nederland|Netherlands|International|Global|Group)?\s*B\.?V\.?)",
            # N.V. formats (public companies)
            rf"({escaped_name}\s+(?:\w+\s+)?N\.?V\.?)",
            rf"({escaped_name}\s+(?:Nederland|Netherlands|International|Global|Group)?\s*N\.?V\.?)",
            # Other formats
            rf"({escaped_name}\s+(?:plc|PLC|Ltd|Limited|GmbH|AG))",
            # With location prefix (e.g., "Hays Nederland B.V.")
            rf"({escaped_name}\s+(?:Nederland|Netherlands)\s+B\.?V\.?)",
            # Relaxed pattern for any company suffix after agency name
            rf"({escaped_name}[\s\w]*?(?:B\.?V\.?|N\.?V\.?|plc|PLC))",
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                legal_name = match.group(1).strip()
                # Clean up extra whitespace
                legal_name = re.sub(r'\s+', ' ', legal_name)
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
        
        # Use the global SECTOR_KEYWORDS mapping
        for sector, keywords in SECTOR_KEYWORDS.items():
            for keyword in keywords:
                if self._matches_keyword(keyword, text_lower) and sector not in sectors:
                    sectors.append(sector)
                    self.logger.info(f"✓ Found sector: {sector} | Source: {url}")
                    break
        
        return sectors
    
    # ========================================================================
    # SERVICES (Field 24 from _sample.json)
    # ========================================================================
    
    def fetch_services(self, text: str, url: str) -> AgencyServices:
        """
        Extract services from text.
        
        Service mapping:
        - uitzenden: Temporary staffing (blue-collar, operational roles)
        - detacheren: Interim/secondment (specialists, professionals working at client sites)
        - werving_selectie: Recruitment & selection (permanent placements)
        - payrolling: Payroll services
        - zzp_bemiddeling: Freelance/ZZP intermediation
        - msp: Managed Service Provider
        - rpo: Recruitment Process Outsourcing
        - executive_search: Executive search / headhunting
        """
        text_lower = text.lower()
        
        services = AgencyServices()
        
        # Uitzenden (temporary staffing)
        if "uitzenden" in text_lower:
            services.uitzenden = True
            self.logger.info(f"✓ Found service: uitzenden | Source: {url}")
        
        # Detacheren / Interim (secondment, interim professionals)
        # Note: "interim" often refers to interim management/specialists
        if any(keyword in text_lower for keyword in [
            "detacheren", "detachering", 
            "interim",  # Interim management, interim professionals
            "secondment"
        ]):
            services.detacheren = True
            self.logger.info(f"✓ Found service: detacheren (interim/secondment) | Source: {url}")
        
        # Werving & Selectie (recruitment & selection)
        if "werving" in text_lower or "selectie" in text_lower:
            services.werving_selectie = True
            self.logger.info(f"✓ Found service: werving_selectie | Source: {url}")
        
        # Payrolling
        if "payroll" in text_lower:
            services.payrolling = True
            self.logger.info(f"✓ Found service: payrolling | Source: {url}")
        
        # ZZP / Freelance intermediation
        if "zzp" in text_lower or "freelance" in text_lower:
            services.zzp_bemiddeling = True
            self.logger.info(f"✓ Found service: zzp_bemiddeling | Source: {url}")
        
        # MSP (Managed Service Provider)
        if "msp" in text_lower or "managed service" in text_lower:
            services.msp = True
            self.logger.info(f"✓ Found service: msp | Source: {url}")
        
        # RPO (Recruitment Process Outsourcing)
        if "rpo" in text_lower or "recruitment process outsourcing" in text_lower:
            services.rpo = True
            self.logger.info(f"✓ Found service: rpo | Source: {url}")
        
        # Executive Search
        if "executive search" in text_lower:
            services.executive_search = True
            self.logger.info(f"✓ Found service: executive_search | Source: {url}")
        
        return services
    
    # ========================================================================
    # CAO & LEGAL (Fields 28-35 from _sample.json)
    # ========================================================================
    
    def fetch_cao_type(self, text: str | Dict[str, str], url: str = "accumulated_text") -> CaoType:
        """
        Extract CAO type.
        
        Args:
            text: Either a string of text or a dict mapping URLs to text
            url: URL to log (only used if text is a string)
        """
        # Support both string and dict (URL mapping)
        if isinstance(text, dict):
            # Search through each page separately for better logging
            for page_url, page_text in text.items():
                text_lower = page_text.lower()
                if self._matches_keyword("abu", text_lower):
                    self.logger.info(f"✓ Found CAO: ABU | Source: {page_url}")
                    return CaoType.ABU
                elif self._matches_keyword("nbbu", text_lower):
                    self.logger.info(f"✓ Found CAO: NBBU | Source: {page_url}")
                    return CaoType.NBBU
        else:
            # Old API: single string
            text_lower = text.lower()
            if self._matches_keyword("abu", text_lower):
                self.logger.info(f"✓ Found CAO: ABU | Source: {url}")
                return CaoType.ABU
            elif self._matches_keyword("nbbu", text_lower):
                self.logger.info(f"✓ Found CAO: NBBU | Source: {url}")
                return CaoType.NBBU
        
        return CaoType.ONBEKEND
    
    def fetch_membership(self, text: str | Dict[str, str], url: str = "accumulated_text") -> List[str]:
        """
        Extract membership organizations.
        
        Args:
            text: Either a string of text or a dict mapping URLs to text
            url: URL to log (only used if text is a string)
        """
        membership = []
        
        # Support both string and dict (URL mapping)
        if isinstance(text, dict):
            # Search through each page separately for better logging
            for page_url, page_text in text.items():
                text_lower = page_text.lower()
                if self._matches_keyword("abu", text_lower) and "ABU" not in membership:
                    membership.append("ABU")
                    self.logger.info(f"✓ Found membership: ABU | Source: {page_url}")
                if self._matches_keyword("nbbu", text_lower) and "NBBU" not in membership:
                    membership.append("NBBU")
                    self.logger.info(f"✓ Found membership: NBBU | Source: {page_url}")
                if self._matches_keyword("nrto", text_lower) and "NRTO" not in membership:
                    membership.append("NRTO")
                    self.logger.info(f"✓ Found membership: NRTO | Source: {page_url}")
        else:
            # Old API: single string
            text_lower = text.lower()
            if self._matches_keyword("abu", text_lower):
                membership.append("ABU")
                self.logger.info(f"✓ Found membership: ABU | Source: {url}")
            if self._matches_keyword("nbbu", text_lower):
                membership.append("NBBU")
                self.logger.info(f"✓ Found membership: NBBU | Source: {url}")
            if self._matches_keyword("nrto", text_lower):
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
    
    def fetch_certifications(self, text: str | Dict[str, str], url: str = "accumulated_text") -> List[str]:
        """
        Extract certifications.
        
        Args:
            text: Either a string of text or a dict mapping URLs to text
            url: URL to log (only used if text is a string)
        """
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
        
        # Support both string and dict (URL mapping)
        if isinstance(text, dict):
            # Search through each page separately for better logging
            for page_url, page_text in text.items():
                text_lower = page_text.lower()
                for keyword, cert_name in cert_keywords.items():
                    if self._matches_keyword(keyword, text_lower) and cert_name not in certs:
                        certs.append(cert_name)
                        self.logger.info(f"✓ Found certification: {cert_name} | Source: {page_url}")
        else:
            # Old API: single string
            text_lower = text.lower()
            for keyword, cert_name in cert_keywords.items():
                if self._matches_keyword(keyword, text_lower) and cert_name not in certs:
                    certs.append(cert_name)
                    self.logger.info(f"✓ Found certification: {cert_name} | Source: {url}")
        
        return certs
    
    # ========================================================================
    # DIGITAL CAPABILITIES - PORTAL DETECTION (Client requirement #3)
    # ========================================================================
    
    def detect_candidate_portal(self, soup: BeautifulSoup, text: str, url: str) -> bool:
        """
        Detect candidate/employee portal.
        
        Client requirement: Look for specific candidate login indicators.
        Note: Generic "login" is too vague - we need specific evidence.
        """
        text_lower = text.lower()
        url_lower = url.lower()
        
        # FIRST: Check the current URL itself for candidate indicators
        # (Important: When we're ON the login page, there's no link TO it!)
        for pattern in CANDIDATE_URL_PATTERNS:
            if pattern in url_lower:
                # Make sure it's not an employer portal
                if not any(x in url_lower for x in EMPLOYER_INDICATORS):
                    self.logger.info(f"✓ Found candidate_portal (URL pattern: {pattern} in {url}) | Source: {url}")
                    return True
        
        # Check for candidate-specific portal keywords
        if any(keyword in text_lower for keyword in CANDIDATE_TEXT_KEYWORDS):
            self.logger.info(f"✓ Found candidate_portal (text match) | Source: {url}")
            return True
        
        # Check for candidate-specific login links
        # Look for "mijn"/"my" patterns in URLs and link text
        for link in soup.find_all("a", href=True):
            href = link.get("href", "").lower()
            link_text = link.get_text(strip=True).lower()
            
            if any(indicator in href or indicator in link_text for indicator in CANDIDATE_LINK_INDICATORS):
                # Exclude employer/client portals
                if not any(x in href or x in link_text for x in EMPLOYER_INDICATORS):
                    self.logger.info(f"✓ Found candidate_portal (link: {link_text} → {link.get('href', '')}) | Source: {url}")
                    return True
        
        return False
    
    def detect_client_portal(self, soup: BeautifulSoup, text: str, url: str) -> bool:
        """
        Detect client/employer portal.
        
        Client requirement: Look for "employer portal", "client portal", "werkgever".
        """
        text_lower = text.lower()
        
        # Check for employer/client-specific text
        if any(keyword in text_lower for keyword in CLIENT_TEXT_KEYWORDS):
            self.logger.info(f"✓ Found client_portal (text match) | Source: {url}")
            return True
        
        # Check for employer login links
        # Note: Must have BOTH employer indicator AND login/portal indicator!
        for link in soup.find_all("a", href=True):
            href = link.get("href", "").lower()
            link_text = link.get_text(strip=True).lower()
            
            # Employer/client indicators
            has_employer = any(keyword in href or keyword in link_text for keyword in EMPLOYER_INDICATORS)
            
            # Login/portal indicators
            has_portal = any(keyword in href or keyword in link_text for keyword in PORTAL_INDICATORS)
            
            # Only detect if BOTH are present
            # (Avoids false positives from "Voor Opdrachtgevers" pages)
            if has_employer and has_portal:
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
        
        Note: Uses word boundaries to avoid false positives (e.g., "expertise" matching "expert").
        """
        text_lower = text.lower()
        levels = []
        
        # Check each role level using the constants with word boundaries
        for level, keywords in ROLE_LEVEL_KEYWORDS.items():
            for keyword in keywords:
                # Use word boundary regex to match whole words only
                # This prevents "expertise" from matching "expert", etc.
                pattern = r'\b' + re.escape(keyword) + r'\b'
                if re.search(pattern, text_lower):
                    if level not in levels:
                        levels.append(level)
                        self.logger.info(f"✓ Found role_level: {level} | Source: {url}")
                    break  # Move to next level once found
        
        return levels
    
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
        
        # Check all links in footer and body
        for link in soup.find_all("a", href=True):
            href = link.get("href", "")
            link_text = link.get_text(strip=True).lower()
            
            for platform, keywords in REVIEW_PLATFORMS.items():
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
    
    # ========================================================================
    # GROWTH SIGNALS
    # ========================================================================
    
    def fetch_growth_signals(self, text: str, url: str) -> List[str]:
        """
        Extract growth signals from text (history, about pages).
        
        Detects factual claims like:
        - National/regional coverage
        - Years active (founding date)
        - Part of international group
        - Listed on stock exchange
        - Number of offices/locations
        - Acquisitions/mergers
        - International presence
        """
        text_lower = text.lower()
        signals = []
        
        # National coverage
        if any(self._matches_keyword(keyword, text_lower) for keyword in GROWTH_SIGNAL_KEYWORDS["landelijke_dekking"]):
            signals.append("landelijke_dekking")
            self.logger.info(f"✓ Found growth signal: landelijke_dekking | Source: {url}")
        
        # International presence
        if any(self._matches_keyword(keyword, text_lower) for keyword in GROWTH_SIGNAL_KEYWORDS["internationale_groep"]):
            signals.append("onderdeel_van_internationale_groep")
            self.logger.info(f"✓ Found growth signal: onderdeel_van_internationale_groep | Source: {url}")
        
        # Years active - look for founding years (1900-2024)
        founding_match = re.search(r'\b(19\d{2}|20[0-2]\d)\b', text)
        if founding_match:
            year = founding_match.group(1)
            # Only consider as founding year if mentioned with context like "sinds", "opgericht", "founded"
            if any(keyword in text_lower for keyword in [
                f"sinds {year}", f"in {year}", f"opgericht {year}",
                f"founded {year}", f"established {year}"
            ]):
                signals.append(f"sinds_{year}_actief")
                self.logger.info(f"✓ Found growth signal: sinds_{year}_actief | Source: {url}")
        
        # Stock exchange listing
        if any(self._matches_keyword(keyword, text_lower) for keyword in GROWTH_SIGNAL_KEYWORDS["beursgenoteerd"]):
            signals.append("beursgenoteerd")
            self.logger.info(f"✓ Found growth signal: beursgenoteerd | Source: {url}")
        
        # Large office network
        office_match = re.search(r'(\d+)\s*(?:kantoren|kantoor|offices|office)', text_lower)
        if office_match:
            count = int(office_match.group(1))
            if count >= 10:
                signals.append(f"{count}_plus_kantoren")
                self.logger.info(f"✓ Found growth signal: {count}_plus_kantoren | Source: {url}")
        
        # Acquisitions
        if any(self._matches_keyword(keyword, text_lower) for keyword in GROWTH_SIGNAL_KEYWORDS["overnames"]):
            signals.append("overnames_gedaan")
            self.logger.info(f"✓ Found growth signal: overnames_gedaan | Source: {url}")
        
        # International offices
        country_match = re.search(r'(\d+)\s*(?:landen|countries)', text_lower)
        if country_match:
            count = int(country_match.group(1))
            if count >= 5:
                signals.append(f"actief_in_{count}_landen")
                self.logger.info(f"✓ Found growth signal: actief_in_{count}_landen | Source: {url}")
        
        # Awards and certifications (growth indicator)
        if any(self._matches_keyword(keyword, text_lower) for keyword in GROWTH_SIGNAL_KEYWORDS["awards"]):
            signals.append("awards_ontvangen")
            self.logger.info(f"✓ Found growth signal: awards_ontvangen | Source: {url}")
        
        # Remove duplicates while preserving order
        seen = set()
        unique_signals = []
        for signal in signals:
            if signal not in seen:
                seen.add(signal)
                unique_signals.append(signal)
        
        return unique_signals
    
    def fetch_company_size_fit(self, text: str, url: str) -> List[str]:
        """
        Extract company size fit categories from text.
        
        Returns list of company size categories:
        - micro_1_10
        - smb_11_200
        - mid_market_201_1000
        - enterprise_1000_plus
        - public_sector
        """
        text_lower = text.lower()
        size_fits = []
        
        for size_category, keywords in COMPANY_SIZE_FIT_KEYWORDS.items():
            if any(self._matches_keyword(keyword, text_lower) for keyword in keywords):
                size_fits.append(size_category)
                self.logger.info(f"✓ Found company size fit: {size_category} | Source: {url}")
        
        return list(set(size_fits))  # Remove duplicates
    
    def fetch_customer_segments(self, text: str, url: str) -> List[str]:
        """
        Extract customer segment categories from text.
        
        Returns list of customer segments:
        - MKB
        - grootbedrijf
        - overheid
        - zorginstelling
        - onderwijsinstelling
        """
        text_lower = text.lower()
        segments = []
        
        for segment, keywords in CUSTOMER_SEGMENTS_KEYWORDS.items():
            if any(self._matches_keyword(keyword, text_lower) for keyword in keywords):
                segments.append(segment)
                self.logger.info(f"✓ Found customer segment: {segment} | Source: {url}")
        
        return list(set(segments))  # Remove duplicates
    
    def fetch_focus_segments(self, text: str, url: str) -> List[str]:
        """
        Extract focus segment categories from text.
        
        Returns list of focus segments:
        - studenten
        - young_professionals
        - blue_collar
        - white_collar
        - technisch_specialisten
        - zorgprofessionals
        """
        text_lower = text.lower()
        segments = []
        
        for segment, keywords in FOCUS_SEGMENTS_KEYWORDS.items():
            if any(self._matches_keyword(keyword, text_lower) for keyword in keywords):
                segments.append(segment)
                self.logger.info(f"✓ Found focus segment: {segment} | Source: {url}")
                self.logger.info(f'Text: {text_lower}')
        
        return list(set(segments))  # Remove duplicates
    
    def fetch_shift_types_supported(self, text: str, url: str) -> List[str]:
        """
        Extract shift types supported from text.
        
        Returns list of shift types:
        - dagdienst
        - avonddienst
        - nachtdienst
        - weekend
        - 24_7_bereikbaar
        """
        text_lower = text.lower()
        shift_types = []
        
        for shift_type, keywords in SHIFT_TYPES_KEYWORDS.items():
            if any(self._matches_keyword(keyword, text_lower) for keyword in keywords):
                shift_types.append(shift_type)
                self.logger.info(f"✓ Found shift type: {shift_type} | Source: {url}")
        
        return list(set(shift_types))  # Remove duplicates
    
    def fetch_typical_use_cases(self, text: str, url: str) -> List[str]:
        """
        Extract typical use cases from text.
        
        Returns list of use cases:
        - piekdruk_opvangen
        - langdurige_detachering
        - projecten
        - seizoenswerk
        - weekenddiensten
        - 24_7_bezetting
        """
        text_lower = text.lower()
        use_cases = []
        
        for use_case, keywords in TYPICAL_USE_CASES_KEYWORDS.items():
            if any(self._matches_keyword(keyword, text_lower) for keyword in keywords):
                use_cases.append(use_case)
                self.logger.info(f"✓ Found typical use case: {use_case} | Source: {url}")
        
        return list(set(use_cases))  # Remove duplicates
    
    def fetch_speed_claims(self, text: str, url: str) -> List[str]:
        """
        Extract speed claims from text.
        
        Returns list of speed claims:
        - binnen_24_uur_kandidaten
        - snel_schakelen
        - grote_pools_direct_beschikbaar
        """
        text_lower = text.lower()
        speed_claims = []
        
        for claim, keywords in SPEED_CLAIMS_KEYWORDS.items():
            if any(self._matches_keyword(keyword, text_lower) for keyword in keywords):
                speed_claims.append(claim)
                self.logger.info(f"✓ Found speed claim: {claim} | Source: {url}")
        
        return list(set(speed_claims))  # Remove duplicates
    
    def fetch_volume_specialisation(self, text: str, url: str) -> str:
        """
        Infer volume specialisation from text.
        
        Returns one of:
        - ad_hoc_1_5: Ad-hoc placements (1-5 people)
        - pools_5_50: Pool management (5-50 people)
        - massa_50_plus: Mass recruitment (50+ people)
        - unknown
        """
        text_lower = text.lower()
        
        # Check for mass recruitment indicators
        if any(keyword in text_lower for keyword in [
            "massa", "hoog volume", "100+", "grote aantallen", "bulk",
            "seizoen", "piek", "grootschalig"
        ]):
            self.logger.info(f"✓ Found volume specialisation: massa_50_plus | Source: {url}")
            return "massa_50_plus"
        
        # Check for pool management indicators
        if any(keyword in text_lower for keyword in [
            "pool", "flexpool", "bestand", "database", "50 kandidaten",
            "talent pool", "candidate pool"
        ]):
            self.logger.info(f"✓ Found volume specialisation: pools_5_50 | Source: {url}")
            return "pools_5_50"
        
        # Check for ad-hoc/small scale indicators
        if any(keyword in text_lower for keyword in [
            "maatwerk", "bespoke", "specialist", "exact match", "1-op-1",
            "individueel", "niche"
        ]):
            self.logger.info(f"✓ Found volume specialisation: ad_hoc_1_5 | Source: {url}")
            return "ad_hoc_1_5"
        
        return "unknown"
    
    def fetch_pricing_model(self, text: str, url: str) -> str:
        """
        Extract pricing model from text.
        
        Returns one of:
        - omrekenfactor
        - fixed_margin
        - fixed_fee
        - unknown
        """
        text_lower = text.lower()
        
        for model, keywords in PRICING_MODEL_KEYWORDS.items():
            if any(keyword in text_lower for keyword in keywords):
                self.logger.info(f"✓ Found pricing model: {model} | Source: {url}")
                return model
        
        return "unknown"
    
    def fetch_pricing_transparency(self, text: str, url: str) -> Optional[str]:
        """
        Extract pricing transparency level from text.
        
        Returns one of:
        - public_examples: Public pricing examples available
        - explainer_only: Explanation of pricing model without examples
        - quote_only: Contact for quote only
        - None: No pricing information found
        """
        text_lower = text.lower()
        
        # Check for public pricing examples (tariff tables, rate cards, etc.)
        if any(keyword in text_lower for keyword in [
            "tarief", "uurtarief", "voorbeeld", "vanaf €", "€ per uur",
            "rate card", "pricing example", "kosten per", "tarievenlijst"
        ]):
            self.logger.info(f"✓ Found pricing transparency: public_examples | Source: {url}")
            return "public_examples"
        
        # Check for pricing model explanation
        if any(keyword in text_lower for keyword in [
            "omrekenfactor", "pricing model", "kostenmodel", "hoe werkt",
            "tariefstructuur", "prijsopbouw"
        ]):
            self.logger.info(f"✓ Found pricing transparency: explainer_only | Source: {url}")
            return "explainer_only"
        
        # Check for quote-only approach
        if any(keyword in text_lower for keyword in [
            "offerte", "vrijblijvend gesprek", "neem contact op",
            "request quote", "aanvragen", "maatwerk"
        ]):
            self.logger.info(f"✓ Found pricing transparency: quote_only | Source: {url}")
            return "quote_only"
        
        return None
    
    def fetch_no_cure_no_pay(self, text: str, url: str) -> Optional[bool]:
        """
        Check if agency offers no cure no pay recruitment.
        
        Returns:
        - True if mentioned
        - False if explicitly stated they don't offer it
        - None if not mentioned
        """
        text_lower = text.lower()
        
        if any(keyword in text_lower for keyword in NO_CURE_NO_PAY_KEYWORDS):
            self.logger.info(f"✓ Found no cure no pay: True | Source: {url}")
            return True
        
        return None
    
    def fetch_omrekenfactor(self, text: str, url: str) -> tuple[Optional[float], Optional[float]]:
        """
        Extract omrekenfactor range from text.
        
        Returns:
        - (min, max) tuple of floats
        - (None, None) if not found
        """
        # Pattern: omrekenfactor 1.45, omrekenfactor vanaf 1.4, 1.35-1.55, etc.
        patterns = [
            r'omrekenfactor\s+(?:van\s+)?(\d+[.,]\d+)(?:\s*(?:-|tot)\s*(\d+[.,]\d+))?',
            r'multiplicator\s+(?:van\s+)?(\d+[.,]\d+)(?:\s*(?:-|tot)\s*(\d+[.,]\d+))?',
            r'markup\s+(?:van\s+)?(\d+[.,]\d+)(?:\s*(?:-|tot)\s*(\d+[.,]\d+))?',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text.lower())
            if match:
                min_val = float(match.group(1).replace(',', '.'))
                max_val = float(match.group(2).replace(',', '.')) if match.group(2) else None
                
                self.logger.info(f"✓ Found omrekenfactor: {min_val} - {max_val} | Source: {url}")
                return (min_val, max_val)
        
        return (None, None)
    
    def fetch_avg_time_to_fill(self, text: str, url: str) -> Optional[int]:
        """
        Extract average time to fill in days from speed claims.
        
        Returns:
        - Number of days
        - None if not mentioned
        """
        text_lower = text.lower()
        
        # Pattern: "binnen 24 uur", "binnen 2 dagen", "binnen een week"
        patterns = [
            (r'binnen\s+(\d+)\s+uur', lambda h: max(1, int(h) // 24)),  # hours to days
            (r'binnen\s+(\d+)\s+dag', lambda d: int(d)),  # days
            (r'binnen\s+een\s+dag', lambda: 1),  # "binnen een dag"
            (r'binnen\s+(\d+)\s+we+k', lambda w: int(w) * 7),  # weeks to days
        ]
        
        for pattern, converter in patterns:
            match = re.search(pattern, text_lower)
            if match:
                if callable(converter):
                    days = converter() if len(match.groups()) == 0 else converter(match.group(1))
                else:
                    days = converter
                self.logger.info(f"✓ Found avg time to fill: {days} days | Source: {url}")
                return days
        
        return None
    
    def fetch_candidate_pool_size(self, text: str, url: str) -> Optional[int]:
        """
        Extract candidate pool size estimate from text.
        
        Returns:
        - Estimated pool size as integer
        - None if not mentioned
        """
        # Pattern: "15.000 kandidaten", "5000 professionals in database", etc.
        patterns = [
            r'(\d+[\.,]\d+|\d+)\s+(?:kandidaten|candidates|professionals|medewerkers)\s+(?:in|beschikbaar)',
            r'(?:database|bestand|pool)\s+van\s+(\d+[\.,]\d+|\d+)',
            r'(\d+[\.,]\d+|\d+)\s+(?:actieve|beschikbare)\s+(?:kandidaten|professionals)',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text.lower())
            if match:
                size_str = match.group(1).replace('.', '').replace(',', '')
                size = int(size_str)
                self.logger.info(f"✓ Found candidate pool size: {size} | Source: {url}")
                return size
        
        return None
    
    def fetch_annual_placements(self, text: str, url: str) -> Optional[int]:
        """
        Extract annual placements estimate from text.
        
        Returns:
        - Estimated annual placements
        - None if not mentioned
        """
        # Pattern: "5.000 plaatsingen per jaar", "10000 placements annually", etc.
        patterns = [
            r'(\d+[\.,]\d+|\d+)\s+plaatsingen?\s+(?:per\s+jaar|jaarlijks|annually)',
            r'(\d+[\.,]\d+|\d+)\s+(?:people|professionals|kandidaten)\s+(?:placed|geplaatst)\s+(?:per jaar|annually)',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text.lower())
            if match:
                count_str = match.group(1).replace('.', '').replace(',', '')
                count = int(count_str)
                self.logger.info(f"✓ Found annual placements: {count} | Source: {url}")
                return count
        
        return None
    
    def fetch_uses_inlenersbeloning(self, text: str, url: str) -> Optional[bool]:
        """
        Check if agency mentions using inlenersbeloning.
        
        Returns:
        - True if mentioned
        - None if not mentioned
        """
        text_lower = text.lower()
        
        if any(keyword in text_lower for keyword in [
            "inlenersbeloning", "inlenersloon", "loon inlener"
        ]):
            self.logger.info(f"✓ Found uses_inlenersbeloning: True | Source: {url}")
            return True
        
        return None
    
    def fetch_applies_inlenersbeloning_from_day1(self, text: str, url: str) -> Optional[bool]:
        """
        Check if agency applies inlenersbeloning from day 1.
        
        Returns:
        - True if explicitly mentioned from day 1
        - False if mentioned after a waiting period
        - None if not mentioned
        """
        text_lower = text.lower()
        
        if any(keyword in text_lower for keyword in [
            "inlenersbeloning vanaf dag 1", "inlenersbeloning dag 1",
            "inlenersbeloning vanaf de eerste dag"
        ]):
            self.logger.info(f"✓ Found applies_inlenersbeloning_from_day1: True | Source: {url}")
            return True
        
        # Check for waiting period mentions
        if re.search(r'inlenersbeloning\s+(?:na|vanaf)\s+(?:\d+|fase)', text_lower):
            self.logger.info(f"✓ Found applies_inlenersbeloning_from_day1: False | Source: {url}")
            return False
        
        return None
    
    def fetch_min_assignment_duration(self, text: str, url: str) -> Optional[int]:
        """
        Extract minimum assignment duration in weeks.
        
        Returns:
        - Number of weeks
        - None if not mentioned
        """
        # Pattern: "minimaal 4 weken", "minimum 2 maanden", etc.
        patterns = [
            (r'minim(?:aal|um)\s+(\d+)\s+we+k', lambda w: int(w)),
            (r'minim(?:aal|um)\s+(\d+)\s+maand', lambda m: int(m) * 4),
        ]
        
        for pattern, converter in patterns:
            match = re.search(pattern, text.lower())
            if match:
                weeks = converter(match.group(1))
                self.logger.info(f"✓ Found min assignment duration: {weeks} weeks | Source: {url}")
                return weeks
        
        return None
    
    def fetch_min_hours_per_week(self, text: str, url: str) -> Optional[int]:
        """
        Extract minimum hours per week.
        
        Returns:
        - Number of hours
        - None if not mentioned
        """
        # Pattern: "minimaal 20 uur per week", "minimum 32 uur"
        pattern = r'minim(?:aal|um)\s+(\d+)\s+uur\s+(?:per\s+week)?'
        match = re.search(pattern, text.lower())
        
        if match:
            hours = int(match.group(1))
            self.logger.info(f"✓ Found min hours per week: {hours} | Source: {url}")
            return hours
        
        return None
    
    def fetch_avg_hourly_rate(self, text: str, url: str) -> tuple[Optional[float], Optional[float]]:
        """
        Extract average hourly rate range from text.
        
        Returns:
        - (low, high) tuple of floats
        - (None, None) if not found
        """
        # Patterns: "€25-35 per uur", "vanaf €20 per uur", "€18,50 per uur"
        patterns = [
            r'€\s*(\d+(?:[.,]\d+)?)\s*-\s*€?\s*(\d+(?:[.,]\d+)?)\s*(?:per\s+)?uur',  # Range: €25-35 per uur
            r'€\s*(\d+(?:[.,]\d+)?)\s+tot\s+€?\s*(\d+(?:[.,]\d+)?)\s*(?:per\s+)?uur',  # Range: €25 tot €35 per uur
            r'vanaf\s+€\s*(\d+(?:[.,]\d+)?)\s*(?:per\s+)?uur',  # From: vanaf €25 per uur
            r'€\s*(\d+(?:[.,]\d+)?)\s*(?:per\s+)?uur',  # Single: €25 per uur
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text.lower())
            if match:
                if len(match.groups()) == 2:
                    # Range found
                    low = float(match.group(1).replace(',', '.'))
                    high = float(match.group(2).replace(',', '.'))
                    self.logger.info(f"✓ Found avg hourly rate: €{low}-{high} | Source: {url}")
                    return (low, high)
                elif len(match.groups()) == 1:
                    # Single value or "from" value
                    rate = float(match.group(1).replace(',', '.'))
                    if 'vanaf' in text.lower():
                        self.logger.info(f"✓ Found avg hourly rate: from €{rate} | Source: {url}")
                        return (rate, None)
                    else:
                        self.logger.info(f"✓ Found avg hourly rate: €{rate} | Source: {url}")
                        return (rate, rate)
        
        return (None, None)
    
    def fetch_review_rating_and_count(self, soup: BeautifulSoup, url: str) -> tuple[Optional[float], Optional[int]]:
        """
        Extract review rating and count from their own website.
        
        Returns:
        - (rating, count) tuple
        - (None, None) if not found
        """
        # Look for common rating patterns
        rating = None
        count = None
        
        # Pattern 1: Schema.org structured data
        # <span itemprop="ratingValue">4.5</span>
        # <span itemprop="reviewCount">123</span>
        rating_elem = soup.find(attrs={"itemprop": "ratingValue"})
        if rating_elem:
            try:
                rating = float(rating_elem.get_text(strip=True))
                self.logger.info(f"✓ Found review rating: {rating} | Source: {url}")
            except:
                pass
        
        count_elem = soup.find(attrs={"itemprop": "reviewCount"})
        if count_elem:
            try:
                count_text = count_elem.get_text(strip=True).replace('.', '').replace(',', '')
                count = int(count_text)
                self.logger.info(f"✓ Found review count: {count} | Source: {url}")
            except:
                pass
        
        # Pattern 2: Text patterns like "4.5 sterren (123 reviews)"
        if rating is None or count is None:
            page_text = soup.get_text()
            rating_match = re.search(r'(\d+[.,]\d+)\s*(?:sterren|stars|uit 5)', page_text)
            if rating_match:
                rating = float(rating_match.group(1).replace(',', '.'))
                self.logger.info(f"✓ Found review rating: {rating} | Source: {url}")
            
            count_match = re.search(r'\(?\s*(\d+)\s*(?:reviews|beoordelingen|recensies)', page_text, re.IGNORECASE)
            if count_match:
                count = int(count_match.group(1))
                self.logger.info(f"✓ Found review count: {count} | Source: {url}")
        
        return (rating, count)
    
    def fetch_external_review_urls(self, review_sources: List[Dict[str, str]]) -> List[str]:
        """
        Extract external review URLs from review_sources list.
        
        Args:
            review_sources: List of dicts with 'platform' and 'url' keys
        
        Returns:
            List of external review URLs
        """
        if not review_sources:
            return []
        
        urls = [source.get("url") for source in review_sources if source.get("url")]
        return urls
    
    def fetch_takeover_policy(self, text: str, url: str) -> dict:
        """
        Extract takeover/overname policy from terms & conditions.
        
        Returns dict with:
        - free_takeover_hours: int or None
        - free_takeover_weeks: int or None
        - overname_fee_model: "none" | "flat_fee" | "percentage_salary" | "scaled" | "unknown"
        - overname_fee_hint: str or None
        - overname_contract_reference: str (url) or None
        """
        text_lower = text.lower()
        result = {
            "free_takeover_hours": None,
            "free_takeover_weeks": None,
            "overname_fee_model": "unknown",
            "overname_fee_hint": None,
            "overname_contract_reference": url if any(x in url.lower() for x in ["terms", "conditions", "voorwaarden", "algemene"]) else None
        }
        
        # Look for free takeover period
        # Pattern: "na X uren gratis overnemen", "na X weken overname zonder kosten"
        hours_match = re.search(r'na\s+(\d+)\s+uren?\s+(?:gratis|kosteloos|zonder\s+kosten)?\s*(?:overnemen|overname)', text_lower)
        if hours_match:
            result["free_takeover_hours"] = int(hours_match.group(1))
            self.logger.info(f"✓ Found free takeover hours: {result['free_takeover_hours']} | Source: {url}")
        
        weeks_match = re.search(r'na\s+(\d+)\s+weken?\s+(?:gratis|kosteloos|zonder\s+kosten)?\s*(?:overnemen|overname)', text_lower)
        if weeks_match:
            result["free_takeover_weeks"] = int(weeks_match.group(1))
            self.logger.info(f"✓ Found free takeover weeks: {result['free_takeover_weeks']} | Source: {url}")
        
        # Detect fee model
        if "geen overnamekosten" in text_lower or "gratis overnemen" in text_lower:
            result["overname_fee_model"] = "none"
            result["overname_fee_hint"] = "Gratis overnemen na werkperiode"
            self.logger.info(f"✓ Found takeover fee model: none | Source: {url}")
        elif "vast bedrag" in text_lower or "vaste vergoeding" in text_lower:
            result["overname_fee_model"] = "flat_fee"
            # Try to extract the amount
            fee_match = re.search(r'€\s*(\d+(?:[.,]\d+)?)\s*(?:voor\s+)?overname', text_lower)
            if fee_match:
                amount = fee_match.group(1)
                result["overname_fee_hint"] = f"Vast bedrag van €{amount}"
            else:
                result["overname_fee_hint"] = "Vast bedrag voor overname"
            self.logger.info(f"✓ Found takeover fee model: flat_fee | Source: {url}")
        elif "percentage" in text_lower and "salaris" in text_lower:
            result["overname_fee_model"] = "percentage_salary"
            # Try to extract percentage
            pct_match = re.search(r'(\d+)%\s*(?:van|of)?\s*(?:het\s+)?(?:bruto\s+)?(?:jaar|maand)?salaris', text_lower)
            if pct_match:
                pct = pct_match.group(1)
                result["overname_fee_hint"] = f"{pct}% van salaris"
            else:
                result["overname_fee_hint"] = "Percentage van salaris"
            self.logger.info(f"✓ Found takeover fee model: percentage_salary | Source: {url}")
        elif "schaal" in text_lower or "oplopend" in text_lower:
            result["overname_fee_model"] = "scaled"
            result["overname_fee_hint"] = "Schaaltarief afhankelijk van periode"
            self.logger.info(f"✓ Found takeover fee model: scaled | Source: {url}")
        
        return result
    
    def map_city_to_province(self, city: str) -> Optional[str]:
        """
        Map a Dutch city name to its province.
        
        Args:
            city: City name (case-insensitive)
        
        Returns:
            Province name if found, None otherwise
        
        Example:
            >>> utils.map_city_to_province("Amsterdam")
            'Noord-Holland'
            >>> utils.map_city_to_province("Utrecht")
            'Utrecht'
        """
        if not city:
            return None
        
        city_lower = city.strip().lower()
        return CITY_TO_PROVINCE.get(city_lower)

