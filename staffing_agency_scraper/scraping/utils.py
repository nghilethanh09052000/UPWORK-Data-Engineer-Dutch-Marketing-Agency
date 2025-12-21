"""
Shared utility functions for all staffing agency scrapers.

This module provides reusable extraction methods using BS4 and regex.
Based on the 69 fields defined in output/_sample.json.
"""

from __future__ import annotations

import re
from typing import Dict, List, Optional, Union

from bs4 import BeautifulSoup

from staffing_agency_scraper.lib.normalize import (
    normalize_regions_served,
    normalize_sectors,
    normalize_focus_segments,
    normalize_contact_phone,
)
from staffing_agency_scraper.models import (
    AgencyServices,
    CaoType,
    GeoFocusType,
    OfficeLocation,
    PhaseSystem,
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
# Rule: Only match explicit role level mentions in context of job levels/candidates
# Require context that indicates these are actual job levels offered, not just generic mentions
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
        "entree",
        "startende",
    ],
    "medior": [
        "medior",
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
    "bouw": ["bouw", "construction", "aannemer", "infrastructuur"],
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
    "it_telecom": ["it & telecom"],
}

# List of normalized sectors (for reference)
NORMALIZED_SECTORS = list(SECTOR_KEYWORDS.keys())

# Growth Signals - Detection Keywords
# Rule: Only match explicit growth signal statements, not generic words
# IMPORTANT: Only factual, verifiable data - NO marketing language or subjective statements
GROWTH_SIGNAL_KEYWORDS = {
    "landelijke_dekking": [
        "landelijke dekking",
        "landelijk actief",
        "heel nederland",
        "nationwide coverage",
        "landelijk netwerk",
    ],
    "internationale_groep": [
        "internationale groep",
        "international group",
        "wereldwijd actief",
        "worldwide presence",
        "global presence",
    ],
    "beursgenoteerd": [
        "beursgenoteerd",
        "genoteerd aan de beurs",
        "listed company",
        "public company",
    ],
    "overnames": [
        "overname",
        "acquisitie",
        "overgenomen",
        "acquisition",
    ],
    "awards": [
        "award gewonnen",
        "prijs gewonnen",
        "winnaar van",
        "award winner",
    ],
}

# Company Size Fit - Detection Keywords
# Rule: Only match explicit company size mentions, not generic size words
COMPANY_SIZE_FIT_KEYWORDS = {
    "micro_1_10": [
        "micro bedrijf",
        "1-10 medewerkers",
        "kleine bedrijven tot 10 medewerkers",
        "startups",
    ],
    "smb_11_200": [
        "middelgroot bedrijf",
        "11-200 medewerkers",
        "mkb",
        "small medium business",
    ],
    "mid_market_201_1000": [
        "mid market",
        "201-1000 medewerkers",
        "middelgrote organisaties",
        "scale-up",
    ],
    "enterprise_1000_plus": [
        "grootbedrijf",
        "enterprise",
        "1000+ medewerkers",
        "multinational",
        "grote organisaties",
        "multinationals"
    ],
    "public_sector": [
        "publieke sector",
        "overheid",
        "gemeenten",
        "provincies",
        "ministeries",
    ],
}

# Customer Segments - Detection Keywords
# Rule: Only match explicit customer segment mentions, not generic sector words
CUSTOMER_SEGMENTS_KEYWORDS = {
    "MKB": [
        "mkb",
        "midden- en kleinbedrijf",
        "kleine en middelgrote bedrijven",
    ],
    "grootbedrijf": [
        "grootbedrijf",
        "grote bedrijven",
        "enterprise klanten",
    ],
    "overheid": [
        "overheid",
        "publieke sector",
        "overheidsinstellingen",
    ],
    "zorginstelling": [
        "zorginstellingen",
        "ziekenhuizen",
        "verpleeghuizen",
        "ggz instellingen",
    ],
    "onderwijsinstelling": [
        "onderwijsinstellingen",
        "scholen",
        "universiteiten",
        "hogescholen",
    ],
}

# Focus Segments - Detection Keywords
# Rule: Only match explicit focus segment mentions, not generic sector words
# These should appear in context of "we focus on" or "specialize in"
FOCUS_SEGMENTS_KEYWORDS = {
    "studenten": [
        "studenten werk",
        "studenten vacatures",
        "bijbaan voor studenten",
        "studenten uitzendwerk",
    ],
    "young_professionals": [
        "young professionals",
        "starters",
        "recent graduates",
        "jong talent",
    ],
    "blue_collar": [
        "productie personeel",
        "logistiek personeel",
        "bouw personeel",
        "technisch personeel",
        "magazijn medewerkers",
    ],
    "white_collar": [
        "kantoor personeel",
        "administratief personeel",
        "finance professionals",
        "hr professionals",
    ],
    "technisch_specialisten": [
        "technisch specialisten",
        "ict specialisten",
        "software engineers",
        "technische experts",
    ],
    "zorgprofessionals": [
        "zorgprofessionals",
        "verpleegkundigen",
        "zorgmedewerkers",
        "zorg professionals",
    ],
}

# Shift Types - Detection Keywords
# Rule: Only match explicit shift type mentions, not generic time words
SHIFT_TYPES_KEYWORDS = {
    "dagdienst": [
        "dagdienst",
        "overdag werken",
        "kantooruren",
        "dagdiensten",
    ],
    "avonddienst": [
        "avonddienst",
        "avonddiensten",
        "avondwerk",
    ],
    "nachtdienst": [
        "nachtdienst",
        "nachtdiensten",
        "nachtwerk",
    ],
    "weekend": [
        "weekenddiensten",
        "weekend werk",
        "zaterdag en zondag",
        "weekend beschikbaar",
    ],
    "24_7_bereikbaar": [
        "24/7 bereikbaar",
        "dag en nacht bereikbaar",
        "altijd bereikbaar",
        "24 uur per dag bereikbaar",
    ],
}

# Typical Use Cases - Detection Keywords
# Rule: Only match explicit use case descriptions, not generic words
TYPICAL_USE_CASES_KEYWORDS = {
    "piekdruk_opvangen": [
        "piekdruk opvangen",
        "opvangen van piekdruk",
        "piekmomenten opvangen",
        "drukke periodes opvangen",
    ],
    "langdurige_detachering": [
        "langdurige detachering",
        "structurele detachering",
        "permanente detachering",
        "vaste detachering",
    ],
    "projecten": [
        "project detachering",
        "projectbasis",
        "tijdelijk project",
        "projectondersteuning",
    ],
    "seizoenswerk": [
        "seizoenswerk",
        "seizoensarbeid",
        "seizoens personeel",
    ],
    "weekenddiensten": [
        "weekenddiensten",
        "weekend bezetting",
        "weekend personeel",
    ],
    "24_7_bezetting": [
        "24/7 bezetting",
        "dag en nacht bezetting",
        "altijd bezet",
        "continue bezetting",
    ],
}

# Speed Claims - Detection Keywords
# Rule: Only match explicit speed claims, not generic words
# Require full phrases that explicitly state speed promises
SPEED_CLAIMS_KEYWORDS = {
    "binnen_24_uur_kandidaten": [
        "binnen 24 uur kandidaten",
        "voorstel binnen 24 uur",
        "kandidaten binnen 24 uur",
        "binnen 24 uur een voorstel",
        "binnen 1 werkdag kandidaten",
        "kandidaten binnen 1 werkdag",
        "binnen 24 uur beschikbaar",
    ],
    "binnen_48_uur_kandidaten": [
        "binnen 48 uur kandidaten",
        "kandidaten binnen 48 uur",
        "binnen 2 werkdagen kandidaten",
        "kandidaten binnen twee werkdagen",
        "binnen 48 uur een voorstel",
    ],
    "snel_schakelen": [
        "snel schakelen",
        "snelle schakeling",
        "snel kunnen schakelen",
        "snel reageren",
        "snelle reactie",
    ],
    "grote_pools_direct_beschikbaar": [
        "grote pool direct beschikbaar",
        "groot bestand direct beschikbaar",
        "direct beschikbare pool",
        "grote database direct beschikbaar",
        "direct beschikbaar uit onze pool",
    ],
}


# Pricing Model - Detection Keywords
# Rule: Only match explicit pricing model mentions, not generic financial terms
PRICING_MODEL_KEYWORDS = {
    "omrekenfactor": [
        "omrekenfactor",
        "omrekenfactor model",
        "multiplicator model",
        "markup model",
    ],
    "fixed_margin": [
        "vaste marge",
        "fixed margin",
        "vaste marge percentage",
        "fixed margin percentage",
    ],
    "fixed_fee": [
        "vast tarief",
        "fixed fee",
        "all-in tarief",
        "all-in prijs",
        "vast bedrag",
    ],
}

# No Cure No Pay - Detection Keywords
# Rule: Only match explicit "no cure no pay" statements, not generic "free" or "risk free"
NO_CURE_NO_PAY_KEYWORDS = [
    "no cure no pay",
    "geen resultaat geen betaling",
    "no cure, no pay",
    "geen resultaat geen kosten",
    "resultaat geen kosten",
    "geen plaatsing geen betaling",
]

# Regions Served - Detection Keywords
REGIONS_KEYWORDS = {
    # Dutch Provinces
    "Noord-Holland": ["noord-holland", "noord holland", "province of north holland"],
    "Zuid-Holland": ["zuid-holland", "zuid holland", "province of south holland"],
    "Utrecht": ["utrecht", "provincie utrecht", "utrecht region"],
    "Noord-Brabant": ["noord-brabant", "noord brabant", "brabant", "province of north brabant"],
    "Gelderland": ["gelderland", "provincie gelderland", "province of gelderland"],
    "Limburg": ["limburg", "provincie limburg", "province of limburg"],
    "Overijssel": ["overijssel", "provincie overijssel", "province of overijssel"],
    "Groningen": ["groningen", "provincie groningen", "groningen region"],
    "Friesland": ["friesland", "fryslân", "provincie friesland", "province of friesland"],
    "Flevoland": ["flevoland", "provincie flevoland", "province of flevoland"],
    "Zeeland": ["zeeland", "provincie zeeland", "province of zeeland"],
    "Drenthe": ["drenthe", "provincie drenthe", "province of drenthe"],
    
    # Regional groupings
    "Randstad": ["randstad", "randstad region"],
    "Noord-Nederland": ["noord-nederland", "noord nederland", "northern netherlands"],
    "Zuid-Nederland": ["zuid-nederland", "zuid nederland", "southern netherlands"],
    "Oost-Nederland": ["oost-nederland", "oost nederland", "eastern netherlands"],
    "West-Nederland": ["west-nederland", "west nederland", "western netherlands"],
    
    # National coverage
    "heel_Nederland": [
        "heel nederland", "geheel nederland", "landelijk", "landelijke dekking",
        "throughout the netherlands", "nationwide", "national coverage",
        "alle provincies", "all provinces"
    ],
    
    # International (neighboring countries)
    "België": ["belgië", "belgium", "belgisch", "belgian"],
    "Duitsland": ["duitsland", "germany", "duits", "german"],
    "Luxemburg": ["luxemburg", "luxembourg"],
}

# City to Province Mapping - Dutch Cities
CITY_TO_PROVINCE = {
    # Noord-Holland
    "amsterdam": "Noord-Holland", "haarlem": "Noord-Holland", "zaandam": "Noord-Holland",
    "alkmaar": "Noord-Holland", "hoorn": "Noord-Holland", "hoofddorp": "Noord-Holland",
    "purmerend": "Noord-Holland", "beverwijk": "Noord-Holland", "hilversum": "Noord-Holland",
    "diemen": "Noord-Holland", "amsterdam-duivendrecht": "Noord-Holland", "schiphol": "Noord-Holland",
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
    "leerdam": "Utrecht",
    # Noord-Brabant
    "eindhoven": "Noord-Brabant", "tilburg": "Noord-Brabant", "breda": "Noord-Brabant",
    "'s-hertogenbosch": "Noord-Brabant", "den bosch": "Noord-Brabant", "helmond": "Noord-Brabant",
    "oss": "Noord-Brabant", "roosendaal": "Noord-Brabant", "bergen op zoom": "Noord-Brabant",
    "uden": "Noord-Brabant", "veghel": "Noord-Brabant", "veldhoven": "Noord-Brabant",
    "waalwijk": "Noord-Brabant", "oosterhout": "Noord-Brabant", "schijndel": "Noord-Brabant",
    "deurne": "Noord-Brabant", "etten-leur": "Noord-Brabant", "valkenswaard": "Noord-Brabant",
    "zevenbergen": "Noord-Brabant",
    # Gelderland
    "nijmegen": "Gelderland", "arnhem": "Gelderland", "apeldoorn": "Gelderland",
    "ede": "Gelderland", "zutphen": "Gelderland", "tiel": "Gelderland",
    "harderwijk": "Gelderland", "zaltbommel": "Gelderland", "zevenaar": "Gelderland",
    "lichtenvoorde": "Gelderland", "doetinchem": "Gelderland", "winterswijk": "Gelderland",
    "didam": "Gelderland", "wageningen": "Gelderland", "barneveld": "Gelderland",
    "'s heerenberg": "Gelderland", "geldermalsen": "Gelderland", "nijkerk": "Gelderland",
    # Limburg
    "maastricht": "Limburg", "venlo": "Limburg", "roermond": "Limburg",
    "heerlen": "Limburg", "sittard": "Limburg", "sittard-geleen": "Limburg",
    "weert": "Limburg",
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
    "hulst": "Zeeland", "oostburg": "Zeeland",
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
        self.logger.info(f"🔍 Fetching logo from {url}")
        
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
        self.logger.info(f"🔍 Fetching KvK number from {url}")
        
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
        self.logger.info(f"🔍 Fetching legal name from {url}")
        
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
        self.logger.info(f"🔍 Fetching contact email from {url}")
        
        email_match = re.search(r'([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})', text)
        if email_match:
            email = email_match.group(1)
            # Prefer info@, contact@, sales@
            if any(prefix in email.lower() for prefix in ['info@', 'contact@', 'sales@', 'werkgevers@']):
                self.logger.info(f"✓ Found email: {email} | Source: {url}")
                return email
        return None
    
    def fetch_contact_phone(self, text: str, url: str) -> Optional[str]:
        """Extract business phone number and normalize it."""
        self.logger.info(f"🔍 Fetching contact phone from {url}")
        
        phone_patterns = [
            r'\+31[\s-]?\d{1,2}[\s-]?\d{3}[\s-]?\d{2}[\s-]?\d{2}',
            r'0\d{2,3}[\s-]?\d{3}[\s-]?\d{2}[\s-]?\d{2}',
        ]
        
        for pattern in phone_patterns:
            phone_match = re.search(pattern, text)
            if phone_match:
                phone = phone_match.group(0)
                normalized_phone = normalize_contact_phone(phone)
                if normalized_phone != phone:
                    self.logger.info(f"✓ Found phone: {phone} -> normalized to: {normalized_phone} | Source: {url}")
                else:
                    self.logger.info(f"✓ Found phone: {phone} | Source: {url}")
                    return normalized_phone
            
        return None
    
    
    def fetch_regions_served(self, text: str, url: str) -> List[str]:
        """
        Extract geographical regions/provinces served by the agency.
        
        Returns list of regions/provinces normalized to controlled labels:
        - "landelijk" (for national coverage)
        - "Randstad" (for Randstad region)
        - Province-level labels (e.g. "Noord-Holland", "Zuid-Holland", "Utrecht", etc.)
        
        International countries and other regional groupings are filtered out.
        
        Args:
            text: Text to search in (page content)
            url: URL for logging
        
        Returns:
            List of normalized region names using controlled labels
        
        Example:
            >>> utils.fetch_regions_served("Wij zijn actief in heel Nederland", url)
            ['landelijk']
            >>> utils.fetch_regions_served("Vestigingen in Noord-Holland en Zuid-Holland", url)
            ['Noord-Holland', 'Zuid-Holland']
        """
        self.logger.info(f"🔍 Fetching regions served from {url}")
        
        text_lower = text.lower()
        regions = []
        
        # Check for national coverage first (highest priority)
        # Use "landelijk" directly (normalized label) instead of "heel_Nederland"
        has_national = False
        for keyword in REGIONS_KEYWORDS["heel_Nederland"]:
            if self._matches_keyword(keyword, text_lower):
                if "landelijk" not in regions:
                    regions.append("landelijk")
                    has_national = True
                    self.logger.info(f"✓ Found region: landelijk (national coverage) | Source: {url}")
                break
        
        # Check for specific provinces
        # Rule: Only include if explicitly mentioned as regions served (not just city mentions)
        # If "landelijk" is found, only include provinces if explicitly mentioned as additional regions
        provinces = [
            "Noord-Holland", "Zuid-Holland", "Utrecht", "Noord-Brabant",
            "Gelderland", "Limburg", "Overijssel", "Groningen",
            "Friesland", "Flevoland", "Zeeland", "Drenthe"
        ]
        
        for province in provinces:
            province_keywords = REGIONS_KEYWORDS[province]
            
            # Check if province is explicitly mentioned
            for keyword in province_keywords:
                if self._matches_keyword(keyword, text_lower):
                    # For provinces that are also city names (Groningen, Utrecht), require explicit context
                    keyword_pos = text_lower.find(keyword)
                    if keyword_pos >= 0:
                        # Check surrounding context (100 chars before and after)
                        context = text_lower[max(0, keyword_pos-100):min(len(text_lower), keyword_pos+100)]
                        
                        # Explicit province/region indicators
                        explicit_indicators = [
                            "provincie", "province", "region", "regio",
                            "actief in", "vestigingen in", "dekking", "coverage",
                            "gebied", "area", "regions served", "regions"
                        ]
                        
                        # Check if it's in a region/coverage context
                        is_region_context = any(indicator in context for indicator in explicit_indicators)
                        
                        # If "landelijk" is present, DO NOT add individual provinces
                        # "landelijk" means national coverage, so listing provinces is redundant and inconsistent
                        # Client feedback: "Mixed formats (heel_Nederland vs city/province names)" - avoid mixing
                        if has_national:
                            # Skip adding provinces when "landelijk" is already present
                            # This prevents the inconsistency of having both "landelijk" and individual provinces
                            self.logger.info(f"   Skipping province {province}: 'landelijk' already present (national coverage) | Source: {url}")
                            break
                        else:
                            # If no "landelijk", require region context for city-name provinces
                            if is_region_context:
                                if province not in regions:
                                    regions.append(province)
                                    self.logger.info(f"✓ Found region: {province} (in region context) | Source: {url}")
                    break
        
        # Check for regional groupings (only Randstad is allowed)
        if "Randstad" in REGIONS_KEYWORDS:
            for keyword in REGIONS_KEYWORDS["Randstad"]:
                if self._matches_keyword(keyword, text_lower):
                    if "Randstad" not in regions:
                        regions.append("Randstad")
                        self.logger.info(f"✓ Found region: Randstad | Source: {url}")
                    break
        
        # Note: International countries and other regional groupings are filtered out
        # by the normalization function
        
        # Normalize regions to use only controlled labels
        normalized = normalize_regions_served(regions)
        
        if normalized != regions:
            self.logger.info(f"✓ Normalized regions: {regions} -> {normalized} | Source: {url}")
        
        return normalized
    
    def fetch_geo_focus_type(self, text: str, url: str) -> GeoFocusType:
        """
        Extract geographic focus type from text.
        
        Returns one of:
        - LOCAL: Local/city-level focus
        - REGIONAL: Regional/province-level focus
        - NATIONAL: National coverage (entire Netherlands)
        - INTERNATIONAL: International/global presence
        
        Args:
            text: Text to search in (page content)
            url: URL for logging
        
        Returns:
            GeoFocusType enum value
        
        Example:
            >>> utils.fetch_geo_focus_type("Wij zijn actief in heel Nederland", url)
            GeoFocusType.NATIONAL
            >>> utils.fetch_geo_focus_type("Vestigingen in Amsterdam en Rotterdam", url)
            GeoFocusType.REGIONAL
        """
        self.logger.info(f"🔍 Fetching geo focus type from {url}")
        
        text_lower = text.lower()
        
        # Check for international/global presence first (highest priority)
        international_keywords = [
            "internationaal", "international", "wereldwijd", "worldwide",
            "global", "globale", "meerdere landen", "multiple countries",
            "europa", "europe", "actief in", "operates in", "countries",
            "landen", "internationale groep", "international group"
        ]
        if any(self._matches_keyword(keyword, text_lower) for keyword in international_keywords):
            # Check if it's truly international (mentions multiple countries or global)
            country_count_match = re.search(r'(\d+)\s*(?:landen|countries)', text_lower)
            if country_count_match:
                count = int(country_count_match.group(1))
                # Only set if explicitly stated - no threshold assumptions
                self.logger.info(f"✓ Found geo focus type: INTERNATIONAL ({count} countries explicitly stated) | Source: {url}")
                return GeoFocusType.INTERNATIONAL
            
            # Check for explicit international mentions (without number requirement)
            if any(phrase in text_lower for phrase in [
                "internationale groep", "international group", "global presence",
                "wereldwijd actief", "worldwide", "multiple countries"
            ]):
                self.logger.info(f"✓ Found geo focus type: INTERNATIONAL (explicitly stated) | Source: {url}")
                return GeoFocusType.INTERNATIONAL
        
        # Check for national coverage
        national_keywords = [
            "landelijk", "nationaal", "national", "heel nederland",
            "geheel nederland", "throughout the netherlands", "nationwide",
            "national coverage", "alle provincies", "all provinces",
            "landelijke dekking", "nationale dekking"
        ]
        if any(self._matches_keyword(keyword, text_lower) for keyword in national_keywords):
            self.logger.info(f"✓ Found geo focus type: NATIONAL | Source: {url}")
            return GeoFocusType.NATIONAL
        
        # Check for regional coverage (multiple provinces/regions mentioned)
        regional_keywords = [
            "regionaal", "regional", "provincie", "province",
            "randstad", "noord-nederland", "zuid-nederland",
            "oost-nederland", "west-nederland"
        ]
        province_count = 0
        provinces = [
            "noord-holland", "zuid-holland", "utrecht", "noord-brabant",
            "gelderland", "limburg", "overijssel", "groningen",
            "friesland", "flevoland", "zeeland", "drenthe"
        ]
        for province in provinces:
            if self._matches_keyword(province, text_lower):
                province_count += 1
        
        if province_count >= 2 or any(self._matches_keyword(keyword, text_lower) for keyword in regional_keywords):
            self.logger.info(f"✓ Found geo focus type: REGIONAL ({province_count} provinces) | Source: {url}")
            return GeoFocusType.REGIONAL
        
        # Check for local coverage (single city or very limited area)
        # Only set if explicitly stated - no assumptions
        local_keywords = [
            "lokaal", "local", "plaatselijk", "in de stad",
            "binnen de gemeente", "within the city"
        ]
        if any(self._matches_keyword(keyword, text_lower) for keyword in local_keywords):
            self.logger.info(f"✓ Found geo focus type: LOCAL | Source: {url}")
            return GeoFocusType.LOCAL
        
        # No default - return NATIONAL only if explicitly stated
        # If unclear, return NATIONAL as it's the model default (not an assumption)
        # Note: This is the Pydantic model default, not an assumption from text
        return GeoFocusType.NATIONAL
    
    # ========================================================================
    # SECTORS (Normalized list from client)
    # ========================================================================
    
    def fetch_sectors(self, text: str, url: str) -> List[str]:
        """
        Extract sectors using client's normalized list.
        
        Client requirement: Only standard sectors, not work types like "thuiswerk", "oproepkracht".
        Normalizes all sector names to controlled vocabulary (e.g., "it" -> "ict", "digital" -> "ict").
        """
        self.logger.info(f"🔍 Fetching sectors from {url}")
        
        text_lower = text.lower()
        sectors = []
        
        # Use the global SECTOR_KEYWORDS mapping
        for sector, keywords in SECTOR_KEYWORDS.items():
            for keyword in keywords:
                if self._matches_keyword(keyword, text_lower) and sector not in sectors:
                    sectors.append(sector)
                    self.logger.info(f"✓ Found sector: {sector} | Source: {url}")
                    break
        
        # Normalize sectors to use controlled vocabulary (IT -> ICT, digital -> ICT, etc.)
        normalized = normalize_sectors(sectors)
        if normalized != sectors:
            self.logger.info(f"✓ Normalized sectors: {sectors} -> {normalized} | Source: {url}")
        
        return normalized
    
    # ========================================================================
    # SERVICES (Field 24 from _sample.json)
    # ========================================================================
    
    def fetch_services(self, text: str, url: str) -> AgencyServices:
        """
        Extract services from text with comprehensive Dutch and English keyword mappings.
        
        Service mapping:
        - uitzenden: Temporary staffing (blue-collar, operational roles, flexwerk)
        - detacheren: Interim/secondment (specialists, professionals working at client sites)
        - werving_selectie: Recruitment & selection (permanent placements, vaste baan)
        - payrolling: Payroll services
        - zzp_bemiddeling: Freelance/ZZP intermediation (self-employed)
        - msp: Managed Service Provider
        - rpo: Recruitment Process Outsourcing
        - executive_search: Executive search / headhunting
        """
        self.logger.info(f"🔍 Fetching services from {url}")
        
        text_lower = text.lower()
        
        services = AgencyServices()
        
        # Uitzenden (temporary staffing, flexwerk)
        uitzenden_keywords = [
            "uitzenden", "uitzendbureau", "uitzendkracht", "uitzendwerk",
            "flexwerk", "flexwerker", "flexpool", "flexbureau",
            "tijdelijk personeel", "tijdelijke arbeid",
            "temporary staffing", "temp work", "temp agency"
        ]
        if any(keyword in text_lower for keyword in uitzenden_keywords):
            services.uitzenden = True
            self.logger.info(f"✓ Found service: uitzenden (temporary staffing) | Source: {url}")
        
        # Detacheren / Interim (secondment, interim professionals)
        detacheren_keywords = [
            "detacheren", "detachering", "gedetacheerd",
            "interim", "interim-professional", "interim management", "interim manager",
            "secondment", "seconded professional",
            "opdracht", "opdrachtgever",  # Often used in context of detachering
            "professional", "interim opdracht"
        ]
        if any(keyword in text_lower for keyword in detacheren_keywords):
            services.detacheren = True
            self.logger.info(f"✓ Found service: detacheren (interim/secondment) | Source: {url}")
        
        # Werving & Selectie (recruitment & selection, permanent placements)
        werving_keywords = [
            "werving", "selectie", "werving & selectie", "werving en selectie",
            "recruitment", "recruiting", "recruitment & selection",
            "vaste baan", "vast dienstverband", "permanent",
            "bemiddeling vast", "vaste functie",
            "headhunting"  # Often part of recruitment
        ]
        if any(keyword in text_lower for keyword in werving_keywords):
            services.werving_selectie = True
            self.logger.info(f"✓ Found service: werving_selectie (recruitment & selection) | Source: {url}")
        
        # Payrolling
        payrolling_keywords = [
            "payroll", "payrolling", "salarisadministratie",
            "loonstrook", "salary administration"
        ]
        if any(keyword in text_lower for keyword in payrolling_keywords):
            services.payrolling = True
            self.logger.info(f"✓ Found service: payrolling | Source: {url}")
        
        # ZZP / Freelance intermediation (self-employed)
        zzp_keywords = [
            "zzp", "zzp'er", "zzp-bemiddeling", "zzp bemiddeling",
            "zelfstandige", "zelfstandig professional", "zelfstandige zonder personeel",
            "freelance", "freelancer", "freelance opdracht",
            "self-employed", "independent contractor",
            "zzp-opdracht", "zzp opdracht"
        ]
        if any(keyword in text_lower for keyword in zzp_keywords):
            services.zzp_bemiddeling = True
            self.logger.info(f"✓ Found service: zzp_bemiddeling (freelance) | Source: {url}")
        
        # MSP (Managed Service Provider) - Require explicit confirmation
        # Only set to True if clearly stated as a service offering
        # Use strict keywords - "outsourcing" alone is too generic
        msp_keywords = [
            "msp",  # Explicit acronym
            "managed service provider",  # Full term
            "managed services"  # Plural form
        ]
        # Note: "vendor management" and "contingent workforce management" removed - too generic
        if any(keyword in text_lower for keyword in msp_keywords):
            services.msp = True
            self.logger.info(f"✓ Found service: msp (explicit confirmation) | Source: {url}")
        
        # RPO (Recruitment Process Outsourcing) - Require explicit confirmation
        # Only set to True if clearly stated as a service offering
        # Use strict keywords - "consultancy" alone is too generic
        rpo_keywords = [
            "rpo",  # Explicit acronym
            "recruitment process outsourcing",  # Full term
            "wervingsuitbesteding"  # Dutch term
        ]
        # Note: "recruitment outsourcing" removed - too generic without "process"
        if any(keyword in text_lower for keyword in rpo_keywords):
            services.rpo = True
            self.logger.info(f"✓ Found service: rpo (explicit confirmation) | Source: {url}")
        
        # Executive Search - Require explicit confirmation
        # Only set to True if clearly stated as a service offering
        # Use strict keywords - "headhunting" alone is too generic
        executive_keywords = [
            "executive search",  # Primary term
            "executive recruitment",  # Alternative term
            "executive werving",  # Dutch term
            "executive selectie"  # Dutch term
        ]
        # Note: "headhunting" and "headhunter" removed - too generic without "executive" context
        if any(keyword in text_lower for keyword in executive_keywords):
            services.executive_search = True
            self.logger.info(f"✓ Found service: executive_search (explicit confirmation) | Source: {url}")
        
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
    
    def fetch_phase_system(self, text: str, url: str) -> Optional[PhaseSystem]:
        """
        Extract phase system (fasensysteem) for ABU or NBBU CAO.
        
        Returns PhaseSystem object with:
        - abu_phases: List of phase letters (e.g., ["A", "B", "C"]) if ABU phases mentioned
        - nbbu_phases: List of phase letters (e.g., ["A", "B", "C"]) if NBBU phases mentioned
        
        Detects phase letters (A, B, C, D) and determines if they're ABU or NBBU based on context.
        """
        self.logger.info(f"🔍 Fetching phase system from {url}")
        
        text_lower = text.lower()
        phase_system = PhaseSystem()
        
        # Check if text mentions ABU or NBBU to determine context
        is_abu_context = self._matches_keyword("abu", text_lower)
        is_nbbu_context = self._matches_keyword("nbbu", text_lower)
        
        # Pattern to find phase letters: "fase A", "fase B", "phase C", etc.
        # Also handles: "A-fase", "B-fase", "fase 1 (A)", etc.
        phase_patterns = [
            r'fase\s+([A-D])',  # "fase A", "fase B"
            r'([A-D])-?fase',  # "A-fase", "B-fase"
            r'fase\s+\d+\s*\(([A-D])\)',  # "fase 1 (A)"
            r'phase\s+([A-D])',  # "phase A", "phase B"
            r'([A-D])\s+phase',  # "A phase", "B phase"
        ]
        
        found_phases = set()
        for pattern in phase_patterns:
            matches = re.findall(pattern, text_lower, re.IGNORECASE)
            for match in matches:
                phase_letter = match.upper()
                if phase_letter in ['A', 'B', 'C', 'D']:
                    found_phases.add(phase_letter)
        
        if found_phases:
            phases_list = sorted(list(found_phases))  # Sort: A, B, C, D
            
            # Determine if ABU or NBBU based on context
            if is_abu_context and not is_nbbu_context:
                phase_system.abu_phases = phases_list
                self.logger.info(f"✓ Found ABU phases: {phases_list} | Source: {url}")
            elif is_nbbu_context and not is_abu_context:
                phase_system.nbbu_phases = phases_list
                self.logger.info(f"✓ Found NBBU phases: {phases_list} | Source: {url}")
            elif is_abu_context and is_nbbu_context:
                # Both mentioned - try to determine from context
                # Check which one is mentioned closer to the phase info
                abu_pos = text_lower.find("abu")
                nbbu_pos = text_lower.find("nbbu")
                
                # Find the position of the first phase mention
                phase_positions = []
                for phase in phases_list:
                    for pattern in phase_patterns:
                        match = re.search(pattern, text_lower, re.IGNORECASE)
                        if match:
                            phase_positions.append(match.start())
                            break
                
                if phase_positions:
                    phase_pos = min(phase_positions)
                    if abs(abu_pos - phase_pos) < abs(nbbu_pos - phase_pos):
                        phase_system.abu_phases = phases_list
                        self.logger.info(f"✓ Found ABU phases: {phases_list} (closer to ABU mention) | Source: {url}")
                    else:
                        phase_system.nbbu_phases = phases_list
                        self.logger.info(f"✓ Found NBBU phases: {phases_list} (closer to NBBU mention) | Source: {url}")
                else:
                    # Can't determine proximity - don't assume, return None
                    self.logger.warning(f"⚠ Found phases {phases_list} but both ABU and NBBU mentioned without clear context | Source: {url}")
                    return None
            else:
                # No clear CAO context, but phases found - don't assume ABU
                # Only return if explicitly stated with CAO context
                self.logger.warning(f"⚠ Found phases {phases_list} but no CAO context (ABU/NBBU) - not setting | Source: {url}")
                return None
        
        # Also check for numeric phase counts (3 fasen, 4 fasen) - only if explicitly stated with CAO context
        # If we found phases, we already have them. Otherwise, check for counts.
        if not found_phases:
            if "3 fasen" in text_lower or "3 phases" in text_lower:
                # Only set if CAO context is explicitly stated
                if is_abu_context:
                    phase_system.abu_phases = ["A", "B", "C"]
                    self.logger.info(f"✓ Found ABU 3-phase system: ['A', 'B', 'C'] | Source: {url}")
                elif is_nbbu_context:
                    phase_system.nbbu_phases = ["A", "B", "C"]
                    self.logger.info(f"✓ Found NBBU 3-phase system: ['A', 'B', 'C'] | Source: {url}")
                # No default - only set if CAO context is explicit
            elif "4 fasen" in text_lower or "4 phases" in text_lower:
                # Only set if CAO context is explicitly stated
                if is_abu_context:
                    phase_system.abu_phases = ["A", "B", "C", "D"]
                    self.logger.info(f"✓ Found ABU 4-phase system: ['A', 'B', 'C', 'D'] | Source: {url}")
                elif is_nbbu_context:
                    phase_system.nbbu_phases = ["A", "B", "C", "D"]
                    self.logger.info(f"✓ Found NBBU 4-phase system: ['A', 'B', 'C', 'D'] | Source: {url}")
                # No default - only set if CAO context is explicit
        
        # Return None if no phases found, otherwise return the PhaseSystem object
        if phase_system.abu_phases is None and phase_system.nbbu_phases is None:
            return None
        
        return phase_system
    
    def fetch_certifications(self, text: str | Dict[str, str], url: str = "accumulated_text") -> List[str]:
        """
        Extract certifications.
        
        Args:
            text: Either a string of text or a dict mapping URLs to text
            url: URL to log (only used if text is a string)
        """
        from staffing_agency_scraper.lib.normalize import normalize_certifications
        
        certs = []
        
        cert_keywords = {
            "iso 9001": "ISO 9001",
            "iso9001": "ISO 9001",
            "iso_9001": "ISO 9001",
            "iso-9001": "ISO 9001",
            "iso 14001": "ISO 14001",
            "iso14001": "ISO 14001",
            "iso_14001": "ISO 14001",
            "iso-14001": "ISO 14001",
            "iso 27001": "ISO 27001",
            "iso27001": "ISO 27001",
            "iso_27001": "ISO 27001",
            "iso-27001": "ISO 27001",
            "iso/iec 27001": "ISO 27001",
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
        
        # Normalize certifications to ensure consistent format
        normalized = normalize_certifications(certs)
        if normalized != certs:
            self.logger.info(f"✓ Normalized certifications: {certs} -> {normalized} | Source: {url}")
        
        return normalized
    
    # ========================================================================
    # DIGITAL CAPABILITIES - PORTAL DETECTION (Client requirement #3)
    # ========================================================================
    
    def detect_candidate_portal(self, soup: BeautifulSoup, text: str, url: str) -> bool:
        """
        Detect candidate/employee portal.
        
        Client requirement: Look for specific candidate login indicators.
        Note: Generic "login" is too vague - we need specific evidence.
        """
        self.logger.info(f"🔍 Detecting candidate portal on {url}")
        
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
        self.logger.info(f"🔍 Detecting client portal on {url}")
        
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
        self.logger.info(f"🔍 Fetching role levels from {url}")
        
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
        self.logger.info(f"🔍 Fetching review sources from {url}")
        
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
        
        IMPORTANT: Only extracts objective, factual, verifiable data.
        NO marketing language, slogans, or subjective statements.
        
        Detects factual claims like:
        - National/regional coverage (explicitly stated)
        - Years active (founding date with context)
        - Part of international group (explicitly stated)
        - Listed on stock exchange (explicitly stated)
        - Number of offices/locations (with specific numbers)
        - Acquisitions/mergers (explicitly stated)
        - International presence (with specific country counts)
        - Awards (only if explicitly mentioned)
        
        Examples of what is NOT extracted:
        - "Winnaarsmentaliteit" (marketing slogan)
        - "Grootste database" (comparative claim)
        - "Veel successen" (subjective statement)
        - "Beste service" (marketing claim)
        """
        self.logger.info(f"🔍 Fetching growth signals from {url}")
        
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
        
        # Large office network - only if explicitly stated
        office_match = re.search(r'(\d+)\s*(?:kantoren|kantoor|offices|office)', text_lower)
        if office_match:
            count = int(office_match.group(1))
            # Only add if explicitly stated - no threshold assumptions
            signals.append(f"{count}_plus_kantoren")
            self.logger.info(f"✓ Found growth signal: {count}_plus_kantoren (explicitly stated) | Source: {url}")
        
        # Acquisitions - only if explicitly stated
        if any(self._matches_keyword(keyword, text_lower) for keyword in GROWTH_SIGNAL_KEYWORDS["overnames"]):
            signals.append("overnames_gedaan")
            self.logger.info(f"✓ Found growth signal: overnames_gedaan (explicitly stated) | Source: {url}")
        
        # International offices - only if explicitly stated
        country_match = re.search(r'(\d+)\s*(?:landen|countries)', text_lower)
        if country_match:
            count = int(country_match.group(1))
            # Only add if explicitly stated - no threshold assumptions
            signals.append(f"actief_in_{count}_landen")
            self.logger.info(f"✓ Found growth signal: actief_in_{count}_landen (explicitly stated) | Source: {url}")
        
        # Awards and certifications (growth indicator)
        # Only extract if explicitly stated - awards are factual if mentioned
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
        
        # Final validation: Ensure no marketing language slipped through
        # Filter out any signals that contain marketing keywords
        marketing_keywords = [
            "grootste", "beste", "meeste", "winnaar", "succes", "mentaliteit",
            "largest", "best", "most", "winner", "success", "mentality",
            "extreme", "uiterste", "vier", "celebrate", "specialist"
        ]
        
        filtered_signals = []
        for signal in unique_signals:
            signal_lower = signal.lower()
            # Check if signal contains marketing keywords
            if any(mk in signal_lower for mk in marketing_keywords):
                self.logger.warning(
                    f"⚠ Filtered out potential marketing language from growth signal: '{signal}' | Source: {url}"
                )
                continue
            filtered_signals.append(signal)
        
        return filtered_signals
    
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
        self.logger.info(f"🔍 Fetching company size fit from {url}")
        
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
        self.logger.info(f"🔍 Fetching customer segments from {url}")
        
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
        
        Returns normalized list of focus segments:
        - studenten
        - young_professionals
        - blue_collar
        - white_collar
        - technisch_specialisten
        - zorgprofessionals
        
        Normalizes all segment names to controlled vocabulary for consistency.
        """
        self.logger.info(f"🔍 Fetching focus segments from {url}")
        
        text_lower = text.lower()
        segments = []
        
        for segment, keywords in FOCUS_SEGMENTS_KEYWORDS.items():
            if any(self._matches_keyword(keyword, text_lower) for keyword in keywords):
                segments.append(segment)
                self.logger.info(f"✓ Found focus segment: {segment} | Source: {url}")
        
        # Remove duplicates
        unique_segments = list(set(segments))
        
        # Normalize focus segments to use controlled vocabulary
        normalized = normalize_focus_segments(unique_segments)
        if normalized != unique_segments:
            self.logger.info(f"✓ Normalized focus segments: {unique_segments} -> {normalized} | Source: {url}")
        
        return normalized
    
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
        self.logger.info(f"🔍 Fetching shift types from {url}")
        
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
        self.logger.info(f"🔍 Fetching typical use cases from {url}")
        
        text_lower = text.lower()
        use_cases = []
        
        for use_case, keywords in TYPICAL_USE_CASES_KEYWORDS.items():
            if any(self._matches_keyword(keyword, text_lower) for keyword in keywords):
                use_cases.append(use_case)
                self.logger.info(f"✓ Found typical use case: {use_case} | Source: {url}")
        
        return list(set(use_cases))  # Remove duplicates
    
    def fetch_speed_claims(self, text: str, url: str) -> List[str]:
        """
        Extract speed claims from text - only explicit speed promises.
        
        Rule: Only match explicit speed claims, not generic words like "snel" or "direct".
        Requires full phrases that explicitly state speed promises.
        
        Returns list of speed claims:
        - binnen_24_uur_kandidaten: Explicit promise of candidates within 24 hours
        - binnen_48_uur_kandidaten: Explicit promise of candidates within 48 hours
        - snel_schakelen: Explicit claim of fast switching/reaction
        - grote_pools_direct_beschikbaar: Explicit claim of large pools with direct availability
        """
        self.logger.info(f"🔍 Fetching speed claims from {url}")
        
        text_lower = text.lower()
        speed_claims = []
        
        for claim, keywords in SPEED_CLAIMS_KEYWORDS.items():
            # Require full phrase match, not just word presence
            for keyword in keywords:
                if self._matches_keyword(keyword, text_lower):
                    speed_claims.append(claim)
                    self.logger.info(f"✓ Found speed claim: {claim} (matched: '{keyword}') | Source: {url}")
                    break  # Only add once per claim type
        
        return list(set(speed_claims))  # Remove duplicates
    
    def fetch_volume_specialisation(self, text: str, url: str) -> str:
        """
        Extract volume specialisation from text - only explicit mentions with context.
        
        Rule: Only include data that is explicitly stated in a volume/specialisation context.
        Requires keywords to appear in contexts like "we specialize in", "our focus is", 
        "we offer", or explicit service descriptions.
        
        No assumptions or weak indicators. Excludes generic mentions without context.

        Returns one of:
        - ad_hoc_1_5       : Ad-hoc / niche / specialist placements (1-5 people) - only if explicitly stated
        - pools_5_50       : Pool management (5-50 people) - only if explicitly stated
        - massa_50_plus    : Mass recruitment (50+ people) - only if explicitly stated
        - unknown          : Cannot determine (default - not an assumption)
        """

        self.logger.info(f"🔍 Fetching volume specialisation from {url}")

        text_lower = text.lower()

        # -------------------------------
        # MASS RECRUITMENT (50+) - Only explicit mentions with context
        # -------------------------------
        mass_keywords = [
            "mass recruitment",
            "grootschalige werving",
            "bulk recruitment",
            "high volume recruitment",
            "massawerving",
            "grootschalige recruitment",
        ]

        # -------------------------------
        # POOL MANAGEMENT (5–50) - Only explicit mentions with context
        # -------------------------------
        pool_keywords = [
            "flexpool",
            "talentpool",
            "talent pool",
            "vaste pool",
            "kandidatenpool",
            "poolmanagement",
            "pool management",
            "inzetpool",
            "vaste flexibele schil",
        ]

        # -------------------------------
        # AD-HOC / SPECIALIST (1–5) - Only explicit mentions with context
        # -------------------------------
        adhoc_keywords = [
            "executive search",
            "direct search",
            "headhunting",
            "niche recruitment",
            "schaarse profielen",
            "1-op-1 bemiddeling",
            "persoonlijke search",
            "maatwerk voor sleutelposities",
        ]

        # -------------------------------
        # CONTEXT KEYWORDS - Require these nearby to ensure it's about specialisation
        # -------------------------------
        specialisation_context = [
            "specialiseren", "specialize", "specialisatie", "specialization",
            "focus", "focussen", "focus op", "focus on",
            "gespecialiseerd", "specialized", "expertise",
            "aanbod", "offer", "diensten", "services",
            "we bieden", "we offer", "ons aanbod", "our services",
            "wij zijn gespecialiseerd", "we specialize",
        ]

        # -------------------------------
        # MATCHING LOGIC - Require explicit context, not just keyword presence
        # -------------------------------

        # Helper function to check if keyword appears in specialisation context
        def _has_specialisation_context(keyword: str, text: str) -> bool:
            """Check if keyword appears near specialisation context indicators."""
            keyword_pos = text.find(keyword)
            if keyword_pos < 0:
                return False
            
            # Check surrounding context (200 chars before and after)
            context_before = text[max(0, keyword_pos - 200):keyword_pos]
            context_after = text[keyword_pos + len(keyword):min(len(text), keyword_pos + len(keyword) + 200)]
            full_context = context_before + " " + context_after
            
            # Check if any specialisation context keyword appears nearby
            for ctx_keyword in specialisation_context:
                if ctx_keyword in full_context:
                    return True
            
            # Also check if keyword appears in a services/diensten section
            # Look for services section indicators within 300 chars
            services_indicators = ["diensten", "services", "ons aanbod", "wat wij bieden", "onze diensten"]
            extended_context = text[max(0, keyword_pos - 300):min(len(text), keyword_pos + len(keyword) + 300)]
            if any(indicator in extended_context for indicator in services_indicators):
                return True
            
            return False

        # MASS: Only explicit mentions with specialisation context
        for keyword in mass_keywords:
            if self._matches_keyword(keyword, text_lower):
                if _has_specialisation_context(keyword, text_lower):
                    self.logger.info(f"✓ Volume specialisation: massa_50_plus (explicit mention with context: '{keyword}') | Source: {url}")
                    return "massa_50_plus"
                else:
                    self.logger.info(f"   Skipping '{keyword}': found but not in specialisation context | Source: {url}")

        # POOL: Only explicit mentions with specialisation context
        for keyword in pool_keywords:
            if self._matches_keyword(keyword, text_lower):
                if _has_specialisation_context(keyword, text_lower):
                    self.logger.info(f"✓ Volume specialisation: pools_5_50 (explicit mention with context: '{keyword}') | Source: {url}")
                    return "pools_5_50"
                else:
                    self.logger.info(f"   Skipping '{keyword}': found but not in specialisation context | Source: {url}")

        # AD-HOC: Only explicit mentions with specialisation context
        for keyword in adhoc_keywords:
            if self._matches_keyword(keyword, text_lower):
                if _has_specialisation_context(keyword, text_lower):
                    self.logger.info(f"✓ Volume specialisation: ad_hoc_1_5 (explicit mention with context: '{keyword}') | Source: {url}")
                    return "ad_hoc_1_5"
                else:
                    self.logger.info(f"   Skipping '{keyword}': found but not in specialisation context | Source: {url}")

        # Return unknown if not explicitly stated with context (not an assumption)
        self.logger.info(f"⚠ Volume specialisation: unknown (not explicitly stated with specialisation context) | Source: {url}")
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
        self.logger.info(f"🔍 Fetching pricing model from {url}")
        
        text_lower = text.lower()
        
        for model, keywords in PRICING_MODEL_KEYWORDS.items():
            if any(keyword in text_lower for keyword in keywords):
                self.logger.info(f"✓ Found pricing model: {model} | Source: {url}")
                return model
        
        return "unknown"
    
    def fetch_pricing_transparency(self, text: str, url: str) -> Optional[str]:
        """
        Extract pricing transparency level from text - only explicit statements.
        
        Rule: Only match explicit pricing transparency statements, not generic words.
        Requires full phrases that explicitly state pricing transparency level.
        
        Returns one of:
        - public_examples: Public pricing examples explicitly available (tariff tables, rate cards)
        - explainer_only: Explicit explanation of pricing model without examples
        - quote_only: Explicitly stated as quote-only (contact for quote)
        - None: No explicit pricing transparency information found (default - not an assumption)
        """
        self.logger.info(f"🔍 Fetching pricing transparency from {url}")
        
        text_lower = text.lower()
        
        # Check for public pricing examples - require explicit phrases
        # Must be in context of pricing/tariffs, not just generic mentions
        public_examples_keywords = [
            "tarievenlijst",
            "tarieventabel",
            "rate card",
            "pricing example",
            "voorbeeld tarief",
            "voorbeeld uurtarief",
            "tarieven overzicht",
            "pricing overzicht",
            "uurtarieven",
            "tarieven per uur",
            "€ per uur",
            "vanaf €",
            "kosten per uur",
        ]
        
        # Check if any explicit pricing example phrase is found
        for keyword in public_examples_keywords:
            if self._matches_keyword(keyword, text_lower):
                # Additional context check: should be near pricing-related words
                keyword_pos = text_lower.find(keyword)
                if keyword_pos >= 0:
                    context = text_lower[max(0, keyword_pos-100):min(len(text_lower), keyword_pos+100)]
                    pricing_context = any(ctx in context for ctx in [
                        "tarief", "prijs", "kosten", "pricing", "rate", "uurtarief"
                    ])
                    if pricing_context:
                        self.logger.info(f"✓ Found pricing transparency: public_examples (matched: '{keyword}') | Source: {url}")
            return "public_examples"
        
        # Check for pricing model explanation - require explicit phrases
        explainer_keywords = [
            "hoe werkt onze prijs",
            "hoe werkt het tarief",
            "pricing model uitleg",
            "kostenmodel uitleg",
            "tariefstructuur",
            "prijsopbouw",
            "hoe berekenen we",
            "hoe berekenen wij",
        ]
        
        for keyword in explainer_keywords:
            if self._matches_keyword(keyword, text_lower):
                self.logger.info(f"✓ Found pricing transparency: explainer_only (matched: '{keyword}') | Source: {url}")
            return "explainer_only"
        
        # Check for quote-only approach - require explicit phrases
        quote_only_keywords = [
            "offerte opvragen",
            "vraag een offerte aan",
            "contact voor offerte",
            "neem contact op voor offerte",
            "request quote",
            "maatwerk tarief",
            "op maat gemaakte offerte",
            "vrijblijvende offerte",
        ]
        
        for keyword in quote_only_keywords:
            if self._matches_keyword(keyword, text_lower):
                self.logger.info(f"✓ Found pricing transparency: quote_only (matched: '{keyword}') | Source: {url}")
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
        self.logger.info(f"🔍 Fetching no cure no pay from {url}")
        
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
        self.logger.info(f"🔍 Fetching omrekenfactor from {url}")
        
        text_lower = text.lower()
        
        # Pattern: omrekenfactor 1.45, omrekenfactor vanaf 1.4, 1.35-1.55, etc.
        patterns = [
            # Range patterns: "omrekenfactor 1.35 - 1.55", "omrekenfactor 1,35 tot 1,55"
            r'omrekenfactor\s+(?:van\s+)?(\d+[.,]\d+)\s*(?:-|tot|t/m|en)\s*(\d+[.,]\d+)',
            r'omrekenfactor\s+(\d+[.,]\d+)\s*(?:-|tot|t/m|en)\s*(\d+[.,]\d+)',
            # Single value with "vanaf": "omrekenfactor vanaf 1.4"
            r'omrekenfactor\s+vanaf\s+(\d+[.,]\d+)',
            # Single value: "omrekenfactor 1.45"
            r'omrekenfactor\s+(\d+[.,]\d+)',
            # Alternative terms
            r'multiplicator\s+(?:van\s+)?(\d+[.,]\d+)(?:\s*(?:-|tot|t/m|en)\s*(\d+[.,]\d+))?',
            r'markup\s+(?:van\s+)?(\d+[.,]\d+)(?:\s*(?:-|tot|t/m|en)\s*(\d+[.,]\d+))?',
            r'mark-up\s+(?:van\s+)?(\d+[.,]\d+)(?:\s*(?:-|tot|t/m|en)\s*(\d+[.,]\d+))?',
            # Pattern with "tussen": "tussen 1.35 en 1.55"
            r'(?:omrekenfactor|multiplicator|markup)\s+tussen\s+(\d+[.,]\d+)\s+en\s+(\d+[.,]\d+)',
            # Pattern: "1.35x tot 1.55x" or "1,35x - 1,55x"
            r'(\d+[.,]\d+)x\s*(?:-|tot|t/m|en)\s*(\d+[.,]\d+)x',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text_lower)
            if match:
                try:
                    min_val = float(match.group(1).replace(',', '.'))
                    max_val = None
                    if len(match.groups()) >= 2 and match.group(2):
                        max_val = float(match.group(2).replace(',', '.'))
                    # If only one value found and it's a range pattern, use it as both min and max
                    elif len(match.groups()) == 1:
                        max_val = min_val
                    
                    self.logger.info(f"✓ Found omrekenfactor: {min_val} - {max_val} | Source: {url}")
                    return (min_val, max_val)
                except (ValueError, AttributeError):
                    continue
        
        return (None, None)
    
    def fetch_example_pricing_hint(self, text: str, url: str) -> Optional[str]:
        """
        Extract example pricing hint from text - extract exact text as stated, no assumptions or calculations.
        
        Rule: Only match if price is mentioned in explicit pricing context (tarief, prijs, kosten, rate, pricing).
        Excludes: employee salaries, unrelated prices, blog/article prices.
        
        Extracts the exact text as it appears on the website, without reformatting or calculations.
        
        Returns:
        - Example pricing hint as string (exact text as stated, ONLY if in explicit pricing context)
        - None if not found or context is unclear
        """
        self.logger.info(f"🔍 Fetching example pricing hint from {url}")
        
        text_lower = text.lower()
        original_text = text  # Keep original for exact extraction
        
        # REQUIRED: Pricing context keywords (must appear near the price)
        pricing_context_keywords = [
            'tarief', 'tarieven', 'uurtarief', 'prijs', 'prijzen', 'kosten',
            'rate', 'rates', 'pricing', 'price', 'fee', 'fees',
            'voorbeeld tarief', 'voorbeeld prijs', 'example rate',
            'tarieventabel', 'tarievenlijst', 'rate card', 'prijslijst',
            'omrekenfactor', 'marge', 'markup',
        ]
        
        # EXCLUDE: Non-pricing contexts (salaries, employee compensation, etc.)
        exclude_keywords = [
            'salaris', 'salary', 'loon', 'wage', 'wages', 'inkomen', 'income',
            'werknemer', 'employee', 'medewerker', 'staff', 'personeel',
            'bruto', 'netto', 'bruto-netto', 'loonstrook', 'payslip',
            'vakantiegeld', 'holiday pay', 'bonus', 'premie',
            'sollicitatie', 'application', 'solliciteren', 'apply',
            'vacature', 'vacancy', 'job', 'functie', 'position',
        ]
        
        def _has_pricing_context(match_start: int, match_end: int, window: int = 150) -> bool:
            """Check if price match is in explicit pricing context."""
            context_start = max(0, match_start - window)
            context_end = min(len(text_lower), match_end + window)
            context = text_lower[context_start:context_end]
            
            # Must have at least one pricing context keyword
            has_pricing_keyword = any(keyword in context for keyword in pricing_context_keywords)
            
            # Must NOT have exclude keywords (unless they're clearly not about employee salaries)
            # Check close context (50 chars) for exclude keywords
            close_context = text_lower[max(0, match_start - 50):match_end + 50]
            has_exclude_keyword = any(keyword in close_context for keyword in exclude_keywords)
            
            if has_exclude_keyword:
                self.logger.info(f"   Skipping: price found but in excluded context (salary/employee/etc.) | Source: {url}")
                return False
            
            if not has_pricing_keyword:
                self.logger.info(f"   Skipping: price found but no explicit pricing context (tarief/prijs/kosten) | Source: {url}")
                return False
            
            return True
        
        def _extract_exact_text(match_start: int, match_end: int, window: int = 50) -> str:
            """Extract the exact text around the match, preserving original formatting."""
            # Extract a reasonable snippet that includes the full pricing statement
            # Try to get a complete phrase/sentence
            start = max(0, match_start - window)
            end = min(len(original_text), match_end + window)
            
            # Try to find sentence boundaries
            snippet = original_text[start:end]
            
            # Find the actual match in original case
            match_text = original_text[match_start:match_end]
            
            # If the snippet is reasonable, return it; otherwise return just the match
            # Clean up extra whitespace but preserve the original format
            snippet = ' '.join(snippet.split())
            
            # Prefer returning just the match if it's clear, otherwise return snippet
            if len(snippet) > len(match_text) * 2:
                # Snippet is too long, return just the match
                return match_text.strip()
            
            return snippet.strip()
        
        # STRICT PATTERNS: Require explicit pricing context, extract exact text
        # Pattern 1: Hourly rates with range
        hourly_range_pattern = r'€\s*\d+(?:[.,]\d+)?\s*(?:-|tot|t/m)\s*€?\s*\d+(?:[.,]\d+)?\s*(?:per\s+)?uur'
        match = re.search(hourly_range_pattern, text_lower)
        if match:
            if _has_pricing_context(match.start(), match.end()):
                hint = _extract_exact_text(match.start(), match.end())
                self.logger.info(f"✓ Found example pricing hint: {hint} (in pricing context) | Source: {url}")
                return hint
        
        # Pattern 2: Hourly rates "vanaf" (from)
        vanaf_pattern = r'vanaf\s+€\s*\d+(?:[.,]\d+)?\s*(?:per\s+)?uur'
        match = re.search(vanaf_pattern, text_lower)
        if match:
            if _has_pricing_context(match.start(), match.end()):
                hint = _extract_exact_text(match.start(), match.end())
                self.logger.info(f"✓ Found example pricing hint: {hint} (in pricing context) | Source: {url}")
                return hint
        
        # Pattern 3: Single hourly rate (most restrictive - require explicit pricing keyword nearby)
        hourly_single_pattern = r'€\s*\d+(?:[.,]\d+)?\s*(?:per\s+)?uur'
        match = re.search(hourly_single_pattern, text_lower)
        if match:
            if _has_pricing_context(match.start(), match.end(), window=100):  # Smaller window for single rates
                hint = _extract_exact_text(match.start(), match.end())
                self.logger.info(f"✓ Found example pricing hint: {hint} (in pricing context) | Source: {url}")
                return hint
        
        # Pattern 4: Monthly rates (only if in explicit pricing context)
        monthly_pattern = r'€\s*\d+(?:[.,]\d+)?\s*(?:per\s+)?maand'
        match = re.search(monthly_pattern, text_lower)
        if match:
            if _has_pricing_context(match.start(), match.end()):
                hint = _extract_exact_text(match.start(), match.end())
                self.logger.info(f"✓ Found example pricing hint: {hint} (in pricing context) | Source: {url}")
                return hint
        
        # Pattern 5: Annual rates (only if in explicit pricing context)
        annual_pattern = r'€\s*\d+(?:[.,]\d+)?\s*(?:per\s+)?jaar'
        match = re.search(annual_pattern, text_lower)
        if match:
            if _has_pricing_context(match.start(), match.end()):
                hint = _extract_exact_text(match.start(), match.end())
                self.logger.info(f"✓ Found example pricing hint: {hint} (in pricing context) | Source: {url}")
                return hint
        
        self.logger.info(f"   No example pricing hint found (requires explicit pricing context: tarief/prijs/kosten) | Source: {url}")
        return None
    
    def fetch_avg_time_to_fill(self, text: str, url: str) -> Optional[int]:
        """
        Extract average time to fill in days from explicit time-to-fill statements.
        
        Rule: Only match explicit statements about average time to fill positions.
        Requires context keywords like "gemiddeld", "gemiddelde tijd", "time to fill", 
        "vullen", "kandidaten", or explicit speed claims.
        
        Do NOT calculate conversions (hours to days, weeks to days) - client feedback: avoid all calculations
        Only extract if explicitly stated in days.
        
        Returns:
        - Number of days (only if explicitly stated in time-to-fill context)
        - None if not mentioned or context is unclear
        """
        self.logger.info(f"🔍 Fetching avg time to fill from {url}")
        
        text_lower = text.lower()
        
        # STRICT PATTERNS: Require explicit time-to-fill context
        # These patterns require context keywords that indicate time-to-fill statements
        strict_patterns = [
            # "gemiddelde tijd om te vullen: binnen X dagen" / "average time to fill: within X days"
            (r'(?:gemiddeld|gemiddelde|average)\s+(?:tijd|time)\s+(?:om\s+te\s+)?(?:vullen|fill|plaatsen|place).*?binnen\s+(\d+)\s+dag', 
             lambda d: int(d)),
            (r'(?:gemiddeld|gemiddelde|average)\s+(?:tijd|time)\s+(?:om\s+te\s+)?(?:vullen|fill|plaatsen|place).*?binnen\s+een\s+dag', 
             lambda: 1),
            
            # "binnen X dagen kandidaten" / "kandidaten binnen X dagen" (explicit speed claim)
            (r'binnen\s+(\d+)\s+dag(?:en)?\s+(?:kandidaten|een\s+voorstel|een\s+kandidaat|kandidaten\s+leveren)', 
             lambda d: int(d)),
            (r'(?:kandidaten|een\s+voorstel|een\s+kandidaat|kandidaten\s+leveren)\s+binnen\s+(\d+)\s+dag(?:en)?', 
             lambda d: int(d)),
            (r'binnen\s+een\s+dag\s+(?:kandidaten|een\s+voorstel|een\s+kandidaat|kandidaten\s+leveren)', 
             lambda: 1),
            
            # "gemiddeld X dagen om te vullen" / "average X days to fill"
            (r'gemiddeld\s+(\d+)\s+dag(?:en)?\s+(?:om\s+te\s+)?(?:vullen|fill|plaatsen|place)', 
             lambda d: int(d)),
            (r'average\s+(\d+)\s+days?\s+(?:to\s+)?(?:fill|place)', 
             lambda d: int(d)),
            
            # "time to fill: X dagen" / "tijd om te vullen: X dagen"
            (r'(?:time\s+to\s+fill|tijd\s+om\s+te\s+vullen|vultijd|fill\s+time)[:\s]+(\d+)\s+dag(?:en)?', 
             lambda d: int(d)),
        ]
        
        for pattern, converter in strict_patterns:
            match = re.search(pattern, text_lower, re.IGNORECASE | re.DOTALL)
            if match:
                # Check context to ensure it's about time-to-fill, not general time mentions
                match_start = match.start()
                context_before = text_lower[max(0, match_start - 150):match_start]
                context_after = text_lower[match.end():min(len(text_lower), match.end() + 150)]
                full_context = context_before + " " + context_after
                
                # Exclude if mentions non-recruitment contexts (e.g., "binnen 2 dagen levering", "binnen 3 dagen antwoord")
                exclude_keywords = [
                    'levering', 'delivery', 'bezorging', 'shipping',
                    'antwoord', 'response', 'reactie', 'reply',
                    'afspraak', 'appointment', 'meeting',
                    'verwerking', 'processing', 'behandeling',
                    'factuur', 'invoice', 'betaling', 'payment',
                ]
                
                # Only exclude if the excluded keyword appears very close to the match (within 50 chars)
                close_context = context_before[-50:] + " " + context_after[:50]
                if any(keyword in close_context for keyword in exclude_keywords):
                    self.logger.info(f"   Skipping: time mention found but in excluded context (delivery/response/etc.) | Source: {url}")
                    continue
                
                # Extract days value
                if callable(converter):
                    days = converter() if len(match.groups()) == 0 else converter(match.group(1))
                else:
                    days = converter
                
                # Validate reasonable range (1-365 days)
                if days and 1 <= days <= 365:
                    self.logger.info(f"✓ Found avg time to fill: {days} days (explicit time-to-fill context: '{match.group(0)[:50]}...') | Source: {url}")
                    return days
                else:
                    self.logger.info(f"   Skipping: time value {days} days is outside reasonable range (1-365) | Source: {url}")
        
        # If no explicit time-to-fill context found, return None
        # Do NOT fall back to generic "binnen X dagen" without recruitment context
        self.logger.info(f"   No avg time to fill found (requires explicit time-to-fill context) | Source: {url}")
        return None
    
    def fetch_candidate_pool_size(self, text: str, url: str) -> Optional[int]:
        """
        Extract candidate pool size estimate from text - ONLY explicit pool mentions.
        
        Rule: Only match explicit statements about candidate pool size, not generic numbers.
        Requires full phrases that explicitly state pool size.
        
        IMPORTANT: Pool size ≠ Database size ≠ Team size ≠ Vacancies ≠ Placements
        - Pool size = active/available candidates ready for placement (EXPLICIT "pool" mention required)
        - Database size = total people they can source from (NOT extracted - excluded)
        - Team size = internal team/employees (NOT extracted - excluded)
        - Vacancies = job openings (NOT extracted - excluded)
        - Placements = successful matches (NOT extracted - excluded)
        
        Returns:
        - Estimated pool size as integer (ONLY if explicitly stated as "pool")
        - None if not explicitly mentioned as pool size
        """
        self.logger.info(f"🔍 Fetching candidate pool size from {url}")
        
        text_lower = text.lower()
        
        # Helper to parse number with thousand separators
        # Only parse if number format is clear - avoid assumptions
        def _parse_number(num_str: str) -> Optional[int]:
            """Parse number string, handling dots/commas as thousand separators or decimals."""
            try:
                # Remove thousand separators (dots/commas used as separators)
                # Only parse if format is unambiguous
                if ',' in num_str and '.' in num_str:
                    # Both present: European format (e.g., "1.234,56" or "1,234.56")
                    # Check which is likely decimal separator based on position
                    if num_str.rindex('.') > num_str.rindex(','):
                        # Dot is last - likely decimal (e.g., "1,234.56")
                        num_str = num_str.replace(',', '')
                    else:
                        # Comma is last - likely decimal (e.g., "1.234,56")
                        num_str = num_str.replace('.', '').replace(',', '.')
                elif ',' in num_str:
                    # Only comma: check if it's decimal or thousand separator
                    parts = num_str.split(',')
                    if len(parts) == 2 and len(parts[1]) <= 2:
                        # Likely decimal (e.g., "1,5")
                        num_str = num_str.replace(',', '.')
                    else:
                        # Likely thousand separator (e.g., "1,234")
                        num_str = num_str.replace(',', '')
                elif '.' in num_str:
                    # Only dot: check if it's decimal or thousand separator
                    parts = num_str.split('.')
                    if len(parts) == 2 and len(parts[1]) <= 2:
                        # Likely decimal (e.g., "1.5")
                        pass  # Keep as is
                    else:
                        # Likely thousand separator (e.g., "1.234")
                        num_str = num_str.replace('.', '')
                
                return int(float(num_str))
            except (ValueError, AttributeError):
                return None
        
        # STRICT PATTERNS: Only match explicit "pool" mentions
        # These are the ONLY patterns that indicate actual candidate pool size
        strict_pool_patterns = [
            # "actieve pool van X kandidaten" / "active pool of X candidates"
            r'(?:actieve|active)\s+pool\s+(?:van|of|with)\s+(\d+[\.,]?\d*)\s+(?:kandidaten|candidates|professionals)',
            # "pool van X kandidaten" / "pool of X candidates"
            r'pool\s+(?:van|of|with)\s+(\d+[\.,]?\d*)\s+(?:kandidaten|candidates|professionals)',
            # "X kandidaten in de pool" / "X candidates in the pool"
            r'(\d+[\.,]?\d*)\s+(?:kandidaten|candidates|professionals)\s+(?:in|in de|in het)\s+(?:de|het)?\s*pool',
            # "onze pool bevat X" / "our pool contains X"
            r'(?:onze|our|de|het)\s+pool\s+(?:bevat|contains|heeft|has)\s+(\d+[\.,]?\d*)\s+(?:kandidaten|candidates|professionals)',
            # "pool met X kandidaten" / "pool with X candidates"
            r'pool\s+met\s+(\d+[\.,]?\d*)\s+(?:kandidaten|candidates|professionals)',
        ]
        
        for pattern in strict_pool_patterns:
            match = re.search(pattern, text_lower)
            if match:
                # Check context to ensure it's not about database, team, vacancies, or placements
                match_start = match.start()
                context_before = text_lower[max(0, match_start - 200):match_start]
                context_after = text_lower[match.end():min(len(text_lower), match.end() + 200)]
                full_context = context_before + " " + context_after
                
                # Exclude if mentions database, team, vacancies, placements, or other non-pool contexts
                exclude_keywords = [
                    'database', 'talentendatabase', 'bestand', 'databank',
                    'team', 'medewerkers', 'employees', 'collega',
                    'vacatures', 'vacancies', 'jobs', 'functies',
                    'plaatsingen', 'placements', 'matches',
                    'klanten', 'clients', 'opdrachtgevers',
                    'vestigingen', 'offices', 'locaties',
                ]
                
                if any(keyword in full_context for keyword in exclude_keywords):
                    self.logger.info(f"   Skipping: pool mention found but in excluded context (database/team/vacancies/etc.) | Source: {url}")
                    continue
                
                    size = _parse_number(match.group(1))
                    if size and size > 0:
                        self.logger.info(f"✓ Found candidate pool size: {size:,} (explicit pool mention: '{match.group(0)}') | Source: {url}")
                        return size
        
        # If no explicit pool mention found, return None
        # Do NOT fall back to "active candidates" or "available candidates" without "pool" keyword
        # This prevents false positives from team size, database size, vacancies, etc.
        self.logger.info(f"   No candidate pool size found (requires explicit 'pool' mention) | Source: {url}")
        return None
    
    def fetch_annual_placements(self, text: str, url: str) -> Optional[int]:
        """
        Extract annual placements estimate from text.
        
        Returns:
        - Estimated annual placements
        - None if not mentioned
        """
        self.logger.info(f"🔍 Fetching annual placements from {url}")
        
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
        self.logger.info(f"🔍 Fetching uses inlenersbeloning from {url}")
        
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
        self.logger.info(f"🔍 Fetching applies inlenersbeloning from day 1 from {url}")
        
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
        self.logger.info(f"🔍 Fetching min assignment duration from {url}")
        
        # Pattern: "minimaal 4 weken"
        # Do NOT calculate conversions (months to weeks) - client feedback: avoid all calculations
        # Only extract if explicitly stated in weeks
        patterns = [
            (r'minim(?:aal|um)\s+(\d+)\s+we+k', lambda w: int(w)),  # weeks (exact, no calculation)
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
        self.logger.info(f"🔍 Fetching min hours per week from {url}")
        
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
        self.logger.info(f"🔍 Fetching avg hourly rate from {url}")
        
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
        self.logger.info(f"🔍 Fetching review rating and count from {url}")
        
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
    
    def fetch_external_review_urls(self, review_sources: List[Dict[str, str] | str]) -> List[str]:
        """
        Extract external review URLs from review_sources list.
        
        Args:
            review_sources: List of strings or dicts with 'platform' and 'url' keys
        
        Returns:
            List of external review URLs
        """
        if not review_sources:
            return []
        
        urls = []
        for source in review_sources:
            # Handle both string format (e.g., "Google") and dict format (e.g., {"platform": "Google", "url": "..."})
            if isinstance(source, dict):
                url = source.get("url")
                if url:
                    urls.append(url)
            # If it's a string, we don't have a URL to extract
            # (just the platform name like "Google")
        
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
        self.logger.info(f"🔍 Fetching takeover policy from {url}")
        
        text_lower = text.lower()
        result = {
            "free_takeover_hours": None,
            "free_takeover_weeks": None,
            "overname_fee_model": "unknown",  # Default to "unknown" if not explicitly found (enum doesn't accept None)
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
        
        # Detect fee model - require explicit context with "overname" or "takeover"
        # Only match if keywords appear near "overname"/"takeover" to avoid false positives
        
        # Pattern: "geen overnamekosten" or "gratis overnemen" (explicit)
        if re.search(r'(?:geen\s+overnamekosten|gratis\s+overnemen|overname\s+(?:is\s+)?gratis|overname\s+zonder\s+kosten)', text_lower):
            result["overname_fee_model"] = "none"
            # Only set hint if explicitly stated
            hint_match = re.search(r'(?:gratis\s+overnemen|overname\s+(?:is\s+)?gratis|overname\s+zonder\s+kosten)(?:\s+na\s+[\w\s]+)?', text_lower)
            if hint_match:
                result["overname_fee_hint"] = hint_match.group(0).strip()
            self.logger.info(f"✓ Found takeover fee model: none | Source: {url}")
        
        # Pattern: "vast bedrag voor overname" or "vaste vergoeding voor overname" (require "overname" context)
        elif re.search(r'(?:vast\s+bedrag|vaste\s+vergoeding)\s+(?:voor\s+)?(?:de\s+)?overname', text_lower):
            result["overname_fee_model"] = "flat_fee"
            # Try to extract the amount - only if explicitly stated
            fee_match = re.search(r'(?:vast\s+bedrag|vaste\s+vergoeding)\s+(?:van\s+)?€\s*(\d+(?:[.,]\d+)?)\s*(?:voor\s+)?(?:de\s+)?overname', text_lower)
            if fee_match:
                amount = fee_match.group(1)
                result["overname_fee_hint"] = f"Vast bedrag van €{amount} voor overname"
            else:
                # Only set generic hint if "vast bedrag voor overname" is explicitly stated
                explicit_match = re.search(r'(vast\s+bedrag|vaste\s+vergoeding)\s+(?:voor\s+)?(?:de\s+)?overname', text_lower)
                if explicit_match:
                    result["overname_fee_hint"] = explicit_match.group(0).strip()
            self.logger.info(f"✓ Found takeover fee model: flat_fee | Source: {url}")
        
        # Pattern: "percentage van salaris voor overname" (require both "percentage", "salaris", AND "overname")
        elif re.search(r'percentage\s+(?:van|of)\s+(?:het\s+)?(?:bruto\s+)?(?:jaar|maand)?salaris\s+(?:voor\s+)?(?:de\s+)?overname', text_lower):
            result["overname_fee_model"] = "percentage_salary"
            # Try to extract percentage - only if explicitly stated
            pct_match = re.search(r'(\d+)%\s*(?:van|of)\s*(?:het\s+)?(?:bruto\s+)?(?:jaar|maand)?salaris\s+(?:voor\s+)?(?:de\s+)?overname', text_lower)
            if pct_match:
                pct = pct_match.group(1)
                result["overname_fee_hint"] = f"{pct}% van salaris voor overname"
            else:
                # Only set hint if explicitly stated
                explicit_match = re.search(r'percentage\s+(?:van|of)\s+(?:het\s+)?(?:bruto\s+)?(?:jaar|maand)?salaris\s+(?:voor\s+)?(?:de\s+)?overname', text_lower)
                if explicit_match:
                    result["overname_fee_hint"] = explicit_match.group(0).strip()
            self.logger.info(f"✓ Found takeover fee model: percentage_salary | Source: {url}")
        
        # Pattern: "schaaltarief voor overname" or "oplopende overnamekosten" (require "overname" context)
        elif re.search(r'(?:schaal(?:tarief|tarieven)|oplopend(?:e)?\s+overnamekosten|overnamekosten\s+(?:in\s+)?schalen)', text_lower):
            result["overname_fee_model"] = "scaled"
            # Only set hint if explicitly stated
            explicit_match = re.search(r'(?:schaal(?:tarief|tarieven)|oplopend(?:e)?\s+overnamekosten|overnamekosten\s+(?:in\s+)?schalen)', text_lower)
            if explicit_match:
                result["overname_fee_hint"] = explicit_match.group(0).strip()
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

