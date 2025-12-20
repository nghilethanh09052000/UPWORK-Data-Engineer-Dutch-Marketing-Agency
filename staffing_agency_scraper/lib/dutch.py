"""
Dutch-specific constants and utilities for staffing agency scraping.

This module contains:
- Postal code to province mapping
- Dutch address parsing utilities
- Dutch phone/email patterns
- Dutch business terminology
"""

from __future__ import annotations


# =============================================================================
# POSTAL CODE TO PROVINCE MAPPING
# =============================================================================
# Dutch postal codes are 4 digits + 2 letters (e.g., 1234 AB)
# The first 2 digits determine the province/region

DUTCH_POSTAL_TO_PROVINCE: dict[str, str] = {
    # Noord-Holland (Amsterdam, Haarlem, etc.)
    "10": "Noord-Holland",
    "11": "Noord-Holland",
    "12": "Noord-Holland",
    "13": "Noord-Holland",
    "14": "Noord-Holland",
    "15": "Noord-Holland",
    "17": "Noord-Holland",
    "18": "Noord-Holland",
    "19": "Noord-Holland",
    
    # Zuid-Holland (Rotterdam, Den Haag, etc.)
    "20": "Zuid-Holland",
    "21": "Zuid-Holland",
    "22": "Zuid-Holland",
    "23": "Zuid-Holland",
    "24": "Zuid-Holland",
    "25": "Zuid-Holland",
    "26": "Zuid-Holland",
    "27": "Zuid-Holland",
    "28": "Zuid-Holland",
    "29": "Zuid-Holland",
    
    # Zuid-Holland (Rotterdam area: 30xx-33xx)
    "30": "Zuid-Holland",
    "31": "Zuid-Holland",
    "32": "Zuid-Holland",
    "33": "Zuid-Holland",
    "34": "Utrecht",
    "35": "Utrecht",
    
    # Flevoland
    "16": "Flevoland",
    "36": "Flevoland",
    "37": "Flevoland",
    "38": "Flevoland",
    "82": "Flevoland",
    
    # Gelderland (Arnhem, Nijmegen, Zaltbommel, etc.)
    "39": "Gelderland",
    "52": "Gelderland",
    "53": "Gelderland",  # Zaltbommel (5301)
    "54": "Gelderland",
    "55": "Gelderland",
    "56": "Gelderland",
    "57": "Gelderland",
    "58": "Gelderland",
    "65": "Gelderland",
    "66": "Gelderland",
    "67": "Gelderland",
    "68": "Gelderland",
    "69": "Gelderland",
    "81": "Gelderland",
    
    # Noord-Brabant (Eindhoven, Tilburg, Den Bosch, etc.)
    "40": "Noord-Brabant",
    "41": "Noord-Brabant",
    "42": "Noord-Brabant",
    "44": "Noord-Brabant",
    "45": "Noord-Brabant",
    "46": "Noord-Brabant",
    "47": "Noord-Brabant",
    "48": "Noord-Brabant",
    "49": "Noord-Brabant",
    "50": "Noord-Brabant",
    "51": "Noord-Brabant",
    
    # Limburg (Maastricht, etc.)
    "43": "Limburg",
    "60": "Limburg",
    "61": "Limburg",
    "62": "Limburg",
    "63": "Limburg",
    "64": "Limburg",
    
    # Overijssel (Zwolle, Enschede, etc.)
    "59": "Overijssel",
    "70": "Overijssel",
    "71": "Overijssel",
    "72": "Overijssel",
    "73": "Overijssel",
    "74": "Overijssel",
    "75": "Overijssel",
    "76": "Overijssel",
    "80": "Overijssel",
    
    # Drenthe (Assen, Emmen, etc.)
    "77": "Drenthe",
    "78": "Drenthe",
    "79": "Drenthe",
    "98": "Drenthe",
    "99": "Drenthe",
    
    # Friesland (Leeuwarden, etc.)
    "83": "Friesland",
    "84": "Friesland",
    "85": "Friesland",
    "86": "Friesland",
    "87": "Friesland",
    "88": "Friesland",
    "89": "Friesland",
    
    # Groningen
    "90": "Groningen",
    "91": "Groningen",
    "92": "Groningen",
    "93": "Groningen",
    "94": "Groningen",
    "95": "Groningen",
    "96": "Groningen",
    "97": "Groningen",
}


# =============================================================================
# DUTCH PROVINCES LIST
# =============================================================================

DUTCH_PROVINCES = [
    "Noord-Holland",
    "Zuid-Holland", 
    "Utrecht",
    "Flevoland",
    "Gelderland",
    "Noord-Brabant",
    "Limburg",
    "Overijssel",
    "Drenthe",
    "Friesland",
    "Groningen",
    "Zeeland",
]


# =============================================================================
# MAJOR DUTCH CITIES
# =============================================================================

MAJOR_DUTCH_CITIES = [
    "amsterdam",
    "rotterdam",
    "den haag",
    "'s-gravenhage",
    "utrecht",
    "eindhoven",
    "tilburg",
    "groningen",
    "almere",
    "breda",
    "nijmegen",
    "enschede",
    "haarlem",
    "arnhem",
    "zaanstad",
    "haarlemmermeer",
    "amersfoort",
    "apeldoorn",
    "den bosch",
    "'s-hertogenbosch",
    "zwolle",
    "maastricht",
    "leiden",
    "dordrecht",
    "zoetermeer",
    "deventer",
    "delft",
    "alkmaar",
    "heerlen",
    "venlo",
    "leeuwarden",
    "hilversum",
    "assen",
    "middelburg",
]


# =============================================================================
# NON-CITY WORDS (to filter out from address parsing)
# =============================================================================

NON_CITY_WORDS = {
    "nederland",
    "netherlands",
    "the",
    "en",
    "van",
    "de",
    "het",
    "te",
    "aan",
    "bij",
    "voor",
    "met",
    "door",
    "uit",
    "naar",
    "tot",
    "bv",
    "b.v.",
    "nv",
    "n.v.",
    "holding",
    "group",
    "groep",
}


# =============================================================================
# STAFFING INDUSTRY TERMINOLOGY (Dutch)
# =============================================================================

# Service types
SERVICE_KEYWORDS = {
    "uitzenden": ["uitzenden", "uitzendwerk", "uitzendbureau", "flexibel personeel", "tijdelijke krachten"],
    "detacheren": ["detacheren", "detachering", "gedetacheerd"],
    "werving_selectie": ["werving en selectie", "werving & selectie", "recruitment", "werving selectie"],
    "payrolling": ["payroll", "payrolling"],
    "zzp_bemiddeling": ["zzp bemiddeling", "freelance bemiddeling", "zzp", "zelfstandige"],
    "inhouse_services": ["inhouse", "in-house", "on-site"],
    "msp": ["managed service provider", "msp"],
    "rpo": ["recruitment process outsourcing", "rpo"],
    "executive_search": ["executive search", "headhunting"],
    "opleiden_ontwikkelen": ["opleiden", "ontwikkelen", "training", "academy", "opleiding"],
    "reintegratie_outplacement": ["reïntegratie", "re-integratie", "outplacement"],
}

# Sector/industry names
SECTOR_KEYWORDS = {
    "logistiek": ["logistiek", "warehouse", "magazijn", "transport", "distributie"],
    "productie": ["productie", "manufacturing", "fabricage", "industrie"],
    "techniek": ["techniek", "technisch", "engineering", "constructie", "bouw"],
    "ict": ["it", "ict", "software", "digital", "tech"],
    "zorg": ["zorg", "medisch", "healthcare", "verpleging", "gezondheidszorg"],
    "finance": ["finance", "financieel", "accounting", "boekhouding"],
    "hr": ["hr", "human resources", "personeelszaken"],
    "sales": ["sales", "commercieel", "verkoop"],
    "administratief": ["administratief", "administratie", "office"],
    "horeca": ["horeca", "hospitality", "catering"],
    "retail": ["retail", "winkel", "detailhandel"],
    "callcenter": ["callcenter", "klantenservice", "customer service"],
}

# Certifications
CERTIFICATION_KEYWORDS = {
    "NEN-4400-1": ["nen-4400", "nen 4400", "nen4400"],
    "SNA": ["sna-keurmerk", "sna keurmerk", "stichting normering arbeid"],
    "VCU": ["vcu", "vcu*", "vcu**"],
    "ISO9001": ["iso 9001", "iso9001"],
    "ISO27001": ["iso 27001", "iso27001"],
    "ISO45001": ["iso 45001", "iso45001"],
    "PSO": ["pso", "prestatieladder socialer ondernemen"],
}

# CAO (Collective Labor Agreement) types
CAO_KEYWORDS = {
    "ABU": ["abu", "algemene bond uitzendondernemingen"],
    "NBBU": ["nbbu", "nederlandse bond van bemiddelings- en uitzendondernemingen"],
}


# =============================================================================
# CITY TO PROVINCE MAPPING
# =============================================================================
# Direct mapping for major cities to their provinces
# Used when postal code is not available

CITY_TO_PROVINCE: dict[str, str] = {
    # Noord-Holland
    "amsterdam": "Noord-Holland",
    "haarlem": "Noord-Holland",
    "zaandam": "Noord-Holland",
    "alkmaar": "Noord-Holland",
    "hilversum": "Noord-Holland",
    "hoofddorp": "Noord-Holland",
    
    # Zuid-Holland
    "rotterdam": "Zuid-Holland",
    "den haag": "Zuid-Holland",
    "the hague": "Zuid-Holland",
    "'s-gravenhage": "Zuid-Holland",
    "leiden": "Zuid-Holland",
    "dordrecht": "Zuid-Holland",
    "zoetermeer": "Zuid-Holland",
    "delft": "Zuid-Holland",
    "gouda": "Zuid-Holland",
    
    # Utrecht
    "utrecht": "Utrecht",
    "amersfoort": "Utrecht",
    "nieuwegein": "Utrecht",
    "zeist": "Utrecht",
    
    # Noord-Brabant
    "eindhoven": "Noord-Brabant",
    "tilburg": "Noord-Brabant",
    "breda": "Noord-Brabant",
    "den bosch": "Noord-Brabant",
    "'s-hertogenbosch": "Noord-Brabant",
    "helmond": "Noord-Brabant",
    "oss": "Noord-Brabant",
    "cuijk": "Noord-Brabant",
    "waalwijk": "Noord-Brabant",
    "boxmeer": "Noord-Brabant",
    "roosendaal": "Noord-Brabant",
    "bergen op zoom": "Noord-Brabant",
    "oosterhout": "Noord-Brabant",
    
    # Gelderland
    "arnhem": "Gelderland",
    "nijmegen": "Gelderland",
    "apeldoorn": "Gelderland",
    "ede": "Gelderland",
    "zaltbommel": "Gelderland",
    "doetinchem": "Gelderland",
    "varsseveld": "Gelderland",
    "duiven": "Gelderland",
    "zevenaar": "Gelderland",
    "wageningen": "Gelderland",
    "veenendaal": "Gelderland",
    "geldermalsen": "Gelderland",
    
    # Limburg
    "maastricht": "Limburg",
    "venlo": "Limburg",
    "heerlen": "Limburg",
    "sittard": "Limburg",
    "roermond": "Limburg",
    "weert": "Limburg",
    "wijlre": "Limburg",
    "kerkrade": "Limburg",
    "geleen": "Limburg",
    "venray": "Limburg",
    "horst": "Limburg",
    "sevenum": "Limburg",
    "echt": "Limburg",
    
    # Overijssel
    "zwolle": "Overijssel",
    "enschede": "Overijssel",
    "deventer": "Overijssel",
    "hengelo": "Overijssel",
    "almelo": "Overijssel",
    "kampen": "Overijssel",
    "ijsselmuiden": "Overijssel",
    "hardenberg": "Overijssel",
    
    # Groningen
    "groningen": "Groningen",
    "oude pekela": "Groningen",
    
    # Friesland
    "leeuwarden": "Friesland",
    "drachten": "Friesland",
    "heerenveen": "Friesland",
    "sneek": "Friesland",
    "joure": "Friesland",
    
    # Drenthe
    "assen": "Drenthe",
    "emmen": "Drenthe",
    "coevorden": "Drenthe",
    "beilen": "Drenthe",
    
    # Flevoland
    "almere": "Flevoland",
    "lelystad": "Flevoland",
    
    # Zeeland
    "middelburg": "Zeeland",
    "vlissingen": "Zeeland",
    "goes": "Zeeland",
}


# =============================================================================
# URL SLUG MAPPINGS FOR SCRAPING
# =============================================================================

# City slugs commonly used in URLs (e.g., /vacatures/amsterdam)
CITY_SLUGS: set[str] = {
    "amsterdam", "rotterdam", "den-haag", "utrecht", "eindhoven",
    "tilburg", "groningen", "arnhem", "den-bosch", "maastricht",
    "nijmegen", "enschede", "zwolle", "venlo", "almere", "breda",
    "leiden", "amersfoort", "apeldoorn", "haarlem", "leeuwarden",
    "deventer", "dordrecht", "zoetermeer", "emmen", "helmond",
    "heerlen", "oss", "alkmaar", "delft", "hilversum", "hoofddorp",
}

# Sector slugs to standardized sector names
SECTOR_SLUG_TO_NAME: dict[str, str] = {
    "administratief": "administratief",
    "callcenter": "callcenter",
    "klantenservice": "callcenter",
    "customer-service": "callcenter",
    "commercieel": "sales",
    "commercieel-en-marketing": "sales",
    "sales": "sales",
    "verkoop": "sales",
    "horeca": "horeca",
    "hospitality": "horeca",
    "catering": "horeca",
    "transport-en-logistiek": "logistiek",
    "logistiek": "logistiek",
    "warehouse": "logistiek",
    "magazijn": "logistiek",
    "productie": "productie",
    "manufacturing": "productie",
    "industrie": "productie",
    "techniek": "techniek",
    "technical": "techniek",
    "engineering": "techniek",
    "it": "ict",
    "ict": "ict",
    "software": "ict",
    "digital": "ict",
    "financieel": "finance",
    "finance": "finance",
    "accounting": "finance",
    "boekhouding": "finance",
    "juridisch": "juridisch",
    "legal": "juridisch",
    "medisch": "zorg",
    "zorg": "zorg",
    "healthcare": "zorg",
    "verpleging": "zorg",
    "secretarieel": "secretarieel",
    "hr": "hr",
    "human-resources": "hr",
    "personeelszaken": "hr",
    "verzekeringen": "verzekeringen",
    "insurance": "verzekeringen",
    "bouw": "bouw",
    "construction": "bouw",
    "retail": "retail",
    "winkel": "retail",
    "overheid": "overheid",
    "government": "overheid",
    "onderwijs": "onderwijs",
    "education": "onderwijs",
}


def get_province_for_city(city: str) -> str | None:
    """
    Get Dutch province for a city name.
    
    Parameters
    ----------
    city : str
        City name (case-insensitive)
    
    Returns
    -------
    str | None
        Province name or None if not found
    """
    if not city:
        return None
    
    # Normalize city name
    city_lower = city.lower().strip()
    
    # Handle URL slug format (den-haag -> den haag)
    city_normalized = city_lower.replace("-", " ")
    
    # Direct lookup
    if city_normalized in CITY_TO_PROVINCE:
        return CITY_TO_PROVINCE[city_normalized]
    
    # Try original format
    if city_lower in CITY_TO_PROVINCE:
        return CITY_TO_PROVINCE[city_lower]
    
    return None


def normalize_sector_slug(slug: str) -> str | None:
    """
    Convert a URL slug to standardized sector name.
    
    Parameters
    ----------
    slug : str
        URL slug (e.g., "transport-en-logistiek")
    
    Returns
    -------
    str | None
        Standardized sector name or None if not recognized
    """
    if not slug:
        return None
    
    slug_lower = slug.lower().strip()
    return SECTOR_SLUG_TO_NAME.get(slug_lower)


def is_city_slug(slug: str) -> bool:
    """
    Check if a URL slug represents a city.
    
    Parameters
    ----------
    slug : str
        URL slug to check
    
    Returns
    -------
    bool
        True if slug is a known city
    """
    return slug.lower().strip() in CITY_SLUGS


def get_province_from_postal_code(postal_code: str) -> str | None:
    """
    Get Dutch province from postal code.
    
    Parameters
    ----------
    postal_code : str
        Dutch postal code (4 digits, e.g., "5301" or full "5301 LL")
    
    Returns
    -------
    str | None
        Province name or None if not found
    """
    # Extract first 4 digits
    import re
    match = re.search(r"(\d{4})", postal_code)
    if not match:
        return None
    
    digits = match.group(1)
    prefix = digits[:2]
    
    return DUTCH_POSTAL_TO_PROVINCE.get(prefix)


def is_valid_dutch_city(city: str) -> bool:
    """
    Check if a string is likely a valid Dutch city name.
    
    Parameters
    ----------
    city : str
        Potential city name
    
    Returns
    -------
    bool
        True if likely a city name
    """
    if not city:
        return False
    
    city_lower = city.lower().strip()
    
    # Check if it's a non-city word
    if city_lower in NON_CITY_WORDS:
        return False
    
    # Must be at least 2 characters
    if len(city_lower) < 2:
        return False
    
    # Should only contain letters, spaces, hyphens, and apostrophes
    import re
    if not re.match(r"^[a-zA-Z\s\-']+$", city):
        return False
    
    return True

