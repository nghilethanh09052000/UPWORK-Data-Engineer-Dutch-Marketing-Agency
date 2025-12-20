"""
Data normalization utilities for standardizing scraped data.

Maps raw text to standardized enum values for the schema.
"""

from __future__ import annotations

import re


# Dutch province mappings
PROVINCE_MAPPINGS = {
    # Standard names
    "noord-holland": "Noord-Holland",
    "zuid-holland": "Zuid-Holland",
    "noord-brabant": "Noord-Brabant",
    "gelderland": "Gelderland",
    "overijssel": "Overijssel",
    "limburg": "Limburg",
    "flevoland": "Flevoland",
    "utrecht": "Utrecht",
    "groningen": "Groningen",
    "friesland": "Friesland",
    "drenthe": "Drenthe",
    "zeeland": "Zeeland",
    # Variations
    "nh": "Noord-Holland",
    "zh": "Zuid-Holland",
    "nb": "Noord-Brabant",
    "noord brabant": "Noord-Brabant",
    "noord holland": "Noord-Holland",
    "zuid holland": "Zuid-Holland",
}


# Sector mappings (Dutch to normalized)
SECTOR_MAPPINGS = {
    # Logistics
    "logistiek": "logistiek",
    "transport": "logistiek",
    "warehousing": "logistiek",
    "magazijn": "logistiek",
    "supply chain": "logistiek",
    # Production
    "productie": "productie",
    "industrie": "productie",
    "manufacturing": "productie",
    "fabriek": "productie",
    # Healthcare
    "zorg": "zorg",
    "gezondheidszorg": "zorg",
    "healthcare": "zorg",
    "verpleging": "zorg",
    "verzorging": "zorg",
    # IT
    "ict": "ict",
    "it": "ict",
    "tech": "ict",
    "software": "ict",
    "development": "ict",
    # Finance
    "finance": "finance",
    "financieel": "finance",
    "banking": "finance",
    "accountancy": "finance",
    # Retail
    "retail": "retail",
    "winkel": "retail",
    "verkoop": "retail",
    # Hospitality
    "horeca": "horeca",
    "hospitality": "horeca",
    "catering": "horeca",
    # Technical
    "techniek": "techniek",
    "technical": "techniek",
    "engineering": "techniek",
    # Construction
    "bouw": "bouw",
    "construction": "bouw",
    # Office
    "administratief": "administratief",
    "kantoor": "administratief",
    "office": "administratief",
    # Education
    "onderwijs": "onderwijs",
    "education": "onderwijs",
    # Legal
    "juridisch": "juridisch",
    "legal": "juridisch",
}


# Service type mappings
SERVICE_MAPPINGS = {
    "uitzenden": ["uitzenden", "uitzendwerk", "uitzendbureau", "temporary staffing"],
    "detacheren": ["detacheren", "detachering", "secondment"],
    "werving_selectie": ["werving", "selectie", "recruitment", "werving & selectie"],
    "payrolling": ["payroll", "payrolling"],
    "zzp_bemiddeling": ["zzp", "freelance", "zelfstandig"],
    "inhouse_services": ["inhouse", "on-site", "in-house"],
    "msp": ["msp", "managed service"],
    "rpo": ["rpo", "recruitment process outsourcing"],
    "executive_search": ["executive search", "headhunting", "executive"],
    "opleiden_ontwikkelen": ["opleiding", "training", "ontwikkeling", "academy"],
    "reintegratie_outplacement": ["reintegratie", "outplacement", "re-integratie"],
}


# Certification mappings
CERTIFICATION_PATTERNS = {
    "NEN-4400-1": [r"nen[-\s]?4400[-\s]?1", r"nen4400"],
    "NEN-4400-2": [r"nen[-\s]?4400[-\s]?2"],
    "SNA": [r"\bsna\b", r"stichting normering arbeid"],
    "VCU": [r"\bvcu\b", r"veiligheid[-\s]?checklist"],
    "VCA": [r"\bvca\b", r"veiligheid certificaat aannemers"],
    "ISO9001": [r"iso[-\s]?9001"],
    "ISO27001": [r"iso[-\s]?27001"],
    "MVO": [r"\bmvo\b", r"maatschappelijk verantwoord"],
    "PSO": [r"\bpso\b", r"prestatieladder"],
}


# CAO mappings
CAO_PATTERNS = {
    "ABU": [r"\babu\b", r"abu[-\s]?cao"],
    "NBBU": [r"\bnbbu\b", r"nbbu[-\s]?cao"],
}


def normalize_province(text: str) -> str | None:
    """
    Normalize a province name to standard format.

    Parameters
    ----------
    text : str
        Raw province text

    Returns
    -------
    str | None
        Normalized province name or None
    """
    key = text.lower().strip()
    return PROVINCE_MAPPINGS.get(key)


def normalize_sector(text: str) -> str | None:
    """
    Normalize a sector name to standard format.

    Parameters
    ----------
    text : str
        Raw sector text

    Returns
    -------
    str | None
        Normalized sector name or None
    """
    key = text.lower().strip()
    return SECTOR_MAPPINGS.get(key)


def extract_sectors_from_text(text: str) -> list[str]:
    """
    Extract all matching sectors from a text.

    Parameters
    ----------
    text : str
        Text to search

    Returns
    -------
    list[str]
        List of normalized sector names
    """
    text_lower = text.lower()
    sectors = set()
    for keyword, normalized in SECTOR_MAPPINGS.items():
        if keyword in text_lower:
            sectors.add(normalized)
    return sorted(sectors)


# Comprehensive sector normalization mapping
# Maps all variations to controlled vocabulary
SECTOR_NORMALIZATION = {
    # IT/ICT variations -> ict
    "it": "ict",
    "ict": "ict",
    "ict & it": "ict",
    "it & ict": "ict",
    "ict/it": "ict",
    "it/ict": "ict",
    "digital": "ict",
    "digital enablement": "ict",
    "software": "ict",
    "tech": "ict",
    "technology": "ict",
    "development": "ict",
    "data": "ict",
    "cloud": "ict",
    "cyber": "ict",
    
    # Finance variations -> finance
    "finance": "finance",
    "financieel": "finance",
    "financial": "finance",
    "banking": "finance",
    "accountancy": "finance",
    "accounting": "finance",
    "boekhouding": "finance",
    "treasury": "finance",
    
    # Marketing variations -> marketing
    "marketing": "marketing",
    "commercieel": "marketing",
    "commercial": "marketing",
    "sales": "marketing",
    "communicatie": "marketing",
    "communication": "marketing",
    "marketing en communicatie": "marketing",
    "pr": "marketing",
    
    # Logistics variations -> logistiek
    "logistiek": "logistiek",
    "logistics": "logistiek",
    "transport": "logistiek",
    "warehousing": "logistiek",
    "magazijn": "logistiek",
    "supply chain": "logistiek",
    "supplychain": "logistiek",
    
    # Hospitality variations -> horeca
    "horeca": "horeca",
    "horeca & catering": "horeca",
    "hospitality": "horeca",
    "catering": "horeca",
    "restaurant": "horeca",
    "hotel": "horeca",
    
    # Public sector variations -> publieke_sector
    "overheid": "publieke_sector",
    "publieke sector": "publieke_sector",
    "publieke_sector": "publieke_sector",
    "government": "publieke_sector",
    "gemeente": "publieke_sector",
    "rijk": "publieke_sector",
    
    # Customer service variations -> callcenter
    "customer service": "callcenter",
    "klantenservice": "callcenter",
    "callcenter": "callcenter",
    "call center": "callcenter",
    "contactcenter": "callcenter",
    "contact center": "callcenter",
    
    # Office/Admin variations -> office
    "administratief": "office",
    "kantoor": "office",
    "office": "office",
    "secretarieel": "office",
    "secretaresses": "office",
    "backoffice": "office",
    "administratie": "office",
    
    # Production variations -> productie
    "productie": "productie",
    "production": "productie",
    "industrie": "productie",
    "industry": "productie",
    "manufacturing": "productie",
    "fabriek": "productie",
    
    # Healthcare variations -> zorg
    "zorg": "zorg",
    "healthcare": "zorg",
    "gezondheidszorg": "zorg",
    "verpleging": "zorg",
    "verzorging": "zorg",
    "care": "zorg",
    "ggz": "zorg",
    "thuiszorg": "zorg",
    
    # Technical variations -> techniek
    "techniek": "techniek",
    "technical": "techniek",
    "engineering": "techniek",
    "installatie": "techniek",
    "montage": "techniek",
    
    # Construction variations -> bouw
    "bouw": "bouw",
    "construction": "bouw",
    "aannemer": "bouw",
    "infrastructuur": "bouw",
    "infrastractuur": "bouw",
    
    # HR variations -> hr
    "hr": "hr",
    "human resources": "hr",
    "recruitment": "hr",
    "p&o": "hr",
    "hrm": "hr",
    
    # Legal variations -> legal
    "legal": "legal",
    "juridisch": "legal",
    "recht": "legal",
    "advocatuur": "legal",
    
    # Education variations -> onderwijs
    "onderwijs": "onderwijs",
    "education": "onderwijs",
    "leraar": "onderwijs",
    "docent": "onderwijs",
    "kinderopvang": "onderwijs",
    
    # Retail variations -> retail
    "retail": "retail",
    "winkel": "retail",
    "verkoop": "retail",
    "winkelier": "retail",
    
    # Other standard sectors (keep as-is if already normalized)
    "automotive": "automotive",
    "beveiliging": "beveiliging",
    "chemie": "chemie",
    "consulting": "consulting",
    "energie": "energie",
    "food": "food",
    "agri": "agri",
    "high_tech": "high_tech",
    "life_science": "life_science",
    "media": "media",
    "non_profit": "non_profit",
    "pharma": "pharma",
    "schoonmaak": "schoonmaak",
    "telecom": "telecom",
    "bouw_infra": "bouw_infra",
    "it_telecom": "it_telecom",
}


# Valid normalized sectors (controlled vocabulary)
VALID_SECTORS = set(SECTOR_NORMALIZATION.values())


def normalize_sectors(sectors: list[str]) -> list[str]:
    """
    Normalize sector names to use controlled vocabulary.
    
    Maps variations like "IT", "ICT", "digital" -> "ict"
    Maps "commercieel" -> "marketing"
    Maps "customer service" -> "callcenter"
    etc.
    
    Parameters
    ----------
    sectors : list[str]
        Raw list of sector names from scraping
        
    Returns
    -------
    list[str]
        Normalized list of sectors using controlled vocabulary
        
    Example
    -------
    >>> normalize_sectors(["IT", "ICT & IT", "digital", "commercieel"])
    ['ict', 'marketing']
    >>> normalize_sectors(["customer service", "callcenter", "klantenservice"])
    ['callcenter']
    """
    if not sectors:
        return []
    
    normalized = set()
    
    for sector in sectors:
        if not sector or not isinstance(sector, str):
            continue
        
        sector_clean = sector.strip()
        if not sector_clean:
            continue
        
        sector_lower = sector_clean.lower()
        
        # Try direct mapping first (exact match)
        if sector_lower in SECTOR_NORMALIZATION:
            normalized.add(SECTOR_NORMALIZATION[sector_lower])
            continue
        
        # Check if it's already a valid normalized sector
        if sector_lower in VALID_SECTORS:
            normalized.add(sector_lower)
            continue
        
        # Try to find partial matches (e.g., "ICT & IT" contains "ict")
        # Sort by key length (longest first) to match more specific patterns first
        matched = False
        for key, value in sorted(SECTOR_NORMALIZATION.items(), key=lambda x: len(x[0]), reverse=True):
            if key in sector_lower:
                normalized.add(value)
                matched = True
                break
        
        # If no match found, skip this sector (don't include invalid sectors)
        if not matched:
            continue
    
    # Return sorted list for consistency
    return sorted(list(normalized))


# Focus segments normalization
FOCUS_SEGMENTS_NORMALIZATION = {
    # Student variations
    "studenten": "studenten",
    "student": "studenten",
    "scholier": "studenten",
    "bijbaan": "studenten",
    
    # Young professionals
    "young_professionals": "young_professionals",
    "young professional": "young_professionals",
    "starter": "young_professionals",
    "jong talent": "young_professionals",
    
    # Blue collar
    "blue_collar": "blue_collar",
    "blue collar": "blue_collar",
    "productie": "blue_collar",
    "logistiek": "blue_collar",
    "bouw": "blue_collar",
    "magazijn": "blue_collar",
    "technisch": "blue_collar",
    
    # White collar
    "white_collar": "white_collar",
    "white collar": "white_collar",
    "kantoor": "white_collar",
    "administratief": "white_collar",
    "office": "white_collar",
    "finance": "white_collar",
    
    # Technical specialists
    "technisch_specialisten": "technisch_specialisten",
    "technisch specialist": "technisch_specialisten",
    "engineer": "technisch_specialisten",
    "it specialist": "technisch_specialisten",
    "ict specialist": "technisch_specialisten",
    
    # Healthcare professionals
    "zorgprofessionals": "zorgprofessionals",
    "zorg professional": "zorgprofessionals",
    "zorg": "zorgprofessionals",
    "verpleeg": "zorgprofessionals",
    "verzorg": "zorgprofessionals",
}


# Valid normalized focus segments
VALID_FOCUS_SEGMENTS = set(FOCUS_SEGMENTS_NORMALIZATION.values())


def normalize_focus_segments(segments: list[str]) -> list[str]:
    """
    Normalize focus segment names to use controlled vocabulary.
    
    Parameters
    ----------
    segments : list[str]
        Raw list of focus segment names from scraping
        
    Returns
    -------
    list[str]
        Normalized list of focus segments using controlled vocabulary
    """
    if not segments:
        return []
    
    normalized = set()
    
    for segment in segments:
        if not segment or not isinstance(segment, str):
            continue
        
        segment_clean = segment.strip()
        segment_lower = segment_clean.lower()
        
        # Try direct mapping
        if segment_lower in FOCUS_SEGMENTS_NORMALIZATION:
            normalized.add(FOCUS_SEGMENTS_NORMALIZATION[segment_lower])
            continue
        
        # Check if it's already a valid normalized segment
        if segment_lower in VALID_FOCUS_SEGMENTS:
            normalized.add(segment_lower)
            continue
        
        # Try to find partial matches
        for key, value in FOCUS_SEGMENTS_NORMALIZATION.items():
            if key in segment_lower:
                normalized.add(value)
                break
    
    # Return sorted list for consistency
    return sorted(list(normalized))


def detect_services(text: str) -> dict[str, bool | None]:
    """
    Detect which services are mentioned in text.

    Parameters
    ----------
    text : str
        Text to search

    Returns
    -------
    dict[str, bool | None]
        Dictionary of service types with True if found, None if not found
        (following client feedback: unknown = null, not False)
    """
    text_lower = text.lower()
    services = {}
    for service_key, patterns in SERVICE_MAPPINGS.items():
        found = any(p in text_lower for p in patterns)
        services[service_key] = True if found else None
    return services


def detect_certifications(text: str) -> list[str]:
    """
    Detect certifications mentioned in text.

    Parameters
    ----------
    text : str
        Text to search

    Returns
    -------
    list[str]
        List of detected certification codes
    """
    certifications = []
    for cert_name, patterns in CERTIFICATION_PATTERNS.items():
        for pattern in patterns:
            if re.search(pattern, text, re.IGNORECASE):
                certifications.append(cert_name)
                break
    return certifications


def detect_cao_type(text: str) -> str:
    """
    Detect CAO type from text.

    Parameters
    ----------
    text : str
        Text to search

    Returns
    -------
    str
        CAO type: "ABU", "NBBU", "eigen_cao", or "onbekend"
    """
    for cao_type, patterns in CAO_PATTERNS.items():
        for pattern in patterns:
            if re.search(pattern, text, re.IGNORECASE):
                return cao_type
    return "onbekend"


def normalize_geo_focus(text: str) -> str:
    """
    Determine geographic focus from text.

    Parameters
    ----------
    text : str
        Text to analyze

    Returns
    -------
    str
        One of: "local", "regional", "national", "international"
    """
    text_lower = text.lower()

    if any(
        w in text_lower
        for w in ["internationaal", "europa", "worldwide", "global", "belgium"]
    ):
        return "international"
    if any(w in text_lower for w in ["landelijk", "nederland", "heel nederland"]):
        return "national"
    if any(w in text_lower for w in ["regio", "regionaal"]):
        return "regional"
    return "local"


def detect_focus_segments(text: str) -> list[str]:
    """
    Detect focus segments (target worker types) from text.

    Parameters
    ----------
    text : str
        Text to analyze

    Returns
    -------
    list[str]
        List of focus segments
    """
    segments = []
    text_lower = text.lower()

    segment_keywords = {
        "studenten": ["student", "scholier", "bijbaan"],
        "young_professionals": ["young professional", "starter", "jong talent"],
        "blue_collar": ["productie", "logistiek", "bouw", "magazijn", "technisch"],
        "white_collar": ["kantoor", "administratief", "office", "finance"],
        "technisch_specialisten": ["engineer", "technisch specialist", "it specialist"],
        "zorgprofessionals": ["zorg", "verpleeg", "verzorg"],
    }

    for segment, keywords in segment_keywords.items():
        if any(kw in text_lower for kw in keywords):
            segments.append(segment)

    return segments


# Valid province names (normalized)
VALID_PROVINCES = {
    "Noord-Holland",
    "Zuid-Holland",
    "Noord-Brabant",
    "Gelderland",
    "Overijssel",
    "Limburg",
    "Flevoland",
    "Utrecht",
    "Groningen",
    "Friesland",
    "Drenthe",
    "Zeeland",
}

# Valid regional groupings
VALID_REGIONAL_GROUPS = {
    "Randstad",
}

# Valid national coverage label
VALID_NATIONAL = "landelijk"


def normalize_regions_served(regions: list[str]) -> list[str]:
    """
    Normalize regions_served to use only controlled, normalized labels.
    
    Rules:
    - Use only: "landelijk", "Randstad", or province-level labels
    - Convert "heel_Nederland" to "landelijk"
    - Filter out international countries and other regional groupings
    - Normalize province names to standard format
    
    Parameters
    ----------
    regions : list[str]
        Raw list of regions from scraping
        
    Returns
    -------
    list[str]
        Normalized list of regions using only controlled labels
        
    Example
    -------
    >>> normalize_regions_served(["heel_Nederland", "Utrecht", "Randstad", "België"])
    ['landelijk', 'Utrecht', 'Randstad']
    >>> normalize_regions_served(["noord-holland", "Zuid-Holland", "Groningen"])
    ['Noord-Holland', 'Zuid-Holland', 'Groningen']
    """
    normalized = []
    
    for region in regions:
        region_clean = region.strip()
        
        # Convert "heel_Nederland" to "landelijk"
        if region_clean.lower() in ["heel_nederland", "heel nederland", "geheel nederland"]:
            if VALID_NATIONAL not in normalized:
                normalized.append(VALID_NATIONAL)
            continue
        
        # Check if it's already "landelijk"
        if region_clean.lower() == "landelijk":
            if VALID_NATIONAL not in normalized:
                normalized.append(VALID_NATIONAL)
            continue
        
        # Check if it's "Randstad"
        if region_clean == "Randstad" or region_clean.lower() == "randstad":
            if "Randstad" not in normalized:
                normalized.append("Randstad")
            continue
        
        # Try to normalize as province
        normalized_province = normalize_province(region_clean)
        if normalized_province and normalized_province in VALID_PROVINCES:
            if normalized_province not in normalized:
                normalized.append(normalized_province)
            continue
        
        # Check if it's already a valid province name
        if region_clean in VALID_PROVINCES:
            if region_clean not in normalized:
                normalized.append(region_clean)
            continue
    
    # Sort for consistency
    # Priority: landelijk first, then Randstad, then provinces alphabetically
    result = []
    if VALID_NATIONAL in normalized:
        result.append(VALID_NATIONAL)
    if "Randstad" in normalized:
        result.append("Randstad")
    
    # Add provinces in sorted order
    provinces = sorted([r for r in normalized if r in VALID_PROVINCES])
    result.extend(provinces)
    
    return result


# Certification normalization mapping
# Maps all variations to standardized format (space-separated, e.g., "ISO 9001")
CERTIFICATION_NORMALIZATION = {
    # ISO 9001 variations
    "iso_9001": "ISO 9001",
    "iso-9001": "ISO 9001",
    "iso9001": "ISO 9001",
    "iso 9001": "ISO 9001",
    "ISO_9001": "ISO 9001",
    "ISO-9001": "ISO 9001",
    "ISO9001": "ISO 9001",
    "ISO 9001": "ISO 9001",
    
    # ISO 14001 variations
    "iso_14001": "ISO 14001",
    "iso-14001": "ISO 14001",
    "iso14001": "ISO 14001",
    "iso 14001": "ISO 14001",
    "ISO_14001": "ISO 14001",
    "ISO-14001": "ISO 14001",
    "ISO14001": "ISO 14001",
    "ISO 14001": "ISO 14001",
    
    # ISO 27001 variations
    "iso_27001": "ISO 27001",
    "iso-27001": "ISO 27001",
    "iso27001": "ISO 27001",
    "iso 27001": "ISO 27001",
    "ISO_27001": "ISO 27001",
    "ISO-27001": "ISO 27001",
    "ISO27001": "ISO 27001",
    "ISO 27001": "ISO 27001",
    
    # ISO/IEC 27001 variations
    "iso/iec 27001": "ISO 27001",
    "iso/iec-27001": "ISO 27001",
    "iso/iec_27001": "ISO 27001",
    "ISO/IEC 27001": "ISO 27001",
    "ISO/IEC-27001": "ISO 27001",
    "ISO/IEC_27001": "ISO 27001",
    
    # Other common certifications (keep as-is if already normalized, normalize variations)
    "sna": "SNA",
    "SNA": "SNA",
    "nba": "NBA",
    "NBA": "NBA",
    "vcu": "VCU",
    "VCU": "VCU",
    "vcr": "VCR",
    "VCR": "VCR",
    "sri": "SRI",
    "SRI": "SRI",
    "psom": "PSOM",
    "PSOM": "PSOM",
    "psom": "PSOM",
    "PSO": "PSO",
    "pso": "PSO",
    "kiwa": "Kiwa",
    "Kiwa": "Kiwa",
    "KIWA": "Kiwa",
    "abu": "ABU",
    "ABU": "ABU",
    "nfv": "NFV",
    "NFV": "NFV",
    "nen-4400-1": "NEN-4400-1",
    "NEN-4400-1": "NEN-4400-1",
    "nen4400": "NEN-4400-1",
    "NEN4400": "NEN-4400-1",
    "nen-4400": "NEN-4400-1",
    "NEN-4400": "NEN-4400-1",
}


def normalize_certifications(certifications: list[str]) -> list[str]:
    """
    Normalize certification names to use standardized format.
    
    Rules:
    - ISO certifications: Use space format (e.g., "ISO 9001", not "ISO_9001" or "ISO-9001")
    - Other certifications: Keep standard format (e.g., "SNA", "VCU", "ABU")
    - Remove duplicates
    - Preserve order (first occurrence)
    
    Parameters
    ----------
    certifications : list[str]
        Raw list of certification names from scraping
        
    Returns
    -------
    list[str]
        Normalized list of certifications using standardized format
        
    Example
    -------
    >>> normalize_certifications(["ISO_9001", "ISO 14001", "ISO-27001", "SNA"])
    ['ISO 9001', 'ISO 14001', 'ISO 27001', 'SNA']
    >>> normalize_certifications(["iso9001", "ISO_14001", "sna"])
    ['ISO 9001', 'ISO 14001', 'SNA']
    """
    if not certifications:
        return []
    
    normalized = []
    seen = set()
    
    for cert in certifications:
        if not cert or not isinstance(cert, str):
            continue
        
        cert_clean = cert.strip()
        if not cert_clean:
            continue
        
        # Try direct mapping first (case-insensitive)
        cert_lower = cert_clean.lower()
        if cert_lower in CERTIFICATION_NORMALIZATION:
            normalized_cert = CERTIFICATION_NORMALIZATION[cert_lower]
        else:
            # Check if it's already in normalized format (exact match)
            if cert_clean in CERTIFICATION_NORMALIZATION.values():
                normalized_cert = cert_clean
            else:
                # Try case-insensitive match against normalized values
                normalized_cert = None
                for key, value in CERTIFICATION_NORMALIZATION.items():
                    if key.lower() == cert_lower or value.lower() == cert_lower:
                        normalized_cert = value
                        break
                
                # If still not found, keep original (might be a custom certification)
                if normalized_cert is None:
                    normalized_cert = cert_clean
        
        # Add to list if not already seen
        if normalized_cert not in seen:
            normalized.append(normalized_cert)
            seen.add(normalized_cert)
    
    # Sort for consistency (ISO certifications first, then alphabetical)
    iso_certs = [c for c in normalized if c.startswith("ISO")]
    other_certs = sorted([c for c in normalized if not c.startswith("ISO")])
    
    return iso_certs + other_certs


def filter_evidence_urls(urls: list[str]) -> list[str]:
    """
    Filter evidence URLs to keep only human-readable, relevant pages.
    
    Removes:
    - API endpoints (api.seamly-app.com, /api/, etc.)
    - User-specific pages (/mijn-*, /account, /login, /portal, etc.)
    - Duplicates
    - Technical endpoints
    
    Keeps only:
    - Main website pages
    - Business pages: /werkgevers, /over-ons, /diensten, /voorwaarden, /certificeringen
    - Contact pages: /contact
    - Office location pages (if reasonable)
    
    Parameters
    ----------
    urls : list[str]
        List of raw evidence URLs
        
    Returns
    -------
    list[str]
        Filtered list of relevant, human-readable URLs
        
    Example
    -------
    >>> filter_evidence_urls([
    ...     "https://www.randstad.nl/werkgevers",
    ...     "https://api.seamly-app.com/channels/api/v2/...",
    ...     "https://www.randstad.nl/mijn-randstad"
    ... ])
    ['https://www.randstad.nl/werkgevers']
    """
    if not urls:
        return []
    
    filtered = []
    seen = set()
    
    # Patterns to exclude
    exclude_patterns = [
        # API endpoints
        "api.seamly-app.com",
        "/api/",
        "/api/v",
        "api/",
        # User-specific pages
        "/mijn-",
        "/account",
        "/login",
        "/inloggen",
        "/portal",
        "/dashboard",
        "/profiel",
        "/profile",
        "/settings",
        "/instellingen",
        # Technical endpoints
        "/translations/",
        "/configs",
        "/channels/api",
        # JSON/API file extensions
        ".json",
        ".xml",
        "/data/",
        # Other technical paths
        "/embed",
        "/widget",
        "/tracking",
    ]
    
    # Patterns to keep (business-relevant pages)
    keep_keywords = [
        "/werkgevers",
        "/over-ons",
        "/over-",
        "/diensten",
        "/voorwaarden",
        "/certificering",
        "/contact",
        "/vestigingen",
        "/vacatures",
        "/privacy",
        "/disclaimer",
        "/algemene-voorwaarden",
    ]
    
    for url in urls:
        if not url or not isinstance(url, str):
            continue
        
        url_lower = url.lower().strip()
        
        # Skip empty URLs
        if not url_lower:
            continue
        
        # Normalize URL (remove fragments, query params for comparison)
        url_normalized = url.split('#')[0].split('?')[0].rstrip('/')
        
        # Skip if already seen (deduplicate)
        if url_normalized in seen:
            continue
        
        # Check if URL matches exclude patterns
        should_exclude = False
        for pattern in exclude_patterns:
            if pattern in url_lower:
                should_exclude = True
                break
        
        if should_exclude:
            continue
        
        # For office location URLs, be more selective
        # Keep main /vestigingen page but filter out individual office detail pages
        if "/vestigingen/" in url_lower and url_lower.count("/") > 4:
            # This is likely a specific office detail page, skip it
            # Keep only the main /vestigingen page
            continue
        
        # Keep main website root (homepage) and homepage paths
        # Examples: https://example.com, https://example.com/nl-nl, https://example.nl/
        # Check if URL is homepage or homepage path (domain + up to 1 path segment)
        from urllib.parse import urlparse
        parsed = urlparse(url_normalized)
        path_parts = [p for p in parsed.path.split('/') if p]  # Remove empty parts
        
        # Keep if it's domain root or domain with single path segment (like /nl-nl)
        # This includes homepages like https://www.adecco.com/nl-nl
        if len(path_parts) <= 1 and parsed.netloc:
            # Check if netloc looks like a domain (contains a dot)
            if '.' in parsed.netloc:
                seen.add(url_normalized)
                filtered.append(url)
                continue
        
        # Check if URL contains keep keywords (business-relevant pages)
        has_keep_keyword = any(keyword in url_lower for keyword in keep_keywords)
        
        if has_keep_keyword:
            # For keep keywords, allow reasonable depth (up to 4 path segments)
            path_parts = [p for p in url_normalized.split('/') if p]
            if len(path_parts) <= 4:
                seen.add(url_normalized)
                filtered.append(url)
            continue
        
        # For other URLs, be very strict - only keep if it's a very simple path
        # This catches main pages like /home, /about, etc. but not deep paths
        path_parts = [p for p in url_normalized.split('/') if p]
        if len(path_parts) <= 2:  # Only allow 1-2 levels deep for non-keyword URLs
            seen.add(url_normalized)
            filtered.append(url)
    
    # Sort for consistency
    return sorted(filtered)


def normalize_contact_phone(phone: str | None) -> str | None:
    """
    Normalize contact phone number to digits only format (no spaces, dashes, or other separators).
    
    Rules:
    - Extract all digits from the phone number
    - Remove all spaces, dashes, dots, and other separators
    - For Dutch international numbers (starting with +31 or 31): remove country code and return national format
    - For Dutch national numbers: format as digits only (e.g., "080072637823", "0205695911")
    - All numbers are returned as digits only (no + prefix)
    
    Parameters
    ----------
    phone : str | None
        Raw phone number string
        
    Returns
    -------
    str | None
        Normalized phone number (digits only, no prefix) or None if input is None/empty
        
    Example
    -------
    >>> normalize_contact_phone("0800-72-63-78-23")
    '080072637823'
    >>> normalize_contact_phone("0800 72 63 78 23")
    '080072637823'
    >>> normalize_contact_phone("+31 (0)20 569 5911")
    '0205695911'
    >>> normalize_contact_phone("+31 36 529 9555")
    '0365299555'
    >>> normalize_contact_phone("31 36 529 9555")
    '0365299555'
    >>> normalize_contact_phone("020 569 5911")
    '0205695911'
    """
    if not phone or not isinstance(phone, str):
        return None
    
    # Remove leading/trailing whitespace
    phone = phone.strip()
    
    if not phone:
        return None
    
    # Check if it has international format (starts with +)
    has_plus_prefix = phone.startswith("+")
    
    # Extract all digits (remove + and all non-digits)
    digits_only = re.sub(r'\D', '', phone)
    
    if not digits_only:
        return None
    
    # If the number starts with 31 (Dutch country code), convert to national format
    # Remove +31 or 31 prefix and add leading 0
    if digits_only.startswith("31") and len(digits_only) >= 11:
        # It's a Dutch international number, convert to national format
        # Remove the country code (31) and add leading 0
        national_digits = digits_only[2:]  # Remove "31"
        
        # If there's already a leading 0, keep it; otherwise add it
        if not national_digits.startswith("0"):
            national_digits = "0" + national_digits
        
        return national_digits
    
    # Return digits only (national format)
    return digits_only

