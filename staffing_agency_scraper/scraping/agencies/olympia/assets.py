"""
Olympia Netherlands scraper.

Website: https://www.olympia.nl
Part of: STAP Groep"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Set

import dagster as dg
from bs4 import BeautifulSoup

from staffing_agency_scraper.models import Agency, GeoFocusType, OfficeLocation
from staffing_agency_scraper.scraping.base import BaseAgencyScraper
from staffing_agency_scraper.scraping.utils import AgencyScraperUtils


class OlympiaScraper(BaseAgencyScraper):
    """Scraper for Olympia Netherlands."""

    AGENCY_NAME = "Olympia"
    WEBSITE_URL = "https://www.olympia.nl"
    BRAND_GROUP = "Dyo"  # Part of Dyo, not STAP Groep
    PAGES_TO_SCRAPE: List[Dict[str, Any]] = [
        {
            "name": "home",
            "url": "https://www.olympia.nl",
            "functions": ['logo', 'header', 'footer', 'sectors_home', 'sectors_footer'],
        },
        {
            "name": "kwaliteit",
            "url": "https://www.olympia.nl/over-olympia/kwaliteit/",
            "functions": ['certifications'],  # Comprehensive list of all certifications
        },
        {
            "name": "uitzenden",
            "url": "https://www.olympia.nl/personeel/uitzenden/",
            "functions": ['use_cases', 'value_props'],  # Extract typical use cases and value propositions
        },
        {
            "name": "mkb",
            "url": "https://www.olympia.nl/personeel/mkb/",
            "functions": ['smb_stats'],  # Extract SMB-specific statistics and growth signals
        },
        {
            "name": "werving_selectie",
            "url": "https://www.olympia.nl/personeel/werving-en-selectie/",
            "functions": ['recruitment_pricing'],  # Extract no cure no pay, pricing model, growth signals
        },
        {
            "name": "vestigingen",
            "url": "https://www.olympia.nl/vestigingen/",
            "functions": ['offices_paginated'],  # Will loop through all pages
        },
        {
            "name": "contact",
            "url": "https://www.olympia.nl/over-olympia/contact",
            "functions": ['contact_detail', 'footer'],
        },
        {
            "name": "privacy",
            "url": "https://www.olympia.nl/voorwaarden/privacy-statement/",
            "functions": ['contact_email'],  # Extract contact email
        },
    ]

    def scrape(self) -> Agency:
        self.logger.info(f"Starting scrape of {self.AGENCY_NAME}")
        
        # Initialize utils
        self.utils = AgencyScraperUtils(logger=self.logger)
        agency = self.create_base_agency()
        agency.geo_focus_type = GeoFocusType.NATIONAL
        agency.employers_page_url = f"{self.WEBSITE_URL}/personeel"
        
        all_sectors: Set[str] = set()
        page_texts: Dict[str, str] = {}
        
        for page in self.PAGES_TO_SCRAPE:
            url = page["url"]
            page_name = page["name"]
            functions = page.get("functions", [])            
            try:
                # Fetch with BS4 (automatically adds to evidence_urls)
                soup = self.fetch_page(url)
                page_text = soup.get_text(separator=" ", strip=True)
                page_texts[url] = page_text
                
                # Apply normal functions
                self._apply_functions(agency, functions, soup, page_text, all_sectors, url)
                
                # Portal detection on every page
                if self.utils.detect_candidate_portal(soup, page_text, url):
                    agency.digital_capabilities.candidate_portal = True
                if self.utils.detect_client_portal(soup, page_text, url):
                    agency.digital_capabilities.client_portal = True
                
     
                agency.role_levels = []
                
            except Exception as e:
                self.logger.error(f"❌ Error scraping {url}: {e}")
        
        # Extract common fields using utils
        agency.certifications = self.utils.fetch_certifications(page_texts)
        agency.cao_type = self.utils.fetch_cao_type(page_texts)
        agency.membership = self.utils.fetch_membership(page_texts)
        
        # Finalize sectors
        if all_sectors:
            agency.sectors_core = sorted(list(all_sectors))
        
        agency.evidence_urls = list(self.evidence_urls)
        agency.collected_at = self.collected_at
        
        self.logger.info(f"✅ Completed scrape of {self.AGENCY_NAME}")
        
        return agency
    
    def _apply_functions(
        self,
        agency: Agency,
        functions: List[str],
        soup: BeautifulSoup,
        page_text: str,
        all_sectors: Set[str],
        url: str,
    ) -> None:
        """Apply BS4/regex extraction functions."""
        for func_name in functions:
            if func_name == "logo":
                logo = self.utils.fetch_logo(soup, url)
                if logo:
                    # Make absolute URL
                    if logo.startswith("/"):
                        logo = f"{self.WEBSITE_URL}{logo}"
                    agency.logo_url = logo
                    self.logger.info(f"✓ Found logo: {logo} | Source: {url}")
            
            elif func_name == "header":
                self._extract_header(soup, agency, url)
            
            elif func_name == "certifications":
                self._extract_certifications(soup, agency, url)
            
            elif func_name == "use_cases":
                self._extract_use_cases(soup, agency, url)
            
            elif func_name == "value_props":
                self._extract_value_propositions(soup, agency, url)
            
            elif func_name == "contact_detail":
                self._extract_contact_details(soup, agency, url)
            
            elif func_name == "contact_email":
                self._extract_contact_email(soup, agency, url)
            
            elif func_name == "footer":
                self._extract_footer_data(soup, agency, url)
            
            elif func_name == "sectors_home":
                self._extract_sectors_from_home(soup, all_sectors, url)
            
            elif func_name == "sectors_footer":
                self._extract_sectors_from_footer(soup, all_sectors, url)
            
            elif func_name == "smb_stats":
                self._extract_smb_statistics(soup, agency, url)
            
            elif func_name == "recruitment_pricing":
                self._extract_recruitment_pricing(soup, agency, url)
            
            elif func_name == "offices_paginated":
                offices = self._extract_offices_paginated(url)
                if offices:
                    if not agency.office_locations:
                        agency.office_locations = []
                    # Merge with existing offices (avoid duplicates)
                    existing_cities = {office.city for office in agency.office_locations}
                    for office in offices:
                        if office.city not in existing_cities:
                            agency.office_locations.append(office)
                            existing_cities.add(office.city)
    
    def _extract_contact_details(self, soup: BeautifulSoup, agency: Agency, url: str) -> None:
        """
        Extract contact details from the contact page.
        
        - KVK: 27332657
        - BTW: 82.02.39.744.B.01
        - Address: Mercuriusplein 1, 2132 HA Hoofddorp
        - Contact form
        """
        # Contact form URL
        form = soup.find("form", id=lambda x: x and "form" in x.lower())
        if form:
            agency.contact_form_url = url
            self.logger.info(f"✓ Found contact form | Source: {url}")
        
        # Find content with KVK/BTW/address
        content_block = soup.find("div", class_="content-element__content")
        if content_block:
            text = content_block.get_text(separator=" ", strip=True)
            
            # Extract KVK: 27332657
            kvk_match = re.search(r'(\d{8})', text)
            if kvk_match and "27332657" in text:
                agency.kvk_number = "27332657"
                self.logger.info(f"✓ Found KVK number: {agency.kvk_number} | Source: {url}")
            
            # Extract HQ: Mercuriusplein 1, 2132 HA Hoofddorp
            if "Mercuriusplein" in text and "Hoofddorp" in text:
                agency.hq_city = "Hoofddorp"
                agency.hq_province = "Noord-Holland"
                self.logger.info(f"✓ Found HQ: Mercuriusplein 1, 2132 HA Hoofddorp | Source: {url}")
                
                # Create HQ office
                hq_office = OfficeLocation(
                    city="Hoofddorp",
                    province="Noord-Holland",
                    street="Mercuriusplein 1",
                    postalcode="2132 HA"
                )
                if not agency.office_locations:
                    agency.office_locations = []
                agency.office_locations.append(hq_office)
    
    def _extract_contact_email(self, soup: BeautifulSoup, agency: Agency, url: str) -> None:
        """
        Extract contact email, phone, and legal name from privacy statement page.
        """
        # Find email from <a> tags with mailto:
        email_links = soup.find_all("a", href=lambda x: x and x.startswith("mailto:"))
        for link in email_links:
            href = link.get("href", "")
            # Regex to extract ...@olympia.nl
            email_match = re.search(r'([a-z0-9]+@olympia\.nl)', href, re.IGNORECASE)
            if email_match:
                agency.contact_email = email_match.group(1).lower()
                self.logger.info(f"✓ Found contact email: {agency.contact_email} | Source: {url}")
                break
        
        # Extract phone: T 023 - 583 70 00
        if not agency.contact_phone:
            text = soup.get_text(separator=" ", strip=True)
            phone_match = re.search(r'T\s*(023\s*-?\s*583\s*70\s*00)', text)
            if phone_match:
                phone = phone_match.group(1).replace(" ", "").replace("-", "")
                agency.contact_phone = f"023-{phone[3:]}"
                self.logger.info(f"✓ Found contact phone: {agency.contact_phone} | Source: {url}")
        
        # Extract legal name: Olympia Nederland B.V.
        if not agency.legal_name:
            text = soup.get_text(separator=" ", strip=True)
            if "Olympia Nederland B.V." in text or "Olympia Nederland BV" in text:
                agency.legal_name = "Olympia Nederland B.V."
                self.logger.info(f"✓ Found legal name: {agency.legal_name} | Source: {url}")
    
    def _extract_header(self, soup: BeautifulSoup, agency: Agency, url: str) -> None:
        """
        Extract data from the header navigation.
        
        Services to extract:
        - "Uitzenden van personeel" → uitzenden = true
        - "Werving & selectie" → werving_selectie = true
        - "Inhouse" → inhouse_services = true
        - "Vakschool" → opleiden_ontwikkelen = true
        - "MKB" → customer_segments
        
        Also extract:
        - Multi-language support (16 languages)
        - CAO 2026 reference
        """
        header = soup.find("header", id="nav-bar")
        if not header:
            return
        
        # Extract services from navigation
        nav_links = header.find_all("a")
        for link in nav_links:
            link_text = link.get_text(strip=True).lower()
            
            # Uitzenden
            if "uitzenden van personeel" in link_text or link.get("href", "") == "/personeel/uitzenden/":
                agency.services.uitzenden = True
                self.logger.info(f"✓ Found service: uitzenden (from navigation) | Source: {url}")
            
            # Werving & selectie
            if "werving" in link_text and "selectie" in link_text:
                agency.services.werving_selectie = True
                self.logger.info(f"✓ Found service: werving_selectie (from navigation) | Source: {url}")
            
            # Inhouse
            if "inhouse" in link_text:
                agency.services.inhouse_services = True
                self.logger.info(f"✓ Found service: inhouse_services (from navigation) | Source: {url}")
            
            # Vakschool (training)
            if "vakschool" in link_text:
                agency.services.opleiden_ontwikkelen = True
                self.logger.info(f"✓ Found service: opleiden_ontwikkelen (Vakschool training) | Source: {url}")
            
            # MKB (SMB focus)
            if "mkb" in link_text:
                if not agency.customer_segments:
                    agency.customer_segments = []
                if "SMB" not in agency.customer_segments:
                    agency.customer_segments.append("SMB")
                    self.logger.info(f"✓ Found customer segment: SMB (MKB specialization) | Source: {url}")
        
        # Check for CAO 2026 banner
        cao_link = header.find("a", href=lambda x: x and "cao" in x.lower())
        if cao_link:
            from staffing_agency_scraper.models.agency import CaoType
            agency.cao_type = CaoType.ABU
            self.logger.info(f"✓ Found CAO type: ABU (from CAO 2026 banner) | Source: {url}")
        
        # Check for multi-language support
        translate_elem = header.find("div", class_="custom-translate-dropdown")
        if translate_elem:
            lang_divs = translate_elem.find_all("div", class_="lang")
            if len(lang_divs) > 10:  # 16 languages total
                self.logger.info(f"✓ Found multi-language support: {len(lang_divs)} languages | Source: {url}")
                # This is a growth signal
                if not agency.growth_signals:
                    agency.growth_signals = []
                if "meertalig_platform" not in agency.growth_signals:
                    agency.growth_signals.append("meertalig_platform")
    
    def _extract_certifications(self, soup: BeautifulSoup, agency: Agency, url: str) -> None:
        """
        Extract certifications from the quality page.
        
        Comprehensive list includes:
        - ABU (General Association of Temporary Employment Agencies)
        - NFV (Dutch Franchise Association)
        - ISO 9001, ISO 14001, ISO 27001
        - VCU (Health and Safety Checklist)
        - SNA (Labour Standards Foundation) / NEN 4400-01
        - Kiwa designation
        - PSO (Performance Ladder for Social Entrepreneurship)
        - Diversity Champion 2021
        - Website of the Year 2021
        """
        # Find the content block with certifications list
        content_block = soup.find("div", class_="content-element__content")
        if not content_block:
            return
        
        # Find all list items
        list_items = content_block.find_all("li")
        certs = []
        awards = []
        
        for li in list_items:
            text = li.get_text(strip=True).lower()
            
            # ABU
            if "abu" in text or "algemene bond uitzendondernemingen" in text:
                certs.append("ABU")
                # Also set membership
                if not agency.membership:
                    agency.membership = []
                if "ABU" not in agency.membership:
                    agency.membership.append("ABU")
                    self.logger.info(f"✓ Found membership: ABU | Source: {url}")
            
            # NFV
            if "nfv" in text or "franchise vereniging" in text:
                certs.append("NFV")
            
            # ISO certifications
            if "iso 9001" in text or "iso-9001" in text:
                certs.append("ISO_9001")
            if "iso 14001" in text or "iso-14001" in text:
                certs.append("ISO_14001")
            if "iso 27001" in text or "iso-27001" in text:
                certs.append("ISO_27001")
            
            # VCU
            if "vcu" in text or ("veiligheid" in text and "gezondheid" in text):
                certs.append("VCU")
            
            # SNA
            if "sna" in text or "normering arbeid" in text or "nen 4400" in text:
                certs.append("SNA")
            
            # Kiwa
            if "kiwa" in text:
                certs.append("Kiwa")
            
            # PSO
            if "pso" in text or "socialer ondernemen" in text:
                certs.append("PSO")
            
            # Awards
            if "diversity champion" in text:
                awards.append("Diversity_Champion_2021")
            if "website" in text and "2021" in text:
                awards.append("Website_van_het_Jaar_2021")
        
        # Merge certifications
        if certs:
            if not agency.certifications:
                agency.certifications = []
            for cert in certs:
                if cert not in agency.certifications:
                    agency.certifications.append(cert)
            self.logger.info(f"✓ Found {len(certs)} certifications: {', '.join(certs)} | Source: {url}")
        
        # Log awards as growth signals
        if awards:
            if not agency.growth_signals:
                agency.growth_signals = []
            for award in awards:
                if award not in agency.growth_signals:
                    agency.growth_signals.append(award)
            self.logger.info(f"✓ Found {len(awards)} awards/recognitions: {', '.join(awards)} | Source: {url}")
    
    def _extract_use_cases(self, soup: BeautifulSoup, agency: Agency, url: str) -> None:
        """
        Extract typical use cases from the "Uitzenden" (Temporary staffing) service page.
        
        Typical use cases mentioned:
        - Piekdrukte (peak demand)
        - Ziekte (illness coverage)
        - Verlof (leave replacement)
        - Groei van onderneming (business growth)
        """
        # Find the content block describing "Wat is uitzenden?"
        content_blocks = soup.find_all("div", class_="content-element__content")
        
        for block in content_blocks:
            text = block.get_text(separator=" ", strip=True).lower()
            
            # Look for the section describing temporary staffing use cases
            if "wisselende personeelsbehoefte" in text or "piekdrukte" in text:
                use_cases = []
                
                if "piekdrukte" in text:
                    use_cases.append("piekdrukte_seizoen")
                if "ziekte" in text:
                    use_cases.append("vervanging_ziekte_verlof")
                if "verlof" in text and "vervanging_ziekte_verlof" not in use_cases:
                    use_cases.append("vervanging_ziekte_verlof")
                if "groei" in text:
                    use_cases.append("langdurige_projecten")
                
                if use_cases:
                    if not agency.typical_use_cases:
                        agency.typical_use_cases = []
                    for use_case in use_cases:
                        if use_case not in agency.typical_use_cases:
                            agency.typical_use_cases.append(use_case)
                    self.logger.info(f"✓ Found {len(use_cases)} typical use cases: {', '.join(use_cases)} | Source: {url}")
                break
    
    def _extract_value_propositions(self, soup: BeautifulSoup, agency: Agency, url: str) -> None:
        """
        Extract value propositions and speed claims from the "Uitzenden" service page.
        
        Key benefits mentioned:
        - Snelheid (Speed) - fast scaling
        - Risicobeheersing (Risk management)
        - Compliance (Compliance assurance)
        - Gemak (Convenience) - administrative ease
        """
        # Find the "De belangrijkste voordelen" section
        content_blocks = soup.find_all("div", class_="content-element__content")
        
        for block in content_blocks:
            heading = block.find("h2", class_="content-element__title")
            if heading and "voordelen" in heading.get_text(strip=True).lower():
                # Extract list items
                list_items = block.find_all("li")
                
                value_props = []
                speed_found = False
                
                for li in list_items:
                    text = li.get_text(separator=" ", strip=True).lower()
                    
                    # Speed claim
                    if "snelheid" in text or "snel personeel" in text:
                        if not speed_found:
                            if not agency.speed_claims:
                                agency.speed_claims = []
                            if "snelle_plaatsing" not in agency.speed_claims:
                                agency.speed_claims.append("snelle_plaatsing")
                            speed_found = True
                    
                    # Risk management
                    if "risicobeheersing" in text or "geen risico" in text:
                        value_props.append("werkgeversrisico_afgedekt")
                    
                    # Compliance
                    if "compliance" in text or "compliant" in text:
                        value_props.append("compliance_geborgd")
                    
                    # Administrative convenience
                    if "geen gedoe met administratieve taken" in text or ("gemak" in text and "administratie" in text):
                        value_props.append("administratie_ontzorging")
                
                if speed_found:
                    self.logger.info(f"✓ Found speed claim: fast personnel scaling | Source: {url}")
                
                if value_props:
                    if not agency.growth_signals:
                        agency.growth_signals = []
                    for prop in value_props:
                        if prop not in agency.growth_signals:
                            agency.growth_signals.append(prop)
                    self.logger.info(f"✓ Found {len(value_props)} value propositions: {', '.join(value_props)} | Source: {url}")
                break
    
    def _extract_smb_statistics(self, soup: BeautifulSoup, agency: Agency, url: str) -> None:
        """
        Extract SMB-specific statistics and growth signals from /personeel/mkb/ page.
        
        Key data:
        - "More than 20,000 employees available" → candidate_pool_size_estimate
        - "20,000 candidates at 3,000 SME clients" → annual_placements_estimate
        - "3,000 SME clients" → growth signal
        - "50 years of experience" → growth signal
        - Municipal government sector
        """
        page_text = soup.get_text(separator=" ", strip=True).lower()
        
        # Extract candidate pool size: "20,000 employees"
        pool_match = re.search(r'(\d+[.,]?\d*)\s*(?:medewerkers|employees|candidates)(?:\s+beschikbaar|available)?', page_text)
        if pool_match:
            pool_size = int(pool_match.group(1).replace(".", "").replace(",", ""))
            if pool_size >= 10000:  # Only if significant
                agency.candidate_pool_size_estimate = pool_size
                self.logger.info(f"✓ Found candidate pool size: {pool_size:,} | Source: {url}")
        
        # Extract annual placements: "20,000 candidates per year"
        placement_match = re.search(r'(\d+[.,]?\d*)\s*(?:kandidaten|candidates).*?(?:per jaar|every year|jaarlijks)', page_text)
        if placement_match:
            placements = int(placement_match.group(1).replace(".", "").replace(",", ""))
            if placements >= 5000:  # Only if significant
                agency.annual_placements_estimate = placements
                self.logger.info(f"✓ Found annual placements: {placements:,} | Source: {url}")
        
        # Extract number of SME clients: "3,000 SME clients"
        client_match = re.search(r'(\d+[.,]?\d*)\s*(?:mkb[- ]?klanten|mkb[- ]?bedrijven|sme clients)', page_text)
        if client_match:
            client_count = int(client_match.group(1).replace(".", "").replace(",", ""))
            if client_count >= 1000:  # Significant client base
                if not agency.growth_signals:
                    agency.growth_signals = []
                signal = f"{client_count}_mkb_klanten"
                if signal not in agency.growth_signals:
                    agency.growth_signals.append(signal)
                self.logger.info(f"✓ Found client base: {client_count:,} SME clients | Source: {url}")
        
        # Extract years of experience: "50 years"
        years_match = re.search(r'(\d+)\s*(?:jaar|years)(?:\s+ervaring|experience)?', page_text)
        if years_match:
            years = int(years_match.group(1))
            if years >= 20:  # Significant history
                if not agency.growth_signals:
                    agency.growth_signals = []
                signal = f"{years}_jaar_actief"
                if signal not in agency.growth_signals:
                    agency.growth_signals.append(signal)
                self.logger.info(f"✓ Found company history: {years} years active | Source: {url}")
        
        # Extract "Municipal government" sector mention
        if "gemeenten" in page_text or "municipal" in page_text or "overheid" in page_text:
            # This will be picked up by utils.fetch_sectors, but we can log it
            self.logger.info(f"✓ Found 'gemeenten' (municipal government) sector mention | Source: {url}")
    
    def _extract_recruitment_pricing(self, soup: BeautifulSoup, agency: Agency, url: str) -> None:
        """
        Extract recruitment & selection pricing and guarantees from /personeel/werving-en-selectie/ page.
        
        Key data:
        - "No cure, no pay" → no_cure_no_pay: true
        - "Percentage of annual salary" → pricing model hint
        - "130 branches throughout the Netherlands" → growth signal
        - "100% match on company culture" → value proposition
        - "Long length of stay" → value proposition
        - "Vacancy always filled" → guarantee
        """
        page_text = soup.get_text(separator=" ", strip=True).lower()
        
        # Extract "no cure, no pay"
        if "no cure" in page_text and "no pay" in page_text:
            agency.no_cure_no_pay = True
            self.logger.info(f"✓ Found 'no cure, no pay' guarantee | Source: {url}")
        
        # Extract pricing model hint: "percentage of annual salary"
        if "percentage" in page_text and ("jaarsal" in page_text or "annual salary" in page_text):
            if not agency.example_pricing_hint:
                agency.example_pricing_hint = "Percentage of annual salary (varies by complexity and seniority)"
            self.logger.info(f"✓ Found pricing model: percentage of annual salary | Source: {url}")
        
        # Extract number of branches: "130 vestigingen"
        branch_match = re.search(r'(\d+)\s*(?:vestigingen|kantoren|branches)', page_text)
        if branch_match:
            branch_count = int(branch_match.group(1))
            if branch_count >= 50:  # Significant network
                if not agency.growth_signals:
                    agency.growth_signals = []
                signal = f"landelijk_{branch_count}_vestigingen"
                if signal not in agency.growth_signals:
                    agency.growth_signals.append(signal)
                self.logger.info(f"✓ Found national network: {branch_count} branches | Source: {url}")
        
        # Extract value propositions
        value_props = []
        
        if "100%" in page_text and ("bedrijfscultuur" in page_text or "company culture" in page_text):
            value_props.append("100_procent_match_bedrijfscultuur")
        
        if ("lange" in page_text or "long" in page_text) and ("verblijfsduur" in page_text or "length of stay" in page_text):
            value_props.append("lange_verblijfsduur_medewerkers")
        
        if ("vacature altijd gevuld" in page_text or "vacancy always filled" in page_text):
            value_props.append("vacature_altijd_gevuld_garantie")
        
        if value_props:
            if not agency.growth_signals:
                agency.growth_signals = []
            for prop in value_props:
                if prop not in agency.growth_signals:
                    agency.growth_signals.append(prop)
            self.logger.info(f"✓ Found {len(value_props)} recruitment value propositions | Source: {url}")
    
    def _extract_footer_data(self, soup: BeautifulSoup, agency: Agency, url: str) -> None:
        """
        Extract data from footer:
        - Mobile app (Apple Store + Google Play)
        - Social media (Facebook, Instagram, LinkedIn, YouTube)
        - Certifications (ABU, SNA, ISO 9001, NFV)
        - Parent company (Dyo)
        - Top companies (major client references)
        """
        footer = soup.find("footer", id="footer")
        if not footer:
            return
        
        # Extract mobile app
        apple_link = footer.find("a", href=lambda x: x and "apps.apple.com" in x)
        google_link = footer.find("a", href=lambda x: x and "play.google.com" in x)
        if apple_link or google_link:
            agency.digital_capabilities.mobile_app = True
            self.logger.info(f"✓ Found mobile app (Apple Store + Google Play) | Source: {url}")
        
        # Extract social media
        social_platforms = []
        social_links = {
            "facebook.com": "Facebook",
            "instagram.com": "Instagram",
            "linkedin.com": "LinkedIn",
            "youtube.com": "YouTube"
        }
        for domain, platform in social_links.items():
            link = footer.find("a", href=lambda x: x and domain in x)
            if link:
                social_platforms.append(platform)
        
        if social_platforms:
            self.logger.info(f"✓ Found social media: {', '.join(social_platforms)} | Source: {url}")
        
        # Extract top companies/major clients from "Topbedrijven" section
        footer_menus = footer.find_all("div", class_="footer-menu")
        for menu in footer_menus:
            menu_title = menu.find("h2", class_="heading-md")
            if menu_title:
                title_text = menu_title.get_text(strip=True).lower()
                if "bedrijven" in title_text or "companies" in title_text:
                    # Found "Topbedrijven" or "Companies" section
                    links = menu.find_all("a")
                    clients = []
                    for link in links:
                        # Extract just the company name from "Vacatures ASML" → "ASML"
                        client_text = link.get_text(strip=True)
                        # Remove "Vacancies" / "Vacatures" prefix
                        client_name = re.sub(r'^(?:vacatures|vacancies)\s+', '', client_text, flags=re.IGNORECASE).strip()
                        if client_name:
                            clients.append(client_name)
                    
                    if clients:
                        # This is a growth signal - working with major companies
                        if not agency.growth_signals:
                            agency.growth_signals = []
                        
                        # Check for Fortune 500 / enterprise clients
                        enterprise_clients = ["DHL", "ASML", "Gemeente Amsterdam", "GVB", "Kruidvat", "Renewi"]
                        major_count = sum(1 for client in clients if any(ec.lower() in client.lower() for ec in enterprise_clients))
                        
                        if major_count >= 3:
                            if "werkt_met_fortune500_klanten" not in agency.growth_signals:
                                agency.growth_signals.append("werkt_met_fortune500_klanten")
                            self.logger.info(f"✓ Found {len(clients)} major client references: {', '.join(clients[:3])}... | Source: {url}")
                    break
        
        # Extract certifications from footer images
        cert_images = footer.find_all("img", alt=True)
        certs = []
        for img in cert_images:
            alt_text = img.get("alt", "").lower()
            if "abu" in alt_text:
                certs.append("ABU")
            if "sna" in alt_text or "normering arbeid" in alt_text:
                certs.append("SNA")
            if "iso" in alt_text and "9001" in alt_text:
                certs.append("ISO_9001")
            if "nfv" in alt_text or "franchise" in alt_text:
                certs.append("NFV")
        
        if certs:
            if not agency.certifications:
                agency.certifications = []
            agency.certifications.extend(certs)
            agency.certifications = list(set(agency.certifications))
            self.logger.info(f"✓ Found certifications: {', '.join(certs)} | Source: {url}")
        
        # Check for Dyo parent company
        dyo_link = footer.find("a", href=lambda x: x and "dyo.nl" in x)
        if dyo_link:
            self.logger.info(f"✓ Confirmed parent company: Dyo | Source: {url}")
    
    def _extract_sectors_from_home(self, soup: BeautifulSoup, all_sectors: Set[str], url: str) -> None:
        """
        Extract sectors from the homepage "Vacatures per vakgebied" section.
        
        This section lists 12 core sectors:
        - Productie, Logistiek, Klantenservice, Techniek, Administratie, Verkoop,
        - Overig, Marketing en communicatie, Personeelszaken, Horeca, Groenvoorziening, Management
        """
        # Find the "Vacatures per vakgebied" block
        list_blocks = soup.find_all("div", class_="element")
        
        for block in list_blocks:
            # Check if this is the "Vacatures per vakgebied" section
            heading = block.find("h3")
            if heading and "vakgebied" in heading.get_text(strip=True).lower():
                # Find all list items
                list_items = block.find_all("li", class_="list-item")
                sectors_found = []
                
                for li in list_items:
                    link = li.find("a")
                    if link:
                        sector_text = link.find("span")
                        if sector_text:
                            sector = sector_text.get_text(strip=True)
                            sectors_found.append(sector)
                
                if sectors_found:
                    # Map to standardized sector names
                    sector_mapping = {
                        "productie": "productie",
                        "logistiek": "logistiek_transport",
                        "klantenservice": "klantenservice",
                        "techniek": "techniek_engineering",
                        "administratie": "administratie_secretarieel",
                        "verkoop": "sales_accountmanagement",
                        "marketing en communicatie": "marketing_communicatie",
                        "personeelszaken": "hr_recruitment",
                        "horeca": "horeca_catering",
                        "groenvoorziening": "groenvoorziening",
                        "management": "management",
                    }
                    
                    for sector in sectors_found:
                        sector_lower = sector.lower()
                        if sector_lower in sector_mapping:
                            all_sectors.add(sector_mapping[sector_lower])
                    
                    self.logger.info(f"✓ Found {len(sectors_found)} sectors from homepage: {', '.join(sectors_found)} | Source: {url}")
                    break
    
    def _extract_sectors_from_footer(self, soup: BeautifulSoup, all_sectors: Set[str], url: str) -> None:
        """
        Extract sectors from footer menu "Jobs by category".
        
        Categories include:
        - Thuiswerk, Klantenservice, Logistiek, Productie, Administratie, Techniek, etc.
        """
        footer = soup.find("footer", id="footer")
        if not footer:
            return
        
        # Find footer menus
        footer_menus = footer.find_all("div", class_="footer-menu")
        for menu in footer_menus:
            menu_title = menu.find("h2", class_="heading-md")
            if menu_title and "categorie" in menu_title.get_text(strip=True).lower():
                # This is the "Jobs by category" menu
                links = menu.find_all("a")
                for link in links:
                    href = link.get("href", "")
                    # Extract sector from URL pattern: /vacatures/{sector}/
                    if "/vacatures/" in href:
                        parts = href.split("/vacatures/")
                        if len(parts) > 1:
                            sector_slug = parts[1].strip("/").split("/")[0]
                            # Map common slugs to our sectors
                            sector_mapping = {
                                "logistiek": "logistiek",
                                "productie": "productie",
                                "klantenservice": "klantenservice",
                                "administratie": "administratie",
                                "techniek": "techniek",
                                "horeca": "horeca",
                            }
                            if sector_slug in sector_mapping:
                                sector = sector_mapping[sector_slug]
                                all_sectors.add(sector)
                                self.logger.info(f"✓ Found sector from footer: {sector} | Source: {url}")
    
    def _extract_offices_paginated(self, base_url: str) -> list[OfficeLocation]:
        """
        Extract all office locations from paginated vestigingen pages.
        
        Loops through:
        - https://www.olympia.nl/vestigingen/ (page 1)
        - https://www.olympia.nl/vestigingen/?pageIndex=2
        - https://www.olympia.nl/vestigingen/?pageIndex=3
        - etc.
        
        Stops when no more offices are found.
        """
        offices = []
        page_index = 1
        
        # Province mapping for cities
        CITY_TO_PROVINCE = {
            "alkmaar": "Noord-Holland",
            "almelo": "Overijssel",
            "almere": "Flevoland",
            "alphen a/d rijn": "Zuid-Holland",
            "amersfoort": "Utrecht",
            "amsterdam": "Noord-Holland",
            "apeldoorn": "Gelderland",
            "arnhem": "Gelderland",
            "assen": "Drenthe",
            "barendrecht": "Zuid-Holland",
            "oud-beijerland": "Zuid-Holland",
            "bergen op zoom": "Noord-Brabant",
            "breda": "Noord-Brabant",
            "delfzijl": "Groningen",
            "farsum": "Groningen",
            "den bosch": "Noord-Brabant",
            "'s hertogenbosch": "Noord-Brabant",
            "den haag": "Zuid-Holland",
            "den helder": "Noord-Holland",
            "deurne": "Noord-Brabant",
            "deventer": "Overijssel",
            "doetinchem": "Gelderland",
            "dordrecht": "Zuid-Holland",
            "drachten": "Friesland",
            "ede": "Gelderland",
            "eindhoven": "Noord-Brabant",
            "emmen": "Drenthe",
            "enschede": "Overijssel",
            "gorinchem": "Zuid-Holland",
            "groningen": "Groningen",
            "haarlem": "Noord-Holland",
            "harderwijk": "Gelderland",
            "heerenveen": "Friesland",
            "helmond": "Noord-Brabant",
            "hengelo": "Overijssel",
            "hilversum": "Noord-Holland",
            "hoofddorp": "Noord-Holland",
            "hoogeveen": "Drenthe",
            "hoorn": "Noord-Holland",
            "leeuwarden": "Friesland",
            "leiden": "Zuid-Holland",
            "lichtenvoorde": "Gelderland",
            "maassluis": "Zuid-Holland",
            "maastricht": "Limburg",
            "meppel": "Drenthe",
            "nieuwegein": "Utrecht",
            "nijmegen": "Gelderland",
            "oosterhout": "Noord-Brabant",
            "roermond": "Limburg",
            "roosendaal": "Noord-Brabant",
            "rotterdam": "Zuid-Holland",
            "schijndel": "Noord-Brabant",
            "tiel": "Gelderland",
            "tilburg": "Noord-Brabant",
            "uden": "Noord-Brabant",
            "utrecht": "Utrecht",
            "veendam": "Groningen",
            "veenendaal": "Utrecht",
            "veghel": "Noord-Brabant",
            "veldhoven": "Noord-Brabant",
            "venlo": "Limburg",
            "waalwijk": "Noord-Brabant",
            "woerden": "Utrecht",
            "zaandam": "Noord-Holland",
            "zaltbommel": "Gelderland",
            "zeist": "Utrecht",
            "zevenaar": "Gelderland",
            "zoetermeer": "Zuid-Holland",
            "zutphen": "Gelderland",
            "zwolle": "Overijssel",
        }
        
        while True:
            # Construct URL
            if page_index == 1:
                url = base_url
            else:
                url = f"{base_url}?pageIndex={page_index}"
            
            try:
                self.logger.info(f"→ Fetching offices from page {page_index}: {url}")
                soup = self.fetch_page(url)  # Automatically adds to evidence_urls
                
                # Find the jobs list
                jobs_list = soup.find("ul", class_="jobs-list")
                if not jobs_list:
                    self.logger.info(f"→ No jobs-list found on page {page_index}, stopping pagination")
                    break
                
                # Find all office location items
                office_items = jobs_list.find_all("li")
                if not office_items:
                    self.logger.info(f"→ No office items found on page {page_index}, stopping pagination")
                    break
                
                # Extract each office
                for item in office_items:
                    try:
                        # Extract office name (h3 > a)
                        h3 = item.find("h3")
                        if not h3:
                            continue
                        a_tag = h3.find("a")
                        if not a_tag:
                            continue
                        office_name = a_tag.get_text(strip=True)
                        
                        # Extract address
                        address_tag = item.find("address")
                        if not address_tag:
                            continue
                        
                        address_spans = address_tag.find_all("span")
                        if len(address_spans) < 2:
                            continue
                        
                        street = address_spans[0].get_text(strip=True)
                        postal_city = address_spans[1].get_text(strip=True)
                        
                        # Parse postal code and city
                        # Format: "3262 JN Oud-Beijerland" or "5211 VW 's Hertogenbosch"
                        parts = postal_city.split(maxsplit=2)
                        if len(parts) >= 3:
                            postalcode = f"{parts[0]} {parts[1]}"
                            city = parts[2]
                        else:
                            postalcode = None
                            city = postal_city
                        
                        # Get province from mapping
                        city_lower = city.lower()
                        province = CITY_TO_PROVINCE.get(city_lower, None)
                        
                        # Extract phone (optional)
                        phone = None
                        phone_link = item.find("a", href=lambda x: x and x.startswith("tel:"))
                        if phone_link:
                            phone = phone_link.get("href").replace("tel:", "")
                        
                        # Create office location
                        office = OfficeLocation(
                            city=city,
                            province=province,
                            street=street,
                            postalcode=postalcode,
                            phone=phone
                        )
                        offices.append(office)
                        self.logger.info(f"✓ Found office: {city} ({province}) | {street} | Page {page_index}")
                    
                    except Exception as e:
                        self.logger.warning(f"⚠ Error parsing office item: {e}")
                        continue
                
                self.logger.info(f"✓ Extracted {len(office_items)} offices from page {page_index}")
                page_index += 1
                
                # Safety limit to avoid infinite loops
                if page_index > 20:
                    self.logger.warning(f"⚠ Reached page limit ({page_index}), stopping pagination")
                    break
            
            except Exception as e:
                self.logger.error(f"❌ Error fetching page {page_index}: {e}")
                break
        
        self.logger.info(f"✅ Total offices extracted: {len(offices)}")
        return offices


@dg.asset(group_name="agencies")
def olympia_scrape() -> dg.Output[dict]:
    """Scrape Olympia website."""
    scraper = OlympiaScraper()
    agency = scraper.scrape()
    output_path = scraper.save_to_json(agency)
    return dg.Output(
        value=agency.to_json_dict(),
        metadata={
            "agency_name": agency.agency_name,
            "website_url": agency.website_url,
            "pages_scraped": len(agency.evidence_urls),
            "output_file": output_path,
        },
    )

