"""
Tempo-Team Netherlands scraper.

Website: https://www.tempo-team.nl
"""

from __future__ import annotations

import json
import re
from typing import Any, Dict, List, Set

import dagster as dg
from bs4 import BeautifulSoup

from staffing_agency_scraper.models import (
    Agency,
    AICapabilities,
    DigitalCapabilities,
    GeoFocusType,
    OfficeLocation,
)
from staffing_agency_scraper.scraping.base import BaseAgencyScraper
from staffing_agency_scraper.scraping.utils import AgencyScraperUtils

class TempoTeamScraper(BaseAgencyScraper):
    """Scraper for Tempo-Team Netherlands."""

    AGENCY_NAME = "Tempo-Team"
    WEBSITE_URL = "https://www.tempo-team.nl"
    BRAND_GROUP = "Randstad N.V."  # Acquired by Randstad in 1983
    
    PAGES_TO_SCRAPE: List[Dict[str, Any]] = [
        {
            "name": "home",
            "url": "https://www.tempo-team.nl",
            "functions": ["header", "footer", "logo"],
        },
        {
            "name": "organization",
            "url": "https://www.tempo-team.nl/over-tempo-team/organisatie",
            "functions": ["about"],
        },
        {
            "name": "certification",
            "url": "https://www.tempo-team.nl/over-tempo-team/organisatie/certificering",
            "functions": ["certifications"],
        },
        {
            "name": "history",
            "url": "https://www.tempo-team.nl/over-tempo-team/organisatie/geschiedenis",
            "functions": ["history"],
        },
        {
            "name": "mission",
            "url": "https://www.tempo-team.nl/over-tempo-team/organisatie/missie-en-visie",
            "functions": ["mission"],
        },
        {
            "name": "contact",
            "url": "https://www.tempo-team.nl/over-tempo-team/contact",
            "functions": ["contact"],
        },
        {
            "name": "offices",
            "url": "https://www.tempo-team.nl/vestigingen",
            "functions": ["offices"],
        },
        {
            "name": "working_netherlands",
            "url": "https://www.tempo-team.nl/werknemers/aan-het-werk/working-in-the-netherlands",
            "functions": ["email"],
        },
        {
            "name": "terms",
            "url": "https://www.tempo-team.nl/over-tempo-team/voorwaarden/gebruikersvoorwaarden-algemeen",
            "functions": ["legal"],
        },
        {
            "name": "branches",
            "url": "https://www.tempo-team.nl/werkgevers/onze-branches",
            "functions": ["sectors"],
        },
        {
            "name": "vacancies",
            "url": "https://www.tempo-team.nl/vacatures",
            "functions": ["sectors_secondary"],
        },
        {
            "name": "pricing",
            "url": "https://www.tempo-team.nl/werkgevers/onze-diensten/tarieven",
            "functions": ["pricing"],
        },
        {
            "name": "services_overview",
            "url": "https://www.tempo-team.nl/werkgevers/onze-diensten",
            "functions": ["services"],
        },
    ]
    
    # Chatbot API to check (not an HTML page)
    CHATBOT_API_URL = "https://api.seamly-app.com/channels/api/v2/client/72ea2296-3cb3-4ad5-9dea-aa85ee1397cc/configs"

    def scrape(self) -> Agency:
        self.logger.info(f"Starting scrape of {self.AGENCY_NAME}")
        
        # Initialize utils
        self.utils = AgencyScraperUtils(logger=self.logger)
        agency = self.create_base_agency()
        agency.employers_page_url = f"{self.WEBSITE_URL}/werkgevers"
        agency.contact_form_url = f"{self.WEBSITE_URL}/werkgevers/contact/contactformulier"
        
        # Add contact form URL to evidence (avoid duplicates)
        if agency.contact_form_url not in self.evidence_urls:
            self.evidence_urls.append(agency.contact_form_url)
        
        all_sectors = set()
        all_sectors_secondary = set()
        page_texts = {}
        
        for page in self.PAGES_TO_SCRAPE:
            url = page["url"]
            page_name = page["name"]
            functions = page.get("functions", [])
            
            try:
                self.logger.info("=" * 80)
                self.logger.info(f"📄 PROCESSING: {page_name}")
                self.logger.info(f"🔗 URL: {url}")
                self.logger.info("-" * 80)
                
                soup = self.fetch_page(url)
                page_text = soup.get_text(separator=" ", strip=True)
                page_texts[url] = page_text
                
                self._apply_functions(agency, functions, soup, page_text, all_sectors, all_sectors_secondary, url)
                
                self.logger.info(f"✅ Completed: {page_name}")
                
            except Exception as e:
                self.logger.error(f"❌ Error scraping {url}: {e}")
        
        # Extract common fields
        all_text = " ".join(page_texts.values())
        self.extract_all_common_fields(agency, all_text)
        
        # Check for chatbot API
        self._check_chatbot_api(agency)
        
        # Finalize
        if all_sectors:
            agency.sectors_core = sorted(list(all_sectors))
        
        if all_sectors_secondary:
            agency.sectors_secondary = sorted(list(all_sectors_secondary))
        
        agency.evidence_urls = list(self.evidence_urls)
        agency.collected_at = self.collected_at
        
        self.logger.info("=" * 80)
        self.logger.info(f"✅ Completed scrape of {self.AGENCY_NAME}")
        self.logger.info(f"📄 Evidence URLs: {len(agency.evidence_urls)}")
        self.logger.info("=" * 80)
        
        return agency
    
    def _apply_functions(
        self,
        agency: Agency,
        functions: List[str],
        soup: BeautifulSoup,
        page_text: str,
        all_sectors: Set[str],
        all_sectors_secondary: Set[str],
        url: str,
    ) -> None:
        """Apply extraction functions."""
        for func_name in functions:
            if func_name == "logo":
                # Try to extract logo from JSON-LD first
                json_ld_script = soup.find('script', type='application/ld+json')
                if json_ld_script and json_ld_script.string:
                    try:
                        json_data = json.loads(json_ld_script.string)
                        if 'logo' in json_data:
                            agency.logo_url = json_data['logo']
                            self.logger.info(f"✓ Logo from JSON-LD: {agency.logo_url} | Source: {url}")
                        else:
                            # Fallback to standard extraction
                            logo = self.utils.fetch_logo(soup, url)
                            if logo:
                                agency.logo_url = logo
                                self.logger.info(f"✓ Found logo: {logo} | Source: {url}")
                    except json.JSONDecodeError as e:
                        self.logger.warning(f"⚠ Failed to parse JSON-LD: {e}")
                        # Fallback to standard extraction
                        logo = self.utils.fetch_logo(soup, url)
                        if logo:
                            agency.logo_url = logo
                            self.logger.info(f"✓ Found logo: {logo} | Source: {url}")
                else:
                    # Fallback to standard extraction if no JSON-LD
                    logo = self.utils.fetch_logo(soup, url)
                    if logo:
                        agency.logo_url = logo
                        self.logger.info(f"✓ Found logo: {logo} | Source: {url}")
            
            elif func_name == "header":
                self._extract_header(soup, agency, url)
            
            elif func_name == "footer":
                self._extract_footer(soup, agency, all_sectors, url)
            
            elif func_name == "about":
                self._extract_about(soup, page_text, agency, url)
            
            elif func_name == "certifications":
                self._extract_certifications(soup, page_text, agency, url)
            
            elif func_name == "contact":
                self._extract_contact(soup, page_text, agency, url)
            
            elif func_name == "history":
                self._extract_history(soup, page_text, agency, url)
            
            elif func_name == "mission":
                self._extract_mission(soup, page_text, agency, url)
            
            elif func_name == "offices":
                self._extract_offices(soup, agency, url)
            
            elif func_name == "email":
                self._extract_email(soup, agency, url)
            
            elif func_name == "legal":
                self._extract_legal(soup, agency, url)
            
            elif func_name == "sectors":
                self._extract_sectors(soup, agency, all_sectors, url)
            
            elif func_name == "sectors_secondary":
                self._extract_sectors_secondary(soup, agency, all_sectors, all_sectors_secondary, url)
            
            elif func_name == "pricing":
                self._extract_pricing(soup, page_text, agency, url)
            
            elif func_name == "services":
                self._extract_services(soup, agency, url)
    
    def _extract_header(self, soup: BeautifulSoup, agency: Agency, url: str) -> None:
        """Extract data from header navigation."""
        try:
            self.logger.info(f"🔍 Extracting header data from: {url}")
            
            # Extract candidate portal link
            candidate_portal_links = soup.find_all('a', href=re.compile(r'/(t-point|inschrijving|mijn-tempo-team)', re.IGNORECASE))
            if candidate_portal_links:
                if not agency.digital_capabilities:
                    agency.digital_capabilities = DigitalCapabilities()
                agency.digital_capabilities.candidate_portal = True
                for link in candidate_portal_links:
                    self.logger.info(f"✓ Found candidate portal link: {link.get('href')} | Source: {url}")
            
            # Extract employer section link
            employer_links = soup.find_all('a', href=re.compile(r'/(werkgevers|personeel-gezocht)', re.IGNORECASE))
            if employer_links:
                for link in employer_links:
                    href = link.get('href', '')
                    if 'werkgevers' in href.lower() or 'personeel-gezocht' in href.lower():
                        if not agency.digital_capabilities:
                            agency.digital_capabilities = DigitalCapabilities()
                        agency.digital_capabilities.client_portal = True
                        self.logger.info(f"✓ Found employer section: {href} | Source: {url}")
                        break
            
            # Check for English language support
            english_links = soup.find_all('a', href=re.compile(r'/english/', re.IGNORECASE))
            if english_links:
                if not agency.growth_signals:
                    agency.growth_signals = []
                if 'meertalig' not in agency.growth_signals:
                    agency.growth_signals.append('meertalig')
                self.logger.info(f"✓ Multi-language support detected (English) | Source: {url}")
        
        except Exception as e:
            self.logger.error(f"❌ Error extracting header: {e}")
    
    def _extract_footer(self, soup: BeautifulSoup, agency: Agency, all_sectors: Set[str], url: str) -> None:
        """
        Extract data from footer:
        - Sectors from 'Per vakgebied' section
        - Mobile app links
        - Social media presence
        - Contact pages
        """
        try:
            self.logger.info(f"🔍 Extracting footer data from: {url}")
            
            footer = soup.find('footer', class_='page-footer')
            if not footer:
                self.logger.warning(f"⚠ Footer not found | Source: {url}")
                return
            
            # Extract sectors from "Per vakgebied" footer links
            vakgebied_section = None
            for h4 in footer.find_all('h4', class_='footer-nav__header'):
                if 'vakgebied' in h4.get_text().lower():
                    vakgebied_section = h4.find_parent('div', class_='footer-nav__column')
                    break
            
            if vakgebied_section:
                sector_links = vakgebied_section.find_all('a')
                self.logger.info(f"   Found {len(sector_links)} sector links in footer")
                
                for link in sector_links:
                    sector_text = link.get_text(strip=True)
                    # Use utils to normalize sector name
                    normalized_sector = self.utils.normalize_sector_name(sector_text)
                    if normalized_sector:
                        all_sectors.add(normalized_sector)
                        self.logger.info(f"✓ Sector: {sector_text} → {normalized_sector} | Source: {url}")
            
            # Extract mobile app presence
            app_store_link = footer.find('a', href=re.compile(r'apps\.apple\.com', re.IGNORECASE))
            google_play_link = footer.find('a', href=re.compile(r'play\.google\.com', re.IGNORECASE))
            
            if app_store_link or google_play_link:
                if not agency.digital_capabilities:
                    agency.digital_capabilities = DigitalCapabilities()
                agency.digital_capabilities.mobile_app = True
                platforms = []
                if app_store_link:
                    platforms.append("iOS")
                if google_play_link:
                    platforms.append("Android")
                self.logger.info(f"✓ Mobile app available: {', '.join(platforms)} | Source: {url}")
            
            # Extract social media presence
            social_links = {
                'facebook': footer.find('a', href=re.compile(r'facebook\.com', re.IGNORECASE)),
                'instagram': footer.find('a', href=re.compile(r'instagram\.com', re.IGNORECASE)),
                'linkedin': footer.find('a', href=re.compile(r'linkedin\.com', re.IGNORECASE)),
                'youtube': footer.find('a', href=re.compile(r'youtube\.com', re.IGNORECASE)),
                'tiktok': footer.find('a', href=re.compile(r'tiktok\.com', re.IGNORECASE)),
            }
            
            found_social = [platform for platform, link in social_links.items() if link]
            if found_social:
                if not agency.growth_signals:
                    agency.growth_signals = []
                signal = f"actief_op_{len(found_social)}_social_media_platforms"
                if signal not in agency.growth_signals:
                    agency.growth_signals.append(signal)
                self.logger.info(f"✓ Social media: {', '.join(found_social)} | Source: {url}")
        
        except Exception as e:
            self.logger.error(f"❌ Error extracting footer: {e}")
    
    def _extract_about(self, soup: BeautifulSoup, page_text: str, agency: Agency, url: str) -> None:
        """Extract company information from about page."""
        try:
            self.logger.info(f"🔍 Extracting about/organization data from: {url}")
            # Implementation will be added based on actual page content
        
        except Exception as e:
            self.logger.error(f"❌ Error extracting about: {e}")
    
    def _extract_certifications(self, soup: BeautifulSoup, page_text: str, agency: Agency, url: str) -> None:
        """
        Extract certifications from certification page:
        - ABU membership
        - NEN 4400-1 (SNA)
        - ISO 9001, ISO 14001, ISO 27001, ISO 27701
        - VCA
        """
        try:
            self.logger.info(f"🔍 Extracting certifications from: {url}")
            
            # Use page_text instead of just first article to capture all sections
            article_text = page_text
            
            if not agency.certifications:
                agency.certifications = []
            
            # ABU membership
            if re.search(r'\bABU\b', article_text):
                cert = "ABU"
                if cert not in agency.certifications:
                    agency.certifications.append(cert)
                    self.logger.info(f"✓ Certification: {cert} | Source: {url}")
            
            # NEN 4400-1 / SNA
            if re.search(r'NEN\s*4400-1|SNA', article_text, re.IGNORECASE):
                cert = "NEN_4400_1"
                if cert not in agency.certifications:
                    agency.certifications.append(cert)
                    self.logger.info(f"✓ Certification: {cert} (SNA) | Source: {url}")
            
            # ISO certifications
            iso_patterns = {
                "ISO_9001": r'ISO\s*9001',
                "ISO_14001": r'ISO\s*14001',
                "ISO_27001": r'ISO\s*27001',
                "ISO_27701": r'ISO[/\s]*IEC\s*27701',
            }
            
            for cert_key, pattern in iso_patterns.items():
                if re.search(pattern, article_text, re.IGNORECASE):
                    if cert_key not in agency.certifications:
                        agency.certifications.append(cert_key)
                        self.logger.info(f"✓ Certification: {cert_key} | Source: {url}")
            
            # VCA
            if re.search(r'\bVCA\b', article_text):
                cert = "VCA"
                if cert not in agency.certifications:
                    agency.certifications.append(cert)
                    self.logger.info(f"✓ Certification: {cert} | Source: {url}")
        
        except Exception as e:
            self.logger.error(f"❌ Error extracting certifications: {e}")
            import traceback
            self.logger.error(f"   Traceback: {traceback.format_exc()}")
    
    def _extract_contact(self, soup: BeautifulSoup, page_text: str, agency: Agency, url: str) -> None:
        """
        Extract contact information from contact page:
        - HQ address (street, zip, city)
        - Contact phone number
        - Postal address
        """
        try:
            self.logger.info(f"🔍 Extracting contact information from: {url}")
            
            # Find the article containing address information (check all articles)
            articles = soup.find_all('article')
            article_text = None
            for art in articles:
                text = art.get_text(separator="\n", strip=True)
                if 'Diemermere' in text or 'hoofdkantoor' in text.lower() or 'Postbus' in text:
                    article_text = text
                    break
            
            if not article_text:
                self.logger.warning(f"⚠ Address article not found | Source: {url}")
                return
            
            # Extract phone number
            # Pattern: "Phone: 020 569 59 22" or "Telefoon: 020 569 59 22"
            phone_match = re.search(r'(?:Phone|Telefoon)[:\s]+(\+?\d{2,3}[\s\-]?\d{3}[\s\-]?\d{2}[\s\-]?\d{2})', article_text, re.IGNORECASE)
            if phone_match:
                agency.contact_phone = phone_match.group(1).strip().replace('-', ' ')
                self.logger.info(f"✓ Contact phone: {agency.contact_phone} | Source: {url}")
            
            # Extract HQ address
            # Pattern: "Tempo-Team\nDiemermere 25\n1112 TC Diemen"
            # Look for street address followed by zip code and city
            address_match = re.search(
                r'Tempo-Team\s*\n\s*([^\n]+?)\s*\n\s*(\d{4}\s*[A-Z]{2})\s+([^\n]+?)(?:\s*\n|$)',
                article_text,
                re.IGNORECASE
            )
            
            if address_match:
                street = address_match.group(1).strip()
                zip_code = address_match.group(2).strip()
                city = address_match.group(3).strip()
                
                # Create HQ office location
                province = self.utils.map_city_to_province(city)
                
                hq_office = OfficeLocation(
                    city=city,
                    province=province
                )
                
                if not agency.office_locations:
                    agency.office_locations = []
                
                # Add as first office (HQ)
                agency.office_locations.insert(0, hq_office)
                agency.hq_city = city
                agency.hq_province = province
                
                self.logger.info(f"✓ HQ Address: {street}, {zip_code} {city} | Source: {url}")
                self.logger.info(f"✓ HQ City: {city}, Province: {province}")
            
            # Extract postal address (P.O. Box)
            postal_match = re.search(
                r'(?:Postbus|P\.O\. Box)\s+(\d+)\s*\n\s*(\d{4}\s*[A-Z]{2})\s+([^\n]+)',
                article_text,
                re.IGNORECASE
            )
            
            if postal_match:
                po_box = postal_match.group(1).strip()
                postal_zip = postal_match.group(2).strip()
                postal_city = postal_match.group(3).strip()
                self.logger.info(f"✓ Postal address: P.O. Box {po_box}, {postal_zip} {postal_city} | Source: {url}")
        
        except Exception as e:
            self.logger.error(f"❌ Error extracting contact: {e}")
            import traceback
            self.logger.error(f"   Traceback: {traceback.format_exc()}")
    
    def _extract_history(self, soup: BeautifulSoup, page_text: str, agency: Agency, url: str) -> None:
        """
        Extract data from history/geschiedenis page:
        - Founding year (1969)
        - Brand group confirmation (Randstad, since 1983)
        - Market position (second largest in NL)
        - International expansion (Belgium, Luxembourg, Portugal, Germany)
        - Historical milestones as growth signals
        """
        try:
            self.logger.info(f"🔍 Extracting history data from: {url}")
            
            article = soup.find('article')
            if not article:
                self.logger.warning(f"⚠ Article section not found | Source: {url}")
                return
            
            article_text = article.get_text(separator=" ", strip=True)
            
            # Extract founding year
            founding_match = re.search(r'25\s+maart\s+1969|March\s+25,?\s+1969', article_text, re.IGNORECASE)
            if founding_match:
                if not agency.growth_signals:
                    agency.growth_signals = []
                signal = "sinds_1969_actief"
                if signal not in agency.growth_signals:
                    agency.growth_signals.append(signal)
                self.logger.info(f"✓ Founding year: 1969 → {signal} | Source: {url}")
            
            # Confirm brand group (Randstad acquisition in 1983)
            randstad_match = re.search(r'Randstad.*(?:takes over|neemt.*over).*Tempo-Team.*1983', article_text, re.IGNORECASE)
            if randstad_match:
                if not agency.growth_signals:
                    agency.growth_signals = []
                signal = "onderdeel_van_randstad_sinds_1983"
                if signal not in agency.growth_signals:
                    agency.growth_signals.append(signal)
                self.logger.info(f"✓ Brand group confirmed: Randstad (since 1983) → {signal} | Source: {url}")
            
            # Extract market position
            market_position_match = re.search(
                r'(?:op één na grootste|second largest).*(?:uitzendbureau|employment agency).*(?:Nederland|Netherlands)',
                article_text,
                re.IGNORECASE
            )
            if market_position_match:
                if not agency.growth_signals:
                    agency.growth_signals = []
                signal = "tweede_grootste_uitzendbureau_nl"
                if signal not in agency.growth_signals:
                    agency.growth_signals.append(signal)
                self.logger.info(f"✓ Market position: Second largest in NL → {signal} | Source: {url}")
            
            # Extract international expansion
            countries = []
            if re.search(r'(?:Belgium|België)', article_text, re.IGNORECASE):
                countries.append("Belgium")
            if re.search(r'Luxemb(?:ou)?rg', article_text, re.IGNORECASE):
                countries.append("Luxembourg")
            if re.search(r'Portugal', article_text, re.IGNORECASE):
                countries.append("Portugal")
            if re.search(r'(?:Germany|Duitsland)', article_text, re.IGNORECASE):
                countries.append("Germany")
            
            if countries:
                if not agency.growth_signals:
                    agency.growth_signals = []
                signal = f"internationaal_actief_{len(countries)}_landen"
                if signal not in agency.growth_signals:
                    agency.growth_signals.append(signal)
                self.logger.info(f"✓ International expansion: {', '.join(countries)} → {signal} | Source: {url}")
            
            # Extract Vedior integration (2007)
            vedior_match = re.search(r'Vedior.*(?:2007|2008)', article_text, re.IGNORECASE)
            if vedior_match:
                if not agency.growth_signals:
                    agency.growth_signals = []
                signal = "vedior_integratie_2007"
                if signal not in agency.growth_signals:
                    agency.growth_signals.append(signal)
                self.logger.info(f"✓ Vedior integration (2007-2008) → {signal} | Source: {url}")
        
        except Exception as e:
            self.logger.error(f"❌ Error extracting history: {e}")
            import traceback
            self.logger.error(f"   Traceback: {traceback.format_exc()}")
    
    def _extract_mission(self, soup: BeautifulSoup, page_text: str, agency: Agency, url: str) -> None:
        """
        Extract data from mission/vision page:
        - Company experience (50 years)
        - Labor market experts count (hundreds)
        - Value propositions (digital-driven, personal attention)
        """
        try:
            self.logger.info(f"🔍 Extracting mission data from: {url}")
            
            article = soup.find('article')
            if not article:
                self.logger.warning(f"⚠ Article section not found | Source: {url}")
                return
            
            article_text = article.get_text(separator=" ", strip=True)
            
            # Extract experience years
            experience_match = re.search(r'50\s+(?:jaar|years?).*(?:ervaring|experience)', article_text, re.IGNORECASE)
            if experience_match:
                if not agency.growth_signals:
                    agency.growth_signals = []
                signal = "50_jaar_ervaring"
                if signal not in agency.growth_signals:
                    agency.growth_signals.append(signal)
                self.logger.info(f"✓ Experience: 50 years → {signal} | Source: {url}")
            
            # Extract labor market experts
            experts_match = re.search(r'honderden\s+arbeidsmarkt-experts|hundreds.*(?:labor|labour)\s+market\s+experts', article_text, re.IGNORECASE)
            if experts_match:
                if not agency.growth_signals:
                    agency.growth_signals = []
                signal = "honderden_arbeidsmarkt_experts"
                if signal not in agency.growth_signals:
                    agency.growth_signals.append(signal)
                self.logger.info(f"✓ Labor market experts: Hundreds → {signal} | Source: {url}")
            
            # Extract "digitally driven" value proposition
            digital_match = re.search(r'digitaal\s+gedreven|digital(?:ly)?\s+driven', article_text, re.IGNORECASE)
            if digital_match:
                if not agency.growth_signals:
                    agency.growth_signals = []
                signal = "digitaal_gedreven"
                if signal not in agency.growth_signals:
                    agency.growth_signals.append(signal)
                self.logger.info(f"✓ Value proposition: Digitally driven → {signal} | Source: {url}")
            
            # Extract "personal attention" value proposition
            personal_match = re.search(r'persoonlijke\s+aandacht|personal\s+attention', article_text, re.IGNORECASE)
            if personal_match:
                if not agency.growth_signals:
                    agency.growth_signals = []
                signal = "persoonlijke_aandacht"
                if signal not in agency.growth_signals:
                    agency.growth_signals.append(signal)
                self.logger.info(f"✓ Value proposition: Personal attention → {signal} | Source: {url}")
        
        except Exception as e:
            self.logger.error(f"❌ Error extracting mission: {e}")
            import traceback
            self.logger.error(f"   Traceback: {traceback.format_exc()}")
    
    def _extract_offices(self, soup: BeautifulSoup, agency: Agency, url: str) -> None:
        """
        Extract all office locations from the vestigingen (offices) page.
        Each office card contains:
        - City name (h2)
        - Phone number (optional, in li with phone icon)
        - Street address (in li with marker icon)
        """
        try:
            self.logger.info(f"🔍 Extracting office locations from: {url}")
            
            # Find all office cards
            office_cards = soup.find_all('article', class_='card')
            if not office_cards:
                self.logger.warning(f"⚠ No office cards found | Source: {url}")
                return
            
            self.logger.info(f"   Found {len(office_cards)} office cards")
            
            if not agency.office_locations:
                agency.office_locations = []
            
            offices_added = 0
            for card in office_cards:
                try:
                    # Extract city name from h2
                    city_elem = card.find('h2', class_='card__header')
                    if not city_elem:
                        continue
                    
                    city = city_elem.get_text(strip=True)
                    
                    # Extract phone and address from metalist items
                    metalist_items = card.find_all('li', class_='metalist__item')
                    
                    phone = None
                    street = None
                    
                    for item in metalist_items:
                        item_text = item.get_text(strip=True)
                        # Check if this item contains a phone icon (phone number)
                        if item.find('use', href=re.compile(r'#phone')):
                            phone = item_text
                        # Check if this item contains a marker icon (address)
                        elif item.find('use', href=re.compile(r'#marker')):
                            street = item_text
                    
                    # Get province from city
                    province = self.utils.map_city_to_province(city)
                    
                    # Check if this office already exists (avoid duplicates)
                    office_exists = any(
                        office.city == city
                        for office in agency.office_locations
                    )
                    
                    if not office_exists:
                        office = OfficeLocation(
                            city=city,
                            province=province
                        )
                        agency.office_locations.append(office)
                        offices_added += 1
                        
                        # Log the extraction
                        log_msg = f"✓ Office: {city}"
                        if province:
                            log_msg += f" ({province})"
                        if phone:
                            log_msg += f", Phone: {phone}"
                        if street:
                            log_msg += f", Address: {street}"
                        self.logger.info(f"{log_msg} | Source: {url}")
                
                except Exception as e:
                    self.logger.warning(f"⚠ Error extracting office card: {e}")
                    continue
            
            self.logger.info(f"✅ Extracted {offices_added} office locations | Source: {url}")
            
            # Add national coverage signal if we have many offices
            if len(agency.office_locations) >= 30:
                if not agency.growth_signals:
                    agency.growth_signals = []
                signal = f"landelijke_dekking_{len(agency.office_locations)}_vestigingen"
                if signal not in agency.growth_signals:
                    agency.growth_signals.append(signal)
                    self.logger.info(f"✓ Growth signal: {signal} | Source: {url}")
        
        except Exception as e:
            self.logger.error(f"❌ Error extracting offices: {e}")
            import traceback
            self.logger.error(f"   Traceback: {traceback.format_exc()}")
    
    def _extract_email(self, soup: BeautifulSoup, agency: Agency, url: str) -> None:
        """
        Extract contact email from the working in Netherlands page.
        This page contains recruitment emails for international candidates:
        - Main email: lookingforajob@tempo-team.nl (EU-flex with housing)
        - Regional recruitment emails
        """
        try:
            self.logger.info(f"🔍 Extracting email from: {url}")
            
            # Find all links that contain @tempo-team.nl (including both mailto: and http:// formats)
            all_links = soup.find_all('a', href=re.compile(r'@tempo-team\.nl', re.IGNORECASE))
            
            if all_links:
                # Priority 1: lookingforajob email (main recruitment email for EU-flex)
                for link in all_links:
                    href = link.get('href', '')
                    if 'lookingforajob' in href.lower():
                        # Extract email from href (handle both mailto: and http:// formats)
                        email = href.replace('mailto:', '').replace('http://', '').replace('https://', '').strip()
                        agency.contact_email = email
                        self.logger.info(f"✓ Main recruitment email (EU-flex): {email} | Source: {url}")
                        break
                
                # Priority 2: If no main email found, use the first mailto: link
                if not agency.contact_email:
                    for link in all_links:
                        href = link.get('href', '')
                        if href.startswith('mailto:'):
                            email = href.replace('mailto:', '').strip()
                            agency.contact_email = email
                            self.logger.info(f"✓ Contact email: {email} | Source: {url}")
                            break
                
                # Extract EU-flex service as growth signal
                article = soup.find('article')
                if article:
                    article_text = article.get_text(separator=" ", strip=True)
                    if re.search(r'EU[-\s]?flex|housing.*work|jobs with housing', article_text, re.IGNORECASE):
                        if not agency.growth_signals:
                            agency.growth_signals = []
                        signal = "eu_flex_dienst_huisvesting_en_werk"
                        if signal not in agency.growth_signals:
                            agency.growth_signals.append(signal)
                            self.logger.info(f"✓ EU-flex service (housing + jobs) → {signal} | Source: {url}")
        
        except Exception as e:
            self.logger.error(f"❌ Error extracting email: {e}")
            import traceback
            self.logger.error(f"   Traceback: {traceback.format_exc()}")
    
    def _extract_legal(self, soup: BeautifulSoup, agency: Agency, url: str) -> None:
        """
        Extract legal information from terms & conditions page:
        - KvK number (Chamber of Commerce registration)
        - Legal entity name
        - Registered office city
        """
        try:
            self.logger.info(f"🔍 Extracting legal information from: {url}")
            
            article = soup.find('article')
            if not article:
                self.logger.warning(f"⚠ Article section not found | Source: {url}")
                return
            
            article_text = article.get_text(separator=" ", strip=True)
            
            # Extract KvK number
            # Pattern: "Kamer van Koophandel ... onder nummer 33041895"
            kvk_match = re.search(
                r'(?:Kamer van Koophandel|Chamber of Commerce).*?(?:onder )?(?:nummer|number)[:\s]+(\d{8})',
                article_text,
                re.IGNORECASE
            )
            if kvk_match:
                agency.kvk_number = kvk_match.group(1).strip()
                self.logger.info(f"✓ KvK number: {agency.kvk_number} | Source: {url}")
            
            # Extract legal name
            # Pattern: "Tempo-Team Group bv"
            legal_name_match = re.search(
                r'(Tempo-Team Group (?:bv|B\.V\.))',
                article_text,
                re.IGNORECASE
            )
            if legal_name_match:
                agency.legal_name = legal_name_match.group(1).strip()
                self.logger.info(f"✓ Legal name: {agency.legal_name} | Source: {url}")
        
        except Exception as e:
            self.logger.error(f"❌ Error extracting legal info: {e}")
            import traceback
            self.logger.error(f"   Traceback: {traceback.format_exc()}")
    
    def _extract_sectors(self, soup: BeautifulSoup, agency: Agency, all_sectors: Set[str], url: str) -> None:
        """
        Extract main sectors from the branches/specializations page.
        This page lists all core sectors that Tempo-Team specializes in.
        Uses BeautifulSoup to extract sector names from the linklist.
        """
        try:
            self.logger.info(f"🔍 Extracting sectors from: {url}")
            
            # Find the linklist containing sectors
            linklist = soup.find('ul', class_='linklist')
            if not linklist:
                self.logger.warning(f"⚠ Linklist not found | Source: {url}")
                return
            
            # Find all linklist items
            items = linklist.find_all('li', class_='linklist__item')
            if not items:
                self.logger.warning(f"⚠ No linklist items found | Source: {url}")
                return
            
            self.logger.info(f"   Found {len(items)} sector items")
            
            sectors_found = []
            for item in items:
                # Extract sector name from span
                title_span = item.find('span', class_='linklist__title')
                if title_span:
                    sector_text = title_span.get_text(strip=True)
                    sectors_found.append(sector_text)
            
            if sectors_found:
                self.logger.info(f"   Raw sectors found: {sectors_found}")
                
                # Join all sector texts and extract using utils
                combined_text = " ".join(sectors_found)
                normalized_sectors = self.utils.fetch_sectors(combined_text, url)
                
                if normalized_sectors:
                    for sector in normalized_sectors:
                        all_sectors.add(sector)
                    self.logger.info(f"✓ Extracted {len(normalized_sectors)} sectors: {normalized_sectors} | Source: {url}")
                else:
                    self.logger.warning(f"⚠ No normalized sectors found from: {sectors_found}")
            else:
                self.logger.warning(f"⚠ No sectors extracted | Source: {url}")
        
        except Exception as e:
            self.logger.error(f"❌ Error extracting sectors: {e}")
            import traceback
            self.logger.error(f"   Traceback: {traceback.format_exc()}")
    
    def _extract_sectors_secondary(
        self, 
        soup: BeautifulSoup, 
        agency: Agency, 
        all_sectors: Set[str],
        all_sectors_secondary: Set[str],
        url: str
    ) -> None:
        """
        Extract secondary sectors from the vacancies filter page.
        These are sectors where Tempo-Team has job listings but aren't core specializations.
        Filters out sectors already in sectors_core.
        """
        try:
            self.logger.info(f"🔍 Extracting secondary sectors from vacancies filter: {url}")
            
            # Find all checkbox elements for sectors
            checkboxes = soup.find_all('div', class_='form-group__input rb-checkbox')
            if not checkboxes:
                self.logger.warning(f"⚠ No sector checkboxes found | Source: {url}")
                return
            
            self.logger.info(f"   Found {len(checkboxes)} sector filter checkboxes")
            
            sectors_found = []
            for checkbox in checkboxes:
                # Find the label span
                label_span = checkbox.find('span', class_='rb-checkbox__label')
                if label_span:
                    # Get the text, excluding the suffix span (job count)
                    # Clone and remove the suffix span to get clean sector name
                    label_text = label_span.get_text(strip=True)
                    # Remove the count suffix (e.g., "159", "1.093")
                    # The count is in a nested span, so we need to extract just the sector name
                    suffix_span = label_span.find('span', class_='rb-checkbox__label--suffix')
                    if suffix_span:
                        suffix_text = suffix_span.get_text(strip=True)
                        # Remove suffix from label text
                        sector_text = label_text.replace(suffix_text, '').strip()
                    else:
                        sector_text = label_text
                    
                    if sector_text and sector_text not in ['Toon', '']:
                        sectors_found.append(sector_text)
            
            if sectors_found:
                self.logger.info(f"   Raw secondary sectors found: {len(sectors_found)} items")
                
                # Join all sector texts and extract using utils
                combined_text = " ".join(sectors_found)
                normalized_sectors = self.utils.fetch_sectors(combined_text, url)
                
                if normalized_sectors:
                    # Filter out sectors already in sectors_core
                    secondary_only = [s for s in normalized_sectors if s not in all_sectors]
                    
                    if secondary_only:
                        for sector in secondary_only:
                            all_sectors_secondary.add(sector)
                        self.logger.info(
                            f"✓ Extracted {len(secondary_only)} secondary sectors "
                            f"(filtered {len(normalized_sectors) - len(secondary_only)} core overlaps) | Source: {url}"
                        )
                    else:
                        self.logger.info(f"   All {len(normalized_sectors)} sectors are already in sectors_core")
                else:
                    self.logger.warning(f"⚠ No normalized sectors found from: {len(sectors_found)} raw items")
            else:
                self.logger.warning(f"⚠ No sectors extracted from checkboxes | Source: {url}")
        
        except Exception as e:
            self.logger.error(f"❌ Error extracting secondary sectors: {e}")
            import traceback
            self.logger.error(f"   Traceback: {traceback.format_exc()}")
    
    def _extract_services(self, soup: BeautifulSoup, agency: Agency, url: str) -> None:
        """
        Extract services offered by the agency from the services overview page.
        Uses BeautifulSoup to parse the linklist and map to AgencyServices fields.
        """
        try:
            self.logger.info(f"🔍 Extracting services from: {url}")
            
            # Find the services linklist
            linklist = soup.find('ul', class_='linklist')
            if not linklist:
                self.logger.warning(f"⚠ Services linklist not found | Source: {url}")
                return
            
            # Extract all service titles
            service_items = linklist.find_all('li', class_='linklist__item')
            if not service_items:
                self.logger.warning(f"⚠ No service items found | Source: {url}")
                return
            
            services_found = []
            for item in service_items:
                title_span = item.find('span', class_='linklist__title')
                if title_span:
                    service_text = title_span.get_text(strip=True)
                    services_found.append(service_text)
            
            self.logger.info(f"   Found {len(services_found)} services: {services_found}")
            
            # Use the utility method to extract and map services
            if services_found:
                combined_text = " ".join(services_found)
                agency.services = self.utils.fetch_services(combined_text, url)
                
                # Log what was detected
                if agency.services:
                    detected = []
                    if agency.services.uitzenden:
                        detected.append("uitzenden")
                    if agency.services.detacheren:
                        detected.append("detacheren")
                    if agency.services.werving_selectie:
                        detected.append("werving_selectie")
                    if agency.services.payrolling:
                        detected.append("payrolling")
                    if agency.services.zzp_bemiddeling:
                        detected.append("zzp_bemiddeling")
                    if agency.services.msp:
                        detected.append("msp")
                    if agency.services.rpo:
                        detected.append("rpo")
                    if agency.services.executive_search:
                        detected.append("executive_search")
                    
                    self.logger.info(f"✓ Services mapped: {detected} | Source: {url}")
                
                # Add growth signals for additional services
                if not agency.growth_signals:
                    agency.growth_signals = []
                
                # Check for advanced HR services
                text_lower = combined_text.lower()
                if "performance management" in text_lower or "performance coaching" in text_lower:
                    signal = "performance_management_diensten"
                    if signal not in agency.growth_signals:
                        agency.growth_signals.append(signal)
                        self.logger.info(f"✓ Advanced service: performance management | Source: {url}")
                
                if "poolmanagement" in text_lower or "pool management" in text_lower:
                    signal = "poolmanagement_dienst"
                    if signal not in agency.growth_signals:
                        agency.growth_signals.append(signal)
                        self.logger.info(f"✓ Advanced service: poolmanagement | Source: {url}")
                
                if "participatie" in text_lower or "participation" in text_lower:
                    signal = "participatie_dienst"
                    if signal not in agency.growth_signals:
                        agency.growth_signals.append(signal)
                        self.logger.info(f"✓ Social responsibility service: participatie | Source: {url}")
                
                if "inhouse" in text_lower:
                    signal = "inhouse_services"
                    if signal not in agency.growth_signals:
                        agency.growth_signals.append(signal)
                        self.logger.info(f"✓ Advanced service: inhouse services | Source: {url}")
                
                if "consultancy" in text_lower:
                    signal = "hr_consultancy"
                    if signal not in agency.growth_signals:
                        agency.growth_signals.append(signal)
                        self.logger.info(f"✓ Advisory service: HR consultancy | Source: {url}")
        
        except Exception as e:
            self.logger.error(f"❌ Error extracting services: {e}")
            import traceback
            self.logger.error(f"   Traceback: {traceback.format_exc()}")
    
    def _extract_pricing(self, soup: BeautifulSoup, page_text: str, agency: Agency, url: str) -> None:
        """
        Extract pricing information from the tarieven (pricing) page.
        
        Extracts:
        - Omrekenfactor range (markup multiplier)
        - Recruitment fee percentage
        - No cure no pay policy
        - Pricing transparency level
        - Response time (speed claim)
        """
        try:
            self.logger.info(f"🔍 Extracting pricing information from: {url}")
            
            # Extract omrekenfactor range from examples
            # Example: "De omrekenfactor = 2,4" and "De omrekenfactor: 2,5"
            omrekenfactor_values = []
            omrekenfactor_matches = re.findall(
                r'omrekenfactor[:\s=]+(\d+[,\.]\d+)',
                page_text,
                re.IGNORECASE
            )
            
            for match in omrekenfactor_matches:
                value = float(match.replace(',', '.'))
                omrekenfactor_values.append(value)
            
            if omrekenfactor_values:
                min_factor = min(omrekenfactor_values)
                max_factor = max(omrekenfactor_values)
                agency.omrekenfactor_min = min_factor
                agency.omrekenfactor_max = max_factor
                
                # Calculate average markup factor
                avg_factor = (min_factor + max_factor) / 2
                agency.avg_markup_factor = round(avg_factor, 2)
                
                self.logger.info(f"✓ Omrekenfactor range: {min_factor} - {max_factor} (avg: {avg_factor:.2f}) | Source: {url}")
            
            # Extract recruitment fee percentage
            # Pattern: "gemiddeld rond de 25% liggen"
            recruitment_fee_match = re.search(
                r'(?:gemiddeld|ongeveer|rond)\s+(?:de\s+)?(\d+)%',
                page_text,
                re.IGNORECASE
            )
            if recruitment_fee_match:
                fee_pct = int(recruitment_fee_match.group(1))
                # Store as a string hint for now (could be added to model later)
                if not agency.growth_signals:
                    agency.growth_signals = []
                signal = f"werving_selectie_fee_{fee_pct}_procent"
                if signal not in agency.growth_signals:
                    agency.growth_signals.append(signal)
                self.logger.info(f"✓ Recruitment fee: ~{fee_pct}% | Source: {url}")
            
            # Check for no cure no pay
            # Pattern: "Je betaalt de fee pas wanneer je iemand hebt aangenomen"
            if "pas wanneer" in page_text.lower() and "aangenomen" in page_text.lower():
                agency.no_cure_no_pay = True
                self.logger.info(f"✓ No cure no pay: True | Source: {url}")
            
            # Pricing transparency - they show examples with calculations
            if "voorbeeld" in page_text.lower() and "berekening" in page_text.lower():
                agency.pricing_transparency = "public_examples"
                self.logger.info(f"✓ Pricing transparency: public_examples (calculation examples shown) | Source: {url}")
            
            # Extract response time: "binnen 24 uur"
            response_match = re.search(
                r'binnen\s+(\d+)\s+uur',
                page_text.lower()
            )
            if response_match:
                hours = int(response_match.group(1))
                days = max(1, hours // 24)  # Convert to days
                agency.avg_time_to_fill_days = days
                self.logger.info(f"✓ Response time: binnen {hours} uur ({days} day) | Source: {url}")
            
            # Extract example hourly rates from calculations
            # Pattern: "= 35,64 (per gewerkt uur)" or "= € 37,13 per gewerkt uur"
            # Look for numbers followed by "per uur" or "(per gewerkt uur)"
            hourly_rate_matches = re.findall(
                r'=\s*€?\s*(\d+[,\.]\d+)\s*(?:\()?per\s+(?:gewerkt\s+)?uur',
                page_text,
                re.IGNORECASE
            )
            
            if hourly_rate_matches:
                rates = [float(rate.replace(',', '.')) for rate in hourly_rate_matches]
                if rates:
                    agency.avg_hourly_rate_low = round(min(rates), 2)
                    agency.avg_hourly_rate_high = round(max(rates), 2)
                    self.logger.info(f"✓ Hourly rate examples: €{min(rates):.2f} - €{max(rates):.2f} | Source: {url}")
            
            # Pricing model is omrekenfactor
            agency.pricing_model = "omrekenfactor"
            self.logger.info(f"✓ Pricing model: omrekenfactor | Source: {url}")
        
        except Exception as e:
            self.logger.error(f"❌ Error extracting pricing info: {e}")
            import traceback
            self.logger.error(f"   Traceback: {traceback.format_exc()}")
    
    def _check_chatbot_api(self, agency: Agency) -> None:
        """
        Check if the chatbot API is accessible.
        If it returns 200 OK, set chatbot and API flags to True.
        """
        try:
            self.logger.info(f"🔍 Checking chatbot API: {self.CHATBOT_API_URL}")
            
            import requests
            
            # Headers from the actual chatbot API request
            headers = {
                "accept": "*/*",
                "accept-encoding": "gzip, deflate, br, zstd",
                "accept-language": "en-US,en;q=0.9",
                "authorization": "Bearer EvVDvdtFawpjZo2iG/QGGZOf1QLwqpiiv+wXnrojLGSuHVPQHmV55brLzVIFszxBCCxNv5Ey4zytaIr8sR5JCg==",
                "origin": "https://www.tempo-team.nl",
                "referer": "https://www.tempo-team.nl/",
                "sec-ch-ua": '"Microsoft Edge";v="143", "Chromium";v="143", "Not A(Brand";v="24"',
                "sec-ch-ua-mobile": "?0",
                "sec-ch-ua-platform": '"macOS"',
                "sec-fetch-dest": "empty",
                "sec-fetch-mode": "cors",
                "sec-fetch-site": "cross-site",
                "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36 Edg/143.0.0.0"
            }
            
            response = requests.get(self.CHATBOT_API_URL, headers=headers, timeout=10)
            
            self.logger.info(f"   API Status Code: {response.status_code}")
            
            if response.status_code == 200:
                self.logger.info(f"✅ Chatbot API is accessible (200 OK)")
                
                # Initialize digital_capabilities if None
                if not agency.digital_capabilities:
                    agency.digital_capabilities = DigitalCapabilities()
                
                # Set API availability
                agency.digital_capabilities.api_available = True
                self.logger.info(f"✓ Set api_available = True")
                
                # Initialize ai_capabilities if None
                if not agency.ai_capabilities:
                    agency.ai_capabilities = AICapabilities()
                
                # Set chatbot flags
                agency.ai_capabilities.internal_ai_matching = True
                agency.ai_capabilities.chatbot_for_candidates = True
                agency.ai_capabilities.chatbot_for_clients = True
                
                self.logger.info(f"✓ Set chatbot_for_candidates = True")
                self.logger.info(f"✓ Set chatbot_for_clients = True")
                
                # Add to evidence URLs (avoid duplicates)
                if self.CHATBOT_API_URL not in self.evidence_urls:
                    self.evidence_urls.append(self.CHATBOT_API_URL)
            else:
                self.logger.info(f"⚠ Chatbot API not accessible (status {response.status_code})")
        
        except Exception as e:
            self.logger.warning(f"⚠ Could not check chatbot API: {e}")


@dg.asset(group_name="agencies")
def tempo_team_scrape() -> dg.Output[dict]:
    """Scrape Tempo-Team website."""
    scraper = TempoTeamScraper()
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
