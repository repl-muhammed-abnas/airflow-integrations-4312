"""
Mapper Loader Utility for Dynamic PUF Conversion

This module provides functionality to:
1. Load country-specific mapper files
2. Merge base and country configurations
3. Detect country from invoice data
4. Provide runtime access to mappings
"""

import json
import os
import logging
from typing import Dict, Optional, Any
from pathlib import Path

logger = logging.getLogger(__name__)

# Get the mappers directory path - try multiple resolution methods
def _get_mappers_dir() -> Path:
    """Resolve the mappers directory path."""
    # Method 1: Relative to this file
    path1 = Path(__file__).parent.parent / "mappers"
    if path1.exists():
        return path1

    # Method 2: Try from Airflow dags directory
    airflow_dags = os.environ.get('AIRFLOW__CORE__DAGS_FOLDER', '/opt/airflow/dags')
    path2 = Path(airflow_dags) / "pagero" / "vantagepoint_onesource_einvoicing_integration" / "mappers"
    if path2.exists():
        return path2

    # Method 3: Fallback to relative path
    return path1

MAPPERS_DIR = _get_mappers_dir()
BASE_MAPPER_FILE = "_base.json"


class MapperLoader:
    """
    Dynamic mapper loader for country-specific PUF configurations.

    Usage:
        loader = MapperLoader()
        mapper = loader.get_mapper("GB")
        # or
        mapper = loader.get_mapper_for_invoice(vp_data)
    """

    _cache: Dict[str, Dict] = {}
    _base_mapper: Optional[Dict] = None

    def __init__(self, mappers_dir: str = None):
        """
        Initialize the mapper loader.

        Args:
            mappers_dir: Optional custom path to mappers directory
        """
        self.mappers_dir = Path(mappers_dir) if mappers_dir else MAPPERS_DIR
        self._load_base_mapper()

    def _load_base_mapper(self) -> Dict:
        """Load the base mapper configuration."""
        if self._base_mapper is None:
            base_path = self.mappers_dir / BASE_MAPPER_FILE
            if base_path.exists():
                with open(base_path, 'r', encoding='utf-8') as f:
                    self._base_mapper = json.load(f)
                logger.info(f"Loaded base mapper from {base_path}")
            else:
                logger.warning(f"Base mapper not found at {base_path}, using empty defaults")
                self._base_mapper = {}
        return self._base_mapper

    def _deep_merge(self, base: Dict, override: Dict) -> Dict:
        """
        Deep merge two dictionaries, with override taking precedence.

        Args:
            base: Base dictionary
            override: Dictionary to merge on top

        Returns:
            Merged dictionary
        """
        result = base.copy()

        for key, value in override.items():
            if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                result[key] = self._deep_merge(result[key], value)
            else:
                result[key] = value

        return result

    def get_mapper(self, country_code: str) -> Dict:
        """
        Get the mapper configuration for a specific country.

        Args:
            country_code: ISO 3166-1 alpha-2 country code (e.g., "GB", "DE", "IT")

        Returns:
            Merged mapper configuration (base + country-specific)
        """
        country_code = country_code.upper()

        # Check cache first
        if country_code in self._cache:
            logger.debug(f"Returning cached mapper for {country_code}")
            return self._cache[country_code]

        # Load base mapper
        base = self._load_base_mapper()

        # Try to load country-specific mapper
        country_path = self.mappers_dir / f"{country_code}.json"

        if not country_path.exists():
            available = self.list_available_mappers()
            raise ValueError(
                f"No mapper found for country '{country_code}'. "
                f"Create {country_code}.json in the mappers directory or use a supported country. "
                f"Available mappers: {available}"
            )

        with open(country_path, 'r', encoding='utf-8') as f:
            country_mapper = json.load(f)
        logger.info(f"Loaded country mapper for {country_code}")

        # Merge base with country-specific
        merged = self._deep_merge(base, country_mapper)

        # Cache the result
        self._cache[country_code] = merged

        return merged

    def get_mapper_for_invoice(self, vp_data: Dict) -> Dict:
        """
        Detect country from Vantagepoint invoice data and return appropriate mapper.

        Country detection priority:
        1. Seller country code from address
        2. Buyer country code from address
        3. Currency code mapping
        4. Default to GB

        Args:
            vp_data: Parsed Vantagepoint invoice data

        Returns:
            Appropriate mapper configuration
        """
        country_code = self.detect_country(vp_data)
        return self.get_mapper(country_code)

    def detect_country(self, vp_data: Dict, supplier_config: Dict = None) -> str:
        """
        Detect the country from invoice data.

        Detection priority:
        1. BuyerAddrCountryCode (from VP data)
        2. Region field (from VP data, mapped to ISO code)
        3. OneSource supplier_config country_code
        4. SellerCountryCode (from VP data, if present)
        5. Currency-based inference

        Args:
            vp_data: Parsed Vantagepoint invoice data
            supplier_config: Optional supplier config from OneSource

        Returns:
            ISO country code
        """
        header = vp_data.get('header', {})

        # Priority 1: Buyer country code (BuyerAddrCountryCode from VP data)
        buyer_country = header.get('buyer_country_code')
        if buyer_country:
            logger.info(f"Country detected from buyer country code: {buyer_country}")
            return buyer_country.upper()

        # Priority 2: Region field (full country name → ISO code)
        region = header.get('region', '').strip()
        if region:
            region_to_country = {
                # Europe
                'united kingdom': 'GB', 'uk': 'GB', 'great britain': 'GB', 'england': 'GB',
                'scotland': 'GB', 'wales': 'GB', 'northern ireland': 'GB',
                'germany': 'DE', 'deutschland': 'DE',
                'france': 'FR',
                'italy': 'IT', 'italia': 'IT',
                'spain': 'ES', 'españa': 'ES',
                'netherlands': 'NL', 'holland': 'NL',
                'belgium': 'BE', 'belgique': 'BE', 'belgië': 'BE',
                'austria': 'AT', 'österreich': 'AT',
                'switzerland': 'CH', 'schweiz': 'CH', 'suisse': 'CH',
                'sweden': 'SE', 'sverige': 'SE',
                'norway': 'NO', 'norge': 'NO',
                'denmark': 'DK', 'danmark': 'DK',
                'finland': 'FI', 'suomi': 'FI',
                'poland': 'PL', 'polska': 'PL',
                'portugal': 'PT',
                'ireland': 'IE',
                'greece': 'GR',
                'czech republic': 'CZ', 'czechia': 'CZ',
                'hungary': 'HU', 'magyarország': 'HU',
                'romania': 'RO', 'românia': 'RO',
                'bulgaria': 'BG',
                'croatia': 'HR', 'hrvatska': 'HR',
                'serbia': 'RS', 'srbija': 'RS',
                'slovenia': 'SI', 'slovenija': 'SI',
                'slovakia': 'SK', 'slovensko': 'SK',
                'luxembourg': 'LU',
                'estonia': 'EE', 'eesti': 'EE',
                'latvia': 'LV', 'latvija': 'LV',
                'lithuania': 'LT', 'lietuva': 'LT',
                'malta': 'MT',
                'cyprus': 'CY',
                'iceland': 'IS', 'ísland': 'IS',
                # Middle East
                'saudi arabia': 'SA',
                'united arab emirates': 'AE', 'uae': 'AE',
                'israel': 'IL',
                'turkey': 'TR', 'türkiye': 'TR',
                # Americas
                'united states': 'US', 'usa': 'US',
                'canada': 'CA',
                'brazil': 'BR', 'brasil': 'BR',
                'mexico': 'MX', 'méxico': 'MX',
                'colombia': 'CO',
                # Asia Pacific
                'australia': 'AU',
                'new zealand': 'NZ',
                'india': 'IN',
                'japan': 'JP',
                'singapore': 'SG',
                'malaysia': 'MY',
                'philippines': 'PH',
                'vietnam': 'VN',
                'indonesia': 'ID',
                'thailand': 'TH',
                'south korea': 'KR', 'korea': 'KR',
            }
            region_lower = region.lower()
            if region_lower in region_to_country:
                country = region_to_country[region_lower]
                logger.info(f"Country detected from region '{region}': {country}")
                return country

        # Priority 3: OneSource supplier config country
        if supplier_config and supplier_config.get('country_code'):
            country = supplier_config['country_code'].upper()
            logger.info(f"Country detected from OneSource supplier config: {country}")
            return country

        # Priority 4: Seller country (from SellerCountryCode if present)
        seller_country = header.get('seller_country_code')
        if seller_country:
            logger.info(f"Country detected from seller: {seller_country}")
            return seller_country.upper()

        # Priority 5: Currency-based inference
        currency = header.get('currency_code', '')
        currency_to_country = {
            # Europe
            'GBP': 'GB',
            'EUR': 'DE',  # Default EUR to Germany, can be overridden
            'PLN': 'PL',
            'HUF': 'HU',
            'SEK': 'SE',
            'DKK': 'DK',
            'NOK': 'NO',
            'CHF': 'CH',
            'RON': 'RO',
            'RSD': 'RS',
            'HRK': 'HR',
            'BGN': 'BG',
            'CZK': 'CZ',
            # Americas
            'USD': 'US',
            'CAD': 'CA',
            'BRL': 'BR',
            'MXN': 'MX',
            # Middle East
            'SAR': 'SA',
            'AED': 'AE',
            'ILS': 'IL',
            'TRY': 'TR',
            # Asia Pacific
            'AUD': 'AU',
            'INR': 'IN',
            'JPY': 'JP',
            'SGD': 'SG',
            'MYR': 'MY',
            'PHP': 'PH',
            'VND': 'VN',
            'IDR': 'ID',
            'THB': 'TH',
            'KRW': 'KR',
            'NZD': 'NZ'
        }

        if currency in currency_to_country:
            country = currency_to_country[currency]
            logger.info(f"Country inferred from currency {currency}: {country}")
            return country

        # No fallback — country must be determinable
        raise ValueError(
            "Cannot detect country from Vantagepoint data. "
            "No seller_country_code, buyer_country_code, region, or recognized currency_code found. "
            "Provide country_code explicitly or ensure supplier_config includes country_code."
        )

    def list_available_mappers(self) -> list:
        """
        List all available country mappers.

        Returns:
            List of country codes with available mappers
        """
        mappers = []

        for file in self.mappers_dir.glob("*.json"):
            if file.name != BASE_MAPPER_FILE:
                country_code = file.stem
                mappers.append(country_code)

        return sorted(mappers)

    def get_field_mapping(self, country_code: str, section: str) -> Dict:
        """
        Get field mappings for a specific section.

        Args:
            country_code: ISO country code
            section: Mapping section (e.g., "header", "buyer", "line")

        Returns:
            Field mapping dictionary
        """
        mapper = self.get_mapper(country_code)
        return mapper.get('field_mappings', {}).get(section, {})

    def get_tax_config(self, country_code: str) -> Dict:
        """
        Get tax system configuration for a country.

        Args:
            country_code: ISO country code

        Returns:
            Tax configuration dictionary
        """
        mapper = self.get_mapper(country_code)
        return mapper.get('tax_system', {})

    def get_identifier_schemes(self, country_code: str) -> Dict:
        """
        Get identifier scheme IDs for a country.

        Args:
            country_code: ISO country code

        Returns:
            Identifier schemes dictionary
        """
        mapper = self.get_mapper(country_code)
        return mapper.get('identifiers', {})

    def get_extensions(self, country_code: str) -> Dict:
        """
        Get required and optional extensions for a country.

        Args:
            country_code: ISO country code

        Returns:
            Extensions configuration dictionary
        """
        mapper = self.get_mapper(country_code)
        return mapper.get('extensions', {})

    def get_validation_rules(self, country_code: str) -> Dict:
        """
        Get validation rules for a country.

        Args:
            country_code: ISO country code

        Returns:
            Validation rules dictionary
        """
        mapper = self.get_mapper(country_code)
        return mapper.get('validation_rules', {})

    def validate_vat_number(self, country_code: str, vat_number: str) -> bool:
        """
        Validate a VAT number against country-specific pattern.

        Args:
            country_code: ISO country code
            vat_number: VAT number to validate

        Returns:
            True if valid, False otherwise
        """
        import re

        rules = self.get_validation_rules(country_code)
        pattern = rules.get('vat_number_pattern', '.*')

        return bool(re.match(pattern, vat_number))

    def get_scheme_id_for_identifier(self, country_code: str, identifier_type: str) -> str:
        """
        Get the appropriate scheme ID for an identifier type.

        Args:
            country_code: ISO country code
            identifier_type: Type of identifier (e.g., "vat_number", "gln", "company_id")

        Returns:
            Scheme ID string
        """
        identifiers = self.get_identifier_schemes(country_code)
        scheme_mappings = identifiers.get('scheme_mappings', {})

        if identifier_type in scheme_mappings:
            return scheme_mappings[identifier_type]

        # Fallback to default endpoint scheme
        return identifiers.get('seller_endpoint_scheme', '0088')

    def requires_extension(self, country_code: str, extension_name: str) -> bool:
        """
        Check if a country requires a specific extension.

        Args:
            country_code: ISO country code
            extension_name: Name of the extension

        Returns:
            True if required, False otherwise
        """
        extensions = self.get_extensions(country_code)
        required = extensions.get('required', [])
        return extension_name in required

    def get_special_rules(self, country_code: str) -> Dict:
        """
        Get special rules for a country.

        Args:
            country_code: ISO country code

        Returns:
            Special rules dictionary
        """
        mapper = self.get_mapper(country_code)
        return mapper.get('special_rules', {})

    def clear_cache(self):
        """Clear the mapper cache."""
        self._cache.clear()
        logger.info("Mapper cache cleared")


# Singleton instance for easy access
_loader_instance: Optional[MapperLoader] = None


def get_mapper_loader() -> MapperLoader:
    """
    Get the singleton mapper loader instance.

    Returns:
        MapperLoader instance
    """
    global _loader_instance
    if _loader_instance is None:
        _loader_instance = MapperLoader()
    return _loader_instance


def get_mapper(country_code: str) -> Dict:
    """
    Convenience function to get a mapper for a country.

    Args:
        country_code: ISO country code

    Returns:
        Mapper configuration dictionary
    """
    return get_mapper_loader().get_mapper(country_code)


def detect_country_from_invoice(vp_data: Dict) -> str:
    """
    Convenience function to detect country from invoice data.

    Args:
        vp_data: Parsed Vantagepoint invoice data

    Returns:
        ISO country code
    """
    return get_mapper_loader().detect_country(vp_data)
