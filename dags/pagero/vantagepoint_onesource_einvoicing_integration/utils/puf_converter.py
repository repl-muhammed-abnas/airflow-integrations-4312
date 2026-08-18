"""
Dynamic PUF Converter using Mapper-Based Configuration

This module converts Vantagepoint invoice data to Pagero Universal Format (PUF)
using country-specific mapper configurations loaded at runtime.

Key Features:
- Dynamic country detection
- Mapper-based field mappings
- Country-specific extensions
- Validation against country rules
"""

from lxml import etree as ET
from datetime import datetime
from typing import Union, Dict, Optional, List
import logging

try:
    # Try relative import first (when used as part of package)
    from .mapper_loader import get_mapper_loader, MapperLoader
except ImportError:
    # Fall back to absolute import (when file is scanned directly by Airflow)
    from pagero.vantagepoint_onesource_einvoicing_integration.utils.mapper_loader import get_mapper_loader, MapperLoader

logger = logging.getLogger(__name__)


class PUFConverter:
    """
    Dynamic PUF converter using mapper-based configuration.

    Usage:
        converter = PUFConverter()
        puf_xml = converter.convert(vp_data, supplier_config)

        # Or with explicit country
        puf_xml = converter.convert(vp_data, supplier_config, country_code="IT")
    """

    # Decimal fields by section - parsed as float instead of text
    _DECIMAL_FIELDS = {
        'header': {
            'taxable_amount', 'amount_without_vat', 'vat_amount',
            'amount_with_vat', 'amount_due'
        },
        'line': {
            'gross_price', 'net_price', 'price_base_quantity', 'quantity',
            'net_price_x_quantity', 'taxable_value', 'vat_rate', 'vat_amount',
            'line_amount_total'
        },
        'tax_breakdown': {
            'rate', 'taxable_amount', 'taxable_amount_sar',
            'tax_amount', 'tax_amount_sar'
        },
        'prepayments': {
            'vat_rate', 'taxable_amount_sar', 'taxable_amount',
            'tax_amount_sar', 'tax_amount'
        },
    }

    def __init__(self, mapper_loader: MapperLoader = None):
        """
        Initialize the PUF converter.

        Args:
            mapper_loader: Optional custom mapper loader instance
        """
        self.loader = mapper_loader or get_mapper_loader()

    def convert(
        self,
        input_data: Union[str, bytes, Dict],
        supplier_config: Dict = None,
        country_code: str = None,
        is_file_path: bool = False,
        use_buyer_as_supplier: bool = False
    ) -> str:
        """
        Convert Vantagepoint invoice to PUF XML.

        Args:
            input_data: XML string/bytes, file path, or pre-parsed dict
            supplier_config: Supplier configuration from OneSource (required unless self-billing)
            country_code: Optional explicit country code (auto-detected if not provided)
            is_file_path: True if input_data is a file path
            use_buyer_as_supplier: Use buyer data as supplier (self-billing)

        Returns:
            PUF XML string

        Raises:
            ValueError: If supplier_config is not provided and use_buyer_as_supplier is False
        """
        # Parse input if needed
        if isinstance(input_data, dict):
            # Check if this is already in parsed VP format (has 'header' key)
            if 'header' in input_data:
                vp_data = input_data
            else:
                # Raw API response dict - try to extract XML from common VP response keys
                xml_content = (
                    input_data.get('result') or
                    input_data.get('data') or
                    input_data.get('value') or
                    input_data.get('response')
                )
                if xml_content and isinstance(xml_content, str):
                    vp_data = self._parse_vantagepoint_data(xml_content, is_file_path=False)
                else:
                    raise ValueError(
                        f"Input dict does not contain expected VP data structure. "
                        f"Expected 'header' key or XML in 'result'/'data'/'value'. "
                        f"Got keys: {list(input_data.keys())}"
                    )
        else:
            vp_data = self._parse_vantagepoint_data(input_data, is_file_path)

        # Detect or use provided country
        # Priority: explicit country_code > VP buyer country > VP region > OneSource > currency
        if country_code:
            country = country_code.upper()
        else:
            country = self.loader.detect_country(vp_data, supplier_config=supplier_config)

        logger.info(f"Converting invoice for country: {country}")

        # Get the mapper for this country
        mapper = self.loader.get_mapper(country)

        # Build supplier config from buyer data (self-billing) or fail
        if supplier_config is None:
            if use_buyer_as_supplier:
                supplier_config = self._build_self_billing_config(
                    vp_data, mapper
                )
            else:
                raise ValueError(
                    "supplier_config is required. Provide supplier details from OneSource "
                    "company API via parse_onesource_company_to_supplier_config(), or set "
                    "use_buyer_as_supplier=True for self-billing scenarios."
                )

        # Second pass: extract country-specific additional fields from VP XML
        # (e.g. IT BuyerCodiceDestinatario, SA InvoiceUUID, etc.)
        self._enrich_with_country_fields(vp_data, mapper)

        # Validate invoice data against country-specific rules (hard stop on failure)
        self._validate_invoice_data(vp_data, mapper, supplier_config, country)

        # Build PUF XML
        puf_xml = self._build_puf_invoice(vp_data, mapper, supplier_config)

        return puf_xml

    def _parse_vantagepoint_data(
        self,
        input_data: Union[str, bytes],
        is_file_path: bool = False
    ) -> Dict:
        """
        Parse Vantagepoint XML into a dictionary.

        Handles the VP stored procedure response format:
        - <NewDataSet> wrapper with embedded XSD schema + data elements
        - Duplicate <Table> elements (takes first only)
        - Missing Table2 (tax breakdown) - creates fallback from header
        - Additional tables: Table4 (prepayments), Table5 (payment modes),
          Table6 (preceding invoice refs), Table7 (credit/debit reasons)
        """

        # Handle JSON-wrapped XML (VP API may return JSON with XML in a field)
        if isinstance(input_data, str) and not is_file_path:
            stripped = input_data.strip()
            if stripped.startswith('{') or stripped.startswith('['):
                try:
                    import json
                    parsed = json.loads(stripped)
                    if isinstance(parsed, dict):
                        xml_content = (
                            parsed.get('result') or
                            parsed.get('data') or
                            parsed.get('value') or
                            parsed.get('response')
                        )
                        if xml_content and isinstance(xml_content, str):
                            input_data = xml_content
                        else:
                            raise ValueError(
                                f"JSON response does not contain XML. Keys: {list(parsed.keys())}"
                            )
                    elif isinstance(parsed, str):
                        input_data = parsed
                except (json.JSONDecodeError, ValueError):
                    pass  # Not JSON, try as XML

        # Parse XML
        if is_file_path:
            tree = ET.parse(input_data)
            root = tree.getroot()
        else:
            if isinstance(input_data, str):
                input_data = input_data.encode('utf-8')
            root = ET.fromstring(input_data)

        # Handle <NewDataSet> wrapper - navigate to the data section
        # The VP stored procedure wraps data in <NewDataSet> which contains
        # an XSD schema followed by actual data elements
        if root.tag == 'NewDataSet' or root.tag.endswith('}NewDataSet'):
            data_root = root
        else:
            # Try to find NewDataSet as a child
            nds = root.find('.//NewDataSet')
            data_root = nds if nds is not None else root

        data = {
            'header': {},
            'lines': [],
            'tax_breakdown': [],
            'notes': [],
            'prepayments': [],
            'payment_modes': [],
            'preceding_invoices': [],
            'credit_debit_reasons': []
        }

        # Load field mappings from base mapper
        # VP SP column names are consistent regardless of country, so base mapper is always used
        base_mapper = self.loader._load_base_mapper()
        fm = base_mapper.get('field_mappings', {})

        # Parse Table (Invoice Header) - take first only (VP may return duplicates)
        header_map = fm.get('header', {})
        detect_tag1 = header_map.get('erp_doc_num', 'ERPDocNum')
        detect_tag2 = header_map.get('doc_type', 'DocType')

        table = data_root.find('Table')
        if table is None:
            # Fallback: search recursively but skip XSD schema elements
            for elem in data_root.iter('Table'):
                if elem.find(detect_tag1) is not None or elem.find(detect_tag2) is not None:
                    table = elem
                    break

        if table is not None:
            data['header'] = self._parse_section(
                table, header_map, self._DECIMAL_FIELDS.get('header', set())
            )
        else:
            logger.error(
                "No <Table> element found in Vantagepoint data. "
                "XML root tag: %s, direct children: %s",
                data_root.tag,
                [child.tag for child in data_root][:10]
            )
            raise ValueError(
                f"No invoice header (<Table>) found in Vantagepoint XML response. "
                f"Root element: '{data_root.tag}'. Check the stored procedure output."
            )

        # Parse Table1 (Invoice Lines)
        line_map = fm.get('line', {})
        line_detect_tags = [v for k, v in line_map.items() if not k.startswith('_')][:2]
        for line_elem in data_root.iter('Table1'):
            # Skip XSD definition elements (no actual data children)
            if line_detect_tags and all(line_elem.find(tag) is None for tag in line_detect_tags):
                continue
            data['lines'].append(self._parse_section(
                line_elem, line_map, self._DECIMAL_FIELDS.get('line', set())
            ))

        # Detect potential VATRate/VATAmt swap in SP
        # Known issue: VP SP may swap ISNULL(detail.TaxAmount,...) into VATRate
        # and ISNULL(detail.TaxRate,...) into VATAmt.
        #
        # Detection: if treating VATAmt as the rate gives us VATRate as the
        # amount (i.e.  taxable * VATAmt / 100 ≈ VATRate), then they are
        # swapped.  We also keep the old heuristic (rate > 100) as a fallback.
        for line in data['lines']:
            vat_rate = line.get('vat_rate', 0)
            vat_amt = line.get('vat_amount', 0)
            taxable = line.get('taxable_value', 0)
            if vat_rate != 0 and vat_amt != 0 and taxable != 0:
                swap = False

                # Heuristic 1 (original): rate field has a large absolute number
                if vat_rate > 100 and 0 < vat_amt < 100:
                    swap = True

                # Heuristic 2 (math-based): if VATAmt looks like the rate
                # because  taxable * VATAmt/100  ≈  VATRate  (within 1 cent)
                if not swap and vat_rate != vat_amt and 0 < vat_amt <= 100:
                    expected_amt_if_swapped = round(taxable * vat_amt / 100, 2)
                    if abs(expected_amt_if_swapped - vat_rate) <= 0.02:
                        swap = True

                if swap:
                    logger.warning(
                        "VATRate/VATAmt swap detected on line %s: "
                        "VATRate=%.2f (actual amount), VATAmt=%.2f (actual rate). Swapping.",
                        line.get('line_id'), vat_rate, vat_amt
                    )
                    line['vat_rate'], line['vat_amount'] = vat_amt, vat_rate

        # Parse Table2 (VAT Breakdown)
        tax_map = fm.get('tax_breakdown', {})
        tax_detect_tags = [v for k, v in tax_map.items() if not k.startswith('_')][:2]
        for tax_elem in data_root.iter('Table2'):
            if tax_detect_tags and all(tax_elem.find(tag) is None for tag in tax_detect_tags):
                continue
            data['tax_breakdown'].append(self._parse_section(
                tax_elem, tax_map, self._DECIMAL_FIELDS.get('tax_breakdown', set())
            ))

        # Fallback: if no Table2 but header has VAT amounts, create synthetic breakdown
        if not data['tax_breakdown'] and data['header'].get('vat_amount', 0) != 0:
            logger.warning(
                "No Table2 (tax breakdown) found. Creating fallback from header amounts."
            )
            # Infer category from first line's VATCatCode instead of hardcoding 'S'
            inferred_cat = 'S'
            for line in data['lines']:
                cat = line.get('vat_category')
                if cat:
                    inferred_cat = cat
                    break

            data['tax_breakdown'].append({
                'category_code': inferred_cat,
                'rate': self._calculate_vat_rate(
                    data['header'].get('vat_amount', 0),
                    data['header'].get('taxable_amount', 0)
                ),
                'taxable_amount': data['header'].get('taxable_amount', 0),
                'tax_amount': data['header'].get('vat_amount', 0),
                'currency': data['header'].get('currency_code', 'GBP')
            })

        # Parse Table3 (Notes)
        notes_map = fm.get('notes', {})
        note_tag = notes_map.get('note', 'InvoiceNote')
        for note_elem in data_root.iter('Table3'):
            note = self._get_text(note_elem, note_tag)
            if note:
                data['notes'].append(note)

        # Parse Table4 (Prepayment Adjustments)
        prepay_map = fm.get('prepayments', {})
        prepay_detect_tags = [v for k, v in prepay_map.items() if not k.startswith('_')][:2]
        for prepay_elem in data_root.iter('Table4'):
            if prepay_detect_tags and all(prepay_elem.find(tag) is None for tag in prepay_detect_tags):
                continue
            data['prepayments'].append(self._parse_section(
                prepay_elem, prepay_map, self._DECIMAL_FIELDS.get('prepayments', set())
            ))

        # Parse Table5 (Payment Modes)
        pay_map = fm.get('payment_modes', {})
        pay_detect_tags = [v for k, v in pay_map.items() if not k.startswith('_')][:2]
        for pay_elem in data_root.iter('Table5'):
            if pay_detect_tags and all(pay_elem.find(tag) is None for tag in pay_detect_tags):
                continue
            data['payment_modes'].append(self._parse_section(
                pay_elem, pay_map, set()
            ))

        # Parse Table6 (Preceding Invoice References)
        ref_map = fm.get('preceding_invoices', {})
        ref_detect_tag = ref_map.get('reference', 'PrecedingInvoiceRef')
        for ref_elem in data_root.iter('Table6'):
            if ref_elem.find(ref_detect_tag) is None:
                continue
            data['preceding_invoices'].append(self._parse_section(
                ref_elem, ref_map, set()
            ))

        # Parse Table7 (Credit/Debit Note Reasons)
        reason_map = fm.get('credit_debit_reasons', {})
        reason_detect_tag = reason_map.get('reason', 'ReasonForCreditDebitNote')
        for reason_elem in data_root.iter('Table7'):
            if reason_elem.find(reason_detect_tag) is None:
                continue
            data['credit_debit_reasons'].append(self._parse_section(
                reason_elem, reason_map, set()
            ))

        logger.info(
            "Parsed VP data: header=%s, lines=%d, tax_breakdown=%d, notes=%d, "
            "prepayments=%d, payment_modes=%d, preceding_invoices=%d",
            bool(data['header']), len(data['lines']), len(data['tax_breakdown']),
            len(data['notes']), len(data['prepayments']), len(data['payment_modes']),
            len(data['preceding_invoices'])
        )

        # Preserve the XML root so country-specific additional fields can be
        # extracted later (two-pass approach: base fields first, then country).
        data['_xml_root'] = data_root

        return data

    def _enrich_with_country_fields(self, vp_data: Dict, mapper: Dict) -> None:
        """Extract country-specific additional fields from VP XML (second pass).

        The base parser extracts only the universal field set.  Country mappers
        may define ``additional_header_fields``, ``additional_buyer_fields`` and
        ``additional_seller_fields`` that map to extra VP stored-procedure
        columns (e.g. IT ``BuyerCodiceDestinatario``, SA ``InvoiceUUID``).

        This method re-reads the preserved XML root to pull those values into
        ``vp_data['header']`` so that validation and PUF building can find them.
        """
        xml_root = vp_data.get('_xml_root')
        if xml_root is None:
            return

        fm = mapper.get('field_mappings', {})
        header = vp_data.get('header', {})

        # Find the first <Table> element (same logic as initial parse)
        table = xml_root.find('Table')
        if table is None:
            for elem in xml_root.iter('Table'):
                if elem.find('ERPDocNum') is not None or elem.find('DocType') is not None:
                    table = elem
                    break
        if table is None:
            return

        additional_sections = [
            'additional_header_fields',
            'additional_buyer_fields',
            'additional_seller_fields',
        ]

        enriched = []
        for section in additional_sections:
            section_map = fm.get(section, {})
            for internal_key, vp_tag in section_map.items():
                if internal_key.startswith('_'):
                    continue
                # Only add if not already present in header
                if internal_key not in header or header[internal_key] is None:
                    elem = table.find(vp_tag)
                    if elem is not None and elem.text:
                        header[internal_key] = elem.text.strip()
                        enriched.append(f"{internal_key}={elem.text.strip()}")

        if enriched:
            logger.info("Enriched VP data with country fields: %s", ', '.join(enriched))

    def _build_self_billing_config(
        self,
        vp_data: Dict,
        mapper: Dict
    ) -> Dict:
        """Build supplier config from buyer data for self-billing scenarios.

        In self-billing, the buyer issues the invoice on behalf of the supplier,
        so the buyer's details become the supplier party in the PUF.
        """

        header = vp_data.get('header', {})
        identifiers = mapper.get('identifiers', {})
        defaults = mapper.get('defaults', {})
        country = mapper.get('country', {})

        buyer_vat = header.get('buyer_vat_num')
        buyer_id = header.get('buyer_id')
        buyer_name = header.get('buyer_name')

        if not buyer_vat and not buyer_id:
            raise ValueError(
                "Self-billing requires buyer identification. "
                "No BuyerVATNum or BuyerID found in Vantagepoint data."
            )

        return {
            'endpoint_id': buyer_vat or buyer_id,
            'party_id': buyer_vat or buyer_id,
            'name': buyer_name or 'Unknown Supplier',
            'vat_id': buyer_vat or '',
            'registration_name': buyer_name or 'Unknown Supplier',
            'company_id': buyer_id or '',
            'street': header.get('buyer_street', ''),
            'building_num': header.get('buyer_building_num', ''),
            'city': header.get('buyer_city', ''),
            'postal_code': header.get('buyer_postal_code', ''),
            'district': header.get('buyer_district', ''),
            'country_code': header.get('buyer_country_code', country.get('code', 'GB')),
            'contact_name': defaults.get('contact_department', 'Accounts Department'),
            'contact_phone': '',
            'contact_email': ''
        }

    # Maps logical required_field names (from mapper JSON) to their actual data source/key.
    # source='header'         → vp_data['header'][key]
    # source='supplier'       → supplier_config[key]
    # source='payment_modes'  → any entry in vp_data['payment_modes'] has truthy key
    _REQUIRED_FIELD_LOCATIONS = {
        # Universal
        'invoice_number':             ('header',   'erp_doc_num'),
        'issue_date':                 ('header',   'erp_doc_date'),
        'currency_code':              ('header',   'currency_code'),
        'seller_vat_number':          ('supplier', 'vat_id'),
        # SA (ZATCA Phase 2)
        'uuid':                       ('supplier', 'uuid'),
        'invoice_type_code':          ('supplier', 'invoice_type_code'),
        'seller_building_number':     ('supplier', 'building_number'),
        'seller_district':            ('supplier', 'district'),
        # IT (FatturaPA / SDI)
        'buyer_codice_destinatario':  ('header',   'codice_destinatario'),
        # DE (SEPA BIC requirement)
        'bank_bic':                   ('payment_modes', 'bic'),
        # AU (ABN)
        'seller_abn':                 ('supplier', 'vat_id'),
        # BE (Hermes email)
        'buyer_email':                ('header',   'buyer_email'),
        # CA (GST/HST number)
        'seller_gst_number':          ('supplier', 'vat_id'),
        # FR (SIRET)
        'seller_siret':               ('supplier', 'siret'),
        # IN (GSTIN, supply type, POS)
        'seller_gstin':               ('supplier', 'gstin'),
        'buyer_gstin':                ('header',   'buyer_vat_num'),
        'supply_type':                ('supplier', 'supply_type'),
        'pos_code':                   ('supplier', 'pos_code'),
        # JP (Qualified Invoice registration number)
        'seller_registration_number': ('supplier', 'vat_id'),
        # MY (TIN)
        'seller_tin':                 ('supplier', 'vat_id'),
        # PL (NIP)
        'seller_nip':                 ('supplier', 'nip'),
        # PH (TIN)
        'seller_ph_tin':              ('supplier', 'vat_id'),
        # PT (NIF)
        'seller_nif':                 ('supplier', 'vat_id'),
        # RO (CUI)
        'seller_cui':                 ('supplier', 'cui'),
        # RS (PIB)
        'seller_pib':                 ('supplier', 'pib'),
        # SG (UEN)
        'seller_uen':                 ('supplier', 'vat_id'),
        # TR (VKN, Tax Office, ETTN)
        'seller_vkn':                 ('supplier', 'vkn'),
        'seller_tax_office':          ('supplier', 'tax_office'),
        'ettn':                       ('supplier', 'ettn'),
        # VN (Tax Code)
        'seller_tax_code':            ('supplier', 'vat_id'),
    }

    def _validate_invoice_data(
        self,
        vp_data: Dict,
        mapper: Dict,
        supplier_config: Dict,
        country_code: str
    ) -> None:
        """Validate invoice data against country-specific mapper rules.

        Collects ALL validation errors before raising so the caller sees the
        full picture in one go rather than fix-and-retry per error.

        Args:
            vp_data:         Parsed VP invoice dict (header, lines, tax_breakdown …)
            mapper:          Merged country mapper config
            supplier_config: Supplier configuration dict
            country_code:    ISO country code (e.g. 'SA', 'GB', 'IT')

        Raises:
            ValueError: If one or more validation rules fail.
        """
        import re

        errors = []
        header          = vp_data.get('header', {})
        lines           = vp_data.get('lines', [])
        tax_breakdown   = vp_data.get('tax_breakdown', [])
        validation_rules = mapper.get('validation_rules', {})
        mapper_defaults  = mapper.get('defaults', {})
        sc = supplier_config or {}

        # ------------------------------------------------------------------
        # Check 1: Required fields
        # ------------------------------------------------------------------
        payment_modes = vp_data.get('payment_modes', [])
        for field in validation_rules.get('required_fields', []):
            source, key = self._REQUIRED_FIELD_LOCATIONS.get(field, ('header', field))
            if source == 'supplier':
                value = sc.get(key)
            elif source == 'payment_modes':
                # True if ANY payment mode entry has a truthy value for key
                # Also check supplier_config as fallback (e.g. BIC may be pre-loaded)
                value = (
                    any(pm.get(key) for pm in payment_modes) or
                    sc.get(key)
                )
            else:
                value = header.get(key)
            if not value:
                # Supplier-sourced fields may have a mapper-level default (e.g. SA invoice_type_code)
                if source == 'supplier' and mapper_defaults.get(key):
                    continue
                errors.append(
                    f"Required field '{field}' is missing or empty "
                    f"(source: {source}, key: '{key}')"
                )

        # ------------------------------------------------------------------
        # Check 2: Seller VAT number format
        # ------------------------------------------------------------------
        seller_vat = sc.get('vat_id', '')
        if seller_vat:
            if not self.loader.validate_vat_number(country_code, seller_vat):
                pattern = validation_rules.get('vat_number_pattern', '')
                errors.append(
                    f"Seller VAT number '{seller_vat}' does not match expected "
                    f"{country_code} format (pattern: {pattern})"
                )

        # ------------------------------------------------------------------
        # Check 3: Buyer VAT number format (optional field — only if present)
        # ------------------------------------------------------------------
        buyer_vat = header.get('buyer_vat_num', '')
        if buyer_vat:
            if not self.loader.validate_vat_number(country_code, buyer_vat):
                errors.append(
                    f"Buyer VAT number '{buyer_vat}' does not match expected "
                    f"{country_code} format"
                )

        # ------------------------------------------------------------------
        # Check 4: UUID format (SA ZATCA Phase 2 — uuid_pattern in mapper)
        # ------------------------------------------------------------------
        uuid_pattern = validation_rules.get('uuid_pattern', '')
        if uuid_pattern:
            uuid_val = sc.get('uuid', '')
            if uuid_val and not re.match(uuid_pattern, uuid_val):
                errors.append(
                    f"UUID '{uuid_val}' does not match expected UUID v4 format"
                )

        # ------------------------------------------------------------------
        # Check 5: Building number format (SA KSA-17 — 4 digits)
        # ------------------------------------------------------------------
        bn_pattern = validation_rules.get('building_number_pattern', '')
        if bn_pattern:
            building_num = sc.get('building_number', '')
            if building_num and not re.match(bn_pattern, building_num):
                errors.append(
                    f"Building number '{building_num}' does not match expected "
                    f"format (pattern: {bn_pattern})"
                )

        # ------------------------------------------------------------------
        # Check 6: Header amounts must be non-negative
        # ------------------------------------------------------------------
        for amount_field in ('taxable_amount', 'amount_with_vat', 'amount_due'):
            val = header.get(amount_field, 0)
            if isinstance(val, (int, float)) and val < 0:
                errors.append(
                    f"Header amount '{amount_field}' cannot be negative ({val})"
                )

        # ------------------------------------------------------------------
        # Check 7: Invoice lines — quantity must be non-zero
        # ------------------------------------------------------------------
        for i, line in enumerate(lines):
            line_id = line.get('line_id') or str(i + 1)
            qty = line.get('quantity', 0.0)
            if qty == 0:
                errors.append(f"Line {line_id}: 'quantity' cannot be zero")

        # ------------------------------------------------------------------
        # Check 8: Tax breakdown consistency
        # ------------------------------------------------------------------
        for tb in tax_breakdown:
            taxable = tb.get('taxable_amount', 0.0)
            tax_amt = tb.get('tax_amount', 0.0)
            rate    = tb.get('rate', 0.0)
            if isinstance(taxable, (int, float)) and taxable < 0:
                errors.append(
                    f"Tax breakdown: taxable_amount cannot be negative ({taxable})"
                )
            if rate > 0 and taxable > 0 and tax_amt > 0:
                expected  = round(taxable * rate / 100, 2)
                actual    = round(tax_amt, 2)
                tolerance = 0.02  # 2-cent rounding allowance
                if abs(expected - actual) > tolerance:
                    errors.append(
                        f"Tax breakdown: tax_amount {actual} is inconsistent with "
                        f"taxable_amount {taxable} × rate {rate}% = {expected}"
                    )

        # ------------------------------------------------------------------
        # Raise with full error report if anything failed
        # ------------------------------------------------------------------
        if errors:
            n = len(errors)
            raise ValueError(
                f"Invoice validation failed for country {country_code} "
                f"({n} error{'s' if n > 1 else ''}):\n" +
                "\n".join(f"  [{i + 1}] {msg}" for i, msg in enumerate(errors))
            )

        logger.debug("Invoice validation passed for country %s", country_code)

    def _build_puf_invoice(
        self,
        vp_data: Dict,
        mapper: Dict,
        supplier_config: Dict
    ) -> str:
        """Build PUF invoice XML using mapper configuration."""

        header = vp_data.get('header', {})

        # Validate required header fields
        erp_doc_num = header.get('erp_doc_num')
        erp_doc_date = header.get('erp_doc_date')
        if not erp_doc_num:
            raise ValueError(
                "Missing required field 'erp_doc_num' (ERPDocNum) in invoice header. "
                "Parsed header fields: %s" % [k for k, v in header.items() if v]
            )

        country = mapper.get('country', {})
        tax_system = mapper.get('tax_system', {})
        identifiers = mapper.get('identifiers', {})
        puf_headers = mapper.get('puf_headers', {})
        puf_namespaces = mapper.get('puf_namespaces', {})
        extensions_config = mapper.get('extensions', {})
        special_rules = mapper.get('special_rules', {})

        # Determine invoice type early - needed for namespace and root element selection
        invoice_type_mapping = mapper.get('invoice_type_mapping', {})
        invoice_type = invoice_type_mapping.get(
            header.get('doc_type'),
            invoice_type_mapping.get(header.get('doc_type1'), '380')
        )
        is_credit_note = invoice_type == '381'

        # Namespaces - use credit note namespace for type 381
        if is_credit_note:
            default_ns = puf_namespaces.get(
                'default_credit_note',
                "urn:pagero:PageroUniversalFormat:CreditNote:1.0"
            )
        else:
            default_ns = puf_namespaces.get(
                'default',
                "urn:pagero:PageroUniversalFormat:Invoice:1.0"
            )

        nsmap = {
            None: default_ns,
            "cbc": puf_namespaces.get('cbc', "urn:oasis:names:specification:ubl:schema:xsd:CommonBasicComponents-2"),
            "cac": puf_namespaces.get('cac', "urn:pagero:CommonAggregateComponents:1.0")
        }

        # Add extension namespaces if required
        # Countries with required extensions, Spain (DIR3/VeriFactu), France (Party RegistrationData),
        # or countries with TaxSubtotal extensions (IT TaxChargeability)
        country_code = country.get('code', 'XX')
        needs_ext_ns = (
            extensions_config.get('required') or
            country_code in ('ES', 'FR', 'IT')
        )
        if needs_ext_ns:
            nsmap["ext"] = puf_namespaces.get('ext', "urn:oasis:names:specification:ubl:schema:xsd:CommonExtensionComponents-2")
            nsmap["puf"] = puf_namespaces.get('puf', "urn:pagero:ExtensionComponent:1.0")

        cbc = "{urn:oasis:names:specification:ubl:schema:xsd:CommonBasicComponents-2}"
        cac = "{urn:pagero:CommonAggregateComponents:1.0}"

        # Create root element - CreditNote for type 381, Invoice otherwise
        root_tag = "CreditNote" if is_credit_note else "Invoice"
        invoice = ET.Element(root_tag, nsmap=nsmap)

        # Add UBL Extensions if country requires them
        if extensions_config.get('required'):
            self._add_country_extensions(invoice, mapper, supplier_config)

        # Spain: VeriFactu InvoiceSeries extension (document-level, before CustomizationID)
        if country_code == 'ES':
            self._add_spanish_document_extensions(invoice, vp_data, mapper, supplier_config)

        # PUF Headers
        ET.SubElement(invoice, f"{cbc}CustomizationID").text = puf_headers.get(
            'customization_id', "urn:pagero.com:puf:billing:2.0"
        )
        ET.SubElement(invoice, f"{cbc}ProfileID").text = puf_headers.get(
            'profile_id', "urn:pagero.com:puf:billing:1.0"
        )

        # Basic Identifiers
        ET.SubElement(invoice, f"{cbc}ID").text = erp_doc_num
        ET.SubElement(invoice, f"{cbc}IssueDate").text = self._parse_date(erp_doc_date)

        # Due Date (payment due date — present in all official PUF examples)
        if header.get('due_date'):
            ET.SubElement(invoice, f"{cbc}DueDate").text = self._parse_date(header['due_date'])

        # Issue Time (if required by country)
        if special_rules.get('requires_issue_time') and header.get('erp_doc_time'):
            ET.SubElement(invoice, f"{cbc}IssueTime").text = header['erp_doc_time']

        # Invoice/Credit Note Type Code element
        type_tag = "CreditNoteTypeCode" if is_credit_note else "InvoiceTypeCode"
        type_elem = ET.SubElement(invoice, f"{cbc}{type_tag}")
        type_elem.text = invoice_type
        # Add @name attribute if defined in mapper (required by SA, FR, ES, HR, PH)
        invoice_type_names = mapper.get('invoice_type_names', {})
        type_name = invoice_type_names.get(invoice_type)
        if type_name:
            type_elem.set('name', type_name)

        # Notes (from Table3 and credit/debit reasons from Table7)
        for note in vp_data.get('notes', []):
            ET.SubElement(invoice, f"{cbc}Note").text = note
        for reason in vp_data.get('credit_debit_reasons', []):
            reason_text = reason.get('reason')
            if reason_text:
                ET.SubElement(invoice, f"{cbc}Note").text = reason_text

        # FR coded notes (required by Factur-X / Chorus Pro)
        if country_code == 'FR':
            self._add_french_coded_notes(invoice, supplier_config, mapper, cbc)

        # Tax Point Date
        if header.get('supply_date'):
            ET.SubElement(invoice, f"{cbc}TaxPointDate").text = self._parse_date(header['supply_date'])

        # Currency
        currency = header.get('currency_code') or country.get('currency', 'GBP')
        ET.SubElement(invoice, f"{cbc}DocumentCurrencyCode").text = currency

        # Tax Currency (for dual-currency reporting, e.g. Saudi Arabia)
        tax_currency = header.get('tax_currency_code')
        if tax_currency and tax_currency != currency:
            ET.SubElement(invoice, f"{cbc}TaxCurrencyCode").text = tax_currency

        # Buyer Reference
        if header.get('accounting_voucher_num'):
            ET.SubElement(invoice, f"{cbc}BuyerReference").text = header['accounting_voucher_num']

        # ---- PUF 2.0 element ordering: InvoicePeriod → OrderReference → BillingReference ----

        # Invoice Period (service period from SupplyDate + SupplyEndDate)
        if header.get('supply_date') or header.get('supply_end_date'):
            period = ET.SubElement(invoice, f"{cac}InvoicePeriod")
            if header.get('supply_date'):
                ET.SubElement(period, f"{cbc}StartDate").text = self._parse_date(header['supply_date'])
            if header.get('supply_end_date'):
                ET.SubElement(period, f"{cbc}EndDate").text = self._parse_date(header['supply_end_date'])

        # Order Reference (from SalesOrderNum)
        if header.get('sales_order_num'):
            order_ref = ET.SubElement(invoice, f"{cac}OrderReference")
            ET.SubElement(order_ref, f"{cbc}ID").text = header['sales_order_num']

        # Billing Reference - Preceding Invoice (from header and/or Table6)
        if header.get('preceding_invoice_ref'):
            billing_ref = ET.SubElement(invoice, f"{cac}BillingReference")
            inv_doc_ref = ET.SubElement(billing_ref, f"{cac}InvoiceDocumentReference")
            ET.SubElement(inv_doc_ref, f"{cbc}ID").text = header['preceding_invoice_ref']
            if header.get('preceding_invoice_date'):
                ET.SubElement(inv_doc_ref, f"{cbc}IssueDate").text = self._parse_date(header['preceding_invoice_date'])
        # Additional preceding refs from Table6
        for ref in vp_data.get('preceding_invoices', []):
            ref_id = ref.get('reference')
            if ref_id and ref_id != header.get('preceding_invoice_ref'):
                billing_ref = ET.SubElement(invoice, f"{cac}BillingReference")
                inv_doc_ref = ET.SubElement(billing_ref, f"{cac}InvoiceDocumentReference")
                ET.SubElement(inv_doc_ref, f"{cbc}ID").text = ref_id

        # ---- UBL 2.1: Parties ----

        # Supplier Party
        self._add_supplier_party(invoice, supplier_config, mapper, cbc, cac)

        # Customer Party
        self._add_customer_party(invoice, header, mapper, cbc, cac)

        # ---- UBL 2.1: Delivery AFTER parties, BEFORE PaymentMeans ----

        # Delivery (ActualDeliveryDate — present in all official PUF examples)
        if header.get('supply_date'):
            delivery = ET.SubElement(invoice, f"{cac}Delivery")
            ET.SubElement(delivery, f"{cbc}ActualDeliveryDate").text = self._parse_date(header['supply_date'])

        # ---- UBL 2.1: PaymentMeans AFTER Delivery, BEFORE TaxTotal ----

        # Payment Means (from Table5 PaymentMode or PaymentTerms)
        self._add_payment_means(invoice, vp_data, header, supplier_config, cbc, cac)

        # Prepaid Amount (from Table4 - sum of PrepaymentVATCategoryTaxableAmount)
        prepaid_total = sum(p.get('taxable_amount', 0) for p in vp_data.get('prepayments', []))

        # Tax Total (in document currency)
        self._add_tax_total(invoice, vp_data, currency, mapper, cbc, cac)

        # Second Tax Total in tax currency (required for dual-currency, e.g. SA TaxCurrencyCode=SAR)
        tax_currency = header.get('tax_currency_code')
        if tax_currency and tax_currency != currency:
            self._add_tax_currency_total(invoice, vp_data, tax_currency, cbc, cac)

        # Legal Monetary Total
        self._add_monetary_total(invoice, header, cbc, cac, prepaid_amount=prepaid_total, doc_currency=currency)

        # Invoice/Credit Note Lines
        for line_data in vp_data.get('lines', []):
            self._add_invoice_line(invoice, line_data, mapper, cbc, cac, is_credit_note=is_credit_note, doc_currency=currency)

        # Convert to string
        return ET.tostring(
            invoice,
            encoding="UTF-8",
            xml_declaration=True,
            pretty_print=True
        ).decode("utf-8")

    def _add_country_extensions(
        self,
        invoice: ET.Element,
        mapper: Dict,
        supplier_config: Dict
    ):
        """Add country-specific UBL extensions."""

        extensions_config = mapper.get('extensions', {})
        special_rules = mapper.get('special_rules', {})
        country_code = mapper.get('country', {}).get('code', 'XX')

        ext = "{urn:oasis:names:specification:ubl:schema:xsd:CommonExtensionComponents-2}"
        puf = "{urn:pagero:ExtensionComponent:1.0}"
        cbc = "{urn:oasis:names:specification:ubl:schema:xsd:CommonBasicComponents-2}"

        ubl_extensions = ET.SubElement(invoice, f"{ext}UBLExtensions")

        # Italy-specific extensions
        if country_code == 'IT':
            self._add_italian_extensions(ubl_extensions, mapper, supplier_config, ext, puf, cbc)

        # France-specific extensions
        elif country_code == 'FR':
            self._add_french_extensions(ubl_extensions, mapper, supplier_config, ext, puf, cbc)

        # India-specific extensions (GST)
        elif country_code == 'IN':
            self._add_indian_extensions(ubl_extensions, mapper, supplier_config, ext, puf, cbc)

        # Poland-specific extensions (KSeF)
        elif country_code == 'PL':
            self._add_polish_extensions(ubl_extensions, mapper, supplier_config, ext, puf, cbc)

        # Romania-specific extensions (e-Factura)
        elif country_code == 'RO':
            self._add_romanian_extensions(ubl_extensions, mapper, supplier_config, ext, puf, cbc)

        # Serbia-specific extensions (eFaktura)
        elif country_code == 'RS':
            self._add_serbian_extensions(ubl_extensions, mapper, supplier_config, ext, puf, cbc)

        # Saudi Arabia-specific extensions (ZATCA Phase 2)
        elif country_code == 'SA':
            self._add_saudi_extensions(ubl_extensions, mapper, supplier_config, ext, puf, cbc)

        # Turkey-specific extensions (e-Fatura)
        elif country_code == 'TR':
            self._add_turkish_extensions(ubl_extensions, mapper, supplier_config, ext, puf, cbc)

        # Vietnam-specific extensions (SequenceNo, FPT_autoNumber)
        elif country_code == 'VN':
            self._add_vietnamese_extensions(ubl_extensions, mapper, supplier_config, ext, puf, cbc)

        # Greece-specific extensions (myDATA)
        elif country_code == 'GR':
            self._add_greek_extensions(ubl_extensions, mapper, supplier_config, ext, puf, cbc)

        # Philippines-specific extensions (PTU)
        elif country_code == 'PH':
            self._add_philippine_extensions(ubl_extensions, mapper, supplier_config, ext, puf, cbc)

    def _add_italian_extensions(
        self,
        parent: ET.Element,
        mapper: Dict,
        supplier_config: Dict,
        ext: str,
        puf: str,
        cbc: str
    ):
        """Add Italian-specific extensions (FatturaPA)."""

        extensions_config = mapper.get('extensions', {})
        registration_data = extensions_config.get('registration_data', {})
        defaults = mapper.get('defaults', {})

        # Registration Data Extension
        extension = ET.SubElement(parent, f"{ext}UBLExtension")
        ET.SubElement(extension, f"{ext}ExtensionURI").text = \
            "urn:pagero:ExtensionComponent:1.0:PageroExtension:PartyExtension"

        content = ET.SubElement(extension, f"{ext}ExtensionContent")
        pagero_ext = ET.SubElement(content, f"{puf}PageroExtension")
        party_ext = ET.SubElement(pagero_ext, f"{puf}PartyExtension")

        # Add registration data fields
        for field_key, field_config in registration_data.items():
            reg_data = ET.SubElement(party_ext, f"{puf}RegistrationData")
            value = supplier_config.get(field_key) or defaults.get(field_config.get('default', ''), '')
            ET.SubElement(reg_data, f"{cbc}ID").text = str(value) if value else ''
            id_type = ET.SubElement(reg_data, f"{puf}IDType")
            id_type.set('listID', 'PUF-001-REGISTRATIONDATA')
            id_type.text = field_config.get('id_type', '')

        # DutyStamp (Bollo) — €2.00 stamp duty on exempt invoices > €77.47
        special_rules = mapper.get('special_rules', {})
        if special_rules.get('requires_duty_stamp'):
            duty_stamp_amount = supplier_config.get('duty_stamp_amount')
            if duty_stamp_amount is not None:
                ds_ext = ET.SubElement(parent, f"{ext}UBLExtension")
                ET.SubElement(ds_ext, f"{ext}ExtensionURI").text = \
                    "urn:pagero:ExtensionComponent:1.0:PageroExtension:DutyStamp"
                ds_content = ET.SubElement(ds_ext, f"{ext}ExtensionContent")
                ds_pagero = ET.SubElement(ds_content, f"{puf}PageroExtension")
                ds_stamp = ET.SubElement(ds_pagero, f"{puf}DutyStamp")
                ET.SubElement(ds_stamp, f"{puf}Amount", currencyID="EUR").text = f"{float(duty_stamp_amount):.2f}"

    def _add_french_extensions(
        self,
        parent: ET.Element,
        mapper: Dict,
        supplier_config: Dict,
        ext: str,
        puf: str,
        cbc: str
    ):
        """Add French-specific extensions (Factur-X).

        NOTE: Official FR PUF example has NO document-level UBLExtensions.
        RegistrationData goes inside the Supplier Party element instead.
        This method is now a no-op; FR registration data is added in
        _add_supplier_party via _add_french_supplier_party_extensions.
        """
        # FR does not use document-level extensions per official Pagero example.
        # Registration data (CapitalSocial, RCS, APE) is added inside the
        # supplier Party element by _add_french_supplier_party_extensions().
        pass

    def _add_indian_extensions(
        self,
        parent: ET.Element,
        mapper: Dict,
        supplier_config: Dict,
        ext: str,
        puf: str,
        cbc: str
    ):
        """Add India-specific extensions (GST e-Invoice compliance)."""

        extensions_config = mapper.get('extensions', {})
        registration_data = extensions_config.get('registration_data', {})
        defaults = mapper.get('defaults', {})

        # Supply Type Extension (puf:SupplyType/puf:Code — per official Pagero IN example)
        supply_type_val = supplier_config.get('supply_type', defaults.get('supply_type', 'B2B'))
        extension = ET.SubElement(parent, f"{ext}UBLExtension")
        ET.SubElement(extension, f"{ext}ExtensionURI").text = \
            "urn:pagero:ExtensionComponent:1.0:PageroExtension:SupplyType"
        content = ET.SubElement(extension, f"{ext}ExtensionContent")
        pagero_ext = ET.SubElement(content, f"{puf}PageroExtension")
        supply_type_elem = ET.SubElement(pagero_ext, f"{puf}SupplyType")
        ET.SubElement(supply_type_elem, f"{puf}Code").text = supply_type_val

        # Place of Supply (POS code) — RestrictedInformation
        pos_ext = ET.SubElement(parent, f"{ext}UBLExtension")
        ET.SubElement(pos_ext, f"{ext}ExtensionURI").text = \
            "urn:pagero:ExtensionComponent:1.0:PageroExtension:RestrictedInformation"
        pos_content = ET.SubElement(pos_ext, f"{ext}ExtensionContent")
        pos_pagero = ET.SubElement(pos_content, f"{puf}PageroExtension")
        pos_config = registration_data.get('pos_code', {})
        restricted_info_pos = ET.SubElement(pos_pagero, f"{puf}RestrictedInformation")
        ET.SubElement(restricted_info_pos, f"{puf}Key").text = pos_config.get('id_type', 'IN:POS')
        ET.SubElement(restricted_info_pos, f"{puf}Value").text = supplier_config.get('pos_code', '07')

        # Reverse Charge indicator
        if supplier_config.get('reverse_charge'):
            rc_config = registration_data.get('reverse_charge', {})
            restricted_info_rc = ET.SubElement(pagero_ext, f"{puf}RestrictedInformation")
            ET.SubElement(restricted_info_rc, f"{puf}Key").text = rc_config.get('id_type', 'IN:ReverseCharge')
            ET.SubElement(restricted_info_rc, f"{puf}Value").text = supplier_config.get('reverse_charge', 'N')

    def _add_polish_extensions(
        self,
        parent: ET.Element,
        mapper: Dict,
        supplier_config: Dict,
        ext: str,
        puf: str,
        cbc: str
    ):
        """Add Poland-specific extensions (KSeF compliance)."""

        extensions_config = mapper.get('extensions', {})
        ksef_config = extensions_config.get('ksef', {})
        registration_data = ksef_config.get('registration_data', {})

        # KSeF Extension
        extension = ET.SubElement(parent, f"{ext}UBLExtension")
        ET.SubElement(extension, f"{ext}ExtensionURI").text = \
            "urn:pagero:ExtensionComponent:1.0:PageroExtension:RestrictedInformation"

        content = ET.SubElement(extension, f"{ext}ExtensionContent")
        pagero_ext = ET.SubElement(content, f"{puf}PageroExtension")

        # KSeF Number (if available - assigned by system after submission)
        if supplier_config.get('ksef_number'):
            ksef_num_config = registration_data.get('ksef_number', {})
            restricted_info = ET.SubElement(pagero_ext, f"{puf}RestrictedInformation")
            ET.SubElement(restricted_info, f"{puf}Key").text = ksef_num_config.get('id_type', 'PL:KSeFNumber')
            ET.SubElement(restricted_info, f"{puf}Value").text = supplier_config.get('ksef_number', '')

        # GTU Codes (Goods/Services Type)
        gtu_config = extensions_config.get('gtu_codes', {})
        if supplier_config.get('gtu_codes'):
            for gtu_code in supplier_config.get('gtu_codes', []):
                restricted_info_gtu = ET.SubElement(pagero_ext, f"{puf}RestrictedInformation")
                ET.SubElement(restricted_info_gtu, f"{puf}Key").text = gtu_config.get('id_type', 'PL:GTU')
                ET.SubElement(restricted_info_gtu, f"{puf}Value").text = gtu_code

        # Transaction markers (TP, SW, MPP, etc.)
        transaction_markers = extensions_config.get('transaction_markers', {})
        for marker_key in ['tp', 'sw', 'mpp']:
            if supplier_config.get(marker_key):
                marker_config = transaction_markers.get(marker_key, {})
                restricted_info_marker = ET.SubElement(pagero_ext, f"{puf}RestrictedInformation")
                ET.SubElement(restricted_info_marker, f"{puf}Key").text = marker_config.get('id_type', f'PL:{marker_key.upper()}')
                ET.SubElement(restricted_info_marker, f"{puf}Value").text = 'true'

        # checkoutMethod (required by official PL PUF example)
        checkout_method = supplier_config.get('checkout_method', '1')
        restricted_info_cm = ET.SubElement(pagero_ext, f"{puf}RestrictedInformation")
        ET.SubElement(restricted_info_cm, f"{puf}Key").text = 'checkoutMethod'
        ET.SubElement(restricted_info_cm, f"{puf}Value").text = checkout_method

        # simplifiedTriangularProcedure (required by official PL PUF example)
        stp = supplier_config.get('simplified_triangular_procedure', 'false')
        restricted_info_stp = ET.SubElement(pagero_ext, f"{puf}RestrictedInformation")
        ET.SubElement(restricted_info_stp, f"{puf}Key").text = 'simplifiedTriangularProcedure'
        ET.SubElement(restricted_info_stp, f"{puf}Value").text = stp

    def _add_romanian_extensions(
        self,
        parent: ET.Element,
        mapper: Dict,
        supplier_config: Dict,
        ext: str,
        puf: str,
        cbc: str
    ):
        """Add Romania-specific extensions (e-Factura/RO e-Invoice compliance)."""

        extensions_config = mapper.get('extensions', {})
        efactura_config = extensions_config.get('efactura', {})
        registration_data = efactura_config.get('registration_data', {})

        # e-Factura Extension
        extension = ET.SubElement(parent, f"{ext}UBLExtension")
        ET.SubElement(extension, f"{ext}ExtensionURI").text = \
            "urn:pagero:ExtensionComponent:1.0:PageroExtension:RestrictedInformation"

        content = ET.SubElement(extension, f"{ext}ExtensionContent")
        pagero_ext = ET.SubElement(content, f"{puf}PageroExtension")

        # Upload Index (SPV reference)
        if supplier_config.get('upload_index'):
            upload_config = registration_data.get('upload_index', {})
            restricted_info = ET.SubElement(pagero_ext, f"{puf}RestrictedInformation")
            ET.SubElement(restricted_info, f"{puf}Key").text = upload_config.get('id_type', 'RO:UploadIndex')
            ET.SubElement(restricted_info, f"{puf}Value").text = supplier_config.get('upload_index', '')

        # Invoice status in SPV
        if supplier_config.get('stare'):
            stare_config = registration_data.get('stare', {})
            restricted_info_stare = ET.SubElement(pagero_ext, f"{puf}RestrictedInformation")
            ET.SubElement(restricted_info_stare, f"{puf}Key").text = stare_config.get('id_type', 'RO:Stare')
            ET.SubElement(restricted_info_stare, f"{puf}Value").text = supplier_config.get('stare', 'VALID')

    def _add_serbian_extensions(
        self,
        parent: ET.Element,
        mapper: Dict,
        supplier_config: Dict,
        ext: str,
        puf: str,
        cbc: str
    ):
        """Add Serbia-specific extensions (eFaktura/SEF compliance)."""

        extensions_config = mapper.get('extensions', {})
        efaktura_config = extensions_config.get('efaktura', {})
        registration_data = efaktura_config.get('registration_data', {})

        # eFaktura Extension
        extension = ET.SubElement(parent, f"{ext}UBLExtension")
        ET.SubElement(extension, f"{ext}ExtensionURI").text = \
            "urn:pagero:ExtensionComponent:1.0:PageroExtension:RestrictedInformation"

        content = ET.SubElement(extension, f"{ext}ExtensionContent")
        pagero_ext = ET.SubElement(content, f"{puf}PageroExtension")

        # SEF Invoice ID
        if supplier_config.get('sef_id'):
            sef_config = registration_data.get('sef_id', {})
            restricted_info = ET.SubElement(pagero_ext, f"{puf}RestrictedInformation")
            ET.SubElement(restricted_info, f"{puf}Key").text = sef_config.get('id_type', 'RS:SEFId')
            ET.SubElement(restricted_info, f"{puf}Value").text = supplier_config.get('sef_id', '')

        # JBKJS (for public sector buyers)
        if supplier_config.get('jbkjs'):
            jbkjs_config = registration_data.get('jbkjs', {})
            restricted_info_jbkjs = ET.SubElement(pagero_ext, f"{puf}RestrictedInformation")
            ET.SubElement(restricted_info_jbkjs, f"{puf}Key").text = jbkjs_config.get('id_type', 'RS:JBKJS')
            ET.SubElement(restricted_info_jbkjs, f"{puf}Value").text = supplier_config.get('jbkjs', '')

        # Invoice status
        if supplier_config.get('status'):
            status_config = registration_data.get('status', {})
            restricted_info_status = ET.SubElement(pagero_ext, f"{puf}RestrictedInformation")
            ET.SubElement(restricted_info_status, f"{puf}Key").text = status_config.get('id_type', 'RS:Status')
            ET.SubElement(restricted_info_status, f"{puf}Value").text = supplier_config.get('status', 'DRAFT')

    def _add_saudi_extensions(
        self,
        parent: ET.Element,
        mapper: Dict,
        supplier_config: Dict,
        ext: str,
        puf: str,
        cbc: str
    ):
        """Add Saudi Arabia-specific extensions (ZATCA Phase 2/FATOORA compliance)."""

        extensions_config = mapper.get('extensions', {})
        zatca_config = extensions_config.get('zatca', {})
        registration_data = zatca_config.get('registration_data', {})
        defaults = mapper.get('defaults', {})

        # ZATCA Extension
        extension = ET.SubElement(parent, f"{ext}UBLExtension")
        ET.SubElement(extension, f"{ext}ExtensionURI").text = \
            "urn:pagero:ExtensionComponent:1.0:PageroExtension:RestrictedInformation"

        content = ET.SubElement(extension, f"{ext}ExtensionContent")
        pagero_ext = ET.SubElement(content, f"{puf}PageroExtension")

        # UUID (mandatory for Phase 2)
        uuid_config = registration_data.get('uuid', {})
        restricted_info_uuid = ET.SubElement(pagero_ext, f"{puf}RestrictedInformation")
        ET.SubElement(restricted_info_uuid, f"{puf}Key").text = uuid_config.get('id_type', 'SA:UUID')
        ET.SubElement(restricted_info_uuid, f"{puf}Value").text = supplier_config.get('uuid', '')

        # Invoice Type Code (transaction type)
        itc_config = registration_data.get('invoice_type_code', {})
        restricted_info_itc = ET.SubElement(pagero_ext, f"{puf}RestrictedInformation")
        ET.SubElement(restricted_info_itc, f"{puf}Key").text = 'SA:InvoiceTypeCode'
        ET.SubElement(restricted_info_itc, f"{puf}Value").text = supplier_config.get('invoice_type_code', defaults.get('invoice_type_code', '0100000'))

        # QR Code (Base64 encoded TLV data)
        if supplier_config.get('qr_code'):
            qr_config = registration_data.get('qr_code', {})
            restricted_info_qr = ET.SubElement(pagero_ext, f"{puf}RestrictedInformation")
            ET.SubElement(restricted_info_qr, f"{puf}Key").text = qr_config.get('id_type', 'SA:QRCode')
            ET.SubElement(restricted_info_qr, f"{puf}Value").text = supplier_config.get('qr_code', '')

        # Previous Invoice Hash (for invoice chaining)
        if supplier_config.get('previous_invoice_hash'):
            pih_config = registration_data.get('previous_invoice_hash', {})
            restricted_info_pih = ET.SubElement(pagero_ext, f"{puf}RestrictedInformation")
            ET.SubElement(restricted_info_pih, f"{puf}Key").text = pih_config.get('id_type', 'SA:PIH')
            ET.SubElement(restricted_info_pih, f"{puf}Value").text = supplier_config.get('previous_invoice_hash', '')

        # Address extensions (Building Number, District)
        address_config = extensions_config.get('address_requirements', {})

        if supplier_config.get('building_number'):
            bn_config = address_config.get('building_number', {})
            restricted_info_bn = ET.SubElement(pagero_ext, f"{puf}RestrictedInformation")
            ET.SubElement(restricted_info_bn, f"{puf}Key").text = bn_config.get('id_type', 'KSA-17')
            ET.SubElement(restricted_info_bn, f"{puf}Value").text = supplier_config.get('building_number', '')

        if supplier_config.get('district'):
            dist_config = address_config.get('district', {})
            restricted_info_dist = ET.SubElement(pagero_ext, f"{puf}RestrictedInformation")
            ET.SubElement(restricted_info_dist, f"{puf}Key").text = dist_config.get('id_type', 'KSA-3')
            ET.SubElement(restricted_info_dist, f"{puf}Value").text = supplier_config.get('district', '')

    def _add_turkish_extensions(
        self,
        parent: ET.Element,
        mapper: Dict,
        supplier_config: Dict,
        ext: str,
        puf: str,
        cbc: str
    ):
        """Add Turkey-specific extensions (e-Fatura/e-Arsiv compliance)."""

        extensions_config = mapper.get('extensions', {})
        efatura_config = extensions_config.get('efatura', {})
        registration_data = efatura_config.get('registration_data', {})
        defaults = mapper.get('defaults', {})

        # e-Fatura Extension
        extension = ET.SubElement(parent, f"{ext}UBLExtension")
        ET.SubElement(extension, f"{ext}ExtensionURI").text = \
            "urn:pagero:ExtensionComponent:1.0:PageroExtension:RestrictedInformation"

        content = ET.SubElement(extension, f"{ext}ExtensionContent")
        pagero_ext = ET.SubElement(content, f"{puf}PageroExtension")

        # ETTN (UUID for Turkish invoices)
        ettn_config = registration_data.get('ettn', {})
        restricted_info_ettn = ET.SubElement(pagero_ext, f"{puf}RestrictedInformation")
        ET.SubElement(restricted_info_ettn, f"{puf}Key").text = ettn_config.get('id_type', 'TR:ETTN')
        ET.SubElement(restricted_info_ettn, f"{puf}Value").text = supplier_config.get('ettn', '')

        # Invoice Scenario (TEMELFATURA, TICARIFATURA, etc.)
        restricted_info_scenario = ET.SubElement(pagero_ext, f"{puf}RestrictedInformation")
        ET.SubElement(restricted_info_scenario, f"{puf}Key").text = 'TR:InvoiceScenario'
        ET.SubElement(restricted_info_scenario, f"{puf}Value").text = supplier_config.get('scenario', defaults.get('scenario', 'TEMELFATURA'))

        # Invoice Type (SATIS, IADE, TEVKIFAT, etc.)
        restricted_info_type = ET.SubElement(pagero_ext, f"{puf}RestrictedInformation")
        ET.SubElement(restricted_info_type, f"{puf}Key").text = 'TR:InvoiceType'
        ET.SubElement(restricted_info_type, f"{puf}Value").text = supplier_config.get('invoice_type', defaults.get('invoice_type', 'SATIS'))

        # Tax Office
        if supplier_config.get('tax_office'):
            restricted_info_to = ET.SubElement(pagero_ext, f"{puf}RestrictedInformation")
            ET.SubElement(restricted_info_to, f"{puf}Key").text = 'TR:TaxOffice'
            ET.SubElement(restricted_info_to, f"{puf}Value").text = supplier_config.get('tax_office', '')

        # GIB Postbox alias
        if supplier_config.get('gib_postbox'):
            gib_config = registration_data.get('gib_postbox', {})
            restricted_info_gib = ET.SubElement(pagero_ext, f"{puf}RestrictedInformation")
            ET.SubElement(restricted_info_gib, f"{puf}Key").text = gib_config.get('id_type', 'TR:GIBPostbox')
            ET.SubElement(restricted_info_gib, f"{puf}Value").text = supplier_config.get('gib_postbox', '')

        # Withholding code (for TEVKIFAT invoices)
        withholding_config = extensions_config.get('withholding', {})
        if supplier_config.get('withholding_code'):
            restricted_info_wh = ET.SubElement(pagero_ext, f"{puf}RestrictedInformation")
            ET.SubElement(restricted_info_wh, f"{puf}Key").text = withholding_config.get('id_type', 'TR:Withholding')
            ET.SubElement(restricted_info_wh, f"{puf}Value").text = supplier_config.get('withholding_code', '')

        # WithholdingTaxTotal (PUF extension for TEVKIFAT invoices with withholding amounts)
        if supplier_config.get('withholding_amount') is not None:
            cac = "{urn:pagero:CommonAggregateComponents:1.0}"
            currency = supplier_config.get('currency', 'TRY')
            wh_ext = ET.SubElement(parent, f"{ext}UBLExtension")
            ET.SubElement(wh_ext, f"{ext}ExtensionURI").text = \
                "urn:pagero:ExtensionComponent:1.0:PageroExtension:WithholdingTaxTotal"
            wh_content = ET.SubElement(wh_ext, f"{ext}ExtensionContent")
            wh_pagero = ET.SubElement(wh_content, f"{puf}PageroExtension")
            wh_total = ET.SubElement(wh_pagero, f"{puf}WithholdingTaxTotal")

            wh_amount = supplier_config.get('withholding_amount', 0)
            ET.SubElement(wh_total, f"{cbc}TaxAmount", currencyID=currency).text = f"{wh_amount:.2f}"

            wh_subtotal = ET.SubElement(wh_total, f"{cac}TaxSubtotal")
            wh_taxable = supplier_config.get('withholding_taxable_amount', 0)
            ET.SubElement(wh_subtotal, f"{cbc}TaxableAmount", currencyID=currency).text = f"{wh_taxable:.2f}"
            ET.SubElement(wh_subtotal, f"{cbc}TaxAmount", currencyID=currency).text = f"{wh_amount:.2f}"

            wh_category = ET.SubElement(wh_subtotal, f"{cac}TaxCategory")
            ET.SubElement(wh_category, f"{cbc}ID").text = 'S'
            wh_rate = supplier_config.get('withholding_rate', 0)
            ET.SubElement(wh_category, f"{cbc}Percent").text = f"{wh_rate:.2f}"
            wh_scheme = ET.SubElement(wh_category, f"{cac}TaxScheme")
            ET.SubElement(wh_scheme, f"{cbc}ID").text = '0015'

    def _add_vietnamese_extensions(
        self,
        parent: ET.Element,
        mapper: Dict,
        supplier_config: Dict,
        ext: str,
        puf: str,
        cbc: str
    ):
        """Add Vietnam-specific extensions (SequenceNo, FPT_autoNumber)."""

        extension = ET.SubElement(parent, f"{ext}UBLExtension")
        ET.SubElement(extension, f"{ext}ExtensionURI").text = \
            "urn:pagero:ExtensionComponent:1.0:PageroExtension:RestrictedInformation"
        content = ET.SubElement(extension, f"{ext}ExtensionContent")
        pagero_ext = ET.SubElement(content, f"{puf}PageroExtension")

        # SequenceNo (invoice sequence number in tax authority system)
        seq_no = supplier_config.get('sequence_no', '')
        ri_seq = ET.SubElement(pagero_ext, f"{puf}RestrictedInformation")
        ET.SubElement(ri_seq, f"{puf}Key").text = 'SequenceNo'
        ET.SubElement(ri_seq, f"{puf}Value").text = seq_no

        # FPT_autoNumber (auto-numbering flag)
        auto_num = supplier_config.get('fpt_auto_number', 'true')
        ri_auto = ET.SubElement(pagero_ext, f"{puf}RestrictedInformation")
        ET.SubElement(ri_auto, f"{puf}Key").text = 'FPT_autoNumber'
        ET.SubElement(ri_auto, f"{puf}Value").text = auto_num

    def _add_greek_extensions(
        self,
        parent: ET.Element,
        mapper: Dict,
        supplier_config: Dict,
        ext: str,
        puf: str,
        cbc: str
    ):
        """Add Greece-specific extensions (myDATA as attachment reference)."""

        # myDATA is submitted as a base64 AdditionalDocumentReference
        # with ID = ##InvoicesDoc##. The actual myDATA XML is provided
        # by the supplier_config after generation.
        mydata_xml_b64 = supplier_config.get('mydata_xml_base64')
        if mydata_xml_b64:
            extension = ET.SubElement(parent, f"{ext}UBLExtension")
            ET.SubElement(extension, f"{ext}ExtensionURI").text = \
                "urn:pagero:ExtensionComponent:1.0:PageroExtension:RestrictedInformation"
            content = ET.SubElement(extension, f"{ext}ExtensionContent")
            pagero_ext = ET.SubElement(content, f"{puf}PageroExtension")

            ri = ET.SubElement(pagero_ext, f"{puf}RestrictedInformation")
            ET.SubElement(ri, f"{puf}Key").text = 'GR:myDATA'
            ET.SubElement(ri, f"{puf}Value").text = 'enabled'

    def _add_philippine_extensions(
        self,
        parent: ET.Element,
        mapper: Dict,
        supplier_config: Dict,
        ext: str,
        puf: str,
        cbc: str
    ):
        """Add Philippines-specific extensions (PTU — Permit to Use)."""

        extension = ET.SubElement(parent, f"{ext}UBLExtension")
        ET.SubElement(extension, f"{ext}ExtensionURI").text = \
            "urn:pagero:ExtensionComponent:1.0:PageroExtension:RestrictedInformation"
        content = ET.SubElement(extension, f"{ext}ExtensionContent")
        pagero_ext = ET.SubElement(content, f"{puf}PageroExtension")

        # PTU Number (Permit to Use from BIR)
        ptu_number = supplier_config.get('ptu_number', '')
        ri_ptu = ET.SubElement(pagero_ext, f"{puf}RestrictedInformation")
        ET.SubElement(ri_ptu, f"{puf}Key").text = 'PH:PTU'
        ET.SubElement(ri_ptu, f"{puf}Value").text = ptu_number

    def _add_spanish_document_extensions(
        self,
        invoice: ET.Element,
        vp_data: Dict,
        mapper: Dict,
        supplier_config: Dict
    ):
        """Add Spain-specific document-level extensions (VeriFactu InvoiceSeries).

        VeriFactu requires an InvoiceSeries extension at the document level,
        before CustomizationID. Only added if verifactu_enabled is set in
        special_rules and an invoice_series is available.
        """
        special_rules = mapper.get('special_rules', {})
        if not special_rules.get('verifactu_enabled'):
            return

        header = vp_data.get('header', {})
        # Invoice series from VP data, supplier_config override, or print_identifier
        invoice_series = (
            header.get('invoice_series') or
            supplier_config.get('invoice_series') or
            header.get('print_identifier')
        )
        if not invoice_series:
            return

        ext = "{urn:oasis:names:specification:ubl:schema:xsd:CommonExtensionComponents-2}"
        puf = "{urn:pagero:ExtensionComponent:1.0}"
        cbc = "{urn:oasis:names:specification:ubl:schema:xsd:CommonBasicComponents-2}"

        ubl_extensions = ET.SubElement(invoice, f"{ext}UBLExtensions")
        extension = ET.SubElement(ubl_extensions, f"{ext}UBLExtension")
        ET.SubElement(extension, f"{ext}ExtensionURI").text = \
            "urn:pagero:ExtensionComponent:1.0:PageroExtension:InvoiceSeries"
        content = ET.SubElement(extension, f"{ext}ExtensionContent")
        pagero_ext = ET.SubElement(content, f"{puf}PageroExtension")
        series_elem = ET.SubElement(pagero_ext, f"{puf}InvoiceSeries")
        ET.SubElement(series_elem, f"{cbc}ID").text = invoice_series

    def _add_spanish_buyer_extensions(
        self,
        party: ET.Element,
        header: Dict,
        mapper: Dict,
        cbc: str
    ):
        """Add Spain-specific buyer party extensions (DIR3 codes for B2G).

        DIR3 codes go INSIDE the buyer <cac:Party> element as UBLExtensions
        with puf:ADAID elements — different from other countries where
        extensions are at document level.
        """
        ext_ns = "urn:oasis:names:specification:ubl:schema:xsd:CommonExtensionComponents-2"
        puf_ns = "urn:pagero:ExtensionComponent:1.0"
        ext = f"{{{ext_ns}}}"
        puf = f"{{{puf_ns}}}"

        extensions_config = mapper.get('extensions', {})
        public_sector = extensions_config.get('public_sector', {})
        registration_data = public_sector.get('registration_data', {})

        # Collect available DIR3 codes from VP data
        dir3_codes = []
        for field_key, config in registration_data.items():
            value = header.get(field_key)
            if value:
                dir3_codes.append((value, config.get('id_type', f'ES:{field_key}')))

        if not dir3_codes:
            return

        # Insert UBLExtensions as FIRST child of party (before PartyName, PostalAddress, etc.)
        ubl_extensions = ET.Element(f"{ext}UBLExtensions")
        extension = ET.SubElement(ubl_extensions, f"{ext}UBLExtension")
        ET.SubElement(extension, f"{ext}ExtensionURI").text = \
            "urn:pagero:ExtensionComponent:1.0:PageroExtension:PartyExtension"
        content = ET.SubElement(extension, f"{ext}ExtensionContent")
        pagero_ext = ET.SubElement(content, f"{puf}PageroExtension")
        party_ext = ET.SubElement(pagero_ext, f"{puf}PartyExtension")

        for code_value, id_type in dir3_codes:
            adaid = ET.SubElement(party_ext, f"{puf}ADAID")
            ET.SubElement(adaid, f"{cbc}ID").text = code_value
            id_type_elem = ET.SubElement(adaid, f"{puf}IDType")
            id_type_elem.set('listID', 'PUF-002-ADAID')
            id_type_elem.text = id_type

        # Insert as first child of party element
        party.insert(0, ubl_extensions)

    def _add_french_supplier_party_extensions(
        self,
        party: ET.Element,
        mapper: Dict,
        supplier_config: Dict,
        cbc: str
    ):
        """Add French RegistrationData inside supplier Party (not document-level).

        Official FR PUF example places RegistrationData (CapitalSocial, RCS, APE)
        inside the supplier Party UBLExtensions, not at document level.
        """
        ext_ns = "urn:oasis:names:specification:ubl:schema:xsd:CommonExtensionComponents-2"
        puf_ns = "urn:pagero:ExtensionComponent:1.0"
        ext = f"{{{ext_ns}}}"
        puf = f"{{{puf_ns}}}"

        extensions_config = mapper.get('extensions', {})
        registration_data = extensions_config.get('registration_data', {})
        if not registration_data:
            return

        ubl_extensions = ET.Element(f"{ext}UBLExtensions")
        extension = ET.SubElement(ubl_extensions, f"{ext}UBLExtension")
        ET.SubElement(extension, f"{ext}ExtensionURI").text = \
            "urn:pagero:ExtensionComponent:1.0:PageroExtension:PartyExtension"
        content = ET.SubElement(extension, f"{ext}ExtensionContent")
        pagero_ext = ET.SubElement(content, f"{puf}PageroExtension")
        party_ext = ET.SubElement(pagero_ext, f"{puf}PartyExtension")

        for field_key, field_config in registration_data.items():
            reg_data = ET.SubElement(party_ext, f"{puf}RegistrationData")
            value = supplier_config.get(field_key, '')
            ET.SubElement(reg_data, f"{cbc}ID").text = str(value) if value else ''
            id_type = ET.SubElement(reg_data, f"{puf}IDType")
            id_type.set('listID', 'PUF-001-REGISTRATIONDATA')
            id_type.text = field_config.get('id_type', '')

        # Insert as first child of party
        party.insert(0, ubl_extensions)

    def _add_french_coded_notes(
        self,
        invoice: ET.Element,
        supplier_config: Dict,
        mapper: Dict,
        cbc: str
    ):
        """Add French mandatory coded notes (Factur-X / Chorus Pro).

        Official FR PUF examples include coded notes with prefixes:
        #REG# — Legal registration info
        #ABL# — Payment penalty text
        #AAI# — General information
        #PMD# — Payment discount info
        #PMT# — Collection indemnity (40 EUR fixed)
        #BAR# — Treatment type
        """
        defaults = mapper.get('defaults', {})

        # #REG# — Legal registration (auto-generated from supplier data)
        reg_name = supplier_config.get('registration_name', '')
        capital = supplier_config.get('capital_social', '')
        rcs = supplier_config.get('rcs_number', '')
        if reg_name or capital or rcs:
            parts = [p for p in [reg_name, f"Capital {capital}" if capital else '', rcs] if p]
            ET.SubElement(invoice, f"{cbc}Note").text = f"#REG# {' - '.join(parts)}"

        # #ABL# — Late payment penalties
        abl_text = supplier_config.get('late_payment_penalty')
        if abl_text:
            ET.SubElement(invoice, f"{cbc}Note").text = f"#ABL# {abl_text}"

        # #PMT# — Collection indemnity (fixed 40 EUR per French law)
        ET.SubElement(invoice, f"{cbc}Note").text = "#PMT# Indemnite forfaitaire de recouvrement: 40 EUR"

    def _add_supplier_party(
        self,
        parent: ET.Element,
        config: Dict,
        mapper: Dict,
        cbc: str,
        cac: str
    ):
        """Add supplier party with mapper-based scheme IDs."""

        identifiers = mapper.get('identifiers', {})

        supplier = ET.SubElement(parent, f"{cac}AccountingSupplierParty")
        party = ET.SubElement(supplier, f"{cac}Party")

        # FR: RegistrationData goes inside supplier Party (before other children)
        country_code = mapper.get('country', {}).get('code', 'XX')
        if country_code == 'FR':
            self._add_french_supplier_party_extensions(party, mapper, config, cbc)

        # Endpoint — scheme ID always from the single detected country mapper
        seller_scheme = identifiers.get('seller_endpoint_scheme', '0088')
        if config.get('endpoint_id'):
            ET.SubElement(party, f"{cbc}EndpointID", schemeID=seller_scheme).text = config['endpoint_id']

        # Party ID (primary)
        if config.get('party_id'):
            party_id = ET.SubElement(party, f"{cac}PartyIdentification")
            ET.SubElement(party_id, f"{cbc}ID", schemeID=seller_scheme).text = config['party_id']

        # Additional seller IDs from mapper (e.g. SA:CRN, IN:GSTIN, PL:NIP)
        additional_ids = identifiers.get('additional_seller_ids', {})
        for config_key, scheme_id in additional_ids.items():
            value = config.get(config_key)
            if value:
                extra_id = ET.SubElement(party, f"{cac}PartyIdentification")
                ET.SubElement(extra_id, f"{cbc}ID", schemeID=scheme_id).text = value

        # Name
        if config.get('name'):
            party_name = ET.SubElement(party, f"{cac}PartyName")
            ET.SubElement(party_name, f"{cbc}Name").text = config['name']

        # Address
        address = ET.SubElement(party, f"{cac}PostalAddress")
        street = config.get('street', '')
        building_num = config.get('building_num', '')
        # Only prepend building_num if street doesn't already start with it
        if building_num and street and not street.startswith(building_num):
            street = f"{building_num} {street}".strip()
        elif building_num and not street:
            street = building_num
        if street:
            ET.SubElement(address, f"{cbc}StreetName").text = street
        if config.get('city'):
            ET.SubElement(address, f"{cbc}CityName").text = config['city']
        if config.get('postal_code'):
            ET.SubElement(address, f"{cbc}PostalZone").text = config['postal_code']
        if config.get('district'):
            ET.SubElement(address, f"{cbc}CountrySubentity").text = config['district']

        country = ET.SubElement(address, f"{cac}Country")
        ET.SubElement(country, f"{cbc}IdentificationCode").text = config.get('country_code', 'GB')

        # VAT
        if config.get('vat_id'):
            tax_scheme = ET.SubElement(party, f"{cac}PartyTaxScheme")
            ET.SubElement(tax_scheme, f"{cbc}CompanyID").text = config['vat_id']
            scheme = ET.SubElement(tax_scheme, f"{cac}TaxScheme")
            tax_type = mapper.get('tax_system', {}).get('type', 'VAT')
            ET.SubElement(scheme, f"{cbc}ID").text = tax_type

        # IT: Second PartyTaxScheme for Codice Fiscale (TAX scheme)
        special_rules = mapper.get('special_rules', {})
        if special_rules.get('requires_fiscal_code') and config.get('fiscal_code'):
            tax_scheme2 = ET.SubElement(party, f"{cac}PartyTaxScheme")
            ET.SubElement(tax_scheme2, f"{cbc}CompanyID").text = config['fiscal_code']
            scheme2 = ET.SubElement(tax_scheme2, f"{cac}TaxScheme")
            ET.SubElement(scheme2, f"{cbc}ID").text = 'TAX'

        # Legal Entity
        if config.get('registration_name'):
            legal = ET.SubElement(party, f"{cac}PartyLegalEntity")
            ET.SubElement(legal, f"{cbc}RegistrationName").text = config['registration_name']
            if config.get('company_id'):
                company_scheme = identifiers.get('company_scheme_id', '0009')
                ET.SubElement(legal, f"{cbc}CompanyID", schemeID=company_scheme).text = config['company_id']

        # Contact
        if config.get('contact_name') or config.get('contact_phone') or config.get('contact_email'):
            contact = ET.SubElement(party, f"{cac}Contact")
            if config.get('contact_name'):
                ET.SubElement(contact, f"{cbc}Name").text = config['contact_name']
            if config.get('contact_phone'):
                ET.SubElement(contact, f"{cbc}Telephone").text = config['contact_phone']
            if config.get('contact_email'):
                ET.SubElement(contact, f"{cbc}ElectronicMail").text = config['contact_email']

    def _add_customer_party(
        self,
        parent: ET.Element,
        header: Dict,
        mapper: Dict,
        cbc: str,
        cac: str
    ):
        """Add customer party with mapper-based scheme IDs."""

        identifiers = mapper.get('identifiers', {})
        country = mapper.get('country', {})

        # Validate buyer identification exists
        buyer_vat = header.get('buyer_vat_num')
        buyer_id = header.get('buyer_id')
        buyer_name = header.get('buyer_name')

        if not buyer_vat and not buyer_id:
            raise ValueError(
                "Missing buyer identification in Vantagepoint data. "
                "At least one of BuyerVATNum or BuyerID is required for "
                "AccountingCustomerParty. Check the stored procedure output."
            )

        customer = ET.SubElement(parent, f"{cac}AccountingCustomerParty")

        # Supplier-assigned account ID (use BuyerID from VP, fallback to BuyerOtherID)
        account_id = buyer_id or header.get('buyer_other_id')
        if not account_id:
            raise ValueError(
                f"Missing BuyerID in Vantagepoint data for buyer '{buyer_name}'. "
                f"SupplierAssignedAccountID requires a buyer identifier."
            )
        ET.SubElement(customer, f"{cbc}SupplierAssignedAccountID").text = account_id

        party = ET.SubElement(customer, f"{cac}Party")

        # Spain: DIR3 codes go inside buyer party as UBLExtensions (before other party children)
        country_code = country.get('code', 'XX')
        if country_code == 'ES':
            self._add_spanish_buyer_extensions(party, header, mapper, cbc)

        # Determine endpoint and scheme
        # Italy: EndpointID uses Codice Destinatario (0201) or PEC (0202) for SDI routing.
        # Other countries: Use BuyerVATNum as primary, fallback to BuyerID.
        additional_buyer_fields = mapper.get('field_mappings', {}).get('additional_buyer_fields', {})

        if country_code == 'IT':
            # IT routing: prefer Codice Destinatario (0201), then PEC (0202), then VAT
            codice_dest = header.get('codice_destinatario')
            pec_address = header.get('pec_address')
            scheme_mappings = identifiers.get('scheme_mappings', {})
            if codice_dest:
                endpoint = codice_dest
                scheme = scheme_mappings.get('codice_destinatario', '0201')
            elif pec_address:
                endpoint = pec_address
                scheme = scheme_mappings.get('pec', '0202')
            else:
                endpoint = buyer_vat or buyer_id
                scheme = self._determine_scheme_id(endpoint, identifiers)
        else:
            endpoint = buyer_vat or buyer_id
            scheme = self._determine_scheme_id(endpoint, identifiers)

        # Endpoint
        ET.SubElement(party, f"{cbc}EndpointID", schemeID=scheme).text = endpoint

        # Party ID (primary — VAT number for all countries)
        party_id = ET.SubElement(party, f"{cac}PartyIdentification")
        primary_id = buyer_vat or buyer_id
        primary_scheme = self._determine_scheme_id(primary_id, identifiers)
        ET.SubElement(party_id, f"{cbc}ID", schemeID=primary_scheme).text = primary_id

        # Additional buyer IDs from mapper (e.g. IN:GSTIN, RS:PIB, PL:NIP, IT:FiscalCode)
        additional_buyer_ids = identifiers.get('additional_buyer_ids', {})
        for config_key, scheme_id in additional_buyer_ids.items():
            vp_field = additional_buyer_fields.get(config_key)
            # After enrichment, additional fields are stored by internal key in header
            value = header.get(config_key) if not vp_field else header.get(vp_field)
            # Also try internal key directly (enrichment stores by internal key)
            if not value:
                value = header.get(config_key)
            if value:
                extra_id = ET.SubElement(party, f"{cac}PartyIdentification")
                ET.SubElement(extra_id, f"{cbc}ID", schemeID=scheme_id).text = value

        # Name
        buyer_name = header.get('buyer_name', 'Customer')
        party_name = ET.SubElement(party, f"{cac}PartyName")
        ET.SubElement(party_name, f"{cbc}Name").text = buyer_name

        # Address
        address = ET.SubElement(party, f"{cac}PostalAddress")
        street = header.get('buyer_street', '')
        if header.get('buyer_building_num'):
            street = f"{header['buyer_building_num']} {street}".strip()
        if street:
            ET.SubElement(address, f"{cbc}StreetName").text = street
        if header.get('buyer_additional_street'):
            ET.SubElement(address, f"{cbc}AdditionalStreetName").text = header['buyer_additional_street']
        if header.get('buyer_city'):
            ET.SubElement(address, f"{cbc}CityName").text = header['buyer_city']
        if header.get('buyer_postal_code'):
            ET.SubElement(address, f"{cbc}PostalZone").text = header['buyer_postal_code']
        if header.get('buyer_state') or header.get('buyer_district'):
            ET.SubElement(address, f"{cbc}CountrySubentity").text = \
                header.get('buyer_state') or header.get('buyer_district')

        buyer_country = ET.SubElement(address, f"{cac}Country")
        ET.SubElement(buyer_country, f"{cbc}IdentificationCode").text = \
            header.get('buyer_country_code', country.get('code', 'GB'))

        # VAT
        if header.get('buyer_vat_num'):
            tax_scheme = ET.SubElement(party, f"{cac}PartyTaxScheme")
            ET.SubElement(tax_scheme, f"{cbc}CompanyID").text = header['buyer_vat_num']
            scheme_elem = ET.SubElement(tax_scheme, f"{cac}TaxScheme")
            tax_type = mapper.get('tax_system', {}).get('type', 'VAT')
            ET.SubElement(scheme_elem, f"{cbc}ID").text = tax_type

        # IT: Second PartyTaxScheme for buyer Codice Fiscale (TAX scheme)
        special_rules = mapper.get('special_rules', {})
        if special_rules.get('requires_fiscal_code') and header.get('buyer_fiscal_code'):
            tax_scheme2 = ET.SubElement(party, f"{cac}PartyTaxScheme")
            ET.SubElement(tax_scheme2, f"{cbc}CompanyID").text = header['buyer_fiscal_code']
            scheme_elem2 = ET.SubElement(tax_scheme2, f"{cac}TaxScheme")
            ET.SubElement(scheme_elem2, f"{cbc}ID").text = 'TAX'

        # Legal Entity (UBL 2.1: PartyLegalEntity MUST come before Contact)
        legal = ET.SubElement(party, f"{cac}PartyLegalEntity")
        ET.SubElement(legal, f"{cbc}RegistrationName").text = buyer_name

        # Contact
        if header.get('buyer_email'):
            contact = ET.SubElement(party, f"{cac}Contact")
            ET.SubElement(contact, f"{cbc}ElectronicMail").text = header['buyer_email']

    def _determine_scheme_id(self, identifier: str, identifiers_config: Dict) -> str:
        """Determine the appropriate scheme ID for an identifier."""

        scheme_mappings = identifiers_config.get('scheme_mappings', {})

        if not identifier or identifier == 'UNKNOWN':
            return identifiers_config.get('buyer_endpoint_scheme', '0088')

        # GLN (13 digits)
        if identifier.isdigit() and len(identifier) == 13:
            return scheme_mappings.get('gln', '0088')

        # VAT number (starts with 2-letter country code)
        if len(identifier) > 2 and identifier[:2].isalpha():
            return scheme_mappings.get('vat_number', identifiers_config.get('vat_scheme_id', '9906'))

        # DUNS or other numeric
        if identifier.isdigit():
            return scheme_mappings.get('duns', '0060')

        return identifiers_config.get('buyer_endpoint_scheme', '0088')

    def _add_tax_total(
        self,
        parent: ET.Element,
        vp_data: Dict,
        currency: str,
        mapper: Dict,
        cbc: str,
        cac: str
    ):
        """Add tax total with mapper-based tax scheme.

        UBL 2.1 requires at least one TaxTotal even for zero-tax invoices.
        When no tax breakdown exists, a zero-amount TaxTotal with the
        appropriate category (inferred from line-level VATCatCode) is emitted.
        """

        tax_breakdown = vp_data.get('tax_breakdown', [])
        header = vp_data.get('header', {})
        tax_system = mapper.get('tax_system', {})
        tax_type = tax_system.get('type', 'VAT')

        # If no tax breakdown, create a zero-tax entry using line-level VATCatCode
        if not tax_breakdown:
            # Infer category from first line's vat_category, default to 'Z' for zero-tax
            lines = vp_data.get('lines', [])
            inferred_cat = 'Z'
            for line in lines:
                cat = line.get('vat_category')
                if cat:
                    inferred_cat = cat
                    break

            tax_breakdown = [{
                'category_code': inferred_cat,
                'rate': 0.0,
                'taxable_amount': header.get('taxable_amount', 0),
                'tax_amount': 0.0,
            }]

        tax_total = ET.SubElement(parent, f"{cac}TaxTotal")

        total_tax = sum(t.get('tax_amount', 0) for t in tax_breakdown)
        ET.SubElement(tax_total, f"{cbc}TaxAmount", currencyID=currency).text = f"{total_tax:.2f}"

        # Spain VeriFactu: SpecialRegimeKey goes inside each TaxSubtotal
        country_code = mapper.get('country', {}).get('code', 'XX')
        special_rules = mapper.get('special_rules', {})
        verifactu_enabled = special_rules.get('verifactu_enabled', False)
        default_regime_key = mapper.get('defaults', {}).get('special_regime_key', '01')

        for tax in tax_breakdown:
            subtotal = ET.SubElement(tax_total, f"{cac}TaxSubtotal")

            # TaxSubtotal extensions (country-specific)
            ext_ns = "{urn:oasis:names:specification:ubl:schema:xsd:CommonExtensionComponents-2}"
            puf_ns = "{urn:pagero:ExtensionComponent:1.0}"
            needs_subtotal_ext = False
            subtotal_ext_items = {}

            # Spain VeriFactu: SpecialRegimeKey
            if country_code == 'ES' and verifactu_enabled:
                subtotal_ext_items['special_regime_key'] = tax.get('special_regime_key') or default_regime_key
                needs_subtotal_ext = True

            # Italy: TaxChargeability (I=immediate, D=deferred, S=split payment)
            if country_code == 'IT' and special_rules.get('requires_tax_chargeability'):
                default_chargeability = mapper.get('defaults', {}).get('tax_chargeability', 'I')
                subtotal_ext_items['tax_chargeability'] = tax.get('tax_chargeability') or default_chargeability
                needs_subtotal_ext = True

            if needs_subtotal_ext:
                sub_ext = ET.SubElement(subtotal, f"{ext_ns}UBLExtensions")
                sub_ubl_ext = ET.SubElement(sub_ext, f"{ext_ns}UBLExtension")
                ET.SubElement(sub_ubl_ext, f"{ext_ns}ExtensionURI").text = \
                    "urn:pagero:ExtensionComponent:1.0:PageroExtension:TaxSubtotalExtension"
                sub_content = ET.SubElement(sub_ubl_ext, f"{ext_ns}ExtensionContent")
                sub_pagero = ET.SubElement(sub_content, f"{puf_ns}PageroExtension")
                sub_tax_ext = ET.SubElement(sub_pagero, f"{puf_ns}TaxSubtotalExtension")
                if 'special_regime_key' in subtotal_ext_items:
                    ET.SubElement(sub_tax_ext, f"{puf_ns}SpecialRegimeKey").text = subtotal_ext_items['special_regime_key']
                if 'tax_chargeability' in subtotal_ext_items:
                    chargeability = ET.SubElement(sub_tax_ext, f"{puf_ns}TaxChargeability")
                    ET.SubElement(chargeability, f"{cbc}TaxTypeCode").text = subtotal_ext_items['tax_chargeability']

            taxable_amt = tax.get('taxable_amount', 0)
            tax_amt = tax.get('tax_amount', 0)
            rate = tax.get('rate', 0)

            ET.SubElement(subtotal, f"{cbc}TaxableAmount", currencyID=currency).text = f"{taxable_amt:.2f}"
            ET.SubElement(subtotal, f"{cbc}TaxAmount", currencyID=currency).text = f"{tax_amt:.2f}"

            category = ET.SubElement(subtotal, f"{cac}TaxCategory")
            cat_code = tax.get('category_code', 'S')
            ET.SubElement(category, f"{cbc}ID").text = cat_code
            ET.SubElement(category, f"{cbc}Percent").text = f"{rate:.2f}"

            # TaxExemptionReasonCode for non-standard categories (E, Z, O, AE, K, G)
            if cat_code != 'S':
                exemption_code = tax.get('exemption_reason_code')
                exemption_text = tax.get('exemption_reason_text')
                # Fallback chain for exemption codes:
                # 1. IT natura_codes (N1-N7)
                # 2. Country-specific exemption_code_defaults (EU VATEX, RS, SG)
                # 3. SA exemption_reasons (VATEX-SA-*)
                if not exemption_code:
                    natura_codes = tax_system.get('natura_codes', {})
                    if natura_codes:
                        # IT: map UBL category to default Natura code
                        natura_default = {'E': 'N4', 'Z': 'N3.1', 'AE': 'N6.9', 'O': 'N2.2'}
                        exemption_code = natura_default.get(cat_code)
                        if exemption_code:
                            exemption_text = natura_codes.get(exemption_code, '')
                if not exemption_code:
                    # Country-specific exemption code defaults (EU VATEX, RS, SG)
                    exemption_defaults = tax_system.get('exemption_code_defaults', {})
                    exemption_code = exemption_defaults.get(cat_code)
                    if exemption_code:
                        # Use tax_categories description as reason text
                        tax_cats = tax_system.get('tax_categories', {})
                        exemption_text = tax_cats.get(cat_code, '')
                if exemption_code:
                    ET.SubElement(category, f"{cbc}TaxExemptionReasonCode").text = exemption_code
                if exemption_text:
                    ET.SubElement(category, f"{cbc}TaxExemptionReason").text = exemption_text

            scheme = ET.SubElement(category, f"{cac}TaxScheme")
            # Per-entry tax scheme override (for IN CGST/SGST/IGST, TR 0015, ES IGIC/IPSI)
            entry_scheme = tax.get('tax_scheme_id') or tax_type
            ET.SubElement(scheme, f"{cbc}ID").text = entry_scheme

    def _add_tax_currency_total(
        self,
        parent: ET.Element,
        vp_data: Dict,
        tax_currency: str,
        cbc: str,
        cac: str
    ):
        """Add second TaxTotal in tax currency for dual-currency invoices (e.g. SA).

        PUF/UBL requires a second TaxTotal with only the aggregate TaxAmount
        in the tax reporting currency when TaxCurrencyCode != DocumentCurrencyCode.
        Uses taxable_amount_sar/tax_amount_sar fields from VP tax breakdown.
        """
        tax_breakdown = vp_data.get('tax_breakdown', [])
        total_tax_sar = sum(t.get('tax_amount_sar', 0) for t in tax_breakdown)

        # Only emit if there are actual SAR/tax-currency amounts
        if total_tax_sar or any(t.get('tax_amount_sar') for t in tax_breakdown):
            tax_total = ET.SubElement(parent, f"{cac}TaxTotal")
            ET.SubElement(tax_total, f"{cbc}TaxAmount", currencyID=tax_currency).text = f"{total_tax_sar:.2f}"

    # Map VP PaymentMode values to UBL PaymentMeansCode (UNCL 4461)
    # VP hardcodes 'Item1' for credit memos → '97' (clearing between partners)
    PAYMENT_MODE_MAP = {
        'ITEM1': '97',  # VP credit memo payment mode
        'CASH': '10',
        'CHECK': '20', 'CHEQUE': '20',
        'CREDIT_TRANSFER': '30', 'BANK_TRANSFER': '30', 'WIRE': '30',
        'DEBIT_TRANSFER': '31',
        'CREDIT_CARD': '48',
        'DIRECT_DEBIT': '49',
        'STANDING_AGREEMENT': '57',
        'SEPA_CREDIT_TRANSFER': '58',
        'SEPA_DIRECT_DEBIT': '59',
        'ONLINE_PAYMENT': '68',
    }

    def _add_payment_means(
        self,
        parent: ET.Element,
        vp_data: Dict,
        header: Dict,
        supplier_config: Dict,
        cbc: str,
        cac: str
    ):
        """Add payment means with PayeeFinancialAccount (IBAN/BIC) for SEPA countries."""

        payment_modes = vp_data.get('payment_modes', [])

        if payment_modes:
            for pm in payment_modes:
                pay_means = ET.SubElement(parent, f"{cac}PaymentMeans")
                # Map VP PaymentMode to UBL code
                vp_mode = (pm.get('mode') or '').upper().replace(' ', '_')
                means_code = self.PAYMENT_MODE_MAP.get(vp_mode, '30')
                ET.SubElement(pay_means, f"{cbc}PaymentMeansCode").text = means_code
                # Add description as note if available
                if pm.get('description'):
                    ET.SubElement(pay_means, f"{cbc}PaymentID").text = pm['description']
                # PayeeFinancialAccount (IBAN/BIC) from payment mode data or supplier config
                iban = pm.get('iban') or supplier_config.get('iban')
                if iban:
                    account = ET.SubElement(pay_means, f"{cac}PayeeFinancialAccount")
                    ET.SubElement(account, f"{cbc}ID").text = iban
                    acct_name = pm.get('account_name') or supplier_config.get('account_name')
                    if acct_name:
                        ET.SubElement(account, f"{cbc}Name").text = acct_name
                    bic = pm.get('bic') or supplier_config.get('bic')
                    if bic:
                        branch = ET.SubElement(account, f"{cac}FinancialInstitutionBranch")
                        ET.SubElement(branch, f"{cbc}ID").text = bic
        else:
            # Fallback: emit default PaymentMeans from supplier_config (code 30 = credit transfer)
            # All official Pagero examples include at least one PaymentMeans
            iban = supplier_config.get('iban')
            if iban:
                pay_means = ET.SubElement(parent, f"{cac}PaymentMeans")
                ET.SubElement(pay_means, f"{cbc}PaymentMeansCode").text = '30'
                account = ET.SubElement(pay_means, f"{cac}PayeeFinancialAccount")
                ET.SubElement(account, f"{cbc}ID").text = iban
                acct_name = supplier_config.get('account_name')
                if acct_name:
                    ET.SubElement(account, f"{cbc}Name").text = acct_name
                bic = supplier_config.get('bic')
                if bic:
                    branch = ET.SubElement(account, f"{cac}FinancialInstitutionBranch")
                    ET.SubElement(branch, f"{cbc}ID").text = bic

        # PaymentTerms - always emit if available (PUF allows alongside PaymentMeans)
        if header.get('payment_terms'):
            pay_terms = ET.SubElement(parent, f"{cac}PaymentTerms")
            ET.SubElement(pay_terms, f"{cbc}Note").text = header['payment_terms']

    def _add_monetary_total(
        self,
        parent: ET.Element,
        header: Dict,
        cbc: str,
        cac: str,
        prepaid_amount: float = 0.0,
        doc_currency: str = None
    ):
        """Add legal monetary total."""

        total = ET.SubElement(parent, f"{cac}LegalMonetaryTotal")
        currency = doc_currency or header.get('currency_code', 'GBP')

        taxable_amt = header.get('taxable_amount', 0)
        amt_without_vat = header.get('amount_without_vat', 0)
        amt_with_vat = header.get('amount_with_vat', 0)
        amt_due = header.get('amount_due', 0)

        ET.SubElement(total, f"{cbc}LineExtensionAmount", currencyID=currency).text = f"{taxable_amt:.2f}"
        ET.SubElement(total, f"{cbc}TaxExclusiveAmount", currencyID=currency).text = f"{amt_without_vat:.2f}"
        ET.SubElement(total, f"{cbc}TaxInclusiveAmount", currencyID=currency).text = f"{amt_with_vat:.2f}"
        if prepaid_amount:
            ET.SubElement(total, f"{cbc}PrepaidAmount", currencyID=currency).text = f"{prepaid_amount:.2f}"
        ET.SubElement(total, f"{cbc}PayableAmount", currencyID=currency).text = f"{amt_due:.2f}"

    def _add_invoice_line(
        self,
        parent: ET.Element,
        line_data: Dict,
        mapper: Dict,
        cbc: str,
        cac: str,
        is_credit_note: bool = False,
        doc_currency: str = None
    ):
        """Add invoice/credit note line with mapper-based configuration."""

        tax_system = mapper.get('tax_system', {})
        tax_type = tax_system.get('type', 'VAT')
        defaults = mapper.get('defaults', {})

        line_tag = "CreditNoteLine" if is_credit_note else "InvoiceLine"
        line = ET.SubElement(parent, f"{cac}{line_tag}")

        # Line ID
        ET.SubElement(line, f"{cbc}ID").text = str(line_data.get('line_id', '1'))

        # Quantity
        quantity = line_data.get('quantity', 1.0)
        unit_code = defaults.get('unit_code', 'EA')
        qty_tag = "CreditedQuantity" if is_credit_note else "InvoicedQuantity"
        ET.SubElement(line, f"{cbc}{qty_tag}", unitCode=unit_code).text = f"{quantity:.2f}"

        # Amount - use line currency, fallback to document currency (not hardcoded GBP)
        fallback_currency = doc_currency or mapper.get('country', {}).get('currency', 'GBP')
        currency = line_data.get('currency') or fallback_currency
        taxable_value = line_data.get('taxable_value', 0)
        ET.SubElement(line, f"{cbc}LineExtensionAmount", currencyID=currency).text = f"{taxable_value:.2f}"

        # SA: TaxInclusiveLineExtensionAmount extension (line amount including tax)
        country_code_line = mapper.get('country', {}).get('code', 'XX')
        if country_code_line == 'SA':
            line_tax_inclusive = taxable_value + line_data.get('vat_amount', 0)
            ext_ns = "{urn:oasis:names:specification:ubl:schema:xsd:CommonExtensionComponents-2}"
            puf_ns = "{urn:pagero:ExtensionComponent:1.0}"
            line_ext_container = ET.SubElement(line, f"{ext_ns}UBLExtensions")
            line_ubl_ext = ET.SubElement(line_ext_container, f"{ext_ns}UBLExtension")
            ET.SubElement(line_ubl_ext, f"{ext_ns}ExtensionURI").text = \
                "urn:pagero:ExtensionComponent:1.0:PageroExtension:TaxInclusiveLineExtensionAmount"
            line_ext_content = ET.SubElement(line_ubl_ext, f"{ext_ns}ExtensionContent")
            line_pagero = ET.SubElement(line_ext_content, f"{puf_ns}PageroExtension")
            ET.SubElement(line_pagero, f"{puf_ns}TaxInclusiveLineExtensionAmount",
                         currencyID=currency).text = f"{line_tax_inclusive:.2f}"

        # Allowance/Charge (if gross != net, there's a discount)
        gross_price = line_data.get('gross_price', 0)
        net_price = line_data.get('net_price', 0)
        if gross_price and net_price and gross_price != net_price:
            allowance = ET.SubElement(line, f"{cac}AllowanceCharge")
            ET.SubElement(allowance, f"{cbc}ChargeIndicator").text = "false"
            discount = gross_price - net_price
            ET.SubElement(allowance, f"{cbc}Amount", currencyID=currency).text = f"{discount:.2f}"

        # Line-level TaxTotal (required by IT, SA official examples)
        country_code = mapper.get('country', {}).get('code', 'XX')
        special_rules = mapper.get('special_rules', {})
        vat_amount = line_data.get('vat_amount', 0)
        vat_rate = line_data.get('vat_rate', 0)
        if country_code in ('IT', 'SA') and vat_amount is not None:
            line_tax_total = ET.SubElement(line, f"{cac}TaxTotal")
            ET.SubElement(line_tax_total, f"{cbc}TaxAmount", currencyID=currency).text = f"{vat_amount:.2f}"
            line_subtotal = ET.SubElement(line_tax_total, f"{cac}TaxSubtotal")
            ET.SubElement(line_subtotal, f"{cbc}TaxableAmount", currencyID=currency).text = f"{taxable_value:.2f}"
            ET.SubElement(line_subtotal, f"{cbc}TaxAmount", currencyID=currency).text = f"{vat_amount:.2f}"
            line_cat = ET.SubElement(line_subtotal, f"{cac}TaxCategory")
            ET.SubElement(line_cat, f"{cbc}ID").text = line_data.get('vat_category', 'S')
            ET.SubElement(line_cat, f"{cbc}Percent").text = f"{vat_rate:.2f}"
            line_scheme = ET.SubElement(line_cat, f"{cac}TaxScheme")
            ET.SubElement(line_scheme, f"{cbc}ID").text = tax_type

        # Item
        item = ET.SubElement(line, f"{cac}Item")
        if line_data.get('item_name'):
            ET.SubElement(item, f"{cbc}Name").text = line_data['item_name']

        # HSN/Item Classification Code (required for IN, optional for others)
        hsn_code = line_data.get('hsn_code') or line_data.get('item_classification_code')
        if hsn_code:
            classification = ET.SubElement(item, f"{cac}CommodityClassification")
            cls_code = ET.SubElement(classification, f"{cbc}ItemClassificationCode")
            cls_code.text = hsn_code
            # IN uses HSN, others may use different lists
            if country_code in ('IN',):
                cls_code.set('listID', 'HSN')

        # Tax category
        category = ET.SubElement(item, f"{cac}ClassifiedTaxCategory")
        ET.SubElement(category, f"{cbc}ID").text = line_data.get('vat_category', 'S')

        ET.SubElement(category, f"{cbc}Percent").text = f"{vat_rate:.2f}"

        # VAT exemption reason (required when category is E, Z, O, AE, K, G)
        if line_data.get('vat_exemption_reason_code'):
            ET.SubElement(category, f"{cbc}TaxExemptionReasonCode").text = line_data['vat_exemption_reason_code']
        if line_data.get('vat_exemption_reason_text'):
            ET.SubElement(category, f"{cbc}TaxExemptionReason").text = line_data['vat_exemption_reason_text']

        scheme = ET.SubElement(category, f"{cac}TaxScheme")
        ET.SubElement(scheme, f"{cbc}ID").text = tax_type

        # Price
        price = ET.SubElement(line, f"{cac}Price")
        ET.SubElement(price, f"{cbc}PriceAmount", currencyID=currency).text = f"{net_price:.2f}"
        base_qty = line_data.get('price_base_quantity', 1.0)
        ET.SubElement(price, f"{cbc}BaseQuantity", unitCode=unit_code).text = f"{base_qty:.2f}"

    # Helper methods
    def _parse_section(self, element: ET.Element, field_map: Dict, decimal_fields: set) -> Dict:
        """Parse an XML element into a dict using field mappings from the mapper config.

        Args:
            element: XML element to parse
            field_map: Dict mapping internal key -> VP XML tag name
            decimal_fields: Set of keys that should be parsed as float
        """
        result = {}
        for key, xml_tag in field_map.items():
            if key.startswith('_'):
                continue
            if key in decimal_fields:
                result[key] = self._get_decimal(element, xml_tag)
            else:
                result[key] = self._get_text(element, xml_tag)
        return result

    def _get_text(self, parent: ET.Element, tag: str) -> Optional[str]:
        """Safely get text from element."""
        elem = parent.find(tag)
        return elem.text.strip() if elem is not None and elem.text else None

    def _get_decimal(self, parent: ET.Element, tag: str, default: float = 0.0) -> float:
        """Safely get decimal from element."""
        text = self._get_text(parent, tag)
        if text:
            try:
                return float(text)
            except ValueError:
                return default
        return default

    def _parse_date(self, date_str: str) -> str:
        """Parse date to YYYY-MM-DD format."""
        if not date_str:
            return datetime.now().strftime("%Y-%m-%d")
        try:
            dt = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
            return dt.strftime("%Y-%m-%d")
        except (ValueError, TypeError):
            try:
                return date_str[:10]
            except (TypeError, IndexError):
                return datetime.now().strftime("%Y-%m-%d")

    def _calculate_vat_rate(self, vat_amount: float, taxable_amount: float) -> float:
        """Calculate VAT rate from amounts (fallback when Table2 is missing)."""
        if taxable_amount and taxable_amount != 0:
            return round((vat_amount / taxable_amount) * 100, 2)
        return 0.0


