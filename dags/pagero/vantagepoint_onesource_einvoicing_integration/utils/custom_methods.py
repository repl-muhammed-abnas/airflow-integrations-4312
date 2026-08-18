"""
Utility functions for Vantagepoint OneSource eInvoicing Integration
"""

from lxml import etree as ET
from typing import Union, Dict
from airflow.hooks.base import BaseHook
from airflow.models import Variable
import base64
import json
import requests
import time
from io import BytesIO

try:
    from .puf_converter import PUFConverter
except ImportError:
    from pagero.vantagepoint_onesource_einvoicing_integration.utils.puf_converter import PUFConverter


# =============================================================================
# OneSource Authentication
# =============================================================================

def extract_and_save_token(**context):
    """Extract token from authentication response and save to Airflow Variable"""
    ti = context['ti']
    auth_response = ti.xcom_pull(task_ids='onesource_authentication')

    if not auth_response:
        raise ValueError("No authentication response available")

    if isinstance(auth_response, str):
        auth_data = json.loads(auth_response)
    else:
        auth_data = auth_response

    access_token = auth_data.get('access_token')
    if not access_token:
        raise ValueError("No access token found in authentication response")

    Variable.set('onesource_access_token', access_token)
    return access_token


# =============================================================================
# OneSource Document Operations
# =============================================================================

def download_pdf_from_onesource(http_conn_id: str):
    """
    Factory function that returns a callable for downloading PDF from OneSource.

    Args:
        http_conn_id: Airflow connection ID for OneSource API
    """
    def _download(**context):
        ti = context['ti']
        dag_run = context['dag_run']

        document_id = dag_run.conf['item']['id']
        auth_token = ti.xcom_pull(task_ids='extract_token')

        if not document_id or not auth_token:
            raise ValueError("Missing document ID or authentication token")

        conn = BaseHook.get_connection(http_conn_id)
        base_url = conn.host.rstrip('/')

        url = f'{base_url}/einvoicing/document/v1/documents/{document_id}/presentation'
        headers = {
            'Accept': 'application/octet-stream',
            'Authorization': f'Bearer {auth_token}'
        }

        response = requests.get(url, headers=headers, stream=True)
        response.raise_for_status()

        pdf_bytes = response.content
        if not pdf_bytes:
            raise ValueError("PDF download resulted in empty content")

        return base64.b64encode(pdf_bytes).decode('utf-8')

    return _download


# =============================================================================
# Vantagepoint Operations
# =============================================================================

def upload_pdf_to_vantagepoint(vp_conn_id: str):
    """
    Factory function that returns a callable for uploading PDF to Vantagepoint.

    Args:
        vp_conn_id: Airflow connection ID for Vantagepoint API
    """
    def _upload(**context):
        ti = context['ti']
        dag_run = context['dag_run']

        pdf_base64 = ti.xcom_pull(task_ids='get_document_presentation')
        if not pdf_base64:
            raise ValueError("No PDF data available")

        pdf_bytes = base64.b64decode(pdf_base64)
        if pdf_bytes[:4] != b'%PDF':
            raise ValueError("PDF data is corrupted")

        # Build filename from invoice number
        invoice_number = dag_run.conf['item'].get('documentInfo', {}).get(
            'documentIdentifier', dag_run.conf['item']['id']
        )
        filename = f"Invoice_OneSource_{invoice_number}.pdf"

        # Get Vantagepoint connection details
        conn = BaseHook.get_connection(vp_conn_id)
        extra = json.loads(conn.extra) if conn.extra else {}

        if conn.host and conn.host.startswith(('http://', 'https://')):
            base_url = conn.host.rstrip('/')
        elif conn.schema and conn.host:
            base_url = f"{conn.schema}://{conn.host}".rstrip('/')
        elif conn.host:
            base_url = f"https://{conn.host}".rstrip('/')
        else:
            base_url = extra.get('host_name') or extra.get('base_url')
            if not base_url:
                raise ValueError(f"Cannot determine base URL from connection {vp_conn_id}")

        access_token = extra.get('access_token')
        if not access_token:
            raise ValueError("No access_token found in connection extra")

        response = requests.post(
            url=f"{base_url}/api/project/fw_files",
            files={'File': (filename, BytesIO(pdf_bytes), 'application/pdf')},
            headers={'Authorization': f'Bearer {access_token}', 'Accept': 'application/json'}
        )
        response.raise_for_status()

        # Parse response to get file ID
        response_data = response.json() if response.text else {}
        if isinstance(response_data, list) and response_data:
            file_info = response_data[0]
        else:
            file_info = response_data

        file_id = file_info.get('fileID') or file_info.get('FileID') or file_info.get('id')
        if not file_id:
            raise ValueError(f"No file ID in upload response: {response_data}")

        return {'fileID': file_id, 'fileName': filename}

    return _upload


def prepare_update_data(**context):
    """Prepare data for project and eInvoice log updates"""
    ti = context['ti']
    dag_run = context['dag_run']

    # Get eInvoice log record
    einvoice_result = ti.xcom_pull(task_ids='get_einvoice_log')
    if isinstance(einvoice_result, list):
        if not einvoice_result:
            invoice_id = dag_run.conf['item'].get('documentInfo', {}).get('documentIdentifier', 'unknown')
            raise ValueError(f"No eInvoice log record found for invoice: {invoice_id}")
        einvoice_record = einvoice_result[0]
    else:
        einvoice_record = einvoice_result

    cust_project = einvoice_record.get('CustProject')
    udic_uid = einvoice_record.get('UDIC_UID')

    if not cust_project:
        raise ValueError("CustProject is missing in eInvoice record")
    if not udic_uid:
        raise ValueError("UDIC_UID is missing in eInvoice record")

    # Get uploaded file info
    file_info = ti.xcom_pull(task_ids='upload_pdf')

    return {
        'CustProject': cust_project,
        'UDIC_UID': udic_uid,
        'fileID': file_info['fileID'],
        'fileName': file_info['fileName']
    }


# =============================================================================
# OneSource Company Mapping
# =============================================================================

def parse_onesource_company_to_supplier_config(onesource_response, company_id=None, company_name=None):
    """
    Convert OneSource company API response to supplier_config format.

    Extracts seller identity data (name, VAT, address) from OneSource.
    Scheme IDs are NOT set here — they are applied by the PUF converter
    using the single country mapper detected from VP buyer data.

    Args:
        onesource_response: Response from get_onesource_company_details task
        company_id: Optional company ID to filter
        company_name: Optional company name to filter (used if company_id is not provided)

    Returns:
        Dictionary in supplier_config format for convert_vantagepoint_to_puf
    """
    if isinstance(onesource_response, str):
        companies = json.loads(onesource_response)
    else:
        companies = onesource_response

    if not companies or not isinstance(companies, list) or len(companies) == 0:
        raise ValueError(
            "No companies returned from OneSource company API. "
            "Check onesource_company_id/onesource_company_name configuration and API connectivity."
        )

    company = None
    if company_id:
        company = next((c for c in companies if c.get('id') == company_id), None)
        if not company:
            available_ids = [c.get('id') for c in companies]
            raise ValueError(
                f"Company ID '{company_id}' not found in OneSource response. "
                f"Available company IDs: {available_ids}"
            )
    elif company_name:
        # Match by name (case-insensitive)
        company = next(
            (c for c in companies if c.get('name', '').lower() == company_name.lower()),
            None
        )
        if not company:
            available_names = [c.get('name') for c in companies]
            raise ValueError(
                f"Company name '{company_name}' not found in OneSource response. "
                f"Available companies: {available_names}"
            )
    else:
        company = companies[0]

    identifiers = company.get('identifiers', [])
    vat_id = None
    company_id_val = company.get('id')

    for identifier in identifiers:
        if 'VAT' in identifier.get('idType', '').upper():
            vat_id = identifier.get('value')
            break

    # Validate required fields
    if not vat_id and not company_id_val:
        raise ValueError(
            f"OneSource company has no VAT number and no company ID. "
            f"Company name: '{company.get('name')}'. "
            f"Cannot build supplier party without at least one identifier."
        )

    company_name = company.get('name')
    if not company_name:
        raise ValueError(
            f"OneSource company ID '{company_id_val}' has no name. "
            f"Supplier name is required for PUF AccountingSupplierParty."
        )

    address = company.get('address', {})
    contact = company.get('contact', {})
    country_code = address.get('countryCode', '')

    building = address.get('buildingNumber', '')

    return {
        'endpoint_id': vat_id or company_id_val,
        'party_id': company_id_val or vat_id,
        'name': company_name,
        'vat_id': vat_id or '',
        'registration_name': company_name,
        'company_id': company_id_val or '',
        'street': address.get('street', ''),
        'building_num': building,
        'building_number': building,  # SA extensions expect this key
        'city': address.get('city', ''),
        'postal_code': address.get('postalCode', ''),
        'district': address.get('district', ''),
        'country_code': country_code,
        'contact_name': contact.get('name', 'Accounts Department'),
        'contact_phone': contact.get('phone', ''),
        'contact_email': contact.get('email', '')
    }


def prepare_multipart_data(onesource_company_id=None, **context):
    """Prepare multipart body with PUF XML for SimpleHttpOperator"""
    ti = context['ti']

    puf_xml = ti.xcom_pull(task_ids='convert_vantagepoint_to_puf')
    token = ti.xcom_pull(task_ids='extract_token')
    onesource_companies = ti.xcom_pull(task_ids='get_onesource_company_details')

    company_id = None
    if onesource_companies:
        companies = json.loads(onesource_companies) if isinstance(onesource_companies, str) else onesource_companies
        if companies and len(companies) > 0:
            if onesource_company_id:
                company_id = next(
                    (c.get('id') for c in companies if c.get('id') == onesource_company_id),
                    companies[0].get('id')
                )
            else:
                company_id = companies[0].get('id')

    if not company_id:
        raise ValueError("No Company ID found")

    if not puf_xml:
        raise ValueError("No PUF XML data available")
    if not token:
        raise ValueError("No token available from extract_token task")

    # Extract invoice details from PUF XML
    invoice_id = 'UNKNOWN'
    document_type = 'Invoice'
    try:
        puf_root = ET.fromstring(puf_xml.encode('utf-8') if isinstance(puf_xml, str) else puf_xml)
        id_elem = puf_root.find('.//{urn:oasis:names:specification:ubl:schema:xsd:CommonBasicComponents-2}ID')
        if id_elem is not None and id_elem.text:
            invoice_id = id_elem.text

        # Detect credit note from root element tag (CreditNote vs Invoice)
        root_local = ET.QName(puf_root.tag).localname
        if root_local == 'CreditNote':
            document_type = 'CreditNote'
    except Exception:
        pass

    boundary = "----WebKitFormBoundary7MA4YWxkTrZu0gW"
    timestamp = str(int(time.time() * 1000))

    form_fields = {
        'documentType': document_type,
        'senderReference': f"{invoice_id}_{timestamp}",
        'sendingCompanyId': company_id,
        'systemName': 'Vantagepoint',
        'systemType': 'Other',
        'documentIdentifier': invoice_id
    }

    parts = []
    for name, value in form_fields.items():
        parts.append(f'--{boundary}')
        parts.append(f'Content-Disposition: form-data; name="{name}"')
        parts.append('')
        parts.append(value)

    parts.append(f'--{boundary}')
    parts.append('Content-Disposition: form-data; name="payload"; filename="invoice_puf.xml"')
    parts.append('Content-Type: text/xml')
    parts.append('')
    parts.append(puf_xml)
    parts.append(f'--{boundary}--')

    return {
        'body': '\r\n'.join(parts),
        'boundary': boundary,
        'token': token
    }


# =============================================================================
# Supplier Config Enrichment
# =============================================================================

def enrich_supplier_config(
    supplier_config: Dict,
    overrides: Dict = None,
    country_code: str = None,
) -> Dict:
    """
    Enrich supplier_config with country-specific fields and instance overrides.

    This bridges the gap between the basic supplier_config from OneSource
    (name, VAT, address) and the country-specific fields required by PUF
    extensions (IT registration data, SA UUID, FR SIRET, etc.).

    Enrichment order (later wins):
    1. Base supplier_config from OneSource
    2. Instance-level overrides (from config.supplier_config_overrides)
    3. Auto-generated fields (e.g. SA UUID if missing)

    Args:
        supplier_config: Base config from parse_onesource_company_to_supplier_config()
        overrides: Optional dict of country-specific fields from instance config.
                   Examples:
                     IT: {"ufficio": "MI", "numero_rea": "MI-1234567",
                          "capitale_sociale": "100000.00", "socio_unico": "SM",
                          "stato_liquidazione": "LN"}
                     SA: {"building_number": "1234", "crn": "1234567890"}
                     FR: {"capital_social": "50000", "rcs_number": "RCS Paris B 123456789",
                          "ape_code": "6201Z"}
                     IN: {"supply_type": "B2B", "pos_code": "07", "reverse_charge": "N"}
                     TR: {"scenario": "TEMELFATURA", "tax_office": "Istanbul VD"}
        country_code: ISO country code (used for auto-generation rules)

    Returns:
        Enriched supplier_config dict (mutated in place and returned)
    """
    if supplier_config is None:
        supplier_config = {}

    # Merge instance overrides
    if overrides:
        for key, value in overrides.items():
            if value is not None and value != '':
                supplier_config[key] = value

    # Auto-generate SA UUID if country is SA and no UUID provided
    country = (country_code or supplier_config.get('country_code', '')).upper()
    if country == 'SA' and not supplier_config.get('uuid'):
        import uuid
        supplier_config['uuid'] = str(uuid.uuid4())

    return supplier_config


# =============================================================================
# Vantagepoint to PUF Conversion
# =============================================================================

def convert_vantagepoint_to_puf(
    input_data: Union[str, bytes],
    supplier_config: Dict = None,
    country_code: str = None,
    is_file_path: bool = True,
    use_buyer_as_supplier: bool = False
) -> str:
    """
    Convert Vantagepoint invoice XML to Pagero Universal Format (PUF).

    Uses the mapper-based PUFConverter for country-specific scheme IDs,
    extensions, and validation rules.

    Country detection priority:
    1. Explicit country_code parameter
    2. supplier_config['country_code'] (from OneSource company)
    3. Auto-detect from VP data (buyer country, then currency)

    Args:
        input_data: Either file path (str) or XML string/bytes
        supplier_config: Dictionary with supplier details (optional, includes country_code from OneSource)
        country_code: Optional explicit country code override (ISO 3166-1 alpha-2)
        is_file_path: True if input_data is a file path, False if XML content
        use_buyer_as_supplier: If True and no supplier_config, use buyer data as supplier

    Returns:
        PUF XML as string with correct UBL 2.1 element ordering
    """
    converter = PUFConverter()
    return converter.convert(
        input_data=input_data,
        supplier_config=supplier_config,
        country_code=country_code,
        is_file_path=is_file_path,
        use_buyer_as_supplier=use_buyer_as_supplier
    )
