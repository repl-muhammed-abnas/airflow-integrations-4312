import base64
import io
import xml.etree.ElementTree as ET
import zipfile
from datetime import datetime
from procore_ce_integration.initial_setup_sync.shared_utils import normalize_ce_identifier
# pylint: disable=too-many-branches, too-many-statements, unused-variable


def generate_ce_invoice_xml(invoice_data):
    """
    Generate ComputerEase AP Invoice XML from Procore invoice data.

    Args:
        invoice_data: Dictionary containing processed invoice data

    Returns:
        XML string for a single invoice element
    """
    # Extract invoice data (handle both direct data and nested 'data' key)
    if 'data' in invoice_data:
        data = invoice_data['data']
    else:
        data = invoice_data

    # Create invoice element (not wrapped in root yet)
    invoice = ET.Element('invoice')

    # Extract vendor code and add to xml (already provided in flat structure)
    vendor_code = str(data.get('vendor_code', ''))
    ET.SubElement(invoice, 'vennum').text = vendor_code or '0'

    # If vendor code is 0 or empty, add vendor name
    if not vendor_code or vendor_code == '0':
        vendor_name = data.get('vendor_name', '')
        if vendor_name:
            ET.SubElement(invoice, 'name').text = vendor_name

    # Add Invoice date (xs:date format YYYY-MM-DD)
    invoice_date = data.get('invoice_date', '')
    if invoice_date:
        ET.SubElement(invoice, 'invdate').text = format_date_for_ce(
            invoice_date)

    # Add Due date
    payment_due_date = data.get('payment_due_date')
    if payment_due_date:
        ET.SubElement(invoice, 'duedate').text = format_date_for_ce(
            payment_due_date)

    # Add PO number (commitment code)
    po_num = str(data.get('po_number', ''))
    if po_num:
        ET.SubElement(invoice, 'ponum').text = po_num

    # Add Invoice number
    invnum = str(data.get('invoice_number', ''))
    if invnum:
        ET.SubElement(invoice, 'invnum').text = invnum

    # Add Description
    description = data.get('description', '')
    if description:
        ET.SubElement(invoice, 'desc').text = description

    # Add Total amount (required field)
    total_amount = float(data.get('amount', 0))
    ET.SubElement(invoice, 'amount').text = str(total_amount)

    # Add retention amount if present
    retention_amount = float(data.get('retention_amount', 0))
    if retention_amount:
        ET.SubElement(invoice, 'retamt').text = str(retention_amount)

    # Check if it's a credit (assumed true if amount < 0 per XSD documentation)
    # if total_amount < 0:
    #     ET.SubElement(invoice, 'iscredit').text = 'true'

    distributions = ET.SubElement(invoice, 'distributions')
    line_items = data.get('line_items', [])
    job_code = str(data.get('job_code', ''))

    if line_items:
        for item in line_items:
            dist_elem = ET.SubElement(distributions, 'distribution')

            # Distribution amount (required)
            item_amount = float(item.get('amount', 0))
            ET.SubElement(dist_elem, 'amount').text = str(item_amount)

            # Add job number
            if job_code:
                ET.SubElement(dist_elem, 'jobnum').text = job_code

            # Add phase, category and cost type
            phase_code = str(item.get('phase_code', ''))
            if phase_code:
                ET.SubElement(dist_elem, 'phasenum').text = phase_code

            category_code = str(item.get('category_code', ''))
            if category_code:
                ET.SubElement(dist_elem, 'catnum').text = category_code

            cost_type = str(item.get('cost_type', ''))
            if cost_type:
                ET.SubElement(dist_elem, 'ctcode').text = cost_type

            # Add description from line item
            item_desc = item.get('description', '')
            if item_desc:
                ET.SubElement(dist_elem, 'desc').text = item_desc

            if data.get('is_subcontract'):
                subitemnum = str(item.get('subitemnum', ''))
                if subitemnum:
                    ET.SubElement(dist_elem, 'subitemnum').text = subitemnum

                subrfcnum = str(item.get('subrfcnum', ''))
                if subrfcnum:
                    ET.SubElement(dist_elem, 'subrfcnum').text = subrfcnum

                subbillqty = item.get('subbillqty')
                if subbillqty is not None:
                    ET.SubElement(dist_elem, 'subbillqty').text = str(subbillqty)
    else:
        # If no line items, create single distribution with total amount
        dist_elem = ET.SubElement(distributions, 'distribution')
        ET.SubElement(dist_elem, 'amount').text = str(total_amount)
        if job_code:
            ET.SubElement(dist_elem, 'jobnum').text = job_code
        if description:
            ET.SubElement(dist_elem, 'desc').text = description

    # Convert to XML string (just the invoice element, no declaration yet)
    return ET.tostring(invoice, encoding='unicode', method='xml')


def format_date_for_ce(date_str):
    if not date_str:
        return ''

    # Convert to string if not already
    date_str = str(date_str)

    # Try parsing different formats and convert to YYYY-MM-DD
    formats = [
        '%Y-%m-%d',
        '%Y-%m-%dT%H:%M:%S',
        '%Y-%m-%dT%H:%M:%SZ',
        '%Y-%m-%dT%H:%M:%S.%fZ'
    ]

    for fmt in formats:
        try:
            dt = datetime.strptime(date_str, fmt)
            return dt.strftime('%Y-%m-%d')
        except ValueError:
            continue

    # Return empty string if no format matches
    return ''


def combine_invoice_xmls(invoice_xmls):
    """
    Combine multiple invoice XML strings into a single XML document.

    Args:
        invoice_xmls: List of XML strings for individual invoices

    Returns:
        Combined XML string with proper import root element per XSD
    """
    if not invoice_xmls:
        return None

    # Create import root element with required type attribute
    root = ET.Element('import')
    root.set('type', 'apinvoices')

    # Parse and append each invoice
    for xml_str in invoice_xmls:
        if xml_str:
            try:
                # Parse the invoice element
                invoice_elem = ET.fromstring(xml_str)
                # Append the invoice element to the import root
                if invoice_elem.tag == 'invoice':
                    root.append(invoice_elem)
            except ET.ParseError as e:
                # Skip malformed XML
                continue

    xml_str = ET.tostring(root, encoding='unicode', method='xml')
    final_xml = '<?xml version="1.0" encoding="UTF-8"?>\n' + xml_str

    return final_xml


def zip_and_base64_encode_xml(xml_str):
    if not xml_str:
        return None
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
        zip_file.writestr('invoices.xml', xml_str)
    return base64.b64encode(zip_buffer.getvalue()).decode('utf-8')


def generate_xml_payload(valid_invoices, initial_errors):
    errors = list(initial_errors)
    xml_invoices = []
    for invoice_data in valid_invoices:
        invoice_id = invoice_data.get('invoice_id')
        try:
            xml_invoices.append(generate_ce_invoice_xml(invoice_data.get('data', invoice_data)))
        except Exception as e:
            errors.append({
                'invoice_id': invoice_id,
                'invoice_number': invoice_data.get('data', {}).get('invoice_number', '') if invoice_data else '',
                'error_message': str(e),
                'error_type': 'XML Generation',
            })
    final_xml = combine_invoice_xmls(xml_invoices) if xml_invoices else None
    return {
        'xml': final_xml,
        'import_data': zip_and_base64_encode_xml(final_xml),
        'errors': errors,
    }


def clean_contract_name(contract_name):
    if not contract_name:
        return ''
    return contract_name.replace('Purchase Order', 'PO').replace('Contract', 'SC')


def validate_field_lengths(invoice_data, ce_field_validations):
    warnings = []
    invoice_fields = [(k, v) for k, v in ce_field_validations.items() if v['field_type'] == 'invoice']
    line_item_fields = [(k, v) for k, v in ce_field_validations.items() if v['field_type'] == 'line_item']

    for field_key, validation_config in invoice_fields:
        value = invoice_data.get(field_key)
        if value and len(str(value)) > validation_config['char_limit']:
            if validation_config['truncate']:
                invoice_data[field_key] = str(value)[:validation_config['char_limit']]
            else:
                warnings.append(
                    f"{validation_config['display_name']} exceeds CE limit "
                    f"({len(str(value))} > {validation_config['char_limit']} chars)")

    for idx, item in enumerate(invoice_data.get('line_items', [])):
        for field_key, validation_config in line_item_fields:
            item_field_key = 'description' if field_key == 'item_description' else field_key
            value = item.get(item_field_key)
            if value and len(str(value)) > validation_config['char_limit']:
                if validation_config['truncate']:
                    invoice_data['line_items'][idx][item_field_key] = str(value)[:validation_config['char_limit']]
                else:
                    warnings.append(
                        f"Line Item {idx+1} {validation_config['display_name']} exceeds CE limit "
                        f"({len(str(value))} > {validation_config['char_limit']} chars)")
    return warnings


def get_ce_sequence_id(ce_items, phase_code, category_code, cost_type):
    if not ce_items:
        return None
    phase_code = normalize_ce_identifier(phase_code)
    category_code = normalize_ce_identifier(category_code)
    for ce_item in ce_items:
        if (ce_item.get('phase_code') == phase_code
                and ce_item.get('category_code') == category_code
                and ce_item.get('costtype') == cost_type):
            return ce_item.get('sequence_id')
    return None