import xml.etree.ElementTree as ET
from datetime import datetime


def generate_ce_ar_invoice_xml(invoice_data):  # pylint: disable=too-many-branches
    # Extract invoice data
    if 'data' in invoice_data:
        data = invoice_data['data']
    else:
        data = invoice_data

    # Create invoice element
    invoice = ET.Element('invoice')

    # Customer number (client code)
    client_code = str(data.get('client_code', ''))
    if client_code:
        ET.SubElement(invoice, 'cusnum').text = client_code

    # Invoice number
    invoice_number = str(data.get('invoice_number', ''))
    if invoice_number:
        ET.SubElement(invoice, 'invnum').text = invoice_number

    # Job number
    job_code = str(data.get('job_code', ''))
    if job_code:
        ET.SubElement(invoice, 'jobnum').text = job_code

    # Invoice date - use billing_date
    billing_date = data.get('billing_date')
    if billing_date:
        ET.SubElement(invoice, 'invdate').text = format_date_for_ce(
            billing_date)

    # Invoice description
    description = data.get('description')
    if description:
        ET.SubElement(invoice, 'description').text = str(description)

    # Retention amount
    retainage_amount = data.get('retainage_amount', '0.00')
    if retainage_amount and float(retainage_amount) != 0:
        ET.SubElement(invoice, 'retamt').text = str(retainage_amount)

    # Items section - adding one single item for balancing the amount
    items_data = data.get('items')
    if items_data:
        items_elem = ET.SubElement(invoice, 'items')
        item_elem = ET.SubElement(items_elem, 'item')

        # Quantity
        qty = items_data.get('qty', 1)
        ET.SubElement(item_elem, 'qty').text = str(qty)

        # Unit price
        unit_price = items_data.get('unit_price', 0)
        ET.SubElement(item_elem, 'unitprice').text = str(unit_price)

        # Item description
        item_description = items_data.get('description', '')
        if item_description:
            ET.SubElement(item_elem, 'description').text = str(
                item_description)

    # Job distributions section
    distributions = data.get('distributions', [])
    if distributions:
        jobdist_elem = ET.SubElement(invoice, 'jobdistributions')
        for dist in distributions:
            dist_elem = ET.SubElement(jobdist_elem, 'distribution')

            # Phase number
            phase_code = str(dist.get('phase_code', ''))
            if phase_code:
                ET.SubElement(dist_elem, 'phasenum').text = phase_code

            # Category number
            category_code = str(dist.get('category_code', ''))
            if category_code:
                ET.SubElement(dist_elem, 'catnum').text = category_code

            # Amount
            amount = dist.get('amount', 0)
            ET.SubElement(dist_elem, 'amt').text = str(amount)

    return ET.tostring(invoice, encoding='unicode')


def combine_ar_invoice_xmls(invoice_xmls):
    """
    Combine multiple AR invoice XMLs into final import structure.
    """
    # Create root import element
    root = ET.Element('import', type='freeform')

    # Parse and add each invoice
    for xml_str in invoice_xmls:
        if xml_str:
            invoice_elem = ET.fromstring(xml_str)
            root.append(invoice_elem)

    # Create complete XML with declaration
    xml_declaration = '<?xml version="1.0" encoding="UTF-8"?>\n'
    xml_body = ET.tostring(root, encoding='unicode')

    return xml_declaration + xml_body


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
