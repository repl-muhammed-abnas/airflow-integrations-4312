"""Response parsers and data handlers for the MMR Consulting invoice export."""
from datetime import datetime
import rail
from mmr_consulting.invoice_export import config


def get_date_string(dateobj):
    return f"{dateobj.get('year')}-{dateobj.get('month')}-{dateobj.get('day')}"


def get_time_string(timeobj):
    return f"{timeobj.get('hour')}:{timeobj.get('minute')}:{timeobj.get('second')}"


def page_handler(request, response):
    if len(response['rows']) > 0:
        request['page'] += 1
        return request
    return None


def get_invoice_detail(row, ignored_data_types):
    try:
        return {
            'invoice': row['cells'][0]['uri'],
            'invoice_number': row['cells'][1]['textValue'],
            'client': {
                'textValue': row['cells'][2]['textValue'],
                'uri': row['cells'][2]['uri']
            },
            'creation_datetime': get_date_string(row['cells'][3]['dateValue']),
            'last_modified_datetime': {
                'date': get_date_string(row['cells'][4]['dateValue']),
                'time': get_time_string(row['cells'][4]['timeValue'])
            },
            'invoice_status': {
                'textValue': row['cells'][5]['textValue'],
                'uri': row['cells'][5]['uri']
            },
            'payment_due_date': get_date_string(row['cells'][6]['dateValue']),
            'invoice_date': row['cells'][7]['textValue'],
            'total_invoice_amount': {k: v for k, v in row['cells'][8].items() if k not in ignored_data_types},
            'invoice_currency': {k: v for k, v in row['cells'][9].items() if k not in ignored_data_types},
            'payment_term': {k: v for k, v in row['cells'][10].items() if k not in ignored_data_types},
            'invoice_amount_in_base_currency': {k: v for k, v in row['cells'][11].items() if k not in ignored_data_types},
            'description': row['cells'][12].get('textValue', '')
        }
    except KeyError:
        return None


def filter_data(response):
    flatten_rows = [row for item in response for row in item['rows']]
    ignored_data_types = ('dataType', 'objectType')
    queued_for_sync_invoices = [
        get_invoice_detail(row, ignored_data_types) for row in flatten_rows
    ] if flatten_rows else []
    return [invoice for invoice in queued_for_sync_invoices if invoice is not None]


def extract_invoice_po_number(invoice_details):
    """Returns the invoice-level PO number from customMetadata."""
    for meta in invoice_details.get('customMetadata') or []:
        if meta.get('keyUri') == config.INVOICE_PO_NUMBER_KEY:
            return (meta.get('value') or {}).get('text')
    return None


def handle_updated_invoices(response, item):
    def compare_datetime_value(datetime_value):
        datetime_value = datetime(
            year=datetime_value['year'], month=datetime_value['month'], day=datetime_value['day'],
            hour=datetime_value['hour'], minute=datetime_value['minute'], second=datetime_value['second'])
        last_sync_time = datetime.strptime(rail.result('get_lastsync_time')[
                                           'last_synctime'], '%Y-%m-%dT%H:%M:%S')
        return datetime_value >= last_sync_time
    last_modified_timestamp = response['lastModifiedTimestamp']['valueInUtc']
    is_valid_invoice = compare_datetime_value(last_modified_timestamp)
    if is_valid_invoice:
        return {**item, 'po_number': extract_invoice_po_number(response)}
    return ''


def attach_client_country(response, item):
    """Attaches client billing country to the invoice for routing."""
    client_address = response.get('clientAddress') or {}
    country = (client_address.get('country') or {}).get('name')
    return {**item, 'client_country': country}


def extract_invoice_item_description(invoice_item_detail):
    """Returns the item-level description from customMetadata."""
    for meta in (invoice_item_detail or {}).get('customMetadata') or []:
        if meta.get('keyUri') == config.INVOICE_ITEM_DESCRIPTION_KEY:
            return (meta.get('value') or {}).get('text')
    return None


def extract_project_po_type(project_detail):
    """Returns the PO Type extension field value for a project."""
    for field in (project_detail or {}).get('extensionFieldValues') or []:
        definition = field.get('definition') or {}
        if definition.get('displayText') == config.PO_TYPE_FIELD_NAME:
            return (field.get('tag') or {}).get('displayText')
    return None
