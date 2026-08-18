from datetime import datetime
import rail
from onepointapac.invoice_export_singapore import config


def get_date_string(dateobj):
    return f"{dateobj.get('year')}-{dateobj.get('month')}-{dateobj.get('day')}"


def page_handler(request, response):
    if len(response['rows']) > 0:
        request['page'] += 1
        return request
    return None


def get_invoice_detail(row, ignored_data_types):
    """Map one InvoiceListService2 GetData row to the invoice dict carried to the child."""
    try:
        cells = row['cells']
        return {
            'invoice': cells[0]['uri'],
            'invoice_number': cells[1]['textValue'],
            'client': {
                'textValue': cells[2]['textValue'],
                'uri': cells[2]['uri']
            },
            'creation_datetime': get_date_string(cells[3]['dateValue']),
            'invoice_status': {
                'textValue': cells[5]['textValue'],
                'uri': cells[5]['uri']
            },
            'payment_due_date': get_date_string(cells[6]['dateValue']),
            'total_invoice_amount': {k: v for k, v in cells[7].items() if k not in ignored_data_types},
            'invoice_currency': {k: v for k, v in cells[8].items() if k not in ignored_data_types},
        }
    except KeyError:
        return None


def filter_data(response):
    flatten_rows = [row for item in response for row in item['rows']]
    ignored_data_types = ('dataType', 'objectType')
    invoices = [
        get_invoice_detail(row, ignored_data_types) for row in flatten_rows
    ] if flatten_rows else []
    return [invoice for invoice in invoices if invoice is not None]


def handle_updated_invoices(response, item):
    """Watermark filter: keep only invoices modified at/after the last sync time."""
    def is_at_or_after_lastsync(datetime_value):
        modified = datetime(
            year=datetime_value['year'], month=datetime_value['month'], day=datetime_value['day'],
            hour=datetime_value['hour'], minute=datetime_value['minute'], second=datetime_value['second'])
        last_sync_time = datetime.strptime(
            rail.result('get_lastsync_time')['last_synctime'], '%Y-%m-%dT%H:%M:%S')
        return modified >= last_sync_time

    last_modified_timestamp = response['lastModifiedTimestamp']['valueInUtc']
    if is_at_or_after_lastsync(last_modified_timestamp):
        return {**item}
    return None


def extract_invoice_item_description(invoice_item_detail):
    """Ad-hoc line description: join the text of every customMetadata entry whose key
    references 'description' (matches the recipe's `where(keyUri: /description/i)`)."""
    texts = []
    for meta in (invoice_item_detail or {}).get('customMetadata') or []:
        key_uri = meta.get('keyUri') or ''
        if config.INVOICE_ITEM_DESCRIPTION_KEY in key_uri or 'description' in key_uri.lower():
            text = (meta.get('value') or {}).get('text')
            if text:
                texts.append(text)
    return " ".join(texts) if texts else None
