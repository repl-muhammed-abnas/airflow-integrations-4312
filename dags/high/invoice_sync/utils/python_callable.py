import rail
from datetime import datetime, timedelta, timezone
import json
import itertools


def last_sync_time(last_sync_var):
    sync_time = (datetime.now(
                timezone.utc) - timedelta(minutes=5)).strftime('%Y-%m-%d %H:%M:%S')
    return rail.get_lastsync_time_variable(
        variable_name= last_sync_var,
        date_format='%Y-%m-%dT%H:%M:%SZ',
        initial_sync_time= sync_time,
        reset_after_threshold=False
        )

def update_last_sync(update_sync_time):
    return rail.set_lastsync_time_variable(
            variable_name= update_sync_time,
            value_to_set= rail.result('get_last_sync_time')['current_time']
        )

def json_formatter_get_inoivce():
    item = (rail.result("get_required_invoices"))
    filtered_data = [i for i in item if i is not None]
    return json.dumps(filtered_data, indent=4, ensure_ascii=False)


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
            'creation_datetime': row['cells'][3]['textValue'],
            'last_modified_datetime': row['cells'][4]['textValue'],
            'invoice_status': {
                'textValue': row['cells'][5]['textValue'],
                'uri': row['cells'][5]['uri']
            },
            'payment_due_date': row['cells'][6]['textValue'],
            'invoice_date': row['cells'][7]['textValue'],
            'total_invoice_amount': {k: v for k, v in row['cells'][8].items() if k not in ignored_data_types},
            'invoice_currency': {k: v for k, v in row['cells'][9].items() if k not in ignored_data_types},
            'payment_term': {k: v for k, v in row['cells'][10].items() if k not in ignored_data_types},
            'invoice_amount_in_base_currency': {k: v for k, v in row['cells'][11].items() if k not in ignored_data_types},
            'description': row['cells'][12].get('textValue', ''),
        }
    except KeyError:
        return None

def filter_data(response):
            flatten_rows = list(itertools.chain(
                *list(map(lambda x: x['rows'], response))))
            accepted_billing_status = ('In Draft')
            ignored_data_types = ('dataType', 'objectType')
            queued_for_sync_invoice = list(
                map(lambda row: get_invoice_detail(row, ignored_data_types),
                    filter(lambda x: x['cells'][5]['textValue'] in accepted_billing_status, flatten_rows))) if flatten_rows else []
            return [invoice for invoice in queued_for_sync_invoice if invoice is not None]

def handle_updated_invoices(response):
            def compare_datetime_value(datetime_value):
                datetime_value = datetime(
                    year=datetime_value['year'], month=datetime_value['month'], day=datetime_value['day'],
                    hour=datetime_value['hour'], minute=datetime_value['minute'], second=datetime_value['second'])
                last_synctime = datetime.strptime(
                    rail.result('get_last_sync_time')['last_synctime'], '%Y-%m-%dT%H:%M:%SZ')
                return datetime_value >= last_synctime
            last_modified_timestamp = response['lastModifiedTimestamp']['valueInUtc']
            is_valid_invoice = compare_datetime_value(last_modified_timestamp)
            if is_valid_invoice:
                return response
            return None

