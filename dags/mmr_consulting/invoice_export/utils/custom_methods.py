"""Business-logic helpers for the MMR Consulting invoice export."""
from datetime import datetime, timedelta, timezone
import rail
from airflow.models import Variable
from mmr_consulting.invoice_export.mapper import countries


def read_lastsync_time(config):
    last = Variable.get(
        config.last_sync_time_var_name, default_var=None)
    if not last:
        last = (datetime.now(timezone.utc) -
                timedelta(minutes=5)).strftime('%Y-%m-%dT%H:%M:%S')
    current = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S')
    return {'last_synctime': last, 'current_time': current}


def write_lastsync_time(config):
    Variable.set(config.last_sync_time_var_name,
                 rail.result('get_lastsync_time')['current_time'])


def currency_name(item):
    return (item.get('invoice_currency') or {}).get('textValue')


def country_for_client_country(client_country):
    return countries.CLIENT_COUNTRY_TO_COUNTRY.get(client_country)


def country_for_item(item):
    return country_for_client_country(item.get('client_country'))

def currency_code_for(currency_name):
    return countries.CURRENCY_CODE_BY_NAME.get(currency_name)

def currency_code_for_item(item):
    return currency_code_for(currency_name(item))


def routable_invoices():
    """Invoices whose client country maps to a configured country; others skipped."""
    invoices = []
    for invoice in rail.result('enrich_invoices_with_client_country'):
        if not invoice:
            continue
        if country_for_item(invoice):
            invoices.append(invoice)
    return invoices


def distinct_project_uris():
    """Unique project URIs across the invoice's line items."""
    uris = []
    for item in (rail.result('get_all_invoice_items') or []):
        uri = (item.get('project') or {}).get('uri')
        if uri and uri not in uris:
            uris.append(uri)
    return uris


def get_downstreamtasks_error(invoice_number_name, error_message):
    return {
        'error': f'Error with {invoice_number_name} - {error_message}'
    }
