from datetime import datetime, timedelta, timezone
import rail
from airflow.models import Variable
from onepointapac.invoice_export_singapore import config


def read_lastsync_time(config):
    """Reads the previous watermark and immediately advances it to now, so the read and
    the update happen in this single task (no separate update_lastsync_time task)."""
    last = Variable.get(config.last_sync_time_var_name, default_var=None)
    if not last:
        last = (datetime.now(timezone.utc) -
                timedelta(minutes=5)).strftime('%Y-%m-%dT%H:%M:%S')
    current = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S')
    Variable.set(config.last_sync_time_var_name, current)
    return {'last_synctime': last, 'current_time': current}


def updated_invoices():
    """Invoices that passed the watermark filter (drops the None skips)."""
    return [invoice for invoice in (rail.result('get_required_invoices') or []) if invoice]


def get_all_invoice_child_dagrun_ids(parallel_count):
    """Flattens the per-lane dagrun ids from trigger_parallel_dagrun's
    trigger_invoice_child_dag_1..N tasks into a single list for gathering."""
    dagrun_ids = list(filter(None, map(
        lambda x: rail.result(f'trigger_invoice_child_dag_{x + 1}'), range(parallel_count))))
    if not dagrun_ids:
        return []
    flattened = []
    for ids in dagrun_ids:
        flattened.extend(ids)
    return flattened


def is_currency_sgd(dag_run):
    """Recipe guard: only Singapore invoices (amount cell text starts with 'SGD$')."""
    amount_text = (dag_run.conf.get('total_invoice_amount') or {}).get('textValue') or ''
    return amount_text.startswith(config.REQUIRED_CURRENCY_PREFIX)


def is_invoice_status_processable(dag_run):
    """Recipe guard: skip invoices already Billed or Paid."""
    status_text = (dag_run.conf.get('invoice_status') or {}).get('textValue') or ''
    return not any(skip in status_text for skip in config.SKIP_INVOICE_STATUSES)


def get_downstreamtasks_error(invoice_number_name, error_message):
    return {
        'error': f'Error with {invoice_number_name} - {error_message}'
    }
