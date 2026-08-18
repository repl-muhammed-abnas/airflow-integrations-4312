import json
from datetime import datetime, timezone

from airflow.models import Variable


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')


def load(airflow_variable_key: str) -> dict:
    try:
        raw_json = Variable.get(airflow_variable_key, default_var='{}')
        return json.loads(raw_json)
    except Exception:
        print(f"Could not parse retry variable '{airflow_variable_key}'; resetting to empty.")
        return {}


def save(airflow_variable_key: str, pending_retries: dict) -> None:
    Variable.set(airflow_variable_key, json.dumps(pending_retries, separators=(',', ':')))



def upsert(pending_retries: dict, invoice_id, invoice_number: str, project_id, company_id) -> dict:
    invoice_key = str(invoice_id)
    queued_at = _now_iso()
    if invoice_key in pending_retries:
        pending_retries[invoice_key]['last_retried_at'] = queued_at
        pending_retries[invoice_key]['retry_count'] += 1
    else:
        pending_retries[invoice_key] = {
            'invoice_id': invoice_id,
            'invoice_number': invoice_number,
            'project_id': project_id,
            'company_id': company_id,
            'first_seen_at': queued_at,
            'last_retried_at': queued_at,
            'retry_count': 1,
        }
    return pending_retries


def discard_resolved(pending_retries: dict, successfully_processed_invoice_ids: set) -> dict:
    return {
        invoice_key: retry_entry
        for invoice_key, retry_entry in pending_retries.items()
        if retry_entry['invoice_id'] not in successfully_processed_invoice_ids
    }


def update_retry_queue(record_not_found_invoices, successfully_processed_invoice_ids,
                       errors_this_run, initial_invoice_by_id,
                       airflow_variable_key, max_retry_attempts):
    pending_retries = load(airflow_variable_key)

    for failed_invoice in record_not_found_invoices:
        failed_invoice_id = failed_invoice['invoice_id']
        original_invoice = initial_invoice_by_id.get(failed_invoice_id, {})
        pending_retries = upsert(
            pending_retries,
            invoice_id=failed_invoice_id,
            invoice_number=failed_invoice.get('invoice_number', ''),
            project_id=original_invoice.get('project_id', ''),
            company_id=original_invoice.get('company_id', ''),
        )

    if successfully_processed_invoice_ids:
        pending_retries = discard_resolved(pending_retries, successfully_processed_invoice_ids)

    permanent_failure_ids = {
        error_entry['invoice_id'] for error_entry in errors_this_run
        if str(error_entry['invoice_id']) in pending_retries
    }
    if permanent_failure_ids:
        pending_retries = discard_resolved(pending_retries, permanent_failure_ids)

    exhausted_entries = [
        entry for entry in pending_retries.values()
        if entry.get('retry_count', 0) >= max_retry_attempts
    ]
    if exhausted_entries:
        pending_retries = {
            k: v for k, v in pending_retries.items()
            if v.get('retry_count', 0) < max_retry_attempts
        }

    save(airflow_variable_key, pending_retries)
    print(f"Retry variable updated: {len(record_not_found_invoices)} queued, "
          f"{len(successfully_processed_invoice_ids)} resolved, "
          f"{len(permanent_failure_ids)} permanently failed, "
          f"{len(exhausted_entries)} exhausted.")

    return exhausted_entries
