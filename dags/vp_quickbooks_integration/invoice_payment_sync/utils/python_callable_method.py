"""
Common utility methods for QBO -> VP Invoice Payment Sync.

Translates the Workato recipe chain (poll → process → post) into Python
callables for the 3-DAG Airflow template
(main → dispatcher → invoice_payment_create). Error reporting goes to
middleware via PostDagRunDetailsToMiddlewareApiOperator + FailOperator on
the dispatcher's failure branch, matching customer_sync / vendor_sync.
"""
# pylint: disable=invalid-name,broad-exception-caught,too-many-locals
import logging
import re
import time
import uuid
from datetime import datetime, timedelta, timezone
from airflow.models import Variable
import rail
from vp_quickbooks_integration.invoice_payment_sync.config import (
    initial_sync_time,
    payment_lookback_minutes,
)
from vp_quickbooks_integration.common.python_callable_method import (
    collection_single_row,
    watermark_key_template,
)
from vp_quickbooks_integration.common.tables import BANK_CODE_MAP_TABLE_NAME as bank_code_map_table_name


logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Watermark helpers
# ---------------------------------------------------------------------------
WATERMARK_KEY_TEMPLATE = watermark_key_template('invoice_payment_sync')


def _customer_id_from_conf():
    return rail.get_current_context()['dag_run'].conf.get('customerId')


def _watermark_key(template, customer_id, instance):
    return template.format(
        customer_id=customer_id or 'default', instance=instance
    )


def _now_iso():
    now = datetime.now(timezone.utc)
    return now.strftime('%Y-%m-%dT%H:%M:%S') + f'.{now.microsecond // 1000:03d}Z'


def _parse_iso_utc(ts: str) -> datetime:
    """Parse a validated ISO-8601 UTC timestamp into an aware datetime (Python 3.9 safe).

    _validate_qbo_timestamp guarantees the input matches
    YYYY-MM-DDTHH:MM:SS[.f][Z|+HH:MM], so we strip optional fractional
    seconds and the Z suffix before calling strptime.
    """
    clean = re.sub(r'\.\d+', '', ts).rstrip('Z')
    return datetime.strptime(clean, '%Y-%m-%dT%H:%M:%S').replace(tzinfo=timezone.utc)


_QBO_TIMESTAMP_RE = re.compile(
    r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?(?:Z|[+-]\d{2}:?\d{2})?$"
)


def _validate_qbo_timestamp(value, field_name):
    if not isinstance(value, str) or not _QBO_TIMESTAMP_RE.match(value):
        raise ValueError(
            f"Refusing to build QBO query: {field_name}={value!r} is not a "
            "valid ISO-8601 timestamp. Watermark Variable may have been "
            "corrupted; reset it before retrying."
        )
    return value


def prepare_payment_sync_timestamps_method(instance):
    """
    Capture the QBO query window with a lookback overlap.

    The stored watermark advances to current_sync_time on success, but the
    QBO query lower bound is (last_sync_time - payment_lookback_minutes) so
    payments at a window boundary or under QBO LastUpdatedTime clock skew
    are retried on the next poll. Mirrors Workato since_offset: -1800.
    """
    customer_id = _customer_id_from_conf()
    key = _watermark_key(WATERMARK_KEY_TEMPLATE, customer_id, instance)
    current = _now_iso()

    try:
        last_sync_time = Variable.get(key)
    except KeyError:
        last_sync_time = initial_sync_time
        logger.info("Variable '%s' not found; using initial_sync_time", key)

    _validate_qbo_timestamp(last_sync_time, 'last_sync_time')

    # Subtract lookback so the QBO WHERE clause starts payment_lookback_minutes
    # before the stored watermark, giving a rolling overlap between polls.
    # update_payment_last_sync_times_method advances the watermark to
    # current_sync_time, not to this query_since value.
    query_since_dt = _parse_iso_utc(last_sync_time) - timedelta(
        minutes=payment_lookback_minutes
    )
    query_since = (
        query_since_dt.strftime('%Y-%m-%dT%H:%M:%S') + '.000Z'
    )

    logger.info(
        "Invoice payment sync window: query [%s, %s) "
        "(watermark last_sync_time=%s, lookback=%dm)",
        query_since, current, last_sync_time, payment_lookback_minutes
    )
    return {
        'last_sync_time': query_since,
        'current_sync_time': current,
    }


def update_payment_last_sync_times_method(instance):
    """Persist current_sync_time into the watermark Variable."""
    try:
        is_enabled = rail.result('check_disabled_flag')
    except KeyError:
        is_enabled = True
    if not is_enabled:
        logger.info("Integration disabled; skipping watermark advance")
        return None

    customer_id = _customer_id_from_conf()
    key = _watermark_key(WATERMARK_KEY_TEMPLATE, customer_id, instance)
    timestamps = rail.result('prepare_sync_timestamps')
    current = timestamps['current_sync_time']
    Variable.set(key, current)
    logger.info("Advanced payment watermark '%s' to: %s", key, current)
    return current


# ---------------------------------------------------------------------------
# Disabled-flag check
# ---------------------------------------------------------------------------
def is_integration_enabled_method(instance):
    """
    True unless CFG_DisableInvoicePaymentIntegration is set to 'true'.
    Checks per-tenant key first, then instance-level kill switch.
    """
    customer_id = _customer_id_from_conf() or 'default'
    tenant_key = (
        f'CFG_DisableInvoicePaymentIntegration_{customer_id}_{instance}'
    )
    instance_key = f'CFG_DisableInvoicePaymentIntegration_{instance}'

    tenant_flag = Variable.get(tenant_key, default_var=None)
    if tenant_flag is not None and str(tenant_flag).strip().lower() == 'true':
        logger.info(
            "Invoice payment integration disabled for tenant '%s' on "
            "instance '%s' via %s", customer_id, instance, tenant_key
        )
        return False

    instance_flag = Variable.get(instance_key, default_var='false')
    if str(instance_flag).strip().lower() == 'true':
        logger.info(
            "Invoice payment integration disabled for entire instance '%s' "
            "via %s", instance, instance_key
        )
        return False

    return True


# ---------------------------------------------------------------------------
# Payment list extraction + zero-amount filter
# ---------------------------------------------------------------------------
def _extract_one_payment(raw):
    """
    Flatten a single raw QBO Payment into the shape passed to the worker.

    Extracts InvoiceID from Payment.Line[0].LinkedTxn[0].TxnId — mirrors
    the Workato recipe 1 trigger expression:
      _('data.quickbooks.7949e2d0.Line.first.LinkedTxn.first.TxnId')
    """
    lines = raw.get('Line') or []
    invoice_id = ''
    if lines:
        linked_txns = lines[0].get('LinkedTxn') or []
        if linked_txns:
            invoice_id = str(linked_txns[0].get('TxnId') or '')

    return {
        'PaymentID': str(raw.get('Id') or ''),
        'InvoiceID': invoice_id,
        'TotalAmt': float(raw.get('TotalAmt') or 0),
        'TxnDate': raw.get('TxnDate') or '',
        'CustomerRef': (raw.get('CustomerRef') or {}).get('value') or '',
        'DepositToAccountRef': (
            (raw.get('DepositToAccountRef') or {}).get('value') or ''
        ),
        'SyncToken': raw.get('SyncToken') or '',
    }


def extract_and_filter_payments_method():
    """
    Extract the payment list from QuickBooksPaymentOperator response and
    apply the zero-amount filter.

    Mirrors Workato recipe 1 step 1: `if TotalAmt != 0`.
    Payments without an InvoiceID (no LinkedTxn) are also skipped since the
    worker would have nothing to match against in Vantagepoint.
    """
    result = rail.result('get_recently_changed_payments')
    if isinstance(result, dict) and not result.get('success', True):
        logger.warning(
            "QuickBooks payment query failed: %s", result.get('error')
        )
        return []

    if isinstance(result, dict):
        raw_list = result.get('data') or result.get('Payment') or []
    elif isinstance(result, list):
        raw_list = result
    else:
        raw_list = []

    payments = []
    skipped_zero = 0
    skipped_no_invoice = 0

    for raw in raw_list:
        if not raw:
            continue
        extracted = _extract_one_payment(raw)

        if extracted['TotalAmt'] == 0:
            skipped_zero += 1
            logger.debug(
                "Skipping zero-amount payment PaymentID=%s",
                extracted['PaymentID']
            )
            continue

        if not extracted['InvoiceID']:
            skipped_no_invoice += 1
            logger.warning(
                "Skipping payment PaymentID=%s: no LinkedTxn InvoiceID found",
                extracted['PaymentID']
            )
            continue

        payments.append(extracted)

    logger.info(
        "Payments: %d to process, %d skipped (zero-amount), "
        "%d skipped (no invoice link)",
        len(payments), skipped_zero, skipped_no_invoice
    )
    return payments


# ---------------------------------------------------------------------------
# S3 mapping collection helpers
# ---------------------------------------------------------------------------

def _query_mapping_row(task_id, query, query_params, table_name):  # pylint: disable=unused-argument
    return collection_single_row(query, query_params, read_task_id=task_id)


# ---------------------------------------------------------------------------
# Worker DAG helpers
# ---------------------------------------------------------------------------

def _get_conf():
    return rail.get_current_context()['dag_run'].conf or {}


def build_invoice_filter():
    """
    OData filter for VP PSALedger TransType=IN query.

    Resolves DocNumber from the QBO invoice fetched in fetch_qbo_invoice,
    then builds ?$filter=Invoice eq '{DocNumber}'.

    Replaces the Workato Outstanding Sales Invoices lookup table lookup
    (recipe 2 step 7) — we query VP directly by invoice number instead.
    """
    invoice_result = rail.result('fetch_qbo_invoice') or {}
    data = invoice_result.get('data') or []
    if isinstance(data, list):
        invoice = data[0] if data else {}
    else:
        invoice = data

    doc_number = str(invoice.get('DocNumber') or '').strip()
    if not doc_number:
        raise ValueError(
            f"Could not resolve VP invoice number: QBO Invoice "
            f"{ _get_conf().get('InvoiceID') } has no DocNumber. "
            "Verify the invoice exists in QuickBooks."
        )
    # Escape single quotes for OData safety.
    safe_doc = doc_number.replace("'", "''")
    return f"?$filter=Invoice eq '{safe_doc}'"


def _derive_company_code():
    """
    Derive VP company code from the invoice Org field.

    Mirrors Workato recipe 2 step 13:
      Company = Org[:len(first_org_code)]
    where first_org_code is the first Code from CFGOrgCodes.
    Returns empty string when either input is unavailable.
    """
    try:
        org_codes_result = rail.result('fetch_vp_org_codes') or {}
        items = org_codes_result.get('items') or []
        if not items:
            return ''
        first_code = str(items[0].get('Code') or '').strip()
        if not first_code:
            return ''

        ledger_rows = rail.result('fetch_vp_invoice_lines') or []
        org = str(
            (ledger_rows[0] if ledger_rows else {}).get('Org') or ''
        ).strip()
        if not org:
            return ''

        return org[:len(first_code)]
    except Exception:
        logger.debug(
            "Could not derive company code; defaulting to ''", exc_info=True
        )
        return ''


def build_ar_account_filter():
    """
    Query string for VP GET AccountConfiguration/CFGAutoPosting.

    Replaces Workato recipe 2 step 14 (Get Accounts Receivable Code).
    """
    company = _derive_company_code()
    if company:
        return f'?company={company}'
    return ''


def resolve_bank_code_method(instance):
    """
    Look up the Vantagepoint bank code for the QBO deposit account.

    Queries the shared mapping_sync S3 collection:
      bank_code_map WHERE QBOID = ?

    S3 table schema (bank_code_map):
      VantagepointName, VantagepointCode, QBOName, QBOID,
      Status, Company, Org, Account

    Mirrors Workato lookup table 014-503 PSA Bank Code Map +
    recipe 3 step 3 (Resolve Bank Code sub-recipe).

    Returns the VantagepointCode string, or '' if not found.
    """
    payment_result = rail.result('fetch_qbo_payment') or {}
    payment_data = (
        payment_result.get('data')
        if isinstance(payment_result, dict)
        else payment_result
    )
    if isinstance(payment_data, list):
        payment_data = payment_data[0] if payment_data else {}

    qbo_account_id = str(
        (payment_data.get('DepositToAccountRef') or {}).get('value') or ''
    ).strip()

    if not qbo_account_id:
        logger.warning(
            "Payment has no DepositToAccountRef; cannot resolve bank code"
        )
        return ''

    row = _query_mapping_row(
        task_id='_lookup_bank_code',
        query=(
            f'SELECT VantagepointCode FROM {bank_code_map_table_name} '
            f'WHERE QBOID = ? LIMIT 1'
        ),
        query_params=[qbo_account_id],
        table_name=bank_code_map_table_name,
    )

    if not row:
        logger.warning(
            "No bank code mapping for QBO account '%s' in S3 collection",
            qbo_account_id,
        )
        return ''

    if isinstance(row, dict):
        code = str(row.get('VantagepointCode') or '').strip()
    else:
        try:
            code = str(row[0] or '').strip()
        except (TypeError, IndexError):
            code = ''

    if code:
        logger.info(
            "Resolved bank code '%s' for QBO account '%s'",
            code, qbo_account_id,
        )
    return code


def compute_payment_lines_method():
    """
    Compute weighted payment allocation across VP PSA Ledger line items.

    Mirrors Workato recipe 2 step 12 smart-list SQL:

        SELECT
            a.Invoice, a.WBS1, a.WBS2, a.WBS3,
            abs(a.TransactionAmount)           AS Outstanding_Amount,
            {payment_amount}                   AS Total_Payment_Amount,
            b.Total,
            abs(a.TransactionAmount)/b.Total   AS Weighting,
            {payment_amount} * (abs(a.TransactionAmount)/b.Total) AS Weighted_Payment,
            abs(a.TransactionAmount) - Weighted_Payment            AS Balance,
            abs(a.TaxBasis)                    AS Tax_Basis,
            a.TaxCode                          AS Tax_Code,
            a.Org
        FROM psaledger a
        INNER JOIN (SELECT SUM(abs(TransactionAmount)) Total FROM psaledger) b

    Payment amount is taken from Payment.Line[0].Amount (the line linked to
    the invoice being processed), which equals the per-invoice applied amount
    rather than the payment's TotalAmt (relevant when a single payment covers
    multiple invoices — we only process the first linked invoice per the
    dispatcher's InvoiceID extraction logic).

    Returns list[dict] — one entry per PSA Ledger row.
    """
    ledger_rows = rail.result('fetch_vp_invoice_lines') or []
    if not ledger_rows:
        raise ValueError(
            "No VP PSA Ledger rows available for payment line computation."
        )

    # Resolve payment amount for this specific invoice's line.
    payment_result = rail.result('fetch_qbo_payment') or {}
    payment_data = (
        payment_result.get('data')
        if isinstance(payment_result, dict)
        else payment_result
    )
    if isinstance(payment_data, list):
        payment_data = payment_data[0] if payment_data else {}

    payment_lines = payment_data.get('Line') or []
    # Use the first line's Amount (matches dispatcher's .first logic).
    payment_amount = float(
        (payment_lines[0] if payment_lines else {}).get('Amount') or 0
    )

    if payment_amount == 0:
        logger.warning(
            "Payment amount resolved to 0 for PaymentID=%s; "
            "all Weighted_Payment values will be 0.",
            payment_data.get('Id', '')
        )

    # Total = sum of absolute TransactionAmounts across all ledger rows.
    total = sum(
        abs(float(row.get('TransactionAmount') or 0)) for row in ledger_rows
    )
    if total == 0:
        raise ValueError(
            "VP PSA Ledger rows have zero total TransactionAmount; "
            "cannot distribute payment."
        )

    invoice_result = rail.result('fetch_qbo_invoice') or {}
    invoice_data = invoice_result.get('data') or []
    if isinstance(invoice_data, list):
        invoice_data = invoice_data[0] if invoice_data else {}
    vp_invoice_number = str(invoice_data.get('DocNumber') or '').strip()

    lines = []
    allocated = 0.0
    last_idx = len(ledger_rows) - 1
    for idx, row in enumerate(ledger_rows):
        txn_amount = abs(float(row.get('TransactionAmount') or 0))
        weighting = txn_amount / total

        if idx == last_idx:
            # Assign the remainder to the final line so the sum of all
            # Weighted_Payment values equals payment_amount exactly.
            # Independent per-line rounding accumulates fractional drift
            # that causes the crDetail total to diverge from crMaster Total,
            # which Vantagepoint rejects as an out-of-balance batch.
            weighted_payment = round(payment_amount - allocated, 2)
        else:
            weighted_payment = round(payment_amount * weighting, 2)

        allocated += weighted_payment
        balance = round(txn_amount - weighted_payment, 2)

        lines.append({
            'Invoice': vp_invoice_number or row.get('Invoice', ''),
            'WBS1': row.get('WBS1') or '',
            'WBS2': row.get('WBS2') or '',
            'WBS3': row.get('WBS3') or '',
            'Outstanding_Amount': txn_amount,
            'Total_Payment_Amount': payment_amount,
            'Total': total,
            'Weighting': weighting,
            'Weighted_Payment': weighted_payment,
            'Balance': balance,
            'Tax_Basis': abs(float(row.get('TaxBasis') or 0)),
            'Tax_Code': row.get('TaxCode') or '',
            'Org': row.get('Org') or '',
        })

    logger.info(
        "Computed %d payment line(s) for PaymentID=%s, "
        "payment_amount=%s, total_txn=%s, allocated=%s",
        len(lines), payment_data.get('Id', ''), payment_amount, total, allocated
    )
    return lines


def build_cash_receipt_body():
    """
    Build the Vantagepoint POST /DataEntry/crControl request body.

    Mirrors Workato recipe 3 step 9 (cash_receipt action):
    - crMaster: one header row per batch (bank code, ref, date, etc.)
    - crDetail: one row per computed payment line (WBS, amount, account, etc.)

    Batch number = Unix epoch seconds (mirrors Workato =now.to_i).
    PKey = UUID without hyphens (mirrors Workato =workato.uuid.gsub('-','')).
    """
    # Resolved inputs
    payment_result = rail.result('fetch_qbo_payment') or {}
    payment_data = (
        payment_result.get('data')
        if isinstance(payment_result, dict)
        else payment_result
    )
    if isinstance(payment_data, list):
        payment_data = payment_data[0] if payment_data else {}

    computed_lines = rail.result('compute_payment_lines') or []
    bank_code = rail.result('resolve_bank_code') or ''

    active_period_result = rail.result('get_active_period') or {}
    if isinstance(active_period_result, list):
        active_period_result = active_period_result[0] if active_period_result else {}
    period = str(active_period_result.get('Period') or '').strip()

    ar_account_result = rail.result('fetch_ar_account') or {}
    if isinstance(ar_account_result, list):
        ar_account_result = ar_account_result[0] if ar_account_result else {}
    ar_account = str(ar_account_result.get('AcctsReceivable') or '').strip()

    ledger_rows = rail.result('fetch_vp_invoice_lines') or []
    first_row = ledger_rows[0] if ledger_rows else {}

    # Use first invoice line's invoice number as the receipt description.
    invoice_number = (computed_lines[0] or {}).get('Invoice', '') if computed_lines else ''
    payment_id = str(payment_data.get('Id') or '').strip()
    txn_date = str(payment_data.get('TxnDate') or '').strip()
    total_amt = float(payment_data.get('TotalAmt') or 0)

    company = _derive_company_code()
    batch = str(int(time.time()))

    cr_detail = []
    for line in computed_lines:
        cr_detail.append({
            'Batch': batch,
            'RefNo': payment_id,
            'PKey': uuid.uuid4().hex,
            'Seq': '',  # VP auto-assigns
            'Description': 'Sales',
            'WBS1': line['WBS1'],
            'WBS2': line['WBS2'],
            'WBS3': line['WBS3'],
            'Org': line['Org'],
            'Account': ar_account,
            'Amount': line['Weighted_Payment'],
            'Interest': '0',
            'TaxCode': line['Tax_Code'],
            'TaxBasis': line['Tax_Basis'],
            'Retainer': '0',
            'CurrencyExchangeOverrideRate': '0',
            'SourceCurrencyCode': '',
            'SourceBalance': line['Balance'],
            'SourceAmount': line['Weighted_Payment'],
            'SourceExchangeInfo': first_row.get('SourceExchangeInfo') or '',
            'LinkCompany': first_row.get('LinkCompany') or '',
            'PreInvoice': first_row.get('PreInvoice') or '',
            'Invoice': line['Invoice'],
        })

    return {
        'Batch': batch,
        'Description': f"Payment on Invoice {invoice_number}",
        'Recurring': 'N',
        'EndDate': txn_date,
        'Total': total_amt,
        'DefaultBank': bank_code,
        'Selected': 'N',
        'Posted': 'N',
        'Creator': 'Integration',
        'Period': period,
        'PostPeriod': '',
        'Company': company,
        'crMaster': {
            'Batch': batch,
            'RefNo': payment_id,
            'Posted': 'N',
            'CurrencyExchangeOverrideMethod': 'N',
            'CurrencyExchangeOverrideRate': '0',
            'Status': 'A',
            'ModDate': '',
            'DiaryNo': '0',
            'TransDate': txn_date,
            'TransComment': 'Payment',
            'BankCode': bank_code,
            'Seq': '1',
            'CurrencyCode': '',
            'CurrencyExchangeOverrideDate': '',
            'AuthorizedBy': '',
            'RejectReason': '',
            'ModUser': '',
            'Diary': '',
        },
        'crDetail': cr_detail,
    }


def build_post_transaction_body():
    """
    Build the Vantagepoint PUT /DataEntry/PostTransFile request body.

    Mirrors Workato recipe 3 step 10 (post_transaction_entries):
      parms[].batch      = Batch from posted cash receipt
      parms[].description = TransComment from the receipt header
      parms[].period     = RawPeriod (numeric YYYYPP)
      parms[].transtype  = 'CR'
    """
    cr_result = rail.result('post_cash_receipt') or {}
    if isinstance(cr_result, list):
        cr_result = cr_result[0] if cr_result else {}

    batch = str(cr_result.get('Batch') or '').strip()
    comment = str(cr_result.get('TransComment') or 'Payment').strip()

    active_period_result = rail.result('get_active_period') or {}
    if isinstance(active_period_result, list):
        active_period_result = active_period_result[0] if active_period_result else {}
    raw_period = str(
        active_period_result.get('RawPeriod')
        or active_period_result.get('Period')
        or ''
    ).strip()

    return {
        'parms': [{
            'batch': batch,
            'description': comment,
            'period': raw_period,
            'transtype': 'CR',
        }]
    }


# ---------------------------------------------------------------------------
# Failure callables for guard tasks (replaces obfuscated generator-throw pattern)
# ---------------------------------------------------------------------------
def fail_invoice_not_found_method():
    """Raise a descriptive RuntimeError when no VP PSALedger rows exist for the invoice."""
    invoice_id = rail.get_current_context()['dag_run'].conf.get('InvoiceID')
    raise RuntimeError(
        f"No VP PSALedger rows found for invoice {invoice_id}. "
        "Verify the invoice exists in Vantagepoint and that the QBO Invoice "
        "DocNumber matches the VP invoice reference."
    )


def fail_bank_code_error_method(instance):
    """Raise a descriptive RuntimeError when the QBO deposit account has no bank code mapping."""
    payment_result = rail.result('fetch_qbo_payment') or {}
    payment_data = payment_result.get('data') or {}
    if isinstance(payment_data, list):
        payment_data = payment_data[0] if payment_data else {}
    qbo_account = str(
        (payment_data.get('DepositToAccountRef') or {}).get('value') or 'unknown'
    )
    raise RuntimeError(
        f"QuickBooks bank account '{qbo_account}' not matched to a Vantagepoint bank. "
        f"Add a row to the bank_code_map S3 collection for this customer "
        f"(QBOID='{qbo_account}')."
    )


# ---------------------------------------------------------------------------
# Error capture for the worker DAG
# ---------------------------------------------------------------------------
def capture_payment_dag_error(payment_id, invoice_id, fallback_error):
    """Return an error dict when something failed; None on a clean run."""
    if not fallback_error:
        return None
    return {
        'error': (
            f"PaymentID {payment_id} (InvoiceID {invoice_id}) - "
            f"invoice payment worker failed: {fallback_error}"
        ),
        'PaymentID': payment_id,
        'InvoiceID': invoice_id,
    }
