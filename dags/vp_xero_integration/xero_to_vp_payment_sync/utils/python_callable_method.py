"""Python callable helpers for Xero -> VP Payment Sync.

Provides all callable methods referenced by the four payment-sync DAGs:
  - dispatcher_dag: extract AR/AP payments, merge errors
  - invoice_payment_processor_dag: AR (ACCRECPAYMENT -> CR) path
  - bill_payment_processor_dag: PP (AP Voucher) and EP (Expense) paths

Workato parity: mirrors `014_501_psa_xero_invoice_payment_adds_to_vantagepoint`
and `014_501_psa_xero_bill_payment_adds_to_vantagepoint`.
"""
# pylint: disable=too-many-statements,too-many-locals,too-many-branches
import logging
import re
import time
import uuid
from datetime import datetime, timezone

import rail
from airflow.models import Variable
from rail.lib.errors import get_error_message, get_failed_upstream_task_ids
from vp_xero_integration.common.python_callable_method import (
    build_watermark_variable_key,
    collection_rows,
    collection_single_row,
    collection_update,
    collection_upsert,
    prepare_sync_timestamps as _prepare_sync_timestamps,
)
from vp_xero_integration.common.tables import (
    MAP_BANK_CODE_TABLE_NAME,
    MAP_BANK_CODE_COLUMNS,
    MAP_BANK_CODE_UNIQUE_COLUMNS,
    MAP_FIRM_TABLE_NAME,
    MAP_FIRM_COLUMNS,
    OUTSTANDING_PURCHASE_INVOICES_TABLE_NAME,
    OUTSTANDING_PURCHASE_INVOICES_COLUMNS,
    OUTSTANDING_EMPLOYEE_EXPENSES_TABLE_NAME,
    OUTSTANDING_EMPLOYEE_EXPENSES_COLUMNS,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Shared private helpers
# ---------------------------------------------------------------------------

def _unwrap_xero_result(task_id):
    """Unwrap a XeroOperator XCom to a single object.

    XeroOperators (search AND get_by_id) both return:
        {"count": N, "data": [...], "entity_type": "...", ...}
    This helper extracts data[0] so callers can use .get() directly.
    """
    result = rail.result(task_id) or {}
    if isinstance(result, dict) and 'data' in result:
        data = result.get('data') or []
        return data[0] if data else {}
    if isinstance(result, list):
        return result[0] if result else {}
    return result


def _find_period_for_date(payment_date_str, periods_list):
    """Return period identifier where AccountPdStart <= payment_date <= AccountPdEnd.

    Returns the period string value (e.g. '2026-03') or None when no period
    matches. Both comparison keys use string ISO-date comparison (YYYY-MM-DD
    prefix) which works because VP period dates are fixed-format ISO strings.
    """
    if not payment_date_str or not periods_list:
        return None
    date_prefix = payment_date_str[:10]
    for period in (periods_list or []):
        start = (period.get('AccountPdStart') or '')[:10]
        end = (period.get('AccountPdEnd') or '')[:10]
        if start and end and start <= date_prefix <= end:
            period_value = period.get('Period') or period.get('AccountPeriod')
            return str(period_value) if period_value else None
    return None


def _resolve_bank_code(xero_account_id, context=None):
    """Look up or lazy-populate `map_bank_code` for the given Xero AccountID.

    1. Try S3 collection hit (map_bank_code WHERE XeroID = xero_account_id).
    2. On miss: fetch Xero account by ID -> get Name.
    3. Fetch VP bank list (VantagepointSettingsBankOperator).
    4. Match VP bank by Description (case-insensitive, whitespace-normalised).
    5. Upsert result row into map_bank_code.
    6. Return {'VantagepointCode': code_or_blank, ...}.
    """
    context = context or rail.get_current_context()
    conf = context['dag_run'].conf or {}
    xero_conn_id = conf.get('connections', {}).get('xero', 'xero_default')
    vp_conn_id = conf.get('connections', {}).get('vantagepoint', 'vantagepoint_default')

    if not xero_account_id:
        logger.warning("[resolve_bank_code] empty xero_account_id — returning blank")
        return {'VantagepointCode': '', 'XeroID': '', 'XeroName': '', 'VantagepointName': ''}

    row = collection_single_row(
        f"SELECT {', '.join(MAP_BANK_CODE_COLUMNS)} FROM {MAP_BANK_CODE_TABLE_NAME} "
        f"WHERE XeroID = ?",
        [xero_account_id],
        context=context,
        read_task_id='_read_map_bank_code',
    )
    if row and row.get('VantagepointCode'):
        return row

    # Cache miss — resolve via Xero + VP APIs
    logger.info("[resolve_bank_code] cache miss for AccountID=%s — fetching from Xero+VP APIs", xero_account_id)
    xero_account_op = rail.XeroAccountOperator(
        task_id='_inline_xero_account_lookup',
        xero_conn_id=xero_conn_id,
        operation='get_by_id',
        record_id=xero_account_id,
    )
    xero_account_raw = xero_account_op.execute(context) or {}
    if isinstance(xero_account_raw, dict) and 'data' in xero_account_raw:
        data = xero_account_raw.get('data') or []
        xero_account = data[0] if data else {}
    else:
        xero_account = xero_account_raw
    xero_name = xero_account.get('Name', '')

    vp_bank_op = rail.VantagepointSettingsBankOperator(
        task_id='_inline_vp_bank_lookup',
        vp_conn_id=vp_conn_id,
        request_method='GET',
    )
    vp_banks = vp_bank_op.execute(context) or []
    if isinstance(vp_banks, dict):
        vp_banks = vp_banks.get('rows') or vp_banks.get('Body') or []

    vp_code = ''
    vp_name = ''
    company = ''
    org = ''
    account = ''
    normalized_xero_name = (xero_name or '').strip().casefold()
    for bank in vp_banks:
        if (bank.get('Description') or '').strip().casefold() == normalized_xero_name:
            vp_code = bank.get('Code') or ''
            vp_name = bank.get('Description') or ''
            company = bank.get('Company') or ''
            org = bank.get('Org') or ''
            account = bank.get('Account') or ''
            logger.info("[resolve_bank_code] matched VP bank '%s' -> Code=%s", vp_name, vp_code)
            break
    else:
        logger.warning(
            "[resolve_bank_code] no VP bank matched Xero name '%s' (AccountID=%s); "
            "candidates=%s",
            xero_name, xero_account_id, [b.get('Description') for b in vp_banks],
        )

    bank_row = {
        'VantagepointName': vp_name,
        'VantagepointCode': vp_code,
        'XeroName': xero_name,
        'XeroID': xero_account_id,
        'Status': xero_account.get('Status', 'Active'),
        'Company': company,
        'Org': org,
        'Account': account,
    }
    try:
        collection_upsert(
            MAP_BANK_CODE_TABLE_NAME,
            key_columns=MAP_BANK_CODE_UNIQUE_COLUMNS,
            data_columns=bank_row,
            context=context,
        )
    except Exception as exc:  # pylint: disable=broad-exception-caught
        logger.warning("Could not upsert map_bank_code row: %s", exc)

    return bank_row


def capture_processor_error(*args):
    """Sole leaf of every processor DAG (trigger_rule='one_failed').

    Returns None on the happy path (never reached when trigger_rule fires) and
    {'error': ...} when an upstream task fails. The FailOperator in the
    dispatcher reads these gathered dicts and surfaces them as one message.
    """
    context = rail.get_current_context()
    dag_run_conf = context.get('dag_run').conf or {}
    payment_id = dag_run_conf.get('PaymentID', 'unknown')
    try:
        failed_task_ids = get_failed_upstream_task_ids(context)
        raw_msg = get_error_message(context)
        if failed_task_ids:
            task_label = ', '.join(failed_task_ids)
            error_msg = f"[{task_label}] {raw_msg}" if raw_msg else f"[{task_label}] failed"
        else:
            error_msg = raw_msg or 'unknown error'
    except Exception as exc:  # pylint: disable=broad-exception-caught
        logger.warning(
            "Could not extract upstream error context for PaymentID=%s: %s",
            payment_id, exc, exc_info=True,
        )
        error_msg = 'unknown error'
    return {'error': f"PaymentID={payment_id} — {error_msg}"}


# ---------------------------------------------------------------------------
# Dispatcher callables
# ---------------------------------------------------------------------------

def prepare_sync_timestamps_method(instance, template, initial_sync_time):
    """Wrap common prepare_sync_timestamps, normalising current_sync_time to second precision.

    Strips sub-second component so both timestamps in the XCom use YYYY-MM-DDTHH:MM:SSZ,
    matching the format Xero's modified_since parameter expects.
    """
    result = _prepare_sync_timestamps(instance, template, initial_sync_time)
    if isinstance(result, dict) and result.get('current_sync_time'):
        result['current_sync_time'] = re.sub(r'\.\d+Z$', 'Z', result['current_sync_time'])
    return result


def update_last_sync_time_method(instance, template):
    """Write watermark in second-precision ISO format (no milliseconds).

    Reads current_sync_time from prepare_sync_timestamps XCom and strips any
    sub-second component before persisting — Xero's modified_since parameter
    requires YYYY-MM-DDTHH:MM:SSZ, not YYYY-MM-DDTHH:MM:SS.mmmZ.
    """
    timestamps = rail.result('prepare_sync_timestamps')
    if not isinstance(timestamps, dict) or not timestamps.get('current_sync_time'):
        logger.warning(
            "prepare_sync_timestamps missing current_sync_time — watermark not updated"
        )
        return None
    customer_id = rail.get_current_context()['dag_run'].conf.get('customerId')
    key = build_watermark_variable_key(template, instance, customer_id)
    current_time = timestamps['current_sync_time']
    Variable.set(key, current_time)
    logger.info("Updated last sync time Variable '%s' to: %s", key, current_time)
    return current_time


def prepare_payment_items_method():
    """Normalise poll result and filter to handled payment types.

    XeroPaymentOperator(operation='search') returns:
        {"count": N, "data": [...], "entity_type": "Payments", ...}

    Returns [{PaymentID, InvoiceID, PaymentType}] for ACCRECPAYMENT and
    ACCPAYPAYMENT only. TRANSFER, PREPAYMENT and any other types are silently
    skipped — matching Workato's no-else-branch behaviour.
    """
    xcom_value = rail.result('poll_xero_payments') or []
    if isinstance(xcom_value, dict):
        raw = (
            xcom_value.get('data')
            or xcom_value.get('Payments')
            or xcom_value.get('rows')
            or []
        )
    else:
        raw = xcom_value

    handled_types = {'ACCRECPAYMENT', 'ACCPAYPAYMENT'}
    return [
        {
            'PaymentID': p.get('PaymentID'),
            'InvoiceID': p.get('Invoice', {}).get('InvoiceID'),
            'InvoiceNumber': p.get('Invoice', {}).get('InvoiceNumber', ''),
            'XeroBankAccountID': (p.get('Account') or {}).get('AccountID', ''),
            'PaymentType': p.get('PaymentType'),
        }
        for p in raw
        if p.get('PaymentType') in handled_types
        and p.get('Invoice', {}).get('InvoiceID')
    ]


def build_payment_processor_conf(item):
    """Build dag_run.conf for either processor DAG from a dispatcher item.

    Spreads the dispatcher's full conf (connections, customer metadata) so the
    processor receives everything it needs, then overrides PaymentID/InvoiceID
    with this specific payment's values.
    """
    context = rail.get_current_context()
    base_conf = context['dag_run'].conf or {}
    return {
        **base_conf,
        'PaymentID': item.get('PaymentID'),
        'InvoiceID': item.get('InvoiceID'),
        'InvoiceNumber': item.get('InvoiceNumber', ''),
        'XeroBankAccountID': item.get('XeroBankAccountID', ''),
    }


# ---------------------------------------------------------------------------
# Invoice payment processor callables (ACCRECPAYMENT -> CR)
# ---------------------------------------------------------------------------

def _build_filter_hash(fields):
    """Build a VP filterHash query string from a list of (name, value) pairs.

    Produces: ?filterHash[0][name]=X&filterHash[0][value]=Y&filterHash[0][opp]==&...
    """
    params = []
    for i, (name, value) in enumerate(fields):
        params.append(f'filterHash[{i}][name]={name}')
        params.append(f'filterHash[{i}][value]={value}')
        params.append(f'filterHash[{i}][opp]==')
    return '?' + '&'.join(params)


def _parse_xero_date(xero_date_str):
    """Convert Xero /Date(epoch_ms+offset)/ format to YYYY-MM-DD string.

    Xero v2 API returns dates as /Date(1785309283510+0000)/. If the string is
    already ISO-formatted (YYYY-MM-DD...) it is returned unchanged (sliced to
    10 chars). Returns '' for blank/unrecognised input.
    """
    if not xero_date_str:
        return ''
    m = re.search(r'/Date\((\d+)', xero_date_str)
    if m:
        ts_ms = int(m.group(1))
        return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).strftime('%Y-%m-%d')
    return xero_date_str[:10]


def build_ar_psa_filter_method():
    """Build filterHash query string for VantagepointPsaledgerOperator TransType=in.

    InvoiceNumber format (VP-assigned): '{Invoice}.{Period}.{PostSeq}'
    e.g. '000000010808.202606.59'

    Uses InvoiceNumber from the dispatcher conf (populated from the poll result)
    where it is always present. Falls back to the fetch_xero_payment response
    in case the conf was not populated (should not happen in normal flow).
    The Xero InvoiceID (UUID) is never used as a VP filter value.
    """
    context = rail.get_current_context()
    conf = context['dag_run'].conf or {}
    xero_payment = _unwrap_xero_result('fetch_xero_payment')
    invoice = xero_payment.get('Invoice') or {}
    invoice_number = (
        conf.get('InvoiceNumber')
        or invoice.get('InvoiceNumber')
        or ''
    )
    parts = invoice_number.split('.')
    invoice_num = parts[0] if parts else ''
    period = parts[1] if len(parts) >= 2 else ''
    post_seq = parts[2] if len(parts) >= 3 else ''
    return _build_filter_hash([
        ('Period', period),
        ('Invoice', invoice_num),
        ('PostSeq', post_seq),
    ])


def build_cr_dedup_filter_method():
    """Build filterHash query string for duplicate-CR check: TransType=cr, Batch=PaymentID stripped."""
    context = rail.get_current_context()
    conf = context['dag_run'].conf or {}
    payment_id = conf.get('PaymentID', '')
    batch = payment_id.replace('-', '')[:32]
    return _build_filter_hash([('Batch', batch)])


def compute_weighted_lines_method():
    """Pro-rata weight PSA Ledger rows by absolute TxnAmt.

    Returns list of dicts with added keys: Weighted (payment portion), Balance.
    Workato steps 15-16 parity.
    """
    psa_rows = rail.result('fetch_vp_invoice_lines') or []
    if isinstance(psa_rows, dict):
        psa_rows = psa_rows.get('rows') or psa_rows.get('Body') or []
    xero_payment = _unwrap_xero_result('fetch_xero_payment')
    payment_amount = float(xero_payment.get('Amount') or 0)

    total_abs = sum(abs(float(r.get('TransactionAmount') or 0)) for r in psa_rows)
    if not total_abs:
        return []

    weighted_rows = []
    allocated = 0.0
    for index, row in enumerate(psa_rows):
        txn_amt = abs(float(row.get('TransactionAmount') or 0))
        if index == len(psa_rows) - 1:
            weighted = round(payment_amount - allocated, 2)
        else:
            weighted = round(payment_amount * (txn_amt / total_abs), 2)
        allocated = round(allocated + weighted, 2)
        balance = round(txn_amt - weighted, 2)
        weighted_rows.append({**row, 'Weighted': weighted, 'Balance': balance})
    return weighted_rows


def build_payment_vars_method():
    """Derive Batch, Company, RefNo, Description from payment + PSA data.

    Returns dict: {Batch, Company, RefNo, Description}.
    Workato step 18 parity.
    """
    context = rail.get_current_context()
    conf = context['dag_run'].conf or {}
    xero_payment = _unwrap_xero_result('fetch_xero_payment')
    psa_rows = rail.result('fetch_vp_invoice_lines') or []
    if isinstance(psa_rows, dict):
        psa_rows = psa_rows.get('rows') or psa_rows.get('Body') or []
    org_codes = rail.result('fetch_org_codes') or []
    if isinstance(org_codes, dict):
        org_codes = org_codes.get('rows') or org_codes.get('Body') or []

    payment_id = conf.get('PaymentID', '')
    batch = payment_id.replace('-', '')[:32]

    org = (psa_rows[0].get('Org') or '') if psa_rows else ''
    org_code_len = len(org_codes[0].get('Code', '')) if org_codes else 0
    company = org[:org_code_len] if org_code_len else ''

    invoice_ref = xero_payment.get('Reference') or ''
    invoice_num = (xero_payment.get('Invoice') or {}).get('InvoiceNumber') or ''
    ref_no = (invoice_ref or invoice_num)[:7]
    description = f"Payment {ref_no}"[:40]

    return {
        'Batch': batch,
        'Company': company,
        'RefNo': ref_no,
        'Description': description,
    }


def build_ar_account_filter():
    """Build ?company= query string for GET /AccountConfiguration/CFGAutoPosting.

    Mirrors QBO invoice_payment_sync build_ar_account_filter. Returns empty
    string when company is not yet known (safe — VP returns default record).
    """
    payment_vars = rail.result('build_payment_vars') or {}
    company = payment_vars.get('Company', '')
    if company:
        return f'?company={company}'
    return ''


def resolve_bank_code_method():
    """Resolve bank code for invoice payment (ACCRECPAYMENT).

    Reads Account.AccountID from the dispatcher conf (populated from poll result,
    always present). Falls back to fetch_xero_payment in case conf is missing it.
    Returns {VantagepointCode, ...}. Workato step 20 parity.
    """
    context = rail.get_current_context()
    conf = context['dag_run'].conf or {}
    xero_payment = _unwrap_xero_result('fetch_xero_payment')
    account_id = (
        conf.get('XeroBankAccountID')
        or (xero_payment.get('Account') or {}).get('AccountID')
        or ''
    )
    return _resolve_bank_code(account_id)


def find_period_for_payment_method(payment_task_id, periods_task_id):
    """Find VP accounting period containing the Xero payment date.

    payment_task_id: XCom task that returned the Xero payment object.
    periods_task_id: XCom task that returned the VP periods list.
    Returns the period string or None.
    """
    xero_payment = _unwrap_xero_result(payment_task_id)
    payment_date = _parse_xero_date(xero_payment.get('Date') or '')
    periods = rail.result(periods_task_id) or []
    if isinstance(periods, dict):
        periods = periods.get('rows') or periods.get('Body') or []
    return _find_period_for_date(payment_date, periods)


def find_payment_period_method():
    """Find VP period for the CR path payment. Workato steps 26-28 parity."""
    return find_period_for_payment_method('fetch_xero_payment', 'fetch_vp_periods')


def build_cr_body_method():
    """Assemble the full CR body (crMaster + crDetail[]) for VantagepointCashReceiptOperator.

    Workato step 33 parity. Returns the complete POST body dict.
    """
    xero_payment = _unwrap_xero_result('fetch_xero_payment')
    weighted_rows = rail.result('compute_weighted_lines') or []
    payment_vars = rail.result('build_payment_vars') or {}
    bank = rail.result('resolve_bank_code') or {}
    ar_account_raw = rail.result('fetch_ar_account') or {}
    period = rail.result('find_payment_period')

    batch = payment_vars.get('Batch', '')
    ref_no = payment_vars.get('RefNo', '')
    company = payment_vars.get('Company', '')
    bank_code = bank.get('VantagepointCode', '')

    invoice = xero_payment.get('Invoice') or {}
    payment_date = _parse_xero_date(xero_payment.get('Date') or '')
    payment_amount = float(xero_payment.get('Amount') or 0)
    reference = xero_payment.get('Reference') or ''
    currency_code = invoice.get('CurrencyCode') or 'USD'
    invoice_num = invoice.get('InvoiceNumber') or ''
    description = f"Payment on Invoice {invoice_num}"

    if isinstance(ar_account_raw, list):
        ar_account_raw = ar_account_raw[0] if ar_account_raw else {}
    elif isinstance(ar_account_raw, dict) and 'rows' in ar_account_raw:
        ar_account_raw = (ar_account_raw['rows'] or [{}])[0]
    ar_account = ar_account_raw.get('AcctsReceivable') or ar_account_raw.get('Account') or ''

    cr_detail = []
    for row in weighted_rows:
        cr_detail.append({
            'Batch': batch,
            'RefNo': ref_no,
            'PKey': uuid.uuid4().hex,
            'Description': 'Payment from Xero',
            'WBS1': row.get('WBS1', ''),
            'WBS2': row.get('WBS2', ''),
            'WBS3': row.get('WBS3', ''),
            'Org': row.get('Org', ''),
            'Account': ar_account,
            'Amount': row.get('Weighted', 0),
            'SourceAmount': row.get('Weighted', 0),
            'TaxCode': row.get('TaxCode', ''),
            'TaxBasis': row.get('TaxBasis', ''),
            'Interest': '0',
            'Retainer': '0',
            'CurrencyExchangeOverrideRate': '0',
            'SourceCurrencyCode': currency_code,
            'SourceBalance': row.get('Balance', 0),
            'SourceExchangeInfo': row.get('SourceExchangeInfo', ''),
            'LinkCompany': row.get('LinkCompany', ''),
            'PreInvoice': row.get('PreInvoice', ''),
            'Invoice': row.get('Invoice', ''),
        })

    return {
        'Batch': batch,
        'Description': description[:40],
        'Recurring': 'N',
        'Selected': 'N',
        'Posted': 'N',
        'Creator': 'Integration',
        'EndDate': payment_date,
        'Total': payment_amount,
        'DefaultBank': bank_code,
        'Period': period,
        'PostPeriod': '',
        'Company': company,
        'crMaster': [{
            'Batch': batch,
            'RefNo': ref_no,
            'Posted': 'N',
            'CurrencyExchangeOverrideMethod': 'N',
            'CurrencyExchangeOverrideRate': '0',
            'Status': 'A',
            'DiaryNo': '0',
            'TransDate': payment_date,
            'TransComment': reference or 'Payment',
            'BankCode': bank_code,
            'Seq': '1',
            'CurrencyCode': currency_code,
        }],
        'crDetail': cr_detail,
    }


def build_cr_post_trans_body_method():
    """Build the PostTransFile body for CR. Workato step 34 parity."""
    payment_vars = rail.result('build_payment_vars') or {}
    period = rail.result('find_payment_period')
    return {
        'parms': [{
            'batch': payment_vars.get('Batch', ''),
            'description': '',
            'period': period,
            'transtype': 'CR',
        }]
    }


# ---------------------------------------------------------------------------
# Bill payment processor callables — PP path (AP Voucher)
# ---------------------------------------------------------------------------

def build_outstanding_voucher_lines_method():
    """Attach Total_Payment_Amount=payment.Amount to each outstanding_purchase_invoices row.

    Workato step 9 parity. Returns list of enriched rows.
    """
    context = rail.get_current_context()
    conf = context['dag_run'].conf or {}
    invoice_id = conf.get('InvoiceID', '')
    s3_rows = collection_rows(
        OUTSTANDING_PURCHASE_INVOICES_TABLE_NAME,
        OUTSTANDING_PURCHASE_INVOICES_COLUMNS,
        'InvoiceID = ?',
        [invoice_id],
        context=context,
        read_task_id='_read_outstanding_purchase',
    )
    xero_payment = _unwrap_xero_result('fetch_xero_payment_for_pp')
    payment_amount = float(xero_payment.get('Amount') or 0)
    return [{**row, 'Total_Payment_Amount': payment_amount} for row in s3_rows]


def compute_pp_weighted_lines_method():
    """Pro-rata weight AP voucher outstanding amounts.

    Formula: Weighted_Payment = Total_Payment_Amount * (Outstanding / SUM(Outstanding))
             Balance = Outstanding - Weighted_Payment
    Workato step 11 parity.
    """
    voucher_lines = rail.result('build_outstanding_voucher_lines') or []
    total_outstanding = sum(float(r.get('OutstandingAmount') or 0) for r in voucher_lines)
    if not total_outstanding:
        return []
    payment_total = float((voucher_lines[0] if voucher_lines else {}).get('Total_Payment_Amount') or 0)
    result_rows = []
    allocated = 0.0
    for index, row in enumerate(voucher_lines):
        outstanding = float(row.get('OutstandingAmount') or 0)
        total_payment = float(row.get('Total_Payment_Amount') or 0)
        if index == len(voucher_lines) - 1:
            weighted = round(payment_total - allocated, 2)
        else:
            weighted = round(total_payment * (outstanding / total_outstanding), 2)
        allocated = round(allocated + weighted, 2)
        balance = round(outstanding - weighted, 2)
        result_rows.append({**row, 'Weighted_Payment': weighted, 'Balance': balance})
    return result_rows


def resolve_bank_code_ap_method():
    """Resolve bank code for AP Voucher payment path.

    Reads Account.AccountID from the Xero payment. Workato step 21 parity.
    """
    xero_payment = _unwrap_xero_result('fetch_xero_payment_for_pp')
    account = xero_payment.get('Account') or {}
    account_id = account.get('AccountID') or ''
    return _resolve_bank_code(account_id)


def lookup_firm_for_ap_method():
    """Search map_firm WHERE ContactID = payment contact. Workato step 26 parity."""
    context = rail.get_current_context()
    xero_payment = _unwrap_xero_result('fetch_xero_payment_for_pp')
    contact_id = (xero_payment.get('Contact') or {}).get('ContactID') or ''
    if not contact_id:
        xero_invoice = _unwrap_xero_result('fetch_xero_invoice_for_pp')
        contact_id = (xero_invoice.get('Contact') or {}).get('ContactID') or ''
    return collection_single_row(
        f"SELECT {', '.join(MAP_FIRM_COLUMNS)} FROM {MAP_FIRM_TABLE_NAME} WHERE ContactID = ?",
        [contact_id],
        context=context,
        read_task_id='_read_map_firm_ap',
    )


def fetch_full_ap_voucher_method():
    """Combine /apControl, /apMaster, and /apDetail calls into Workato-style AP Voucher dict.

    Workato's 'Get AP Voucher from Vantagepoint' returns a single nested object.
    RAIL's VantagepointApVoucherOperator with pagination=True only returns top-level
    apControl fields. This callable makes three inline API calls and merges the results:
      1. /DataEntry/apControl/{batch}  -> header fields (Batch, Total, Company, Period, ...)
      2. /DataEntry/apMaster/{batch}   -> list of master records (Invoice, InvoiceDate, ...)
      3. /DataEntry/apDetail/{batch} -> all detail lines (batch-scoped), embedded into each master
    """
    context = rail.get_current_context()
    conf = context['dag_run'].conf or {}
    vp_conn_id = conf.get('connections', {}).get('vantagepoint', 'vantagepoint_default')

    s3_rows = rail.result('lookup_outstanding_purchase') or []
    batch = (s3_rows[0] if isinstance(s3_rows, list) and s3_rows else {}).get('Batch', '')

    if not batch:
        logger.warning('[fetch_full_ap_voucher] no Batch in lookup_outstanding_purchase')
        return {}

    # 1. GET apControl header (top-level fields only)
    control_op = rail.VantagepointApVoucherOperator(
        task_id='_ap_control_inner',
        vp_conn_id=vp_conn_id,
        request_method='GET',
        batch=batch,
    )
    control_rows = control_op.execute(context) or []
    ap_control = (control_rows[0] if isinstance(control_rows, list) else control_rows) or {}

    # 2. GET apMaster rows for this batch
    master_op = rail.VantagepointApVoucherOperator(
        task_id='_ap_master_inner',
        vp_conn_id=vp_conn_id,
        request_method='GET',
        endpoint=f'/DataEntry/apMaster/{batch}',
    )
    ap_master_rows = master_op.execute(context) or []
    if not isinstance(ap_master_rows, list):
        ap_master_rows = [ap_master_rows] if ap_master_rows else []

    # 3. GET apDetail by batch (batch-scoped, same list for every master) and embed
    for master in ap_master_rows:
        detail_op = rail.VantagepointApVoucherOperator(
            task_id='_ap_detail_inner',
            vp_conn_id=vp_conn_id,
            request_method='GET',
            endpoint=f'/DataEntry/apDetail/{batch}',
        )
        detail_rows = detail_op.execute(context) or []
        if not isinstance(detail_rows, list):
            detail_rows = [detail_rows] if detail_rows else []
        master['apDetail'] = detail_rows

    return {**ap_control, 'apMaster': ap_master_rows}


def build_pp_payload_method():
    """Build APPPCHECKS payload and track outstanding balance updates.

    FOREACH weighted row: build one APPPCHECKS entry; track balance>0 -> UPDATE,
    balance<=0 -> FullyPaid=True. Workato steps 28-35 parity.

    Returns {APPPCHECKS, fully_paid, rows_to_update}.
    """
    weighted_rows = rail.result('compute_pp_weighted_lines') or []
    ap_voucher = rail.result('fetch_vp_ap_voucher') or {}
    if isinstance(ap_voucher, list):
        ap_voucher = (ap_voucher or [{}])[0]
    elif isinstance(ap_voucher, dict) and 'rows' in ap_voucher:
        ap_voucher = (ap_voucher['rows'] or [{}])[0]
    # apMaster[0]: each Xero bill maps to one VP batch → one apMaster. Workato recipe also uses
    # apMaster.current_item (positional), which resolves to [0] for all rows in the single-master case.
    ap_master = (ap_voucher.get('apMaster') or [{}])[0] if isinstance(ap_voucher, dict) else {}
    vp_firm = rail.result('fetch_vp_firm_for_ap') or {}
    if isinstance(vp_firm, list):
        vp_firm = (vp_firm or [{}])[0]
    elif isinstance(vp_firm, dict) and 'rows' in vp_firm:
        vp_firm = (vp_firm['rows'] or [{}])[0]
    bank = rail.result('resolve_bank_code_ap') or {}
    period = rail.result('find_period_ap')

    bank_code = bank.get('VantagepointCode', '')
    vendor = vp_firm.get('Vendor') or vp_firm.get('VendorCode') or ''

    xero_payment = _unwrap_xero_result('fetch_xero_payment_for_pp')
    txn_date = _parse_xero_date(xero_payment.get('Date') or '')

    base_check_no = int(time.time())
    checks = []
    rows_to_update = []
    fully_paid = bool(weighted_rows)

    period_str = f'{period}.0' if period and '.' not in str(period) else str(period or '')

    for seq, row in enumerate(weighted_rows):
        check_no = str(base_check_no + seq)
        weighted = row.get('Weighted_Payment', 0)
        checks.append({
            'Period': period_str,
            'Vendor': vendor,
            'Voucher': row.get('Voucher', ''),
            'Invoice': ap_master.get('Invoice') or ap_master.get('InvoiceNo') or '',
            'InvoiceDate': ap_master.get('InvoiceDate') or '',
            'WBS1': row.get('WBS1', ''),
            'WBS2': row.get('WBS2', ''),
            'WBS3': row.get('WBS3', ''),
            'Account': row.get('Account', ''),
            'Org': row.get('Org', ''),
            'Amount': weighted,
            'Payment': weighted,
            'PrevPay': '0',
            'BankCode': bank_code,
            'LiabCode': ap_master.get('LiabCode') or '',
            'PayTerms': ap_master.get('PayTerms') or '',
            'CheckNo': check_no,
            'CheckNoRef': check_no,
            'CheckDate': txn_date,
            'Seq': str(seq),
        })

        balance = row.get('Balance', 0)
        if balance > 0:
            fully_paid = False
            rows_to_update.append({'rowid': row.get('_rowid'), 'balance': balance, 'delete': False})
        else:
            rows_to_update.append({'rowid': row.get('_rowid'), 'balance': 0, 'delete': True})

    return {'APPPCHECKS': checks, 'fully_paid': fully_paid, 'rows_to_update': rows_to_update}


def build_pp_request_body_method():
    """Build the vendor_payment POST body. Workato step 36 parity."""
    pp_payload = rail.result('build_pp_payload') or {}
    xero_payment = _unwrap_xero_result('fetch_xero_payment_for_pp')
    ap_voucher = rail.result('fetch_vp_ap_voucher') or {}
    if isinstance(ap_voucher, list):
        ap_voucher = (ap_voucher or [{}])[0]
    elif isinstance(ap_voucher, dict) and 'rows' in ap_voucher:
        ap_voucher = (ap_voucher['rows'] or [{}])[0]
    period = rail.result('find_period_ap')
    period_str = f'{period}.0' if period and '.' not in str(period) else str(period or '')
    txn_date = _parse_xero_date(xero_payment.get('Date') or '')
    company = ap_voucher.get('Company') or ''

    return {
        'TransType': 'PP',
        'Period': period_str,
        'PaymentDate': txn_date,
        'CheckDate': txn_date,
        'Company': company,
        'PostSeq': '1',
        'APPPCHECKS': pp_payload.get('APPPCHECKS', []),
    }


def update_outstanding_ap_method():
    """Update S3 outstanding_purchase_invoices balances after successful VP post.

    Workato steps 37-38 parity: balance>0 -> UPDATE, balance<=0 -> DELETE row.
    """
    context = rail.get_current_context()
    conf = context['dag_run'].conf or {}
    invoice_id = conf.get('InvoiceID', '')
    pp_payload = rail.result('build_pp_payload') or {}
    rows_to_update = pp_payload.get('rows_to_update') or []

    for row_info in rows_to_update:
        rowid = row_info.get('rowid')
        if not rowid:
            continue
        if row_info.get('delete'):
            collection_update(
                OUTSTANDING_PURCHASE_INVOICES_TABLE_NAME,
                f"DELETE FROM {OUTSTANDING_PURCHASE_INVOICES_TABLE_NAME} WHERE rowid = ?",
                [rowid],
                context=context,
            )
        else:
            collection_update(
                OUTSTANDING_PURCHASE_INVOICES_TABLE_NAME,
                f"UPDATE {OUTSTANDING_PURCHASE_INVOICES_TABLE_NAME} "
                f"SET OutstandingAmount = ? WHERE rowid = ?",
                [row_info['balance'], rowid],
                context=context,
            )
    return {'updated': len(rows_to_update), 'invoice_id': invoice_id}


def delete_fully_paid_purchase_invoices_method():
    """DELETE all outstanding_purchase_invoices WHERE InvoiceID if fully paid.

    Workato step 39 parity.
    """
    context = rail.get_current_context()
    conf = context['dag_run'].conf or {}
    invoice_id = conf.get('InvoiceID', '')
    pp_payload = rail.result('build_pp_payload') or {}
    if not pp_payload.get('fully_paid'):
        return None
    collection_update(
        OUTSTANDING_PURCHASE_INVOICES_TABLE_NAME,
        f"DELETE FROM {OUTSTANDING_PURCHASE_INVOICES_TABLE_NAME} WHERE InvoiceID = ?",
        [invoice_id],
        context=context,
    )
    return {'deleted_invoice_id': invoice_id}


# ---------------------------------------------------------------------------
# Bill payment processor callables — EP path (Expense)
# ---------------------------------------------------------------------------

def compute_total_bill_payments_method():
    """SUM Xero invoice Payments[].Amount.

    Partial-payment guard is at is_not_fully_paid_ep (reads build_ep_payload's
    fully_paid). Workato steps 46-49 parity.

    Returns {TotalPayments}.
    """
    xero_invoice = _unwrap_xero_result('fetch_xero_invoice_for_ep')
    payments = xero_invoice.get('Payments') or []
    total_payments = sum(float(p.get('Amount') or 0) for p in payments)
    return {'TotalPayments': total_payments}


def build_outstanding_expense_lines_method():
    """Attach Total_Payment_Amount=TotalPayments to each outstanding_employee_expenses row.

    Workato steps 50-51 parity. Returns list of enriched rows.
    """
    context = rail.get_current_context()
    conf = context['dag_run'].conf or {}
    invoice_id = conf.get('InvoiceID', '')
    s3_rows = collection_rows(
        OUTSTANDING_EMPLOYEE_EXPENSES_TABLE_NAME,
        OUTSTANDING_EMPLOYEE_EXPENSES_COLUMNS,
        'InvoiceID = ?',
        [invoice_id],
        context=context,
        read_task_id='_read_outstanding_expenses',
    )
    bill_payments = rail.result('compute_total_bill_payments') or {}
    total_payments = bill_payments.get('TotalPayments', 0)
    return [{**row, 'Total_Payment_Amount': total_payments} for row in s3_rows]


def compute_ep_weighted_lines_method():
    """Pro-rata weight expense outstanding amounts (includes Period, Employee).

    Formula: Weighted_Payment = Total_Payment_Amount * (Outstanding / SUM(Outstanding))
             Balance = Outstanding - Weighted_Payment
    Workato step 52 parity.
    """
    expense_lines = rail.result('build_outstanding_expense_lines') or []
    total_outstanding = sum(float(r.get('OutstandingAmount') or 0) for r in expense_lines)
    if not total_outstanding:
        return []
    payment_total = float((expense_lines[0] if expense_lines else {}).get('Total_Payment_Amount') or 0)
    result_rows = []
    allocated = 0.0
    for index, row in enumerate(expense_lines):
        outstanding = float(row.get('OutstandingAmount') or 0)
        total_payment = float(row.get('Total_Payment_Amount') or 0)
        if index == len(expense_lines) - 1:
            weighted = round(payment_total - allocated, 2)
        else:
            weighted = round(total_payment * (outstanding / total_outstanding), 2)
        allocated = round(allocated + weighted, 2)
        balance = round(outstanding - weighted, 2)
        result_rows.append({**row, 'Weighted_Payment': weighted, 'Balance': balance})
    return result_rows


def resolve_bank_code_ep_method():
    """Resolve bank code for expense payment path.

    On bank miss: returns {VantagepointCode:'', bank_error:True} — no IfOperator
    here; the CompoundError is checked at has_compound_errors_ep (step 81 parity).
    Workato steps 63-65 parity.
    """
    xero_payment = _unwrap_xero_result('fetch_xero_payment_for_ep')
    account = xero_payment.get('Account') or {}
    account_id = account.get('AccountID') or ''
    resolved = _resolve_bank_code(account_id)
    if not resolved.get('VantagepointCode'):
        logger.warning(
            "EP bank code not resolved for AccountID=%s — setting bank_error=True",
            account_id
        )
        return {**resolved, 'VantagepointCode': '', 'bank_error': True}
    return {**resolved, 'bank_error': False}


def lookup_firm_for_ep_method():
    """Search map_firm WHERE ContactID = payment contact. Workato step 67 parity."""
    context = rail.get_current_context()
    xero_payment = _unwrap_xero_result('fetch_xero_payment_for_ep')
    contact_id = (xero_payment.get('Contact') or {}).get('ContactID') or ''
    if not contact_id:
        xero_invoice = _unwrap_xero_result('fetch_xero_invoice_for_ep')
        contact_id = (xero_invoice.get('Contact') or {}).get('ContactID') or ''
    return collection_single_row(
        f"SELECT {', '.join(MAP_FIRM_COLUMNS)} FROM {MAP_FIRM_TABLE_NAME} WHERE ContactID = ?",
        [contact_id],
        context=context,
        read_task_id='_read_map_firm_ep',
    )


def build_ep_payload_method():
    """Build EXCHECKS payload and track outstanding balance updates.

    FOREACH weighted row: build one EXCHECKS entry; track balance>0 -> UPDATE,
    balance<=0 -> FullyPaid=True. Workato steps 68-75 parity.

    Returns {EXChecks, fully_paid, rows_to_update, bank_error}.
    """
    weighted_rows = rail.result('compute_ep_weighted_lines') or []
    vp_employee = rail.result('fetch_vp_employee') or {}
    if isinstance(vp_employee, list):
        vp_employee = (vp_employee or [{}])[0]
    elif isinstance(vp_employee, dict) and 'rows' in vp_employee:
        vp_employee = (vp_employee['rows'] or [{}])[0]
    bank = rail.result('resolve_bank_code_ep') or {}

    bank_code = bank.get('VantagepointCode', '')
    bank_error = bank.get('bank_error', False)
    employee_org = vp_employee.get('Org') or vp_employee.get('HomeOrg') or ''
    employee_company = vp_employee.get('HomeCompany') or vp_employee.get('Company') or ''

    xero_payment = _unwrap_xero_result('fetch_xero_payment_for_ep')
    txn_date = _parse_xero_date(xero_payment.get('Date') or '')

    check_no = str(int(time.time()))
    checks = []
    rows_to_update = []
    fully_paid = bool(weighted_rows)

    for seq, row in enumerate(weighted_rows):
        checks.append({
            'Voucher': row.get('Voucher', ''),
            'Org': employee_org,
            'Period': row.get('Period', ''),
            'Employee': row.get('Employee', ''),
            'Amount': row.get('Weighted_Payment', 0),
            'Seq': str(seq + 1),
            'Company': employee_company,
            'CheckNo': check_no,
            'CheckNoRef': check_no,
            'BankCode': bank_code,
            'CheckDate': txn_date,
        })

        balance = row.get('Balance', 0)
        if balance > 0:
            fully_paid = False
            rows_to_update.append({'rowid': row.get('_rowid'), 'balance': balance, 'delete': False})
        else:
            rows_to_update.append({'rowid': row.get('_rowid'), 'balance': 0, 'delete': True})

    return {
        'EXChecks': checks,
        'fully_paid': fully_paid,
        'rows_to_update': rows_to_update,
        'bank_error': bank_error,
    }


def is_not_fully_paid_ep_method():
    """Return True if expense payment is NOT fully paid (drives IfOperator graceful stop).

    Workato step 76 parity: IfOperator -> YES means partial, stops gracefully.
    """
    ep_payload = rail.result('build_ep_payload') or {}
    return not ep_payload.get('fully_paid', False)


def build_grouped_ep_payments_method():
    """GROUP BY Period/Employee/Voucher/Org/Company/CheckNo and SUM(Amount).

    Workato steps 79-80 parity. Returns grouped list.
    """
    ep_payload = rail.result('build_ep_payload') or {}
    checks = ep_payload.get('EXChecks') or []
    grouped = {}
    for check in checks:
        key = (
            check.get('Period', ''),
            check.get('Employee', ''),
            check.get('Voucher', ''),
            check.get('Org', ''),
            check.get('Company', ''),
            check.get('CheckNo', ''),
        )
        if key not in grouped:
            grouped[key] = {**check, 'Amount': 0}
        grouped[key]['Amount'] = round(grouped[key]['Amount'] + float(check.get('Amount') or 0), 2)
    return list(grouped.values())


def has_compound_errors_ep_method():
    """Return True if bank code resolution failed (CompoundError gate).

    Workato step 81 parity: stops gracefully when bank was not found.
    """
    ep_payload = rail.result('build_ep_payload') or {}
    return bool(ep_payload.get('bank_error', False))


def build_ep_request_body_method():
    """Build the expense_payment POST body. Workato step 84 parity."""
    bill_payments = rail.result('compute_total_bill_payments') or {}
    grouped_rows = rail.result('build_grouped_ep_payments') or []
    vp_employee = rail.result('fetch_vp_employee') or {}
    if isinstance(vp_employee, list):
        vp_employee = (vp_employee or [{}])[0]
    elif isinstance(vp_employee, dict) and 'rows' in vp_employee:
        vp_employee = (vp_employee['rows'] or [{}])[0]
    period = grouped_rows[0].get('Period', '') if grouped_rows else (rail.result('find_period_ep') or '')
    xero_payment = _unwrap_xero_result('fetch_xero_payment_for_ep')
    txn_date = _parse_xero_date(xero_payment.get('Date') or '')
    company = vp_employee.get('HomeCompany') or ''
    total_payments = bill_payments.get('TotalPayments', 0)

    # Amount/CheckAmt = full Xero TotalPayments per entry (VP EP contract, Workato parity)
    ex_checks = []
    for seq, row in enumerate(grouped_rows, start=1):
        ex_checks.append({
            'DetailType': 'E',
            'Period': row.get('Period', ''),
            'Employee': row.get('Employee', ''),
            'Voucher': row.get('Voucher', ''),
            'BankCode': row.get('BankCode', ''),
            'Org': row.get('Org', ''),
            'Amount': total_payments,
            'CheckAmt': total_payments,
            'CheckDate': txn_date,
            'ReportDate': txn_date,
            'Seq': str(seq),
            'CheckNo': row.get('CheckNo', ''),
            'CheckNoRef': row.get('CheckNo', ''),
        })

    return {
        'TransType': 'EP',
        'Period': period,
        'PaymentDate': txn_date,
        'CheckDate': txn_date,
        'Company': company,
        'PostSeq': '1',
        'PostDate': '',
        'EXCHECKS': ex_checks,
    }


def update_outstanding_ep_method():
    """Update S3 outstanding_employee_expenses balances after successful VP post.

    Workato step 84 post parity: balance>0 -> UPDATE Outstanding_Amount.
    """
    context = rail.get_current_context()
    ep_payload = rail.result('build_ep_payload') or {}
    rows_to_update = ep_payload.get('rows_to_update') or []

    for row_info in rows_to_update:
        rowid = row_info.get('rowid')
        if not rowid:
            continue
        if not row_info.get('delete'):
            collection_update(
                OUTSTANDING_EMPLOYEE_EXPENSES_TABLE_NAME,
                f"UPDATE {OUTSTANDING_EMPLOYEE_EXPENSES_TABLE_NAME} "
                f"SET OutstandingAmount = ? WHERE rowid = ?",
                [row_info['balance'], rowid],
                context=context,
            )
    return {'updated': len([r for r in rows_to_update if not r.get('delete')])}


def delete_fully_paid_expense_entries_method():
    """DELETE all outstanding_employee_expenses WHERE InvoiceID.

    Workato steps 85-87 parity.
    """
    context = rail.get_current_context()
    conf = context['dag_run'].conf or {}
    invoice_id = conf.get('InvoiceID', '')
    ep_payload = rail.result('build_ep_payload') or {}
    if not ep_payload.get('fully_paid'):
        return None
    collection_update(
        OUTSTANDING_EMPLOYEE_EXPENSES_TABLE_NAME,
        f"DELETE FROM {OUTSTANDING_EMPLOYEE_EXPENSES_TABLE_NAME} WHERE InvoiceID = ?",
        [invoice_id],
        context=context,
    )
    return {'deleted_invoice_id': invoice_id}
