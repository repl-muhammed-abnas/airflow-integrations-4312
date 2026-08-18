"""
Common utility methods for QBO -> VP Bill Payment Sync.

Translates the Workato recipe chain (poll -> process -> post) into Python
callables for the 3-DAG Airflow template (main -> dispatcher ->
bill_payment_create). Error reporting goes to middleware via
PostDagRunDetailsToMiddlewareApiOperator + FailOperator on the dispatcher's
failure branch, matching invoice_payment_sync / vendor_sync.

Lookup tables (bank_code_map, map_firm, outstanding_purchase_invoices,
outstanding_employee_expenses, pay_terms) are the S3 collections created and
populated by the `mapping_sync` integration. They are read/written here under
the FIXED `mapping_sync` integration_type partition (see config.py
MAPPING_COLLECTION_INTEGRATION_TYPE) so this integration hits the same
collections.db mapping_sync owns. Reads go through a read-only collection
open; writes go through rail.S3UpdateCollectionOperator (the canonical lock
surface), never raw artifact mutation.
"""
# pylint: disable=invalid-name,broad-exception-caught,too-many-locals,import-error
import logging
import json
import re
import time
from datetime import datetime, timedelta, timezone
from airflow.models import Variable
import rail
from vp_quickbooks_integration.bill_payment_sync.config import (
    initial_sync_time,
    payment_lookback_minutes,
    MAPPING_COLLECTION_INTEGRATION_TYPE,
)
# Reuse mapping_sync's locator + table-name / column constants so the S3 path
# and the SQLite identifiers can never drift from what mapping_sync created.
from vp_quickbooks_integration.mapping_sync.config import IntegrationConfig
from vp_quickbooks_integration.common.python_callable_method import (
    watermark_key_template,
)
from vp_quickbooks_integration.common.tables import (
    BANK_CODE_MAP_TABLE_NAME,
    OUTSTANDING_PURCHASE_INVOICES_TABLE_NAME,
    OUTSTANDING_PURCHASE_INVOICES_COLUMNS,
    OUTSTANDING_EMPLOYEE_EXPENSES_TABLE_NAME,
    OUTSTANDING_EMPLOYEE_EXPENSES_COLUMNS,
)


logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Shared mapping_sync collection access (fixed 'mapping_sync' partition)
# ---------------------------------------------------------------------------
def _collection_integration(context):
    """The (integration, customer, integration_type) triple that locates the
    mapping_sync-owned collections for the current tenant. integration_type is
    pinned to 'mapping_sync' regardless of this integration's own conf
    integrationType, so we read/write the db mapping_sync populated.
    """
    return (
        IntegrationConfig.S3_INTEGRATION_NAME,
        IntegrationConfig.get_s3_customer(context),
        MAPPING_COLLECTION_INTEGRATION_TYPE,
    )


def _collection_single_row(query, params, context=None):
    """Run a read query returning 0 or 1 rows through S3QueryCollectionOperator
    (single-row mode — the canonical read surface; reads skip the S3 lock).
    Returns the row (dict) or None. A missing collection / table is treated as
    "no row" (None), matching mapping_sync's count_collection_rows /
    check_step_status helpers.
    """
    context = context or rail.get_current_context()
    integration, customer, integration_type = _collection_integration(context)
    op = rail.S3QueryCollectionOperator(
        task_id='_read_bill_payment_collection',
        query=query,
        query_params=params,
        integration=integration,
        customer=customer,
        integration_type=integration_type,
        mode='single-row',
    )
    try:
        return op.execute(context)
    except FileNotFoundError:
        logger.warning("Mapping collection not found yet for this tenant.")
        return None
    except Exception as exc:  # pylint: disable=broad-exception-caught
        if 'no such table' in str(exc).lower():
            logger.warning("Collection table missing: %s", exc)
            return None
        raise


def _collection_rows(table, columns, where_sql, params, context=None):
    """Read multiple rows from a mapping_sync collection through a SINGLE
    S3QueryCollectionOperator call.

    S3QueryCollectionOperator only returns rows in 'single-row' mode (its
    'dataset' mode writes a derived table and returns a name, not rows), so we
    pack every matching row into one JSON array via
    json_group_array(json_object(...)) and unpack it in Python. This keeps the
    read on the canonical operator surface (no raw download + sqlite) while
    still returning a list. Column names come from common.tables
    constants so identifiers can't drift. Each row includes its sqlite rowid as
    '_rowid' for downstream write-back. Returns list[dict] ([] if the
    collection/table is missing or nothing matches).
    """
    context = context or rail.get_current_context()
    pairs = "'_rowid', rowid, " + ", ".join(f"'{c}', {c}" for c in columns)
    query = (
        f"SELECT json_group_array(json_object({pairs})) AS rows "
        f"FROM {table} WHERE {where_sql}"
    )
    row = _collection_single_row(query, params, context)
    if not row:
        return []
    raw = row.get('rows') if isinstance(row, dict) else (row[0] if row else None)
    if not raw:
        return []
    try:
        return json.loads(raw) or []
    except (TypeError, ValueError):
        logger.warning("Could not parse collection rows JSON for %s", table)
        return []


def _collection_update(collection_name, query, params, context=None):
    """Run an INSERT/UPDATE/DELETE against the mapping_sync collection via
    S3UpdateCollectionOperator (the canonical lock surface). Returns the
    operator result dict.
    """
    context = context or rail.get_current_context()
    integration, customer, integration_type = _collection_integration(context)
    op = rail.S3UpdateCollectionOperator(
        task_id=f'_update_{collection_name}',
        integration=integration,
        customer=customer,
        integration_type=integration_type,
        collection_name=collection_name,
        query=query,
        query_params=params,
    )
    return op.execute(context)


# ---------------------------------------------------------------------------
# Watermark helpers
# ---------------------------------------------------------------------------
WATERMARK_KEY_TEMPLATE = watermark_key_template('bill_payment_sync')


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
    """Parse a validated ISO-8601 UTC timestamp into an aware datetime
    (Python 3.9 safe). _validate_qbo_timestamp guarantees the input matches
    YYYY-MM-DDTHH:MM:SS[.f][Z|+HH:MM], so we strip optional fractional seconds
    and the Z suffix before strptime.
    """
    clean = re.sub(r'\.\d+', '', ts).rstrip('Z')
    return datetime.strptime(
        clean, '%Y-%m-%dT%H:%M:%S'
    ).replace(tzinfo=timezone.utc)


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

    The stored watermark advances to current_sync_time on success, but the QBO
    query lower bound is (last_sync_time - payment_lookback_minutes) so
    payments at a window boundary or under QBO LastUpdatedTime clock skew are
    retried on the next poll. Mirrors Workato since_offset: -1800.
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

    query_since_dt = _parse_iso_utc(last_sync_time) - timedelta(
        minutes=payment_lookback_minutes
    )
    query_since = query_since_dt.strftime('%Y-%m-%dT%H:%M:%S') + '.000Z'

    logger.info(
        "Bill payment sync window: query [%s, %s) "
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
    logger.info("Advanced bill payment watermark '%s' to: %s", key, current)
    return current


# ---------------------------------------------------------------------------
# Disabled-flag check
# ---------------------------------------------------------------------------
def is_integration_enabled_method(instance):
    """
    True unless CFG_DisableBillPaymentIntegration is set to 'true'.
    Checks per-tenant key first, then instance-level kill switch.
    """
    customer_id = _customer_id_from_conf() or 'default'
    tenant_key = (
        f'CFG_DisableBillPaymentIntegration_{customer_id}_{instance}'
    )
    instance_key = f'CFG_DisableBillPaymentIntegration_{instance}'

    tenant_flag = Variable.get(tenant_key, default_var=None)
    if tenant_flag is not None and str(tenant_flag).strip().lower() == 'true':
        logger.info(
            "Bill payment integration disabled for tenant '%s' on "
            "instance '%s' via %s", customer_id, instance, tenant_key
        )
        return False

    instance_flag = Variable.get(instance_key, default_var='false')
    if str(instance_flag).strip().lower() == 'true':
        logger.info(
            "Bill payment integration disabled for entire instance '%s' "
            "via %s", instance, instance_key
        )
        return False

    return True


# ---------------------------------------------------------------------------
# Bill payment list extraction
# ---------------------------------------------------------------------------
def _bill_payment_items(raw):
    """
    Flatten a single raw QBO BillPayment into one work item per linked bill.

    The Workato poll recipe iterates over BillPayment.Line and calls the
    orchestration sub-recipe once per line with that line's
    LinkedTxn.first.TxnId as the InvoiceID (the QBO Bill Id). We mirror that:
    one worker item per (PaymentID, BillID) pair.
    """
    payment_id = str(raw.get('Id') or '')
    vendor = str((raw.get('VendorRef') or {}).get('value') or '')
    bank_acct = str(
        ((raw.get('CheckPayment') or {}).get('BankAccountRef') or {})
        .get('value') or ''
    )
    txn_date = raw.get('TxnDate') or ''
    total = float(raw.get('TotalAmt') or 0)

    items = []
    for line in (raw.get('Line') or []):
        linked = line.get('LinkedTxn') or []
        if not linked:
            continue
        bill_id = str(linked[0].get('TxnId') or '')
        if not bill_id:
            continue
        items.append({
            'PaymentID': payment_id,
            'BillID': bill_id,
            'VendorRef': vendor,
            'BankAccountRef': bank_acct,
            'TxnDate': txn_date,
            'TotalAmt': total,
            'LineAmount': float(line.get('Amount') or 0),
        })
    return items


def extract_and_filter_bill_payments_method():
    """
    Extract bill payment work items from the QuickBooksBillPaymentOperator response
    and apply filters.

    Mirrors Workato poll recipe: skip zero-amount payments and any line with
    no linked bill (the worker would have nothing to match in Vantagepoint).
    """
    result = rail.result('get_recently_changed_bill_payments')

    # QuickBooksBillPaymentOperator returns the normalised
    # {success, entity_type, data, count} shape.
    if isinstance(result, dict) and not result.get('success', True):
        logger.warning(
            "QuickBooks bill payment query failed: %s", result.get('error')
        )
        return []

    if isinstance(result, dict):
        raw_list = result.get('data') or []
    elif isinstance(result, list):
        raw_list = result
    else:
        raw_list = []

    items = []
    skipped_zero = 0
    skipped_no_bill = 0
    for raw in raw_list:
        if not raw:
            continue
        if float(raw.get('TotalAmt') or 0) == 0:
            skipped_zero += 1
            continue
        produced = _bill_payment_items(raw)
        if not produced:
            skipped_no_bill += 1
            continue
        items.extend(produced)

    logger.info(
        "Bill payments: %d work item(s) to process, %d skipped (zero-amount), "
        "%d skipped (no linked bill)",
        len(items), skipped_zero, skipped_no_bill
    )
    return items


# ===========================================================================
# Worker DAG helpers
# ===========================================================================

def _get_conf():
    return rail.get_current_context()['dag_run'].conf or {}


def _filter_none(body):
    """Drop keys whose value is None. Empty strings are KEPT (recipe parity)."""
    return {k: v for k, v in body.items() if v is not None}


# ---------------------------------------------------------------------------
# Collection lookups that decide the PP vs EP branch
# ---------------------------------------------------------------------------
def lookup_outstanding_purchase_method():
    """Read the outstanding AP voucher lines for this bill from the
    `outstanding_purchase_invoices` collection (keyed by InvoiceID = QBO Bill
    Id). Returns list[dict] including the sqlite rowid for later write-back.
    Mirrors Workato recipe step 4 (lookup by col10 Invoice ID).
    """
    bill_id = str(_get_conf().get('BillID') or '')
    rows = _collection_rows(
        OUTSTANDING_PURCHASE_INVOICES_TABLE_NAME,
        OUTSTANDING_PURCHASE_INVOICES_COLUMNS,
        "InvoiceID = ?",
        [bill_id],
    )
    logger.info(
        "outstanding_purchase_invoices rows for BillID=%s: %d",
        bill_id, len(rows)
    )
    return rows


def lookup_outstanding_expense_method():
    """Read the outstanding employee-expense voucher lines for this bill from
    the `outstanding_employee_expenses` collection (keyed by InvoiceID = QBO
    Bill Id). Mirrors Workato recipe step 36 (lookup by col8 Invoice ID).
    """
    bill_id = str(_get_conf().get('BillID') or '')
    rows = _collection_rows(
        OUTSTANDING_EMPLOYEE_EXPENSES_TABLE_NAME,
        OUTSTANDING_EMPLOYEE_EXPENSES_COLUMNS,
        "InvoiceID = ?",
        [bill_id],
    )
    logger.info(
        "outstanding_employee_expenses rows for BillID=%s: %d",
        bill_id, len(rows)
    )
    return rows


def is_bill_fully_paid_method():
    """EP branch gate. Mirrors Workato recipe step 42 (Balance == 0). The
    employee-expense path only supports fully-paid bills; partial payments are
    explicitly unsupported in the recipe.
    """
    res = rail.result('fetch_qbo_bill') or {}
    # QuickBooksBillOperator(get_bill) normalises to {success, entity_type, data, count}.
    if isinstance(res, dict):
        bills = res.get('data') or (res.get('QueryResponse') or {}).get('Bill') or []
    elif isinstance(res, list):
        bills = res
    else:
        bills = []
    bill = bills[0] if bills else {}
    return float(bill.get('Balance') or 0) == 0


# ---------------------------------------------------------------------------
# Bank code resolution (read bank_code_map; insert-on-miss stub)
# ---------------------------------------------------------------------------
def _fetch_qbo_account_name(context, conf, account_id):
    """Best-effort QBO account display name for an insert-on-miss bank_code_map
    stub. Returns '' on any failure (the stub is still inserted with QBOID).
    """
    try:
        conn_id = (conf.get('connections') or {}).get('intuit')
        op = rail.QuickBooksAccountOperator(
            task_id='_fetch_qbo_account',
            intuit_conn_id=conn_id,
            operation='search_account',
            query=f"SELECT * FROM Account WHERE Id = '{account_id}'",
        )
        res = op.execute(context) or {}
        # QuickBooksAccountOperator normalises to {success, entity_type, data, count}.
        accounts = res.get('data') or []
        return str((accounts[0] if accounts else {}).get('Name') or '')
    except Exception:
        logger.warning(
            "Could not fetch QBO account name for %s", account_id,
            exc_info=True
        )
        return ''


def resolve_bank_code_method(instance):
    """
    Resolve the Vantagepoint bank code for the QBO payment's bank account from
    the `bank_code_map` collection (keyed by QBOID = BankAccountRef).

    Mirrors Workato `Resolve Bank Code` sub-recipe:
    - If a mapped row exists (VantagepointCode non-empty) -> return it.
    - If no row exists -> fetch the QBO account name and INSERT an unmapped
      stub row (so an admin can fill in the VP code), then return {} so the
      caller fails with "bank not matched".
    - If a row exists but is unmapped -> return {}.

    Returns dict {Vantagepoint_Code, Company, Org, Account} or {} (unresolved).
    """
    context = rail.get_current_context()
    conf = context['dag_run'].conf or {}
    qbo_account_id = str(conf.get('BankAccountRef') or '').strip()
    if not qbo_account_id:
        logger.warning("Payment has no BankAccountRef; cannot resolve bank code")
        return {}

    row = _collection_single_row(
        f"SELECT VantagepointCode, Company, Org, Account "
        f"FROM {BANK_CODE_MAP_TABLE_NAME} WHERE QBOID = ? LIMIT 1",
        [qbo_account_id], context,
    )
    if row:
        if isinstance(row, dict):
            code = str(row.get('VantagepointCode') or '').strip()
            company = row.get('Company') or ''
            org = row.get('Org') or ''
            account = row.get('Account') or ''
        else:  # tuple fallback (column order matches the SELECT)
            code = str(row[0] or '').strip()
            company, org, account = row[1] or '', row[2] or '', row[3] or ''
        if code:
            logger.info(
                "Resolved bank code '%s' for QBO account '%s'",
                code, qbo_account_id
            )
            return {
                'Vantagepoint_Code': code,
                'Company': company,
                'Org': org,
                'Account': account,
            }
        logger.warning(
            "bank_code_map row for QBO account '%s' has no VantagepointCode",
            qbo_account_id
        )
        return {}

    # Insert-on-miss stub (Workato Resolve Bank Code recipe step 7).
    name = _fetch_qbo_account_name(context, conf, qbo_account_id)
    try:
        _collection_update(
            BANK_CODE_MAP_TABLE_NAME,
            f"INSERT INTO {BANK_CODE_MAP_TABLE_NAME} (VantagepointName, "
            "VantagepointCode, QBOName, QBOID, Status, Company, Org, Account) "
            "VALUES ('', '', ?, ?, 'Active', '', '', '')",
            [name, qbo_account_id], context,
        )
        logger.info(
            "Inserted unmapped bank_code_map stub for QBO account '%s'",
            qbo_account_id
        )
    except Exception:
        logger.warning(
            "Failed to insert bank_code_map stub for %s", qbo_account_id,
            exc_info=True
        )
    return {}


# ---------------------------------------------------------------------------
# PP (vendor payment) — weighted lines + body builders + write-back
# ---------------------------------------------------------------------------
def compute_pp_lines_method():
    """
    Weighted payment allocation across the outstanding AP voucher lines.

    Mirrors Workato recipe step 12-14 smart-list SQL:
      Weighting       = OutstandingAmount / SUM(OutstandingAmount)
      Weighted_Payment = payment_amount * Weighting
      Balance          = OutstandingAmount - Weighted_Payment
    Payment amount is this bill's payment line Amount (conf.LineAmount), the
    amount applied to this specific bill. The remainder is assigned to the last
    line so the sum of Weighted_Payment equals payment_amount exactly (avoids
    out-of-balance rejection from per-line rounding drift).
    """
    rows = rail.result('lookup_outstanding_purchase') or []
    if not rows:
        raise ValueError(
            "No outstanding_purchase_invoices rows for PP line computation."
        )

    payment_amount = float(_get_conf().get('LineAmount') or 0)
    total = sum(abs(float(r.get('OutstandingAmount') or 0)) for r in rows)
    if total == 0:
        raise ValueError(
            "outstanding_purchase_invoices rows have zero total "
            "OutstandingAmount; cannot distribute payment."
        )

    lines = []
    allocated = 0.0
    last_idx = len(rows) - 1
    for idx, row in enumerate(rows):
        outstanding = abs(float(row.get('OutstandingAmount') or 0))
        weighting = outstanding / total
        if idx == last_idx:
            weighted = round(payment_amount - allocated, 2)
        else:
            weighted = round(payment_amount * weighting, 2)
        allocated += weighted
        balance = round(outstanding - weighted, 2)
        original = float(row.get('LineAmount') or 0)
        prev_pay = round(original - outstanding, 2)

        lines.append({
            '_rowid': row.get('_rowid'),
            'Voucher': row.get('Voucher') or '',
            'WBS1': row.get('WBS1') or '',
            'WBS2': row.get('WBS2') or '',
            'WBS3': row.get('WBS3') or '',
            'Account': row.get('Account') or '',
            'Org': row.get('Org') or '',
            'OriginalLineAmount': original,
            'Outstanding_Amount': outstanding,
            'Weighted_Payment': weighted,
            'Balance': balance,
            'PreviousPaymentAmount': prev_pay,
        })

    logger.info(
        "Computed %d PP line(s): payment_amount=%s total_outstanding=%s "
        "allocated=%s", len(lines), payment_amount, total, allocated
    )
    return lines


def _ap_master_from_voucher():
    """Extract the apMaster header dict from the get_ap_voucher response."""
    voucher = rail.result('get_ap_voucher') or {}
    if isinstance(voucher, list):
        voucher = voucher[0] if voucher else {}
    ap_master_list = voucher.get('apMaster') or []
    if isinstance(ap_master_list, dict):
        return ap_master_list
    return ap_master_list[0] if ap_master_list else {}


def build_vendor_payment_body():
    """
    Build the Vantagepoint POST /vision/ledger/PP (vendor payment) body.

    Mirrors Workato recipe step 31 (vendor_payment action, TransType 'PP').
    APPPCHECKS holds one row per weighted line. Header fields (Vendor, Invoice,
    InvoiceDate, PayTerms, Address) come from the existing AP voucher's
    apMaster; bank code from bank_code_map; period from the active period.
    """
    conf = _get_conf()
    lines = rail.result('compute_pp_lines') or []
    bank = rail.result('resolve_bank_code') or {}
    bank_code = bank.get('Vantagepoint_Code') or ''

    period_res = rail.result('get_active_period') or {}
    if isinstance(period_res, list):
        period_res = period_res[0] if period_res else {}
    period = str(period_res.get('Period') or '').strip()

    ap_master = _ap_master_from_voucher()
    vendor = str(ap_master.get('Vendor') or conf.get('VendorRef') or '')
    invoice = ap_master.get('Invoice') or ''
    invoice_date = ap_master.get('InvoiceDate') or ''
    pay_terms = ap_master.get('PayTerms') or ''
    address = ap_master.get('Address') or ''
    company = (
        str(ap_master.get('Company') or '').replace(' ', '')
        or bank.get('Company') or ''
    )

    txn_date = str(conf.get('TxnDate') or '').strip()
    # CheckNo = the QBO BillPayment Id: deterministic and stable across the
    # operator's retries (request_body is a callable, so the old clock value
    # changed on every attempt) and free of the same-second collisions the
    # Workato `=now.to_i` scheme risked under concurrent workers. One QBO
    # payment is one physical check, so a payment's multiple bills
    # intentionally share the number; VP does not enforce CheckNo uniqueness.
    # Falls back to the epoch only if PaymentID is somehow absent.
    # (was: str(int(time.time())) — see review thread on MAP2-3591.)
    payment_id = str(conf.get('PaymentID') or '').strip()
    check_no = payment_id or str(int(time.time()))

    appp_checks = []
    for idx, line in enumerate(lines):
        appp_checks.append(_filter_none({
            'Period': period,
            'Vendor': vendor,
            'Voucher': line['Voucher'],
            'Invoice': invoice,
            'InvoiceDate': invoice_date,
            'LiabCode': 'General',
            'WBS1': line['WBS1'],
            'WBS2': line['WBS2'],
            'WBS3': line['WBS3'],
            'Account': line['Account'],
            'Org': line['Org'],
            'Amount': line['OriginalLineAmount'],
            'Payment': line['Weighted_Payment'],
            'PrevPay': line['PreviousPaymentAmount'],
            'PayTerms': pay_terms,
            'BankCode': bank_code,
            'CheckNo': check_no,
            'CheckDate': txn_date,
            'Seq': str(idx + 1),
            'Address': address,
            'Line': str(idx + 1),
        }))

    return _filter_none({
        'TransType': 'PP',
        'Period': period,
        'PaymentDate': txn_date,
        'CheckDate': txn_date,
        'Company': company or None,
        'PostSeq': '1',
        'APPPCHECKS': appp_checks,
    })


def _post_transaction_body(post_task_id, trans_type):
    """Shared PostTransFile body builder for PP / EP."""
    posted = rail.result(post_task_id) or {}
    if isinstance(posted, list):
        posted = posted[0] if posted else {}
    batch = str(posted.get('Batch') or '').strip()

    period_res = rail.result('get_active_period') if trans_type == 'PP' \
        else rail.result('get_active_period_ep')
    period_res = period_res or {}
    if isinstance(period_res, list):
        period_res = period_res[0] if period_res else {}
    raw_period = str(
        period_res.get('RawPeriod') or period_res.get('Period') or ''
    ).strip()

    return {
        'parms': [{
            'batch': batch,
            'description': 'Payment',
            'period': raw_period,
            'transtype': trans_type,
        }]
    }


def build_pp_post_transaction_body():
    """PUT /DataEntry/PostTransFile body to commit the PP batch (recipe
    posts the vendor payment after creation)."""
    return _post_transaction_body('post_vendor_payment', 'PP')


def update_outstanding_purchase_method():
    """
    Write back outstanding balances after a successful PP post.

    Mirrors Workato recipe steps 27-34: for each voucher line, decrement the
    OutstandingAmount to its new Balance; when a line is fully paid (Balance
    ~ 0) delete the row. (Generalises the recipe's single FullyPaid flag to a
    per-line decision, which is correct for multi-line vouchers.) All writes
    go through S3UpdateCollectionOperator (the canonical lock surface).
    """
    lines = rail.result('compute_pp_lines') or []
    context = rail.get_current_context()
    updated = 0
    deleted = 0
    for line in lines:
        rowid = line.get('_rowid')
        if rowid is None:
            continue
        if line['Balance'] > 0.005:
            _collection_update(
                'outstanding_purchase_invoices',
                "UPDATE outstanding_purchase_invoices SET OutstandingAmount = ? "
                "WHERE rowid = ?",
                [line['Balance'], rowid], context,
            )
            updated += 1
        else:
            _collection_update(
                'outstanding_purchase_invoices',
                "DELETE FROM outstanding_purchase_invoices WHERE rowid = ?",
                [rowid], context,
            )
            deleted += 1
    logger.info(
        "outstanding_purchase_invoices write-back: %d updated, %d deleted",
        updated, deleted
    )
    return {'updated': updated, 'deleted': deleted}


# ---------------------------------------------------------------------------
# EP (employee expense payment) — lines + body builders + write-back
# ---------------------------------------------------------------------------
def compute_ep_lines_method():
    """
    Build the employee-expense payment lines from outstanding_employee_expenses.

    The EP path only runs for fully-paid bills, so each outstanding line is
    paid in full (Amount = OutstandingAmount). Mirrors Workato recipe steps
    55-59 (grouped EXChecks); we keep one line per voucher row.
    """
    rows = rail.result('lookup_outstanding_expense') or []
    if not rows:
        raise ValueError(
            "No outstanding_employee_expenses rows for EP line computation."
        )
    lines = []
    for idx, row in enumerate(rows):
        amount = abs(float(row.get('OutstandingAmount') or 0))
        lines.append({
            '_rowid': row.get('_rowid'),
            'Period': row.get('Period') or '',
            'Employee': row.get('Employee') or '',
            'Voucher': row.get('Voucher') or '',
            'Org': row.get('Org') or '',
            'Amount': amount,
            'Seq': str(row.get('PostSeq') or (idx + 1)),
        })
    logger.info("Computed %d EP line(s)", len(lines))
    return lines


def build_expense_payment_body():
    """
    Build the Vantagepoint POST /vision/ledger/EP (expense payment) body.

    Mirrors Workato recipe step 59 (expense_payment action, TransType 'EP').
    """
    conf = _get_conf()
    lines = rail.result('compute_ep_lines') or []
    bank = rail.result('resolve_bank_code_ep') or {}
    bank_code = bank.get('Vantagepoint_Code') or ''
    company = bank.get('Company') or ''

    period_res = rail.result('get_active_period_ep') or {}
    if isinstance(period_res, list):
        period_res = period_res[0] if period_res else {}
    period = str(period_res.get('Period') or '').strip()

    txn_date = str(conf.get('TxnDate') or '').strip()
    # CheckNo = the QBO BillPayment Id: deterministic and stable across the
    # operator's retries (request_body is a callable, so the old clock value
    # changed on every attempt) and free of the same-second collisions the
    # Workato `=now.to_i` scheme risked under concurrent workers. One QBO
    # payment is one physical check, so a payment's multiple bills
    # intentionally share the number; VP does not enforce CheckNo uniqueness.
    # Falls back to the epoch only if PaymentID is somehow absent.
    # (was: str(int(time.time())) — see review thread on MAP2-3591.)
    payment_id = str(conf.get('PaymentID') or '').strip()
    check_no = payment_id or str(int(time.time()))
    total = round(sum(line['Amount'] for line in lines), 2)

    ex_checks = []
    for line in lines:
        ex_checks.append(_filter_none({
            'DetailType': 'E',
            'Period': line['Period'],
            'Employee': line['Employee'],
            'Voucher': line['Voucher'],
            'BankCode': bank_code,
            'Org': line['Org'],
            'Amount': line['Amount'],
            'CheckAmt': total,
            'CheckDate': txn_date,
            'ReportDate': txn_date,
            'Seq': str(line['Seq']),
            'CheckNo': check_no,
            'CheckNoRef': check_no,
        }))

    return _filter_none({
        'TransType': 'EP',
        'Period': period,
        'PaymentDate': txn_date,
        'CheckDate': txn_date,
        'Company': company or None,
        'PostSeq': '1',
        'EXCHECKS': ex_checks,
    })


def build_ep_post_transaction_body():
    """PUT /DataEntry/PostTransFile body to commit the EP batch."""
    return _post_transaction_body('post_expense_payment', 'EP')


def delete_outstanding_expense_method():
    """Delete the paid employee-expense rows after a successful EP post.
    Mirrors Workato recipe step 60 (delete_entry)."""
    bill_id = str(_get_conf().get('BillID') or '')
    context = rail.get_current_context()
    result = _collection_update(
        'outstanding_employee_expenses',
        "DELETE FROM outstanding_employee_expenses WHERE InvoiceID = ?",
        [bill_id], context,
    )
    logger.info(
        "Deleted %s outstanding_employee_expenses row(s) for BillID=%s",
        result.get('rows_affected'), bill_id
    )
    return result


# ---------------------------------------------------------------------------
# Failure callables for guard tasks (return None; these RAISE on purpose)
# ---------------------------------------------------------------------------
def fail_invoice_not_found_method():
    """Raised when the bill matches neither an outstanding purchase invoice nor
    an outstanding employee expense in the mapping_sync collections."""
    bill_id = _get_conf().get('BillID')
    raise RuntimeError(
        f"Bill {bill_id} not found in outstanding_purchase_invoices or "
        "outstanding_employee_expenses. Verify the AP voucher / employee "
        "expense exists in Vantagepoint and was synced by mapping_sync."
    )


def fail_bank_code_error_method(instance):
    """Raised when the QBO bank account has no mapped bank code."""
    qbo_account = str(_get_conf().get('BankAccountRef') or 'unknown')
    raise RuntimeError(
        f"QuickBooks bank account '{qbo_account}' is not mapped to a "
        "Vantagepoint bank code in the bank_code_map collection. An unmapped "
        "stub row has been added; populate its VantagepointCode and retry."
    )


def fail_partial_not_supported_method():
    """Raised for employee-expense bills that are only partially paid (the
    Workato recipe explicitly does not support partial EP payments)."""
    bill_id = _get_conf().get('BillID')
    raise RuntimeError(
        f"Employee-expense bill {bill_id} is only partially paid. Partial "
        "employee expense payments are not supported (Workato recipe parity)."
    )


# ---------------------------------------------------------------------------
# Error capture for the worker DAG
# ---------------------------------------------------------------------------
def capture_bill_payment_dag_error(payment_id, bill_id, fallback_error):
    """Return an error dict when something failed; None on a clean run."""
    if not fallback_error:
        return None
    return {
        'error': (
            f"PaymentID {payment_id} (BillID {bill_id}) - "
            f"bill payment worker failed: {fallback_error}"
        ),
        'PaymentID': payment_id,
        'BillID': bill_id,
    }
