# dags/vp_xero_integration_v2/ap_voucher_sync/utils/python_callable_method.py
"""Python callable methods for VP -> Xero AP Voucher Sync.

Ports the Workato AP Voucher for Xero bundle (014_501 PSA recipes):
  - poll recipe:      PostDate watermark filter, AutoEntry/TaxCode grouping
  - dispatcher:       voucher identity grouping
  - create processor: PSALedger re-fetch, map lookups, Xero ACCPAY Invoice build

Direction: VP -> Xero, create-only (Xero ACCPAY Invoice / Bill). Polls
PSALedger (TransType='ap'), groups header rows (AutoEntry="N", TaxCode="")
by (Period, PostSeq, Voucher) into voucher identities, enriches all lines
with Xero AccountCode + TaxType lookups, and POSTs an ACCPAY Invoice to
Xero /api.xro/2.0/Invoices.

Deviations from Workato:
  - No VP /firm search step: PSALedger.Vendor is matched against
    map_firm.Vendor (the vendor code parsed from the Xero Contact
    AccountNumber's `PL…` segment) to resolve the Xero ContactID.
  - No WBS/project resolution: Xero ManualJournal/Invoice has no
    project-client CustomerRef requirement.
  - CFG_VoucherStatusSubmitted from middleware conf controls ACCPAY status.

Failure pattern: QBO-style (catch_processor_dag_error, one_failed, direct
edges from every upstream task).
"""
# pylint: disable=invalid-name,broad-exception-caught,too-many-locals
import logging
from decimal import Decimal, InvalidOperation
from urllib.parse import quote

import rail
from vp_xero_integration_v2.common.python_callable_method import (
    collection_rows,
    collection_operations,
    unwrap_vp_response,
)
from vp_xero_integration_v2.common.tables import (
    MAP_CHART_OF_ACCOUNTS_TABLE_NAME,
    MAP_FIRM_TABLE_NAME,
    MAP_TAX_CODE_TABLE_NAME,
    OUTSTANDING_PURCHASE_INVOICES_TABLE_NAME,
    OUTSTANDING_PURCHASE_INVOICES_COLUMNS,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Generic helpers
# ---------------------------------------------------------------------------
def _conf_value(key, default=''):
    """Fetch a single key from the running dag_run.conf."""
    conf = rail.get_current_context()['dag_run'].conf
    value = conf.get(key)
    return value if value is not None else default


def _amount_str(value):
    """Format a numeric/string amount as a Decimal-clean string."""
    if value is None or value == '':
        return '0'
    try:
        return str(Decimal(str(value)))
    except (InvalidOperation, ValueError):
        return '0'


def _normalize_date(raw):
    """Trim an ISO datetime or bare date to YYYY-MM-DD."""
    if not raw:
        return ''
    return str(raw).split('T')[0][:10]


def _compute_due_date(date_str):
    """DueDate = date_str + default_payment_period_days days."""
    from datetime import datetime, timedelta
    from vp_xero_integration_v2.ap_voucher_sync.config import default_payment_period_days
    if not date_str:
        return ''
    try:
        dt = datetime.strptime(date_str[:10], '%Y-%m-%d')
        return (dt + timedelta(days=default_payment_period_days)).strftime('%Y-%m-%d')
    except ValueError:
        return date_str


# ---------------------------------------------------------------------------
# Dispatcher: PSALedger PostDate watermark filter
# ---------------------------------------------------------------------------
def build_vp_psaledger_ap_filter_method():
    """filterHash for the dispatcher's PSALedger AP poll.

    Closed-lower / open-upper PostDate window:
      last_sync_time <= PostDate < current_sync_time
    PostDate is the commit timestamp (never backdated); TransDate is the
    business date and can be backdated — PostDate is the correct cursor.
    """
    timestamps = rail.result('prepare_sync_timestamps')
    last = quote(timestamps['last_sync_time'], safe='')
    current = quote(timestamps['current_sync_time'], safe='')
    gte = quote('>=', safe='')
    lt = quote('<', safe='')
    return (
        f"?filterHash[0][name]=PostDate"
        f"&filterHash[0][value]={last}"
        f"&filterHash[0][type]=datetime"
        f"&filterHash[0][opp]={gte}"
        f"&filterHash[0][condition]=AND"
        f"&filterHash[0][seq]=0"
        f"&filterHash[1][name]=PostDate"
        f"&filterHash[1][value]={current}"
        f"&filterHash[1][type]=datetime"
        f"&filterHash[1][opp]={lt}"
        f"&filterHash[1][seq]=1"
    )


# ---------------------------------------------------------------------------
# Dispatcher: voucher grouping (Xero recipe parity — AutoEntry/TaxCode filter)
# ---------------------------------------------------------------------------
def extract_ap_vouchers_list_method():
    """Group PSALedger AP rows into unique (Period, PostSeq, Voucher) vouchers.

    Xero recipe parity: only rows with AutoEntry="N" AND TaxCode="" are used
    to identify unique voucher headers. This mirrors the Workato Xero AP
    voucher poll recipe's grouping step — rows with AutoEntry or a TaxCode
    are sub-components of the AP entry, not the header row that identifies
    the voucher.

    Header fields carried from the first matching row:
    Batch, Vendor, RefNo, Desc1, FirstTransDate (= TransDate of first row).
    The create DAG re-fetches all lines (including AutoEntry/TaxCode rows)
    for the full bill body.

    Returns list[dict], one entry per unique (Period, PostSeq, Voucher).
    """
    raw = rail.result('get_changed_psaledger_ap_rows')
    rows = unwrap_vp_response(raw, strict=True)
    grouped = {}
    skipped_filter = 0
    skipped_invalid = 0
    for row in rows:
        if not isinstance(row, dict):
            skipped_invalid += 1
            continue
        # Xero recipe header-row filter
        if row.get('AutoEntry', 'N') != 'N' or (row.get('TaxCode') or '') != '':
            skipped_filter += 1
            continue
        period = row.get('Period')
        post_seq = row.get('PostSeq')
        voucher = row.get('Voucher')
        if period is None or post_seq is None or not voucher:
            skipped_invalid += 1
            continue
        key = (str(period), str(post_seq), str(voucher))
        if key not in grouped:
            grouped[key] = {
                'Period': str(period),
                'PostSeq': str(post_seq),
                'Voucher': str(voucher),
                'Batch': row.get('Batch') or '',
                'Vendor': row.get('Vendor') or '',
                'RefNo': row.get('RefNo') or '',
                'Desc1': row.get('Desc1') or '',
                'FirstTransDate': row.get('TransDate') or '',
                'RowCount': 1,
            }
        else:
            grouped[key]['RowCount'] += 1
    vouchers = list(grouped.values())
    logger.info(
        "Grouped %d PSALedger AP rows into %d unique (Period, PostSeq, Voucher) "
        "vouchers (AutoEntry=N/TaxCode='' filter excluded %d rows; "
        "%d rows skipped for missing identity fields)",
        len(rows), len(vouchers), skipped_filter, skipped_invalid,
    )
    return vouchers


def check_if_ap_vouchers_exist_method():
    """IfOperator test: did the PSALedger poll surface any vouchers?"""
    return len(rail.result('extract_ap_vouchers_list') or []) > 0


# ---------------------------------------------------------------------------
# Create DAG: PSALedger per-voucher re-fetch filter
# ---------------------------------------------------------------------------
def build_psaledger_period_postseq_ap_filter_method():
    """filterHash to re-fetch all lines for this exact (Period, PostSeq, Voucher).

    Three AND'd clauses: Period (int), PostSeq (int), Voucher (string).
    This form is what VP's PSALedger endpoint actually honors — plain
    ?Period=X query params are silently ignored.
    """
    period_value = _conf_value('Period')
    post_seq_value = _conf_value('PostSeq')
    voucher_value = _conf_value('Voucher')
    if not period_value or not post_seq_value or not voucher_value:
        raise RuntimeError(
            "Processor dag_run.conf missing Period/PostSeq/Voucher — got "
            f"Period={period_value!r}, PostSeq={post_seq_value!r}, "
            f"Voucher={voucher_value!r}. Refusing to query PSALedger."
        )
    period = quote(str(period_value), safe='')
    post_seq = quote(str(post_seq_value), safe='')
    voucher = quote(str(voucher_value), safe='')
    eq = quote('=', safe='')
    return (
        f"?filterHash[0][name]=Period"
        f"&filterHash[0][value]={period}"
        f"&filterHash[0][type]=int"
        f"&filterHash[0][opp]={eq}"
        f"&filterHash[0][condition]=AND"
        f"&filterHash[0][seq]=0"
        f"&filterHash[1][name]=PostSeq"
        f"&filterHash[1][value]={post_seq}"
        f"&filterHash[1][type]=int"
        f"&filterHash[1][opp]={eq}"
        f"&filterHash[1][condition]=AND"
        f"&filterHash[1][seq]=1"
        f"&filterHash[2][name]=Voucher"
        f"&filterHash[2][value]={voucher}"
        f"&filterHash[2][type]=string"
        f"&filterHash[2][opp]={eq}"
        f"&filterHash[2][seq]=2"
    )


# ---------------------------------------------------------------------------
# Create DAG: dedup guard (outstanding_purchase_invoices read)
# ---------------------------------------------------------------------------
def is_voucher_already_exported_method():
    """IfOperator test: has this (Batch, Voucher) already been exported?

    Reads outstanding_purchase_invoices by Batch+Voucher. Returns True (skip)
    when found; False (proceed) otherwise. Guards against blank Batch/Voucher
    — never skip on an empty identity.
    """
    batch = str(_conf_value('Batch') or '').strip()
    voucher = str(_conf_value('Voucher') or '').strip()
    if not batch or not voucher:
        return False
    rows = collection_rows(
        OUTSTANDING_PURCHASE_INVOICES_TABLE_NAME,
        ['Batch', 'Voucher'],
        'Batch = ? AND Voucher = ?',
        [batch, voucher],
    )
    if rows:
        logger.info(
            "Voucher (Batch=%s, Voucher=%s) already exported (present in "
            "outstanding_purchase_invoices) — skipping duplicate Xero Bill.",
            batch, voucher,
        )
        return True
    return False


# ---------------------------------------------------------------------------
# Create DAG: PSALedger line unwrap
# ---------------------------------------------------------------------------
def extract_psaledger_lines_method():
    """Unwrap the per-voucher PSALedger re-fetch into a list of line dicts.

    All lines are included (no AutoEntry/TaxCode filter here — the dispatcher
    filter is for voucher identification only; the bill body includes every
    line).
    """
    raw = rail.result('get_psaledger_lines_for_voucher')
    rows = unwrap_vp_response(raw, strict=True)
    lines = [r for r in rows if isinstance(r, dict)]
    period = _conf_value('Period')
    post_seq = _conf_value('PostSeq')
    voucher = _conf_value('Voucher')
    logger.info(
        "PSALedger AP voucher (Period=%s, PostSeq=%s, Voucher=%s) has %d lines",
        period, post_seq, voucher, len(lines),
    )
    if not lines:
        raise RuntimeError(
            f"No PSALedger lines for (Period={period}, PostSeq={post_seq}, "
            f"Voucher={voucher}). Refusing to post an empty Xero Bill."
        )
    return lines


# ---------------------------------------------------------------------------
# Create DAG: lookup table loading
# ---------------------------------------------------------------------------
def load_lookup_tables_method():
    """Load map_chart_of_accounts, map_firm, and map_tax_code from S3.

    Returns:
        account_map: {VantagepointCode -> XeroCode (account code string)}
        firm_map:    {Vendor -> ContactID (Xero contact UUID string)}
        tax_map:     {VantagepointCode -> XeroCode (Xero TaxType string)}

    All three maps are 1:1 (first-row-wins on duplicate key). Tax map value
    is the Xero TaxType code (e.g. 'INPUT', 'NONE') used in LineItem.TaxType.
    """
    context = rail.get_current_context()

    account_map = {}
    for r in collection_rows(
        MAP_CHART_OF_ACCOUNTS_TABLE_NAME,
        ['VantagepointCode', 'XeroCode'],
        '1 = 1', [], context,
    ):
        code = (r.get('VantagepointCode') or '').strip()
        if not code or code in account_map:
            continue
        account_map[code] = (r.get('XeroCode') or '').strip()

    firm_map = {}
    for r in collection_rows(
        MAP_FIRM_TABLE_NAME,
        ['Vendor', 'ContactID'],
        '1 = 1', [], context,
    ):
        vendor_key = (r.get('Vendor') or '').strip()
        if not vendor_key or vendor_key in firm_map:
            continue
        firm_map[vendor_key] = (r.get('ContactID') or '').strip()

    tax_map = {}
    for r in collection_rows(
        MAP_TAX_CODE_TABLE_NAME,
        ['VantagepointCode', 'XeroCode'],
        '1 = 1', [], context,
    ):
        code = (r.get('VantagepointCode') or '').strip()
        if not code or code in tax_map:
            continue
        tax_map[code] = (r.get('XeroCode') or '').strip()

    logger.info(
        "Loaded lookup tables: account_map=%d entries, firm_map=%d entries, "
        "tax_map=%d entries",
        len(account_map), len(firm_map), len(tax_map),
    )
    return {'account_map': account_map, 'firm_map': firm_map, 'tax_map': tax_map}


# ---------------------------------------------------------------------------
# Create DAG: vendor -> Xero ContactID resolution
# ---------------------------------------------------------------------------
def resolve_firm_vendorref_method():
    """Resolve the voucher Vendor code to a Xero ContactID via map_firm.

    PSALedger.Vendor matches map_firm.Vendor (the vendor code parsed from the
    Xero AccountNumber PL… prefix). Raises (fail-loud) when Vendor is blank or
    not found in map_firm so a misconfigured tenant fails this voucher's
    processor without poisoning siblings.

    Known limitation: if create_xero_bill succeeds but record_outstanding_invoices
    then fails, the Bill exists in Xero but the dedup guard won't see it on
    retry — a duplicate ACCPAY Bill could result. This matches the QBO sibling
    behaviour and is accepted as a residual risk.

    Returns the ContactID string.
    """
    vendor_code = _conf_value('Vendor')
    period = _conf_value('Period')
    post_seq = _conf_value('PostSeq')
    if not vendor_code:
        raise RuntimeError(
            f"AP voucher (Period={period}, PostSeq={post_seq}) has no Vendor "
            "code — cannot resolve a Xero ContactID for the Bill."
        )
    firm_map = (rail.result('load_lookup_tables') or {}).get('firm_map') or {}
    contact_id = firm_map.get(vendor_code, '').strip()
    if not contact_id:
        raise RuntimeError(
            f"Vantagepoint Vendor {vendor_code!r} not found in map_firm "
            f"(Period={period}, PostSeq={post_seq}). "
            "Cannot create Xero Bill without a ContactID."
        )
    logger.info(
        "Resolved ContactID for voucher (Period=%s, PostSeq=%s): "
        "Vendor=%s -> ContactID=%s",
        period, post_seq, vendor_code, contact_id,
    )
    return contact_id


# ---------------------------------------------------------------------------
# Create DAG: line enrichment
# ---------------------------------------------------------------------------
def enrich_lines_method():
    """Annotate each PSALedger line with Xero AccountCode and TaxType.

    _XeroAccountCode: account_map[Account] -> XeroCode string
    _XeroTaxType:     tax_map[TaxCode]     -> XeroCode string, default 'NONE'
    _AccountCode:     raw VP Account field (for validate + record steps)
    _TaxCode:         raw VP TaxCode field
    _Description:     Desc2 or Desc1 (for LineItem Description)
    _Amount:          raw Amount field (for UnitAmount + outstanding record)
    """
    lines = rail.result('extract_psaledger_lines') or []
    tables = rail.result('load_lookup_tables') or {}
    account_map = tables.get('account_map') or {}
    tax_map = tables.get('tax_map') or {}
    enriched = []
    for line in lines:
        account_code = (line.get('Account') or '').strip()
        xero_account_code = account_map.get(account_code, '')
        tax_code = (line.get('TaxCode') or '').strip()
        xero_tax_type = tax_map.get(tax_code) if tax_code else 'NONE'
        xero_tax_type_missing = tax_code and xero_tax_type is None
        if xero_tax_type is None:
            xero_tax_type = 'NONE'
        enriched.append({
            **line,
            '_XeroAccountCode': xero_account_code,
            '_XeroTaxType': xero_tax_type,
            '_XeroTaxTypeMissing': xero_tax_type_missing,
            '_AccountCode': account_code,
            '_TaxCode': tax_code,
            '_Description': (line.get('Desc2') or line.get('Desc1') or '').strip(),
            '_Amount': line.get('Amount'),
        })
    logger.info(
        "Enriched %d lines with Xero account codes and tax types", len(enriched)
    )
    return enriched


# ---------------------------------------------------------------------------
# Create DAG: validation
# ---------------------------------------------------------------------------
def validate_enriched_lines_method():
    """Validate enriched lines and raise on any mapping gap.

    Accumulates all errors (Workato CompoundError style) so an operator can
    fix all map_chart_of_accounts gaps in a single edit rather than iterating.
    """
    enriched = rail.result('enrich_lines') or []
    period = _conf_value('Period')
    post_seq = _conf_value('PostSeq')
    voucher = _conf_value('Voucher')
    if not enriched:
        raise RuntimeError(
            f"Refusing to post a Xero Bill with zero lines "
            f"(Period={period}, PostSeq={post_seq}, Voucher={voucher})."
        )
    errors = []
    for line in enriched:
        if not line.get('_XeroAccountCode'):
            errors.append(
                f"Vantagepoint account {line.get('_AccountCode')!r} not matched "
                "to a Xero account code in map_chart_of_accounts "
                f"(line Account={line.get('Account')})."
            )
        tax_code = line.get('_TaxCode', '')
        if line.get('_XeroTaxTypeMissing'):
            errors.append(
                f"Vantagepoint tax code {tax_code!r} not matched "
                "to a Xero tax type in map_tax_code "
                f"(line Account={line.get('Account')})."
            )
    if errors:
        raise RuntimeError(
            f"AP voucher (Period={period}, PostSeq={post_seq}, Voucher={voucher}) "
            "failed validation:\n  - " + "\n  - ".join(errors)
        )
    logger.info(
        "Validation passed for (Period=%s, PostSeq=%s, Voucher=%s): %d lines",
        period, post_seq, voucher, len(enriched),
    )
    return enriched


# ---------------------------------------------------------------------------
# Create DAG: Xero ACCPAY Invoice body builder
# ---------------------------------------------------------------------------
def build_bill_body_method():
    """Build the Xero ACCPAY Invoice request body.

    Wrapped in {"Invoices": [invoice]} as required by POST /api.xro/2.0/Invoices.

    CFG_VoucherStatusSubmitted='true' -> Status='SUBMITTED', else 'AUTHORISED'.
    DueDate = Date + default_payment_period_days days.
    Reference = 'AP {Period} {PostSeq} {Voucher}' (idempotency key).
    One LineItem per PSALedger row (ALL lines — AutoEntry and TaxCode rows are
    included in the bill body; the dispatcher filter is for voucher identity only).
    """
    enriched = rail.result('validate_enriched_lines') or []
    contact_id = rail.result('resolve_firm_vendorref') or ''
    period = _conf_value('Period')
    post_seq = _conf_value('PostSeq')
    voucher = _conf_value('Voucher')
    first_trans_date = _conf_value('FirstTransDate')
    cfg_submitted = str(_conf_value('CFG_VoucherStatusSubmitted') or '').strip().lower()

    status = 'SUBMITTED' if cfg_submitted == 'true' else 'AUTHORISED'
    date_str = _normalize_date(first_trans_date)
    due_date_str = _compute_due_date(date_str)

    line_items = []
    for line in enriched:
        line_items.append({
            'Description': line.get('_Description') or '',
            'Quantity': 1.0,
            'UnitAmount': _amount_str(line.get('_Amount')),
            'AccountCode': line['_XeroAccountCode'],
            'TaxType': line.get('_XeroTaxType') or 'NONE',
        })

    invoice = {
        'Type': 'ACCPAY',
        'Contact': {'ContactID': contact_id},
        'Date': date_str,
        'DueDate': due_date_str,
        'Reference': f'AP {period} {post_seq} {voucher}',
        'Status': status,
        'LineAmountTypes': 'EXCLUSIVE',
        'LineItems': line_items,
    }

    logger.info(
        "Built Xero ACCPAY Invoice for voucher (Period=%s, PostSeq=%s, "
        "Voucher=%s): %d line items, Status=%s, ContactID=%s, Date=%s",
        period, post_seq, voucher, len(line_items), status, contact_id, date_str,
    )
    return {'Invoices': [invoice]}


# ---------------------------------------------------------------------------
# Create DAG: outstanding_purchase_invoices write
# ---------------------------------------------------------------------------
def record_outstanding_invoices_method(create_bill_task_id):
    """Write one outstanding-invoice row per line after a successful Xero Bill create.

    Reads the Xero InvoiceID from the XeroInvoiceOperator result
    (response['data'][0]['InvoiceID']). RAISES if InvoiceID is absent —
    the outstanding row is the dedup marker; a missing write means the next
    poll would create a duplicate Bill.

    Idempotent: atomically DELETE this Batch+Voucher then INSERT current lines
    in a single S3 cycle (collection_operations, atomic=True).
    """
    context = rail.get_current_context()
    period = _conf_value('Period')
    post_seq = _conf_value('PostSeq')
    batch = str(_conf_value('Batch') or '')
    voucher = str(_conf_value('Voucher') or '')
    enriched = rail.result('validate_enriched_lines') or []
    create_result = rail.result(create_bill_task_id)

    invoice_id = ''
    if isinstance(create_result, dict):
        invoices = create_result.get('data') or []
        if invoices and isinstance(invoices[0], dict):
            invoice_id = str(invoices[0].get('InvoiceID') or '').strip()
    if not invoice_id:
        raise RuntimeError(
            f"Xero Bill create for voucher (Period={period}, PostSeq={post_seq}, "
            f"Voucher={voucher}) did not return an InvoiceID — cannot record "
            "outstanding invoice. Check create_xero_bill task result."
        )

    row_placeholder = '(' + ', '.join(
        ['?'] * len(OUTSTANDING_PURCHASE_INVOICES_COLUMNS)
    ) + ')'
    columns = ', '.join(OUTSTANDING_PURCHASE_INVOICES_COLUMNS)

    rows_params = []
    for line in enriched:
        amount = _amount_str(line.get('_Amount'))
        values = {
            'Batch': batch,
            'Voucher': voucher,
            'WBS1': (line.get('WBS1') or '').strip(),
            'WBS2': (line.get('WBS2') or '').strip(),
            'WBS3': (line.get('WBS3') or '').strip(),
            'LineAmount': amount,
            'OutstandingAmount': amount,
            'Account': line.get('_AccountCode') or (line.get('Account') or ''),
            'Org': line.get('Org') or '',
            'InvoiceID': invoice_id,
        }
        rows_params.append([values[c] for c in OUTSTANDING_PURCHASE_INVOICES_COLUMNS])

    operations = [{
        'query': (
            f"DELETE FROM {OUTSTANDING_PURCHASE_INVOICES_TABLE_NAME} "
            "WHERE Batch = ? AND Voucher = ?"
        ),
        'query_params': [batch, voucher],
    }]
    chunk_size = 50
    for start in range(0, len(rows_params), chunk_size):
        chunk = rows_params[start:start + chunk_size]
        values_clause = ', '.join([row_placeholder] * len(chunk))
        flat_params = [p for params in chunk for p in params]
        operations.append({
            'query': (
                f"INSERT INTO {OUTSTANDING_PURCHASE_INVOICES_TABLE_NAME} "
                f"({columns}) VALUES {values_clause}"
            ),
            'query_params': flat_params,
        })

    collection_operations(OUTSTANDING_PURCHASE_INVOICES_TABLE_NAME, operations, context)
    logger.info(
        "Recorded %d outstanding_purchase_invoices rows for voucher "
        "(Period=%s, PostSeq=%s, Voucher=%s); Xero InvoiceID=%s",
        len(rows_params), period, post_seq, voucher, invoice_id,
    )
    return None


# ---------------------------------------------------------------------------
# Error capture (QBO-style: return dict, do NOT raise)
# ---------------------------------------------------------------------------
def capture_processor_error(period, post_seq, error_message):
    """Return an error dict the dispatcher can aggregate.

    Called by catch_processor_dag_error (trigger_rule='one_failed'). Must NOT
    raise — the processor DAG must finish as SUCCESS so the dispatcher's
    WaitForDagRunsSensor sees a terminal state and GatherResultsFromDagRunsOperator
    can collect this error dict.
    """
    label = f"AP Voucher (Period={period}, PostSeq={post_seq})"
    return {'error': f"{label} - sync failed: {error_message}"}
