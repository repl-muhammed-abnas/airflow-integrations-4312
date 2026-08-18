"""
Python callable methods for VP PSA -> Xero Posted Invoices Sync.

Ports the Workato `014-501 PSA Posted Invoices for Xero` bundle into Python
callables for the 3-DAG Airflow template (main -> dispatcher -> processor).

Workato lookup tables consumed (read-only; owned and maintained by mapping_sync):
  - map_chart_of_accounts  (VP account code -> Xero account code)
  - map_tax_code           (VP tax code -> Xero TaxRate name)

PSA Ledger TransType=IN rows are grouped by (Invoice, Period, PostSeq) into
per-invoice structures. InvoiceSection='T' rows are excluded from invoice lines
and used only for compound-tax rate computation (Workato recipe 3 steps 9-16).
Each invoice routes to either ACCREC (invoice) or ACCRECCREDIT (credit note)
based on IsCreditMemo (from SalesInvoice master) or CreditMemoRefNo presence.
After all invoices in a batch are posted, a revenue-recognition ManualJournal
is created from PSA Ledger rows re-fetched by Period+PostSeq.
"""
# pylint: disable=invalid-name,broad-exception-caught,too-many-return-statements
# pylint: disable=too-many-locals,too-many-branches,too-many-statements
import logging
from urllib.parse import quote
import rail
from vp_xero_integration.common.python_callable_method import (
    collection_rows,
    unwrap_vp_response,
)
from vp_xero_integration.common.tables import (
    MAP_CHART_OF_ACCOUNTS_TABLE_NAME,
    MAP_CHART_OF_ACCOUNTS_COLUMNS,
    MAP_TAX_CODE_TABLE_NAME,
    MAP_TAX_CODE_COLUMNS,
)

logger = logging.getLogger(__name__)

# Xero LineAmountTypes values — casing per Xero API docs and Workato parity
_NOTAX = 'NoTax'
_EXCLUSIVE = 'Exclusive'

# Xero invoice status values
_STATUS_AUTHORISED = 'AUTHORISED'
_STATUS_SUBMITTED = 'SUBMITTED'

# Xero invoice type values
_TYPE_ACCREC = 'ACCREC'
_TYPE_ACCRECCREDIT = 'ACCRECCREDIT'

# VP CFGAutoPosting account keys used for revenue recognition
_REVENUE_ACCOUNT_KEYS = ('UninvoicedRevenue', 'UnbilledServices')


# ---------------------------------------------------------------------------
# Generic helpers
# ---------------------------------------------------------------------------
def _conf():
    return rail.get_current_context()['dag_run'].conf or {}


def _s(value):
    """Coerce a field to a stripped string; None -> ''."""
    if value is None:
        return ''
    return str(value).strip()


def _extract_xero_records(rail_result):
    """Normalize a Xero*Operator response into a flat list of records.

    Handles the three shapes the Xero operators emit:
      - {'success': bool, 'data': [...], 'error': '...'} (typed-operator shape)
      - a raw list (older shape)
      - {'<Resource>': [...]} (raw envelope, e.g. {'Invoices': [...]})
    """
    if rail_result is None:
        return []
    if isinstance(rail_result, list):
        return rail_result
    if isinstance(rail_result, dict):
        if rail_result.get('success') is False:
            raise RuntimeError(
                f"Xero query failed: {rail_result.get('error')}"
            )
        if 'data' in rail_result:
            data = rail_result['data']
            if isinstance(data, list):
                return data
            return [data] if data else []
        for key in ('Invoices', 'CreditNotes', 'TaxRates', 'ManualJournals'):
            records = rail_result.get(key)
            if isinstance(records, list):
                return records
    return []


def _find_zero_rate_tax_code(tax_rates):
    """Find the Xero TaxType CODE for the zero-rate tax rate.

    Workato parity (014_501_psa_xero_no_tax_code): finds a TaxRate with
    EffectiveRate=0 and Name in {'No VAT', 'Tax Exempt'}. Falls back to any
    rate with EffectiveRate=0. Returns 'NONE' when nothing qualifies.

    Returns the TaxType CODE (e.g. 'NONE'), not the display Name — the Xero
    API expects the code in LineItem.TaxType, not the human-readable name.
    """
    preferred_names = {'No VAT', 'Tax Exempt'}
    for rate in (tax_rates or []):
        if not isinstance(rate, dict):
            continue
        name = _s(rate.get('Name'))
        try:
            effective_rate = float(rate.get('EffectiveRate', 1))
        except (TypeError, ValueError):
            effective_rate = 1.0
        if name in preferred_names and effective_rate == 0:
            return _s(rate.get('TaxType')) or 'NONE'
    # Fallback: any zero-rate
    for rate in (tax_rates or []):
        if not isinstance(rate, dict):
            continue
        try:
            effective_rate = float(rate.get('EffectiveRate', 1))
        except (TypeError, ValueError):
            effective_rate = 1.0
        if effective_rate == 0:
            return _s(rate.get('TaxType')) or 'NONE'
    return 'NONE'


def _build_vp_tax_codes_map(vp_tax_codes_rows):
    """Build a lookup map from VP TaxCode rows.

    Returns: {Code: {'Rate': float, 'CompoundOnTaxCode': str}}
    Used for compound effective rate computation (Workato recipe 3 step 4 + steps 11-13).
    """
    tc_map = {}
    for row in (vp_tax_codes_rows or []):
        if not isinstance(row, dict):
            continue
        code = _s(row.get('Code'))
        if not code:
            continue
        try:
            rate = float(row.get('Rate', 0) or 0)
        except (TypeError, ValueError):
            rate = 0.0
        tc_map[code] = {
            'Rate': rate,
            'CompoundOnTaxCode': _s(row.get('CompoundOnTaxCode')),
        }
    return tc_map


def _compute_effective_rate(vp_tax_code, vp_tc_map):
    """Compute compound effective tax rate for one VP tax code.

    Workato parity (recipe 3 steps 11-13):
      effective = (1 + Rate/100) * (1 + CompoundOnTaxCode.Rate/100) - 1
    Non-compound codes use compound_rate=0, giving effective = Rate/100.
    Returns 0.0 for unknown or blank codes.
    """
    if not vp_tax_code:
        return 0.0
    tc = vp_tc_map.get(vp_tax_code, {})
    if not tc:
        return 0.0
    try:
        rate = float(tc.get('Rate', 0) or 0) / 100.0
    except (TypeError, ValueError):
        rate = 0.0
    compound_on = _s(tc.get('CompoundOnTaxCode'))
    compound_rate = 0.0
    if compound_on:
        compound_tc = vp_tc_map.get(compound_on, {})
        try:
            compound_rate = float(compound_tc.get('Rate', 0) or 0) / 100.0
        except (TypeError, ValueError):
            compound_rate = 0.0
    return round((1.0 + rate) * (1.0 + compound_rate) - 1.0, 6)


# ---------------------------------------------------------------------------
# Dispatcher callables
# ---------------------------------------------------------------------------
def build_psa_ledger_filter_method():
    """filterHash filter selecting TransType=IN rows within the current watermark window."""
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


def extract_invoice_batches_method():
    """Extract unique {Batch, PostDate} pairs from the PSA Ledger poll result.

    Workato parity: the trigger emitted one Batch per firing; here we deduplicate
    to produce one processor DAG trigger per unique batch number.
    Returns list[dict] with keys Batch and PostDate.
    """
    rows = unwrap_vp_response(rail.result('poll_psa_ledger'), strict=False)
    seen = set()
    batches = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        batch = _s(row.get('Batch'))
        if batch and batch not in seen:
            seen.add(batch)
            batches.append({
                'Batch': batch,
                'PostDate': _s(row.get('PostDate')),
            })
    logger.info("Found %d unique invoice batch(es) in poll window", len(batches))
    return batches


def build_processor_dag_conf(item):
    """Build the per-batch conf dict for the processor DAG."""
    ctx_conf = _conf()
    return {
        'Batch': item.get('Batch'),
        'PostDate': item.get('PostDate'),
        'connections': ctx_conf.get('connections'),
        'customerId': ctx_conf.get('customerId'),
        'config': ctx_conf.get('config', {}),
    }


# ---------------------------------------------------------------------------
# Processor callables — Phase 1: filter functions
# ---------------------------------------------------------------------------
def build_invoice_batch_filter():
    """filterHash filter selecting all 'IN' rows for dag_run.conf.Batch."""
    batch_value = _conf().get('Batch')
    if not batch_value:
        raise RuntimeError(
            "Processor dag_run.conf missing Batch — "
            f"got Batch={batch_value!r}. "
            "Refusing to query PSALedger with an empty batch identity."
        )
    batch = quote(str(batch_value), safe='')
    eq = quote('=', safe='')
    return (
        f"?filterHash[0][name]=Batch"
        f"&filterHash[0][value]={batch}"
        f"&filterHash[0][type]=string"
        f"&filterHash[0][opp]={eq}"
        f"&filterHash[0][seq]=0"
    )



def build_ledger_tax_filter():
    """filterHash filter for VantagepointLedgertaxOperator by Period+PostSeq.

    Workato parity: recipe 3 step 3 fetches LedgerTax rows after the batch
    PSALedger fetch; uses Period+PostSeq from the first fetch_invoice_batch row.
    """
    rows = unwrap_vp_response(rail.result('fetch_invoice_batch'), strict=False)
    first_row = next((r for r in rows if isinstance(r, dict)), {})
    period_value = _s(first_row.get('Period'))
    post_seq_value = _s(first_row.get('PostSeq'))
    if not (period_value and post_seq_value):
        raise RuntimeError(
            "fetch_invoice_batch produced no Period/PostSeq — "
            f"got Period={period_value!r}, PostSeq={post_seq_value!r}. "
            "Refusing to query LedgerTax with an empty filter."
        )
    period = quote(period_value, safe='')
    post_seq = quote(post_seq_value, safe='')
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
        f"&filterHash[1][seq]=1"
    )


def build_revenue_psa_filter():
    """filterHash filter for revenue PSALedger re-fetch by Period+PostSeq.

    Workato parity: recipe 5 step 2 re-fetches PSALedger by Period+PostSeq
    (not by Batch) so the revenue journal uses canonical period-level data.
    Period+PostSeq are taken from the first transformed invoice in
    group_and_transform_invoices.
    """
    invoices = rail.result('group_and_transform_invoices') or []
    first_inv = invoices[0] if invoices else {}
    header = first_inv.get('Header', {}) if isinstance(first_inv, dict) else {}
    period_value = _s(header.get('Period'))
    post_seq_value = _s(header.get('PostSeq'))
    if not (period_value and post_seq_value):
        raise RuntimeError(
            "group_and_transform_invoices produced no Period/PostSeq — "
            f"got Period={period_value!r}, PostSeq={post_seq_value!r}. "
            "Refusing to query PSALedger with an empty filter."
        )
    period = quote(period_value, safe='')
    post_seq = quote(post_seq_value, safe='')
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
        f"&filterHash[1][seq]=1"
    )


# ---------------------------------------------------------------------------
# Processor callables — Phase 1: data loading
# ---------------------------------------------------------------------------
def fetch_account_and_tax_maps_method():
    """Load map_chart_of_accounts + map_tax_code from the mapping_sync S3 collection.

    Workato parity: the Post Invoice recipe looked up account code and tax code
    for each line via the lookup tables. Here we load both maps once per batch
    and pass them downstream via XCom.

    Returns:
        {
            'account_map': {vp_code: xero_code, ...},
            'tax_code_map': {vp_code: xero_tax_name, ...},
        }
    """
    context = rail.get_current_context()

    # VP account code -> Xero account code
    acct_rows = collection_rows(
        MAP_CHART_OF_ACCOUNTS_TABLE_NAME,
        MAP_CHART_OF_ACCOUNTS_COLUMNS,
        "VantagepointCode IS NOT NULL AND VantagepointCode != ''",
        [],
        context,
    )
    account_map = {}
    for row in acct_rows:
        vp_code = _normalise_account(_s(row.get('VantagepointCode')))
        xero_code = _s(row.get('XeroCode'))
        if vp_code and xero_code:
            account_map[vp_code] = xero_code

    # VP tax code -> Xero TaxRate name (take first row per VP code)
    tax_rows = collection_rows(
        MAP_TAX_CODE_TABLE_NAME,
        MAP_TAX_CODE_COLUMNS,
        "1=1",
        [],
        context,
    )
    tax_code_map = {}
    for row in tax_rows:
        vp_code = _s(row.get('VantagepointCode'))
        if not vp_code or vp_code in tax_code_map:
            continue
        xero_name = _s(row.get('XeroName'))
        if xero_name:
            tax_code_map[vp_code] = xero_name

    logger.info(
        "Loaded %d account mapping(s), %d tax code mapping(s)",
        len(account_map), len(tax_code_map)
    )
    return {'account_map': account_map, 'tax_code_map': tax_code_map}


# ---------------------------------------------------------------------------
# Processor callables — Phase 2: group and transform
# ---------------------------------------------------------------------------
def _build_invoice_number(invoice, period, post_seq):
    """Composite InvoiceNumber: '{Invoice}.{Period}.{PostSeq}' (Workato parity)."""
    return f"{invoice}.{period}.{post_seq}"


def _normalise_account(code):
    """Normalise VP account codes that arrive as float strings.

    VP APIs sometimes return integer account codes as '121.00' or '425.0'.
    Only strip the decimal when the fractional part is zero — '122.05' is a
    real decimal account code and must be preserved as-is.
    """
    if code and '.' in code:
        try:
            f = float(code)
            if f == int(f):
                return str(int(f))
        except (ValueError, OverflowError):
            pass
    return code


def group_and_transform_invoices_method():
    """Group PSA Ledger rows into per-invoice structures with Xero mapping applied.

    Workato parity (recipe 3 steps 1-16):
    - Uses SalesInvoice master data (step 1) for IsCreditMemo, DueDate, WBS1-3,
      Description, ClientName — more reliable than PSALedger row fields alone.
    - IsCreditMemo: SalesInvoice.IsCreditMemo=='Y' OR CreditMemoRefNo present.
    - Excludes InvoiceSection='T' rows from invoice lines (step 9 filter).
    - Computes compound effective tax rate per line from VP TaxCodes (steps 11-13):
        effective = (1 + Rate/100) * (1 + CompoundOnTaxCode.Rate/100) - 1
    - TaxAmount per line = abs(Amount) * EffectiveRate (step 16).
    - Determines LineAmountTypes (NoTax vs Exclusive) per invoice.

    Returns list[dict]:
        {
            'Header': { Invoice, InvoiceNumber, WBS1-3, ClientName,
                        TransDate, DueDate, CurrencyCode, IsCreditMemo,
                        CreditMemoRefNo, Period, PostSeq, InvoiceStatus,
                        Description },
            'Lines':  [{ XeroAccountCode, Amount, TaxAmount, XeroTaxName,
                         Description, InvoiceSection }],
            'TaxType': 'NoTax' | 'Exclusive',
        }
    """
    # Step 2 result: all PSA Ledger rows for the batch (includes InvoiceSection='T' rows).
    # Used ONLY for Period+PostSeq grouping keys and invoice header fields; NOT for Lines.
    all_batch_rows = unwrap_vp_response(rail.result('fetch_invoice_batch'), strict=False)

    # Step 1c result: user-visible billing lines from /DataEntry/inDetail/{Batch}.
    # Workato parity: the AR Invoice detail recipe reads inDetail, NOT the PSA Ledger.
    # PSA Ledger also contains GL offset entries (WIP reversals, unbilled-services, etc.)
    # that VP auto-generates and that must not appear as Xero invoice line items.
    detail_raw = unwrap_vp_response(rail.result('fetch_sales_invoice_detail'), strict=False)
    detail_by_invoice = {}
    for _d in detail_raw:
        if not isinstance(_d, dict):
            continue
        _inv = _s(_d.get('Invoice') or _d.get('InvoiceNumber'))
        if _inv:
            detail_by_invoice.setdefault(_inv, []).append(_d)

    # Step 1b result: per-invoice SalesInvoice master records from /DataEntry/inMaster/{Batch}.
    # Returns a flat list of {Invoice, ClientName, WBS1-3, DueDate, CurrencyCode, IsCreditMemo, ...}
    # — one record per invoice in the batch (Workato parity: Deltek connector combines inControl
    # + inMaster; we read inMaster directly here since that is where the per-invoice fields live).
    si_raw = unwrap_vp_response(rail.result('fetch_sales_invoice_master'), strict=False)
    si_map = {}
    for si in si_raw:
        if not isinstance(si, dict):
            continue
        inv_key = _s(si.get('Invoice') or si.get('InvoiceNumber'))
        if inv_key and inv_key not in si_map:
            si_map[inv_key] = si

    # Account + tax code maps from S3 collections
    maps_result = rail.result('fetch_account_tax_maps') or {}
    account_map = maps_result.get('account_map', {})
    tax_code_map = maps_result.get('tax_code_map', {})

    # Step 4 result: VP Tax Codes → compound rate computation (steps 11-13)
    vp_tc_raw = unwrap_vp_response(rail.result('fetch_vp_tax_codes'), strict=False)
    vp_tc_map = _build_vp_tax_codes_map(vp_tc_raw)

    # Resolve zero-rate tax code name from XeroTaxRateOperator result
    tax_rates = _extract_xero_records(rail.result('resolve_zero_rate_tax_code'))
    zero_rate_name = _find_zero_rate_tax_code(tax_rates)

    # CFG_InvoiceStatusSubmitted controls AUTHORISED vs SUBMITTED
    integration_config = _conf().get('config', {}) or {}
    cfg_submitted = str(integration_config.get('CFG_InvoiceStatusSubmitted', 'false')).lower()
    invoice_status = _STATUS_SUBMITTED if cfg_submitted == 'true' else _STATUS_AUTHORISED

    # Step 9: Invoice Line Items = rows where InvoiceSection != 'T' AND Invoice is non-blank.
    # PSALedger includes auto-generated GL accounting entries (AutoEntry='Y', Invoice='') that
    # are not actual invoice lines — filter them out by requiring a non-blank Invoice field.
    non_tax_rows = [
        r for r in all_batch_rows
        if isinstance(r, dict)
        and _s(r.get('InvoiceSection')) != 'T'
        and _s(r.get('Invoice'))
    ]

    # Group non-T rows by (Invoice, Period, PostSeq)
    invoices = {}
    invoice_order = []

    for row in non_tax_rows:
        invoice = _s(row.get('Invoice'))
        period = _s(row.get('Period'))
        post_seq = _s(row.get('PostSeq'))
        key = (invoice, period, post_seq)

        if key not in invoices:
            invoice_order.append(key)
            si = si_map.get(invoice, {})

            # Workato parity: credit note routing is based solely on CreditMemoRefNo
            # being non-blank. IsCreditMemo flag is not used for routing.
            # PSA Ledger rows don't always carry CreditMemoRefNo — fall back to inDetail
            # rows for the same invoice, which reliably contain the field.
            credit_memo_ref = _s(row.get('CreditMemoRefNo'))
            if not credit_memo_ref:
                for _dr in detail_by_invoice.get(invoice, []):
                    _ref = _s(_dr.get('CreditMemoRefNo'))
                    if _ref:
                        credit_memo_ref = _ref
                        break
            is_credit_memo = bool(credit_memo_ref)

            # Header fields: prefer SalesInvoice master, fall back to PSALedger row
            wbs1 = _s(si.get('WBS1') or row.get('WBS1')) if si else _s(row.get('WBS1'))
            wbs2 = _s(si.get('WBS2') or row.get('WBS2')) if si else _s(row.get('WBS2'))
            wbs3 = _s(si.get('WBS3') or row.get('WBS3')) if si else _s(row.get('WBS3'))
            client_name = _s(si.get('ClientName') or row.get('ClientName'))
            trans_date = _s(si.get('TransDate') or row.get('TransDate'))
            due_date = (
                _s(si.get('DueDate') or row.get('DueDate') or row.get('TransDate'))
                if si else
                _s(row.get('DueDate') or row.get('TransDate'))
            )
            currency_code = _s(si.get('CurrencyCode') or row.get('CurrencyCode'))
            inv_desc = _s(si.get('Description') or row.get('Description') or row.get('Desc1'))

            invoices[key] = {
                'Header': {
                    'Invoice': invoice,
                    'InvoiceNumber': _build_invoice_number(invoice, period, post_seq),
                    'WBS1': wbs1,
                    'WBS2': wbs2,
                    'WBS3': wbs3,
                    'ClientName': client_name,
                    'TransDate': trans_date,
                    'DueDate': due_date or trans_date,
                    'CurrencyCode': currency_code,
                    'IsCreditMemo': is_credit_memo,
                    'CreditMemoRefNo': credit_memo_ref if is_credit_memo else '',
                    'Period': period,
                    'PostSeq': post_seq,
                    'InvoiceStatus': invoice_status,
                    'Description': inv_desc,
                },
                'Lines': [],
            }

    # Build Lines from inDetail rows — Workato parity.
    # inDetail = /DataEntry/inDetail/{Batch}: exactly the user-entered billing lines.
    # PSA Ledger rows are NOT used for Lines to avoid GL offset entries being sent to Xero.
    for key in invoice_order:
        invoice, period, post_seq = key
        for row in detail_by_invoice.get(invoice, []):
            if not isinstance(row, dict) or _s(row.get('InvoiceSection')) == 'T':
                continue
            vp_account = _normalise_account(_s(row.get('Account')))
            xero_account_code = account_map.get(vp_account, '')
            if not xero_account_code:
                logger.warning(
                    "No Xero account mapping for VP code '%s' (Invoice %s.%s.%s)",
                    vp_account, invoice, period, post_seq
                )
            vp_tax_code = _s(row.get('TaxCode'))
            xero_tax_name = (
                tax_code_map.get(vp_tax_code, zero_rate_name) if vp_tax_code else zero_rate_name
            )
            try:
                amount = abs(float(row.get('Amount', 0) or 0))
            except (TypeError, ValueError):
                amount = 0.0
            effective_rate = _compute_effective_rate(vp_tax_code, vp_tc_map) if vp_tax_code else 0.0
            tax_amount = round(amount * effective_rate, 6) if effective_rate > 0 else 0.0
            invoices[key]['Lines'].append({
                'XeroAccountCode': xero_account_code,
                'Amount': amount,
                'TaxAmount': tax_amount,
                'XeroTaxName': xero_tax_name,
                'Description': _s(row.get('Description') or row.get('Desc1')),
                'InvoiceSection': _s(row.get('InvoiceSection')),
                'WBS1': _s(row.get('WBS1')),
                'WBS2': _s(row.get('WBS2')),
                'WBS3': _s(row.get('WBS3')),
            })

    # Determine TaxType per invoice: EXCLUSIVE if any line has non-zero TaxAmount.
    # Skip invoices with no inDetail lines — Xero rejects a 0-line invoice/credit note
    # with a 400, which would fail the whole batch after some invoices are already committed.
    result_list = []
    skipped = 0
    for key in invoice_order:
        inv_data = invoices[key]
        lines = inv_data['Lines']
        if not lines:
            logger.warning(
                "Invoice %s has no inDetail lines in Batch %s — skipping to avoid Xero 400",
                inv_data.get('Header', {}).get('Invoice', str(key)),
                _s(_conf().get('Batch')),
            )
            skipped += 1
            continue
        has_non_zero_tax = any(line.get('TaxAmount', 0) > 0 for line in lines)
        inv_data['TaxType'] = _EXCLUSIVE if has_non_zero_tax else _NOTAX
        result_list.append(inv_data)

    logger.info(
        "Grouped %d PSA ledger row(s) into %d invoice(s) (%d skipped — no lines) "
        "using %d inDetail line(s) for Batch %s",
        len(non_tax_rows), len(result_list), skipped, len(detail_raw), _s(_conf().get('Batch'))
    )
    return result_list


# ---------------------------------------------------------------------------
# Processor callables — Phase 3: per-invoice ForEach
# ---------------------------------------------------------------------------
def check_invoice_exists_method():
    """Search Xero for the current ForEach invoice by InvoiceNumber.

    Workato parity: idempotency gate — skip create if invoice already exists.
    Returns list of Xero invoice records (empty list = new invoice).
    """
    context = rail.get_current_context()
    item = rail.result('for_each_invoice') or {}
    header = item.get('Header', {}) if isinstance(item, dict) else {}
    invoice_number = _s(header.get('InvoiceNumber'))
    if not invoice_number:
        logger.warning("check_invoice_exists: current ForEach item has no InvoiceNumber")
        return []

    xero_conn_id = (
        context['dag_run'].conf.get('connections', {}).get('xero', 'xero_default')
    )
    op = rail.XeroInvoiceOperator(
        task_id='_check_invoice_exists',
        xero_conn_id=xero_conn_id,
        operation='search',
        where=f'InvoiceNumber="{invoice_number}"',
        paginate=False,
    )
    records = _extract_xero_records(op.execute(context))
    logger.info(
        "Xero search for InvoiceNumber=%r: found %d record(s)",
        invoice_number, len(records)
    )
    return records


def is_new_invoice_method():
    """IfOperator test: is the current invoice absent from Xero?"""
    existing = rail.result('check_invoice_exists')
    if existing is None:
        return True
    records = existing if isinstance(existing, list) else _extract_xero_records(existing)
    return len(records) == 0


def is_credit_note_method():
    """IfOperator test: is the current ForEach invoice a credit note?

    Workato parity: routing is based on CreditMemoRefNo being non-blank,
    NOT the IsCreditMemo flag. IsCreditMemo is present in the VP schema but
    is not used for routing in the Workato recipe.
    """
    item = rail.result('for_each_invoice')
    if not isinstance(item, dict):
        return False
    return bool(_s(item.get('Header', {}).get('CreditMemoRefNo')))


def build_invoice_body_method():
    """Build the Xero ACCREC Invoice POST body for the current ForEach item.

    Workato parity (014_501_psa_post_invoice_to_xero — invoice branch):
    - Type = ACCREC; Contact.Name = ClientName
    - InvoiceNumber = composite key; Reference = WBS1
    - LineAmountTypes = 'NoTax' or 'Exclusive' (from invoice TaxType)
    - NoTax lines: TaxType='NONE'; Exclusive lines: TaxType from mapped code
    - TaxAmount included per line when non-zero (compound tax parity)
    - Status from CFG_InvoiceStatusSubmitted
    """
    item = rail.result('for_each_invoice') or {}
    header = item.get('Header', {}) if isinstance(item, dict) else {}
    lines = item.get('Lines', []) if isinstance(item, dict) else []
    tax_type = item.get('TaxType', _NOTAX) if isinstance(item, dict) else _NOTAX

    line_items = []
    for line in lines:
        # Workato parity (recipe 4 step 3f): line Description = WBS1.WBS2[.WBS3] formula.
        wbs_parts = [p for p in (
            _s(line.get('WBS1')), _s(line.get('WBS2')), _s(line.get('WBS3'))
        ) if p]
        line_desc = '.'.join(wbs_parts) if wbs_parts else _s(line.get('Description'))
        line_item = {
            'AccountCode': _s(line.get('XeroAccountCode')),
            'Description': line_desc,
            'Quantity': 1,
            'UnitAmount': line.get('Amount', 0.0),
        }
        if tax_type == _EXCLUSIVE:
            # Taxed invoice: TaxType code from map (or 'NONE' for zero-rate).
            # XeroTaxName holds the TaxType CODE after _find_zero_rate_tax_code fix.
            line_item['TaxType'] = _s(line.get('XeroTaxName')) or 'NONE'
            tax_amount = line.get('TaxAmount', 0.0)
            if tax_amount and tax_amount > 0:
                line_item['TaxAmount'] = tax_amount
        else:
            # No-tax invoice: Xero requires TaxType='NONE' on lines with LineAmountTypes='NoTax'.
            line_item['TaxType'] = 'NONE'
        line_items.append(line_item)

    body = {
        'Type': _TYPE_ACCREC,
        'Contact': {'Name': _s(header.get('ClientName'))},
        'Date': _s(header.get('TransDate')),
        'DueDate': _s(header.get('DueDate')) or _s(header.get('TransDate')),
        'Reference': _s(header.get('WBS1')),
        'InvoiceNumber': _s(header.get('InvoiceNumber')),
        'LineAmountTypes': tax_type,
        'Status': _s(header.get('InvoiceStatus')) or _STATUS_AUTHORISED,
        'LineItems': line_items,
    }
    currency = _s(header.get('CurrencyCode'))
    if currency:
        body['CurrencyCode'] = currency
    logger.info(
        "Built ACCREC invoice body: %s (%d line(s), LineAmountTypes=%s)",
        header.get('InvoiceNumber'), len(line_items), tax_type
    )
    return body


def build_credit_note_body_method():
    """Build the Xero ACCRECCREDIT CreditNote POST body for the current ForEach item.

    Workato parity (014_501_psa_post_invoice_to_xero — credit note branch):
    - Type = ACCRECCREDIT; CreditNoteNumber = CreditMemoRefNo
    - LineAmountTypes: follows invoice TaxType (Exclusive when taxed, NoTax when not).
      Workato hardcodes Exclusive but Xero rejects it when all lines have TaxType=NONE.
    - TaxAmount included per line when non-zero (compound tax parity)
    - Status from CFG_InvoiceStatusSubmitted
    """
    item = rail.result('for_each_invoice') or {}
    header = item.get('Header', {}) if isinstance(item, dict) else {}
    lines = item.get('Lines', []) if isinstance(item, dict) else []
    tax_type = item.get('TaxType', _NOTAX) if isinstance(item, dict) else _NOTAX

    line_items = []
    for line in lines:
        # Description: WBS1.WBS2[.WBS3] formula (Workato parity), same as invoice.
        wbs_parts = [p for p in (
            _s(line.get('WBS1')), _s(line.get('WBS2')), _s(line.get('WBS3'))
        ) if p]
        line_desc = (
            '.'.join(wbs_parts) if wbs_parts
            else _s(line.get('Description')) or _s(header.get('Description'))
        )
        line_item = {
            'AccountCode': _s(line.get('XeroAccountCode')),
            'Description': line_desc,
            'Quantity': 1,
            'UnitAmount': line.get('Amount', 0.0),
            # XeroTaxName now holds the TaxType CODE (e.g. 'NONE') from
            # _find_zero_rate_tax_code or map_tax_code. Always set it —
            # EXCLUSIVE LineAmountTypes requires an explicit TaxType per line.
            'TaxType': _s(line.get('XeroTaxName')) or 'NONE',
        }
        tax_amount = line.get('TaxAmount', 0.0)
        if tax_amount and tax_amount > 0:
            line_item['TaxAmount'] = tax_amount
        line_items.append(line_item)

    body = {
        'Type': _TYPE_ACCRECCREDIT,
        'Contact': {'Name': _s(header.get('ClientName'))},
        'Date': _s(header.get('TransDate')),
        'Reference': _s(header.get('WBS1')),
        'LineAmountTypes': tax_type,
        'Status': _s(header.get('InvoiceStatus')) or _STATUS_AUTHORISED,
        'LineItems': line_items,
    }
    # CreditNoteNumber = CreditMemoRefNo (Workato parity).
    # Only reached when CreditMemoRefNo is non-blank (is_credit_note_method guards this).
    credit_note_number = _s(header.get('CreditMemoRefNo'))
    if credit_note_number:
        body['CreditNoteNumber'] = credit_note_number
    currency = _s(header.get('CurrencyCode'))
    if currency:
        body['CurrencyCode'] = currency
    logger.info(
        "Built ACCRECCREDIT credit note body: %s (%d line(s))",
        header.get('CreditMemoRefNo'), len(line_items)
    )
    return body


def allocate_credit_note_method():
    """Allocate the created credit note to the original Xero invoice.

    Workato parity (014_501_psa_post_invoice_to_xero — two-step allocation,
    recipe steps 13-15 + 28-30):
    1. Search VP PSALedger (TransType=IN) by Invoice=CreditMemoRefNo to get
       the original invoice's Period+PostSeq.
    2. Construct original Xero InvoiceNumber as '{Invoice}.{Period}.{PostSeq}'.
    3. Search Xero by that InvoiceNumber to get InvoiceID.
    4. PUT /CreditNotes/{CreditNoteID}/Allocations with Amount + InvoiceID.

    Only fires when invoice_status == AUTHORISED (SUBMITTED invoices cannot
    be allocated). Xero does not support create-and-allocate simultaneously.
    """
    context = rail.get_current_context()
    item = rail.result('for_each_invoice') or {}
    if not isinstance(item, dict):
        logger.warning("allocate_credit_note_method: current ForEach item is not a dict")
        return None

    header = item.get('Header', {})
    invoice_status = _s(header.get('InvoiceStatus', _STATUS_AUTHORISED))

    if invoice_status != _STATUS_AUTHORISED:
        logger.info(
            "Credit note %s has status=%s — allocation only applies to AUTHORISED; skipping",
            header.get('CreditMemoRefNo'), invoice_status
        )
        return None

    # Get CreditNoteID from the create step
    create_result = rail.result('create_xero_credit_note') or {}
    credit_note_records = _extract_xero_records(create_result)
    if not credit_note_records:
        logger.warning(
            "allocate_credit_note_method: create_xero_credit_note returned no records"
        )
        return None
    credit_note_id = _s(credit_note_records[0].get('CreditNoteID'))
    if not credit_note_id:
        logger.warning("allocate_credit_note_method: created CreditNote has no CreditNoteID")
        return None

    credit_memo_ref = _s(header.get('CreditMemoRefNo'))
    vp_conn_id = context['dag_run'].conf.get('connections', {}).get(
        'vantagepoint', 'vantagepoint_default'
    )
    xero_conn_id = context['dag_run'].conf.get('connections', {}).get('xero', 'xero_default')

    # Step 1: Search VP PSALedger for the original invoice by Invoice=CreditMemoRefNo
    # to get Period+PostSeq — Workato parity step 13.
    inv_encoded = quote(credit_memo_ref, safe='')
    eq = quote('=', safe='')
    vp_filter = (
        f"?filterHash[0][name]=Invoice"
        f"&filterHash[0][value]={inv_encoded}"
        f"&filterHash[0][type]=string"
        f"&filterHash[0][opp]={eq}"
        f"&filterHash[0][seq]=0"
    )
    vp_search_op = rail.VantagepointPsaledgerOperator(
        task_id='_vp_search_original_invoice',
        vp_conn_id=vp_conn_id,
        trans_type='IN',
        filters=vp_filter,
    )
    vp_rows = unwrap_vp_response(vp_search_op.execute(context), strict=False)
    orig_row = next((r for r in vp_rows if isinstance(r, dict)), {})
    orig_invoice = _s(orig_row.get('Invoice'))
    orig_period = _s(orig_row.get('Period'))
    orig_post_seq = _s(orig_row.get('PostSeq'))

    if not (orig_invoice and orig_period and orig_post_seq):
        logger.warning(
            "allocate_credit_note_method: VP PSALedger search for Invoice=%r "
            "returned no rows — credit note %s will not be allocated",
            credit_memo_ref, credit_note_id
        )
        return None

    # Step 2: Construct original Xero InvoiceNumber — Workato parity step 14
    original_invoice_number = _build_invoice_number(orig_invoice, orig_period, orig_post_seq)

    # Step 3: Search Xero for the original invoice by InvoiceNumber
    search_op = rail.XeroInvoiceOperator(
        task_id='_search_original_invoice',
        xero_conn_id=xero_conn_id,
        operation='search',
        where=f'InvoiceNumber="{original_invoice_number}"',
        paginate=False,
    )
    original_records = _extract_xero_records(search_op.execute(context))
    if not original_records:
        logger.warning(
            "allocate_credit_note_method: original Xero invoice %r not found — "
            "credit note %s will not be allocated",
            original_invoice_number, credit_note_id
        )
        return None

    original_invoice_id = _s(original_records[0].get('InvoiceID'))
    if not original_invoice_id:
        logger.warning("allocate_credit_note_method: original invoice has no InvoiceID")
        return None

    # Allocation amount = credit note gross total (SubTotal + TaxAmount).
    # Workato parity: Xero's credit note total includes tax for EXCLUSIVE credit notes,
    # so the allocation must cover the full gross amount, not just the subtotal.
    allocate_amount = round(
        sum(
            line.get('Amount', 0.0) + line.get('TaxAmount', 0.0)
            for line in item.get('Lines', [])
        ),
        2,
    )

    # Step 4: PUT /CreditNotes/{CreditNoteID}/Allocations
    allocate_op = rail.XeroCreditNoteOperator(
        task_id='_allocate_credit_note',
        xero_conn_id=xero_conn_id,
        operation='allocate',
        record_id=credit_note_id,
        request_body={
            'Allocations': [{
                'Invoice': {'InvoiceID': original_invoice_id},
                'Amount': allocate_amount,
            }]
        },
    )
    result_raw = allocate_op.execute(context)
    logger.info(
        "Allocated credit note %s (amount=%.2f) to invoice %s (InvoiceNumber=%r)",
        credit_note_id, allocate_amount, original_invoice_id, original_invoice_number
    )
    return result_raw


# ---------------------------------------------------------------------------
# Processor callables — Phase 4: revenue journal
# ---------------------------------------------------------------------------
def is_revenue_configured_method():
    """IfOperator test: is revenue journal creation needed for this batch?

    Workato parity (014_501_psa_vantagepoint_revenue_generation_posts_to_xero
    steps 1+3): True when CFGAutoPosting has UninvoicedRevenue or UnbilledServices
    configured AND at least one fetch_invoice_batch line matches those accounts.
    The pre-check uses fetch_invoice_batch (already available) to avoid an
    unnecessary fetch_revenue_psa_rows task run.
    """
    raw = rail.result('fetch_revenue_accounts')
    rows = unwrap_vp_response(raw, strict=False)
    if not rows:
        return False
    config_row = rows[0] if isinstance(rows, list) else rows
    if not isinstance(config_row, dict):
        return False
    uninvoiced = _normalise_account(_s(config_row.get('UninvoicedRevenue')))
    unbilled = _normalise_account(_s(config_row.get('UnbilledServices')))
    if not uninvoiced and not unbilled:
        return False

    revenue_accounts = {a for a in (uninvoiced, unbilled) if a}
    psa_rows = unwrap_vp_response(rail.result('fetch_invoice_batch'), strict=False)
    return any(
        _normalise_account(_s(row.get('Account'))) in revenue_accounts
        for row in psa_rows
        if isinstance(row, dict)
    )


def build_revenue_journal_body_method():
    """Build the Xero ManualJournal POST body for revenue recognition.

    Workato parity (014_501_psa_vantagepoint_revenue_generation_posts_to_xero
    steps 2-12):
    - Uses fetch_revenue_psa_rows (Period+PostSeq re-fetch, not Batch rows)
    - Filters to UninvoicedRevenue / UnbilledServices lines
    - Validates every filtered line has a Xero account mapping (raises otherwise)
    - Creates ManualJournal with Status=POSTED, LineAmountTypes=NoTax

    Narration format: '{Desc1} Period: {Period}, Post Seq: {PostSeq}'
    """
    raw = rail.result('fetch_revenue_accounts')
    rows = unwrap_vp_response(raw, strict=False)
    config_row = (rows[0] if isinstance(rows, list) and rows else rows) or {}
    uninvoiced = _normalise_account(_s(config_row.get('UninvoicedRevenue')))
    unbilled = _normalise_account(_s(config_row.get('UnbilledServices')))
    revenue_accounts = {a for a in (uninvoiced, unbilled) if a}

    # Use Period+PostSeq-filtered PSA rows (Workato recipe 5 step 2)
    psa_rows = unwrap_vp_response(rail.result('fetch_revenue_psa_rows'), strict=False)
    maps_result = rail.result('fetch_account_tax_maps') or {}
    account_map = maps_result.get('account_map', {})
    batch = _s(_conf().get('Batch'))

    journal_lines = []
    period = ''
    post_seq = ''
    desc1 = ''
    trans_date = ''

    for row in psa_rows:
        if not isinstance(row, dict):
            continue
        account = _normalise_account(_s(row.get('Account')))
        if account not in revenue_accounts:
            continue

        if not period:
            period = _s(row.get('Period'))
            post_seq = _s(row.get('PostSeq'))
            desc1 = _s(row.get('Description') or row.get('Desc1'))
            trans_date = _s(row.get('TransDate'))

        xero_code = account_map.get(account, '')
        if not xero_code:
            raise RuntimeError(
                f"Revenue journal: VP account '{account}' has no Xero account "
                f"mapping in map_chart_of_accounts (Batch={batch}). "
                "Cannot create ManualJournal without a mapped Xero account code."
            )

        try:
            amount = float(row.get('Amount', 0) or 0)
        except (TypeError, ValueError):
            amount = 0.0

        journal_lines.append({
            'AccountCode': xero_code,
            'Description': _s(row.get('Description') or row.get('Desc1')),
            'LineAmount': amount,
        })

    if not journal_lines:
        logger.info(
            "No revenue recognition lines for Batch %s — "
            "no UninvoicedRevenue/UnbilledServices rows in Period+PostSeq PSA re-fetch",
            batch
        )
        return None

    body = {
        'Narration': f"{desc1} Period: {period}, Post Seq: {post_seq}",
        'Date': trans_date,
        'Status': 'POSTED',
        'LineAmountTypes': _NOTAX,
        'JournalLines': journal_lines,
    }
    logger.info(
        "Built revenue ManualJournal for Batch %s: %d line(s), Period=%s, PostSeq=%s",
        batch, len(journal_lines), period, post_seq
    )
    return body


# ---------------------------------------------------------------------------
# Error capture
# ---------------------------------------------------------------------------
def capture_processor_error(batch, error_message):
    """Return an error dict the dispatcher aggregates, or None if called with no error.

    Never raises. trigger_rule='one_failed' on catch_processor_dag_error means this
    task is SKIPPED on the happy path (all upstreams succeed) — a skipped leaf does
    not fail the DAG run, so the child processor run still ends SUCCESS. This callable
    only executes when at least one upstream has failed; the None guard is
    defence-in-depth for the case where the task fires but get_error_message() is empty.
    """
    if not error_message:
        return None
    label = f"Posted Invoices Batch={batch}"
    logger.error("%s — sync failed: %s", label, error_message)
    return {'error': f"{label} — sync failed: {error_message}"}
