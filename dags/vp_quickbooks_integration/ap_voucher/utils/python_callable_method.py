"""
Python callable methods for VP -> QBO AP Voucher Sync.

Ports the Workato AP Voucher bundle
(`C:\\Workspaces\\vp_qbo_workato\\ap_voucher\\`):
  - `014_503_psa_poll_vantagepoint_posted_ap_voucher`        (poll/route)
  - `014_503_psa_vantagepoint_ap_voucher_exports_to_quickbooks_us`     (US dispatcher)
  - `014_503_psa_vantagepoint_ap_voucher_exports_to_quickbooks_ca_uk`  (CA-UK dispatcher)
  - `014_503_psa_post_ap_voucher_to_quickbooks_us`           (US processor)
  - `014_503_psa_post_ap_voucher_to_quickbooks_ca_uk`        (CA-UK processor)
  - `014_503_psa_quickbooks_add_bill`                        (shared CA-UK bill builder)

into Python callables for a 4-DAG Airflow topology:
  main (region-agnostic) -> dispatcher (region-agnostic, routes by
  CFG_Region) -> {us_processor | ca_uk_processor}.

Direction: VP -> QBO, create-only (QBO Bill). Polls PSALedger
(TransType='AP'), groups line rows by (Period, PostSeq) into one voucher,
enriches with firm/account/tax lookups, and POSTs a QBO Bill. CA-UK adds a
LedgerTax fetch plus compound-tax math; US uses a binary TAX/NON tax code.

Lookup tables read from the shared mapping_sync S3 collections (the same
tables Workato's recipes use), under the FIXED 'mapping_sync' integration_type
partition:
  - `map_firm`         (FirmID -> QBOID/IsVendor/Name)        READ-only
  - `map_account_code` (VantagepointCode -> QBO account id)   READ-only
  - `map_tax_code`     (VantagepointCode -> QBO tax refs)     READ-only, may
                        fan out (one VP tax code -> several QBO rate rows)
  - `outstanding_purchase_invoices` (Batch/Voucher -> QBO Bill id) READ+WRITE:
                        the dedup guard reads it; a successful Bill create
                        writes one row per line (InvoiceID = QBO Bill Id), which
                        the bill_payment_sync integration later consumes.

Re-run safety is watermark-only (no posted-voucher map). Validation gaps
raise from the processor; the dispatcher's per-child error capture keeps
the watermark behind on any failure so the same window re-polls next run.
"""
# pylint: disable=invalid-name,broad-exception-caught,too-many-locals
import logging
from decimal import Decimal, InvalidOperation
from functools import lru_cache
from urllib.parse import quote
import rail
# Shared collection-access helpers + table-name / column constants live in
# `common` so the S3 access logic and SQLite identifiers can't drift across
# integrations (one canonical home — see common/python_callable_method.py).
from vp_quickbooks_integration.common.python_callable_method import (
    collection_rows,
    collection_operations,
    unwrap_vp_response,
)
from vp_quickbooks_integration.common.tables import (
    MAP_ACCOUNT_CODE_TABLE_NAME,
    MAP_FIRM_TABLE_NAME,
    MAP_TAX_CODE_TABLE_NAME,
    OUTSTANDING_PURCHASE_INVOICES_TABLE_NAME,
    OUTSTANDING_PURCHASE_INVOICES_COLUMNS,
)

logger = logging.getLogger(__name__)


@lru_cache(maxsize=1)
def _vp_modules():
    """Lazy-load the rail submodules that trigger expensive operator-package
    discovery at import time.

    `from rail.hooks.vantagepoint_hook import VantagepointHook` and
    `from rail.operators.vantagepoint import utils` cause
    `rail.operators.__init__` to evaluate, which walks every operator
    subpackage on disk — including ones that may not exist on a given
    deployment (e.g. `xero_internal`). Each missing-path filesystem stat
    compounds and pushes DAG parse past Airflow's 30s
    `dagbag_import_timeout`.

    Wrapping the imports in this `@lru_cache(maxsize=1)` loader keeps them
    in one place (no duplication across consumers) and defers them to
    first call — by which point the DAG has already parsed. Subsequent
    calls hit the cache for free.
    """
    from rail.hooks.vantagepoint_hook import VantagepointHook
    from rail.operators.vantagepoint import utils as vp_utils
    return VantagepointHook, vp_utils


# The four Workato lookup tables live in the shared mapping_sync S3 collections
# (map_firm, map_account_code, map_tax_code, outstanding_purchase_invoices),
# accessed via the shared helpers in common.python_callable_method.


# ---------------------------------------------------------------------------
# Generic helpers shared across dispatcher + processor
# ---------------------------------------------------------------------------
def _vp_conn_id():
    """Pull the VP connection id out of the running DAG's conf."""
    conf = rail.get_current_context()['dag_run'].conf
    connections = conf.get('connections') or {}
    return connections.get('vantagepoint')


def _conf_value(key, default=''):
    """Fetch a single key out of the current dag_run.conf."""
    conf = rail.get_current_context()['dag_run'].conf
    return conf.get(key) if conf.get(key) is not None else default


# Collection access (read map_*/outstanding_purchase_invoices, write the
# outstanding tracker) uses the shared helpers in common.python_callable_method
# — `collection_rows` / `collection_operations`, imported above and called
# directly. The outstanding-invoice write is a one-to-many replace (DELETE this
# voucher's rows + INSERT the current lines), so it goes through
# `collection_operations` (atomic operations batch), not a single upsert.


def _line_amount_decimal(line):
    """Parse line.Amount as a Decimal (PSALedger amounts may be strings)."""
    raw = line.get('Amount')
    if raw is None or raw == '':
        return Decimal('0')
    try:
        return Decimal(str(raw))
    except (InvalidOperation, ValueError):
        return Decimal('0')


# ---------------------------------------------------------------------------
# Region routing (Workato poll recipe `CFG_Region` branch)
#
# The Workato poll recipe `014_503_psa_poll_vantagepoint_posted_ap_voucher`
# reads the account property `014_503_PSA.CFG_Region` and routes US to the US
# recipes and CA/UK to the (combined) CA-UK recipes. Here that branch lives in
# the single dispatcher: it reads CFG_Region (delivered in the integration
# `config` field, spread into the dag_run.conf) and routes each voucher to the
# region-specific processor DAG. CA and UK share the `ca_uk` processor.
# ---------------------------------------------------------------------------
_REGION_SLUG_BY_CFG_REGION = {
    'US': 'us',
    'CA': 'ca_uk',
    'UK': 'ca_uk',
}


def resolve_region_slug_method(cfg_region):
    """Map a CFG_Region value (US/CA/UK) to a processor slug (us/ca_uk).

    Raises on an unknown/blank region rather than silently defaulting — a
    misconfigured tenant should fail loudly at routing time, not post a US
    Bill for a CA tenant (wrong tax treatment).
    """
    key = (cfg_region or '').strip().upper()
    slug = _REGION_SLUG_BY_CFG_REGION.get(key)
    if not slug:
        raise RuntimeError(
            f"Unknown/unsupported CFG_Region {cfg_region!r} — expected one "
            f"of {sorted(_REGION_SLUG_BY_CFG_REGION)}. Cannot route this "
            "tenant's AP vouchers to a processor."
        )
    return slug


def cfg_region_from_conf():
    """Read CFG_Region out of the running DAG's conf.

    Middleware delivers the integration record's `config` object spread into
    the dag_run.conf, so CFG_Region may appear at `conf['config']['CFG_Region']`
    or, defensively, at the conf top level. Returns '' when absent.
    """
    conf = rail.get_current_context()['dag_run'].conf or {}
    config_obj = conf.get('config') or {}
    return (
        config_obj.get('CFG_Region')
        or conf.get('CFG_Region')
        or ''
    )


# ---------------------------------------------------------------------------
# Dispatcher: PSALedger PostDate watermark filter + voucher grouping
# ---------------------------------------------------------------------------
def build_vp_psaledger_ap_filter_method():
    """filterHash for the dispatcher's PSALedger AP poll.

    Two-sided `last <= PostDate < current` window so each poll claims a
    closed lower / open upper interval and the watermark advance is gap- and
    overlap-free. PSALedger rows carry no `ModDate`; `PostDate` is the commit
    timestamp (never moves backwards), while `TransDate` is the business date
    and can be backdated — so `PostDate` is the correct cursor. Identical to
    `journal_entry_sync.build_vp_psaledger_filter_method` (the AP vs JE split
    is in the operator's `trans_type`, not in this filter).
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


def extract_ap_vouchers_list_method():
    """Group PSALedger AP line rows by (Period, PostSeq, Voucher).

    The Workato US dispatcher groups headers by
    `Period, PostSeq, RefNo, Voucher, Desc1, Vendor, InvoiceNumber` and the
    per-voucher line query filters on `Period AND PostSeq AND Voucher`
    (recipe step 16). So the voucher grain is (Period, PostSeq, Voucher) — a
    single (Period, PostSeq) can carry more than one Voucher, and each Voucher
    becomes its own QBO Bill. We poll the PSALedger directly and get
    line-level rows back, so we collapse them into voucher identities here.

    Header fields (Batch, InvoiceNumber, Vendor, RefNo, Desc1, TransDate) are
    carried from the first line of each group for the processor's
    dag_run.conf — the processor still re-fetches all lines by
    (Period, PostSeq, Voucher) for the detail.

    Returns a list of dicts shaped for the processor conf:
        [{'Period','PostSeq','Voucher','Batch','InvoiceNumber','Vendor',
          'RefNo','Desc1','FirstTransDate','RowCount'}, ...]
    """
    raw = rail.result('get_changed_psaledger_ap_rows')
    rows = unwrap_vp_response(raw, strict=True)
    grouped = {}
    # (Period, PostSeq, Voucher) is the voucher primary key. Track rows
    # missing any of them (or non-dict) so a non-zero `skipped` count surfaces
    # in the task log — silently dropping would let the watermark advance past
    # vouchers that never reached QBO with no breadcrumb to debug. Workato
    # also requires Voucher != "" (recipe step 16 WHERE clause).
    skipped = 0
    for row in rows:
        if not isinstance(row, dict):
            skipped += 1
            continue
        period = row.get('Period')
        post_seq = row.get('PostSeq')
        voucher = row.get('Voucher')
        if period is None or post_seq is None or not voucher:
            skipped += 1
            continue
        key = (str(period), str(post_seq), str(voucher))
        entry = grouped.get(key)
        if entry is None:
            grouped[key] = {
                'Period': str(period),
                'PostSeq': str(post_seq),
                'Voucher': str(voucher),
                'Batch': row.get('Batch') or '',
                'InvoiceNumber': row.get('InvoiceNumber') or '',
                'Vendor': row.get('Vendor') or '',
                'RefNo': row.get('RefNo') or '',
                'Desc1': row.get('Desc1') or '',
                'FirstTransDate': row.get('TransDate') or '',
                'RowCount': 1,
            }
        else:
            entry['RowCount'] += 1
    vouchers = list(grouped.values())
    summary = (
        f"Grouped {len(rows)} PSALedger AP rows into {len(vouchers)} "
        f"unique (Period, PostSeq, Voucher) vouchers"
    )
    if skipped:
        summary += (
            f" — WARNING: skipped {skipped} rows "
            "(missing Period/PostSeq/Voucher or non-dict shape)"
        )
    logger.info(summary)
    return vouchers


def check_if_ap_vouchers_exist_method():
    """IfOperator test: did the PSALedger poll surface any vouchers?"""
    return len(rail.result('extract_ap_vouchers_list') or []) > 0


# ---------------------------------------------------------------------------
# Processor: per-(Period, PostSeq) PSALedger fetch filter + unwrap
# ---------------------------------------------------------------------------
def build_psaledger_period_postseq_ap_filter_method():
    """Re-fetch all lines of this exact voucher by (Period, PostSeq, Voucher).

    Three AND'd filterHash clauses: Period + PostSeq as `type=int` exact-match
    (integers on the wire), Voucher as a string exact-match — the form VP's
    PSALedger endpoint actually honors (direct `?Period=X` query params are
    silently ignored and return every row). Mirrors the Workato US dispatcher
    step-16 line query `Period = X AND PostSeq = Y AND Voucher = "Z"`.
    """
    period_value = _conf_value('Period')
    post_seq_value = _conf_value('PostSeq')
    voucher_value = _conf_value('Voucher')
    if not period_value or not post_seq_value or not voucher_value:
        raise RuntimeError(
            "Processor dag_run.conf missing Period/PostSeq/Voucher — got "
            f"Period={period_value!r}, PostSeq={post_seq_value!r}, "
            f"Voucher={voucher_value!r}. Refusing to query PSALedger with an "
            "empty voucher identity."
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


def extract_psaledger_lines_method():
    """Unwrap the per-voucher PSALedger re-fetch into a list of line dicts."""
    raw = rail.result('get_psaledger_lines_for_voucher')
    rows = unwrap_vp_response(raw, strict=True)
    lines = [r for r in rows if isinstance(r, dict)]
    period = _conf_value('Period')
    post_seq = _conf_value('PostSeq')
    logger.info(
        f"PSALedger AP voucher (Period={period}, PostSeq={post_seq}) "
        f"has {len(lines)} lines"
    )
    if not lines:
        raise RuntimeError(
            f"No PSALedger lines found for (Period={period}, "
            f"PostSeq={post_seq}) — this voucher was surfaced by the "
            "dispatcher's poll but the per-voucher re-fetch returned "
            "zero rows. Refusing to post an empty Bill."
        )
    return lines


def build_ledgertax_period_postseq_filter_method():
    """CA-UK only: fetch this voucher's tax detail lines from VP /LedgerTax/ap.

    Mirrors the Workato CA-UK dispatcher ledgertax query (TransType + Period +
    PostSeq). Period/PostSeq are integers on the wire; same exact-match
    filterHash shape as the PSALedger fetch.
    """
    period_value = _conf_value('Period')
    post_seq_value = _conf_value('PostSeq')
    if not period_value or not post_seq_value:
        raise RuntimeError(
            "Processor dag_run.conf missing Period or PostSeq — cannot query "
            f"LedgerTax (Period={period_value!r}, PostSeq={post_seq_value!r})."
        )
    period = quote(str(period_value), safe='')
    post_seq = quote(str(post_seq_value), safe='')
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


def extract_ledgertax_lines_method():
    """CA-UK only: unwrap + NORMALIZE the LedgerTax re-fetch into tax-line dicts.

    The compound-tax math joins on PKey and reads `TaxCode` + `TaxAmount`. But
    VP's LedgerTax rows carry the amount under `TaxAmountSourceCurrency` — the
    Workato CA-UK dispatcher SQL selects `tlm.TaxAmountSourceCurrency [TaxAmount]`
    (recipe line 6202), i.e. it ALIASES the source-currency amount to TaxAmount.
    We replicate that here: emit `{PKey, TaxCode, TaxAmount}` with TaxAmount
    taken from TaxAmountSourceCurrency (falling back to a native TaxAmount field
    if a deployment exposes one). Without this normalization every tax amount
    would read as 0 and the tax block would be wrong.

    Empty is valid: a voucher with no tax lines posts
    GlobalTaxCalculation='NotApplicable' and no TxnTaxDetail.
    """
    raw = rail.result('get_ledgertax_for_voucher')
    rows = unwrap_vp_response(raw, strict=False)
    tax_lines = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        amount = row.get('TaxAmountSourceCurrency')
        if amount is None or amount == '':
            amount = row.get('TaxAmount')
        tax_lines.append({
            'PKey': row.get('PKey'),
            'TaxCode': (row.get('TaxCode') or '').strip(),
            'TaxAmount': amount,
        })
    period = _conf_value('Period')
    post_seq = _conf_value('PostSeq')
    logger.info(
        f"LedgerTax for AP voucher (Period={period}, PostSeq={post_seq}) "
        f"has {len(tax_lines)} tax lines"
    )
    return tax_lines


# ---------------------------------------------------------------------------
# Lookup-table loading from the shared mapping_sync S3 collections.
# ---------------------------------------------------------------------------
def load_lookup_tables_method():
    """Load the lookup tables from the shared mapping_sync S3 collections.

    Workato parity: the recipes do search_entries / get_entries over Map Firm,
    Map Account Code and Map Tax Code. We load every row once and build the
    in-memory structures the downstream tasks consume, so enrich / validate /
    bill-build / compound-tax stay unchanged:
      - firm_map:    {FirmID -> {QBOID, IsVendor, Name}}              (1 row)
      - account_map: {VantagepointCode -> {QBOID, QBOName, QBOCode}}  (1 row)
      - tax_code_map:{VantagepointCode -> [ {TaxCodeRef, TaxRateRef, Rate,
                       TaxTypeApplicable, IsTaxGroup, TaxOn}, ... ]}

    firm/account are 1:1 (Workato `search_entries`); on a rare duplicate key we
    keep the first row and log. `tax_code_map` is a LIST per VP code because
    Workato CA-UK loads ALL tax rows (`get_entries`) and a single VP tax code
    can map to SEVERAL QBO rate rows (tax groups / compound tax) — the
    compound-tax math fans out over them.
    """
    context = rail.get_current_context()

    firm_map = {}
    for r in collection_rows(
        MAP_FIRM_TABLE_NAME, ['FirmID', 'QBOID', 'IsVendor', 'Name'],
        '1 = 1', [], context,
    ):
        firm_id = (r.get('FirmID') or '').strip()
        if not firm_id:
            continue
        if firm_id in firm_map:
            logger.warning(
                "Duplicate map_firm row for FirmID %r — keeping the first.",
                firm_id
            )
            continue
        firm_map[firm_id] = {
            'QBOID': r.get('QBOID') or '',
            'IsVendor': r.get('IsVendor') or '',
            'Name': r.get('Name') or '',
        }

    account_map = {}
    for r in collection_rows(
        MAP_ACCOUNT_CODE_TABLE_NAME,
        ['VantagepointCode', 'QBOID', 'QBOName', 'QBOCode'],
        '1 = 1', [], context,
    ):
        code = (r.get('VantagepointCode') or '').strip()
        if not code:
            continue
        if code in account_map:
            logger.warning(
                "Duplicate map_account_code row for VantagepointCode %r — "
                "keeping the first.", code
            )
            continue
        account_map[code] = {
            'QBOID': r.get('QBOID') or '',
            'QBOName': r.get('QBOName') or '',
            'QBOCode': r.get('QBOCode') or '',
        }

    tax_code_map = {}
    for r in collection_rows(
        MAP_TAX_CODE_TABLE_NAME,
        ['VantagepointCode', 'QBOCodeID', 'QBORateID', 'Rate',
         'TaxTypeApplicable', 'IsTaxGroup', 'TaxOn'],
        '1 = 1', [], context,
    ):
        code = (r.get('VantagepointCode') or '').strip()
        if not code:
            continue
        tax_code_map.setdefault(code, []).append({
            'TaxCodeRef': r.get('QBOCodeID') or '',
            'TaxRateRef': r.get('QBORateID') or '',
            'Rate': r.get('Rate') or '',
            'TaxTypeApplicable': r.get('TaxTypeApplicable') or '',
            'IsTaxGroup': r.get('IsTaxGroup') or '',
            'TaxOn': r.get('TaxOn') or '',
        })

    logger.info(
        "Loaded lookup tables from mapping_sync S3 collections: "
        "firm_map=%d entries, account_map=%d entries, "
        "tax_code_map=%d VP codes (%d rows)",
        len(firm_map), len(account_map), len(tax_code_map),
        sum(len(v) for v in tax_code_map.values())
    )
    return {
        'firm_map': firm_map,
        'account_map': account_map,
        'tax_code_map': tax_code_map,
    }


# ---------------------------------------------------------------------------
# WBS1 -> project/client lookup (Workato `get_project_clients` helper) used to
# derive each line's CustomerRef (project's client firm -> QBO customer id).
# Ported from journal_entry_sync (identical batched VP /api/project fetch).
# ---------------------------------------------------------------------------
# Mirrors the Workato `014_503_psa_get_project_clients` helper recipe, which
# batches WBS1 codes in groups of 10 to keep VP /api/project URLs under VP's
# length cap.
_PROJECT_CLIENTS_BATCH_SIZE = 10

# Delimiter folding the composite (WBS1, WBS2, WBS3) key into one string for
# XCom JSON storage (JSON rejects tuple keys). Unit Separator (\x1f) is
# collision-free against alphanumeric/dotted WBS codes.
_WBS_KEY_DELIMITER = '\x1f'


def extract_unique_wbs1_method():
    """Dedupe non-empty WBS1 values from the PSALedger lines."""
    lines = rail.result('extract_psaledger_lines') or []
    unique = sorted({
        (line.get('WBS1') or '').strip()
        for line in lines
        if (line.get('WBS1') or '').strip()
    })
    logger.info(f"Unique WBS1 codes in this voucher: {len(unique)}")
    return unique


def _build_wbs1_batch_filter(batch):
    """filterHash query string for one batch of WBS1 codes.

    Mirrors the Workato `get_project_clients` recipe block exactly — only the
    `name`/`value` clauses per WBS1, joined by `&`, with no explicit
    `type`/`opp`/`seq`/`condition` so VP defaults treat repeated same-field
    clauses as OR ("any project whose WBS1 is in this batch").
    """
    parts = ['fieldFilter=WBSNumber,WBS1,WBS2,WBS3,Name,ClientID']
    for index, wbs1 in enumerate(batch):
        parts.append(f"filterHash[{index}][name]=WBS1")
        parts.append(f"filterHash[{index}][value]={quote(wbs1, safe='')}")
    return '?' + '&'.join(parts)


def get_project_clients_from_vp_method():
    """Fetch WBS1 -> ClientID rows from VP /api/project, batched 10 per call.

    A single VantagepointProjectOperator task can't loop, so we use
    VantagepointHook directly and accumulate per batch. Returns a flat list
    of project dicts (WBS1, WBS2, WBS3, Name, ClientID).
    """
    unique_wbs1 = rail.result('extract_unique_wbs1') or []
    if not unique_wbs1:
        logger.info("No WBS1 codes — skipping VP /api/project lookup")
        return []

    vp_conn_id = _vp_conn_id()
    if not vp_conn_id:
        raise RuntimeError(
            "No `connections.vantagepoint` in dag_run.conf — cannot "
            "query VP /api/project for customer resolution."
        )
    VantagepointHook, vp_utils = _vp_modules()
    vp_client = VantagepointHook(vp_conn_id)

    log = rail.get_current_context()['task'].log
    accumulated = []
    batch_size = _PROJECT_CLIENTS_BATCH_SIZE
    for batch_idx in range(0, len(unique_wbs1), batch_size):
        batch = unique_wbs1[batch_idx:batch_idx + batch_size]
        filters = _build_wbs1_batch_filter(batch)
        log.info(
            "Fetching VP /api/project batch %d (%d WBS1 codes)",
            (batch_idx // batch_size) + 1, len(batch),
        )
        raw = vp_utils.execute_api_request(
            vp_client=vp_client,
            endpoint='/project',
            request_method='GET',
            filters=filters,
            request_body=None,
            pagination=True,
            log=log,
            base_path='/api',
        )
        rows = unwrap_vp_response(raw, strict=False)
        accumulated.extend(r for r in rows if isinstance(r, dict))

    logger.info(
        f"VP /api/project returned {len(accumulated)} project rows "
        f"for {len(unique_wbs1)} unique WBS1 codes"
    )
    return accumulated


def _wbs_key(wbs1, wbs2, wbs3):
    """Build the project-index dict key from three WBS components."""
    return (
        f"{(wbs1 or '').strip()}"
        f"{_WBS_KEY_DELIMITER}{(wbs2 or '').strip()}"
        f"{_WBS_KEY_DELIMITER}{(wbs3 or '').strip()}"
    )


def build_project_client_index_method():
    """Index project-client rows by WBS1/WBS2/WBS3 composite key.

    Mirrors the Workato `LEFT JOIN psa.WBS1=proj.WBS1 AND WBS2 AND WBS3` step.
    Empty WBS2/WBS3 slots are normalized via `.strip()` for exact-match
    dict lookups against the PSALedger line WBS values.
    """
    rows = rail.result('get_project_clients_from_vp') or []
    index = {}
    for row in rows:
        key = _wbs_key(row.get('WBS1'), row.get('WBS2'), row.get('WBS3'))
        index[key] = {
            'ClientID': (row.get('ClientID') or '').strip(),
            'Name': row.get('Name') or '',
        }
    logger.info(f"Built project-client index with {len(index)} entries")
    return index


# ---------------------------------------------------------------------------
# Vendor resolution (Workato US processor step 4 firm search + step 7 map_firm)
# ---------------------------------------------------------------------------
# The voucher header carries a `Vendor` code, but map_firm is keyed by the
# firm's ClientID (a GUID). The Workato US processor first SEARCHES VP firms by
# the Vendor code (step 4, `firm` connector `verb: search`, input `Vendor`) to
# translate the code into a ClientID, then looks that ClientID up in map_firm
# (step 7, search_entries col1) to get the QBO VendorRef id.
#
# VERIFIED (2026-06-12, live VP /firm response): the firm record's `Vendor`
# field holds the AP voucher's vendor code (e.g. firm "Test Firm 001" has
# `Vendor: "000147"`), and filtering /firm by it returns exactly the matching
# firm (a single-element array, not the whole firm list). So `Vendor` is the
# correct filter field — pinned. (The firm record also carries `ClientID`,
# `VendorInd: "Y"`, and even its own `QBOID`, but we resolve the QBO vendor id
# via the firm_map lookup keyed on ClientID, per Workato parity.)
_VP_FIRM_VENDOR_FILTER_FIELD = 'Vendor'


def _vp_firm_search_by_vendor(vendor_code):
    """GET VP /firm filtered by the voucher Vendor code; return first record.

    Returns the firm dict (carrying ClientID) or {} when not found.
    """
    vp_conn_id = _vp_conn_id()
    if not vp_conn_id:
        raise RuntimeError(
            "No `connections.vantagepoint` in dag_run.conf — cannot "
            "search VP firms for vendor resolution."
        )
    VantagepointHook, vp_utils = _vp_modules()
    vp_client = VantagepointHook(vp_conn_id)
    log = rail.get_current_context()['task'].log

    value = quote(str(vendor_code), safe='')
    eq = quote('=', safe='')
    filters = (
        f"?filterHash[0][name]={_VP_FIRM_VENDOR_FILTER_FIELD}"
        f"&filterHash[0][value]={value}"
        f"&filterHash[0][opp]={eq}"
        f"&filterHash[0][seq]=0"
    )
    raw = vp_utils.execute_api_request(
        vp_client=vp_client,
        endpoint='/firm',
        request_method='GET',
        filters=filters,
        request_body=None,
        pagination=False,
        log=log,
        base_path='/api',
    )
    rows = unwrap_vp_response(raw, strict=False)
    for row in rows:
        if isinstance(row, dict):
            return row
    return {}


def resolve_firm_vendorref_method():
    """Resolve the voucher's Vendor code to a QBO VendorRef via map_firm.

    Vendor code -> VP /firm search -> firm.ClientID -> firm_map[ClientID] ->
    {QBOID, IsVendor, Name}. Raises (per Workato CompoundError) when the firm
    is not found in VP or not mapped in firm_map, so a missing vendor fails
    this one voucher's processor (siblings continue, watermark held).

    Returns {'QBOVendorID', 'FirmName', 'ClientID', 'IsVendor'} for the
    bill-builder header.
    """
    vendor_code = _conf_value('Vendor')
    period = _conf_value('Period')
    post_seq = _conf_value('PostSeq')
    if not vendor_code:
        raise RuntimeError(
            f"AP voucher (Period={period}, PostSeq={post_seq}) has no Vendor "
            "code — cannot resolve a QuickBooks VendorRef for the Bill."
        )

    firm = _vp_firm_search_by_vendor(vendor_code)
    client_id = (firm.get('ClientID') or '').strip()
    if not client_id:
        raise RuntimeError(
            f"Vendor firm code {vendor_code} not found in Vantagepoint "
            f"(Period={period}, PostSeq={post_seq})."
        )

    firm_map = (rail.result('load_lookup_tables') or {}).get('firm_map') or {}
    firm_row = firm_map.get(client_id) or {}
    qbo_vendor_id = (firm_row.get('QBOID') or '').strip()
    if not qbo_vendor_id:
        raise RuntimeError(
            f"Vantagepoint firm {firm.get('Name') or client_id} "
            "not matched to a QuickBooks vendor "
            f"(ClientID={client_id}, Period={period}, PostSeq={post_seq})."
        )

    is_vendor = (firm_row.get('IsVendor') or '').strip()
    resolved = {
        'QBOVendorID': qbo_vendor_id,
        'FirmName': firm_row.get('Name') or firm.get('Name') or '',
        'ClientID': client_id,
        'IsVendor': is_vendor,
    }
    logger.info(
        f"Resolved VendorRef for voucher (Period={period}, PostSeq={post_seq}): "
        f"Vendor={vendor_code} -> ClientID={client_id} -> QBOID={qbo_vendor_id} "
        f"(IsVendor={is_vendor or 'n/a'})"
    )
    return resolved


# ---------------------------------------------------------------------------
# Enrich + validate (shared by US and CA-UK processors)
# ---------------------------------------------------------------------------
def enrich_lines_method():
    """Attach QBO account + customer refs and tax fields to each line.

    Per line:
      _AccountCode    : VP Account code (lookup key)
      _QBOAccountID   : account_map[Account].QBOID  (QBO AccountRef value)
      _QBOAccountName : account_map[Account].QBOName
      _ClientID       : project's client (WBS1/2/3 -> project_index.ClientID)
      _QBOCustomerID  : firm_map[_ClientID].QBOID   (QBO CustomerRef value)
      _CustomerName   : firm_map[_ClientID].Name
      _TaxCode        : raw VP tax code (drives US TAX/NON and CA-UK mapping)
      _HasTax         : bool(_TaxCode)
      _Amount         : OriginalAmountSourceCurrency (fallback Amount)
      _Description    : Desc2

    The CA-UK processor layers compound-tax math on top of these in its
    bill builder; the US builder consumes them directly.
    """
    lines = rail.result('extract_psaledger_lines') or []
    lookups = rail.result('load_lookup_tables') or {}
    account_map = lookups.get('account_map') or {}
    firm_map = lookups.get('firm_map') or {}
    project_index = rail.result('build_project_client_index') or {}

    enriched = []
    for line in lines:
        account_code = (line.get('Account') or '').strip()
        account_row = account_map.get(account_code) or {}

        wbs_key = _wbs_key(
            line.get('WBS1'), line.get('WBS2'), line.get('WBS3'),
        )
        project_row = project_index.get(wbs_key) or {}
        client_id = project_row.get('ClientID') or ''
        customer_row = firm_map.get(client_id) if client_id else {}
        customer_row = customer_row or {}

        tax_code = (line.get('TaxCode') or '').strip()
        amount_raw = line.get('OriginalAmountSourceCurrency')
        if amount_raw is None or amount_raw == '':
            amount_raw = line.get('Amount')

        enriched.append({
            **line,
            '_AccountCode': account_code,
            '_QBOAccountID': account_row.get('QBOID') or '',
            '_QBOAccountName': account_row.get('QBOName') or '',
            '_ClientID': client_id,
            '_QBOCustomerID': customer_row.get('QBOID') or '',
            '_CustomerName': customer_row.get('Name') or '',
            '_TaxCode': tax_code,
            '_HasTax': bool(tax_code),
            '_Amount': amount_raw,
            '_Description': (line.get('Desc2') or '').strip(),
        })
    logger.info(f"Enriched {len(enriched)} lines with account + customer + tax refs")
    return enriched


def validate_enriched_lines_method():
    """Walk the enriched lines and raise on any mapping gap (per voucher).

    Accumulates every gap into one message (Workato CompoundError style) so
    operators fix all of them in a single map edit. Scoped to this voucher's
    (Period, PostSeq) so a bad voucher does not poison siblings.

    Checks: every line needs a mapped QBO account; CA-UK additionally needs
    every taxed line's VP tax code present in tax_code_map.
    """
    enriched = rail.result('enrich_lines') or []
    period = _conf_value('Period')
    post_seq = _conf_value('PostSeq')
    if not enriched:
        raise RuntimeError(
            f"Refusing to post a Bill with zero lines "
            f"(Period={period}, PostSeq={post_seq})."
        )

    tax_code_map = (
        rail.result('load_lookup_tables') or {}
    ).get('tax_code_map') or {}
    region_slug = resolve_region_slug_method(_conf_value('CFG_Region'))

    errors = []
    for line in enriched:
        if not line.get('_QBOAccountID'):
            errors.append(
                f"Vantagepoint account {line.get('_AccountCode')} "
                "not matched to a QuickBooks account "
                f"(line PKey={line.get('PKey')})."
            )
        elif not line.get('_QBOAccountName'):
            errors.append(
                f"Account map row for {line.get('_AccountCode')} is missing "
                f"QBOName (line PKey={line.get('PKey')})."
            )
        # CA-UK posts a real QBO TaxCodeRef/TaxRateRef from the tax_code_map;
        # a taxed line with an unmapped VP tax code would silently drop tax.
        # US uses the binary TAX/NON and needs no tax_code_map entry.
        if (
            region_slug == 'ca_uk'
            and line.get('_HasTax')
            and line.get('_TaxCode') not in tax_code_map
        ):
            errors.append(
                f"Vantagepoint tax code {line.get('_TaxCode')} "
                "not matched to a QuickBooks tax code "
                f"(line PKey={line.get('PKey')})."
            )

    if errors:
        raise RuntimeError(
            f"AP voucher (Period={period}, PostSeq={post_seq}) "
            f"failed validation:\n  - " + "\n  - ".join(errors)
        )
    logger.info(
        f"Validation passed for (Period={period}, PostSeq={post_seq}): "
        f"{len(enriched)} lines"
    )
    return enriched


# ---------------------------------------------------------------------------
# US bill builder (Workato `014_503_psa_post_ap_voucher_to_quickbooks_us`
# create_bill_v2, recipe lines 7913-7927). Binary TAX/NON, no TxnTaxDetail,
# no GlobalTaxCalculation.
# ---------------------------------------------------------------------------
def _normalize_txn_date(raw):
    """PSALedger TransDate -> bare `YYYY-MM-DD` for QBO TxnDate/DueDate."""
    if not raw:
        return ''
    text = str(raw)
    if 'T' in text:
        return text.split('T', 1)[0]
    return text[:10]


def _default_payment_period_days():
    """CFG_DefaultPaymentPeriod (account property) -> int days for DueDate.

    Delivered in the integration `config` field (spread into dag_run.conf) or
    at the conf top level; falls back to config.default_payment_period_days
    when absent/non-numeric.
    """
    from vp_quickbooks_integration.ap_voucher.config import (
        default_payment_period_days,
    )
    conf = rail.get_current_context()['dag_run'].conf or {}
    config_obj = conf.get('config') or {}
    raw = (
        config_obj.get('CFG_DefaultPaymentPeriod')
        or conf.get('CFG_DefaultPaymentPeriod')
    )
    try:
        return int(str(raw))
    except (TypeError, ValueError):
        return int(default_payment_period_days)


def _compute_due_date(txn_date):
    """DueDate = TransDate + CFG_DefaultPaymentPeriod days (recipe line 7916)."""
    from datetime import date, timedelta
    base = _normalize_txn_date(txn_date)
    if not base:
        return ''
    try:
        y, m, d = (int(p) for p in base.split('-')[:3])
        return (date(y, m, d) + timedelta(
            days=_default_payment_period_days()
        )).isoformat()
    except (ValueError, TypeError):
        return base


def _bill_amount_string(value):
    """Format a line amount as a Decimal-derived string (no float drift)."""
    if value is None or value == '':
        return '0'
    try:
        return str(Decimal(str(value)))
    except (InvalidOperation, ValueError):
        return '0'


def build_us_bill_body_method():
    """Assemble the US QBO Bill body (one Bill per voucher, many lines).

    Mirrors create_bill_v2 (recipe lines 7913-7927):
      header: TxnDate, VendorRef, DueDate, DocNumber
      line:   Amount=OriginalAmountSourceCurrency, Description=Desc2,
              AccountRef=_QBOAccountID, TaxCodeRef="TAX"/"NON",
              CustomerRef=_QBOCustomerID (omitted when blank)
    No GlobalTaxCalculation / TxnTaxDetail (US has no tax math).
    """
    enriched = rail.result('validate_enriched_lines') or []
    vendor = rail.result('resolve_firm_vendorref') or {}
    period = _conf_value('Period')
    post_seq = _conf_value('PostSeq')
    txn_date = _normalize_txn_date(_conf_value('FirstTransDate'))
    invoice_number = _conf_value('InvoiceNumber')

    qbo_lines = []
    for line in enriched:
        detail = {
            'AccountRef': {'value': line['_QBOAccountID']},
            'TaxCodeRef': {'value': 'TAX' if line.get('_HasTax') else 'NON'},
        }
        if line.get('_QBOCustomerID'):
            detail['CustomerRef'] = {'value': line['_QBOCustomerID']}
        qbo_lines.append({
            'Amount': _bill_amount_string(line.get('_Amount')),
            'DetailType': 'AccountBasedExpenseLineDetail',
            'Description': line.get('_Description') or '',
            'AccountBasedExpenseLineDetail': detail,
        })

    body = {
        'VendorRef': {'value': vendor['QBOVendorID']},
        'TxnDate': txn_date,
        'DueDate': _compute_due_date(_conf_value('FirstTransDate')),
        'Line': qbo_lines,
    }
    if invoice_number:
        body['DocNumber'] = invoice_number
    if vendor.get('FirmName'):
        body['VendorRef']['name'] = vendor['FirmName']

    logger.info(
        f"Built US QBO Bill body for voucher (Period={period}, "
        f"PostSeq={post_seq}): {len(qbo_lines)} lines, "
        f"VendorRef={vendor.get('QBOVendorID')}, TxnDate={txn_date}"
    )
    return body


# ---------------------------------------------------------------------------
# CA-UK bill builder + compound-tax math
# (Workato `014_503_psa_post_ap_voucher_to_quickbooks_ca_uk` -> shared
#  `014_503_psa_quickbooks_add_bill`, recipe lines 1028 / 1105 / 1176-1219).
#
# Differences from US: line Amount is NET (gross - tax), the gross goes in
# TaxInclusiveAmt, TaxCodeRef is the real mapped QBO tax-code id (omitted when
# the line has no VP tax code), GlobalTaxCalculation = TaxInclusive when the
# voucher has any tax lines else NotApplicable, and a TxnTaxDetail.TaxLine[]
# is computed per QBO tax-rate group with compound (tax-on-tax) handling.
# ---------------------------------------------------------------------------
_TWO_PLACES = Decimal('0.01')


def _dec(value):
    """Parse to Decimal; 0 on blank/garbage."""
    if value is None or value == '':
        return Decimal('0')
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return Decimal('0')


def _round2(value):
    """ROUND(x, 2) parity with the add_bill SQL."""
    return _dec(value).quantize(_TWO_PLACES)


def _tax_code_rate_fraction(tax_row):
    """MappedTaxCodes.Rate is a FRACTION (add_bill builds it as col6/100).

    The map stores the human percent (col6, e.g. "8.0"); divide by 100 so the
    SQL's `Rate * 100` recovers the percent for TaxPercent.
    """
    return _dec(tax_row.get('Rate')) / Decimal('100')


def compute_ca_uk_tax_lines(enriched, tax_lines, tax_code_map):
    """Port the add_bill compound-tax SQL (recipe line 1105) to Python.

    Returns a list of QBO TaxLine dicts (one per QBO TaxRateRef group):
        {'Amount','DetailType':'TaxLineDetail',
         'TaxLineDetail': {'TaxRateRef': {'value'}, 'PercentBased': True,
                           'TaxPercent', 'NetAmountTaxable'}}

    Per the SQL:
      TotalAmount[PKey] = sum of line gross amounts for that PKey
      TotalTax[PKey]    = sum of tax-line TaxAmount for that PKey
      per (TaxRateRef, PKey): TaxBasis = TotalAmount - TotalTax,
                              CompoundTaxBasis = TotalAmount - thisTaxAmount
      per (TaxRateRef): Amount = round(sum TaxAmount, 2),
                        TaxPercent = round(Rate*100, 2),
                        NetAmountTaxable = round(sum CompoundTaxBasis,2) if the
                          rate is TaxOnAmountPlusTax else round(sum TaxBasis,2)
    """
    # Per-PKey gross totals (BillLines subquery) and tax totals (TaxLines sub).
    total_amount = {}
    for line in enriched:
        pkey = line.get('PKey')
        total_amount[pkey] = total_amount.get(pkey, Decimal('0')) + _dec(
            line.get('_Amount')
        )
    total_tax = {}
    for tl in tax_lines:
        pkey = tl.get('PKey')
        total_tax[pkey] = total_tax.get(pkey, Decimal('0')) + _dec(
            tl.get('TaxAmount')
        )

    # Inner rows `a`: one per matched (TaxRateRef, PKey). Group accumulators
    # keyed by QBO TaxRateRef.
    groups = {}

    def _accumulate(rate_ref, rate_fraction, tax_amount, tax_basis,
                    compound_basis, is_compound):
        grp = groups.get(rate_ref)
        if grp is None:
            grp = {
                'rate_fraction': rate_fraction,
                'tax_amount': Decimal('0'),
                'tax_basis': Decimal('0'),
                'compound_basis': Decimal('0'),
                'is_compound': False,
            }
            groups[rate_ref] = grp
        grp['tax_amount'] += tax_amount
        grp['tax_basis'] += tax_basis
        grp['compound_basis'] += compound_basis
        # b LEFT JOIN: the group is compound if ANY mapped row for this
        # TaxRateRef is TaxOnAmountPlusTax.
        grp['is_compound'] = grp['is_compound'] or is_compound

    # All mapped tax rows flattened — tax_code_map is {VP code -> [rows]}; a
    # single VP code can map to SEVERAL QBO rate rows (tax groups / compound).
    all_rows = [row for rows in tax_code_map.values() for row in rows]

    # Index by QBO TaxCodeRef (for the zero-rated branch); keep ALL rows per code.
    by_qbo_code = {}
    for row in all_rows:
        code_ref = (row.get('TaxCodeRef') or '').strip()
        if code_ref:
            by_qbo_code.setdefault(code_ref, []).append(row)

    def _is_compound_rate(rate_ref):
        for row in all_rows:
            if (
                (row.get('TaxRateRef') or '').strip() == rate_ref
                and (row.get('TaxTypeApplicable') or '') == 'TaxOnAmountPlusTax'
            ):
                return True
        return False

    # Taxed lines: join each tax line to ALL its MappedTaxCode rows by VP code
    # (Workato INNER JOIN tl.TaxCode = mtc.VantagepointCode, GROUP BY
    # mtc.TaxRateRef, tl.PKey) — one accumulation per DISTINCT QBO rate the VP
    # code maps to, so a tax-group VP code fans out into its components. For a
    # plain 1:1 VP code this is identical to the single-row behaviour.
    taxed_pkeys = set()
    for tl in tax_lines:
        pkey = tl.get('PKey')
        taxed_pkeys.add(pkey)
        vp_code = (tl.get('TaxCode') or '').strip()
        rows = tax_code_map.get(vp_code) or []
        if not rows:
            # Unmapped tax code — validate_enriched_lines already raises for
            # this; skip defensively so we don't KeyError mid-build.
            continue
        gross = total_amount.get(pkey, Decimal('0'))
        this_tax = _dec(tl.get('TaxAmount'))
        tax_basis = gross - total_tax.get(pkey, Decimal('0'))
        compound_basis = gross - this_tax
        seen_rate_refs = set()
        for mtc in rows:
            rate_ref = (mtc.get('TaxRateRef') or '').strip()
            if rate_ref in seen_rate_refs:
                continue
            seen_rate_refs.add(rate_ref)
            _accumulate(
                rate_ref, _tax_code_rate_fraction(mtc), this_tax,
                tax_basis, compound_basis, _is_compound_rate(rate_ref),
            )

    # Zero-rated lines (no tax-line for the PKey): match the line's mapped QBO
    # TaxCodeRef to Purchase MappedTaxCode rows (SQL's OR branch), fanning out
    # per distinct rate.
    for line in enriched:
        pkey = line.get('PKey')
        if pkey in taxed_pkeys:
            continue
        line_rows = tax_code_map.get(line.get('_TaxCode')) or []
        line_code_ref = (
            (line_rows[0].get('TaxCodeRef') if line_rows else '') or ''
        ).strip()
        if not line_code_ref:
            continue
        gross = total_amount.get(pkey, Decimal('0'))
        seen_rate_refs = set()
        for mtc in by_qbo_code.get(line_code_ref, []):
            if (mtc.get('TaxOn') or '') != 'Purchase':
                continue
            rate_ref = (mtc.get('TaxRateRef') or '').strip()
            if rate_ref in seen_rate_refs:
                continue
            seen_rate_refs.add(rate_ref)
            _accumulate(
                rate_ref, _tax_code_rate_fraction(mtc), Decimal('0'),
                gross, gross, _is_compound_rate(rate_ref),
            )

    qbo_tax_lines = []
    for rate_ref, grp in groups.items():
        basis = (
            grp['compound_basis'] if grp['is_compound'] else grp['tax_basis']
        )
        tax_percent = (grp['rate_fraction'] * Decimal('100')).quantize(
            _TWO_PLACES
        )
        qbo_tax_lines.append({
            'Amount': str(grp['tax_amount'].quantize(_TWO_PLACES)),
            'DetailType': 'TaxLineDetail',
            'TaxLineDetail': {
                'TaxRateRef': {'value': rate_ref},
                'PercentBased': True,
                'TaxPercent': str(tax_percent),
                'NetAmountTaxable': str(basis.quantize(_TWO_PLACES)),
            },
        })
    return qbo_tax_lines


def _resolve_no_tax_code_id():
    """CA-UK NoTaxCodeID: the QBO TaxCode Id whose Name == CFG_NoTaxCode.

    Mirrors the Workato CA-UK dispatcher, which queries QBO tax codes by
    `Name = account property CFG_NoTaxCode` and uses the returned Id as the
    fallback tax code for untaxed lines (recipe line 4082:
    `line.TaxCode.presence || NoTaxCodeID`). Reads the `get_no_tax_code`
    QuickBooksTaxCodeOperator result (already filtered by name) and the
    CFG_NoTaxCode name from conf. Returns '' when CFG_NoTaxCode is blank or no
    match is found (untaxed lines then carry no TaxCodeRef — same as having no
    no-tax code configured).
    """
    name = str(_conf_value('CFG_NoTaxCode') or '').strip()
    if not name:
        return ''
    result = rail.result('get_no_tax_code')
    if not isinstance(result, dict):
        return ''
    data = result.get('data')
    if isinstance(data, dict):
        data = [data]
    if not isinstance(data, list):
        return ''
    # The query filters by Name, so prefer an exact (case-insensitive) match;
    # fall back to the sole row if QBO returned exactly one.
    for tax_code in data:
        if (
            isinstance(tax_code, dict)
            and (tax_code.get('Name') or '').strip().lower() == name.lower()
        ):
            return str(tax_code.get('Id') or '')
    if len(data) == 1 and isinstance(data[0], dict):
        return str(data[0].get('Id') or '')
    return ''


def build_ca_uk_bill_body_method():
    """Assemble the CA-UK QBO Bill body (recipe lines 1176-1219).

    header: GlobalTaxCalculation (TaxInclusive if any tax lines else
            NotApplicable), TxnDate, VendorRef, DueDate, DocNumber
    line:   Amount=NET (gross - tax for the PKey), TaxInclusiveAmt=gross,
            AccountRef, TaxCodeRef = mapped QBO tax code, else the NoTaxCodeID
            ("No VAT" code) — Workato `line.TaxCode.presence || NoTaxCodeID`
            (recipe line 4082); only omitted if neither resolves, CustomerRef,
            Description
    TxnTaxDetail.TaxLine[]: computed per QBO tax-rate group (compound aware)
    """
    enriched = rail.result('validate_enriched_lines') or []
    tax_lines = rail.result('extract_ledgertax_lines') or []
    tax_code_map = (
        rail.result('load_lookup_tables') or {}
    ).get('tax_code_map') or {}
    vendor = rail.result('resolve_firm_vendorref') or {}
    no_tax_code_id = _resolve_no_tax_code_id()
    period = _conf_value('Period')
    post_seq = _conf_value('PostSeq')
    txn_date = _normalize_txn_date(_conf_value('FirstTransDate'))
    invoice_number = _conf_value('InvoiceNumber')

    # Per-PKey total tax to compute each line's NET amount (gross - tax).
    total_tax = {}
    for tl in tax_lines:
        pkey = tl.get('PKey')
        total_tax[pkey] = total_tax.get(pkey, Decimal('0')) + _dec(
            tl.get('TaxAmount')
        )

    qbo_lines = []
    for line in enriched:
        pkey = line.get('PKey')
        gross = _dec(line.get('_Amount'))
        net = gross - total_tax.get(pkey, Decimal('0'))
        detail = {
            'AccountRef': {'value': line['_QBOAccountID']},
            'TaxInclusiveAmt': str(gross.quantize(_TWO_PLACES)),
        }
        # Workato (recipe line 4082): line tax code = mapped QBO code if the VP
        # line has one, else the NoTaxCodeID ("No VAT") code. Only omit if
        # neither resolves (no CFG_NoTaxCode configured).
        # tax_code_map values are LISTs of mapped rows (tax-group fan-out); the
        # per-line TaxCodeRef takes the first row's QBO code (Workato uses the
        # group/first code on the line, with components in TxnTaxDetail).
        _line_tax_rows = tax_code_map.get(line.get('_TaxCode')) or []
        mapped_code_ref = (
            (_line_tax_rows[0].get('TaxCodeRef') if _line_tax_rows else '')
            or ''
        ).strip()
        code_ref = mapped_code_ref or no_tax_code_id
        if code_ref:
            detail['TaxCodeRef'] = {'value': code_ref}
        if line.get('_QBOCustomerID'):
            detail['CustomerRef'] = {'value': line['_QBOCustomerID']}
        qbo_lines.append({
            'Amount': str(net.quantize(_TWO_PLACES)),
            'DetailType': 'AccountBasedExpenseLineDetail',
            'Description': line.get('_Description') or '',
            'AccountBasedExpenseLineDetail': detail,
        })

    # Workato CA-UK `HasTax` flag (post recipe): GlobalTaxCalculation is
    # 'TaxInclusive' ONLY when a voucher LINE carries a TaxCode — it is NOT
    # driven by the LedgerTax fetch / the computed tax lines. A line-less or
    # zero-amount tax situation (e.g. LedgerTax has a zero row but the PSALedger
    # line has no TaxCode) must post as 'NotApplicable', exactly as Workato does
    # for the same voucher. This matters because a QBO company that disallows
    # inclusive tax rejects 'TaxInclusive' with a 6000 ValidationFault
    # ("Inclusive Tax Type is not allowed"); keying TaxInclusive off the line
    # TaxCode (not off the computed tax lines) keeps parity with the Workato
    # post that succeeds. When HasTax is false the (possibly spurious) computed
    # tax lines are dropped — TxnTaxDetail is only attached when HasTax is true.
    computed_tax_lines = compute_ca_uk_tax_lines(
        enriched, tax_lines, tax_code_map
    )
    has_tax = any((line.get('_TaxCode') or '').strip() for line in enriched)
    body = {
        'GlobalTaxCalculation': 'TaxInclusive' if has_tax else 'NotApplicable',
        'VendorRef': {'value': vendor['QBOVendorID']},
        'TxnDate': txn_date,
        'DueDate': _compute_due_date(_conf_value('FirstTransDate')),
        'Line': qbo_lines,
    }
    if invoice_number:
        body['DocNumber'] = invoice_number
    if vendor.get('FirmName'):
        body['VendorRef']['name'] = vendor['FirmName']
    if has_tax:
        body['TxnTaxDetail'] = {'TaxLine': computed_tax_lines}

    logger.info(
        f"Built CA-UK QBO Bill body for voucher (Period={period}, "
        f"PostSeq={post_seq}): {len(qbo_lines)} lines, "
        f"GlobalTaxCalculation={body['GlobalTaxCalculation']}, "
        f"taxLines={len(body.get('TxnTaxDetail', {}).get('TaxLine', []))}, "
        f"NoTaxCodeID={no_tax_code_id or 'none'}, "
        f"VendorRef={vendor.get('QBOVendorID')}, TxnDate={txn_date}"
    )
    return body


# ---------------------------------------------------------------------------
# Outstanding Purchase Invoices table (Workato parity) — dedup guard + write
#
# The Workato US/CA-UK processors both:
#   (a) READ the "014-503 PSA Outstanding Purchase Invoices" table by
#       Batch+Voucher FIRST and wrap the whole export in `IF voucher not yet
#       exported` (US comment line 8867 / CA-UK line 5204) — so an already-
#       exported voucher is SKIPPED (no duplicate bill); and
#   (b) after a successful bill create, APPEND one row per voucher line
#       (US step 30-31): col1 Batch, col2 Voucher, col3-5 WBS1/2/3,
#       col6 Line amount, col7 Outstanding amount (= full line amount),
#       col8 Account, col9 Org, col10 QBO Bill Id.
#
# We mirror BOTH against the shared mapping_sync `outstanding_purchase_invoices`
# S3 collection — the SAME table the bill_payment_sync integration consumes (it
# reads by InvoiceID = the QBO Bill Id). `is_voucher_already_exported_method` is
# the dedup read (a) used by the processor's first IfOperator; the write (b) is
# `record_outstanding_invoices_method` and is FAIL-LOUD — it raises on error to
# match the Workato recipe (where the write is inside the try block). The write
# rows are also what make the dedup read skip this voucher on any re-poll.
#
# The write is idempotent on retry: it DELETEs this voucher's rows by
# (Batch, Voucher) then INSERTs, so a re-run replaces rather than appends.
# ---------------------------------------------------------------------------
def is_voucher_already_exported_method():
    """IfOperator test: has this (Batch, Voucher) already been exported?

    Mirrors the Workato US/CA-UK guard (read Outstanding Purchase Invoices by
    Batch+Voucher; the whole export is wrapped in `IF voucher not yet
    exported`). Reads the shared mapping_sync `outstanding_purchase_invoices`
    S3 collection. Returns True when a row already exists for this
    Batch+Voucher → the processor SKIPS the create (no duplicate bill). Returns
    False → proceed with the export.

    Guards against a blank Batch/Voucher (returns False — never skip on an
    empty identity, which could otherwise match a malformed row).
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
            "Voucher (Batch=%s, Voucher=%s) already exported (found in "
            "outstanding_purchase_invoices) — skipping to avoid a duplicate "
            "QuickBooks Bill.", batch, voucher
        )
        return True
    return False


def _extract_qbo_bill_id(create_result):
    """Pull the created Bill's Id from the QuickBooksBillOperator result.

    The operator returns `_format_quickbooks_response(..., 'Bill')` →
    {success, entity_type, data, count, metadata}. `data` may be the Bill
    dict, a list, or a {'Bill': {...}} envelope — probe defensively.
    """
    if not isinstance(create_result, dict):
        return ''
    data = create_result.get('data', create_result)
    candidates = []
    if isinstance(data, list):
        candidates = data
    elif isinstance(data, dict):
        candidates = [data.get('Bill'), data]
    for cand in candidates:
        if isinstance(cand, dict) and cand.get('Id'):
            return str(cand['Id'])
    return ''


def record_outstanding_invoices_method(create_bill_task_id):
    """Fail-loud: write one outstanding-invoice row per line (Workato parity).

    After a successful Bill create, writes one row per voucher line to the
    shared mapping_sync `outstanding_purchase_invoices` S3 collection — the
    table the bill_payment_sync integration later consumes. The QBO Bill Id
    goes in the `InvoiceID` column (Workato col10 "Invoice ID"; bill_payment_sync
    reads by `InvoiceID`). RAISES on any error — matching the Workato recipe,
    where this write lives inside the try block; the rows are what make
    `is_voucher_already_exported_method` skip the voucher on a re-poll, so the
    write succeeding is part of the dedup contract.

    Idempotent on retry: in ONE atomic S3 cycle, DELETE this voucher's rows by
    (Batch, Voucher) then INSERT the current line set, so a re-run replaces
    rather than appends. Because the DELETE + INSERTs run as a single atomic
    batch, a mid-write failure rolls the whole thing back (leaving the prior
    rows intact) instead of a deleted-but-not-reinserted window.
    """
    context = rail.get_current_context()
    period = _conf_value('Period')
    post_seq = _conf_value('PostSeq')
    enriched = rail.result('validate_enriched_lines') or []
    create_result = rail.result(create_bill_task_id)
    bill_id = _extract_qbo_bill_id(create_result)
    batch = str(_conf_value('Batch') or '')
    voucher = str(_conf_value('Voucher') or '')

    row_placeholder = '(' + ', '.join(
        ['?'] * len(OUTSTANDING_PURCHASE_INVOICES_COLUMNS)
    ) + ')'
    columns = ', '.join(OUTSTANDING_PURCHASE_INVOICES_COLUMNS)
    rows_params = []
    for line in enriched:
        amount = _bill_amount_string(line.get('_Amount'))
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
            'InvoiceID': bill_id,
        }
        rows_params.append(
            [values[c] for c in OUTSTANDING_PURCHASE_INVOICES_COLUMNS]
        )

    # Build ONE atomic batch: clear this voucher's prior rows, then INSERT the
    # current line set — all in a single S3 download/modify/upload cycle. The
    # INSERTs are chunked to stay well under SQLite's variable limit (~999):
    # 10 columns x 50 rows = 500 params per statement.
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

    collection_operations(
        OUTSTANDING_PURCHASE_INVOICES_TABLE_NAME, operations, context,
    )
    written = len(rows_params)

    logger.info(
        "Recorded %d outstanding_purchase_invoices rows for voucher "
        "(Period=%s, PostSeq=%s, Voucher=%s); QBO Bill Id=%s",
        written, period, post_seq, voucher, bill_id or 'unknown'
    )
    return None


# ---------------------------------------------------------------------------
# Error capture (return dict; do NOT raise — keeps the processor DAG SUCCESS
# so the dispatcher's WaitForDagRunsSensor never sees a failed run and
# GatherResultsFromDagRunsOperator can collect the error dict.)
# ---------------------------------------------------------------------------
def capture_processor_error(period, post_seq, error_message):
    """Return an error dict the dispatcher can aggregate."""
    label = f"AP Voucher (Period={period}, PostSeq={post_seq})"
    return {
        'error': f"{label} - sync failed: {error_message}"
    }


# Watermark helpers (sanitize_customer_id, build_watermark_variable_key,
# utc_now_iso, prepare_sync_timestamps, update_last_sync_time) and
# has_sync_errors_method now live in common.python_callable_method; the
# dispatcher imports them from there.
