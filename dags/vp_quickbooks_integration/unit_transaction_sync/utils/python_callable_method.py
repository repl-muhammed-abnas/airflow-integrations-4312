"""
Common utility methods for VP -> QBO Unit Transaction Sync.

Translates the Workato recipes (poll, orchestrator, JE post) into
Python callables for the 3-DAG Airflow template (main -> dispatcher ->
unit_transaction_create). Single-leaf topology: each (Period, PostSeq)
produces exactly one balanced QBO JournalEntry.

Key recipe finding (L4406, L10382): PSA Ledger `un` rows carry
WBS1/WBS2/WBS3 but NO ClientID. The recipe calls GET /project to resolve
WBS1+WBS2+WBS3 -> ClientID, then joins to the firm map on ClientID = FirmID.
We replicate this with a VantagepointProjectOperator (`fetch_projects`) task
and use the result in `resolve_rows_method`.

Error reporting goes to middleware via PostDagRunDetailsToMiddlewareApiOperator
+ FailOperator on the dispatcher's failure branch; no email/log-table path
(matches vendor_sync, customer_sync, timesheets_sync).
"""
# pylint: disable=invalid-name,too-many-locals
import logging
import re
from datetime import datetime, timezone
from urllib.parse import quote
from airflow.models import Variable
import rail
from vp_quickbooks_integration.unit_transaction_sync.config import (
    initial_sync_time,
)
from vp_quickbooks_integration.common.python_callable_method import (
    collection_rows,
    watermark_key_template,
)
from vp_quickbooks_integration.common.tables import (
    MAP_FIRM_COLUMNS,
    MAP_ACCOUNT_CODE_COLUMNS,
    MAP_FIRM_TABLE_NAME as map_firm_table_name,
    MAP_ACCOUNT_CODE_TABLE_NAME as map_account_code_table_name,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Watermark helpers
# ---------------------------------------------------------------------------
WATERMARK_VARIABLE_KEY_TEMPLATE = watermark_key_template('unit_transaction_sync')

_CUSTOMER_ID_SAFE_RE = re.compile(r'[^A-Za-z0-9_-]')


def _sanitize_customer_id(customer_id):
    if not customer_id:
        return 'default'
    cleaned = _CUSTOMER_ID_SAFE_RE.sub('_', str(customer_id))
    return cleaned or 'default'


def _watermark_variable_key(instance, customer_id):
    return WATERMARK_VARIABLE_KEY_TEMPLATE.format(
        instance=instance,
        customer_id=_sanitize_customer_id(customer_id),
    )


def _utc_now_iso():
    return (
        datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]
        + 'Z'
    )


def prepare_sync_timestamps_method(instance):
    """Capture last sync time + current time for the OData filter."""
    customer_id = (
        rail.get_current_context()['dag_run'].conf.get('customerId')
    )
    key = _watermark_variable_key(instance, customer_id)
    current_time = _utc_now_iso()
    try:
        last_sync_time = Variable.get(key)
        print(f"Retrieved last sync time from Variable '{key}': "
              f"{last_sync_time}")
    except KeyError:
        last_sync_time = initial_sync_time
        print(f"Variable '{key}' not found, using initial sync time: "
              f"{last_sync_time}")
    return {
        'last_sync_time': last_sync_time,
        'current_sync_time': current_time,
    }


def update_last_sync_time_method(instance):
    """
    Persist `current_sync_time` after run completes. trigger_rule=
    'all_done' on the caller means this fires on every terminal state.
    Guards: skip if integration disabled or timestamps dict missing.
    """
    try:
        is_enabled = rail.result('check_disabled_flag')
    except KeyError:
        is_enabled = True
    if not is_enabled:
        print("Integration disabled; skipping watermark advance")
        return None

    try:
        timestamps = rail.result('prepare_sync_timestamps')
    except KeyError:
        timestamps = None
    if not isinstance(timestamps, dict) or not timestamps.get(
        'current_sync_time'
    ):
        print(
            "prepare_sync_timestamps did not produce a current_sync_time "
            "(skipped or failed); leaving watermark Variable unchanged."
        )
        return None

    customer_id = (
        rail.get_current_context()['dag_run'].conf.get('customerId')
    )
    key = _watermark_variable_key(instance, customer_id)
    current_time = timestamps['current_sync_time']
    Variable.set(key, current_time)
    print(f"Updated last sync time Variable '{key}' to: {current_time}")
    return current_time


# ---------------------------------------------------------------------------
# Disabled-flag check
# ---------------------------------------------------------------------------
def is_integration_enabled_method(instance):
    """True when CFG_DisableUnitTransactionIntegration_{instance} != 'true'."""
    flag = Variable.get(
        f'CFG_DisableUnitTransactionIntegration_{instance}',
        default_var='false'
    )
    enabled = str(flag).strip().lower() != 'true'
    if not enabled:
        print(
            f"Unit transaction integration disabled for instance "
            f"'{instance}' via "
            f"CFG_DisableUnitTransactionIntegration_{instance}"
        )
    return enabled


# ---------------------------------------------------------------------------
# PSA Ledger filter
# ---------------------------------------------------------------------------
def build_psa_ledger_filter_method():
    """OData ModifiedDate filter for the dispatcher polling query."""
    timestamps = rail.result('prepare_sync_timestamps')
    return (
        f"?$filter=ModifiedDate ge datetime'{timestamps['last_sync_time']}'"
        f" and ModifiedDate le datetime'{timestamps['current_sync_time']}'"
    )


def cap_records_for_audit(environment='pre-production'):
    """
    Limit polled rows during non-production rollout so QBO sandbox
    writes stay small. Bypassed entirely in production.

    Remove this cap before production rollout — it is intentionally
    absent from production to avoid silently dropping records.
    """
    records = rail.result('poll_psa_ledger') or []
    if not isinstance(records, list):
        return []
    if environment == 'production':
        return records
    # TEMP dev/staging cap: 3 records max. Remove when shipping to prod.
    capped = records[:3]
    print(
        f"PSA unit-transaction poll: {len(records)} -> {len(capped)} "
        f"(audit cap active — environment={environment!r})"
    )
    return capped


# ---------------------------------------------------------------------------
# S3 mapping collection helpers (full-table load for resolve_rows pattern)
# ---------------------------------------------------------------------------

def _load_full_table(table_name):
    columns = MAP_FIRM_COLUMNS if table_name == map_firm_table_name else MAP_ACCOUNT_CODE_COLUMNS
    return collection_rows(table_name, columns, '1=1', [])




# ---------------------------------------------------------------------------
# Lookup tables — S3 mapping collection (full-table load, then in-memory scan)
# S3 tables (in the per-customer mapping_sync collection):
#   map_account_code: QBOCode, QBOName, QBOType, VantagepointCode,
#                     VantagepointName, VantagepointTypeRO, QBOID
#   map_firm:         FirmID, QBOID, IsVendor, Name
# ---------------------------------------------------------------------------
def get_account_code_mapping_method(instance):
    return _load_full_table(map_account_code_table_name)


def get_firm_mapping_method(instance):
    return _load_full_table(map_firm_table_name)


# ---------------------------------------------------------------------------
# Row fetch helpers
# ---------------------------------------------------------------------------
def _fetched_rows():
    """All PSA Ledger rows for this (Period, PostSeq)."""
    result = rail.result('fetch_unit_transfer_rows') or []
    if isinstance(result, list):
        return result
    if isinstance(result, dict):
        return result.get('entries') or result.get('data') or []
    return []


def _fetched_projects():
    """Project rows returned by VantagepointProjectOperator."""
    result = rail.result('fetch_projects') or []
    if isinstance(result, list):
        return result
    if isinstance(result, dict):
        return result.get('body') or result.get('data') or []
    return []


# ---------------------------------------------------------------------------
# PSA Ledger row filter (composite-key re-fetch in create DAG)
# ---------------------------------------------------------------------------
def build_record_filter():
    """
    OData filter to re-fetch all rows for (Period, PostSeq) from conf.
    Raises ValueError on missing/non-numeric PostSeq so the error surfaces
    immediately rather than silently querying for PostSeq=0.
    """
    conf = rail.get_current_context()['dag_run'].conf
    raw_post_seq = conf.get('PostSeq')
    try:
        post_seq = int(raw_post_seq)
    except (TypeError, ValueError) as exc:
        raise ValueError(
            f"PostSeq in dag_run.conf is missing or non-numeric: "
            f"{raw_post_seq!r}"
        ) from exc
    period = str(conf.get('Period') or '').replace("'", "''")
    return (
        f"?$filter=PostSeq eq {post_seq}"
        f" and Period eq '{period}'"
    )


# ---------------------------------------------------------------------------
# VP Project filter (recipe step 17, line 4406)
# Builds GET /project?fieldFilter=WBSNumber,WBS1,WBS2,WBS3,Name,ClientID
# &filterHash[n][name]=WBS1&filterHash[n][value]={wbs1}
# for each unique WBS1 seen in the PSA Ledger rows.
# ---------------------------------------------------------------------------
def build_project_filter():
    """
    Replicates the recipe's foreach loop that builds a filter string
    accumulating one filterHash entry per unique WBS1 value, then calls
    GET /project with fieldFilter + those filterHash params.

    The VP project API uses filterHash[n][name]/[value] pairs for OR
    filtering on the same field — one entry per distinct WBS1 value.
    """
    rows = _fetched_rows()
    unique_wbs1 = sorted({
        str(r.get('WBS1') or '').strip()
        for r in rows
        if r.get('WBS1')
    })
    if not unique_wbs1:
        print("build_project_filter: no WBS1 values in PSA rows")
        return '?fieldFilter=WBSNumber,WBS1,WBS2,WBS3,Name,ClientID'

    filter_parts = ['?fieldFilter=WBSNumber,WBS1,WBS2,WBS3,Name,ClientID']
    for idx, wbs1 in enumerate(unique_wbs1):
        encoded = quote(wbs1, safe='')
        filter_parts.append(
            f'&filterHash%5B{idx}%5D%5Bname%5D=WBS1'
            f'&filterHash%5B{idx}%5D%5Bvalue%5D={encoded}'
        )
    filt = ''.join(filter_parts)
    print(f"build_project_filter: fetching projects for WBS1={unique_wbs1}")
    return filt


# ---------------------------------------------------------------------------
# Row resolution (recipe SQL JOIN at L10382 / L10570)
# ---------------------------------------------------------------------------
def _lookup_account_code(mapping, vp_account_code):
    vp_code = str(vp_account_code or '').strip()
    if not vp_code:
        return None
    for row in mapping:
        if str(row.get('VantagepointCode') or '').strip() == vp_code:
            return row
    return None


def _lookup_project(projects, wbs1, wbs2, wbs3):
    """
    Find project record matching WBS1+WBS2+WBS3. Mirrors the recipe JOIN:
      ON debit.WBS1 = proj.WBS1 AND debit.WBS2 = proj.WBS2
         AND debit.WBS3 = proj.WBS3
    Falls back to WBS1-only match when WBS2/WBS3 are blank on both sides.
    """
    w1 = str(wbs1 or '').strip()
    w2 = str(wbs2 or '').strip()
    w3 = str(wbs3 or '').strip()
    for proj in projects:
        p1 = str(proj.get('WBS1') or '').strip()
        p2 = str(proj.get('WBS2') or '').strip()
        p3 = str(proj.get('WBS3') or '').strip()
        if p1 == w1 and p2 == w2 and p3 == w3:
            return proj
    if not w2 and not w3:
        for proj in projects:
            if str(proj.get('WBS1') or '').strip() == w1:
                return proj
    return None


def _lookup_firm(mapping, client_id):
    """
    Find firm-map row by FirmID matching the project's ClientID.
    Recipe JOIN: proj.ClientID = firm.FirmID AND IFNULL(proj.ClientID,'') != ''
    """
    cid = str(client_id or '').strip()
    if not cid:
        return None
    for row in mapping:
        if str(row.get('FirmID') or '').strip() == cid:
            return row
    return None


def resolve_rows_method():
    """
    Annotate every PSA Ledger row with resolved QBO ids by replicating
    the recipe's 4-table SQL JOIN (L10382 / L10570).
    """
    rows = _fetched_rows()
    projects = _fetched_projects()
    account_map = rail.result('fetch_account_code_mapping') or []
    firm_map = rail.result('fetch_firm_mapping') or []

    annotated = []
    for row in rows:
        if not isinstance(row, dict):
            continue

        acct_row = _lookup_account_code(account_map, row.get('Account'))

        proj_row = _lookup_project(
            projects,
            row.get('WBS1'),
            row.get('WBS2'),
            row.get('WBS3'),
        )
        client_id = (proj_row or {}).get('ClientID') if proj_row else None
        firm_row = _lookup_firm(firm_map, client_id) if client_id else None

        firm_is_vendor = (
            str((firm_row or {}).get('IsVendor') or 'N').upper() == 'Y'
            if firm_row else False
        )
        annotated.append({
            **row,
            'QBOAccountName': (
                (acct_row or {}).get('QBOName') or
                (acct_row or {}).get('QBOCode') or None
            ),
            'QBOAccountID': (acct_row or {}).get('QBOID') or None,
            'ClientID': client_id,
            'FirmQBOID': (firm_row or {}).get('QBOID') or None,
            'FirmIsVendor': firm_is_vendor,
        })

    unresolved_acct = sum(1 for r in annotated if not r.get('QBOAccountName'))
    unresolved_firm = sum(1 for r in annotated if not r.get('FirmQBOID'))
    print(
        f"resolve_rows: {len(rows)} VP rows, {len(projects)} projects "
        f"fetched; unresolved-account={unresolved_acct}, "
        f"unresolved-firm={unresolved_firm}"
    )
    return annotated


# ---------------------------------------------------------------------------
# Validation gates
# ---------------------------------------------------------------------------
def check_rows_fetched_method():
    """True iff resolve_rows returned at least one row."""
    return bool(rail.result('resolve_rows'))


def is_account_mapping_resolved_method():
    """True iff every row has QBOAccountName. Caller guards for empty list."""
    rows = rail.result('resolve_rows') or []
    return all(r.get('QBOAccountName') for r in rows)


def is_firm_mapping_resolved_method():
    """
    True iff every row that HAS a ClientID also has a FirmQBOID.

    Rows where the project has no ClientID (internal/overhead projects)
    are excluded — mirrors the recipe's SQL JOIN condition
    `AND IFNULL(proj.ClientID,'') != ''`.
    """
    rows = rail.result('resolve_rows') or []
    rows_with_client = [r for r in rows if r.get('ClientID')]
    if not rows_with_client:
        return True
    return all(r.get('FirmQBOID') for r in rows_with_client)


# ---------------------------------------------------------------------------
# Logging (replaces 014-503 PSA Log Message recipe)
# ---------------------------------------------------------------------------
def _conf_keys():
    conf = rail.get_current_context()['dag_run'].conf
    return conf.get('PostSeq'), conf.get('Period')


def log_no_rows_to_post():
    """No PSA Ledger rows came back for this (Period, PostSeq)."""
    post_seq, period = _conf_keys()
    return {
        'error': (
            f"No PSA Ledger rows returned for Period={period}, "
            f"PostSeq={post_seq} (TransType=un). Nothing to post."
        )
    }


def log_missing_account_mapping():
    """Recipe error site L13086 — missing account-code mapping."""
    post_seq, period = _conf_keys()
    rows = rail.result('resolve_rows') or []
    missing_codes = sorted({
        str(r.get('Account') or '')
        for r in rows if not r.get('QBOAccountName')
    } - {''})
    return {
        'error': (
            f"Failed to post unit transfer (period: {period}, post "
            f"sequence: {post_seq}) to QuickBooks. Details of error: "
            f"Vantagepoint account(s) "
            f"{', '.join(missing_codes) or '(none)'} not matched to a "
            f"QuickBooks account in 014-503 PSA Map Account Code lookup "
            f"table."
        )
    }


def log_missing_firm_mapping():
    """Recipe error site L13086 — missing firm mapping for rows with a ClientID."""
    post_seq, period = _conf_keys()
    rows = rail.result('resolve_rows') or []
    missing_clients = sorted({
        str(r.get('ClientID') or '')
        for r in rows
        if r.get('ClientID') and not r.get('FirmQBOID')
    } - {''})
    return {
        'error': (
            f"Failed to post unit transfer (period: {period}, post "
            f"sequence: {post_seq}) to QuickBooks. Details of error: "
            f"Vantagepoint client(s) "
            f"{', '.join(missing_clients) or '(none)'} not matched to a "
            f"QuickBooks customer in 014-503 PSA Map Firm lookup table."
        )
    }


# ---------------------------------------------------------------------------
# QBO JournalEntry body builder (recipe L13174 create_journal_entry_v2)
# ---------------------------------------------------------------------------
def _format_qbo_date(value):
    if not value:
        return None
    if isinstance(value, str):
        return value.split('T')[0] if 'T' in value else value[:10]
    return value


def _posting_type_for_row(row):
    """
    Recipe debit/credit split: Amount > 0 -> Debit, Amount < 0 -> Credit
    (recipe SQL L10382: WHERE CAST(debit.Amount AS DECIMAL) > 0;
     recipe SQL L10570: WHERE CAST(credit.Amount AS DECIMAL) < 0).
    """
    try:
        amount = float(row.get('Amount') or 0)
    except (TypeError, ValueError):
        amount = 0.0
    return 'Debit' if amount >= 0 else 'Credit'


def _line_from_row(row):
    """
    Build one QBO JournalEntry Line from one resolved PSA Ledger row.

    AccountRef uses value (QBOID) — RAIL sends the body directly to QBO;
    Workato's AccountRef.Name DSL abstraction does not apply here.
    Entity block is only included when FirmQBOID is non-empty; sending
    EntityRef.value="" causes a QBO ValidationFault on AP/AR accounts.
    Entity.Type is required by QBO when Entity is present.
    """
    try:
        amount = abs(float(row.get('Amount') or 0))
    except (TypeError, ValueError):
        amount = 0.0

    line_detail = {
        'PostingType': _posting_type_for_row(row),
        'AccountRef': {
            'value': str(row.get('QBOAccountID') or ''),
            'name': row.get('QBOAccountName') or '',
        },
    }

    firm_qbo_id = str(row.get('FirmQBOID') or '').strip()
    if firm_qbo_id:
        entity_type = 'Vendor' if row.get('FirmIsVendor') else 'Customer'
        line_detail['Entity'] = {
            'Type': entity_type,
            'EntityRef': {'value': firm_qbo_id},
        }

    return {
        'Amount': amount,
        'Description': row.get('Desc2') or row.get('Desc1') or '',
        'DetailType': 'JournalEntryLineDetail',
        'JournalEntryLineDetail': line_detail,
    }


def build_journal_entry_body():
    """
    Construct the QBO JournalEntry POST body from resolved rows.

    Mirrors recipe L13174-L13261:
      - TxnDate  : earliest TransDate across rows
      - DocNumber: "{Period}-{PostSeq}" soft-dedupe key (recipe L13215)
      - Line[]   : one entry per row, PostingType by Amount sign
      - Validates sum(Debit) == sum(Credit) and both sides are present.
    """
    post_seq, period = _conf_keys()
    rows = rail.result('resolve_rows') or []
    if not rows:
        raise ValueError(
            f"No resolved rows for Period={period}, PostSeq={post_seq}."
        )

    lines = [_line_from_row(r) for r in rows]

    debit_lines = [
        l for l in lines
        if l['JournalEntryLineDetail']['PostingType'] == 'Debit'
    ]
    credit_lines = [
        l for l in lines
        if l['JournalEntryLineDetail']['PostingType'] == 'Credit'
    ]

    if not debit_lines or not credit_lines:
        raise ValueError(
            f"JournalEntry Period={period}, PostSeq={post_seq} has no "
            f"{'debit' if not debit_lines else 'credit'} lines "
            f"(debit_count={len(debit_lines)}, "
            f"credit_count={len(credit_lines)}). "
            f"Check Amount sign on PSA Ledger rows."
        )

    debit_total = sum(l['Amount'] for l in debit_lines)
    credit_total = sum(l['Amount'] for l in credit_lines)
    if round(debit_total - credit_total, 2) != 0:
        raise ValueError(
            f"Unbalanced JournalEntry Period={period}, PostSeq={post_seq}: "
            f"debit={debit_total} credit={credit_total}. Refusing to POST."
        )

    txn_date = None
    for row in rows:
        candidate = _format_qbo_date(row.get('TransDate'))
        if candidate and (txn_date is None or candidate < txn_date):
            txn_date = candidate

    return {
        'TxnDate': txn_date,
        'DocNumber': f"{period}-{post_seq}",
        'PrivateNote': (
            f"vp_psa:Period={period};PostSeq={post_seq};TransType=un"
        ),
        'Line': lines,
    }


# ---------------------------------------------------------------------------
# Error capture terminal
# ---------------------------------------------------------------------------
def capture_create_dag_error(post_seq, period, fallback_error_message):
    """
    trigger_rule='all_done' terminal. Aggregates validation error dicts
    from log_missing_*_action tasks; falls back to get_error_message()
    for QBO API failures. Returns dict or None (never raises).
    """
    child_errors = []
    for log_task in (
        'log_no_rows_action',
        'log_missing_account_mapping_action',
        'log_missing_firm_mapping_action',
    ):
        try:
            err = rail.result(log_task)
            if err:
                child_errors.append(err)
        except KeyError:
            pass

    if child_errors:
        error_message = ' | '.join(
            e.get('error', str(e)) for e in child_errors if e
        )
    elif fallback_error_message:
        error_message = (
            f"PostSeq {post_seq}, Period {period} - "
            f"create JournalEntry failed: {fallback_error_message}"
        )
    else:
        return None

    return {
        'error': error_message,
        'PostSeq': post_seq,
        'Period': period,
    }
