"""
Common utility methods for VP -> Xero Posted Unit Transaction Sync.

Translates the Workato recipes (poll + JE-style export) into Python
callables for the 3-DAG Airflow template (main -> dispatcher ->
journal_export_create). Single-leaf topology: each (Period, PostSeq)
produces exactly one balanced Xero ManualJournal.

Adapted from `vp_quickbooks_integration/unit_transaction_sync/utils/
python_callable_method.py`, with the VP `/project` + firm-map
resolution dropped: the export recipe's active `create_manual_journal`
block has no Entity/contact reference on its JournalLines, so only the
Vantagepoint-code -> Xero-code account map is needed. The recipe's
second `create_manual_journal` block (referencing an `opposite_entry`
field) is `"skip": true` end-to-end in the source recipe and is
intentionally NOT ported — Vantagepoint's ledger already carries both
debit and credit rows per transaction, so each row maps to one
JournalLine and the set balances without a synthetic offset line.

Source recipe: `014_501_psa_vantagepoint_unit_journal_exports_to_xero
.recipe.json` (Vantagepoint-Quickbooks-Migration repo,
`integration_vantagepoint_xero/code/014-501 PSA/GL/`; active
create_manual_journal block ~line 4362, dead opposite_entry block
~line 8466). This Workato recipe export is intentionally NOT committed
into airflow-integrations — treat the citation above as a pointer to
the other repo, not a path in this one.

Error reporting goes to middleware via
PostDagRunDetailsToMiddlewareApiOperator + FailOperator on the
dispatcher's failure branch; no email/log-table path (matches
vp_quickbooks_integration's vendor_sync, customer_sync,
timesheets_sync, unit_transaction_sync).
"""
# pylint: disable=invalid-name,too-many-locals
import json
import logging

from airflow.models import Variable
import rail
from vp_xero_integration.common.python_callable_method import (
    watermark_key_template,
    prepare_sync_timestamps,
    update_last_sync_time,
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Watermark helpers (delegate to common; template bound here once)
# ---------------------------------------------------------------------------
WATERMARK_VARIABLE_KEY_TEMPLATE = watermark_key_template('unit_transaction_sync')


def prepare_sync_timestamps_method(instance, fallback_initial_sync_time):
    """Thin wrapper: binds the integration-specific template for common.prepare_sync_timestamps.

    `fallback_initial_sync_time` must come from the per-instance config
    object (`config.initial_sync_time`) so per-instance overrides are respected.
    """
    return prepare_sync_timestamps(
        instance, WATERMARK_VARIABLE_KEY_TEMPLATE, fallback_initial_sync_time
    )


def update_last_sync_time_method(instance):
    """Thin wrapper: guards for disabled flag then delegates to common.update_last_sync_time.

    trigger_rule='all_done' on the caller means this fires on every terminal
    state. The disabled-flag guard here prevents advancing the watermark when
    the integration is turned off via CFG_DisableUnitTransactionXeroIntegration_{instance}.
    """
    try:
        is_enabled = rail.result('check_disabled_flag')
    except KeyError:
        is_enabled = True
    if not is_enabled:
        print("Integration disabled; skipping watermark advance")
        return None
    return update_last_sync_time(instance, WATERMARK_VARIABLE_KEY_TEMPLATE)


# ---------------------------------------------------------------------------
# Disabled-flag check
# ---------------------------------------------------------------------------
def is_integration_enabled_method(instance):
    """True when CFG_DisableUnitTransactionXeroIntegration_{instance} != 'true'."""
    flag = Variable.get(
        f'CFG_DisableUnitTransactionXeroIntegration_{instance}',
        default_var='false'
    )
    enabled = str(flag).strip().lower() != 'true'
    if not enabled:
        print(
            f"Unit transaction Xero integration disabled for instance "
            f"'{instance}' via "
            f"CFG_DisableUnitTransactionXeroIntegration_{instance}"
        )
    return enabled


# ---------------------------------------------------------------------------
# PSA Ledger filter
# ---------------------------------------------------------------------------
def build_psa_ledger_filter_method():
    """
    OData ModifiedDate filter for the dispatcher polling query.

    Upper bound is exclusive (`lt`, not `le`): the watermark advances to
    exactly `current_sync_time` on success, so an inclusive upper bound
    would let a row modified at precisely that instant be picked up again
    by the NEXT run's inclusive lower bound (`ge`) — a double-poll ->
    double-post window. `lt` on the upper bound closes that gap; `ge` on
    the lower bound is intentionally still inclusive so nothing is missed.
    """
    timestamps = rail.result('prepare_sync_timestamps')
    return (
        f"?$filter=ModifiedDate ge datetime'{timestamps['last_sync_time']}'"
        f" and ModifiedDate lt datetime'{timestamps['current_sync_time']}'"
    )


# ---------------------------------------------------------------------------
# PSA Ledger row filter (composite-key re-fetch in the create DAG)
# ---------------------------------------------------------------------------
def build_record_filter():
    """
    OData filter to re-fetch all rows for (Period, PostSeq) from conf.

    Period/PostSeq arrive from the poll trigger and dispatcher conf as
    STRINGS (per the recipe's output schema — both typed "string").
    The export recipe re-casts PostSeq to int only when re-querying
    PSALedger (`parse_output: integer_conversion`); we mirror that
    exact narrow cast here and nowhere else. Raises ValueError on
    missing/non-numeric PostSeq so the error surfaces immediately
    rather than silently querying for PostSeq=0.
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
# Row fetch helper
# ---------------------------------------------------------------------------
def _fetched_rows():
    """All PSA Ledger rows for this (Period, PostSeq)."""
    result = rail.result('fetch_unit_transfer_rows') or []
    if isinstance(result, list):
        return result
    if isinstance(result, dict):
        return result.get('entries') or result.get('data') or []
    return []


# ---------------------------------------------------------------------------
# Account-code mapping (v1: Airflow Variable, JSON list)
#
# The export recipe resolves Vantagepoint account -> Xero account via the
# "014-501 PSA Map Chart of Accounts" Workato lookup table (recipe steps
# at query_list `e5f2a917`, line 3974-4156). No S3 mapping_sync
# collection exists for Xero yet (unlike QBO's `map_account_code`
# collection), so v1 stores the map as a single ops-managed Airflow
# Variable — a JSON list of {"VantagepointCode": ..., "XeroCode": ...}
# rows. Promote to a real collection if/when this needs a sync/UI.
# ---------------------------------------------------------------------------
def _account_code_mapping_variable_key(instance):
    return f'vp_xero_map_account_code_{instance}'


def get_account_code_mapping_method(instance):
    """Load the Vantagepoint-code -> Xero-code map for this instance."""
    key = _account_code_mapping_variable_key(instance)
    raw = Variable.get(key, default_var='[]')
    try:
        mapping = json.loads(raw)
    except (TypeError, ValueError):
        logger.warning(
            "Variable '%s' is not valid JSON; treating account-code "
            "mapping as empty.", key
        )
        return []
    return mapping if isinstance(mapping, list) else []


def _lookup_account_code(mapping, vp_account_code):
    vp_code = str(vp_account_code or '').strip()
    if not vp_code:
        return None
    for row in mapping:
        if str(row.get('VantagepointCode') or '').strip() == vp_code:
            return row
    return None


# ---------------------------------------------------------------------------
# Row resolution
# ---------------------------------------------------------------------------
def resolve_rows_method():
    """Annotate every PSA Ledger row with its resolved Xero account code."""
    rows = _fetched_rows()
    account_map = rail.result('fetch_account_code_mapping') or []

    annotated = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        acct_row = _lookup_account_code(account_map, row.get('Account'))
        annotated.append({
            **row,
            'XeroCode': (acct_row or {}).get('XeroCode') or None,
        })

    unresolved_acct = sum(1 for r in annotated if not r.get('XeroCode'))
    print(
        f"resolve_rows: {len(rows)} VP rows; "
        f"unresolved-account={unresolved_acct}"
    )
    return annotated


# ---------------------------------------------------------------------------
# Validation gates
# ---------------------------------------------------------------------------
def check_rows_fetched_method():
    """True iff resolve_rows returned at least one row."""
    return bool(rail.result('resolve_rows'))


def is_account_mapping_resolved_method():
    """True iff every row has a resolved XeroCode. Caller guards for empty list."""
    rows = rail.result('resolve_rows') or []
    return all(r.get('XeroCode') for r in rows)


# ---------------------------------------------------------------------------
# Logging (replaces 014-501 PSA Log Message recipe)
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
    """Recipe error site (line 4159-4357) — missing account-code mapping."""
    post_seq, period = _conf_keys()
    rows = rail.result('resolve_rows') or []
    missing_codes = sorted({
        str(r.get('Account') or '')
        for r in rows if not r.get('XeroCode')
    } - {''})
    return {
        'error': (
            f"Failed to post unit transfer (period: {period}, post "
            f"sequence: {post_seq}) to Xero. Details of error: "
            f"Vantagepoint account(s) "
            f"{', '.join(missing_codes) or '(none)'} not matched to a "
            f"Xero account in the 014-501 PSA Map Chart of Accounts "
            f"mapping."
        )
    }


# ---------------------------------------------------------------------------
# Xero ManualJournal body builder (recipe line 4362-4693, active block)
# ---------------------------------------------------------------------------
def _format_xero_date(value):
    if not value:
        return None
    if isinstance(value, str):
        return value.split('T')[0] if 'T' in value else value[:10]
    return value


def _line_from_row(row):
    """
    Build one Xero JournalLine from one resolved PSA Ledger row.

    Xero's ManualJournal JournalLine carries a single signed
    LineAmount (no PostingType/Debit-Credit split, no Entity/contact
    reference — confirmed absent from the active recipe block), so the
    Vantagepoint row's signed Amount is used as-is.
    """
    try:
        amount = float(row.get('Amount') or 0)
    except (TypeError, ValueError):
        amount = 0.0

    return {
        'AccountCode': row.get('XeroCode') or '',
        'LineAmount': amount,
        'Description': row.get('Desc2') or row.get('Desc1') or '',
    }


def build_manual_journal_body():
    """
    Construct the Xero ManualJournal POST body from resolved rows.

    Mirrors the recipe's active create_manual_journal block:
      - Narration : "UN {Period} {PostSeq}"
      - Date      : earliest TransDate across rows
      - LineAmountTypes: "NoTax" (recipe comment: Xero tax
        inclusive/exclusive settings are not respected on journal
        lines, so tax is excluded here too)
      - Status    : "POSTED"
      - JournalLines: one entry per row, signed LineAmount as-is.
      - Validates sum(LineAmount) == 0 (Xero ManualJournals must
        balance to zero — no separate debit/credit line types the
        way QBO's JournalEntry has).
    """
    post_seq, period = _conf_keys()
    rows = rail.result('resolve_rows') or []
    if not rows:
        raise ValueError(
            f"No resolved rows for Period={period}, PostSeq={post_seq}."
        )

    lines = [_line_from_row(r) for r in rows]

    total = sum(l['LineAmount'] for l in lines)
    if round(total, 2) != 0:
        raise ValueError(
            f"Unbalanced ManualJournal Period={period}, PostSeq={post_seq}: "
            f"sum(LineAmount)={total}. Refusing to POST."
        )

    txn_date = None
    for row in rows:
        candidate = _format_xero_date(row.get('TransDate'))
        if candidate and (txn_date is None or candidate < txn_date):
            txn_date = candidate

    return {
        'Narration': f"UN {period} {post_seq}",
        'Date': txn_date,
        'LineAmountTypes': 'NoTax',
        'Status': 'POSTED',
        'JournalLines': lines,
    }


# ---------------------------------------------------------------------------
# Idempotency guard (search-before-create)
#
# ManualJournal has no DocNumber-equivalent user-settable key the way QBO's
# JournalEntry does, so Narration ("UN {Period} {PostSeq}") is the closest
# thing to a dedupe key available on the Xero side. Without this guard, a
# task retry after a lost/timed-out POST response (create_manual_journal
# runs with retries=3) would create a second identical ManualJournal in the
# customer's Xero ledger — Xero's API has no create-time idempotency key.
# search_existing_manual_journal's `filters` (the `?where=...` clause) is
# passed as a Jinja template string directly in
# journal_export_create_dag.py (XeroAPIOperator templates `filters`), not
# as a callable here — see that file for why.
# ---------------------------------------------------------------------------
def is_already_posted_method():
    """
    True iff a ManualJournal with this (Period, PostSeq)'s Narration already
    exists in Xero.

    `search_existing_manual_journal` is the generic XeroAPIOperator, so its
    XCom is the raw Xero response envelope (`{'ManualJournals': [...], ...}`)
    — NOT the typed operator's normalized `{'data': [...], ...}` shape.
    """
    result = rail.result('search_existing_manual_journal') or {}
    data = result.get('ManualJournals') if isinstance(result, dict) else None
    return bool(data)


def log_already_posted():
    """
    Informational, not an error — the idempotency guard found a prior
    successful post (most likely a retried task after a lost response, or
    an overlapping poll window). Deliberately excluded from
    `capture_create_dag_error`'s error aggregation list so it does not
    fail the dispatcher run.
    """
    post_seq, period = _conf_keys()
    message = (
        f"ManualJournal for Period={period}, PostSeq={post_seq} already "
        f"exists in Xero (Narration match) — skipping duplicate create."
    )
    print(message)
    return {'info': message}


# ---------------------------------------------------------------------------
# Error capture terminal
# ---------------------------------------------------------------------------
def capture_create_dag_error(post_seq, period, fallback_error_message):
    """
    trigger_rule='all_done' terminal. Aggregates validation error dicts
    from log_*_action tasks; falls back to get_error_message() for
    Xero API failures. Returns dict or None (never raises).
    """
    child_errors = []
    for log_task in (
        'log_no_rows_action',
        'log_missing_account_mapping_action',
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
            f"create ManualJournal failed: {fallback_error_message}"
        )
    else:
        return None

    return {
        'error': error_message,
        'PostSeq': post_seq,
        'Period': period,
    }
