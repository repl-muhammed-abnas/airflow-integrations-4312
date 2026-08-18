"""
Common utility methods for VP -> Xero Posted Journal Entry Sync.

Translates the Workato recipes (poll + JE export) into Python callables
for the 3-DAG Airflow template (main -> dispatcher ->
journal_export_create). Single-leaf topology: each (Period, PostSeq)
produces exactly one balanced Xero ManualJournal.

Two deliberate deviations from the literal source recipe were made
during implementation:

1. Account-code resolution reads the real `map_chart_of_accounts` S3
   collection that `mapping_sync/map_account_code_dag.py` now populates,
   via `common.python_callable_method.collection_rows` — NOT the
   sibling's ad hoc `vp_xero_map_account_code_{instance}` Airflow
   Variable (that Variable predates mapping_sync's promotion to a real
   collection and was never migrated).
2. Narration is a deterministic `"JE {Period} {PostSeq}"` instead of the
   recipe's literal first-row `Desc1` free text. The literal recipe value
   is not a stable dedupe key, and the ticket requires Period/PostSeq
   deduplication — search-before-create requires a deterministic
   Narration to match on (Xero has no create-time idempotency key).

Source recipe: `014_501_psa_vantagepoint_journal_exports_to_xero
.recipe.json` (Vantagepoint-Quickbooks-Migration repo,
`integration_vantagepoint_xero/code/014-501 PSA/GL/`; active
create_manual_journal block ~line 4331). This Workato recipe export is
intentionally NOT committed into airflow-integrations — treat the
citation above as a pointer to the other repo, not a path in this one.

Error reporting goes to middleware via
PostDagRunDetailsToMiddlewareApiOperator + FailOperator on the
dispatcher's failure branch; no email/log-table path (matches every
other sync in this repo).
"""
# pylint: disable=invalid-name,too-many-locals
import logging

from airflow.models import Variable
import rail
from vp_xero_integration.common.python_callable_method import (
    watermark_key_template,
    collection_rows,
    prepare_sync_timestamps,
    update_last_sync_time,
)
from vp_xero_integration.common.tables import (
    MAP_CHART_OF_ACCOUNTS_TABLE_NAME,
    MAP_CHART_OF_ACCOUNTS_COLUMNS,
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Watermark helpers — delegate to common; only integration-specific bits here
# ---------------------------------------------------------------------------
WATERMARK_VARIABLE_KEY_TEMPLATE = watermark_key_template('journal_sync')


def prepare_sync_timestamps_method(instance, fallback_initial_sync_time):
    """Wrapper: delegates to common.prepare_sync_timestamps.

    `fallback_initial_sync_time` must come from the per-instance config
    object (`config.initial_sync_time`), NOT a module-level import of the
    shared default — each instances/*.py can override it.
    """
    return prepare_sync_timestamps(
        instance, WATERMARK_VARIABLE_KEY_TEMPLATE, fallback_initial_sync_time
    )


def update_last_sync_time_method(instance):
    """Wrapper: skips when the integration is disabled, then delegates to common.

    trigger_rule='all_done' on the caller fires this on every terminal state,
    including the disabled path; the disabled-flag guard here prevents the
    watermark from advancing when the run was skipped entirely.
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
    """True when CFG_DisableJournalXeroIntegration_{instance} != 'true'."""
    flag = Variable.get(
        f'CFG_DisableJournalXeroIntegration_{instance}',
        default_var='false'
    )
    enabled = str(flag).strip().lower() != 'true'
    if not enabled:
        print(
            f"Journal entry Xero integration disabled for instance "
            f"'{instance}' via "
            f"CFG_DisableJournalXeroIntegration_{instance}"
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
    result = rail.result('fetch_journal_rows') or []
    if isinstance(result, list):
        return result
    if isinstance(result, dict):
        return result.get('entries') or result.get('data') or []
    return []


# ---------------------------------------------------------------------------
# Account-code mapping (real `map_chart_of_accounts` S3 collection)
#
# The export recipe resolves Vantagepoint account -> Xero account via the
# "014-501 PSA Map Chart of Accounts" Workato lookup table. On the Airflow
# side that table is now the `map_chart_of_accounts` S3 collection that
# `mapping_sync/map_account_code_dag.py` populates (keyed by XeroID) — read
# it directly rather than using the stale `vp_xero_map_account_code_{instance}`
# Airflow Variable (a v1 stopgap that predates mapping_sync's S3 collection).
# ---------------------------------------------------------------------------
def get_account_code_mapping_method():
    """Load the Vantagepoint-code -> Xero-code map for this tenant."""
    rows = collection_rows(
        MAP_CHART_OF_ACCOUNTS_TABLE_NAME,
        MAP_CHART_OF_ACCOUNTS_COLUMNS,
        where_sql='1=1',
        params=[],
        read_task_id='_read_map_chart_of_accounts',
    )
    return rows


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
            f"PostSeq={post_seq} (TransType=je). Nothing to post."
        )
    }


def log_missing_account_mapping():
    """Missing account-code mapping for one or more resolved rows."""
    post_seq, period = _conf_keys()
    rows = rail.result('resolve_rows') or []
    missing_codes = sorted({
        str(r.get('Account') or '')
        for r in rows if not r.get('XeroCode')
    } - {''})
    return {
        'error': (
            f"Failed to post journal entry (period: {period}, post "
            f"sequence: {post_seq}) to Xero. Details of error: "
            f"Vantagepoint account(s) "
            f"{', '.join(missing_codes) or '(none)'} not matched to a "
            f"Xero account in the map_chart_of_accounts mapping."
        )
    }


# ---------------------------------------------------------------------------
# Xero ManualJournal body builder (recipe line 4331-4526, active block)
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

    The recipe's query_list step splits each row's signed Amount into
    DebitAmount (Amount > 0) / CreditAmount (Amount < 0), then the
    create_manual_journal block sets LineAmount to whichever of the two
    is non-zero — algebraically that is just the row's original signed
    Amount, so it is used as-is here (no PostingType/Debit-Credit split,
    no Entity/contact reference on Xero's JournalLine).
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

    Mirrors the recipe's active create_manual_journal block, except
    Narration is deterministic (see module docstring, deviation 2):
      - Narration : "JE {Period} {PostSeq}" (recipe uses free-text Desc1;
        deviated here so search_existing_manual_journal has a stable key)
      - Date      : earliest TransDate across rows
      - LineAmountTypes: "NoTax" (recipe comment: Xero tax
        inclusive/exclusive settings are not respected on journal
        lines, so tax is excluded here too)
      - Status    : "POSTED"
      - JournalLines: one entry per row, signed LineAmount as-is.
      - Validates sum(LineAmount) == 0 (Xero ManualJournals must
        balance to zero).
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
        'Narration': f"JE {period} {post_seq}",
        'Date': txn_date,
        'LineAmountTypes': 'NoTax',
        'Status': 'POSTED',
        'JournalLines': lines,
    }


# ---------------------------------------------------------------------------
# Idempotency guard (search-before-create)
#
# ManualJournal has no DocNumber-equivalent user-settable key, so the
# deterministic Narration ("JE {Period} {PostSeq}") built above is the
# dedupe key. Without this guard, a task retry after a lost/timed-out
# POST response (create_manual_journal runs with retries=3) would create
# a second identical ManualJournal in the customer's Xero ledger — Xero's
# API has no create-time idempotency key.
# search_existing_manual_journal uses XeroManualJournalOperator(operation=
# 'search'), which embeds `where` into its `filters` template_field so
# Airflow renders the Jinja {{ }} placeholders at execution time — the
# `where` expression is not a callable here.
# ---------------------------------------------------------------------------
def is_already_posted_method():
    """
    True iff a ManualJournal with this (Period, PostSeq)'s Narration already
    exists in Xero.

    `search_existing_manual_journal` uses XeroManualJournalOperator, whose
    `_format_xero_response` normalises the raw envelope into
    `{'success': ..., 'entity_type': 'ManualJournals', 'data': [...], 'count': N}`.
    The matching journals are under `data`, not the raw `ManualJournals` key.
    """
    result = rail.result('search_existing_manual_journal') or {}
    data = result.get('data') if isinstance(result, dict) else None
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
