"""Shared implementation for VP -> Xero ManualJournal syncs.

Used by posted_journal_sync and posted_unit_transaction_sync. Each module
defines its 5 recipe-specific wrappers by calling the _base_* variants:

  _fetched_rows()             -> _base_fetched_rows(xcom_task)
  resolve_rows_method()       -> _base_resolve_rows(_fetched_rows)
  log_no_rows_to_post()       -> _base_log_no_rows_to_post(trans_type)
  log_missing_account_mapping() -> _base_log_missing_account_mapping(tx_label)
  build_manual_journal_body() -> _base_build_manual_journal_body(narration_prefix)

All other public functions are identical across both syncs and re-exported
directly from here.
"""
# pylint: disable=invalid-name
import logging

import rail
from vp_xero_integration_v2.common.python_callable_method import collection_rows
from vp_xero_integration_v2.common.tables import (
    MAP_CHART_OF_ACCOUNTS_TABLE_NAME,
    MAP_CHART_OF_ACCOUNTS_COLUMNS,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# PSA Ledger filter (dispatcher polling query)
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
# PSA Ledger row filter (composite-key re-fetch in the processor DAG)
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
# Row fetch helper (recipe-specific XCom task id injected by caller)
# ---------------------------------------------------------------------------
def _base_fetched_rows(xcom_task):
    """All PSA Ledger rows for this (Period, PostSeq) from `xcom_task`."""
    result = rail.result(xcom_task) or []
    if isinstance(result, list):
        return result
    if isinstance(result, dict):
        return result.get('entries') or result.get('data') or []
    return []


# ---------------------------------------------------------------------------
# Account-code mapping (map_chart_of_accounts S3 collection)
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
# Row resolution (caller supplies its _fetched_rows function)
# ---------------------------------------------------------------------------
def _base_resolve_rows(fetch_rows_fn):
    """Annotate every PSA Ledger row with its resolved Xero account code."""
    rows = fetch_rows_fn()
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
    logger.info(
        "resolve_rows: %d VP rows; unresolved-account=%d", len(rows), unresolved_acct
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
# Logging helpers (recipe-specific labels injected by caller)
# ---------------------------------------------------------------------------
def _conf_keys():
    conf = rail.get_current_context()['dag_run'].conf
    return conf.get('PostSeq'), conf.get('Period')


def _base_log_no_rows_to_post(trans_type):
    """No PSA Ledger rows came back for this (Period, PostSeq)."""
    post_seq, period = _conf_keys()
    return {
        'error': (
            f"No PSA Ledger rows returned for Period={period}, "
            f"PostSeq={post_seq} (TransType={trans_type}). Nothing to post."
        )
    }


def _base_log_missing_account_mapping(tx_type_label):
    """Missing account-code mapping for one or more resolved rows."""
    post_seq, period = _conf_keys()
    rows = rail.result('resolve_rows') or []
    missing_codes = sorted({
        str(r.get('Account') or '')
        for r in rows if not r.get('XeroCode')
    } - {''})
    return {
        'error': (
            f"Failed to post {tx_type_label} (period: {period}, post "
            f"sequence: {post_seq}) to Xero. Details of error: "
            f"Vantagepoint account(s) "
            f"{', '.join(missing_codes) or '(none)'} not matched to a "
            f"Xero account in the map_chart_of_accounts mapping."
        )
    }


# ---------------------------------------------------------------------------
# Xero ManualJournal body helpers
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


def _base_build_manual_journal_body(narration_prefix):
    """
    Construct the Xero ManualJournal POST body from resolved rows.

    `narration_prefix` is 'JE' (journal entry) or 'UN' (unit transfer).
    Narration is deterministic — "{prefix} {Period} {PostSeq}" — so that
    the search-before-create idempotency guard has a stable key to match on
    (Xero has no create-time idempotency key for ManualJournals).
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
        'Narration': f"{narration_prefix} {period} {post_seq}",
        'Date': txn_date,
        'LineAmountTypes': 'NoTax',
        'Status': 'POSTED',
        'JournalLines': lines,
    }


# ---------------------------------------------------------------------------
# Idempotency guard (search-before-create)
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
    logger.info(message)
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
