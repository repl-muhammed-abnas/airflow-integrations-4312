"""
Common utility methods for VP -> Xero Posted Journal Entry Sync (V2).

Translates the Workato recipes (poll + JE export) into Python callables
for the 2-DAG Airflow template (dispatcher -> journal_export_create).
Single-leaf topology: each (Period, PostSeq) produces exactly one balanced
Xero ManualJournal.

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

Shared implementation lives in common._manual_journal_sync; this module
provides the 5 recipe-specific wrappers (narration prefix 'JE', XCom
task 'fetch_journal_rows', trans_type 'je').
"""
# pylint: disable=invalid-name
import logging

import rail
from vp_xero_integration_v2.common.python_callable_method import (
    watermark_key_template,
    prepare_sync_timestamps,
    update_last_sync_time,
)
from vp_xero_integration_v2.common._manual_journal_sync import (
    build_psa_ledger_filter_method,
    build_record_filter,
    get_account_code_mapping_method,
    check_rows_fetched_method,
    is_account_mapping_resolved_method,
    is_already_posted_method,
    log_already_posted,
    capture_create_dag_error,
    _base_fetched_rows,
    _base_resolve_rows,
    _base_log_no_rows_to_post,
    _base_log_missing_account_mapping,
    _base_build_manual_journal_body,
)

logger = logging.getLogger(__name__)

# Re-export shared functions unchanged so dispatcher/processor DAGs can
# import them from this module without knowing about the common base.
__all__ = [
    'build_psa_ledger_filter_method',
    'build_record_filter',
    'get_account_code_mapping_method',
    'check_rows_fetched_method',
    'is_account_mapping_resolved_method',
    'is_already_posted_method',
    'log_already_posted',
    'capture_create_dag_error',
    'WATERMARK_VARIABLE_KEY_TEMPLATE',
    'prepare_sync_timestamps_method',
    'update_last_sync_time_method',
    '_fetched_rows',
    'resolve_rows_method',
    'log_no_rows_to_post',
    'log_missing_account_mapping',
    'build_manual_journal_body',
]


# ---------------------------------------------------------------------------
# Watermark helpers
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
    """Wrapper: delegates to common.update_last_sync_time."""
    return update_last_sync_time(instance, WATERMARK_VARIABLE_KEY_TEMPLATE)


# ---------------------------------------------------------------------------
# Recipe-specific wrappers (journal entry: XCom='fetch_journal_rows', 'JE')
# ---------------------------------------------------------------------------
def _fetched_rows():
    """All PSA Ledger rows for this (Period, PostSeq)."""
    return _base_fetched_rows('fetch_journal_rows')


def resolve_rows_method():
    """Annotate every PSA Ledger row with its resolved Xero account code."""
    return _base_resolve_rows(_fetched_rows)


def log_no_rows_to_post():
    """No PSA Ledger rows came back for this (Period, PostSeq)."""
    return _base_log_no_rows_to_post('je')


def log_missing_account_mapping():
    """Missing account-code mapping for one or more resolved rows."""
    return _base_log_missing_account_mapping('journal entry')


def build_manual_journal_body():
    """Construct the Xero ManualJournal POST body (Narration: 'JE …')."""
    return _base_build_manual_journal_body('JE')
