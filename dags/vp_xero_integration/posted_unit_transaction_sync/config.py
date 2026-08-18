"""
Shared configuration constants for VP -> Xero Posted Unit Transaction
Sync (posted_unit_transaction_sync).

Ports the Workato `014_501_psa_poll_vantagepoint_posted_unit_transactions_for_xero`
trigger + `014_501_psa_vantagepoint_unit_journal_exports_to_xero` sub-recipe.
Mirrors `vp_quickbooks_integration/unit_transaction_sync/config.py` — same
polling source (PSA Ledger, TransType='un') and cadence, different target
(Xero ManualJournal instead of QBO JournalEntry).
"""
# pylint: disable=invalid-name
region = 'us-east-1'
environment = 'pre-production'

max_active_runs = 1
execution_timeout_days = 1

# Vantagepoint PSA Ledger TransType filter for this integration. Per the
# ticket, hardcoded here (not an Airflow Variable) — unit transactions
# are always TransType='un'; a future TransType='je' sibling gets its
# own integration folder + config rather than branching on a Variable.
TRANS_TYPE = 'un'

# First-poll watermark. Deliberately recent so onboarding a new tenant
# does not backfill historical unit transactions into Xero as manual
# journals. Ops overrides the per-tenant Variable for intentional
# backfills.
initial_sync_time = '2026-05-01T00:00:00.000Z'

# Default polling cadence for the main_dag scheduler. Per-tenant
# override Variable: `vp_xero_unit_transaction_sync_schedule_interval_{instance}`.
default_schedule_interval = '*/15 * * * *'
