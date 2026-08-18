"""
Shared configuration constants for VP -> Xero Posted Journal Entry
Sync (posted_journal_sync).

Ports the Workato `014_501_psa_poll_vantagepoint_posted_journal_for_xero`
trigger + `014_501_psa_vantagepoint_journal_exports_to_xero` sub-recipe.
Follows the same 3-DAG main/dispatcher/create polling pattern used
across vp_xero_integration syncs — same PSA Ledger source and cadence,
TransType='je' (journal entries).
"""
# pylint: disable=invalid-name
region = 'us-east-1'
environment = 'pre-production'

max_active_runs = 1
execution_timeout_days = 1

# Vantagepoint PSA Ledger TransType filter for this integration. Per the
# ticket, hardcoded here (not an Airflow Variable) — journal entries are
# always TransType='je'; the sibling TransType='un' integration has its
# own folder + config rather than branching on a Variable.
TRANS_TYPE = 'je'

# First-poll watermark. Deliberately recent so onboarding a new tenant
# does not backfill historical journal entries into Xero as manual
# journals. Ops overrides the per-tenant Variable for intentional
# backfills.
initial_sync_time = '2026-05-01T00:00:00.000Z'

# Default polling cadence for the main_dag scheduler. Per-tenant
# override Variable: `vp_xero_journal_sync_schedule_interval_{instance}`.
default_schedule_interval = '*/15 * * * *'
