"""
Shared configuration constants for VP -> QuickBooks Unit Transaction
Sync (unit_transaction_sync).

Mirrors timesheets_sync/config.py — same polling cadence, same PSA
Ledger source, different TransType ('un' instead of 'ts') and different
QBO target entity (JournalEntry instead of TimeActivity).
"""
# pylint: disable=invalid-name
region = 'us-east-1'
environment = 'pre-production'

max_active_runs = 1
execution_timeout_days = 1

# First-poll watermark. Deliberately recent so onboarding a new tenant
# does not backfill historical unit transactions into QBO as journal
# entries. Ops overrides the per-tenant Variable for intentional
# backfills.
initial_sync_time = '2026-05-01T00:00:00.000Z'

# Default polling cadence for the main_dag scheduler. Per-tenant
# override Variable: `vp_qbo_unit_transaction_sync_schedule_interval_{instance}`.
default_schedule_interval = '*/5 * * * *'
