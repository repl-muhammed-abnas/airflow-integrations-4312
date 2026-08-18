# dags/vp_xero_integration_v2/posted_unit_transaction_sync/config.py
"""Shared configuration constants for VP -> Xero Posted Unit Transaction Sync (V2).

Ports the Workato `014_501_psa_poll_vantagepoint_posted_unit_transactions_for_xero`
trigger + `014_501_psa_vantagepoint_unit_journal_exports_to_xero` sub-recipe.
TransType='un' (unit transactions). Mirrors the V1 config with V2 import paths.
"""
# pylint: disable=invalid-name
from vp_xero_integration_v2.common.python_callable_method import watermark_key_template

region = 'us-east-1'
environment = 'pre-production'

max_active_runs = 1
execution_timeout_days = 1

# Vantagepoint PSA Ledger TransType filter — unit transactions are always 'un'.
TRANS_TYPE = 'un'

# First-poll watermark backstop. Ops overrides via per-tenant Variable for
# intentional backfills.
initial_sync_time = '2026-05-01T00:00:00.000Z'

# Per-customer watermark Variable key template.
# Resolves to: vp_xero_{customer_id}_unit_transaction_sync_last_run
watermark_variable_key_template = watermark_key_template('unit_transaction_sync')

default_schedule_interval = '*/15 * * * *'
