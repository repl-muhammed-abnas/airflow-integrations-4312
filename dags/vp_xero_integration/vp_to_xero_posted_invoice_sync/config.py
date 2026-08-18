"""Configuration constants for VP PSA -> Xero Posted Invoices Sync."""
from vp_xero_integration.common.python_callable_method import watermark_key_template

region = 'us-east-1'
environment = 'pre-production'
max_active_runs = 1
execution_timeout_days = 1
# First-poll watermark. Deliberately recent so onboarding a new tenant does not
# backfill historical AR invoice batches into Xero. Ops overrides the per-tenant
# Variable for intentional backfills.
initial_sync_time = '2026-07-01T00:00:00.000Z'
watermark_variable_key_template = watermark_key_template('posted_invoice_sync')
# resolves to: 'vp_xero_{customer_id}_posted_invoice_sync_last_run'
