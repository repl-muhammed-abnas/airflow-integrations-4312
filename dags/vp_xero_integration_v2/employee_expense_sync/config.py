# dags/vp_xero_integration_v2/employee_expense_sync/config.py
"""Shared configuration constants for VP -> Xero Employee Expense Sync (V2 IPA GitSync)."""
# pylint: disable=invalid-name
from vp_xero_integration_v2.common.python_callable_method import watermark_key_template

region = 'us-east-1'
environment = 'pre-production'

max_active_runs = 1
execution_timeout_days = 1

# VP PSA Ledger transaction type for employee expenses.
PSA_LEDGER_TRANS_TYPE = 'ex'

# First-poll watermark backstop. Ops overrides via per-tenant Variable for
# intentional backfills.
initial_sync_time = '2026-01-01T00:00:00.000Z'

# Per-customer watermark Variable key template.
# Resolves to: vp_xero_{customer_id}_employee_expense_sync_last_run
watermark_variable_key_template = watermark_key_template('employee_expense_sync')

default_schedule_interval = '*/15 * * * *'
