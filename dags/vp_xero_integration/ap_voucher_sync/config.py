# dags/vp_xero_integration/ap_voucher_sync/config.py
"""Shared configuration constants for VP -> Xero AP Voucher Sync."""
# pylint: disable=invalid-name
from vp_xero_integration.common.python_callable_method import watermark_key_template

region = 'us-east-1'
environment = 'pre-production'

max_active_runs = 1
execution_timeout_days = 1
initial_sync_time = '2026-05-01T00:00:00.000Z'

# Fallback bill payment period (days) used to compute DueDate.
default_payment_period_days = 30

# Per-customer watermark Variable key template.
# Resolves to: vp_xero_{customer_id}_ap_voucher_sync_last_run
watermark_variable_key_template = watermark_key_template('ap_voucher_sync')
