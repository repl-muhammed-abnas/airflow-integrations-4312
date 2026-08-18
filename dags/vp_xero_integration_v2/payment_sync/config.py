# dags/vp_xero_integration_v2/payment_sync/config.py
"""Configuration constants for Xero -> VP Payment Sync (V2 IPA GitSync).

Ports the Workato bundle at `vp_xero_workato/payment_sync/`.
Two payment types:
- ACCRECPAYMENT → VP Cash Receipt (CR)
- ACCPAYPAYMENT → VP AP/Expense Payment (PP or EP)
"""
# pylint: disable=invalid-name
from vp_xero_integration_v2.common.python_callable_method import watermark_key_template

region = 'us-east-1'
environment = 'pre-production'

max_active_runs = 1
execution_timeout_days = 1

initial_sync_time = '2026-01-01T00:00:00.000Z'

# Per-customer watermark Variable key template.
# Resolves to: vp_xero_{customer_id}_payment_sync_last_run
watermark_variable_key_template = watermark_key_template('payment_sync')

default_schedule_interval = '*/5 * * * *'
