# dags/vp_xero_integration_v2/employee_sync_upsert/config.py
"""Shared configuration constants for VP -> Xero Employee Sync Upsert (V2 IPA GitSync)."""
# pylint: disable=invalid-name
from vp_xero_integration_v2.common.python_callable_method import watermark_key_template

region = 'us-east-1'
environment = 'pre-production'

max_active_runs = 1
execution_timeout_days = 1

initial_sync_time = '2015-12-16T03:30:41.203Z'

# Per-customer watermark Variable key template.
# Resolves to: vp_xero_{customer_id}_employee_sync_upsert_last_run
watermark_variable_key_template = watermark_key_template('employee_sync_upsert')

default_schedule_interval = '*/5 * * * *'
