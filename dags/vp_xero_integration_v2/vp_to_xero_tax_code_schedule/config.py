# dags/vp_xero_integration_v2/vp_to_xero_tax_code_schedule/config.py
"""Shared configuration constants for VP -> Xero Tax Code Schedule (V2 IPA GitSync)."""
# pylint: disable=invalid-name

region = 'us-east-1'
environment = 'pre-production'

max_active_runs = 1
execution_timeout_days = 1

default_schedule_interval = '0 * * * *'
