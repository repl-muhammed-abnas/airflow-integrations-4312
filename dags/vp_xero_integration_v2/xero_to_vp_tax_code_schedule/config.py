# dags/vp_xero_integration_v2/xero_to_vp_tax_code_schedule/config.py
"""Configuration constants for Tax Code Schedule (V2 IPA GitSync).

Ports the Workato bundle at `vp_xero_workato/xero_to_vp_tax_code_schedule/`.
Distinct from vp_to_xero_tax_code_schedule — Workato recipe is 014-501 Xero pull variant.
"""
# pylint: disable=invalid-name

region = 'us-east-1'
environment = 'pre-production'

max_active_runs = 1
execution_timeout_days = 1

default_schedule_interval = '0 * * * *'
