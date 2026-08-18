"""Shared configuration constants for Xero -> VP Chart of Accounts Sync.

Ports the Workato bundle at `vp_xero_workato/chart_of_accounts/`
(`014_501_psa_poll_xero_chart_of_accounts` + `014_501_psa_sync_accounts`) into
the RAIL 3-DAG polling template (main -> dispatcher -> processor), mirroring the
QuickBooks `vp_quickbooks_integration/chart_of_accounts_sync/config.py`, re-keyed
for Xero.

Top-level constants are shared defaults consumed by every instance file under
`instances/{dev,qa,devops}.py`. `region` and `environment` are read at
module-import time by the production deployment tooling, so they stay at module
scope even though the per-instance files override them.
"""
# pylint: disable=invalid-name
from vp_xero_integration_v2.common.python_callable_method import (
    watermark_key_template,
)

# Production-deployment expectations (read at module import time by RAIL deploy
# tooling; mirrors the sibling integrations' config.py).
region = 'us-east-1'
environment = 'pre-production'

max_active_runs = 1
execution_timeout_days = 1

# First-run watermark backstop. Xero's `updated_account` poll trigger used
# `since_offset = "No limit"` (full backfill on first poll); the equivalent here
# is a far-past initial sync time so the first dispatcher run pulls every
# account via `If-Modified-Since`.
initial_sync_time = '2015-12-16T03:30:41.203Z'

# Per-customer watermark Variable key template:
#   vp_xero_{customer_id}_chart_of_accounts_sync_last_run
watermark_variable_key_template = watermark_key_template(
    'chart_of_accounts_sync'
)

default_schedule_interval = '*/5 * * * *'
