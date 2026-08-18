"""Shared configuration constants for VP -> Xero Firm Sync Upsert.

Ports the Workato bundle at `integration_vantagepoint_xero/code/014-501 PSA/`
(firm_upserted trigger + sync_firms + upsert_contact_in_xero recipes) into the
RAIL 3-DAG polling template (main -> dispatcher -> processor), mirroring the
QBO `vp_quickbooks_integration/customer_sync_upsert/` reference.

Direction: VP -> Xero (VantagePoint is master; Xero contacts are the target).
Polling: VP /firm with filterHash datetime window (no ClientInd filter —
Xero syncs both client AND vendor firms, unlike QBO which filters ClientInd=Y).
"""
# pylint: disable=invalid-name
from vp_xero_integration_v2.common.python_callable_method import (
    watermark_key_template,
)

region = 'us-east-1'
environment = 'pre-production'

max_active_runs = 1
max_active_runs_child = 5
execution_timeout_days = 1

# First-run watermark backstop. Workato's polling_firm_updated trigger used
# "No limit" (full backfill on first poll); the equivalent here is a far-past
# initial sync time so the first dispatcher run pulls all modified firms.
initial_sync_time = '2026-05-01T00:00:00.000Z'

# Per-customer watermark Variable key template:
#   vp_xero_{customer_id}_firm_sync_upsert_last_run
watermark_variable_key_template = watermark_key_template('firm_sync_upsert')

default_schedule_interval = '*/5 * * * *'
