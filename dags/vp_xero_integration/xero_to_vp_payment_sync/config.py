"""Configuration constants for Xero -> VP Payment Sync.

Ports the Workato bundle at `vp_xero_workato/payment_sync/` into the RAIL
4-DAG polling template (main -> dispatcher -> invoice_payment_processor +
bill_payment_processor), mirroring the sibling Xero integrations.

Two callable recipes ported:
- `014_501_psa_xero_invoice_payment_adds_to_vantagepoint`  (ACCRECPAYMENT -> CR)
- `014_501_psa_xero_bill_payment_adds_to_vantagepoint`     (ACCPAYPAYMENT -> PP or EP)
"""
# pylint: disable=invalid-name
from vp_xero_integration.common.python_callable_method import (
    watermark_key_template,
)

region = 'us-east-1'
environment = 'pre-production'

max_active_runs = 1
execution_timeout_days = 1

# First-poll watermark. Set to 2026-01-01 so the first dispatcher run pulls
# only payments created after integration go-live; ops overrides the
# per-tenant Variable for intentional backfills.
initial_sync_time = '2026-01-01T00:00:00.000Z'

# Per-customer watermark Variable key template:
#   vp_xero_{customer_id}_payment_sync_last_run
watermark_variable_key_template = watermark_key_template('payment_sync')
