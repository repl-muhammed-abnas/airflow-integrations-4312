"""
Shared configuration constants for QBO -> Vantagepoint Bill Payment Sync.

Per-instance values (region, environment, company_key, connection ids) live in
instances/*.py, not here.
"""
region = 'us-east-1'
environment = 'pre-production'

# pylint: disable=invalid-name
max_active_runs = 1
max_active_runs_child = 10
execution_timeout_days = 1

# Default watermark for the first run of a new tenant.  After the first
# successful dispatcher run the watermark is advanced and this value is
# no longer read. Kept recent to bound backfill cost if a new tenant
# skips setting their own initial watermark Variable.
initial_sync_time = '2026-05-01T00:00:00.000Z'

# Matches the Workato `since_offset: -1800` (30-minute lookback).
payment_lookback_minutes = 30

# ---------------------------------------------------------------------------
# Shared mapping_sync collection locator
# ---------------------------------------------------------------------------
# The lookup tables this integration consumes (bank_code_map, map_firm,
# outstanding_purchase_invoices, outstanding_employee_expenses, pay_terms) are
# created and populated by the `mapping_sync` integration, which writes them to
# S3 under its own integration_type partition:
#   vp_quickbooks_integration/mapping_sync/<customer>/collections/collections.db.gz
# bill_payment_sync therefore reads/writes those collections with a FIXED
# integration_type of 'mapping_sync' (NOT this integration's own middleware
# integration_type), so it hits the same db mapping_sync populated.
MAPPING_COLLECTION_INTEGRATION_TYPE = 'mapping_sync'
