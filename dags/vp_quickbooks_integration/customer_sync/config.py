"""
Shared configuration constants for QBO -> Vantagepoint Customer Sync.
"""
# pylint: disable=invalid-name
region = 'us-east-1'
environment = 'pre-production'

max_active_runs = 1
execution_timeout_days = 1

# Default backstop for the polling watermark on the FIRST run for a tenant
# (when no `psa_vp_qbo_customers_{created,updated}_last_sync_*` Variable
# exists yet). After the first successful dispatcher run the watermark is
# advanced and this value is no longer read. New tenants going live should
# set their own initial watermark Variables to avoid backfilling from this
# default. Kept recent to bound backfill cost if that step is skipped.
initial_sync_time = '2026-05-01T00:00:00.000Z'

customer_lookback_minutes = 30
