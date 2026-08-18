"""
Shared configuration constants for QBO -> Vantagepoint Invoice Payment Sync.
"""
# pylint: disable=invalid-name
region = 'us-east-1'
environment = 'pre-production'

max_active_runs = 1
execution_timeout_days = 1

# Default watermark for the first run of a new tenant.  After the first
# successful dispatcher run the watermark is advanced and this value is
# no longer read. Kept recent to bound backfill cost if a new tenant
# skips setting their own initial watermark Variable.
initial_sync_time = '2026-05-01T00:00:00.000Z'

from vp_quickbooks_integration.common.config import payment_lookback_minutes  # noqa: F401