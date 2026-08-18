"""
Shared configuration for the vp_quickbooks_integration `common` package.

`common` is the shared-code home (utils, config) reused across the
vp_quickbooks_integration workflow folders (mapping_sync, bill_payment_sync,
vendor_sync, ...). Module-level `region` / `environment` are read at import
time by RAIL deploy tooling (mirrors mapping_sync/config.py + vendor_sync/
config.py); per-instance files under `instances/` override the per-environment
values.
"""
# pylint: disable=invalid-name

# Production-deployment expectations (read at module import time by RAIL deploy
# tooling; mirrors mapping_sync/config.py + vendor_sync/config.py).
region = 'us-east-1'
environment = 'pre-production'

# DAG Execution Settings (shared across all instances).
execution_timeout_days = 2
max_active_runs_master = 1
max_active_runs_child = 10

# Email Configuration (shared across all instances). tenant_email is
# per-instance and lives in each instance file.
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

# QBO polling lookback window shared by syncs that query a time range ending
# at last_sync_time. The lookback ensures late-arriving QBO records (created
# within the window but with a timestamp slightly before last_sync_time) are
# not missed between runs.
payment_lookback_minutes = 30
