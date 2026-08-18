"""Instance config for Xero -> VP Poll Contact Updates Sync — dev environment."""
# pylint: disable=invalid-name,unused-import
from vp_xero_integration.poll_contact_updates_sync.config import (
    max_active_runs,
    execution_timeout_days,
)

instance = 'dev'
region = 'us-east-1'
environment = 'pre-production'
company_key = f"airflowsandbox{region.replace('-', '')}"
middleware_conn_id = f"middleware_conn_{instance}"

initial_sync_time = '2026-07-01T00:00:00.000Z'
