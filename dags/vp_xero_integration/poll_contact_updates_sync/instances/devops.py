"""Instance config for Xero -> VP Poll Contact Updates Sync — devops environment."""
# pylint: disable=invalid-name,unused-import
from vp_xero_integration.poll_contact_updates_sync.config import (
    max_active_runs,
    execution_timeout_days,
)

instance = 'devops'
region = 'us-west-2'
environment = 'devops'
company_key = f"airflowdevops{region.replace('-', '')}"
middleware_conn_id = f"middleware_conn_{instance}"

initial_sync_time = '2026-07-01T00:00:00.000Z'
