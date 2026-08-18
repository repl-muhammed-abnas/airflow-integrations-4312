"""Instance configuration for VP QBO Vendor Sync — Devops environment."""
# pylint: disable=invalid-name,unused-import
from vp_quickbooks_integration.vendor_sync.config import (
    max_active_runs,
    execution_timeout_days,
    initial_sync_time,
    tenant_email
)

instance = 'devops'
region = 'us-west-2'
environment = 'devops'
company_key = f"airflowdevops{region.replace('-', '')}"
middleware_conn_id = f"middleware_conn_{instance}"
