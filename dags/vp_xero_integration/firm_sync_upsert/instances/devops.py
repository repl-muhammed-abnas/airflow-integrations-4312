"""Instance configuration for VP -> Xero Firm Sync Upsert — Devops environment."""
# pylint: disable=invalid-name,unused-import
from vp_xero_integration.firm_sync_upsert.config import (
    max_active_runs,
    max_active_runs_child,
    execution_timeout_days,
    initial_sync_time,
)

instance = 'devops'
region = 'us-west-2'
environment = 'devops'
company_key = f"airflowdevops{region.replace('-', '')}"
middleware_conn_id = f"middleware_conn_{instance}"

tenant_email = '{{ var.value.vp_xero_devops_email }}'
