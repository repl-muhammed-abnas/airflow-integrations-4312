"""Instance configuration for VP -> Xero Firm Sync Upsert — QA environment."""
# pylint: disable=invalid-name,unused-import
from vp_xero_integration.firm_sync_upsert.config import (
    max_active_runs,
    max_active_runs_child,
    execution_timeout_days,
    initial_sync_time,
)

instance = 'qa'
region = 'us-east-1'
environment = 'qa'
company_key = f"airflowqasandbox{region.replace('-', '')}"
middleware_conn_id = f"middleware_conn_{instance}"

tenant_email = '{{ var.value.vp_xero_qa_email }}'
