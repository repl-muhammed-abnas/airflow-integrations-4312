"""Instance configuration for VP -> Xero Employee Sync Upsert — QA environment."""
# pylint: disable=invalid-name,unused-import
from vp_xero_integration.employee_sync_upsert.config import (
    max_active_runs,
    execution_timeout_days,
    initial_sync_time,
)

instance = 'qa'
region = 'us-east-1'
environment = 'qa'
company_key = f"airflowqasandbox{region.replace('-', '')}"
middleware_conn_id = f"middleware_conn_{instance}"

tenant_email = '{{ var.value.vp_xero_qa_email }}'
