"""Instance configuration for VP -> QBO Employee Sync — QA environment."""
# pylint: disable=invalid-name,unused-import
from vp_quickbooks_integration.employee_sync_upsert.config import (
    max_active_runs,
    execution_timeout_days,
    initial_sync_time,
    tenant_email
)

instance = 'qa'
region = 'us-east-1'
environment = 'qa'
company_key = f"airflowqasandbox{region.replace('-', '')}"
middleware_conn_id = f"middleware_conn_{instance}"
