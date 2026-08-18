"""Instance configuration for Xero -> VP Chart of Accounts Sync — dev environment."""
# pylint: disable=invalid-name,unused-import
from vp_xero_integration.chart_of_accounts_sync.config import (
    max_active_runs,
    execution_timeout_days,
    initial_sync_time,
)

instance = 'dev'
region = 'us-east-1'
environment = 'pre-production'
company_key = f"airflowsandbox{region.replace('-', '')}"
middleware_conn_id = f"middleware_conn_{instance}"

# Email notifications (per-instance Airflow Variable, Xero convention).
tenant_email = '{{ var.value.vp_xero_dev_email }}'
