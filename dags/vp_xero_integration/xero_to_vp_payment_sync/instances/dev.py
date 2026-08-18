"""Instance configuration for Xero -> VP Payment Sync — Dev environment."""
# pylint: disable=invalid-name,unused-import
from vp_xero_integration.xero_to_vp_payment_sync.config import (
    max_active_runs,
    execution_timeout_days,
    initial_sync_time,
)

instance = 'dev'
region = 'us-east-1'
environment = 'pre-production'
company_key = f"airflowsandbox{region.replace('-', '')}"
middleware_conn_id = f"middleware_conn_{instance}"
tenant_email = '{{ var.value.vp_xero_dev_email }}'
