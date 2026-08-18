"""Instance configuration for QBO -> VP Bill Payment Sync — QA environment."""
# pylint: disable=invalid-name,unused-import,import-error
from vp_quickbooks_integration.bill_payment_sync.config import (
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
