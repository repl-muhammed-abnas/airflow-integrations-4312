"""Instance configuration for QBO -> VP Bill Payment Sync — dev environment."""
# pylint: disable=invalid-name,unused-import,import-error
from vp_quickbooks_integration.bill_payment_sync.config import (
    max_active_runs,
    max_active_runs_child,
    execution_timeout_days,
    initial_sync_time,
)

instance = 'dev'
region = 'us-east-1'
environment = 'pre-production'
company_key = f"airflowsandbox{region.replace('-', '')}"
middleware_conn_id = f"middleware_conn_{instance}"
