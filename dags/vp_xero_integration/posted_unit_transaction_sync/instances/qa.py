"""Instance config for VP -> Xero Posted Unit Transaction Sync — QA environment."""
# pylint: disable=invalid-name,unused-import
from vp_xero_integration.posted_unit_transaction_sync.config import (
    max_active_runs,
    execution_timeout_days,
    initial_sync_time,
)

instance = 'qa'
region = 'us-east-1'
environment = 'qa'
company_key = f"airflowqasandbox{region.replace('-', '')}"
middleware_conn_id = f"middleware_conn_{instance}"
