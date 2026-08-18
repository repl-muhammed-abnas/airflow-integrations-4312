"""Instance configuration for QBO -> VP Customer Sync — devops environment."""
# pylint: disable=invalid-name,unused-import,import-error
from vp_quickbooks_integration.customer_sync.config import (
    max_active_runs,
    execution_timeout_days,
    initial_sync_time,
)

instance = 'devops'
region = 'us-west-2'
environment = 'devops'
company_key = f"airflowdevops{region.replace('-', '')}"
middleware_conn_id = f"middleware_conn_{instance}"
