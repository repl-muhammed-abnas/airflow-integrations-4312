"""Instance config for VP -> QBO Unit Transaction Sync — devops environment."""
# pylint: disable=invalid-name,unused-import,line-too-long,import-error
from vp_quickbooks_integration.unit_transaction_sync.config import (
    max_active_runs,
    execution_timeout_days,
)

instance = 'devops'
region = 'us-west-2'
environment = 'devops'
company_key = f"airflowdevops{region.replace('-', '')}"
middleware_conn_id = f"middleware_conn_{instance}"

initial_sync_time = '2026-05-01T00:00:00.000Z'
