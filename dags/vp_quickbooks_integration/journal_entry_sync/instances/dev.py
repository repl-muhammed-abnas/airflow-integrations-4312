"""Instance configuration for VP -> QBO Journal Entry Sync — dev environment."""
# pylint: disable=invalid-name,unused-import
from vp_quickbooks_integration.journal_entry_sync.config import (
    max_active_runs,
    execution_timeout_days,
    initial_sync_time,
    tenant_email
)

instance = 'dev'
region = 'us-east-1'
environment = 'pre-production'
company_key = f"airflowsandbox{region.replace('-', '')}"
middleware_conn_id = f"middleware_conn_{instance}"
