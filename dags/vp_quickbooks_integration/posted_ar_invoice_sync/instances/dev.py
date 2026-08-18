"""Instance configuration for VP -> QBO Posted AR Invoice Sync — dev environment."""
# pylint: disable=invalid-name,unused-import,line-too-long,import-error
from vp_quickbooks_integration.posted_ar_invoice_sync.config import (
    max_active_runs,
    execution_timeout_days,
)

instance = 'dev'
region = 'us-east-1'
environment = 'pre-production'
company_key = f"airflowsandbox{region.replace('-', '')}"
middleware_conn_id = f"middleware_conn_{instance}"
