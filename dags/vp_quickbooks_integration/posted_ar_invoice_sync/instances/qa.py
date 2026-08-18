"""Instance configuration for VP -> QBO Posted AR Invoice Sync — qa environment."""
# pylint: disable=invalid-name,unused-import,line-too-long,import-error
from vp_quickbooks_integration.posted_ar_invoice_sync.config import (
    max_active_runs,
    execution_timeout_days,
)

instance = 'qa'
region = 'us-east-1'
environment = 'qa'
company_key = f"airflowqasandbox{region.replace('-', '')}"
middleware_conn_id = f"middleware_conn_{instance}"
