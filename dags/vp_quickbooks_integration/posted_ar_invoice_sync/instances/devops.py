"""Instance configuration for VP -> QBO Posted AR Invoice Sync — devops environment."""
# pylint: disable=invalid-name,unused-import,line-too-long,import-error
from vp_quickbooks_integration.posted_ar_invoice_sync.config import (
    max_active_runs,
    execution_timeout_days,
)

instance = 'devops'
region = 'us-west-2'
environment = 'devops'
company_key = f"airflowdevops{region.replace('-', '')}"
middleware_conn_id = f"middleware_conn_{instance}"
