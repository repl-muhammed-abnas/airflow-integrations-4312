"""Instance configuration for VP -> Xero Posted Invoices Sync — Devops environment."""
# pylint: disable=invalid-name,unused-import
from vp_xero_integration.vp_to_xero_posted_invoice_sync.config import (
    max_active_runs,
    execution_timeout_days,
    initial_sync_time,
)

instance = 'devops'
region = 'us-west-2'
environment = 'devops'
company_key = f"airflowdevops{region.replace('-', '')}"
middleware_conn_id = f"middleware_conn_{instance}"
tenant_email = '{{ var.value.vp_xero_devops_email }}'
