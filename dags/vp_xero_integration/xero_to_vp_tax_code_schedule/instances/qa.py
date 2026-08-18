"""Instance configuration for Xero -> VP Tax Code Schedule — QA environment."""
# pylint: disable=invalid-name,unused-import
from vp_xero_integration.xero_to_vp_tax_code_schedule.config import (
    max_active_runs,
    execution_timeout_days,
)

instance = 'qa'
region = 'us-east-1'
environment = 'qa'
company_key = f"airflowqasandbox{region.replace('-', '')}"
middleware_conn_id = f"middleware_conn_{instance}"
