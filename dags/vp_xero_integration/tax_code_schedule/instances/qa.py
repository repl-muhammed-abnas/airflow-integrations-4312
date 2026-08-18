"""Instance configuration for VP -> Xero Tax Code Schedule — QA environment."""
# pylint: disable=invalid-name,unused-import
from vp_xero_integration.tax_code_schedule.config import (
    max_active_runs,
    execution_timeout_days,
)

instance = 'qa'
region = 'us-east-1'
environment = 'qa'
company_key = f"VantagePointQA{region.replace('-', '')}"
middleware_conn_id = f"middleware_conn_{instance}"
