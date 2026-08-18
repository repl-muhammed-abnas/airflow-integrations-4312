"""Instance configuration for VP -> Xero Tax Code Schedule — dev environment."""
# pylint: disable=invalid-name,unused-import
from vp_xero_integration.tax_code_schedule.config import (
    max_active_runs,
    execution_timeout_days,
)

instance = 'dev'
region = 'us-east-1'
environment = 'pre-production'
company_key = f"VantagePointDev{region.replace('-', '')}"
middleware_conn_id = f"middleware_conn_{instance}"
