"""Instance configuration for Xero -> VP Tax Code Schedule — dev environment."""
# pylint: disable=invalid-name,unused-import
from vp_xero_integration.xero_to_vp_tax_code_schedule.config import (
    max_active_runs,
    execution_timeout_days,
)

instance = 'dev'
region = 'us-east-1'
environment = 'pre-production'
company_key = f"airflowsandbox{region.replace('-', '')}"
middleware_conn_id = f"middleware_conn_{instance}"
