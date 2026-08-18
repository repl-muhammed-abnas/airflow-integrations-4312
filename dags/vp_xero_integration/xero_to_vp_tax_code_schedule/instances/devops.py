"""Instance configuration for Xero -> VP Tax Code Schedule — devops environment."""
# pylint: disable=invalid-name,unused-import
from vp_xero_integration.xero_to_vp_tax_code_schedule.config import (
    max_active_runs,
    execution_timeout_days,
)

instance = 'devops'
region = 'us-west-2'
environment = 'devops'
company_key = f"airflowdevops{region.replace('-', '')}"
middleware_conn_id = f"middleware_conn_{instance}"
