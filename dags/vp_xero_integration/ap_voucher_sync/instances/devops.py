# dags/vp_xero_integration/ap_voucher_sync/instances/devops.py
"""Instance configuration for VP -> Xero AP Voucher Sync — devops environment."""
# pylint: disable=invalid-name,unused-import
from vp_xero_integration.ap_voucher_sync.config import (
    max_active_runs,
    execution_timeout_days,
    initial_sync_time,
)

instance = 'devops'
region = 'us-west-2'
environment = 'devops'
company_key = f"airflowdevops{region.replace('-', '')}"
middleware_conn_id = f"middleware_conn_{instance}"
