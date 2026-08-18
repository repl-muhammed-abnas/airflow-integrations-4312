"""Instance configuration for VantagePoint-Xero Common — dev environment."""
# pylint: disable=invalid-name,unused-import
from vp_xero_integration.common.config import (
    execution_timeout_days,
    max_active_runs_master,
    max_active_runs_child,
    internal_logs_email,
    alert_email,
)

# Instance configuration
instance = 'dev'
region = 'us-east-1'
environment = 'pre-production'
company_key = f"VantagePointDev{region.replace('-', '')}"

# Middleware connection (used by main_dag for the customer-list fetch)
middleware_conn_id = f"middleware_conn_{instance}"

# Regional configuration
default_region = 'US'

# Email notifications
tenant_email = '{{ var.value.vp_xero_dev_email }}'

# Daily at 3 AM (matches the shared rail_config default used elsewhere).
# Overridable per-instance via Airflow Variable
# `vp_xero_mapping_sync_schedule_interval_dev` (see main_dag.py).
mapping_population_schedule = "0 3 * * *"
