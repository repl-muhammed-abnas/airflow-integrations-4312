"""Instance configuration for VantagePoint-Xero Mapping Sync — QA environment."""
# pylint: disable=invalid-name,unused-import
from vp_xero_integration.mapping_sync.config import (
    execution_timeout_days,
    max_active_runs_master,
    max_active_runs_child,
    internal_logs_email,
    alert_email,
)

# Instance configuration
instance = 'qa'
region = 'us-east-1'
environment = 'qa'
company_key = f"VantagePointQA{region.replace('-', '')}"

# Middleware connection (used by main_dag for the customer-list fetch)
middleware_conn_id = f"middleware_conn_{instance}"

# Regional configuration
default_region = 'US'

# Email notifications
tenant_email = '{{ var.value.vp_xero_qa_email }}'

# Monthly at midnight on the 1st — dispatcher skips the sync when already
# initialized, so frequent scheduling adds no value. Overridable per-instance
# via Airflow Variable `vp_xero_mapping_sync_schedule_interval_qa`
# (see main_dag.py).
mapping_population_schedule = "0 0 1 * *"
