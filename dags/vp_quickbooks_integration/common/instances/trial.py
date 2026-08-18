"""Instance configuration for VantagePoint-QuickBooks Common — trial environment."""
# pylint: disable=invalid-name,unused-import
from vp_quickbooks_integration.common.config import (
    execution_timeout_days,
    max_active_runs_master,
    max_active_runs_child,
    internal_logs_email,
    alert_email,
)

# Instance configuration
instance = 'trial'
region = 'us-east-1'
environment = 'pre-production'
company_key = f"VantagePointTrial{region.replace('-', '')}"

# Middleware connection (used by main_dag for the customer-list fetch)
middleware_conn_id = f"middleware_conn_{instance}"

# Regional configuration
default_region = 'US'

# Email notifications
tenant_email = '{{ var.value.vp_quickbooks_trial_email }}'

# Trial-specific schedule: more frequent for testing.
# Overridable per-instance via Airflow Variable
# `vp_qbo_mapping_sync_schedule_interval_trial` (see main_dag.py).
mapping_population_schedule = "0 */23 * * *"  # every 23 hours
