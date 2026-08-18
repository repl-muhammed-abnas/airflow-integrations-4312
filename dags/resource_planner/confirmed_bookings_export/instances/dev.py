# pylint: disable=wildcard-import unused-wildcard-import
from resource_planner.confirmed_bookings_export.config import *
from datetime import datetime

instance = "dev"
region = 'us-east-1'
environment = 'pre-production'

# Replicon configuration
company_key = 'Repliconpincstream6dev'
replicon_conn_id = 'replicon_Repliconpincstream6dev_replicon'

# RP Backend API (integration_gateway) connection
rp_api_conn_id = 'resource_planning_api_connection'
rp_api_target_table = None  # Use production table

# Airflow Variable names — set these manually before first run
cursor_variable_key = f"rp_confirmed_bookings_cursor_{instance}"
tenant_id_variable = f"rp_tenant_id_{instance}"
employee_user_uri_map_variable = f"rp_employee_user_uri_map_{instance}"
resource_planner_confirmed_bookings_export_enable_batch_task = f"resource_planner_confirmed_bookings_export_enable_batch_task_{instance}"

# Failure-notification email recipients (one email per master run when failures occur)
email_failure_recipients = [
    "sammedkawade@deltek.com",
    "DPS-Ops-RP-Support@deltek.com",
]
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'

# DAG configuration
start_date = datetime(2025, 1, 1)
