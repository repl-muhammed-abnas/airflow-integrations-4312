# pylint: disable=wildcard-import unused-wildcard-import
from resource_planner.timeoff_export.config import *
from datetime import datetime

instance = "dev"
region = 'us-east-1'
environment = 'pre-production'

# Replicon configuration
company_key = 'Repliconpincstream6dev'
replicon_conn_id = 'replicon_Repliconpincstream6dev_replicon'

# Airflow Variable for batch task toggle
resource_planner_timeoff_export_enable_batch_task = f"resource_planner_timeoff_export_enable_batch_task_{instance}"

# RP Backend API connection
rp_api_conn_id = 'resource_planning_api_connection'
rp_api_target_table = None  # Use production table

# Failure-notification email recipients (one email per run when failures occur)
email_failure_recipients = [
    "sammedkawade@deltek.com",
    "DPS-Ops-RP-Support@deltek.com",
]
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'

# DAG configuration
start_date = datetime(2025, 1, 1)
schedule_interval = "0 * * * *"  # hourly, on the hour
