# pylint: disable=wildcard-import unused-wildcard-import
from resource_planner.project_task_export_delta.config import *
from datetime import datetime

instance = "prod"
region = 'us-east-1'
environment = 'production'

# Replicon configuration
company_key = 'RepliconPInc'
replicon_conn_id = 'replicon_RepliconPInc_resourceplannertool.integration'

# Airflow Variable for batch task toggle
resource_planner_project_task_export_enable_batch_task = f"resource_planner_project_task_export_enable_batch_task_{instance}"

# RP Backend API connection
rp_api_conn_id = 'resource_planning_api_connection'
rp_api_db_env = "prod"
rp_api_target_table = None  # Use production table

# Failure-notification email recipients (one email per run when failures occur)
email_failure_recipients = [
    "sammedkawade@deltek.com",
    "DPS-Ops-RP-Support@deltek.com",
]
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'

# DAG configuration
start_date = datetime(2025, 1, 1)
max_active_runs = 1
max_active_runs_child = 10
schedule_interval = "*/15 * * * *"  # every 15 minutes
