# pylint: disable=wildcard-import unused-wildcard-import
from resource_planner.ensure_project_tasks.config import *
from datetime import datetime

instance = "test"
region = 'us-east-1'
environment = 'production'

# Replicon configuration
company_key = 'RepliconPIncStream6UAT'
replicon_conn_id = 'replicon_RepliconPIncStream6UAT_resourceplannertool.integration'

# RP Backend API (integration_gateway) connection
rp_api_conn_id = 'resource_planning_api_connection'
rp_api_db_env = "test"
rp_api_target_table = None  # Routes to test DB via rp_api_db_env="test"

# Airflow Variable names — set these manually before first run
tenant_id_variable = f"rp_tenant_id_{instance}"

# Failure-notification email recipients (one email per run when failures occur)
email_failure_recipients = [
    "sammedkawade@deltek.com",
    "DPS-Ops-RP-Support@deltek.com",
]
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'

# DAG configuration
start_date = datetime(2025, 1, 1)
