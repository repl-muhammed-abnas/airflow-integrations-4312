# pylint: disable=wildcard-import unused-wildcard-import
from resource_planner.source_opportunities_project_sync.config import *
from datetime import datetime

instance = "prod"
region = 'us-east-1'
environment = 'production'

# Replicon configuration — standard resource_planner tenant convention (same
# as confirmed_bookings_export). UNCONFIRMED for this specific integration —
# must be verified with the Polaris admin before unpausing, since creating
# real projects against the wrong tenant isn't easily reversible.
company_key = 'RepliconPInc'
replicon_conn_id = 'replicon_RepliconPInc_resourceplannertool.integration'

# RP Backend API (integration_gateway) connection
rp_api_conn_id = 'resource_planning_api_connection'
rp_api_db_env = "prod"
rp_api_target_table = None  # Use production table

# Airflow Variable names — set these manually before first run
cursor_variable_key = f"rp_source_opportunities_cursor_{instance}"
resource_planner_source_opportunities_project_sync_enable_batch_task = f"resource_planner_source_opportunities_project_sync_enable_batch_task_{instance}"

# Failure-notification email recipients (one email per master run when failures occur)
email_failure_recipients = [
    "sammedkawade@deltek.com",
]
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'

# DAG configuration
start_date = datetime(2025, 1, 1)
# schedule_interval deliberately left at config.py's default (None / manual
# trigger only) — unlike confirmed_bookings_export's prod override, this
# integration's target tenant and project templates are still unconfirmed
# (see config.py placeholders), so prod should not run unattended yet.
