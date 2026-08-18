# pylint: disable=wildcard-import unused-wildcard-import
from resource_planner.rp_api_health_check.config import *
from datetime import datetime

instance = "prod"
region = 'us-east-1'
environment = 'production'

company_key = 'RepliconPInc'
replicon_conn_id = 'replicon_RepliconPInc_resourceplannertool.integration'

rp_api_conn_id = 'resource_planning_api_connection'

email_alert_recipients = [
    "DPS-Ops-RP-Support@deltek.com",
]
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'

start_date = datetime(2025, 1, 1)
schedule_interval = "*/10 * * * *"  # every 10 minutes
