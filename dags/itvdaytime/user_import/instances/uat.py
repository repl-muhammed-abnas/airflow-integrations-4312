# pylint: disable=wildcard-import unused-wildcard-import
from itvdaytime.user_import.config import *

instance = "uat"
environment = 'pre-production'
region = 'eu-central-1'
company_key = "itvdaytimetrial03"

sftp_conn_id = "sftp-itvdaytime-563217"
replicon_conn_id = "replicon-itvdaytime-radmin"

input_filepath = "/Trial/Import/User Sync"
archive_filepath = "/Trial/Import/Archive"
log_filepath = "/Trial/Import/Log"

delimiter = ","
master_schedule_interval = 30

max_active_runs_master = 1
max_active_runs_child = 10

pgp_connection_id = f"pgp_{company_key}"
job_role_timeoff_mapper = "itvdattimetrial01_user_sync_jobrole_timeoff_mapper"

tenant_email = "technologyservicedesk@itv.com"
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
