# pylint: disable=wildcard-import unused-wildcard-import
from itvdaytime.user_import.config import *

environment = 'pre-production'

region = 'eu-central-1'
instance = "trial"
company_key = "itvdaytimetrial03"

sftp_conn_id = "sftp_useast2"  # "sftp-itvdaytime-internal"

replicon_conn_id = "replicon-itvdaytime-radmin"

input_filepath = "/iTV/Trial/Import/User Sync/"
archive_filepath = "/iTV/Trial/Import/User Sync/Archive/"
log_filepath = "/iTV/Trial/Import/User Sync/Logs/"

delimiter = ","
master_schedule_interval = 30

max_active_runs_master = 1
max_active_runs_child = 10

pgp_connection_id = f"pgp_{company_key}"
job_role_timeoff_mapper = "itvdattimetrial01_user_sync_jobrole_timeoff_mapper"

# to be updated as per spec while deploying for UAT
tenant_email = '{{ var.value.dagrun_internal_testing_email }}'


internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'

alert_email = '{{ var.value.dagrun_failure_alert_email }}'
disabled = True
