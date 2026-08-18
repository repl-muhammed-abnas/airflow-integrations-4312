# pylint: disable=wildcard-import unused-wildcard-import
from itvdaytime.user_import.config import *

region = "eu-central-1"
environment = 'production'

instance = "production"
company_key = "ITVDaytime"

sftp_conn_id = "sftp_itvdaytime_563217"
replicon_conn_id = "replicon_itvdaytime_radmin"
pgp_connection_id = f"pgp_{company_key}"

input_filepath = "/Production/Import/User Sync"
archive_filepath = "/Production/Import/Archive"
log_filepath = "/Production/Import/Log"

delimiter = ","
master_schedule_interval = 30

max_active_runs_master = 1
max_active_runs_child = 10

job_role_timeoff_mapper = "itvdattime_user_sync_jobrole_timeoff_mapper"

tenant_email = "technologyservicedesk@itv.com"
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
