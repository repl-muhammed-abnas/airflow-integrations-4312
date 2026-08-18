# pylint: disable=wildcard-import unused-wildcard-import
from horizonmedia.timesheet_approval_for_disabled_users.config import *

region = 'us-east-1'
instance = "trial"
environment = 'pre-production'
company_key = 'HorizonMediaGen3afmig'

replicon_conn_id = 'horizonmediagen3afmig_replicon_admin'
sftp_conn_id = "sftp_useast2"

schedule_interval = "0 9 * * 1-5"
time_zone = "America/New_York"

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

log_filepath = '/Force approve timesheets for disabled users/Log files'
disabled = True
