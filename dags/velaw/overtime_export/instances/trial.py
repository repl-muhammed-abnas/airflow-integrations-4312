# pylint: disable=wildcard-import unused-wildcard-import
from velaw.overtime_export.config import *

region = 'us-east-1'
instance = "trial"
environment = 'pre-production'
company_key = 'velawafmig'

replicon_conn_id = 'velawafmig_replicon_admin'
sftp_conn_id = "sftp_useast2"

schedule_interval = "0 19 * * 5"
time_zone = "America/Chicago"

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

log_filepath = '/CMSExport/Production/OT'
disabled = True
