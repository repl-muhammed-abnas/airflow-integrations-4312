# pylint: disable=wildcard-import unused-wildcard-import
from velaw.timesheet_oef_import.config import *
region = 'us-east-1'
instance = 'trial'
environment = 'pre-production'
company_key = 'velawafmig'
replicon_conn_id = 'velawafmig_replicon_rintegrations'
tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

sftp_conn_id = 'sftp_useast2'
input_filepath = '/Velaw/Input'
upload_filepath = 'Velaw/Timesheet UDF/'
archive_filepath = 'Velaw/Authorizerupdate/Production/Archive/'
disabled = True
