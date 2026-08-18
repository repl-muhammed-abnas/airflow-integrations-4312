# pylint: disable=wildcard-import unused-wildcard-import
from pwcglobal.timesheet_approve_delete_disbaled_users.config import *

instance = 'trial'

company_key = 'PwCQA'
replicon_conn_id = 'PwCQA_replicon_eu.automation'
sftp_conn_id = "sftp_useast2"

log_filepath = '/PWC/PwCQA/timesheet_approve_delete_disbaled_users'

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
