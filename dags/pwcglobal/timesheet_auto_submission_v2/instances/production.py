# pylint: disable=wildcard-import unused-wildcard-import
from pwcglobal.timesheet_auto_submission_v2.config import *

instance = 'PwC'
region = 'eu-central-1'
environment = 'production'

company_key = 'PwC'
replicon_conn_id = 'pwcglobal-replicon-eu.automation'

tenant_email = "gbl_replicon_support_team@pwc.com"
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
