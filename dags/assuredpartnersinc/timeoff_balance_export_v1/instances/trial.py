# pylint: disable=wildcard-import unused-wildcard-import
from assuredpartnersinc.timeoff_balance_export_v1.config import *

instance = 'trial'
environment = 'pre-production'

company_key = 'AssuredpartnersIncafmig'

replicon_conn_id = 'assuredpartnersincafmig-replicon-admin'
sftp_conn_id = 'sftp_useast2'
sftp_path = "/PTO Export/TRIAL"
tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
next_run_date = "assuredpartnersinc_timeoff_balance_export_next_run_date"
disabled=True
