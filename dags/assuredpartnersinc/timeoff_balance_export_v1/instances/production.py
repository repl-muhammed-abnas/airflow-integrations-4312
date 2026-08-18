# pylint: disable=wildcard-import unused-wildcard-import
from assuredpartnersinc.timeoff_balance_export_v1.config import *

instance = 'production'
environment = 'production'

company_key = 'AssuredPartnersInc'

replicon_conn_id = 'assuredpartnersinc-replicon-admin'
sftp_conn_id = 'sftp_assuredpartnersinc_564238_AssuredPartnersInc'
sftp_path = "/PTO Export/PROD"

tenant_email = "diego.suarez@assuredpartners.com, kanetra.clayton@assuredpartners.com,kayla.fawkes@assuredpartners.com,HRIS@assuredpartners.com,apcorporatepayroll@assuredpartners.com"
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
next_run_date = "assuredpartnersinc_timeoff_balance_export_next_run_date"
disabled=True
