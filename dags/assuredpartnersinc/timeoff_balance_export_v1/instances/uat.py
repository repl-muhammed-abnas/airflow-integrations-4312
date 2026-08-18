# pylint: disable=wildcard-import unused-wildcard-import
from assuredpartnersinc.timeoff_balance_export_v1.config import *

instance = 'uat'
environment = 'pre-production'

company_key = 'AssuredpartnersInctrial03'

replicon_conn_id = 'assuredpartnersinctrial03-replicon-admin'
sftp_conn_id = 'assuredpartnersinctrial03_sftp_564238'
sftp_path = "/User Import UAT/PTO Export/TRIAL"

tenant_email = "diego.suarez@assuredpartners.com, kanetra.clayton@assuredpartners.com,kayla.fawkes@assuredpartners.com,HRIS@assuredpartners.com,apcorporatepayroll@assuredpartners.com"
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
next_run_date = "assuredpartnersinc_timeoff_balance_export_next_run_date"