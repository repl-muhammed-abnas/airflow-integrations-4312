# pylint: disable=wildcard-import unused-wildcard-import
from pwcfr.otp_master_report_export.config import *

company_key = "pwcfrafmig"
replicon_conn_id = 'pwcframig_replicon_automation.user'
instance = 'trial'
sftp_conn_id = 'sftp_useast2'

sftp_export_filepath = "/PWCFRAFMIG/MONITORING/"

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
disabled = True
