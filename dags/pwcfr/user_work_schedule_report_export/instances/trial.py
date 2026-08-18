# pylint: disable=wildcard-import unused-wildcard-import
from pwcfr.user_work_schedule_report_export.config import *
instance = 'trial'
company_key = 'pwcfrafmig'
replicon_conn_id = "pwcframig_replicon_automation.user"
sftp_conn_id = "sftp_useast2"
sftp_export_file_path = "/PWCFRAFMIG/MONITORING/"

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
disabled = True
