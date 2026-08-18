# pylint: disable=wildcard-import unused-wildcard-import
from pwcfr.user_work_schedule_report_export.config import *
instance = 'production'
environment = 'production'
company_key = 'pwcfr'
replicon_conn_id = "pwcfr_replicon_automation.user"
sftp_conn_id = "sftp_pwcfr_594688"
sftp_export_file_path = "/PROD/MONITORING/"

tenant_email =  '{{ var.value.dagrun_internal_log_email }}'
