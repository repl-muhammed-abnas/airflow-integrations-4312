# pylint: disable=wildcard-import unused-wildcard-import
from pwcfr.otp_master_report_export.config import *

company_key = "pwcfr"
replicon_conn_id = 'pwcfr_replicon_automation.user'
instance = 'production'
environment = 'production'
sftp_conn_id = 'sftp_pwcfr_594688'

sftp_export_filepath = "/PROD/MONITORING/"

tenant_email =  '{{ var.value.dagrun_internal_log_email }}'
