# pylint: disable=wildcard-import unused-wildcard-import
from pwcfr.user_report_export.config import *

instance = "production"
environment="production"
company_key = "pwcfr"
sftp_file_export_path = "/PROD/MONITORING/"
sftp_connid = "sftp_pwcfr_594688"
replicon_connid = "pwcfr_replicon_automation.user"
alerts_email = '{{ var.value.dagrun_failure_alert_email }}'
