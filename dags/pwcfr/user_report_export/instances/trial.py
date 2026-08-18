# pylint: disable=wildcard-import unused-wildcard-import
from pwcfr.user_report_export.config import *

instance = "trial"
company_key = "pwcfrafmig"
sftp_file_export_path = "/PWCFRAFMIG/MONITORING/"
sftp_connid = "sftp_useast2"
replicon_connid = "pwcframig_replicon_automation.user"
alerts_email = '{{ var.value.dagrun_internal_testing_email }}'
disabled = True
