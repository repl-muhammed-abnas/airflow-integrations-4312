# pylint: disable=wildcard-import unused-wildcard-import
from pwcfr.report_export_monitoring_timeoff.config import *

instance = 'production'
environment = 'production'
company_key = 'PWCFR'

sftp_connid = "sftp_pwcfr_prod_594688"
sftp_file_export_path = "/PROD/MONITORING"

replicon_connid = "pwcfr_prod_replicon_automation.user"

master_dagid = f"pwcfr_report_export_monitoring_timeoff_master_{instance}"
