# pylint: disable=wildcard-import unused-wildcard-import
from pwcfr.report_export_monitoring_timeoff.config import *

instance = "trial"
environment = 'pre-production'
company_key = 'pwcfrafmig'
sftp_connid = "sftp_airflowmig_eucentral"
sftp_file_export_path = "/PREPROD/MONITORING"
replicon_connid = "pwcfrafmig_replicon_automation.user"
master_dagid=f"pwcfr_report_export_monitoring_timeoff_master_{instance}"
