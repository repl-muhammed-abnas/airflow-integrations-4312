# pylint: disable=wildcard-import unused-wildcard-import line-too-long
from pwcglobal.user_import.config import *

instance = 'PwCDev'
region = 'eu-central-1'
environment = 'pre-production'

company_key = 'PwCDev'
replicon_conn_id = 'pwcdev-replicon-eu.userimport'
sftp_conn_id = "pwcglobaldev-MFT-STG-replicon"

input_filepath = "/PwCGBL_RepliconGlobal_STG/DEV/Inbound/Staff/PMDM"
archive_filepath = "/PwCGBL_RepliconGlobal_STG/DEV/Inbound/Staff/PMDM/_archive"
log_filepath = "/PwCGBL_BOSALLogs_STG/QA/ToPwC"

tenant_email = 'PWCGlobalLogs@deltek.com,us_replicondevextintegrationalerts@pwc.com'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
disabled = True
