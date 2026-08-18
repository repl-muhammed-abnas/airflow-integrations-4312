# pylint: disable=wildcard-import unused-wildcard-import
from pwcglobal.absence_data_extract.config import *

region = 'eu-central-1'
environment = 'production'

instance = 'production'
company_key = "PwC"

location = 'Greece'
location_code = 'GRC'
report_name = "***Absence Extract automation - Greece"
# Time: 2am Paris
schedule_interval = "0 2 * * *"
schedule_timezone = 'Europe/Paris'
replicon_conn_id = 'pwcglobal-replicon-eu.automation'
allowed = "Yes"
time_zone = 'Europe/Paris'

sftp_conn_id = 'pwcglobal-MFT-PRD-replicon'
output_filepath = '/PwCGBL_RepliconGlobal_PRD/PRD/Outbound/Time/'
log_filepath = '/PwCGBL_RepliconGlobal_PRD/PRD/Outbound/Time/_logs/'
alternate_log_path = ""

tenant_email = 'gbl_replicon_support_team@pwc.com'
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
