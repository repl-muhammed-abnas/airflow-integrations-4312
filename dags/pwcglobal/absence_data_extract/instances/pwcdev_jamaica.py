# pylint: disable=wildcard-import unused-wildcard-import
from pwcglobal.absence_data_extract.config import *

instance = 'pwcdev'
company_key = "pwcdev"

location = 'Jamaica'
location_code = 'JAM'
report_name = "***Absence Extract automation - Jamaica"
# Time: 2am Jamaica
schedule_interval = "0 2 * * *"
schedule_timezone = 'America/Jamaica'
replicon_conn_id = 'pwcdev-replicon-eu.automation'
allowed = "Yes"
time_zone = 'America/Jamaica'

sftp_conn_id = 'pwcglobaldev-MFT-STG-replicon'
output_filepath = '/PwCGBL_RepliconGlobal_STG/DEV/Outbound/Time/WD/'
log_filepath = '/PwCGBL_RepliconGlobal_STG/TimeData/Logs/TimeDEV/'
alternate_log_path = "/PwCGBL_RepliconGlobal_STG/DEV/Outbound/Time/WD/_logs/"

tenant_email = 'PWCGlobalLogs@deltek.com,us_replicondevextintegrationalerts@pwc.com'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
