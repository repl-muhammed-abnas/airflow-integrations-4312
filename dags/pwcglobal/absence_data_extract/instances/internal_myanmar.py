# pylint: disable=wildcard-import unused-wildcard-import
from pwcglobal.absence_data_extract.config import *

instance = 'internal'

location = 'Myanmar'
location_code = 'MMR'
report_name = "***Absence extract base report - Myanmar"
# Time: 2am Rangoon
schedule_interval = "0 2 * * *"
schedule_timezone = "Asia/Rangoon"
replicon_conn_id = 'pwcinternal-replicon-apac.automation'
allowed = "Yes"
time_zone = "Asia/Singapore"

sftp_conn_id = 'sftp_pwc_userimport'
output_filepath = '/PwCGBL_RepliconGlobal_STG/internal/Outbound/Time/WD/'
log_filepath = '/PwCGBL_RepliconGlobal_STG/TimeData/Logs/Timeinternal/'
alternate_log_path = "/PwCGBL_RepliconGlobal_STG/internal/Outbound/Time/WD/_logs/"

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
disabled = True
