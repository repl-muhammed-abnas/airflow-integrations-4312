# pylint: disable=wildcard-import unused-wildcard-import
from pwcglobal.absence_data_extract.config import *

instance = 'internal'

location = 'Singapore'
location_code = 'SGP'
report_name = "***Absence extract base report - Singapore"
# Time: 2am Singapore
schedule_interval = "0 2 * * *"
schedule_timezone = "Asia/Singapore"
replicon_conn_id = 'pwcinternal-replicon-apac.automation'
allowed = "Yes"
time_zone = "Asia/Singapore"

sftp_conn_id = 'sftp_pwc_userimport'
output_filepath = '/PwCGBL_RepliconGlobal_STG/internal/Outbound/Time/WD/'
log_filepath = '/PwCGBL_RepliconGlobal_STG/TimeData/Logs/Timeinternal/'
alternate_log_path = "/PwCGBL_RepliconGlobal_STG/internal/Outbound/Time/WD/_logs/"
disabled = True
