# pylint: disable=wildcard-import unused-wildcard-import
from pwcglobal.absence_data_extract.config import *

instance = 'internal'

location = 'New Zealand'
location_code = 'NZL'
report_name = "***Absence extract base report - New Zealand"
# Time: 2am Auckland
schedule_interval = "0 2 * * *"
schedule_timezone = "Pacific/Auckland"
replicon_conn_id = 'pwcinternal-replicon-eu.automation'
allowed = "Yes"
time_zone = 'Pacific/Auckland'

sftp_conn_id = 'sftp_pwc_userimport'
output_filepath = '/PwCGBL_RepliconGlobal_STG/internal/Outbound/Time/WD/'
log_filepath = '/PwCGBL_RepliconGlobal_STG/TimeData/Logs/Timeinternal/'
alternate_log_path = "/PwCGBL_RepliconGlobal_STG/internal/Outbound/Time/WD/_logs/"
disabled = True
