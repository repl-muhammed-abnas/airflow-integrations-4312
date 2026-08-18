# pylint: disable=wildcard-import unused-wildcard-import
from pwcglobal.absence_data_extract.config import *

instance = 'internal'
company_key = "PwCInternal"

location = 'AC Integrado'
location_code = 'ACI'
report_name = "***Absence Extract automation - AC Integrado"
# Time: 2am Argentina
schedule_interval = "0 2 * * *"
schedule_timezone = 'America/Argentina/Catamarca'
replicon_conn_id = 'pwcinternal-replicon-eu.automation'
allowed = "Yes"
time_zone = 'America/Argentina/Catamarca'

sftp_conn_id = 'sftp_pwc_userimport'
output_filepath = '/PwCGBL_RepliconGlobal_STG/internal/Outbound/Time/WD/'
log_filepath = '/PwCGBL_RepliconGlobal_STG/TimeData/Logs/Timeinternal/'
alternate_log_path = "/PwCGBL_RepliconGlobal_STG/internal/Outbound/Time/WD/_logs/"

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
disabled = True
