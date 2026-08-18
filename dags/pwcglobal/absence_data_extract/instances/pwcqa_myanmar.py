# pylint: disable=wildcard-import unused-wildcard-import
from pwcglobal.absence_data_extract.config import *

instance = 'pwcqa'
company_key = "PwCQA"

location = 'Myanmar'
location_code = 'MMR'
report_name = "***Absence Extract automation - Myanmar"
# Time: 2am Rangoon
schedule_interval = "0 2 * * *"
schedule_timezone = "Asia/Rangoon"
replicon_conn_id = 'pwcqa-replicon-apac.automation'
allowed = "Yes"
time_zone = "Asia/Singapore"

sftp_conn_id = 'pwcglobalqa-MFT-STG-replicon'
output_filepath = '/PwCGBL_RepliconGlobal_STG/QA/Outbound/Time/WD/'
log_filepath = '/PwCGBL_RepliconGlobal_STG/TimeData/Logs/TimeQA/'
alternate_log_path = "/PwCGBL_RepliconGlobal_STG/QA/Outbound/Time/WD/_logs/"

tenant_email = 'PWCGlobalLogs@deltek.com,us_repliconqaextintegrationalerts@pwc.com'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
