# pylint: disable=wildcard-import unused-wildcard-import
from pwcglobal.absence_data_extract.config import *

instance = 'pwcqa'
company_key = "PwCQA"

location = 'British Virgin Islands (BVI)'
location_code = 'VGB'
report_name = "***Absence Extract automation - Virgin Islands BVI"
# Time: 2am British Virgin Island
schedule_interval = "0 2 * * *"
schedule_timezone = 'America/Virgin'
replicon_conn_id = 'pwcqa-replicon-eu.automation'
allowed = "Yes"
time_zone = 'America/Virgin'

sftp_conn_id = 'pwcglobalqa-MFT-STG-replicon'
output_filepath = '/PwCGBL_RepliconGlobal_STG/QA/Outbound/Time/WD/'
log_filepath = '/PwCGBL_RepliconGlobal_STG/TimeData/Logs/TimeQA/'
alternate_log_path = "/PwCGBL_RepliconGlobal_STG/QA/Outbound/Time/WD/_logs/"

tenant_email = 'PWCGlobalLogs@deltek.com,us_repliconqaextintegrationalerts@pwc.com'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
