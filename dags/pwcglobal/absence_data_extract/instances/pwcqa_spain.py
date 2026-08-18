# pylint: disable=wildcard-import unused-wildcard-import
from pwcglobal.absence_data_extract.config import *

instance = 'pwcqa'
company_key = "PwCQA"

location = 'Spain'
location_code = 'ESP'
report_name = "***Absence Extract automation - Spain"
# Time: 2am local (GMT+1)
schedule_interval = "0 2 * * *"
schedule_timezone = 'Europe/Paris'
replicon_conn_id = 'pwcqa-replicon-eu.automation'
allowed = "Yes"
time_zone = 'Europe/Paris'

sftp_conn_id = 'pwcglobalqa-MFT-STG-replicon'
output_filepath = '/PwCGBL_RepliconGlobal_STG/QA/Outbound/Time/'
log_filepath = '/PwCGBL_RepliconGlobal_STG/TimeData/Logs/TimeQA/'
alternate_log_path = "/PwCGBL_RepliconGlobal_STG/QA/Outbound/Time/_logs/"

tenant_email = 'PWCGlobalLogs@deltek.com,us_repliconqaextintegrationalerts@pwc.com'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
