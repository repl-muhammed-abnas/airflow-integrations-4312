# pylint: disable=wildcard-import unused-wildcard-import
from cie_gen3_products.banking_utility.config import *

region = 'us-east-1'
environment = 'pre-production'

instance = "trial"
company_key = 'hrctrial01'
replicon_conn_id = 'Replicon_Connection_BankingUtility_hrctrial01'
wTSDateFormat = 'MMM d, yyyy'


base_report_name = '*Replicon CompTime Report'
user_report_filter_settings = True

time_zone = "America/New_York"
days_to_read_data = 30
approval_status = 'Not Submitted,Waiting for Approval,Approved,Rejected,Submitting'
activity_names = ''
IsNewTimeOffScriptPlatform = 'false'

schedule_interval = "00 21 * * WED"
time_zone = "UTC"
max_active_runs = 1
execution_timeout_days = 14

tenant_email = "ashishtiwari@deltek.com"
internal_logs_email = "ashishtiwari@deltek.com"
alert_email = '{{ var.value.dagrun_failure_alert_email }}'


can_run_batch_task_var_name = "can_run_batch_task_var_name"
disabled = True
