# pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.time_entry_auto_submission.config import *

instance = "sandbox"

region = 'us-east-2'
environment = 'pre-production'
company_key = 'dxcsandbox'

pacific_timezone = 'Australia/Sydney'

replicon_conn_id = "dxcsandbox-replicon-RepliconIntC1"

sftp_conn_id = 'dxcsandbox-sftp-628172_C1'
execution_timeout_days = 14

report_name = 'TimeEntrySubmission_For_All_Locations'

log_filepath = '/Test/Outbound/Time Entry Auto Submission Log/'

tenant_email = 'dxcintegrationlogsreplicon@deltek.com'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

country = "Australia"
project_dag_max_active_runs = 10
