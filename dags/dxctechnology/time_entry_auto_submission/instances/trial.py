# pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.time_entry_auto_submission.config import *

instance = "trial"

region = 'us-east-2'
environment = 'pre-production'
company_key = 'dxctrial01'

pacific_timezone = 'Australia/Sydney'

replicon_conn_id = "dxctrial01"
execution_timeout_days = 14

report_name = 'TimeEntrySubmission_For_All_Locations'

log_filepath = '/DXC/C1WBS/logs/'

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

country = "Australia"
project_dag_max_active_runs = 10

disable=True

disabled=True
