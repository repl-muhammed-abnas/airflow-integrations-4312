# pylint: disable=wildcard-import unused-wildcard-import
from necau.time_off_shift_assignment_90_days_rolling_period.config import *

instance = 'production'
region = 'eu-central-1'
environment = 'production'
company_key = 'necau'
replicon_conn_id = 'necau-replicon-admin'
tenant_email = "HRISSupport@nec.com.au"
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
user_shift_report_name = "***Shift Assignment for Time Off User reference_V2"
dag_max_active_runs = 10
