# pylint: disable=wildcard-import unused-wildcard-import
from necau.auto_shift_assignment.config import *

instance = 'trial'
region = 'eu-central-1'
environment = 'pre-production'
sftp_conn_id = "repliconsftp"
company_key = 'NECAUafmig'
replicon_conn_id = 'NECAUafmig_replicon_admin'
user_shift_report_name = "***Auto Shift Assignment-Master***"
tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
disabled = True

can_run_batch_task_var_name = f'necau_auto_shift_run_batch_task_{instance}'
