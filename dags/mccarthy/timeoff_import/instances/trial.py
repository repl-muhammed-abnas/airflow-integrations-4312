# pylint: disable=wildcard-import unused-wildcard-import
from mccarthy.timeoff_import.config import *

instance = 'trial'
environment = 'pre-production'
company_key = 'McCarthyafmig'
replicon_conn_id = 'mccarthyafmig_replicon_uuser'
tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

sftp_conn_id = 'Airflow_migration_SFTP_useast2'

input_filepath = '/mccarthy/timeoff_import/Input'
log_filepath = '/mccarthy/timeoff_import/Logs'
archive_filepath = '/mccarthy/timeoff_import/Archive'

can_run_batch_task_var_name = f'mccarthy_timeoff_import_{instance}_can_run_batch_task'
disabled = True
