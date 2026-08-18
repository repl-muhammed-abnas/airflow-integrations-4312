# pylint: disable=wildcard-import unused-wildcard-import
from balparag3.project_import.config import *

instance = 'trial'
environment = 'pre-production'

company_key = 'balparag3afmig'
replicon_conn_id = 'balparag3afmig-replicon-sblanche'

sftp_conn_id = 'sftp_eucentral1_airflow'

internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

can_run_batch_task_var_name = f'balparag3_project_import_{instance}_can_run_batch_task'
disabled = True
