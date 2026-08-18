# pylint: disable=wildcard-import unused-wildcard-import
from velaw.project_import.config import *
region = 'us-east-1'
instance = 'trial'
environment = 'pre-production'
company_key = 'velawafmig'
replicon_conn_id = 'velawafmig_replicon_rintegrations'
sftp_conn_id = 'Airflow_migration_SFTP_useast2'

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'


input_filepath = '/Velaw/project_import/Input'
reference_filepath = '/Velaw/project_import/Reference'
archive_filepath = '/Velaw/project_import/Archive'
log_filepath = '/Velaw/project_import/Logs'

can_run_batch_task_var_name = f'velaw_project_import_{instance}_can_run_batch_task'

disabled=True
