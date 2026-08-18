# pylint: disable=wildcard-import unused-wildcard-import
from broadridge.project_import.config import *

region = 'us-east-1'
instance = "prod"
environment = 'production'
company_key = 'broadridge'

replicon_conn_id = 'broadridge_replicon_admin'
sftp_conn_id = "broadridge_sftp_629918"

input_filepath = '/Input'
archive_filepath = '/Archives/'

can_run_batch_task_var_name = f'broadridge_project_import_{instance}_can_run_batch_task'

internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
tenant_email = "projectlogs@broadridge.com"
