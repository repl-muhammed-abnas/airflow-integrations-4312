# pylint: disable=wildcard-import unused-wildcard-import
from broadridge.project_import.config import *

region = 'us-east-1'
instance = "trial"
environment = 'pre-production'
company_key = 'broadridgeafmig'

replicon_conn_id = 'broadridgeafmig_replicon_admin'
sftp_conn_id = "sftp_useast2"

input_filepath = '/broadridge/Input'
archive_filepath = '/broadridge//Archives/'

can_run_batch_task_var_name = f'broadridge_project_import_{instance}_can_run_batch_task'

internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
tenant_email = '{{ var.value.dagrun_internal_testing_email }}'

disabled=True
