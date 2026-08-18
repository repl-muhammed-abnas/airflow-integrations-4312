# pylint: disable=wildcard-import unused-wildcard-import
from eisner_amper.project_import_internal_v1.config import *

instance = 'trial'
environment = 'pre-production'

company_key = 'eisnerampertrial02'

replicon_conn_id = "eisnerampertrial02_replicon_radmin"
sftp_conn_id = 'sftp_eisnerampertrial02_521759'

log_filepath = "/Trial01/Project Import/Internal Project Log"

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

bearer_token_var = f'eisneramper_project_import_internal_secret_{instance}'

can_run_batch_task_var_name = f'esiner_amper_project_import_internal_can_run_batch_task{instance}'

disable=True

disabled=True
