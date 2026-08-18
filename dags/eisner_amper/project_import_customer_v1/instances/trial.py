# pylint: disable=wildcard-import unused-wildcard-import
from eisner_amper.project_import_customer_v1.config import *

instance = 'trial'
environment = 'pre-production'

company_key = 'eisnerampertrial02'

replicon_conn_id = "eisnerampertrial02_replicon_radmin"
sftp_conn_id = 'sftp_eisnerampertrial02_521759'

bearer_token_var = f'eisneramper_project_import_customer_secret_{instance}'

log_filepath = "/Trial02/Project Import/Customer Project Log"

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

can_run_batch_task_var_name = f'eisner_amper_project_import_customer_run_batch_task_{instance}'

disable=True

disabled=True
