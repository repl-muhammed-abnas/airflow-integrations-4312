# pylint: disable=wildcard-import unused-wildcard-import
from eisner_amper.project_import_internal_v1.config import *

instance = 'production'
environment = 'production'

company_key = 'EisnerAmper'

replicon_conn_id = "eisneramper_repliconint.projectimport"
sftp_conn_id = 'sftp_eisneramper_521759'

log_filepath = "/Production/Project Import/Internal Project Log"

tenant_email = 'ashwin.ns@infosys.com,sap.alert.replicon@eisneramper.com'
internal_logs = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

bearer_token_var = f'eisneramper_project_import_internal_secret_{instance}'

can_run_batch_task_var_name = f'esiner_amper_project_import_internal_can_run_batch_task{instance}'
