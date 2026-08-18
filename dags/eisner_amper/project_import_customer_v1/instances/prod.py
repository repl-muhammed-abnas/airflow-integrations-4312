# pylint: disable=wildcard-import unused-wildcard-import
from eisner_amper.project_import_customer_v1.config import *

instance = 'production'
environment = 'production'

company_key = 'EisnerAmper'

replicon_conn_id = "eisneramper_repliconint.projectimport"
sftp_conn_id = 'sftp_eisneramper_521759'

bearer_token_var = f'eisneramper_project_import_customer_secret_{instance}'

log_filepath = "/Production/Project Import/Customer Project Log"

tenant_email = 'ashwin.ns@infosys.com,sap.alert.replicon@eisneramper.com'
internal_logs = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

can_run_batch_task_var_name = f'eisner_amper_project_import_customer_run_batch_task_{instance}'
