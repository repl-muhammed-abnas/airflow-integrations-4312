# pylint: disable=wildcard-import unused-wildcard-import
from eisner_amper.project_import_internal_v1.config import *

instance = 'sandbox_old'
environment = 'pre-production'

company_key = 'EisnerAmperSandbox'

replicon_conn_id = "eisnerampersandbox_repliconint.projectimport"
sftp_conn_id = 'sftp_eisnerampersandbox_521759'

bearer_token_var = f'eisneramper_project_import_internal_secret_{instance}'

log_filepath = "/Sandbox/Project Import/Internal Project Log"

# pylint: disable=line-too-long
tenant_email = 'Amit.tiwari@eisneramper.com, Richa.sinha@eisneramper.com, sap.integration.support@eisneramper.com, sap.proserv.support@eisneramper.com'
internal_logs = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

can_run_batch_task_var_name = f'eisner_amper_project_import_customer_run_batch_task_{instance}'

disable=True

disabled=True
