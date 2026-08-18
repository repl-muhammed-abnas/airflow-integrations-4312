# pylint: disable=wildcard-import unused-wildcard-import
from macquariegroup.clientimport.config import *

instance = "prod"

company_key = "MacquarieProduction"
region = 'eu-central-1'
environment = 'production'

sftp_conn_id = 'macquarieproduction_sftp_22007'
replicon_conn_id = 'macquarieproduction_replicon_ltran17'
internal_email = '{{ var.value.dagrun_internal_log_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

tenant_email = 'COGTechFORFinanceRepliconSupport@macquarie.com'

can_run_batch_task_var_name = f'macquarie_client_import_{instance}_can_run_batch_task'
master_trigger_schedules_var_name = f'macquarie_client_import_master_trigger_schedules_{instance}'

input_filepath = '/Client_Import/Input'
archive_filepath = '/Client_Import/Archive'
reference_filepath = '/Client_Import/Reference'
log_filepath = '/Client_Import/Logs'
