# pylint: disable=wildcard-import unused-wildcard-import
from velaw.user_import.config import *
region = 'us-east-1'
instance = 'prod'
environment = 'production'
company_key = 'Velaw'
replicon_conn_id = 'velaw_replicon_Rintegrations'
sftp_conn_id = 'sftp_velaw_524663'

tenant_email = 'hris@velaw.com'
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'


input_filepath = '/HRIS_UserImport/Input/'
reference_filepath = '/HRIS_UserImport/Reference/'
archive_filepath = '/HRIS_UserImport/Archive/'
log_filepath = '/HRIS_UserImport/Logs/'

can_run_batch_task_var_name = f'velaw_user_import_{instance}_can_run_batch_task'
