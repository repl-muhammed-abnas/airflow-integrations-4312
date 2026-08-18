# pylint: disable=wildcard-import unused-wildcard-import
from hawaiigas.user_import.config import *
region = 'us-east-1'
instance = 'trial'
environment = 'pre-production'
company_key = 'hawaiigasafmig'
replicon_conn_id = 'hawaiigasafmig_replicon_sahmed'

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

sftp_conn_id = 'sftp_useast2'

max_active_runs_child = 5

input_filepath = '/hawaiigas/Input'
reference_filepath = '/hawaiigas/Reference/'
activity_reference_filepath = '/hawaiigas/ActivityReference/'
archive_filepath = '/hawaiigas/Archives/'

can_run_batch_task = f'hawaiigas_user_import_can_run_batch_task_{instance}'
