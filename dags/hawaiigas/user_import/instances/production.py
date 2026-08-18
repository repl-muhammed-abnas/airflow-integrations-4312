# pylint: disable=wildcard-import unused-wildcard-import
from hawaiigas.user_import.config import *
region = 'us-east-1'
instance = 'production'
environment = 'production'
company_key = 'HawaiiGas'
replicon_conn_id = 'hawaiigas_replicon_sahmed'

tenant_email = 'mbermudes@hawaiigas.com,hnguyen@hawaiigas.com'
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

sftp_conn_id = 'sftp_hawaiigas_529941'

max_active_runs_child = 5

input_filepath = '/Input'
reference_filepath = '/reference/'
activity_reference_filepath = '/activityreference/'
archive_filepath = '/Archives/'

can_run_batch_task = f'hawaiigas_user_import_can_run_batch_task_{instance}'
