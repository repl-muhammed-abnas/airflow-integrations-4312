
# pylint: disable=wildcard-import unused-wildcard-import
from assuranceagency.timeoff_import.config import *
region = 'us-east-1'
instance = 'trial'
environment = 'pre-production'
company_key = 'assuranceagencyafmig'
replicon_conn_id = 'assuranceagencyafmig_replicon_admin1'

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

sftp_conn_id = 'sftp_useast2'
master_dag_interval = 30

max_active_runs_child = 1

input_filepath = '/assuranceagency/Input'
reference_filepath = '/assuranceagency/Reference/'
archive_filepath = '/assuranceagency/Archives/'
log_filepath = '/assuranceagency/Logs/'

can_run_batch_task = f'assuranceagency_timeoff_import_can_run_batch_task_{instance}'

disabled=True
