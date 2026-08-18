
# pylint: disable=wildcard-import unused-wildcard-import
from assuranceagency.timeoff_import.config import *
region = 'us-east-1'
instance = 'production'
environment = 'production'
company_key = 'AssuranceAgency'
replicon_conn_id = 'assuranceagency_replicon_admin1'

tenant_email = 'cdreps@assuranceagency.com,koranger@assuranceagency.com,cwhite@assuranceagency.com'
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

sftp_conn_id = 'sftp_assuranceagency_626913'
master_dag_interval = 30

max_active_runs_child = 1

input_filepath = '/Time Off Data/Production/Input'
reference_filepath = '/Time Off Data/Production/Reference/'
archive_filepath = '/Time Off Data/Production/Archives/'
log_filepath = '/Time Off Data/Production/Logs/'

can_run_batch_task = f'assuranceagency_timeoff_import_can_run_batch_task_{instance}'
