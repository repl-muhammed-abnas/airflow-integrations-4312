
# pylint: disable=wildcard-import unused-wildcard-import
from assuranceagency.timeoff_import_hortongroup.config import *
region = 'us-east-1'
instance = 'trial'
environment = 'pre-production'
company_key = 'AssuranceAgencyTrial01'
replicon_conn_id = 'AssuranceAgencyTrial01_replicon_admin1'

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

sftp_conn_id = 'sftp_assuranceagency_626913'
master_dag_interval = 30

max_active_runs_child = 1

input_filepath = '/Horton Group/Time Off Data/UAT/Input/'
reference_filepath = '/Horton Group/Time Off Data/UAT/Reference/'
archive_filepath = '/Horton Group/Time Off Data/UAT/Archives/'
log_filepath = '/Horton Group/Time Off Data/UAT/Logs/'

can_run_batch_task = f'assuranceagency_timeoff_import_hortongroup_can_run_batch_task_{instance}'
