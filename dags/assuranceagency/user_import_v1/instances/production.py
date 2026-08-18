
# pylint: disable=wildcard-import unused-wildcard-import
from assuranceagency.user_import_v1.config import *
region = 'us-east-1'
instance = 'production'
environment = 'production'
company_key = 'Assuranceagency'
replicon_conn_id = 'assuranceagency_replicon_admin1'

to_email = "kayla.johnson@marshmma.com,Christina.Dreps@MarshMMA.com,cdreps@assuranceagency.com,koranger@assuranceagency.com,cwhite@assuranceagency.com"
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

user_name = "admin"

sftp_conn_id = 'sftp_assuranceagency_626913'
master_dag_interval = 30
max_active_runs = 1
max_active_runs_add_user_child = 5
max_active_runs_disable_user_child = 5
max_active_runs_update_user_child = 5
max_active_runs_update_supervisor_child = 5
max_active_runs_dynamic_wait_child = 5

input_filepath = '/User Import Data/Production/Input/'
reference_filepath = '/User Import Data/Production/Reference/'
archive_filepath = '/User Import Data/Production/Archives/'
log_filepath = '/User Import Data/Production/Logs/'

can_run_batch_task_var_name = f'assuranceagency_user_import_can_run_batch_task_{instance}'
can_use_reference_file = f'assuranceagency_user_import_can_use_reference_file_{instance}'

master_dag_id = f'assuranceagency_user_import_master_{instance}_v1'
add_user_child_dag_id = f'assuranceagency_user_import_add_user_child_{instance}_v1'
disable_user_child_dag_id = f'assuranceagency_user_import_disable_user_child_{instance}_v1'
update_user_child_dag_id = f'assuranceagency_user_import_update_user_child_{instance}_v1'
update_supervisor_child_dag_id = f'assuranceagency_user_import_update_supervisor_from_logs_child_{instance}_v1'