
# pylint: disable=wildcard-import unused-wildcard-import
from assuranceagency.user_import.config import *
region = 'us-east-1'
instance = 'production'
environment = 'production'
company_key = 'Assuranceagency'
replicon_conn_id = 'assuranceagency_replicon_admin1'

to_email = "paige.kruis@marshmma.com,elaina.talley@marshmma.com,bree.oshea@marshmma.com,kayla.johnson@marshmma.com"
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

input_filepath = '/Horton Group/User Import Data/Production/Input/'
reference_filepath = '/Horton Group/User Import Data/Production/Reference/'
archive_filepath = '/Horton Group/User Import Data/Production/Archives/'
log_filepath = '/Horton Group/User Import Data/Production/Logs/'

can_run_batch_task_var_name = f'assuranceagency_user_import_hortongroup_can_run_batch_task_{instance}'
can_use_reference_file = f'assuranceagency_user_import_hortongroup_can_use_reference_file_{instance}'
