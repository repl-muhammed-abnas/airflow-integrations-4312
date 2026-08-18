# pylint: disable=wildcard-import unused-wildcard-import
from mammoet.project_import_v1.config import *

instance = "trial"

region = 'eu-central-1'
environment = "pre-production"

company_key = "mammoettrial01trial01"

replicon_conn_id = "mammoettrial01trial01_replicon_admin"
sftp_conn_id = 'rsftp-useast_for_testing'

task_log_filepath = '/mammoet/task/logs'
project_log_filepath = '/mammoet/project/logs'

mammoet_project_bearer_token_variable = "mammoet_project_bearer_token_variable_trial_v1"
mammoet_task_bearer_token_variable = "mammoet_task_bearer_token_variable_trial_v1"

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'

process_project_dag_id = f'mammoet_projects_import_process_each_projects_child_{instance}_v1'
projects_child_dag_id = f'mammoet_projects_import_child_{instance}_v1'
program_child_dag_id = f'mammoet_projects_import_process_programs_child_{instance}_v1'
client_child_dag_id = f'mammoet_projects_import_process_clients_child_{instance}_v1'
task_child_dag_id = f'mammoet_task_import_child_{instance}_v1'
process_each_task_child_dag_id = f'mammoet_task_import_process_each_task_child_{instance}_v1'
project_master_dag_id = f"mammoet_project_import_master_{instance}_v1"
task_master_dag_id = f"mammoet_task_import_master_{instance}_v1"

can_run_batch_task_var_name = f"mammoet_can_run_batch_task_var_name_{instance}"

disabled=True
