# pylint: disable=wildcard-import unused-wildcard-import
from mammoet.project_import_v1.config import *

instance = "prod"

region = 'eu-central-1'
environment = "production"

company_key = "mammoet"

replicon_conn_id = "mammoet_replicon_admin"
sftp_conn_id = 'sftp_mammoet_550793'

task_log_filepath = "/Production/Project Import/CS Order Log"
project_log_filepath = "/Production/Project Import/Log"

mammoet_project_bearer_token_variable = f"mammoet_project_bearer_token_variable_{instance}"
mammoet_task_bearer_token_variable = f"mammoet_task_bearer_token_variable_{instance}"

tenant_email = 'RepliconNotifications@mammoet.com'
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

process_project_dag_id = f'mammoet_projects_import_process_each_projects_child_{instance}_v1'
projects_child_dag_id = f'mammoet_projects_import_child_{instance}_v1'
program_child_dag_id = f'mammoet_projects_import_process_programs_child_{instance}_v1'
client_child_dag_id = f'mammoet_projects_import_process_clients_child_{instance}_v1'
task_child_dag_id = f'mammoet_task_import_child_{instance}_v1'
process_each_task_child_dag_id = f'mammoet_task_import_process_each_task_child_{instance}_v1'
project_master_dag_id = f"mammoet_project_import_master_{instance}"
task_master_dag_id = f"mammoet_task_import_master_{instance}"

can_run_batch_task_var_name = f"mammoet_project_import_can_run_batch_task_var_name_{instance}"
