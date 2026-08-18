# pylint: disable=wildcard-import unused-wildcard-import
from audaxgroup.project_task_import.config import *

instance = 'trial'
environment = 'pre-production'
company_key = 'Audaxgroupafmig'
replicon_conn_id = 'Audaxgroupafmig_replicon_admin'
sftp_conn_id = 'sftp_useast2'

input_filepath = '/audaxgroup/project_task_import/input'
archive_filepath = '/audaxgroup/project_task_import/archive'
log_filepath = '/audaxgroup/project_task_import/logs'
processing_filepath = '/audaxgroup/project_task_import/processing'

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'


can_run_batch_task_var_name = f'audaxgroup_project_import_{instance}_can_run_batch_task'

master_dagid = f'audaxgroup_project_import_master_{instance}'
process_put_projects_file_dag_id = f'audaxgroup_process_put_projects_file_child_{instance}'
process_update_projects_file_dag_id = f'audaxgroup_process_update_projects_file_child_{instance}'
process_tasks_file_dag_id = f'audaxgroup_process_tasks_file_child_{instance}'
process_tasks_per_project_dag_id = f'audaxgroup_process_tasks_per_project_child_{instance}'
add_update_projects_dag_id = f'audaxgroup_add_update_projects_child_{instance}'
add_update_tasks_dag_id = f'audaxgroup_add_update_tasks_child_{instance}'

disabled=True
