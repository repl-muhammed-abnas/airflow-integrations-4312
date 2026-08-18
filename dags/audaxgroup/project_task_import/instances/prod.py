# pylint: disable=wildcard-import unused-wildcard-import
from audaxgroup.project_task_import.config import *

instance = 'production'
environment = 'production'
company_key = 'Audaxgroup'
replicon_conn_id = 'Audaxgroup_replicon_admin'
sftp_conn_id = 'sftp_audaxgroup_515827'

input_filepath = '/ProjectTaskImport/Input'
archive_filepath = '/ProjectTaskImport/Archive'
log_filepath = '/ProjectTaskImport/Logs'
processing_filepath = '/ProjectTaskImport/Processing'

tenant_email = 'xIntegration-Replicon@audaxgroup.com'
internal_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'


can_run_batch_task_var_name = f'audaxgroup_project_import_{instance}_can_run_batch_task'

master_dagid = f'audaxgroup_project_import_master_{instance}'
process_put_projects_file_dag_id = f'audaxgroup_process_put_projects_file_child_{instance}'
process_update_projects_file_dag_id = f'audaxgroup_process_update_projects_file_child_{instance}'
process_tasks_file_dag_id = f'audaxgroup_process_tasks_file_child_{instance}'
process_tasks_per_project_dag_id = f'audaxgroup_process_tasks_per_project_child_{instance}'
add_update_projects_dag_id = f'audaxgroup_add_update_projects_child_{instance}'
add_update_tasks_dag_id = f'audaxgroup_add_update_tasks_child_{instance}'
