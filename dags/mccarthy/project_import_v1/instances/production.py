# pylint: disable=wildcard-import unused-wildcard-import
from mccarthy.project_import_v1.config import *

instance = 'production'
environment = 'production'
company_key = 'McCarthy'
replicon_conn_id = 'mccarthy_replicon_uuser'
tenant_email = 'ABhaskerKumar@McCarthy.com,SBuerk@mccarthy.com'
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
max_active_runs_child=3

sftp_conn_id = 'sftp_mccarthy_524959'

input_filepath = '/Gen3Production/ProjectImport/Input'
processing_filepath = '/Gen3Production/ProjectImport/Processing/'
upload_filepath = '/Gen3Production/ProjectImport/Logs/'
archive_filepath = '/Gen3Production/ProjectImport/Archive/'

can_run_batch_task_child = f'mccarthy_project_import_child_can_run_batch_task_{instance}_v1'

master_dag = f"mccarthy_project_import_in_replicon_master_{instance}_v1"
process_new_projects_dag = f"mccarthy_project_import_process_new_projects_child_dag_{instance}_v1"
create_projects_dag = f"mccarthy_project_import_creating_projects_in_replicon_child_{instance}_v1"
update_projects_dag = f"mccarthy_project_import_update_projects_in_replicon_child_{instance}_v1"
create_update_task_dag = f"mccarthy_project_import_create_update_task_in_replicon_child_{instance}_v1"
