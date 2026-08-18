# pylint: disable=wildcard-import unused-wildcard-import
from mccarthy.project_import_v1.config import *

instance = 'trial'
environment = 'pre-production'
company_key = 'mccarthytrial01'
replicon_conn_id = 'mccarthytrial01_replicon_uuser'
tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

sftp_conn_id = 'sftp_useast2'

input_filepath = '/mccarthy/Input'
processing_filepath = '/mccarthy/Processing/'
upload_filepath = '/mccarthy/Logs/'
archive_filepath = '/mccarthy/Archive/'

can_run_batch_task_child = f'mccarthy_project_import_child_can_run_batch_task_{instance}_v1'

master_dag = f"mccarthy_project_import_in_replicon_master_{instance}_v1"
process_new_projects_dag = f"mccarthy_project_import_process_new_projects_child_dag_{instance}_v1"
create_projects_dag = f"mccarthy_project_import_creating_projects_in_replicon_child_{instance}_v1"
update_projects_dag = f"mccarthy_project_import_update_projects_in_replicon_child_{instance}_v1"
create_update_task_dag = f"mccarthy_project_import_create_update_task_in_replicon_child_{instance}_v1"

disabled=True
